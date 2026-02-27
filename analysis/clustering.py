"""
Prompt clustering — groups prompts into behavioural categories using
embeddings and unsupervised clustering.

Workflow:
  1. Generate embeddings for each prompt via OpenAI's embedding API.
  2. Reduce dimensionality (optional, for visualisation).
  3. Cluster with HDBSCAN (density-based, no need to pre-specify k) or
     K-means (when a fixed k is desired).
  4. Auto-label each cluster by passing representative prompts through an LLM.
  5. Persist cluster assignments to the ``prompt_clusters`` table.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import openai
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score

from config.settings import ClusteringConfig, get_settings
from data_pipeline.database import execute, fetch_all

logger = logging.getLogger(__name__)

# Try HDBSCAN; fall back to K-means if not installed
try:
    import hdbscan  # type: ignore

    _HDBSCAN_AVAILABLE = True
except ImportError:
    _HDBSCAN_AVAILABLE = False
    logger.info("hdbscan not installed — K-means will be used as fallback")


# ---------------------------------------------------------------------------
# Embedding generation
# ---------------------------------------------------------------------------

def generate_embeddings(
    texts: List[str],
    model: str | None = None,
) -> np.ndarray:
    """
    Generate embeddings for a list of texts via the OpenAI embedding API.

    Returns an (n, d) numpy array.
    """
    settings = get_settings()
    model = model or settings.clustering.embedding_model
    client = openai.OpenAI(api_key=settings.llm.openai_api_key)

    response = client.embeddings.create(input=texts, model=model)
    embeddings = [item.embedding for item in response.data]
    matrix = np.array(embeddings, dtype=np.float32)
    logger.info("Generated %d embeddings (dim=%d) with model=%s", len(texts), matrix.shape[1], model)
    return matrix


# ---------------------------------------------------------------------------
# Clustering algorithms
# ---------------------------------------------------------------------------

def cluster_hdbscan(
    embeddings: np.ndarray,
    min_cluster_size: int = 5,
) -> Tuple[np.ndarray, float]:
    """
    Cluster embeddings using HDBSCAN.

    Returns (labels, silhouette).
    """
    if not _HDBSCAN_AVAILABLE:
        raise ImportError("hdbscan is not installed")

    clusterer = hdbscan.HDBSCAN(min_cluster_size=min_cluster_size, metric="euclidean")
    labels = clusterer.fit_predict(embeddings)

    # Silhouette score excludes noise points (label == -1)
    mask = labels >= 0
    sil = float(silhouette_score(embeddings[mask], labels[mask])) if mask.sum() > 1 else 0.0

    n_clusters = len(set(labels) - {-1})
    logger.info("HDBSCAN found %d clusters (noise=%d, silhouette=%.3f)", n_clusters, (~mask).sum(), sil)
    return labels, sil


def cluster_kmeans(
    embeddings: np.ndarray,
    n_clusters: int = 6,
) -> Tuple[np.ndarray, float]:
    """
    Cluster embeddings using K-means.

    Returns (labels, silhouette).
    """
    km = KMeans(n_clusters=n_clusters, n_init=10, random_state=42)
    labels = km.fit_predict(embeddings)
    sil = float(silhouette_score(embeddings, labels))
    logger.info("K-means (%d clusters) silhouette=%.3f", n_clusters, sil)
    return labels, sil


# ---------------------------------------------------------------------------
# Cluster labelling via LLM
# ---------------------------------------------------------------------------

def label_clusters(
    prompts: List[str],
    labels: np.ndarray,
    max_samples_per_cluster: int = 5,
) -> Dict[int, str]:
    """
    Auto-generate a human-readable label for each cluster by asking an LLM
    to summarise representative prompts.
    """
    settings = get_settings()
    client = openai.OpenAI(api_key=settings.llm.openai_api_key)

    cluster_ids = sorted(set(labels) - {-1})
    label_map: Dict[int, str] = {}

    for cid in cluster_ids:
        members = [p for p, l in zip(prompts, labels) if l == cid]
        sample = members[:max_samples_per_cluster]

        user_msg = (
            "Below are user prompts that belong to the same behavioural cluster.\n"
            "Provide a short (3–6 word) label that captures the shared intent.\n"
            "Return ONLY the label, nothing else.\n\n"
            + "\n".join(f"- {s}" for s in sample)
        )

        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": user_msg}],
            temperature=0.0,
            max_tokens=30,
        )
        label = (response.choices[0].message.content or "unknown").strip().strip('"')
        label_map[cid] = label
        logger.debug("Cluster %d → %s", cid, label)

    # Noise cluster
    if -1 in set(labels):
        label_map[-1] = "unclustered"

    return label_map


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def run_clustering(
    config: ClusteringConfig | None = None,
) -> Dict[str, Any]:
    """
    End-to-end clustering pipeline:
      1. Fetch prompts from DB
      2. Generate embeddings
      3. Cluster
      4. Label
      5. Persist to ``prompt_clusters``

    Returns a summary dict with cluster counts and labels.
    """
    config = config or get_settings().clustering

    # 1. Fetch prompts
    rows = fetch_all("SELECT prompt_id, prompt_text FROM prompts WHERE is_active = TRUE ORDER BY prompt_id")
    if not rows:
        logger.warning("No prompts found — skipping clustering")
        return {"clusters": 0}

    prompt_ids = [r["prompt_id"] for r in rows]
    prompt_texts = [r["prompt_text"] for r in rows]

    # 2. Generate embeddings
    embeddings = generate_embeddings(prompt_texts, model=config.embedding_model)

    # 3. Cluster
    if config.algorithm == "hdbscan" and _HDBSCAN_AVAILABLE:
        labels, sil = cluster_hdbscan(embeddings, min_cluster_size=config.min_cluster_size)
        algo = "hdbscan"
    else:
        labels, sil = cluster_kmeans(embeddings, n_clusters=config.n_clusters)
        algo = "kmeans"

    # 4. Label clusters
    label_map = label_clusters(prompt_texts, labels)

    # 5. Persist
    for pid, label_id, emb in zip(prompt_ids, labels, embeddings):
        cluster_label = label_map.get(int(label_id), "unknown")
        execute(
            """
            INSERT INTO prompt_clusters
                (prompt_id, cluster_label, cluster_number, embedding, algorithm, silhouette_score)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (pid, cluster_label, int(label_id), emb.tolist(), algo, sil),
        )

    summary = {
        "algorithm": algo,
        "n_clusters": len(set(labels) - {-1}),
        "silhouette_score": round(sil, 4),
        "labels": label_map,
        "total_prompts": len(prompt_ids),
    }
    logger.info("Clustering complete: %s", summary)
    return summary
