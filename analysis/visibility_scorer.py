"""
AI Share of Voice (AISOV) — visibility scoring engine.

Computes a composite score that measures how prominently a brand surfaces
across LLM responses.  The formula is a weighted sum of four normalised
components:

    AISOV = (mention_rate       × W₁)
          + (avg_rank_score     × W₂)
          + (positive_sentiment × W₃)
          + (recommendation_avg × W₄)

Component definitions
---------------------
mention_rate (W₁ = 0.30)
    Fraction of responses that mention the brand.  This is the most
    fundamental signal — if the brand is not mentioned, nothing else matters.
    Higher weight reflects its gating role.

avg_rank_score (W₂ = 0.25)
    Normalised rank when the brand appears in a ranked list.  Computed as
    ``1 / rank_position`` so that rank-1 → 1.0 and higher ranks decay
    hyperbolically.  Averaged over responses where rank is available.
    This captures *positional prominence* — being listed first signals
    stronger endorsement.

positive_sentiment_ratio (W₃ = 0.25)
    Fraction of brand-mentioning responses classified as "positive".
    Captures qualitative tonality — a brand can be mentioned but criticised,
    which should depress its score.

recommendation_strength_avg (W₄ = 0.20)
    Mean ``recommendation_strength`` (0–1) across all responses.  This
    reflects how strongly the LLM explicitly recommends the brand, beyond
    mere mention or positive tone.  Slightly lower weight because it
    correlates with sentiment.

All components are floats in [0, 1], so AISOV ∈ [0, 1].
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date
from typing import Dict, List, Optional

from config.settings import ScoringConfig, get_settings
from data_pipeline.database import execute, fetch_all

logger = logging.getLogger(__name__)


@dataclass
class AisovScore:
    """Container for a fully computed AISOV score with its components."""
    brand_name: str
    llm_name: Optional[str]
    intent_category: Optional[str]
    mention_rate: float
    avg_rank_score: float
    positive_sentiment_ratio: float
    recommendation_strength_avg: float
    aisov: float
    sample_size: int

    def as_dict(self) -> Dict:
        return {
            "brand_name": self.brand_name,
            "llm_name": self.llm_name,
            "intent_category": self.intent_category,
            "mention_rate": self.mention_rate,
            "avg_rank_score": self.avg_rank_score,
            "positive_sentiment_ratio": self.positive_sentiment_ratio,
            "recommendation_strength_avg": self.recommendation_strength_avg,
            "aisov_score": self.aisov,
            "sample_size": self.sample_size,
        }


def _safe_div(numerator: float, denominator: float, default: float = 0.0) -> float:
    return numerator / denominator if denominator else default


def compute_aisov(
    metrics: List[Dict],
    weights: ScoringConfig | None = None,
    brand: str = "HubSpot",
    llm_name: str | None = None,
    intent_category: str | None = None,
) -> AisovScore:
    """
    Compute the AISOV score from a list of brand_visibility_metrics rows.

    Each row is expected to be a dict with at least:
        brand_mentioned, rank_position, sentiment, recommendation_strength

    Args:
        metrics: List of metric dicts (from DB or classification output).
        weights: Scoring weight configuration.
        brand: Brand label for the score record.
        llm_name: Optional LLM filter label.
        intent_category: Optional intent filter label.

    Returns:
        An ``AisovScore`` dataclass.
    """
    if weights is None:
        weights = get_settings().scoring

    total = len(metrics)
    if total == 0:
        return AisovScore(
            brand_name=brand,
            llm_name=llm_name,
            intent_category=intent_category,
            mention_rate=0.0,
            avg_rank_score=0.0,
            positive_sentiment_ratio=0.0,
            recommendation_strength_avg=0.0,
            aisov=0.0,
            sample_size=0,
        )

    # --- Component 1: mention_rate ---
    mentions = sum(1 for m in metrics if m["brand_mentioned"])
    mention_rate = _safe_div(mentions, total)

    # --- Component 2: avg_rank_score (inverse rank) ---
    ranked = [m for m in metrics if m.get("rank_position") and m["rank_position"] > 0]
    avg_rank_score = (
        sum(1.0 / m["rank_position"] for m in ranked) / len(ranked)
        if ranked else 0.0
    )

    # --- Component 3: positive_sentiment_ratio ---
    mentioned = [m for m in metrics if m["brand_mentioned"]]
    positive = sum(1 for m in mentioned if m.get("sentiment") == "positive")
    positive_sentiment_ratio = _safe_div(positive, len(mentioned))

    # --- Component 4: recommendation_strength_avg ---
    rec_values = [m["recommendation_strength"] for m in metrics if m.get("recommendation_strength") is not None]
    recommendation_strength_avg = _safe_div(sum(rec_values), len(rec_values))

    # --- Composite AISOV ---
    aisov = (
        mention_rate * weights.weight_mention_rate
        + avg_rank_score * weights.weight_rank_score
        + positive_sentiment_ratio * weights.weight_sentiment
        + recommendation_strength_avg * weights.weight_recommendation
    )

    return AisovScore(
        brand_name=brand,
        llm_name=llm_name,
        intent_category=intent_category,
        mention_rate=round(mention_rate, 4),
        avg_rank_score=round(avg_rank_score, 4),
        positive_sentiment_ratio=round(positive_sentiment_ratio, 4),
        recommendation_strength_avg=round(recommendation_strength_avg, 4),
        aisov=round(aisov, 4),
        sample_size=total,
    )


def compute_all_scores(
    period_start: date | None = None,
    period_end: date | None = None,
) -> List[AisovScore]:
    """
    Compute AISOV scores across all dimensions (overall, by LLM, by intent)
    and persist them to the ``visibility_scores`` table.
    """
    settings = get_settings()
    today = date.today()
    period_start = period_start or today
    period_end = period_end or today

    # Fetch all metrics
    all_metrics = fetch_all(
        """
        SELECT m.*, r.llm_name, p.intent_category
        FROM brand_visibility_metrics m
        JOIN llm_responses r ON r.response_id = m.response_id
        JOIN prompts p ON p.prompt_id = r.prompt_id
        WHERE m.created_at::date BETWEEN %s AND %s
        """,
        (period_start, period_end),
    )

    if not all_metrics:
        logger.warning("No metrics found for period %s – %s", period_start, period_end)
        return []

    scores: List[AisovScore] = []

    # Overall score
    scores.append(compute_aisov(all_metrics, brand=settings.brand.primary_brand))

    # Per-LLM scores
    llm_groups: Dict[str, list] = {}
    for m in all_metrics:
        llm_groups.setdefault(m["llm_name"], []).append(m)
    for llm_name, group in llm_groups.items():
        scores.append(compute_aisov(group, brand=settings.brand.primary_brand, llm_name=llm_name))

    # Per-intent scores
    intent_groups: Dict[str, list] = {}
    for m in all_metrics:
        intent_groups.setdefault(m["intent_category"], []).append(m)
    for intent, group in intent_groups.items():
        scores.append(compute_aisov(group, brand=settings.brand.primary_brand, intent_category=intent))

    # Persist
    for s in scores:
        execute(
            """
            INSERT INTO visibility_scores
                (brand_name, llm_name, intent_category,
                 period_start, period_end,
                 mention_rate, avg_rank_score,
                 positive_sentiment_ratio, recommendation_strength_avg,
                 aisov_score, sample_size)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            (
                s.brand_name, s.llm_name, s.intent_category,
                period_start, period_end,
                s.mention_rate, s.avg_rank_score,
                s.positive_sentiment_ratio, s.recommendation_strength_avg,
                s.aisov, s.sample_size,
            ),
        )

    logger.info("Computed and stored %d AISOV scores", len(scores))
    return scores
