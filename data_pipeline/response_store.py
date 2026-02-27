"""
Response store — persists raw LLM responses and classified brand-visibility
metrics into the warehouse.
"""

from __future__ import annotations

import json
import logging
import uuid
from typing import Any, Dict, List, Optional

from data_pipeline.database import execute, fetch_all, fetch_one
from data_pipeline.llm_client import LLMResponse

logger = logging.getLogger(__name__)


def save_response(
    prompt_id: int,
    llm_response: LLMResponse,
    run_id: Optional[uuid.UUID] = None,
) -> str:
    """
    Persist a single LLM response row.

    Returns the generated ``response_id`` (UUID).
    """
    response_id = str(uuid.uuid4())

    execute(
        """
        INSERT INTO llm_responses
            (response_id, prompt_id, llm_name, model_version,
             response_text, token_count, latency_ms, run_id)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            response_id,
            prompt_id,
            llm_response.llm_name,
            llm_response.model_version,
            llm_response.response_text,
            llm_response.token_count,
            llm_response.latency_ms,
            str(run_id) if run_id else None,
        ),
    )
    logger.debug("Saved response %s for prompt %d / %s", response_id, prompt_id, llm_response.llm_name)
    return response_id


def save_metrics(response_id: str, metrics: Dict[str, Any]) -> int:
    """
    Persist brand-visibility metrics extracted by the classifier.

    ``metrics`` must contain keys matching the ``brand_visibility_metrics``
    table columns.  Returns the generated ``metric_id``.
    """
    row = fetch_one(
        """
        INSERT INTO brand_visibility_metrics
            (response_id, brand_name, brand_mentioned, rank_position,
             sentiment, context_type, recommendation_strength,
             competitor_mentioned, competitors_list,
             classification_model, classification_confidence,
             raw_classification)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING metric_id
        """,
        (
            response_id,
            metrics.get("brand_name", "HubSpot"),
            metrics["brand_mentioned"],
            metrics.get("rank_position"),
            metrics["sentiment"],
            metrics["context_type"],
            metrics["recommendation_strength"],
            metrics.get("competitor_mentioned", False),
            metrics.get("competitors_list"),
            metrics.get("classification_model"),
            metrics.get("classification_confidence"),
            json.dumps(metrics.get("raw_classification")) if metrics.get("raw_classification") else None,
        ),
    )
    metric_id = row["metric_id"]
    logger.debug("Saved metrics %d for response %s", metric_id, response_id)
    return metric_id


def get_unclassified_responses(limit: int = 100) -> List[Dict]:
    """Return LLM responses that have no corresponding metrics row yet."""
    return fetch_all(
        """
        SELECT r.response_id, r.prompt_id, r.llm_name, r.response_text,
               p.prompt_text, p.intent_category
        FROM llm_responses r
        JOIN prompts p ON p.prompt_id = r.prompt_id
        LEFT JOIN brand_visibility_metrics m ON m.response_id = r.response_id
        WHERE m.metric_id IS NULL
        ORDER BY r.timestamp
        LIMIT %s
        """,
        (limit,),
    )


def get_responses_by_run(run_id: uuid.UUID) -> List[Dict]:
    """Return all responses for a given pipeline run."""
    return fetch_all(
        "SELECT * FROM llm_responses WHERE run_id = %s ORDER BY timestamp",
        (str(run_id),),
    )
