"""
Pipeline orchestrator — end-to-end execution of the Brand Visibility pipeline.

Stages:
  1. Load prompts (seed DB if needed)
  2. Query each LLM for every prompt
  3. Persist raw responses
  4. Classify each response for brand-visibility signals
  5. Persist structured metrics
  6. Compute aggregate visibility scores

The orchestrator is idempotent: re-running it will skip prompts that already
have responses for a given LLM within the same run.
"""

from __future__ import annotations

import logging
import uuid
from datetime import date, datetime
from typing import List, Optional

from config.settings import Settings, get_settings
from data_pipeline.classifier import classify_response
from data_pipeline.database import execute, fetch_one, init_schema
from data_pipeline.llm_client import query_llm
from data_pipeline.prompt_loader import get_active_prompts, seed_prompts
from data_pipeline.response_store import (
    get_unclassified_responses,
    save_metrics,
    save_response,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Pipeline run tracking
# ---------------------------------------------------------------------------

def _start_run(settings: Settings) -> uuid.UUID:
    """Create a pipeline_runs record and return the run_id."""
    run_id = uuid.uuid4()
    execute(
        """
        INSERT INTO pipeline_runs (run_id, status, config_snapshot)
        VALUES (%s, 'running', %s)
        """,
        (str(run_id), "{}"),
    )
    logger.info("Pipeline run started: %s", run_id)
    return run_id


def _finish_run(run_id: uuid.UUID, status: str, error: str | None = None) -> None:
    execute(
        """
        UPDATE pipeline_runs
        SET finished_at = NOW(), status = %s, error_message = %s
        WHERE run_id = %s
        """,
        (status, error, str(run_id)),
    )
    logger.info("Pipeline run %s finished with status=%s", run_id, status)


# ---------------------------------------------------------------------------
# Stage runners
# ---------------------------------------------------------------------------

def stage_query_llms(
    prompts: List[dict],
    llm_names: List[str],
    run_id: uuid.UUID,
    settings: Settings,
) -> int:
    """
    Query every (prompt, llm) pair and persist responses.

    Returns the total number of responses stored.
    """
    count = 0
    for prompt in prompts:
        for llm_name in llm_names:
            try:
                llm_resp = query_llm(
                    prompt["prompt_text"],
                    llm_name,
                    cfg=settings.llm,
                )
                save_response(prompt["prompt_id"], llm_resp, run_id=run_id)
                count += 1
                logger.info(
                    "Response %d stored — prompt=%d llm=%s",
                    count, prompt["prompt_id"], llm_name,
                )
            except Exception:
                logger.exception(
                    "Query failed — prompt=%d llm=%s", prompt["prompt_id"], llm_name,
                )
    return count


def stage_classify(settings: Settings, batch_size: int = 50) -> int:
    """
    Classify all unclassified responses and persist metrics.

    Returns the number of responses classified.
    """
    unclassified = get_unclassified_responses(limit=batch_size)
    classified = 0

    for item in unclassified:
        try:
            metrics = classify_response(
                prompt_text=item["prompt_text"],
                response_text=item["response_text"],
                brand=settings.brand.primary_brand,
                competitors=settings.brand.competitors,
            )
            save_metrics(item["response_id"], metrics)
            classified += 1
        except Exception:
            logger.exception("Classification failed for response %s", item["response_id"])

    logger.info("Classified %d / %d unclassified responses", classified, len(unclassified))
    return classified


# ---------------------------------------------------------------------------
# Full pipeline
# ---------------------------------------------------------------------------

def run_pipeline(
    llm_names: Optional[List[str]] = None,
    prompt_limit: Optional[int] = None,
) -> uuid.UUID:
    """
    Execute the full pipeline end-to-end.

    Args:
        llm_names: List of LLMs to query (defaults to all configured).
        prompt_limit: Max number of prompts to process (None = all).

    Returns:
        The ``run_id`` UUID for this execution.
    """
    settings = get_settings()
    llm_names = llm_names or list(settings.llm.models.keys())

    # --- Initialise ---
    run_id = _start_run(settings)

    try:
        # Stage 1: ensure prompts are loaded
        seed_prompts()
        prompts = get_active_prompts()
        if prompt_limit:
            prompts = prompts[:prompt_limit]

        logger.info("Processing %d prompts across %s", len(prompts), llm_names)

        # Stage 2 + 3: query LLMs and store responses
        resp_count = stage_query_llms(prompts, llm_names, run_id, settings)

        # Stage 4 + 5: classify and store metrics
        classified = stage_classify(settings, batch_size=resp_count + 10)

        # Update run metadata
        execute(
            "UPDATE pipeline_runs SET prompts_count = %s, responses_count = %s WHERE run_id = %s",
            (len(prompts), resp_count, str(run_id)),
        )

        _finish_run(run_id, "completed")

    except Exception as exc:
        _finish_run(run_id, "failed", error=str(exc))
        raise

    return run_id
