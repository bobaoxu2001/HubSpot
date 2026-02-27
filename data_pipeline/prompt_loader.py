"""
Prompt loader — reads the canonical prompt catalogue from JSON and seeds the
database ``prompts`` table (idempotent).
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Dict, List

from data_pipeline.database import execute, fetch_all

logger = logging.getLogger(__name__)

DEFAULT_PROMPTS_PATH = Path(__file__).resolve().parent.parent / "data" / "prompts.json"


def load_prompts_from_file(path: Path = DEFAULT_PROMPTS_PATH) -> List[Dict]:
    """Read the JSON prompt catalogue from disk."""
    with open(path) as fh:
        prompts = json.load(fh)
    logger.info("Loaded %d prompts from %s", len(prompts), path)
    return prompts


def seed_prompts(prompts: List[Dict] | None = None) -> int:
    """
    Insert prompts into the database.  Skips duplicates based on prompt_text.

    Returns the number of newly inserted rows.
    """
    if prompts is None:
        prompts = load_prompts_from_file()

    existing = {
        row["prompt_text"]
        for row in fetch_all("SELECT prompt_text FROM prompts")
    }

    inserted = 0
    for p in prompts:
        if p["prompt_text"] in existing:
            continue
        execute(
            """
            INSERT INTO prompts (prompt_text, intent_category)
            VALUES (%s, %s)
            """,
            (p["prompt_text"], p["intent_category"]),
        )
        inserted += 1

    logger.info("Seeded %d new prompts (%d already existed)", inserted, len(existing))
    return inserted


def get_active_prompts() -> List[Dict]:
    """Return all active prompts from the database."""
    return fetch_all(
        "SELECT prompt_id, prompt_text, intent_category FROM prompts WHERE is_active = TRUE ORDER BY prompt_id"
    )
