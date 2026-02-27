"""
Seed script — initialise the database schema and load the canonical prompt
catalogue.  Safe to run repeatedly (idempotent).

Usage:
    python -m warehouse.seed
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from data_pipeline.database import init_schema
from data_pipeline.prompt_loader import seed_prompts

logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(name)s | %(message)s")
logger = logging.getLogger(__name__)


def main() -> None:
    logger.info("Applying database schema…")
    init_schema()

    logger.info("Seeding prompts…")
    inserted = seed_prompts()
    logger.info("Done. %d new prompts inserted.", inserted)


if __name__ == "__main__":
    main()
