"""
CLI entry point for the AI Brand Visibility Intelligence Engine.

Usage:
    python main.py seed          — Initialise DB and load prompts
    python main.py run           — Execute full pipeline (query + classify + score)
    python main.py classify      — Classify any unclassified responses
    python main.py score         — Compute AISOV scores
    python main.py cluster       — Run prompt clustering
    python main.py report        — Generate strategy report
    python main.py report --llm  — Generate LLM-synthesised report
"""

from __future__ import annotations

import argparse
import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("brand_visibility")


def cmd_seed(args: argparse.Namespace) -> None:
    from data_pipeline.database import init_schema
    from data_pipeline.prompt_loader import seed_prompts

    init_schema()
    inserted = seed_prompts()
    logger.info("Seed complete — %d new prompts inserted", inserted)


def cmd_run(args: argparse.Namespace) -> None:
    from data_pipeline.orchestrator import run_pipeline

    llms = args.llms.split(",") if args.llms else None
    run_id = run_pipeline(llm_names=llms, prompt_limit=args.limit)
    logger.info("Pipeline run complete — run_id=%s", run_id)


def cmd_classify(args: argparse.Namespace) -> None:
    from config.settings import get_settings
    from data_pipeline.orchestrator import stage_classify

    classified = stage_classify(get_settings(), batch_size=args.batch)
    logger.info("Classified %d responses", classified)


def cmd_score(args: argparse.Namespace) -> None:
    from analysis.visibility_scorer import compute_all_scores

    scores = compute_all_scores()
    for s in scores:
        logger.info(
            "AISOV  brand=%s  llm=%s  intent=%s  score=%.4f  (n=%d)",
            s.brand_name, s.llm_name or "ALL", s.intent_category or "ALL",
            s.aisov, s.sample_size,
        )


def cmd_cluster(args: argparse.Namespace) -> None:
    from analysis.clustering import run_clustering

    summary = run_clustering()
    logger.info("Clustering result: %s", summary)


def cmd_report(args: argparse.Namespace) -> None:
    from report_generation.strategy_report import generate_report

    mode = "llm" if args.llm else "template"
    report = generate_report(brand=args.brand, mode=mode, persist=not args.no_save)
    print(report)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="AI Brand Visibility Intelligence Engine",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    # seed
    sub.add_parser("seed", help="Initialise database and load prompts")

    # run
    p_run = sub.add_parser("run", help="Execute full pipeline")
    p_run.add_argument("--llms", type=str, default=None, help="Comma-separated LLM names")
    p_run.add_argument("--limit", type=int, default=None, help="Max prompts to process")

    # classify
    p_cls = sub.add_parser("classify", help="Classify unclassified responses")
    p_cls.add_argument("--batch", type=int, default=100, help="Batch size")

    # score
    sub.add_parser("score", help="Compute AISOV scores")

    # cluster
    sub.add_parser("cluster", help="Run prompt clustering")

    # report
    p_rep = sub.add_parser("report", help="Generate strategy report")
    p_rep.add_argument("--brand", type=str, default="HubSpot")
    p_rep.add_argument("--llm", action="store_true", help="Use LLM-synthesised report")
    p_rep.add_argument("--no-save", action="store_true", help="Don't persist to DB")

    args = parser.parse_args()

    commands = {
        "seed": cmd_seed,
        "run": cmd_run,
        "classify": cmd_classify,
        "score": cmd_score,
        "cluster": cmd_cluster,
        "report": cmd_report,
    }

    commands[args.command](args)


if __name__ == "__main__":
    main()
