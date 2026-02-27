#!/usr/bin/env python3
"""
One-command script to generate data, run ETL, and launch dashboard.

Usage:
    python run_pipeline.py              # Full pipeline + launch dashboard
    python run_pipeline.py --generate   # Only generate data
    python run_pipeline.py --etl        # Only run ETL
    python run_pipeline.py --dashboard  # Only launch dashboard
"""
import sys
import argparse


def main():
    parser = argparse.ArgumentParser(description="HubSpot Big Data Pipeline Runner")
    parser.add_argument("--generate", action="store_true", help="Generate synthetic data")
    parser.add_argument("--etl", action="store_true", help="Run ETL pipeline")
    parser.add_argument("--dashboard", action="store_true", help="Launch dashboard")
    args = parser.parse_args()

    run_all = not (args.generate or args.etl or args.dashboard)

    if run_all or args.generate:
        from data.generate_data import main as gen_main
        gen_main()

    if run_all or args.etl:
        from src.data_pipeline import run_etl
        run_etl()

    if run_all or args.dashboard:
        from dashboard.app import app
        from src.config import DASHBOARD_HOST, DASHBOARD_PORT, DEBUG
        print(f"\n🌐 Dashboard: http://localhost:{DASHBOARD_PORT}")
        app.run(host=DASHBOARD_HOST, port=DASHBOARD_PORT, debug=DEBUG)


if __name__ == "__main__":
    main()
