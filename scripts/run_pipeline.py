"""
Vanguard Marketing Intelligence — Pipeline Execution Engine.

Usage:
    python scripts/run_pipeline.py                          # Full tactical run
    python scripts/run_pipeline.py --mode incremental       # D-1 delta ingestion
    python scripts/run_pipeline.py --mode full --skip-generate   # Direct processing
    python scripts/run_pipeline.py --mode backfill --start YYYY-MM-DD --end YYYY-MM-DD
"""

import argparse
import sys
from datetime import datetime
from pathlib import Path

# Ensure project root is on path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.settings import BATCH_ID_FORMAT
from src.utils.database import DatabaseManager
from src.utils.logging_config import setup_logging
from src.generator.data_generator import generate_all_data
from src.pipeline.bronze import ingest_to_bronze
from src.pipeline.silver import run_silver_transforms
from src.pipeline.gold import run_gold_aggregations
from src.pipeline.incremental import run_d_minus_1, backfill
from src.quality.checks import run_all_checks

logger = setup_logging("pipeline_runner")


def run_full_pipeline(skip_generate: bool = False):
    """Execute the end-to-end analytical pipeline (Bronze -> Silver -> Gold)."""
    batch_id = datetime.now().strftime(BATCH_ID_FORMAT)
    db = DatabaseManager()
    start_time = datetime.now()

    logger.info("=" * 70)
    logger.info("   VANGUARD MARKETING INTELLIGENCE — FULL EXECUTION")
    logger.info(f"   Batch ID: {batch_id}")
    logger.info("=" * 70)

    # Step 1: Telemetry Generation
    if not skip_generate:
        logger.info("\nPHASE 1/5: Synthetic Telemetry Generation")
        generate_all_data()
    else:
        logger.info("\nPHASE 1/5: Telemetry Generation [SKIPPED]")

    # Step 2: Bronze Ingestion
    logger.info("\nPHASE 2/5: Bronze Layer — Source Ingestion")
    ingest_to_bronze(db, batch_id)

    # Step 3: Silver Transformation
    logger.info("\nPHASE 3/5: Silver Layer — Cleansing & Domain Logic")
    run_silver_transforms(db, batch_id)

    # Step 4: Gold Aggregation
    logger.info("\nPHASE 4/5: Gold Layer — Strategic Aggregation")
    run_gold_aggregations(db)

    # Step 5: Governance Auditing
    logger.info("\nPHASE 5/5: Data Governance & Integrity Audit")
    checker = run_all_checks(db)
    summary = checker.get_summary()

    # Execution Summary
    elapsed = (datetime.now() - start_time).total_seconds()
    logger.info("\n" + "=" * 70)
    logger.info("   EXECUTION COMPLETE")
    logger.info(f"   Duration: {elapsed:.1f}s")
    logger.info(f"   Governance Index: {summary['pass_rate']}% compliance "
                f"({summary['passed']}/{summary['total']} protocols met)")

    # Data Volume Audit
    tables = db.get_tables()
    logger.info("\n   Warehouse Inventory:")
    for t in sorted(tables):
        count = db.row_count(t)
        logger.info(f"     {t:<35} {count:>12,} records")

    logger.info("=" * 70)
    logger.info("\nAnalytical Control Plane: streamlit run dashboard/0_Executive_Overview.py")

    return summary


def main():
    parser = argparse.ArgumentParser(
        description="Vanguard Marketing Intelligence - Execution Plane",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python scripts/run_pipeline.py                          Full execution
  python scripts/run_pipeline.py --mode incremental        D-1 delta load
  python scripts/run_pipeline.py --mode full --skip-generate  Process existing raw telemetry
  python scripts/run_pipeline.py --mode backfill --start 2024-01-01 --end 2024-01-31
        """,
    )
    parser.add_argument(
        "--mode",
        choices=["full", "incremental", "backfill"],
        default="full",
        help="Execution mode (default: full)",
    )
    parser.add_argument(
        "--skip-generate",
        action="store_true",
        help="Skip telemetry generation step",
    )
    parser.add_argument(
        "--start",
        type=str,
        help="Backfill start date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--end",
        type=str,
        help="Backfill end date (YYYY-MM-DD)",
    )

    args = parser.parse_args()

    if args.mode == "full":
        run_full_pipeline(skip_generate=args.skip_generate)

    elif args.mode == "incremental":
        db = DatabaseManager()
        run_d_minus_1(db)
        run_all_checks(db)

    elif args.mode == "backfill":
        if not args.start or not args.end:
            parser.error("Backfill mode requires --start and --end temporal boundaries")
        db = DatabaseManager()
        backfill(db, args.start, args.end)
        run_all_checks(db)


if __name__ == "__main__":
    main()
