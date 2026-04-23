"""
Incremental Load Utilities.

Provides D-1 incremental processing and date-range backfill
for safe, idempotent reprocessing of the pipeline.
"""

import logging
from datetime import datetime, timedelta
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from src.utils.database import DatabaseManager
from src.utils.logging_config import setup_logging
from src.pipeline.bronze import ingest_incremental
from src.pipeline.silver import run_silver_transforms
from src.pipeline.gold import run_gold_aggregations

logger = setup_logging("pipeline.incremental")


def run_incremental_for_date(db: DatabaseManager, target_date: str, batch_id: str = None):
    """
    Run end-to-end incremental pipeline for a single date.
    
    1. Bronze: Ingest only target_date data
    2. Silver: Re-transform (full rebuild for consistency)
    3. Gold: Re-aggregate (full rebuild for consistency)
    
    Args:
        db: DatabaseManager instance
        target_date: Date string YYYY-MM-DD
        batch_id: Optional batch identifier
    """
    logger.info(f"INCREMENTAL PIPELINE — target_date: {target_date}")

    # Step 1: Bronze incremental ingest
    ingest_incremental(db, target_date, batch_id)

    # Step 2 & 3: Silver + Gold (rebuild from full bronze)
    # This ensures consistency; in production, these could also be date-scoped
    run_silver_transforms(db, batch_id)
    run_gold_aggregations(db)

    logger.info(f"INCREMENTAL PIPELINE COMPLETE for {target_date}")


def run_d_minus_1(db: DatabaseManager):
    """Run incremental pipeline for yesterday's data (D-1 pattern)."""
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    logger.info(f"D-1 incremental run for: {yesterday}")
    run_incremental_for_date(db, yesterday)


def backfill(db: DatabaseManager, start_date: str, end_date: str):
    """
    Backfill pipeline for a date range.
    
    Processes each date sequentially for safe reprocessing.
    
    Args:
        db: DatabaseManager instance
        start_date: Start date (YYYY-MM-DD), inclusive
        end_date: End date (YYYY-MM-DD), inclusive
    """
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    total_days = (end - start).days + 1

    logger.info(f"BACKFILL: {start_date} → {end_date} ({total_days} days)")

    current = start
    processed = 0
    while current <= end:
        date_str = current.strftime("%Y-%m-%d")
        run_incremental_for_date(db, date_str)
        processed += 1
        if processed % 7 == 0:
            logger.info(f"  Backfill progress: {processed}/{total_days} days")
        current += timedelta(days=1)

    logger.info(f"BACKFILL COMPLETE — {processed} days processed")


if __name__ == "__main__":
    db = DatabaseManager()
    run_d_minus_1(db)
