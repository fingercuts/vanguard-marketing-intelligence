"""
Bronze Layer — Raw data ingestion into DuckDB.

Loads CSV files from the data/raw directory into bronze tables
with added metadata columns (_loaded_at, _source_file, _batch_id).
Implements idempotent full-load pattern (DROP + CREATE).
"""

import logging
from datetime import datetime
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from config.settings import DATA_DIR, BATCH_ID_FORMAT
from src.utils.database import DatabaseManager
from src.utils.logging_config import setup_logging

logger = setup_logging("pipeline.bronze")


# Mapping: table_name → source Parquet filename
SOURCE_MAP = {
    "bronze_ads_performance": "ads_performance.parquet",
    "bronze_ga_events": "ga_events.parquet",
    "bronze_campaigns": "campaigns.parquet",
    "bronze_adgroups": "adgroups.parquet",
}


def ingest_to_bronze(db: DatabaseManager, batch_id: str = None):
    """
    Ingest all source CSV files into bronze tables.
    
    Uses idempotent DROP + CREATE pattern for full loads.
    Each row is tagged with load metadata.
    
    Args:
        db: DatabaseManager instance
        batch_id: Optional batch identifier (auto-generated if not provided)
    """
    if batch_id is None:
        batch_id = datetime.now().strftime(BATCH_ID_FORMAT)

    logger.info("=" * 60)
    logger.info(f"BRONZE INGESTION — batch_id: {batch_id}")
    logger.info("=" * 60)

    for table_name, csv_file in SOURCE_MAP.items():
        csv_path = DATA_DIR / csv_file

        if not csv_path.exists():
            logger.warning(f"  Source file not found: {csv_path} — skipping {table_name}")
            continue

        logger.info(f"  Loading {csv_file} → {table_name}...")

        with db.connection() as conn:
            # Drop existing table for idempotent full load
            conn.execute(f"DROP TABLE IF EXISTS {table_name}")

            # Create table from Parquet with metadata columns
            conn.execute(f"""
                CREATE TABLE {table_name} AS
                SELECT
                    *,
                    CURRENT_TIMESTAMP AS _loaded_at,
                    '{csv_file}' AS _source_file,
                    '{batch_id}' AS _batch_id
                FROM read_parquet('{csv_path.as_posix().replace("'", "''")}')
            """)

            # Log row count
            count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            logger.info(f"    Success: {table_name} - {count:,} records ingested")

    logger.info("BRONZE INGESTION COMPLETE")
    logger.info("=" * 60)


def ingest_incremental(db: DatabaseManager, target_date: str, batch_id: str = None):
    """
    Incrementally ingest only data for a specific date.
    
    Appends new data and avoids duplicates using DELETE + INSERT.
    Only applies to date-partitioned fact tables.
    
    Args:
        db: DatabaseManager instance
        target_date: Date string (YYYY-MM-DD) to ingest
        batch_id: Optional batch identifier
    """
    if batch_id is None:
        batch_id = datetime.now().strftime(BATCH_ID_FORMAT)

    logger.info(f"BRONZE INCREMENTAL INGEST — date: {target_date}, batch: {batch_id}")

    date_tables = {
        "bronze_ads_performance": "ads_performance.parquet",
        "bronze_ga_events": "ga_events.parquet",
    }

    for table_name, csv_file in date_tables.items():
        csv_path = DATA_DIR / csv_file

        if not csv_path.exists():
            logger.warning(f"  Source file not found: {csv_path}")
            continue

        with db.connection() as conn:
            # Ensure table exists (might be first run)
            if not db.table_exists(table_name):
                logger.info(f"  Table {table_name} does not exist — initiating full ingest")
                ingest_to_bronze(db, batch_id)
                return

            # Delete existing data for target date (idempotent)
            conn.execute(f"DELETE FROM {table_name} WHERE CAST(date AS DATE) = '{target_date}'")

            # Insert only target date's data
            conn.execute(f"""
                INSERT INTO {table_name}
                SELECT
                    *,
                    CURRENT_TIMESTAMP AS _loaded_at,
                    '{csv_file}' AS _source_file,
                    '{batch_id}' AS _batch_id
                FROM read_parquet('{csv_path.as_posix().replace("'", "''")}')
                WHERE CAST(date AS DATE) = '{target_date}'
            """)

            count = conn.execute(
                f"SELECT COUNT(*) FROM {table_name} WHERE CAST(date AS DATE) = '{target_date}'"
            ).fetchone()[0]
            logger.info(f"    Success: {table_name} - {count:,} records for {target_date}")

    logger.info("BRONZE INCREMENTAL INGEST COMPLETE")


if __name__ == "__main__":
    db = DatabaseManager()
    ingest_to_bronze(db)
