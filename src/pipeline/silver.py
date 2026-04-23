"""
Silver Layer - Data cleaning, enrichment, and derived metric calculation.

Transforms raw bronze data into standardized analytics-ready tables:
- Normalization and deduplication
- Attribution KPI calculation (CTR, CPC, CPA, ROAS)
- Dimension enrichment via campaign/adgroup joins
- Behavioral event categorization for digital tracking data
"""

import logging
from datetime import datetime
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from config.settings import BATCH_ID_FORMAT
from src.utils.database import DatabaseManager
from src.utils.logging_config import setup_logging

logger = setup_logging("pipeline.silver")


def transform_ads_performance(db: DatabaseManager, batch_id: str = None):
    """
    Transform raw ad performance data into enriched silver records.
    
    - Computes core marketing efficiency metrics
    - Standardizes reporting grain (Date, Hour, Campaign, Channel)
    - Deduplicates records favoring the latest ingestion timestamp
    - Joins with operational dimension tables for human-readable naming
    """
    if batch_id is None:
        batch_id = datetime.now().strftime(BATCH_ID_FORMAT)

    logger.info("Transforming silver_ads_performance...")

    sql = f"""
        DROP TABLE IF EXISTS silver_ads_performance;

        CREATE TABLE silver_ads_performance AS
        WITH stg_ads AS (
            /* 
               Deduplication logic: Favoring the most recently loaded record 
               per unique combination of hour/campaign/channel.
            */
            SELECT DISTINCT ON (date, hour, campaign_id, adgroup_id, channel)
                date,
                hour,
                campaign_id,
                adgroup_id,
                channel,
                COALESCE(impressions, 0) AS impressions,
                COALESCE(clicks, 0) AS clicks,
                COALESCE(spend, 0.0) AS spend,
                COALESCE(conversions, 0) AS conversions,
                COALESCE(revenue, 0.0) AS revenue
            FROM bronze_ads_performance
            ORDER BY date, hour, campaign_id, adgroup_id, channel, _loaded_at DESC
        )
        SELECT
            a.date,
            a.hour,
            a.campaign_id,
            a.adgroup_id,
            a.channel,
            a.impressions,
            a.clicks,
            a.spend,
            a.conversions,
            a.revenue,
            
            /* 
               Marketing KPI Derivation with defensive division.
               Rounding to 6 decimal places for CTR/ConvRate to ensure 
               precision during downstream aggregation.
            */
            CASE WHEN a.impressions > 0
                 THEN ROUND(a.clicks * 1.0 / a.impressions, 6)
                 ELSE 0.0 END AS ctr,
            CASE WHEN a.clicks > 0
                 THEN ROUND(a.spend / a.clicks, 4)
                 ELSE 0.0 END AS cpc,
            CASE WHEN a.conversions > 0
                 THEN ROUND(a.spend / a.conversions, 4)
                 ELSE 0.0 END AS cpa,
            CASE WHEN a.spend > 0
                 THEN ROUND(a.revenue / a.spend, 4)
                 ELSE 0.0 END AS roas,
            CASE WHEN a.clicks > 0
                 THEN ROUND(a.conversions * 1.0 / a.clicks, 6)
                 ELSE 0.0 END AS conversion_rate,
            
            -- Dimension table enrichment
            c.campaign_name,
            c.objective,
            c.budget_daily,
            ag.adgroup_name,
            ag.targeting_type,
            ag.bid_strategy,
            
            -- Audit Metadata
            CURRENT_TIMESTAMP AS _loaded_at,
            '{batch_id}' AS _batch_id
        FROM stg_ads a
        LEFT JOIN bronze_campaigns c ON a.campaign_id = c.campaign_id
        LEFT JOIN bronze_adgroups ag ON a.adgroup_id = ag.adgroup_id
    """

    with db.connection() as conn:
        for stmt in sql.split(";"):
            stmt = stmt.strip()
            if stmt:
                conn.execute(stmt)

        count = conn.execute("SELECT COUNT(*) FROM silver_ads_performance").fetchone()[0]
        logger.info(f"  [SUCCESS] silver_ads_performance: {count:,} rows")


def transform_ga_events(db: DatabaseManager, batch_id: str = None):
    """
    Normalize event-level behavioral data for conversion funnel analysis.
    
    - Categorizes raw event names into business-aligned buckets
    - Identifies 'Bounce' sessions to monitor landing page friction
    - Standardizes attribution sources (Mapping Nulls to 'direct')
    """
    if batch_id is None:
        batch_id = datetime.now().strftime(BATCH_ID_FORMAT)

    logger.info("Transforming silver_ga_events...")

    sql = f"""
        DROP TABLE IF EXISTS silver_ga_events;

        CREATE TABLE silver_ga_events AS
        WITH stg_session_metrics AS (
            /* Calculating session depth to identify bounce behavior */
            SELECT session_id, COUNT(*) AS event_count
            FROM bronze_ga_events
            GROUP BY session_id
        )
        SELECT
            e.date,
            e.hour,
            e.event_name,
            CASE
                WHEN e.event_name IN ('purchase', 'add_to_cart', 'begin_checkout', 'sign_up')
                    THEN 'conversion'
                WHEN e.event_name IN ('page_view', 'scroll', 'click', 'video_start', 'video_complete', 'file_download')
                    THEN 'engagement'
                WHEN e.event_name IN ('session_start', 'login')
                    THEN 'navigation'
                ELSE 'other'
            END AS event_category,
            e.session_id,
            e.user_id,
            e.page,
            COALESCE(e.source, 'direct') AS source,
            COALESCE(e.medium, '(none)') AS medium,
            e.campaign_id,
            /* 
               A session is considered a bounce if it only contains 
               a single event (usually session_start or initial page_view) 
            */
            CASE WHEN sc.event_count = 1 THEN TRUE ELSE FALSE END AS is_bounce,
            CURRENT_TIMESTAMP AS _loaded_at,
            '{batch_id}' AS _batch_id
        FROM bronze_ga_events e
        LEFT JOIN stg_session_metrics sc ON e.session_id = sc.session_id
    """

    with db.connection() as conn:
        for stmt in sql.split(";"):
            stmt = stmt.strip()
            if stmt:
                conn.execute(stmt)

        count = conn.execute("SELECT COUNT(*) FROM silver_ga_events").fetchone()[0]
        logger.info(f"  [SUCCESS] silver_ga_events: {count:,} rows")


def run_silver_transforms(db: DatabaseManager, batch_id: str = None):
    """Execution orchestration for the Silver Layer."""
    logger.info("-" * 60)
    logger.info("CORE SILVER LAYER PROCESSING")
    logger.info("-" * 60)

    transform_ads_performance(db, batch_id)
    transform_ga_events(db, batch_id)

    logger.info("SILVER LAYER COMPLETE")
    logger.info("-" * 60)


if __name__ == "__main__":
    db = DatabaseManager()
    run_silver_transforms(db)
