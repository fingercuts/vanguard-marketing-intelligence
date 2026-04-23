"""
Gold Layer — Business-level aggregations for dashboard consumption.

Creates pre-aggregated tables:
- gold_campaign_daily: Campaign-level daily KPIs
- gold_channel_daily: Channel comparison metrics
- gold_hourly_trends: Time-of-day analysis
- gold_funnel_summary: Impression → Click → Conversion funnel
"""

import logging
from datetime import datetime
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from src.utils.database import DatabaseManager
from src.utils.logging_config import setup_logging

logger = setup_logging("pipeline.gold")


def build_campaign_daily(db: DatabaseManager):
    """Aggregate silver_ads_performance → gold_campaign_daily."""
    logger.info("Building gold_campaign_daily...")

    sql = """
        DROP TABLE IF EXISTS gold_campaign_daily;

        CREATE TABLE gold_campaign_daily AS
        SELECT
            date,
            campaign_id,
            campaign_name,
            channel,
            objective,
            SUM(impressions)        AS total_impressions,
            SUM(clicks)             AS total_clicks,
            ROUND(SUM(spend), 2)    AS total_spend,
            SUM(conversions)        AS total_conversions,
            ROUND(SUM(revenue), 2)  AS total_revenue,
            CASE WHEN SUM(impressions) > 0
                 THEN ROUND(SUM(clicks) * 1.0 / SUM(impressions), 6)
                 ELSE 0.0 END       AS avg_ctr,
            CASE WHEN SUM(clicks) > 0
                 THEN ROUND(SUM(spend) / SUM(clicks), 4)
                 ELSE 0.0 END       AS avg_cpc,
            CASE WHEN SUM(conversions) > 0
                 THEN ROUND(SUM(spend) / SUM(conversions), 4)
                 ELSE 0.0 END       AS avg_cpa,
            CASE WHEN SUM(spend) > 0
                 THEN ROUND(SUM(revenue) / SUM(spend), 4)
                 ELSE 0.0 END       AS avg_roas,
            CASE WHEN SUM(clicks) > 0
                 THEN ROUND(SUM(conversions) * 1.0 / SUM(clicks), 6)
                 ELSE 0.0 END       AS avg_conversion_rate,
            CURRENT_TIMESTAMP       AS _loaded_at
        FROM silver_ads_performance
        GROUP BY date, campaign_id, campaign_name, channel, objective
        ORDER BY date, campaign_id, channel
    """

    with db.connection() as conn:
        for stmt in sql.split(";"):
            stmt = stmt.strip()
            if stmt:
                conn.execute(stmt)

        count = conn.execute("SELECT COUNT(*) FROM gold_campaign_daily").fetchone()[0]
        logger.info(f"  Success: gold_campaign_daily - {count:,} records aggregated")


def build_channel_daily(db: DatabaseManager):
    """Aggregate silver_ads_performance → gold_channel_daily."""
    logger.info("Building gold_channel_daily...")

    sql = """
        DROP TABLE IF EXISTS gold_channel_daily;

        CREATE TABLE gold_channel_daily AS
        SELECT
            date,
            channel,
            SUM(impressions)            AS total_impressions,
            SUM(clicks)                 AS total_clicks,
            ROUND(SUM(spend), 2)        AS total_spend,
            SUM(conversions)            AS total_conversions,
            ROUND(SUM(revenue), 2)      AS total_revenue,
            CASE WHEN SUM(impressions) > 0
                 THEN ROUND(SUM(clicks) * 1.0 / SUM(impressions), 6)
                 ELSE 0.0 END           AS avg_ctr,
            CASE WHEN SUM(clicks) > 0
                 THEN ROUND(SUM(spend) / SUM(clicks), 4)
                 ELSE 0.0 END           AS avg_cpc,
            CASE WHEN SUM(spend) > 0
                 THEN ROUND(SUM(revenue) / SUM(spend), 4)
                 ELSE 0.0 END           AS avg_roas,
            COUNT(DISTINCT campaign_id) AS campaign_count,
            CURRENT_TIMESTAMP           AS _loaded_at
        FROM silver_ads_performance
        GROUP BY date, channel
        ORDER BY date, channel
    """

    with db.connection() as conn:
        for stmt in sql.split(";"):
            stmt = stmt.strip()
            if stmt:
                conn.execute(stmt)

        count = conn.execute("SELECT COUNT(*) FROM gold_channel_daily").fetchone()[0]
        logger.info(f"  Success: gold_channel_daily - {count:,} records aggregated")


def build_hourly_trends(db: DatabaseManager):
    """Aggregate silver_ads_performance → gold_hourly_trends."""
    logger.info("Building gold_hourly_trends...")

    sql = """
        DROP TABLE IF EXISTS gold_hourly_trends;

        CREATE TABLE gold_hourly_trends AS
        SELECT
            date,
            hour,
            channel,
            SUM(impressions)        AS total_impressions,
            SUM(clicks)             AS total_clicks,
            ROUND(SUM(spend), 2)    AS total_spend,
            SUM(conversions)        AS total_conversions,
            ROUND(SUM(revenue), 2)  AS total_revenue,
            CASE WHEN SUM(impressions) > 0
                 THEN ROUND(SUM(clicks) * 1.0 / SUM(impressions), 6)
                 ELSE 0.0 END       AS avg_ctr,
            CURRENT_TIMESTAMP       AS _loaded_at
        FROM silver_ads_performance
        GROUP BY date, hour, channel
        ORDER BY date, hour, channel
    """

    with db.connection() as conn:
        for stmt in sql.split(";"):
            stmt = stmt.strip()
            if stmt:
                conn.execute(stmt)

        count = conn.execute("SELECT COUNT(*) FROM gold_hourly_trends").fetchone()[0]
        logger.info(f"  Success: gold_hourly_trends - {count:,} records aggregated")


def build_funnel_summary(db: DatabaseManager):
    """Aggregate silver_ads_performance → gold_funnel_summary."""
    logger.info("Building gold_funnel_summary...")

    sql = """
        DROP TABLE IF EXISTS gold_funnel_summary;

        CREATE TABLE gold_funnel_summary AS
        SELECT
            date,
            campaign_id,
            campaign_name,
            SUM(impressions)        AS total_impressions,
            SUM(clicks)             AS total_clicks,
            SUM(conversions)        AS total_conversions,
            CASE WHEN SUM(impressions) > 0
                 THEN ROUND(SUM(clicks) * 1.0 / SUM(impressions), 6)
                 ELSE 0.0 END       AS impression_to_click_rate,
            CASE WHEN SUM(clicks) > 0
                 THEN ROUND(SUM(conversions) * 1.0 / SUM(clicks), 6)
                 ELSE 0.0 END       AS click_to_conversion_rate,
            CASE WHEN SUM(impressions) > 0
                 THEN ROUND(SUM(conversions) * 1.0 / SUM(impressions), 8)
                 ELSE 0.0 END       AS overall_conversion_rate,
            CURRENT_TIMESTAMP       AS _loaded_at
        FROM silver_ads_performance
        GROUP BY date, campaign_id, campaign_name
        ORDER BY date, campaign_id
    """

    with db.connection() as conn:
        for stmt in sql.split(";"):
            stmt = stmt.strip()
            if stmt:
                conn.execute(stmt)

        count = conn.execute("SELECT COUNT(*) FROM gold_funnel_summary").fetchone()[0]
        logger.info(f"  Success: gold_funnel_summary - {count:,} records aggregated")


def run_gold_aggregations(db: DatabaseManager):
    """Run all Gold layer aggregations."""
    logger.info("=" * 60)
    logger.info("GOLD LAYER AGGREGATION")
    logger.info("=" * 60)

    build_campaign_daily(db)
    build_channel_daily(db)
    build_hourly_trends(db)
    build_funnel_summary(db)

    logger.info("GOLD AGGREGATION COMPLETE")
    logger.info("=" * 60)


if __name__ == "__main__":
    db = DatabaseManager()
    run_gold_aggregations(db)
