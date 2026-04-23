"""Tests for ETL pipeline (Bronze → Silver → Gold)."""

import pytest
import duckdb
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from src.utils.database import DatabaseManager
from src.generator.data_generator import generate_all_data
from src.pipeline.bronze import ingest_to_bronze
from src.pipeline.silver import run_silver_transforms
from src.pipeline.gold import run_gold_aggregations


@pytest.fixture(scope="module")
def pipeline_db(tmp_path_factory):
    """Set up a test database with full pipeline execution."""
    tmp_dir = tmp_path_factory.mktemp("test_pipeline")
    db_path = str(tmp_dir / "test_marketing.duckdb")
    db = DatabaseManager(db_path)

    # Generate minimal test data
    generate_all_data()

    # Run pipeline
    ingest_to_bronze(db, "test_batch")
    run_silver_transforms(db, "test_batch")
    run_gold_aggregations(db)

    return db


class TestBronzeLayer:
    """Tests for Bronze layer ingestion."""

    def test_bronze_tables_exist(self, pipeline_db):
        tables = pipeline_db.get_tables()
        assert "bronze_ads_performance" in tables
        assert "bronze_ga_events" in tables
        assert "bronze_campaigns" in tables
        assert "bronze_adgroups" in tables

    def test_bronze_has_metadata_columns(self, pipeline_db):
        df = pipeline_db.execute("SELECT * FROM bronze_ads_performance LIMIT 1")
        assert "_loaded_at" in df.columns
        assert "_source_file" in df.columns
        assert "_batch_id" in df.columns

    def test_bronze_not_empty(self, pipeline_db):
        count = pipeline_db.row_count("bronze_ads_performance")
        assert count > 0


class TestSilverLayer:
    """Tests for Silver layer transformations."""

    def test_silver_tables_exist(self, pipeline_db):
        tables = pipeline_db.get_tables()
        assert "silver_ads_performance" in tables
        assert "silver_ga_events" in tables

    def test_silver_has_derived_metrics(self, pipeline_db):
        df = pipeline_db.execute("SELECT * FROM silver_ads_performance LIMIT 1")
        derived = {"ctr", "cpc", "cpa", "roas", "conversion_rate"}
        assert derived.issubset(set(df.columns))

    def test_silver_ctr_range(self, pipeline_db):
        df = pipeline_db.execute(
            "SELECT COUNT(*) AS violations FROM silver_ads_performance WHERE ctr < 0 OR ctr > 1"
        )
        assert df["violations"].iloc[0] == 0

    def test_silver_no_negative_spend(self, pipeline_db):
        df = pipeline_db.execute(
            "SELECT COUNT(*) AS violations FROM silver_ads_performance WHERE spend < 0"
        )
        assert df["violations"].iloc[0] == 0

    def test_silver_has_enrichment(self, pipeline_db):
        df = pipeline_db.execute("SELECT * FROM silver_ads_performance LIMIT 1")
        assert "campaign_name" in df.columns
        assert "objective" in df.columns

    def test_silver_ga_has_event_category(self, pipeline_db):
        df = pipeline_db.execute("SELECT * FROM silver_ga_events LIMIT 1")
        assert "event_category" in df.columns
        assert "is_bounce" in df.columns


class TestGoldLayer:
    """Tests for Gold layer aggregations."""

    def test_gold_tables_exist(self, pipeline_db):
        tables = pipeline_db.get_tables()
        assert "gold_campaign_daily" in tables
        assert "gold_channel_daily" in tables
        assert "gold_hourly_trends" in tables
        assert "gold_funnel_summary" in tables

    def test_gold_campaign_daily_not_empty(self, pipeline_db):
        count = pipeline_db.row_count("gold_campaign_daily")
        assert count > 0

    def test_gold_campaign_daily_columns(self, pipeline_db):
        df = pipeline_db.execute("SELECT * FROM gold_campaign_daily LIMIT 1")
        expected = {"date", "campaign_id", "channel", "total_impressions",
                    "total_clicks", "total_spend", "total_revenue", "avg_roas"}
        assert expected.issubset(set(df.columns))

    def test_gold_no_negative_metrics(self, pipeline_db):
        df = pipeline_db.execute("""
            SELECT COUNT(*) AS violations FROM gold_campaign_daily 
            WHERE total_spend < 0 OR total_revenue < 0
        """)
        assert df["violations"].iloc[0] == 0

    def test_gold_channel_daily_all_channels(self, pipeline_db):
        df = pipeline_db.execute("SELECT DISTINCT channel FROM gold_channel_daily")
        channels = df["channel"].tolist()
        assert len(channels) >= 3  # At least 3 channels
