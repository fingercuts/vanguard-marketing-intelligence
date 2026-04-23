"""Tests for the data quality framework."""

import pytest
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from src.utils.database import DatabaseManager
from src.quality.checks import DataQualityChecker, run_all_checks
from src.generator.data_generator import generate_all_data
from src.pipeline.bronze import ingest_to_bronze
from src.pipeline.silver import run_silver_transforms
from src.pipeline.gold import run_gold_aggregations


@pytest.fixture(scope="module")
def quality_db(tmp_path_factory):
    """Set up test database with pipeline data and run quality checks."""
    tmp_dir = tmp_path_factory.mktemp("test_quality")
    db_path = str(tmp_dir / "test_quality.duckdb")
    db = DatabaseManager(db_path)

    generate_all_data()
    ingest_to_bronze(db, "test_batch")
    run_silver_transforms(db, "test_batch")
    run_gold_aggregations(db)

    return db


class TestDataQualityChecker:
    """Tests for the quality checking framework."""

    def test_run_all_checks(self, quality_db):
        checker = run_all_checks(quality_db)
        summary = checker.get_summary()
        assert summary["total"] > 0
        assert summary["pass_rate"] > 0

    def test_quality_results_stored(self, quality_db):
        assert quality_db.table_exists("quality_check_results")
        count = quality_db.row_count("quality_check_results")
        assert count > 0

    def test_completeness_checks_pass(self, quality_db):
        checker = DataQualityChecker(quality_db)
        checker.check_completeness("silver_ads_performance",
                                     ["date", "campaign_id", "channel"])
        results = [r for r in checker.results if r["check_type"] == "completeness"]
        assert all(r["status"] == "PASS" for r in results)

    def test_validity_non_negative(self, quality_db):
        checker = DataQualityChecker(quality_db)
        checker.check_validity_non_negative("silver_ads_performance",
                                              ["impressions", "clicks", "spend"])
        results = [r for r in checker.results if r["check_type"] == "validity"]
        assert all(r["status"] == "PASS" for r in results)

    def test_freshness_check(self, quality_db):
        checker = DataQualityChecker(quality_db)
        checker.check_freshness("silver_ads_performance")
        results = [r for r in checker.results if r["check_type"] == "freshness"]
        assert len(results) > 0

    def test_anomaly_detection(self, quality_db):
        checker = DataQualityChecker(quality_db)
        checker.check_anomalies_zscore("silver_ads_performance", "spend")
        results = [r for r in checker.results if r["check_type"] == "anomaly"]
        assert len(results) > 0

    def test_quality_summary_format(self, quality_db):
        checker = DataQualityChecker(quality_db)
        checker.check_completeness("silver_ads_performance", ["date"])
        summary = checker.get_summary()
        assert "total" in summary
        assert "passed" in summary
        assert "warned" in summary
        assert "failed" in summary
        assert "pass_rate" in summary
