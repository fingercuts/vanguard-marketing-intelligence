"""
Data Governance Framework - Quality Control protocols.

Implements automated validation checks targeting:
- Completeness: Null attribution auditing
- Validity: Domain constraint verification
- Consistency: Referential and volume-based integrity
- Anomaly: Statistical outlier hijacking detection
- Freshness: SLA-based data recency verification

Results are persisted in the 'quality_check_results' operational table.
"""

import logging
import uuid
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from config.settings import QUALITY_THRESHOLDS, BATCH_ID_FORMAT
from src.utils.database import DatabaseManager
from src.utils.logging_config import setup_logging

logger = setup_logging("governance")


class DataQualityChecker:
    """Orchestrates multi-layer data quality validation."""

    def __init__(self, db: DatabaseManager, batch_id: str = None):
        self.db = db
        self.batch_id = batch_id or datetime.now().strftime(BATCH_ID_FORMAT)
        self.results = []
        self.thresholds = QUALITY_THRESHOLDS

    def _record(self, check_name, check_type, table_name, column_name,
                status, metric_value, threshold, details=""):
        """Internal telemetry for check results."""
        self.results.append({
            "check_id": uuid.uuid4().hex[:12],
            "check_name": check_name,
            "check_type": check_type,
            "table_name": table_name,
            "column_name": column_name,
            "status": status,
            "metric_value": metric_value,
            "threshold": threshold,
            "details": details,
            "checked_at": datetime.now().isoformat(),
            "batch_id": self.batch_id,
        })

        tag = f"[{status}]".ljust(8)
        logger.info(f"  {tag} {check_name}: {metric_value} (TH: {threshold}) - {details}")

    # ==============================================================
    # COMPLETENESS PROTOCOLS
    # ==============================================================

    def check_completeness(self, table_name: str, columns: list):
        """Audits for null presence in critical dimension/fact columns."""
        logger.info(f"Auditing completeness on {table_name}...")

        with self.db.connection() as conn:
            total = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            if total == 0:
                self._record("completeness", "completeness", table_name, "*",
                             "FAIL", 0, 0, "Null count impossible: Table is empty")
                return

            for col in columns:
                null_count = conn.execute(
                    f"SELECT COUNT(*) FROM {table_name} WHERE {col} IS NULL"
                ).fetchone()[0]
                null_pct = null_count / total
                threshold = self.thresholds["max_null_pct"]
                status = "PASS" if null_pct <= threshold else "FAIL"
                self._record(
                    f"null_check_{col}", "completeness", table_name, col,
                    status, round(null_pct, 4), threshold,
                    f"Detected {null_count:,} nulls in {total:,} rows"
                )

    # ==============================================================
    # VALIDITY PROTOCOLS
    # ==============================================================

    def check_validity_non_negative(self, table_name: str, columns: list):
        """Enforces non-negative constraints on fiscal and volume metrics."""
        logger.info(f"Validating non-negative constraints on {table_name}...")

        with self.db.connection() as conn:
            for col in columns:
                neg_count = conn.execute(
                    f"SELECT COUNT(*) FROM {table_name} WHERE {col} < 0"
                ).fetchone()[0]
                status = "PASS" if neg_count == 0 else "FAIL"
                self._record(
                    f"non_negative_{col}", "validity", table_name, col,
                    status, neg_count, 0,
                    f"Illegal negative values detected ({neg_count:,} instances)"
                )

    def check_validity_range(self, table_name: str, column: str,
                               min_val: float = None, max_val: float = None):
        """Validates that metrics remain within logical business boundaries."""
        logger.info(f"Range verification on {table_name}.{column}...")

        with self.db.connection() as conn:
            conditions = []
            if min_val is not None:
                conditions.append(f"{column} < {min_val}")
            if max_val is not None:
                conditions.append(f"{column} > {max_val}")

            if not conditions:
                return

            where = " OR ".join(conditions)
            violation_count = conn.execute(
                f"SELECT COUNT(*) FROM {table_name} WHERE {where}"
            ).fetchone()[0]

            total = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            violation_pct = violation_count / total if total > 0 else 0

            # Warning if < 1% violation, Fail otherwise
            status = "PASS" if violation_count == 0 else ("WARN" if violation_pct < 0.01 else "FAIL")
            self._record(
                f"range_check_{column}", "validity", table_name, column,
                status, violation_count, 0,
                f"Target range [{min_val}, {max_val}] - Violations: {violation_count:,}"
            )

    # ==============================================================
    # CONSISTENCY PROTOCOLS
    # ==============================================================

    def check_consistency_row_count(self, table_name: str, min_rows: int):
        """Verifies ingestion volume against minimum baseline."""
        logger.info(f"Volume consistency check on {table_name}...")

        with self.db.connection() as conn:
            count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            status = "PASS" if count >= min_rows else "FAIL"
            self._record(
                f"row_count_{table_name}", "consistency", table_name, "*",
                status, count, min_rows,
                f"Current volume: {count:,} (Baseline: {min_rows:,})"
            )

    def check_consistency_referential(self, child_table: str, child_col: str,
                                       parent_table: str, parent_col: str):
        """Audits referential integrity between fact and dimension tables."""
        logger.info(f"Referential audit: {child_table}.{child_col} -> {parent_table}.{parent_col}...")

        with self.db.connection() as conn:
            orphan_count = conn.execute(f"""
                SELECT COUNT(DISTINCT c.{child_col})
                FROM {child_table} c
                LEFT JOIN {parent_table} p ON c.{child_col} = p.{parent_col}
                WHERE p.{parent_col} IS NULL AND c.{child_col} IS NOT NULL
            """).fetchone()[0]

            status = "PASS" if orphan_count == 0 else "WARN"
            self._record(
                f"ref_integrity_{child_col}", "consistency", child_table, child_col,
                status, orphan_count, 0,
                f"Detected {orphan_count} orphaned identifiers in {child_table}"
            )

    # ==============================================================
    # STATISTICAL ANOMALY DETECTION
    # ==============================================================

    def check_anomalies_zscore(self, table_name: str, column: str, group_by: str = None):
        """Flags statistical outliers using Z-score methodology (3-sigma)."""
        logger.info(f"Statistical anomaly scan on {table_name}.{column}...")

        with self.db.connection() as conn:
            if group_by:
                df = conn.execute(f"""
                    SELECT {group_by}, {column},
                        AVG({column}) OVER () AS global_mean,
                        STDDEV({column}) OVER () AS global_std
                    FROM {table_name}
                    WHERE {column} IS NOT NULL
                """).fetchdf()
            else:
                df = conn.execute(f"""
                    SELECT {column} FROM {table_name} WHERE {column} IS NOT NULL
                """).fetchdf()

            if df.empty:
                return

            values = df[column].values
            mean = np.mean(values)
            std = np.std(values)

            if std == 0:
                self._record(
                    f"anomaly_{column}", "anomaly", table_name, column,
                    "PASS", 0, self.thresholds["zscore_threshold"],
                    "Invariance detected (zero variance baseline)"
                )
                return

            z_scores = np.abs((values - mean) / std)
            outlier_count = int(np.sum(z_scores > self.thresholds["zscore_threshold"]))
            outlier_pct = outlier_count / len(values)

            # Warning if < 5% are outliers, otherwise trigger failure profile
            status = "PASS" if outlier_pct < 0.02 else ("WARN" if outlier_pct < 0.05 else "FAIL")
            self._record(
                f"anomaly_{column}", "anomaly", table_name, column,
                status, outlier_count, self.thresholds["zscore_threshold"],
                f"Outlier density: {outlier_pct:.2%} ({outlier_count:,} rows)"
            )

    # ==============================================================
    # FRESHNESS PROTOCOLS (SLA MANAGEMENT)
    # ==============================================================

    def check_freshness(self, table_name: str, date_column: str = "date"):
        """Enforces reporting SLAs by checking data ingestion latency."""
        logger.info(f"SLA-Freshness verification on {table_name}...")

        with self.db.connection() as conn:
            max_date = conn.execute(
                f"SELECT MAX(CAST({date_column} AS DATE)) FROM {table_name}"
            ).fetchone()[0]

            if max_date is None:
                self._record(
                    f"freshness_{table_name}", "freshness", table_name, date_column,
                    "FAIL", 0, self.thresholds["max_staleness_hours"],
                    "Incomplete batch: No date markers found"
                )
                return

            # Standardization for DuckDB date handling
            if isinstance(max_date, str):
                max_date = datetime.strptime(max_date[:10], "%Y-%m-%d")
            elif hasattr(max_date, 'to_pydatetime'):
                max_date = max_date.to_pydatetime()
                
            if not isinstance(max_date, datetime):
                max_date = datetime(max_date.year, max_date.month, max_date.day)

            staleness_hours = (datetime.now() - max_date).total_seconds() / 3600
            threshold = self.thresholds["max_staleness_hours"]

            status = "PASS" if staleness_hours <= threshold else "WARN"
            self._record(
                f"freshness_{table_name}", "freshness", table_name, date_column,
                status, round(staleness_hours, 1), threshold,
                f"Latency: {staleness_hours:.1f}h (SLA Boundary: {threshold}h)"
            )

    # ==============================================================
    # PERSISTENCE & REPORTING
    # ==============================================================

    def save_results(self):
        """Persists audit telemetry to the quality_check_results table."""
        if not self.results:
            logger.warning("Abort: No check telemetry available to persist.")
            return

        df = pd.DataFrame(self.results)

        with self.db.connection() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS quality_check_results (
                    check_id VARCHAR, check_name VARCHAR, check_type VARCHAR,
                    table_name VARCHAR, column_name VARCHAR, status VARCHAR,
                    metric_value DOUBLE, threshold DOUBLE, details VARCHAR,
                    checked_at TIMESTAMP, batch_id VARCHAR
                )
            """)
            conn.execute(f"DELETE FROM quality_check_results WHERE batch_id = '{self.batch_id}'")
            conn.execute("INSERT INTO quality_check_results SELECT * FROM df")

        logger.info(f"Audit telemetry persisted (Batch ID: {self.batch_id})")

    def get_summary(self) -> dict:
        """Computes executive-level pass/fail summary."""
        total = len(self.results)
        passed = sum(1 for r in self.results if r["status"] == "PASS")
        warned = sum(1 for r in self.results if r["status"] == "WARN")
        failed = sum(1 for r in self.results if r["status"] == "FAIL")
        return {
            "total": total,
            "passed": passed,
            "warned": warned,
            "failed": failed,
            "pass_rate": round(passed / total * 100, 1) if total > 0 else 0,
        }


# ==================================================================
# AUTOMATED SUITE ORCHESTRATION
# ==================================================================

def run_all_checks(db: DatabaseManager = None):
    """Executes the standard governance validation suite."""
    if db is None:
        db = DatabaseManager()

    logger.info("-" * 60)
    logger.info("INITIATING DATA GOVERNANCE SUITE")
    logger.info("-" * 60)

    checker = DataQualityChecker(db)

    # Fact Table Audit: Silver Ad Performance
    if db.table_exists("silver_ads_performance"):
        checker.check_completeness("silver_ads_performance",
            ["date", "campaign_id", "channel", "impressions", "clicks", "spend"])
        checker.check_validity_non_negative("silver_ads_performance",
            ["impressions", "clicks", "spend", "conversions", "revenue"])
        checker.check_validity_range("silver_ads_performance", "ctr", 0.0, 1.0)
        checker.check_validity_range("silver_ads_performance", "roas", 0.0,
                                      QUALITY_THRESHOLDS["roas_max"])
        checker.check_anomalies_zscore("silver_ads_performance", "spend")
        checker.check_anomalies_zscore("silver_ads_performance", "revenue")
        checker.check_freshness("silver_ads_performance")

    # Fact Table Audit: Silver GA Events
    if db.table_exists("silver_ga_events"):
        checker.check_completeness("silver_ga_events",
            ["date", "event_name", "session_id", "user_id"])
        checker.check_freshness("silver_ga_events")

    # Gold Layer Audit: Campaign Aggregates
    if db.table_exists("gold_campaign_daily"):
        checker.check_consistency_row_count("gold_campaign_daily",
                                             QUALITY_THRESHOLDS["min_daily_rows"])
        checker.check_validity_non_negative("gold_campaign_daily",
            ["total_impressions", "total_clicks", "total_spend", "total_revenue"])
        checker.check_freshness("gold_campaign_daily")

    # Integrity Audit
    if db.table_exists("silver_ads_performance") and db.table_exists("bronze_campaigns"):
        checker.check_consistency_referential(
            "silver_ads_performance", "campaign_id",
            "bronze_campaigns", "campaign_id"
        )

    checker.save_results()

    summary = checker.get_summary()
    logger.info("-" * 60)
    logger.info(f"AUDIT SUMMARY: {summary['pass_rate']}% Pass Rate")
    logger.info(f"Total: {summary['total']} | Passed: {summary['passed']} | Fail: {summary['failed']}")
    logger.info("-" * 60)

    return checker


if __name__ == "__main__":
    run_all_checks()
