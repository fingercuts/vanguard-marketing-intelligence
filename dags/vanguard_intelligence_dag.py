"""
Apache Airflow DAG — Vanguard Marketing Intelligence.

Orchestrates the end-to-end analytical pipeline:
  generate_data → bronze_ingest → silver_transform → gold_aggregate → quality_checks

Schedule: Daily at 06:00 UTC
Retries: 2 with 5-minute delay
"""

from datetime import datetime, timedelta
from airflow import DAG

try:
    from airflow.providers.standard.operators.python import PythonOperator
except ImportError:
    from airflow.operators.python import PythonOperator

# ---------------------------------------------------------------
# Default arguments
# ---------------------------------------------------------------
default_args = {
    "owner": "vanguard-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}


# ---------------------------------------------------------------
# Task callables
# ---------------------------------------------------------------
def task_generate_data(**context):
    """Generate simulated marketing telemetry."""
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from src.generator.data_generator import generate_all_data

    result = generate_all_data()
    context["ti"].xcom_push(key="row_counts", value={
        k: len(v) for k, v in result.items()
    })


def task_bronze_ingest(**context):
    """Ingest raw telemetry into Bronze layer."""
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from src.utils.database import DatabaseManager
    from src.pipeline.bronze import ingest_to_bronze

    db = DatabaseManager()
    execution_date = context["ds"]
    batch_id = f"vanguard_airflow_{execution_date}"
    ingest_to_bronze(db, batch_id)


def task_silver_transform(**context):
    """Transform Bronze → Silver with domain-specific KPIs."""
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from src.utils.database import DatabaseManager
    from src.pipeline.silver import run_silver_transforms

    db = DatabaseManager()
    batch_id = f"vanguard_airflow_{context['ds']}"
    run_silver_transforms(db, batch_id)


def task_gold_aggregate(**context):
    """Aggregate Silver → Gold for executive reporting."""
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from src.utils.database import DatabaseManager
    from src.pipeline.gold import run_gold_aggregations

    db = DatabaseManager()
    run_gold_aggregations(db)


def task_quality_checks(**context):
    """Run data quality validation and governance suite."""
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from src.utils.database import DatabaseManager
    from src.quality.checks import run_all_checks

    db = DatabaseManager()
    checker = run_all_checks(db)
    summary = checker.get_summary()

    # Fail the task if critical checks failed
    if summary["failed"] > 0:
        raise ValueError(
            f"Governance Audit: {summary['failed']} protocols FAILED "
            f"(compliance rate: {summary['pass_rate']}%)"
        )


# ---------------------------------------------------------------
# DAG Definition
# ---------------------------------------------------------------
with DAG(
    dag_id="vanguard_marketing_intelligence",
    default_args=default_args,
    description="Vanguard End-to-end analytical pipeline: Generate → Bronze → Silver → Gold → Governance",
    schedule="0 6 * * *",  # Daily at 06:00 UTC
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["vanguard", "intelligence", "etl", "medallion"],
    doc_md="""
    ## Vanguard Marketing Intelligence Pipeline
    
    **Architecture:** Medallion (Bronze → Silver → Gold)  
    **Schedule:** Daily at 06:00 UTC  
    **Owner:** Marketing Engineering Team
    
    ### Tasks
    1. **generate_data** — Simulate multi-channel marketing telemetry
    2. **bronze_ingest** — Raw telemetry ingestion with audit metadata
    3. **silver_transform** — Clean, enrich, and calculate domain KPIs
    4. **gold_aggregate** — Strategic aggregation for executive dashboards
    5. **quality_checks** — Data governance audit and integrity validation
    """,
) as dag:

    generate = PythonOperator(
        task_id="generate_data",
        python_callable=task_generate_data,
    )

    bronze = PythonOperator(
        task_id="bronze_ingest",
        python_callable=task_bronze_ingest,
    )

    silver = PythonOperator(
        task_id="silver_transform",
        python_callable=task_silver_transform,
    )

    gold = PythonOperator(
        task_id="gold_aggregate",
        python_callable=task_gold_aggregate,
    )

    quality = PythonOperator(
        task_id="quality_checks",
        python_callable=task_quality_checks,
    )

    # Task dependencies
    generate >> bronze >> silver >> gold >> quality
