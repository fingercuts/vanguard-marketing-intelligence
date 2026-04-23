# Data Governance and Operational Standards

This document outlines the governance framework for the Vanguard Marketing Intelligence platform, focusing on data integrity, PII protection, and operational service levels.

## 1. Data Quality Governance
The platform implements a circuit-breaker pattern for data quality. Data that fails primary validation is sequestered to prevent downstream reporting contamination.

### 1.1 Quality Domains
* **Completeness**: All fact tables must contain valid `campaign_id` and `channel` markers. Missing values exceeding 1.0% trigger a system alert.
* **Validity**: Monetary values (spend, revenue) are subject to non-negative constraints. Logic-based range checks ensure CTR and Conversion Rates fall within the [0, 1] interval.
* **Consistency**: Daily row counts are benchmarked against 30-day moving averages to detect ingestion drops.
* **Anomaly Detection**: 3.0 Sigma Z-score thresholds are applied to hourly traffic spikes to identify potential bot activity.

## 2. PII and Security
Vanguard follows a "Security by Design" principle. 

* **Abstraction**: User identifiers (`user_id`) and session IDs are hashed at the raw ingestion layer. 
* **Access Control**: Gold-layer datasets are scoped specifically for executive reporting and contain no granular user-level activity.
* **Data Persistence**: Raw data extracts in the Bronze layer are subject to a 90-day retention policy to comply with regional privacy regulations.

## 3. Operational SLA
* **Pipeline Refresh**: Data is processed in 24-hour windows with a maximum staleness threshold of 48 hours for reporting.
* **Logging System**: Full structured logging with rotation is implemented across all ETL modules.
* **Alerting**: Production failures in the Airflow orchestration layer trigger immediate retry sequences before escalation.

## 4. Metadata Management
Every record in the analytics warehouse contains mandatory audit columns:
* `_loaded_at`: System timestamp of processing.
* `_batch_id`: Unique identifier for the ETL run.
* `_source_file`: Reference to the original Parquet extract for lineage tracing.
