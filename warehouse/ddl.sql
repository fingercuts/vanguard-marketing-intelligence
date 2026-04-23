-- ============================================================
-- Vanguard Marketing Intelligence (VMI) — Enterprise DDL
-- Medallion Architecture: Bronze → Silver → Gold
-- Engine: DuckDB Analytical Engine
-- ============================================================

-- ===================
-- BRONZE LAYER (Raw)
-- ===================

CREATE TABLE IF NOT EXISTS bronze_ads_performance (
    date              DATE,
    hour              INTEGER,
    campaign_id       VARCHAR,
    adgroup_id        VARCHAR,
    channel           VARCHAR,
    impressions       INTEGER,
    clicks            INTEGER,
    spend             DOUBLE,
    conversions       INTEGER,
    revenue           DOUBLE,
    _loaded_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _source_file      VARCHAR,
    _batch_id         VARCHAR
);

CREATE TABLE IF NOT EXISTS bronze_ga_events (
    date              DATE,
    hour              INTEGER,
    event_name        VARCHAR,
    session_id        VARCHAR,
    user_id           VARCHAR,
    page              VARCHAR,
    source            VARCHAR,
    medium            VARCHAR,
    campaign_id       VARCHAR,
    _loaded_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _source_file      VARCHAR,
    _batch_id         VARCHAR
);

CREATE TABLE IF NOT EXISTS bronze_campaigns (
    campaign_id       VARCHAR,
    campaign_name     VARCHAR,
    objective         VARCHAR,
    budget_daily      DOUBLE,
    start_date        DATE,
    end_date          DATE,
    status            VARCHAR,
    _loaded_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _source_file      VARCHAR,
    _batch_id         VARCHAR
);

CREATE TABLE IF NOT EXISTS bronze_adgroups (
    adgroup_id        VARCHAR,
    campaign_id       VARCHAR,
    adgroup_name      VARCHAR,
    targeting_type    VARCHAR,
    bid_strategy      VARCHAR,
    _loaded_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _source_file      VARCHAR,
    _batch_id         VARCHAR
);


-- ===================
-- SILVER LAYER (Clean + Enriched)
-- ===================

CREATE TABLE IF NOT EXISTS silver_ads_performance (
    date              DATE,
    hour              INTEGER,
    campaign_id       VARCHAR,
    adgroup_id        VARCHAR,
    channel           VARCHAR,
    impressions       INTEGER,
    clicks            INTEGER,
    spend             DOUBLE,
    conversions       INTEGER,
    revenue           DOUBLE,
    -- Derived metrics
    ctr               DOUBLE,    -- Click-Through Rate
    cpc               DOUBLE,    -- Cost Per Click
    cpa               DOUBLE,    -- Cost Per Acquisition
    roas              DOUBLE,    -- Return On Ad Spend
    conversion_rate   DOUBLE,    -- Conversion Rate
    -- Enrichment from dimensions
    campaign_name     VARCHAR,
    objective         VARCHAR,
    budget_daily      DOUBLE,
    adgroup_name      VARCHAR,
    targeting_type    VARCHAR,
    bid_strategy      VARCHAR,
    -- Metadata
    _loaded_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _batch_id         VARCHAR
);

CREATE TABLE IF NOT EXISTS silver_ga_events (
    date              DATE,
    hour              INTEGER,
    event_name        VARCHAR,
    event_category    VARCHAR,    -- Derived: engagement/conversion/navigation
    session_id        VARCHAR,
    user_id           VARCHAR,
    page              VARCHAR,
    source            VARCHAR,
    medium            VARCHAR,
    campaign_id       VARCHAR,
    is_bounce         BOOLEAN,    -- Derived: single-event session flag
    _loaded_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _batch_id         VARCHAR
);


-- ===================
-- GOLD LAYER (Aggregated)
-- ===================

CREATE TABLE IF NOT EXISTS gold_campaign_daily (
    date              DATE,
    campaign_id       VARCHAR,
    campaign_name     VARCHAR,
    channel           VARCHAR,
    objective         VARCHAR,
    total_impressions BIGINT,
    total_clicks      BIGINT,
    total_spend       DOUBLE,
    total_conversions BIGINT,
    total_revenue     DOUBLE,
    avg_ctr           DOUBLE,
    avg_cpc           DOUBLE,
    avg_cpa           DOUBLE,
    avg_roas          DOUBLE,
    avg_conversion_rate DOUBLE,
    _loaded_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS gold_channel_daily (
    date              DATE,
    channel           VARCHAR,
    total_impressions BIGINT,
    total_clicks      BIGINT,
    total_spend       DOUBLE,
    total_conversions BIGINT,
    total_revenue     DOUBLE,
    avg_ctr           DOUBLE,
    avg_cpc           DOUBLE,
    avg_roas          DOUBLE,
    campaign_count    INTEGER,
    _loaded_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS gold_hourly_trends (
    date              DATE,
    hour              INTEGER,
    channel           VARCHAR,
    total_impressions BIGINT,
    total_clicks      BIGINT,
    total_spend       DOUBLE,
    total_conversions BIGINT,
    total_revenue     DOUBLE,
    avg_ctr           DOUBLE,
    _loaded_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS gold_funnel_summary (
    date              DATE,
    campaign_id       VARCHAR,
    campaign_name     VARCHAR,
    total_impressions BIGINT,
    total_clicks      BIGINT,
    total_conversions BIGINT,
    impression_to_click_rate  DOUBLE,
    click_to_conversion_rate  DOUBLE,
    overall_conversion_rate   DOUBLE,
    _loaded_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


-- ===================
-- DATA QUALITY RESULTS
-- ===================

CREATE TABLE IF NOT EXISTS quality_check_results (
    check_id          VARCHAR,
    check_name        VARCHAR,
    check_type        VARCHAR,    -- completeness | validity | consistency | anomaly | freshness
    table_name        VARCHAR,
    column_name       VARCHAR,
    status            VARCHAR,    -- PASS | FAIL | WARN
    metric_value      DOUBLE,
    threshold         DOUBLE,
    details           VARCHAR,
    checked_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    batch_id          VARCHAR
);
