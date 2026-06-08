-- =============================================================================
-- 40_serving_tables.sql
--
-- Serving-layer DDL — owned by this init script (matches the role the old
-- ClickHouse init script played for the MergeTree cache tables). dbt models
-- in models/serving/ MERGE into these pre-created Iceberg tables on every
-- Gold run; pre-creating them here means:
--
--   • BI tools (Superset, DataHub) can introspect the schema before any
--     dbt run has happened — useful at first boot.
--   • dbt-trino's `incremental_strategy='merge'` upserts into the existing
--     table on every run (no DROP/RECREATE conflict).
--   • Schema evolution (append-only) still works: dbt + Iceberg add the
--     new column in-place.
--
-- Storage:     s3://ebc-lakehouse/serving/<table>/
-- Format:      PARQUET (format_version=2)
-- Partition:   month(<report_date>) — matches Gold for clean partition prune
-- =============================================================================

-- ── daily_txn_volume — daily volume + GMV by scheme ────────────────────────
CREATE TABLE IF NOT EXISTS iceberg.serving.daily_txn_volume (
    report_date         DATE,
    scheme              VARCHAR,
    txn_count           BIGINT,
    total_amount_egp    DECIMAL(18,2),
    approved_count      BIGINT,
    failed_count        BIGINT,
    avg_txn_amount_egp  DECIMAL(18,2),
    refreshed_at        TIMESTAMP(6) WITH TIME ZONE
)
WITH (
    format         = 'PARQUET',
    format_version = 2,
    partitioning   = ARRAY['month(report_date)']
);

-- ── scheme_performance — approval rates + latency per channel ──────────────
CREATE TABLE IF NOT EXISTS iceberg.serving.scheme_performance (
    report_date        DATE,
    scheme             VARCHAR,
    channel            VARCHAR,
    approval_rate_pct  DECIMAL(6,3),
    avg_processing_ms  DOUBLE,
    txn_volume         BIGINT,
    refreshed_at       TIMESTAMP(6) WITH TIME ZONE
)
WITH (
    format         = 'PARQUET',
    format_version = 2,
    partitioning   = ARRAY['month(report_date)']
);

-- ── settlement_summary — daily net position per bank pair ──────────────────
CREATE TABLE IF NOT EXISTS iceberg.serving.settlement_summary (
    settlement_date      DATE,
    originating_bank_id  VARCHAR,
    receiving_bank_id    VARCHAR,
    settled_count        BIGINT,
    failed_count         BIGINT,
    returned_count       BIGINT,
    net_settled_egp      DECIMAL(18,2),
    failed_amount_egp    DECIMAL(18,2),
    settlement_rate_pct  DECIMAL(6,2),
    refreshed_at         TIMESTAMP(6) WITH TIME ZONE
)
WITH (
    format         = 'PARQUET',
    format_version = 2,
    partitioning   = ARRAY['month(settlement_date)']
);

-- ── pipeline_metrics — operational telemetry, owned by Airflow DAGs ───────
CREATE TABLE IF NOT EXISTS iceberg.serving.pipeline_metrics (
    run_id            VARCHAR,
    source_topic      VARCHAR,
    kafka_offset      BIGINT,
    bronze_row_count  BIGINT,
    silver_row_count  BIGINT,
    sink_lag_seconds  DOUBLE,
    batch_date        DATE,
    recorded_at       TIMESTAMP(6) WITH TIME ZONE
)
WITH (
    format         = 'PARQUET',
    format_version = 2,
    partitioning   = ARRAY['month(batch_date)']
);

-- ── metricflow_time_spine — calendar table for dbt MetricFlow ─────────────
-- dbt's `dbt run --select metricflow_time_spine` repopulates this on demand.
-- We create the shell here so MetricFlow validation can run before the first
-- dbt build.
CREATE TABLE IF NOT EXISTS iceberg.serving.metricflow_time_spine (
    date_day  DATE
)
WITH (
    format         = 'PARQUET',
    format_version = 2,
    partitioning   = ARRAY['year(date_day)']
);
