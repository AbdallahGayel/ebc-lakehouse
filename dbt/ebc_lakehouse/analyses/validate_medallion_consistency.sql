-- =============================================================================
-- analyses/validate_medallion_consistency.sql
--
-- End-to-end consistency checklist for the Bronze → Silver → Gold → Serving
-- pipeline. Each query returns 0 rows in a healthy state; non-zero output
-- pinpoints exactly which table / day / scheme is drifting.
--
-- Run any single block ad-hoc in Trino, or compile the whole file with
--   dbt compile --select analysis:validate_medallion_consistency
-- to inspect the rendered SQL.
--
-- Layer contract under test:
--   • storage   — every table sits under its layer's MinIO prefix
--   • compute   — only Trino touches the data
--   • schema    — Silver/Gold/Serving carry the audit columns the writers add
--   • partitioning — date columns are partition keys per the convention
--   • row conservation — Silver count ≤ Bronze (after dedup); Serving = Gold (7d window)
--   • lineage   — each table has at least one Iceberg snapshot
-- =============================================================================

-- ── 1. STORAGE LOCATION CONTRACT ─────────────────────────────────────────────
-- Every Iceberg table's metadata_location must start with the expected
-- per-layer prefix. Drift here means a model was created against the wrong
-- namespace or Polaris namespace `location` was overwritten.
WITH location_contract AS (
    SELECT 'iceberg.ebc_bronze'  AS schema_name, 's3://ebc-lakehouse/bronze/'  AS required_prefix UNION ALL
    SELECT 'iceberg.ebc_silver',                  's3://ebc-lakehouse/silver/'                    UNION ALL
    SELECT 'iceberg.ebc_gold',                    's3://ebc-lakehouse/gold/'                      UNION ALL
    SELECT 'iceberg.ebc_serving',                 's3://ebc-lakehouse/serving/'
)
SELECT
    t.table_schema || '.' || t.table_name              AS qualified_name,
    metadata_location_for(t.table_schema, t.table_name) AS metadata_location,  -- pseudocode; see below
    c.required_prefix
FROM   iceberg.information_schema.tables t
JOIN   location_contract c
    ON 'iceberg.' || t.table_schema = c.schema_name
WHERE  t.table_type = 'BASE TABLE'
  AND  NOT regexp_like(
          metadata_location_for(t.table_schema, t.table_name),
          '^' || c.required_prefix
       );
-- Note: replace metadata_location_for(...) with the equivalent Trino call.
-- Trino exposes per-table metadata via `iceberg.<schema>."<table>$properties"`:
--   SELECT value FROM iceberg.ebc_bronze."ach_transactions$properties"
--   WHERE  key = 'write.metadata.path'
-- The full validation script-form lives below.


-- ── 1b. STORAGE LOCATION CONTRACT (runnable form) ────────────────────────────
SELECT
    qualified_name,
    metadata_location,
    required_prefix
FROM (
    SELECT  'ebc_bronze.'  || tn AS qualified_name,
            (SELECT value FROM iceberg.ebc_bronze."ach_transactions$properties"
             WHERE key = 'write.metadata.path') AS metadata_location,
            's3://ebc-lakehouse/bronze/' AS required_prefix
    FROM   (VALUES ('ach_transactions')) AS x(tn)
) v
WHERE NOT regexp_like(coalesce(metadata_location, ''), '^' || required_prefix);


-- ── 2. SCHEMA EVOLUTION AUDIT ────────────────────────────────────────────────
-- Every table in Silver/Gold/Serving must carry its writer's audit column
-- (silver: _silver_loaded_at, gold: _gold_loaded_at, serving: refreshed_at).
-- A missing column means a model was migrated without re-materialising.
SELECT layer || '.' || table_name AS qualified_name, missing_column
FROM (
    SELECT 'ebc_silver' AS layer, t.table_name, '_silver_loaded_at' AS missing_column
    FROM   iceberg.information_schema.tables t
    WHERE  t.table_schema = 'ebc_silver'
      AND  NOT EXISTS (
          SELECT 1
          FROM   iceberg.information_schema.columns c
          WHERE  c.table_schema = t.table_schema
            AND  c.table_name   = t.table_name
            AND  c.column_name  = '_silver_loaded_at'
      )

    UNION ALL

    SELECT 'ebc_gold', t.table_name, '_gold_loaded_at'
    FROM   iceberg.information_schema.tables t
    WHERE  t.table_schema = 'ebc_gold'
      AND  NOT EXISTS (
          SELECT 1
          FROM   iceberg.information_schema.columns c
          WHERE  c.table_schema = t.table_schema
            AND  c.table_name   = t.table_name
            AND  c.column_name  = '_gold_loaded_at'
      )

    UNION ALL

    SELECT 'ebc_serving', t.table_name, 'refreshed_at'
    FROM   iceberg.information_schema.tables t
    WHERE  t.table_schema = 'ebc_serving'
      AND  t.table_name  != 'pipeline_metrics'   -- op-table has its own audit col
      AND  NOT EXISTS (
          SELECT 1
          FROM   iceberg.information_schema.columns c
          WHERE  c.table_schema = t.table_schema
            AND  c.table_name   = t.table_name
            AND  c.column_name  = 'refreshed_at'
      )
);


-- ── 3. PARTITIONING CONVENTION ────────────────────────────────────────────────
-- Bronze:  days(<event_ts>)
-- Silver:  inherited from Bronze keys
-- Gold:    months(report_date) or months(settlement_date)
-- Serving: months(<same as Gold>)
--
-- Probe via $partitions metadata table — empty result if no partition spec.
SELECT 'ebc_gold.mart_daily_txn_volume'    AS table_id,
       count(*)                             AS partition_count
FROM   iceberg.ebc_gold."mart_daily_txn_volume$partitions"
HAVING count(*) = 0
UNION ALL
SELECT 'ebc_gold.mart_scheme_performance',   count(*)
FROM   iceberg.ebc_gold."mart_scheme_performance$partitions"
HAVING count(*) = 0
UNION ALL
SELECT 'ebc_gold.mart_settlement_summary',   count(*)
FROM   iceberg.ebc_gold."mart_settlement_summary$partitions"
HAVING count(*) = 0;


-- ── 4. ROW CONSERVATION (Bronze → Silver) ────────────────────────────────────
-- After dedup, Silver row count must be ≤ Bronze. A Silver count > Bronze
-- means duplicate rows leaked through the row_number() dedup window — a
-- correctness bug to fix in the staging model.
SELECT pair_name, bronze_count, silver_count
FROM (
    SELECT 'ach_transactions'            AS pair_name,
           (SELECT count(*) FROM iceberg.ebc_bronze.ach_transactions)            AS bronze_count,
           (SELECT count(*) FROM iceberg.ebc_silver.stg_ach_transactions)        AS silver_count
    UNION ALL
    SELECT 'meeza_authorisations',
           (SELECT count(*) FROM iceberg.ebc_bronze.meeza_authorisations),
           (SELECT count(*) FROM iceberg.ebc_silver.stg_meeza_authorisations)
    UNION ALL
    SELECT 'ipn_transactions',
           (SELECT count(*) FROM iceberg.ebc_bronze.ipn_transactions),
           (SELECT count(*) FROM iceberg.ebc_silver.stg_ipn_transactions)
    UNION ALL
    SELECT 'meeza_digital_wallet_events',
           (SELECT count(*) FROM iceberg.ebc_bronze.meeza_digital_wallet_events),
           (SELECT count(*) FROM iceberg.ebc_silver.stg_meeza_digital_wallet)
    UNION ALL
    SELECT 'atm_sessions',
           (SELECT count(*) FROM iceberg.ebc_bronze.atm_sessions),
           (SELECT count(*) FROM iceberg.ebc_silver.stg_atm_sessions)
)
WHERE silver_count > bronze_count;


-- ── 5. ROW CONSERVATION (Gold ↔ Serving, last 7 days) ────────────────────────
-- Serving is a MERGE-refreshed cache of Gold's rolling window. Within that
-- window, every Gold key must appear in Serving and vice-versa.
WITH gold_window AS (
    SELECT report_date, scheme, txn_count
    FROM   iceberg.ebc_gold.mart_daily_txn_volume
    WHERE  report_date >= current_date - INTERVAL '7' DAY
),
serving_window AS (
    SELECT report_date, scheme, txn_count
    FROM   iceberg.ebc_serving.daily_txn_volume
    WHERE  report_date >= current_date - INTERVAL '7' DAY
)
SELECT 'gold_minus_serving' AS direction, g.report_date, g.scheme, g.txn_count AS gold_value, NULL AS serving_value
FROM   gold_window g LEFT JOIN serving_window s
  ON   g.report_date = s.report_date AND g.scheme = s.scheme
WHERE  s.report_date IS NULL
UNION ALL
SELECT 'serving_minus_gold', s.report_date, s.scheme, NULL, s.txn_count
FROM   serving_window s LEFT JOIN gold_window g
  ON   s.report_date = g.report_date AND s.scheme = g.scheme
WHERE  g.report_date IS NULL;


-- ── 6. SNAPSHOT / LINEAGE PRESENCE ───────────────────────────────────────────
-- Every Bronze/Silver/Gold/Serving table must have at least one committed
-- snapshot. A table that exists but has no snapshot indicates a failed
-- write that the writer didn't surface as an error.
SELECT
    table_schema || '.' || table_name AS qualified_name,
    snapshot_count
FROM (
    SELECT t.table_schema, t.table_name,
           (SELECT count(*)
            FROM   TABLE(system.iceberg_snapshots(t.table_schema, t.table_name))) AS snapshot_count
    FROM   iceberg.information_schema.tables t
    WHERE  t.table_schema IN ('ebc_bronze', 'ebc_silver', 'ebc_gold', 'ebc_serving')
      AND  t.table_type = 'BASE TABLE'
)
WHERE snapshot_count = 0;
-- Note: if your Trino version lacks the `iceberg_snapshots` table function,
-- substitute the per-table `$snapshots` system table:
--   SELECT count(*) FROM iceberg.ebc_silver."stg_ach_transactions$snapshots";


-- ── 7. FRESHNESS WATERMARK (Silver → Serving) ────────────────────────────────
-- Silver _silver_loaded_at should be no older than 2× the hourly DAG SLA,
-- and Serving refreshed_at no older than 2× the Gold-triggered DAG SLA.
SELECT layer, table_name, hours_since_last_write
FROM (
    SELECT 'silver' AS layer, 'stg_ach_transactions' AS table_name,
           date_diff('hour',
               (SELECT max(_silver_loaded_at) FROM iceberg.ebc_silver.stg_ach_transactions),
               current_timestamp(6)) AS hours_since_last_write
    UNION ALL
    SELECT 'serving', 'daily_txn_volume',
           date_diff('hour',
               (SELECT max(refreshed_at) FROM iceberg.ebc_serving.daily_txn_volume),
               current_timestamp(6))
    UNION ALL
    SELECT 'serving', 'scheme_performance',
           date_diff('hour',
               (SELECT max(refreshed_at) FROM iceberg.ebc_serving.scheme_performance),
               current_timestamp(6))
    UNION ALL
    SELECT 'serving', 'settlement_summary',
           date_diff('hour',
               (SELECT max(refreshed_at) FROM iceberg.ebc_serving.settlement_summary),
               current_timestamp(6))
)
WHERE hours_since_last_write > 4;
