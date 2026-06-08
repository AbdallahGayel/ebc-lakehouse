-- =============================================================================
-- 99_validate.sql
--
-- Validation queries run by init_trino.py after the DDL phase. Each query
-- must return 0 rows in a healthy state — non-zero output identifies the
-- specific schema or operational table that's missing.
--
-- The runner aggregates results across queries and exits non-zero if any
-- check fails, so this file doubles as a smoke test you can run ad-hoc:
--   trino --user ebc_user --execute "$(cat 99_validate.sql)"
-- =============================================================================

-- ── Q1: all 4 medallion schemas exist ──────────────────────────────────────
SELECT 'missing-schema: ' || expected AS issue
FROM   (VALUES ('bronze'), ('silver'), ('gold'), ('serving')) AS t(expected)
WHERE  NOT EXISTS (
    SELECT 1
    FROM   iceberg.information_schema.schemata
    WHERE  schema_name = t.expected
);

-- ── Q2: every operational Serving table exists ─────────────────────────────
SELECT 'missing-serving-table: ' || expected AS issue
FROM   (VALUES
            ('daily_txn_volume'),
            ('scheme_performance'),
            ('settlement_summary'),
            ('pipeline_metrics'),
            ('metricflow_time_spine')
       ) AS t(expected)
WHERE  NOT EXISTS (
    SELECT 1
    FROM   iceberg.information_schema.tables
    WHERE  table_schema = 'serving'
      AND  table_name   = t.expected
);

-- ── Q3: every Serving table carries a refreshed_at audit column ────────────
SELECT 'serving-table-missing-refreshed_at: ' || table_name AS issue
FROM   iceberg.information_schema.tables t
WHERE  t.table_schema = 'serving'
  AND  t.table_name IN ('daily_txn_volume', 'scheme_performance', 'settlement_summary')
  AND  NOT EXISTS (
      SELECT 1
      FROM   iceberg.information_schema.columns c
      WHERE  c.table_schema = t.table_schema
        AND  c.table_name   = t.table_name
        AND  c.column_name  = 'refreshed_at'
  );
