-- dbt/ebc_lakehouse/models/silver/stg_ach_transactions.sql
--
-- Silver layer: cleansed EG-ACH transactions.
--
-- Source: bronze.ach_transactions, written by Flink CDC + upsert-kafka.
-- Unlike the old Debezium path, the upsert-kafka sink does NOT carry a
-- Debezium envelope (no __op / __ts_ms columns). Instead:
--   • Flink's Iceberg sink runs in upsert mode with equality-field-columns
--     keyed on txn_id, so the bronze table already contains the latest
--     state per PK (deletes are emitted as tombstone records and physically
--     removed on compaction).
--   • created_at is the natural event-time column — used both for the
--     incremental high-water mark and for row-level dedup.
--
-- We still dedupe on (txn_id) because two upsert events for the same key
-- can land in the same Flink checkpoint window before Iceberg compaction,
-- which would otherwise inflate the silver row count.
{{ config(
    materialized          = 'incremental',
    incremental_strategy  = 'merge',
    unique_key            = 'txn_id',
    on_schema_change      = 'append_new_columns',
    partitioned_by        = ['day(source_created_at)']
) }}

WITH source AS (
    SELECT
        txn_id, batch_id, txn_type,
        originating_bank_id, receiving_bank_id,
        originator_account, receiver_account,
        amount_egp, currency_code, txn_status,
        created_at, settlement_date
    FROM {{ source('bronze', 'ach_transactions') }}
    {% if is_incremental() %}
      -- Only pull rows whose source-time is newer than what we've already
      -- merged. The 1-hour back-window absorbs late upserts from Flink's
      -- checkpoint commit (worst case = 1 checkpoint interval + clock skew).
      WHERE created_at > (
        SELECT coalesce(max(source_created_at), TIMESTAMP '1970-01-01 00:00:00')
                 - INTERVAL '1' HOUR
        FROM {{ this }}
      )
    {% endif %}
),

deduplicated AS (
    SELECT *,
           row_number() OVER (
               PARTITION BY txn_id
               ORDER BY created_at DESC
           ) AS _row_num
    FROM source
)

SELECT
    CAST(txn_id   AS VARCHAR)                                            AS txn_id,
    CAST(batch_id AS VARCHAR)                                            AS batch_id,
    upper(trim(CAST(txn_type            AS VARCHAR)))                    AS txn_type,
    upper(trim(CAST(originating_bank_id AS VARCHAR)))                    AS originating_bank_id,
    upper(trim(CAST(receiving_bank_id   AS VARCHAR)))                    AS receiving_bank_id,
    CAST(originator_account AS VARCHAR)                                  AS originator_account,
    CAST(receiver_account   AS VARCHAR)                                  AS receiver_account,
    coalesce(CAST(amount_egp AS DECIMAL(18,2)),
             CAST(0 AS DECIMAL(18,2)))                                   AS amount_egp,
    coalesce(CAST(currency_code AS VARCHAR), 'EGP')                      AS currency_code,
    upper(trim(CAST(txn_status AS VARCHAR)))                             AS txn_status,
    CAST(created_at AS TIMESTAMP(6))                                     AS source_created_at,
    CAST(created_at AS DATE)                                             AS created_date,
    settlement_date,
    current_timestamp(6)                                                 AS _silver_loaded_at
FROM deduplicated
WHERE _row_num = 1
  AND txn_id IS NOT NULL
