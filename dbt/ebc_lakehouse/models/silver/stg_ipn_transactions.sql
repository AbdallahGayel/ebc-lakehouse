-- dbt/ebc_lakehouse/models/silver/stg_ipn_transactions.sql
--
-- Silver layer: cleansed InstaPay IPN transactions (with SLA flag).
-- Source: bronze.ipn_transactions from Flink CDC + upsert-kafka.
{{ config(
    materialized          = 'incremental',
    incremental_strategy  = 'merge',
    unique_key            = 'txn_id',
    on_schema_change      = 'append_new_columns',
    partitioned_by        = ['day(initiated_at)']
) }}

WITH source AS (
    SELECT
        txn_id, payment_ref,
        sender_proxy, receiver_proxy,
        sender_bank_id, receiver_bank_id,
        amount_egp, payment_purpose, txn_status, failure_reason,
        initiation_channel, processing_time_ms,
        initiated_at, completed_at
    FROM {{ source('bronze', 'ipn_transactions') }}
    {% if is_incremental() %}
      WHERE initiated_at > (
        SELECT coalesce(max(initiated_at), TIMESTAMP '1970-01-01 00:00:00')
                 - INTERVAL '1' HOUR
        FROM {{ this }}
      )
    {% endif %}
),

deduplicated AS (
    SELECT *,
           row_number() OVER (
               PARTITION BY txn_id
               -- Prefer completed events over partial ones (completed_at non-null),
               -- then take the most recent initiated_at.
               ORDER BY CASE WHEN completed_at IS NULL THEN 1 ELSE 0 END ASC,
                        initiated_at DESC
           ) AS _row_num
    FROM source
)

SELECT
    CAST(txn_id AS VARCHAR)                                               AS txn_id,
    trim(coalesce(CAST(payment_ref AS VARCHAR), ''))                      AS payment_ref,
    coalesce(CAST(sender_proxy   AS VARCHAR), 'UNKNOWN')                  AS sender_proxy,
    coalesce(CAST(receiver_proxy AS VARCHAR), 'UNKNOWN')                  AS receiver_proxy,
    upper(trim(coalesce(CAST(sender_bank_id   AS VARCHAR), '')))          AS sender_bank_id,
    upper(trim(coalesce(CAST(receiver_bank_id AS VARCHAR), '')))          AS receiver_bank_id,
    coalesce(CAST(amount_egp AS DOUBLE), 0)                               AS amount_egp,
    coalesce(upper(trim(CAST(payment_purpose AS VARCHAR))), 'P2P')        AS payment_purpose,
    upper(trim(coalesce(CAST(txn_status AS VARCHAR), 'UNKNOWN')))         AS txn_status,
    trim(coalesce(CAST(failure_reason AS VARCHAR), ''))                   AS failure_reason,
    coalesce(upper(trim(CAST(initiation_channel AS VARCHAR))), 'UNKNOWN') AS initiation_channel,
    coalesce(CAST(processing_time_ms AS BIGINT), 0)                       AS processing_time_ms,
    -- IPN SLA: end-to-end ≤ 2000 ms is considered "fast"
    (coalesce(CAST(processing_time_ms AS BIGINT), 0) <= 2000)             AS met_sla,
    CAST(initiated_at AS DATE)                                            AS initiated_date,
    CAST(initiated_at AS TIMESTAMP(6))                                    AS initiated_at,
    coalesce(CAST(completed_at AS TIMESTAMP(6)),
             CAST(initiated_at AS TIMESTAMP(6)))                          AS completed_at,
    current_timestamp(6)                                                  AS _silver_loaded_at
FROM deduplicated
WHERE _row_num = 1
  AND txn_id IS NOT NULL
