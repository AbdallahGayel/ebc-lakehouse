-- dbt/ebc_lakehouse/models/silver/stg_atm_sessions.sql
--
-- Silver layer: cleansed ATM network sessions.
-- Source: bronze.atm_sessions from Flink CDC (sqlserver-cdc) + upsert-kafka.
-- The legacy Cassandra source has been retired; SQL Server CDC capture
-- instances feed this stream now (see flink/jobs/12_cdc_sqlserver.sql).
{{ config(
    materialized          = 'incremental',
    incremental_strategy  = 'merge',
    unique_key            = ['atm_id', 'session_id'],
    on_schema_change      = 'append_new_columns',
    partitioned_by        = ['day(session_date)']
) }}

WITH source AS (
    SELECT
        atm_id, session_date, session_id, session_ts,
        card_token, issuing_bank_id, txn_type,
        amount_egp, currency_code, response_code, status,
        processing_ms, created_at, updated_at
    FROM {{ source('bronze', 'atm_sessions') }}
    {% if is_incremental() %}
      WHERE updated_at > (
        SELECT coalesce(max(updated_ts), TIMESTAMP '1970-01-01 00:00:00')
                 - INTERVAL '1' HOUR
        FROM {{ this }}
      )
    {% endif %}
),

deduplicated AS (
    SELECT *,
           row_number() OVER (
               PARTITION BY atm_id, session_id
               ORDER BY updated_at DESC
           ) AS _row_num
    FROM source
)

SELECT
    coalesce(CAST(atm_id     AS VARCHAR), 'UNKNOWN')                  AS atm_id,
    coalesce(CAST(session_id AS VARCHAR), 'UNKNOWN')                  AS session_id,
    coalesce(session_date, DATE '1970-01-01')                         AS session_date,
    coalesce(CAST(session_ts AS TIMESTAMP(6)),
             TIMESTAMP '1970-01-01 00:00:00')                         AS session_timestamp,
    coalesce(CAST(card_token      AS VARCHAR), 'UNKNOWN')             AS card_token,
    coalesce(CAST(issuing_bank_id AS VARCHAR), 'UNKNOWN')             AS issuing_bank_id,
    upper(coalesce(CAST(txn_type AS VARCHAR), 'UNKNOWN'))             AS txn_type,
    coalesce(CAST(amount_egp AS DECIMAL(18,2)),
             CAST(0 AS DECIMAL(18,2)))                                AS amount_egp,
    coalesce(CAST(currency_code AS VARCHAR), 'EGP')                   AS currency_code,
    coalesce(CAST(response_code AS VARCHAR), 'UNKNOWN')               AS response_code,
    upper(coalesce(CAST(status AS VARCHAR), 'UNKNOWN'))               AS txn_status,
    coalesce(CAST(processing_ms AS BIGINT), 0)                        AS processing_ms,
    coalesce(CAST(created_at AS TIMESTAMP(6)),
             TIMESTAMP '1970-01-01 00:00:00')                         AS created_ts,
    coalesce(CAST(updated_at AS TIMESTAMP(6)),
             TIMESTAMP '1970-01-01 00:00:00')                         AS updated_ts,
    current_timestamp(6)                                              AS _silver_loaded_at
FROM deduplicated
WHERE _row_num = 1
