-- dbt/ebc_lakehouse/models/silver/stg_meeza_digital_wallet.sql
--
-- Silver layer: cleansed Meeza Digital wallet events (extracted from MongoDB).
-- Source: bronze.meeza_digital_wallet_events from Flink CDC (mongodb-cdc) +
-- upsert-kafka. The Mongo PK `_id` flows through unchanged as document_id.
{{ config(
    materialized          = 'incremental',
    incremental_strategy  = 'merge',
    unique_key            = 'document_id',
    on_schema_change      = 'append_new_columns',
    partitioned_by        = ['day(event_timestamp)']
) }}

WITH source AS (
    SELECT
        _id, wallet_id, event_type, channel, issuing_bank_id,
        amount_egp, status, event_ts
    FROM {{ source('bronze', 'meeza_digital_wallet_events') }}
    {% if is_incremental() %}
      WHERE event_ts > (
        SELECT coalesce(max(event_timestamp), TIMESTAMP '1970-01-01 00:00:00')
                 - INTERVAL '1' HOUR
        FROM {{ this }}
      )
    {% endif %}
),

deduplicated AS (
    SELECT *,
           row_number() OVER (
               PARTITION BY _id
               ORDER BY event_ts DESC
           ) AS _row_num
    FROM source
)

SELECT
    CAST(_id AS VARCHAR)                                              AS document_id,
    coalesce(CAST(wallet_id AS VARCHAR), 'N/A')                       AS wallet_id,
    upper(CAST(event_type AS VARCHAR))                                AS txn_type,
    upper(CAST(channel    AS VARCHAR))                                AS channel,
    CAST(issuing_bank_id AS VARCHAR)                                  AS issuing_bank_id,
    coalesce(CAST(amount_egp AS DECIMAL(18,2)),
             CAST(0 AS DECIMAL(18,2)))                                AS amount_egp,
    upper(CAST(status AS VARCHAR))                                    AS txn_status,
    CAST(event_ts AS TIMESTAMP(6))                                    AS event_timestamp,
    CAST(event_ts AS DATE)                                            AS event_date,
    current_timestamp(6)                                              AS _silver_loaded_at
FROM deduplicated
WHERE _row_num = 1
  AND _id IS NOT NULL
