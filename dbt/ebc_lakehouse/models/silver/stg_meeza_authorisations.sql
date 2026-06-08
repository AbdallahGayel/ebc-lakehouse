-- dbt/ebc_lakehouse/models/silver/stg_meeza_authorisations.sql
--
-- Silver layer: cleansed Meeza card authorisations (tokenised, no raw PAN).
-- Source: bronze.meeza_authorisations from Flink CDC + upsert-kafka.
-- See stg_ach_transactions for the rationale on dropping Debezium envelope
-- fields (__op / __ts_ms) and using the natural event-time column.
{{ config(
    materialized          = 'incremental',
    incremental_strategy  = 'merge',
    unique_key            = 'auth_id',
    on_schema_change      = 'append_new_columns',
    partitioned_by        = ['day(auth_timestamp)']
) }}

WITH source AS (
    SELECT
        auth_id, card_token, merchant_id, merchant_category, terminal_id,
        txn_type, channel, amount_egp, currency_code, response_code,
        auth_status, issuing_bank_id, acquiring_bank_id, is_international,
        auth_timestamp, settlement_date
    FROM {{ source('bronze', 'meeza_authorisations') }}
    {% if is_incremental() %}
      WHERE auth_timestamp > (
        SELECT coalesce(max(auth_timestamp), TIMESTAMP '1970-01-01 00:00:00')
                 - INTERVAL '1' HOUR
        FROM {{ this }}
      )
    {% endif %}
),

deduplicated AS (
    SELECT *,
           row_number() OVER (
               PARTITION BY auth_id
               ORDER BY auth_timestamp DESC
           ) AS _row_num
    FROM source
)

SELECT
    CAST(auth_id AS VARCHAR)                                              AS auth_id,
    coalesce(CAST(card_token         AS VARCHAR), '')                     AS card_token,
    coalesce(CAST(merchant_id        AS VARCHAR), 'UNKNOWN')              AS merchant_id,
    coalesce(CAST(merchant_category  AS VARCHAR), 'OTHER')                AS merchant_category,
    coalesce(CAST(terminal_id        AS VARCHAR), '')                     AS terminal_id,
    upper(CAST(txn_type AS VARCHAR))                                      AS txn_type,
    upper(trim(CAST(channel AS VARCHAR)))                                 AS channel,
    upper(trim(CAST(auth_status AS VARCHAR)))                             AS auth_status,
    (upper(trim(CAST(auth_status AS VARCHAR))) = 'APPROVED'
     OR CAST(response_code AS VARCHAR) = '00')                            AS is_approved,
    CAST(response_code AS VARCHAR)                                        AS response_code,
    coalesce(CAST(amount_egp AS DECIMAL(18,2)),
             CAST(0 AS DECIMAL(18,2)))                                    AS amount_egp,
    coalesce(CAST(currency_code AS VARCHAR), 'EGP')                       AS currency_code,
    CAST(issuing_bank_id AS VARCHAR)                                      AS issuing_bank_id,
    coalesce(CAST(acquiring_bank_id AS VARCHAR), '')                      AS acquiring_bank_id,
    coalesce(CAST(is_international  AS BOOLEAN), false)                   AS is_international,
    CAST(auth_timestamp AS TIMESTAMP(6))                                  AS auth_timestamp,
    CAST(auth_timestamp AS DATE)                                          AS auth_date,
    -- Flink CDC delivers DATE natively (not Debezium INT32 days-since-epoch);
    -- if settlement_date is NULL fall back to the auth date.
    coalesce(settlement_date, CAST(auth_timestamp AS DATE))               AS settlement_date,
    current_timestamp(6)                                                  AS _silver_loaded_at
FROM deduplicated
WHERE _row_num = 1
  AND auth_id IS NOT NULL
