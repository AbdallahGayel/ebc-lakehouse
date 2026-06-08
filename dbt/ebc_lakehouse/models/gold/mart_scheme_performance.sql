-- dbt/ebc_lakehouse/models/gold/mart_scheme_performance.sql
-- Gold: Per-scheme, per-channel approval rates and SLA compliance.
-- Feeds: serving.scheme_performance

{{ config(
    materialized   = 'table',
    partitioned_by = ['month(report_date)']
) }}

WITH meeza_perf AS (
    SELECT
        settlement_date AS report_date,
        'MEEZA'         AS scheme,
        channel,
        count(*)                                                                AS txn_volume,
        round(count_if(is_approved = TRUE) * 100.0 / count(*), 3)               AS approval_rate_pct,
        CAST(0 AS DOUBLE)                                                       AS avg_processing_ms
    FROM {{ ref('stg_meeza_authorisations') }}
    WHERE settlement_date IS NOT NULL
    GROUP BY settlement_date, channel
),

ipn_perf AS (
    SELECT
        initiated_date     AS report_date,
        'IPN-INSTAPAY'     AS scheme,
        initiation_channel AS channel,
        count(*)                                                                AS txn_volume,
        round(count_if(txn_status = 'COMPLETED') * 100.0 / count(*), 3)         AS approval_rate_pct,
        round(avg(CAST(processing_time_ms AS DOUBLE)), 0)                       AS avg_processing_ms
    FROM {{ ref('stg_ipn_transactions') }}
    WHERE initiated_date IS NOT NULL
    GROUP BY initiated_date, initiation_channel
),

wallet_perf AS (
    SELECT
        event_date      AS report_date,
        'MEEZA-DIGITAL' AS scheme,
        channel,
        count(*)                                                                AS txn_volume,
        round(count_if(txn_status = 'COMPLETED') * 100.0 / count(*), 3)         AS approval_rate_pct,
        CAST(0 AS DOUBLE)                                                       AS avg_processing_ms
    FROM {{ ref('stg_meeza_digital_wallet') }}
    WHERE event_date IS NOT NULL
    GROUP BY event_date, channel
),

atm_perf AS (
    SELECT
        session_date      AS report_date,
        '123-ATM-NETWORK' AS scheme,
        'ATM_TERMINAL'    AS channel,
        count(*)                                                                AS txn_volume,
        round(count_if(txn_status = 'APPROVED') * 100.0 / count(*), 3)          AS approval_rate_pct,
        round(avg(CAST(processing_ms AS DOUBLE)), 0)                            AS avg_processing_ms
    FROM {{ ref('stg_atm_sessions') }}
    WHERE session_date IS NOT NULL
    GROUP BY session_date
)

SELECT *, current_timestamp(6) AS _gold_loaded_at FROM meeza_perf  UNION ALL
SELECT *, current_timestamp(6) AS _gold_loaded_at FROM ipn_perf    UNION ALL
SELECT *, current_timestamp(6) AS _gold_loaded_at FROM wallet_perf UNION ALL
SELECT *, current_timestamp(6) AS _gold_loaded_at FROM atm_perf
