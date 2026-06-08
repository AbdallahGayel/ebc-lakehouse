-- dbt/ebc_lakehouse/models/gold/mart_daily_txn_volume.sql
-- Gold: Daily transaction volume aggregated across all EBC payment schemes
-- Feeds: serving.daily_txn_volume (MERGE-refreshed serving table)

{{ config(
    materialized = 'table',
    partitioned_by = ['month(report_date)']
) }}

WITH ach AS (
    SELECT
        settlement_date AS report_date,
        'EG-ACH'        AS scheme,
        count(*)                                                AS txn_count,
        sum(amount_egp)                                         AS total_amount_egp,
        count_if(txn_status = 'SETTLED')                        AS approved_count,
        count_if(txn_status IN ('FAILED', 'RETURNED'))          AS failed_count
    FROM {{ ref('stg_ach_transactions') }}
    WHERE settlement_date IS NOT NULL
    GROUP BY settlement_date
),

meeza AS (
    SELECT
        settlement_date AS report_date,
        'MEEZA'         AS scheme,
        count(*)                          AS txn_count,
        sum(amount_egp)                   AS total_amount_egp,
        count_if(is_approved = TRUE)      AS approved_count,
        count_if(is_approved = FALSE)     AS failed_count
    FROM {{ ref('stg_meeza_authorisations') }}
    WHERE settlement_date IS NOT NULL
    GROUP BY settlement_date
),

ipn AS (
    SELECT
        initiated_date AS report_date,
        'IPN-INSTAPAY' AS scheme,
        count(*)                                                AS txn_count,
        sum(amount_egp)                                         AS total_amount_egp,
        count_if(txn_status = 'COMPLETED')                      AS approved_count,
        count_if(txn_status IN ('FAILED', 'REVERSED'))          AS failed_count
    FROM {{ ref('stg_ipn_transactions') }}
    WHERE initiated_date IS NOT NULL
    GROUP BY initiated_date
),

wallet AS (
    SELECT
        event_date      AS report_date,
        'MEEZA-DIGITAL' AS scheme,
        count(*)                                                AS txn_count,
        sum(amount_egp)                                         AS total_amount_egp,
        count_if(txn_status = 'COMPLETED')                      AS approved_count,
        count_if(txn_status IN ('FAILED', 'REJECTED'))          AS failed_count
    FROM {{ ref('stg_meeza_digital_wallet') }}
    WHERE event_date IS NOT NULL
    GROUP BY event_date
),

atm AS (
    SELECT
        session_date      AS report_date,
        '123-ATM-NETWORK' AS scheme,
        count(*)                                                AS txn_count,
        sum(amount_egp)                                         AS total_amount_egp,
        count_if(txn_status = 'APPROVED')                       AS approved_count,
        count_if(txn_status IN ('FAILED', 'DECLINED', 'REJECTED')) AS failed_count
    FROM {{ ref('stg_atm_sessions') }}
    WHERE session_date IS NOT NULL
    GROUP BY session_date
),

combined AS (
    SELECT * FROM ach    UNION ALL
    SELECT * FROM meeza  UNION ALL
    SELECT * FROM ipn    UNION ALL
    SELECT * FROM wallet UNION ALL
    SELECT * FROM atm
)

SELECT
    report_date,
    scheme,
    txn_count,
    total_amount_egp,
    approved_count,
    failed_count,
    round(total_amount_egp / NULLIF(txn_count, 0), 2) AS avg_txn_amount_egp,
    current_timestamp(6)                              AS _gold_loaded_at
FROM combined
