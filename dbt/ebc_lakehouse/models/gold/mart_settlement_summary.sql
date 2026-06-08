-- dbt/ebc_lakehouse/models/gold/mart_settlement_summary.sql
-- Gold: Daily net settlement position per bank pair for EG-ACH.
-- Feeds: serving.settlement_summary

{{ config(
    materialized   = 'table',
    partitioned_by = ['month(settlement_date)'],
    tags           = ['gold', 'settlement']
) }}

SELECT
    settlement_date,
    originating_bank_id,
    receiving_bank_id,

    count_if(txn_status = 'SETTLED')                                  AS settled_count,
    count_if(txn_status = 'FAILED')                                   AS failed_count,
    count_if(txn_status = 'RETURNED')                                 AS returned_count,

    sum(amount_egp) FILTER (WHERE txn_status = 'SETTLED')             AS net_settled_egp,
    sum(amount_egp) FILTER (WHERE txn_status = 'FAILED')              AS failed_amount_egp,

    round(count_if(txn_status = 'SETTLED') * 100.0 / count(*), 2)     AS settlement_rate_pct,
    current_timestamp(6)                                              AS _gold_loaded_at

FROM {{ ref('stg_ach_transactions') }}
WHERE settlement_date IS NOT NULL
GROUP BY settlement_date, originating_bank_id, receiving_bank_id
