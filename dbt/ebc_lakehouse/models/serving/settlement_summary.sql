-- dbt/ebc_lakehouse/models/serving/settlement_summary.sql
-- Serving layer: BI-optimised copy of Gold's mart_settlement_summary.

{{ config(
    materialized          = 'incremental',
    incremental_strategy  = 'merge',
    unique_key            = ['settlement_date', 'originating_bank_id', 'receiving_bank_id'],
    partitioned_by        = ['month(settlement_date)'],
    on_schema_change      = 'append_new_columns'
) }}

SELECT
    settlement_date,
    originating_bank_id,
    receiving_bank_id,
    settled_count,
    failed_count,
    returned_count,
    net_settled_egp,
    failed_amount_egp,
    settlement_rate_pct,
    current_timestamp(6) AS refreshed_at
FROM {{ ref('mart_settlement_summary') }}
{% if is_incremental() %}
  WHERE settlement_date >= current_date - INTERVAL '7' DAY
{% endif %}
