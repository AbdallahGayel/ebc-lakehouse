-- dbt/ebc_lakehouse/models/serving/scheme_performance.sql
-- Serving layer: BI-optimised copy of Gold's mart_scheme_performance.

{{ config(
    materialized          = 'incremental',
    incremental_strategy  = 'merge',
    unique_key            = ['report_date', 'scheme', 'channel'],
    partitioned_by        = ['month(report_date)'],
    on_schema_change      = 'append_new_columns'
) }}

SELECT
    report_date,
    scheme,
    channel,
    approval_rate_pct,
    avg_processing_ms,
    txn_volume,
    current_timestamp(6) AS refreshed_at
FROM {{ ref('mart_scheme_performance') }}
{% if is_incremental() %}
  WHERE report_date >= current_date - INTERVAL '7' DAY
{% endif %}
