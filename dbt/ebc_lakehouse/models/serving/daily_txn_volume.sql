-- dbt/ebc_lakehouse/models/serving/daily_txn_volume.sql
-- Serving layer: BI-optimised copy of Gold's mart_daily_txn_volume.
-- Materialised as Iceberg under s3://ebc-lakehouse/serving/daily_txn_volume/
-- Maintained by:
--   • dbt full builds (this model)
--   • Airflow MERGE refresh in ebc_dbt_gold (last-7-days hot window)

{{ config(
    materialized          = 'incremental',
    incremental_strategy  = 'merge',
    unique_key            = ['report_date', 'scheme'],
    partitioned_by        = ['month(report_date)'],
    on_schema_change      = 'append_new_columns'
) }}

SELECT
    report_date,
    scheme,
    txn_count,
    total_amount_egp,
    approved_count,
    failed_count,
    avg_txn_amount_egp,
    current_timestamp(6) AS refreshed_at
FROM {{ ref('mart_daily_txn_volume') }}
{% if is_incremental() %}
  -- Only refresh the rolling window dbt would touch outside its full-rebuild path
  WHERE report_date >= current_date - INTERVAL '7' DAY
{% endif %}
