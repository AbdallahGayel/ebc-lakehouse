-- models/semantic/mf_time_spine.sql
-- MetricFlow time spine: one row per calendar day 2020-01-01 … 2030-12-31.
-- time_spine config is declared in _schema.yml (dbt 1.9+ YAML approach).
-- Trino: SEQUENCE() generates the day series; UNNEST flattens it into rows.

{{ config(
    materialized = 'table',
    schema       = 'serving',
    tags         = ['semantic', 'metricflow', 'time-spine']
) }}

SELECT day AS date_day
FROM   UNNEST(
           SEQUENCE(DATE '2020-01-01', DATE '2030-12-31', INTERVAL '1' DAY)
       ) AS t(day)
