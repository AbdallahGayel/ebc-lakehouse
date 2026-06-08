-- =============================================================================
-- macros/generic_tests.sql
-- Generic dbt tests for Iceberg Silver/Gold models queried via Trino.
-- =============================================================================

{#
  unique_final
  ------------
  Iceberg upserts (dbt-trino incremental_strategy='merge') leave the latest
  version of a row in place and tombstone the rest, so a plain SELECT already
  sees deduplicated state. We keep the test name `unique_final` for backward
  compatibility with existing schema.yml references — the implementation is
  now a straight uniqueness assertion against the materialised Iceberg table.
#}
{% test unique_final(model, column_name) %}

SELECT
    {{ column_name }},
    count(*) AS occurrences
FROM {{ model }}
WHERE {{ column_name }} IS NOT NULL
GROUP BY {{ column_name }}
HAVING count(*) > 1

{% endtest %}
