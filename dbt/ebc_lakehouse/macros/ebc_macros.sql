-- dbt/ebc_lakehouse/macros/ebc_macros.sql

-- 1. Database/Schema Routing Override
-- This ensures gold models go to ebc_gold and silver to ebc_silver
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}

-- 2. Incremental Filter
-- Used for efficient delta loads from Bronze to Silver
{% macro ebc_incremental_filter(ts_field='cdc_event_ts_ms') %}
    {% if is_incremental() %}
        WHERE {{ ts_field }} > (SELECT max({{ ts_field }}) FROM {{ this }})
    {% else %}
        WHERE 1=1
    {% endif %}
{% endmacro %}

-- 3. CDC Deduplication Logic
-- Prefer 'u' (update) over 'c' (create), then sort by timestamp
{% macro ebc_latest_cdc_record(op_field='__op', ts_field='__ts_ms') %}
    multiIf({{ op_field }} = 'u', 0, 1) ASC, {{ ts_field }} DESC
{% endmacro %}

-- 4. CDC Delete Check
{% macro ebc_is_cdc_delete(op_field='__op') %}
    ({{ op_field }} = 'd')
{% endmacro %}

-- 5. PII Hashing (CBE Compliance)
{% macro ebc_hash_pii(column_name) %}
    hex(SHA256(CAST({{ column_name }} AS String)))
{% endmacro %}