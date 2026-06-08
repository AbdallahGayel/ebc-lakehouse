# 05 · Validation & Testing

End-to-end verification: confirm every layer (Bronze → Silver → Gold →
Serving) is consistent, fresh, and producing data the BI tools can trust.

---

## 1. One-shot validator

```bash
bash scripts/verify.sh
```

That script walks every check below in order and prints a green/yellow/red
summary. Use it after `init_all.sh` and after any DAG run.

The remainder of this document describes what each check does and how to
interpret unexpected output.

---

## 2. Health gates (per layer)

### 2.1 Bronze (Flink CDC + sink)

| Check | Query / command | Expected |
|-------|-----------------|----------|
| 5 source jobs RUNNING | `curl -s http://localhost:8881/jobs/overview` | 5 jobs whose name contains `kafka_*_sink` |
| 5 sink jobs RUNNING | same | 5 jobs whose name starts with `insert-into_polaris.bronze.` |
| 6 Kafka topics created | `docker exec ebc-kafka kafka-topics --bootstrap-server localhost:29092 --list` | 5 × `ebc.<schema>.<table>` + `_schemas` + `__consumer_offsets` |
| 5 Iceberg tables registered | `curl -H "..." http://localhost:8181/api/catalog/v1/ebc_lakehouse/namespaces/bronze/tables` | 5 identifiers |
| Each table has ≥ 1 snapshot | `SELECT COUNT(*) FROM "iceberg.bronze.X$snapshots"` | ≥ 1 |
| Row counts match source | see "Row alignment" below | exact match after first checkpoint |

### 2.2 Silver / Gold / Serving (dbt + Trino)

| Check | Query / command | Expected |
|-------|-----------------|----------|
| Latest DAG run succeeded | `airflow dags list-runs -d ebc_dbt_<layer>` | `success` |
| All models built | `SHOW TABLES IN iceberg.<layer>` | full table list |
| dbt tests passing | `docker exec ebc-airflow-scheduler bash -c "cd /opt/airflow/dbt/ebc_lakehouse && dbt test --select tag:<layer> --profiles-dir ."` | 0 FAIL |
| Iceberg maintenance applied | `SELECT count(*) FROM "iceberg.<layer>.<table>$snapshots"` | ≥ 1 recent (maintenance ran in DAG TaskGroup) |
| `_*_loaded_at` columns recent | `SELECT max(_silver_loaded_at) FROM iceberg.silver.stg_ach_transactions` | within last DAG run window |

---

## 3. Row alignment

The most useful end-to-end invariant: **each layer's row count must match
the source for the seeded data + match the others within ±1 dedup
window.**

```sql
WITH counts AS (
  SELECT 'bronze.ach_transactions' AS tbl, COUNT(*) AS rows FROM iceberg.bronze.ach_transactions UNION ALL
  SELECT 'silver.stg_ach_transactions',     COUNT(*) FROM iceberg.silver.stg_ach_transactions UNION ALL
  SELECT 'bronze.meeza_authorisations',     COUNT(*) FROM iceberg.bronze.meeza_authorisations UNION ALL
  SELECT 'silver.stg_meeza_authorisations', COUNT(*) FROM iceberg.silver.stg_meeza_authorisations UNION ALL
  SELECT 'bronze.ipn_transactions',         COUNT(*) FROM iceberg.bronze.ipn_transactions UNION ALL
  SELECT 'silver.stg_ipn_transactions',     COUNT(*) FROM iceberg.silver.stg_ipn_transactions UNION ALL
  SELECT 'bronze.meeza_digital_wallet_events', COUNT(*) FROM iceberg.bronze.meeza_digital_wallet_events UNION ALL
  SELECT 'silver.stg_meeza_digital_wallet',  COUNT(*) FROM iceberg.silver.stg_meeza_digital_wallet UNION ALL
  SELECT 'bronze.atm_sessions',             COUNT(*) FROM iceberg.bronze.atm_sessions UNION ALL
  SELECT 'silver.stg_atm_sessions',         COUNT(*) FROM iceberg.silver.stg_atm_sessions
)
SELECT * FROM counts ORDER BY tbl;
```

For the seeded data:

| Table | Bronze | Silver |
|---|---:|---:|
| ach_transactions | 2,000 | 2,000 |
| meeza_authorisations | 4,000 | 4,000 |
| ipn_transactions | 6,000 | 6,000 |
| meeza_digital_wallet_events | 500 | 500 |
| atm_sessions | 20 | 20 |

If Silver < Bronze by a small amount, that's normal — a dbt MERGE drops
rows that fail `unique_key` (`txn_id IS NOT NULL`, `_id IS NOT NULL`).
Drift > 1 % is a smell; check the silver model's `WHERE _row_num = 1`
clauses.

## 4. Freshness gate

```sql
SELECT 'bronze.ach_transactions' AS tbl,
       max(created_at) AS last_event,
       date_diff('second', max(created_at), current_timestamp) / 60.0 AS lag_min
FROM   iceberg.bronze.ach_transactions
UNION ALL
SELECT 'bronze.meeza_authorisations',    max(auth_timestamp),  date_diff('second', max(auth_timestamp),  current_timestamp) / 60.0 FROM iceberg.bronze.meeza_authorisations UNION ALL
SELECT 'bronze.ipn_transactions',        max(initiated_at),    date_diff('second', max(initiated_at),    current_timestamp) / 60.0 FROM iceberg.bronze.ipn_transactions UNION ALL
SELECT 'bronze.meeza_digital_wallet_events', max(event_ts),    date_diff('second', max(event_ts),        current_timestamp) / 60.0 FROM iceberg.bronze.meeza_digital_wallet_events UNION ALL
SELECT 'bronze.atm_sessions',            max(updated_at),      date_diff('second', max(updated_at),      current_timestamp) / 60.0 FROM iceberg.bronze.atm_sessions;
```

This is the same query the silver DAG's `check_bronze_freshness` task
runs. Acceptable lag depends on traffic:

| Mode | Acceptable lag |
|------|----------------|
| Continuous traffic generator running | < 2 min |
| Seeded only (no live source writes) | unbounded — `EBC_BRONZE_HISTORICAL_HOURS` (24 h) kicks the row into "historical" and warns rather than fails |
| Production target | < 1 min |

## 5. Pipeline metrics

```sql
SELECT source_topic,
       max(recorded_at)  AS last_run,
       max(bronze_row_count) AS bronze,
       max(silver_row_count) AS silver,
       max(silver_row_count) - max(bronze_row_count) AS drift
FROM   iceberg.serving.pipeline_metrics
WHERE  batch_date = current_date
GROUP  BY source_topic
ORDER  BY source_topic;
```

`pipeline_metrics` is written by the silver DAG's `record_pipeline_metrics`
task on every run. `drift` should be 0 or slightly negative (silver dedupe
drops bad rows); positive drift means silver is **adding** rows, which is
a bug.

## 6. dbt tests

The dbt project ships with `unique_final`, `not_null`, `accepted_values`,
and `accepted_range` tests on every silver / gold / serving model. The
DAGs run them in the `dbt_test_<layer>` task with `trigger_rule='all_done'`
— failures are surfaced in the task log but don't block downstream
maintenance.

Run them ad-hoc:

```bash
docker exec ebc-airflow-scheduler bash -c \
    "cd /opt/airflow/dbt/ebc_lakehouse && \
     dbt test --select tag:silver --profiles-dir ."
```

Expected: every test passes for the seeded data. Failures usually mean a
silver model is producing duplicates (check `_row_num = 1` filter) or a
source row contains a value outside the `accepted_values` whitelist
(extend the test or the silver mapping).

## 7. Iceberg integrity

```sql
-- One snapshot per layer per recent maintenance pass
SELECT '<table>' AS table_name,
       count(*)               AS snapshots,
       max(committed_at)      AS latest_snapshot,
       max(committed_at) - min(committed_at) AS snapshot_span
FROM   "iceberg.<schema>.<table>$snapshots";

-- Manifest count should stay low (compaction keeps it tidy)
SELECT '<table>' AS t,
       count(*) AS manifest_files
FROM   "iceberg.<schema>.<table>$manifests";

-- Total data file count + bytes
SELECT count(*)                 AS data_files,
       sum(file_size_in_bytes)  AS total_bytes
FROM   "iceberg.<schema>.<table>$files";
```

Healthy after a few DAG cycles:
- `snapshots` should be < 10 (expire_snapshots keeps the tail trimmed).
- `manifest_files` should be 1–3 per table (optimize compacts on each run).
- `data_files` × `total_bytes` should align with the row count × ~200 B/row.

If `manifest_files` grows linearly with DAG runs, the `optimize` task in
the maintenance TaskGroup failed silently — check the DAG task log.

## 8. Polaris + MinIO

```bash
# Polaris namespaces present?
TOK=$(curl -s -X POST http://localhost:8181/api/catalog/v1/oauth/tokens \
      -d 'grant_type=client_credentials&client_id=root&client_secret=s3cr3t&scope=PRINCIPAL_ROLE:ALL' \
      -H 'Polaris-Realm: POLARIS' | jq -r '.access_token')
curl -s -H "Authorization: Bearer $TOK" -H 'Polaris-Realm: POLARIS' \
     http://localhost:8181/api/catalog/v1/ebc_lakehouse/namespaces | jq .
# expect: bronze, silver, gold, serving

# MinIO bucket contents
docker exec ebc-minio mc ls --recursive local/ebc-lakehouse | head -20
# expect prefixes: bronze/*, silver/*, gold/*, serving/*
```

## 9. DataHub catalog

```bash
# Trigger ingestion (Trino + Iceberg + Kafka + dbt + Airflow)
docker exec ebc-datahub-actions datahub ingest -c /etc/datahub/recipes/trino.yaml
docker exec ebc-datahub-actions datahub ingest -c /etc/datahub/recipes/iceberg.yaml
docker exec ebc-datahub-actions datahub ingest -c /etc/datahub/recipes/dbt.yaml

# Then open http://localhost:9002 and search for "iceberg.bronze.ach_transactions"
```

## 10. Continuous validation in CI

A minimal post-deploy check suitable for a CI pipeline:

```bash
bash scripts/init_all.sh
docker exec ebc-airflow-webserver airflow dags trigger ebc_dbt_silver
# Wait for the chain (silver → gold → serving) to finish:
until [ "$(docker exec ebc-airflow-webserver airflow dags list-runs \
            -d ebc_dbt_serving | awk -F'|' '/^ebc_dbt_serving/ \
            {print $3}' | head -1 | xargs)" = "success" ]; do sleep 10; done
bash scripts/verify.sh
```
