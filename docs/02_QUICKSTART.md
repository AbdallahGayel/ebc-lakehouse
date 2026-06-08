# 02 · Quickstart

Get from `git clone` to a live medallion pipeline in ~10 minutes (≈ 30 min
on a cold cache while images pull).

---

## 1. One-shot bring-up

```bash
bash scripts/init_all.sh
```

That runs, in order: `init_core.sh` → `init_sources.sh` → `init_flink.sh` →
`init_governance.sh`, waiting for each stage's healthchecks before moving on.

When it finishes you'll see:

```
[ OK ] Trino         →  http://localhost:8080
[ OK ] Flink UI      →  http://localhost:8881
[ OK ] Polaris       →  http://localhost:8181/api/catalog
[ OK ] Airflow       →  http://localhost:8085   (admin / admin)
[ OK ] Superset      →  http://localhost:8088   (admin / admin)
[ OK ] DataHub       →  http://localhost:9002   (datahub / datahub)
[ OK ] MinIO         →  http://localhost:9001   (minioadmin / minioadmin)
[ OK ] MS SQL Server →  localhost:1433          (sa / EbcAtm_S3cret!)
[ OK ] Kafka UI      →  http://localhost:8090
```

## 2. Confirm Bronze is being populated

The Flink CDC jobs do a snapshot of every source on first start and then
stream from the binlog/oplog/CDC tables. Within ~60 s of `init_all.sh`
finishing, every Bronze table should have rows:

```bash
# Bronze row counts via Trino
for t in ach_transactions meeza_authorisations ipn_transactions \
         meeza_digital_wallet_events atm_sessions; do
  docker exec -e JAVA_TOOL_OPTIONS= ebc-trino \
    trino --user root --catalog iceberg --schema bronze \
    --execute "SELECT '$t' AS tbl, COUNT(*) AS rows FROM $t"
done
```

Expected (matching the seeded data):

| Table | Rows |
|---|---:|
| `ach_transactions` | 2,000 |
| `meeza_authorisations` | 4,000 |
| `ipn_transactions` | 6,000 |
| `meeza_digital_wallet_events` | 500 |
| `atm_sessions` | 20 |

If any are zero, see `04_RUNBOOK.md → Bronze stays empty`.

## 3. Trigger the medallion DAG chain

The three Airflow DAGs are wired silver → gold → serving via
`TriggerDagRunOperator`:

```bash
docker exec ebc-airflow-webserver airflow dags trigger ebc_dbt_silver
```

Then watch the run progress in the Airflow UI (http://localhost:8085) or:

```bash
docker exec ebc-airflow-webserver airflow dags list-runs -d ebc_dbt_silver
```

Within ~2 minutes all three DAGs should report `success`. Verify the
silver / gold / serving layers were populated:

```bash
for layer in silver gold serving; do
  echo "=== $layer ==="
  docker exec -e JAVA_TOOL_OPTIONS= ebc-trino \
    trino --user root --catalog iceberg --schema "$layer" \
    --execute "SHOW TABLES"
done
```

## 4. Run a real query

Open Trino UI at http://localhost:8080 (login as `ebc_user`, no password)
and run:

```sql
SELECT scheme, sum(txn_count) AS total_txns, sum(approved_count) AS approved
FROM   iceberg.serving.daily_txn_volume
GROUP  BY scheme
ORDER  BY scheme;
```

Expected (from the seeded data):

| scheme | total_txns | approved |
|---|---:|---:|
| EG-ACH | 500 | 284 |
| IPN-INSTAPAY | 6,000 | 3,676 |
| MEEZA | 4,000 | 2,485 |
| MEEZA-DIGITAL | 500 | 288 |

## 5. End-to-end validation

```bash
bash scripts/verify.sh
```

That walks every layer (Bronze → Silver → Gold → Serving), compares
counts, checks freshness, and verifies dbt tests are passing. See
`05_VALIDATION.md` for what each check means and how to interpret the
output.

## 6. (Optional) Generate continuous CDC traffic

The seeded data is a single snapshot. To see the pipeline react to new
events in real time:

```bash
bash scripts/generate_continuous_traffic.sh
```

That script writes synthetic transactions to all 5 source tables every
few seconds. Watch Bronze counts grow in Kafka UI and Trino:

```bash
# Topic offsets (Kafka)
docker exec ebc-kafka kafka-get-offsets --bootstrap-server localhost:29092 \
    --topic ebc.public.ach_transactions
```

## 7. Tear-down

```bash
docker compose down              # stop everything, keep data
docker compose down -v           # stop + delete volumes (full reset)
docker network rm ebc-lakehouse-network   # remove the shared bridge
```

For per-module ops, see `compose/README.md`.
