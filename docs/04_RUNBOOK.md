# 04 · Runbook

Day-to-day operations: starting the stack, triggering DAGs, debugging the
common failure modes.

---

## 1. Starting the stack

### Full bring-up

```bash
bash scripts/init_all.sh
```

This is the only command you normally need. It enforces the order:

```
init_core.sh        →  Kafka + Schema Registry + MinIO + Polaris + Trino +
                       postgres-meta + Airflow + Flink (incl. first-pass
                       job submission)
init_sources.sh     →  postgres-src + mongodb + mssql (sources must be
                       healthy before Flink can attach)
init_flink.sh       →  re-submit Flink jobs now that sources are reachable
init_governance.sh  →  DataHub + Superset
```

### Per-module

```bash
bash scripts/init_core.sh         # core only
bash scripts/init_sources.sh      # sources only
bash scripts/init_flink.sh        # re-submit Flink jobs
bash scripts/init_governance.sh   # governance only
```

### Plain compose

```bash
docker network create ebc-lakehouse-network --driver bridge   # once
docker compose -f compose/docker-compose.core.yml       up -d
docker compose -f compose/docker-compose.sources.yml    up -d
docker compose -f compose/docker-compose.governance.yml up -d
```

Use this when you're debugging compose itself; the init scripts add
healthcheck gating + waits that you'd otherwise need to re-implement.

## 2. Triggering the medallion DAG chain

```bash
docker exec ebc-airflow-webserver airflow dags trigger ebc_dbt_silver
```

This kicks off the chain — silver → gold → serving — via
`TriggerDagRunOperator`. The silver DAG runs hourly on its own schedule
(`schedule_interval='@hourly'`); manual trigger is only needed for an
out-of-cycle refresh.

Watch progress in the Airflow UI (http://localhost:8085) or:

```bash
for d in ebc_dbt_silver ebc_dbt_gold ebc_dbt_serving; do
  echo "-- $d --"
  docker exec ebc-airflow-webserver airflow dags list-runs -d $d | head -3
done
```

## 3. Generating test traffic

Sources are seeded once at first bring-up (see `postgres/init/`,
`mongodb/init/`, `mssql/init/`). After that the source DBs are idle, so
the bronze freshness gate will eventually fail unless you generate traffic:

```bash
# Continuous synthetic traffic across all 5 source tables
bash scripts/generate_continuous_traffic.sh

# Just seed more Mongo wallet events
bash scripts/seed_wallet_events.sh
```

Both write to the source DBs, which Flink CDC streams into Kafka within
~1 s, which the sink jobs land in Iceberg within one checkpoint (~30 s).

## 4. Tear-down

```bash
# Stop everything, keep volumes (re-up resumes from data on disk)
docker compose down

# Full reset (deletes Kafka segments, Iceberg data, Polaris realm, etc.)
docker compose down -v
docker network rm ebc-lakehouse-network
```

To wipe just one module:

```bash
docker compose -f compose/docker-compose.governance.yml down -v
```

## 5. Common failure modes

### Silver DAG fails on `check_bronze_freshness`

**Symptom:** `Bronze freshness FAILED: bronze.X: lag 6.2h exceeds 6h SLA…`

**Cause:** No source-side activity for > `EBC_BRONZE_SLA_HOURS` (default 6 h).

**Fixes:**

```bash
# A. Generate traffic
bash scripts/generate_continuous_traffic.sh

# B. Raise the SLA temporarily (per the airflow-scheduler env)
docker compose -f compose/docker-compose.core.yml \
    exec airflow-scheduler bash -c 'echo $EBC_BRONZE_SLA_HOURS'
# edit core compose if you want a permanent change
```

The gate has a built-in escape hatch: tables idle > 24 h
(`EBC_BRONZE_HISTORICAL_HOURS`) are treated as backfills and warn rather
than fail.

### Silver dbt run fails: `column __op does not exist`

**Cause:** A stale silver model still references the Debezium envelope.

**Fix:** Re-pull the repo. The v4.0 rewrite (`stg_*.sql` files) replaced
`__op`/`__ts_ms` dedup with natural PK + event-time dedup. See
`dbt/ebc_lakehouse/models/silver/stg_ach_transactions.sql` for the pattern.

### Flink sink jobs FAILED: `NoResourceAvailableException`

**Cause:** TaskManager slot exhaustion. Default is 16 slots; each running
job consumes 1.

**Fix:**

```bash
# How many slots are in use?
curl -s http://localhost:8881/overview | python -m json.tool | grep slots
```

If you've added jobs, bump
`taskmanager.numberOfTaskSlots` in
`compose/docker-compose.core.yml` and force-recreate the TM:

```bash
docker compose -f compose/docker-compose.core.yml up -d --no-deps \
    --force-recreate flink-taskmanager
bash scripts/init_flink.sh    # re-submit jobs (previous cancelled by TM restart)
```

### Flink Iceberg sink commits 0 to Iceberg

**Symptom:** Sink job is RUNNING, Kafka topic has messages, but
`SELECT COUNT(*) FROM iceberg.bronze.X` is 0 and
`SELECT COUNT(*) FROM "iceberg.bronze.X$snapshots"` is 0.

**Cause:** Checkpointing not enabled in the SQL Gateway session. Iceberg
sink only commits on a successful checkpoint.

**Fix:** `scripts/flink/submit_flink_jobs.sh` already sets every required
property in the session header:

```json
{
  "execution.checkpointing.interval": "30s",
  "execution.checkpointing.mode":     "EXACTLY_ONCE",
  "state.backend.type":               "hashmap",
  "state.checkpoints.dir":            "file:///tmp/flink-checkpoints"
}
```

If you ran a job manually via `curl` or the Flink SQL Client, copy that
block into your session POST.

### Flink: `Failed to create directory for shared state`

**Cause:** Checkpoint dir is on the `flink-state` named volume, which is
root-owned; the `flink` user inside the container can't `mkdir` under it.

**Fix:** Already addressed — the default checkpoint dir is
`/tmp/flink-checkpoints` (writable by `flink`). For prod, swap to S3:

```yaml
state.checkpoints.dir: s3a://ebc-lakehouse/checkpoints/
```

…and add the AWS bundle credentials to the JM/TM env.

### Trino `Access Denied: Cannot execute query`

**Cause:** Using a user not in `trino/etc/rules.json`. The default ACL
allows: `root`, `ebc_admin`, `ebc_user`, `ebc_engineer`, `ebc_bi`.

**Fix:** Use one of those usernames. `ebc_user` gets read across `iceberg`
catalogs and is fine for ad-hoc queries:

```bash
docker exec -e JAVA_TOOL_OPTIONS= ebc-trino \
    trino --user ebc_user --catalog iceberg --schema bronze \
    --execute "SELECT COUNT(*) FROM ach_transactions"
```

### Bronze stays empty

Check, in this order:

```bash
# 1. Source DB has rows?
docker exec ebc-postgres-src psql -U ebc_src -d ebc_sources \
    -c "SELECT COUNT(*) FROM ach_transactions"

# 2. Flink CDC source job RUNNING?
curl -s http://localhost:8881/jobs/overview | grep -A1 kafka_ach_sink

# 3. Kafka topic has messages?
docker exec ebc-kafka kafka-get-offsets --bootstrap-server localhost:29092 \
    --topic ebc.public.ach_transactions

# 4. Flink Iceberg sink job RUNNING + checkpointing?
curl -s http://localhost:8881/jobs/overview | grep polaris.bronze.ach
JID=$(...)   # paste the jid
curl -s http://localhost:8881/jobs/$JID/checkpoints | python -m json.tool
```

If (2) is missing, `bash scripts/init_flink.sh` re-submits all jobs. If
(2) is RUNNING but (3) is empty, see "Flink Iceberg sink commits 0"
above for the checkpoint config.

### Polaris won't start

**Cause:** Realm hasn't been bootstrapped (or postgres-polaris is down).

**Fix:**

```bash
# Is the postgres healthy?
docker exec ebc-polaris-postgres pg_isready

# Did the bootstrap exit 0?
docker logs ebc-polaris-bootstrap | tail -20

# If not, re-run the bootstrap:
docker compose -f compose/docker-compose.core.yml up -d --force-recreate \
    polaris-bootstrap polaris polaris-catalog-init
```

### Re-submitting Flink jobs after a SQL edit

```bash
# A. Edit flink/jobs/*.sql
# B. Re-submit (idempotent — cancels existing matching jobs first)
bash scripts/init_flink.sh

# Force-recreate if you also changed flink/Dockerfile:
docker compose -f compose/docker-compose.core.yml build \
    flink-jobmanager flink-taskmanager flink-sql-gateway
docker rm -f ebc-flink-jobmanager ebc-flink-taskmanager ebc-flink-sql-gateway
docker compose -f compose/docker-compose.core.yml up -d --no-deps \
    flink-jobmanager flink-taskmanager flink-sql-gateway
bash scripts/init_flink.sh
```

### Re-bootstrapping the entire data plane

```bash
docker compose down -v                    # delete all volumes
docker network rm ebc-lakehouse-network   # delete the bridge
bash scripts/init_all.sh                  # full clean rebuild
```

This is the only reliable way to recover from a corrupted Polaris realm or
a Kafka cluster ID mismatch.

## 6. Useful operational commands

| Task | Command |
|------|---------|
| List all DAGs | `docker exec ebc-airflow-webserver airflow dags list` |
| Trigger a DAG | `docker exec ebc-airflow-webserver airflow dags trigger <dag_id>` |
| Pause/unpause a DAG | `docker exec ebc-airflow-webserver airflow dags {pause,unpause} <dag_id>` |
| Show last DAG run | `docker exec ebc-airflow-webserver airflow dags list-runs -d <dag_id>` |
| Show a task's last log | `docker exec ebc-airflow-scheduler tail -100 /opt/airflow/logs/dag_id=<>/run_id=<>/task_id=<>/attempt=*.log` |
| List Flink jobs | `curl -s http://localhost:8881/jobs/overview \| python -m json.tool` |
| Cancel a Flink job | `curl -X PATCH "http://localhost:8881/jobs/<jid>?mode=cancel"` |
| Resubmit all Flink jobs | `bash scripts/init_flink.sh` |
| Topic offsets | `docker exec ebc-kafka kafka-get-offsets --bootstrap-server localhost:29092 --topic <topic>` |
| Consume sample | `docker exec ebc-kafka timeout 5 kafka-console-consumer --bootstrap-server localhost:29092 --topic <topic> --from-beginning --max-messages 3` |
| Trino query | `docker exec -e JAVA_TOOL_OPTIONS= ebc-trino trino --user root --catalog iceberg --execute "<sql>"` |
| Force Iceberg compaction | `ALTER TABLE iceberg.<schema>.<table> EXECUTE optimize;` (run via Trino) |
| Expire old snapshots | `ALTER TABLE iceberg.<schema>.<table> EXECUTE expire_snapshots(retention_threshold => '7d');` |
| Polaris namespace list | `curl -H "Authorization: Bearer <token>" -H "Polaris-Realm: POLARIS" http://localhost:8181/api/catalog/v1/ebc_lakehouse/namespaces` |
| End-to-end verify | `bash scripts/verify.sh` |
