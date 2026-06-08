# EBC Lakehouse — Modular Compose

The stack is split into three independently-deployable Docker Compose files
that share a single external bridge network (`ebc-lakehouse-network`). Each
module owns its own services, volumes, and init script.

```
ebc-lakehouse-network        ← shared external bridge (created once)
├── compose/docker-compose.core.yml          # compute · storage · catalog · streaming · orchestration
├── compose/docker-compose.sources.yml       # operational source systems
└── compose/docker-compose.governance.yml    # metadata catalog + BI
```

The root `docker-compose.yml` is a thin `include:` so `docker compose up`
still brings up everything in one command on Compose ≥ 2.20.

---

## 1. Module responsibilities

| Module | Services | Owns |
|--------|----------|------|
| **Core** (`docker-compose.core.yml`) | Kafka (KRaft), Schema Registry, Kafka UI, MinIO, Polaris (postgres + bootstrap + server + catalog-init), Trino, postgres-meta, Airflow (init/scheduler/web), Flink (jobmanager + taskmanager + sql-gateway + job-submitter init) | Compute, storage, Iceberg REST catalog, streaming CDC/ELT, and the metadata DB shared by Airflow + Superset. **No Kafka Connect** — Flink owns every CDC source and every Iceberg sink. |
| **Sources** (`docker-compose.sources.yml`) | postgres-src, mongodb (+ rs0 init), mssql | All operational source systems. Each is healthchecked and addressable by hostname so Flink CDC can reach it. MS SQL Server replaces the retired Cassandra source. |
| **Governance** (`docker-compose.governance.yml`) | Redis, Superset (init / web / worker), DataHub (mysql, elasticsearch, gms, system update, frontend, actions), OpenLineage→DataHub bridge | Metadata cataloguing + BI. Connects across the network to `trino`, `kafka`, and `postgres-meta` in Core. |

The Core module is the only one that must always be running. Sources is
needed whenever you want fresh CDC into Bronze; Governance is purely
consumer-side and can be brought up/down without disturbing data flow.

---

## 2. Startup order

The init scripts encode the exact dependency order; pick whichever fits
your workflow:

### One-shot (recommended)

```bash
bash scripts/init_all.sh
```

Brings up Core → Sources → Flink (re-submit) → Governance, waiting for
healthchecks between stages. The Flink job re-submit happens *after*
Sources are healthy so the CDC connectors can actually attach to
`postgres-src`, `mongodb`, and `mssql`.

### Module by module

```bash
bash scripts/init_core.sh        # Core stack + first-pass Flink job submit
bash scripts/init_sources.sh     # Source DBs (postgres-src, mongodb, mssql)
bash scripts/init_flink.sh       # Re-submit Flink jobs (sources reachable now)
bash scripts/init_governance.sh  # DataHub + Superset
```

### Plain compose (no orchestration, no waiting)

```bash
docker network create ebc-lakehouse-network --driver bridge   # once
docker compose -f compose/docker-compose.core.yml       up -d
docker compose -f compose/docker-compose.sources.yml    up -d
docker compose -f compose/docker-compose.governance.yml up -d
```

Or with the root include:

```bash
docker compose up -d        # brings up all three modules
```

### Detailed dependency order inside each module

**Core**

1. `kafka` (KRaft self-formats `meta.properties` on first boot)
2. `schema-registry`
3. `minio` → `minio-init` (creates `ebc-lakehouse` bucket + medallion prefixes)
4. `polaris-postgres` → `polaris-bootstrap` (seeds realm) → `polaris` →
   `polaris-catalog-init` (creates `ebc_lakehouse` catalog + namespaces + role)
5. `trino` + `trino-init` (creates Iceberg schemas + serving DDL +
   `pipeline_metrics` operational table)
6. `postgres-meta` → `airflow-init` → `airflow-scheduler` + `airflow-webserver`
7. `flink-jobmanager` → `flink-taskmanager` (16 slots) →
   `flink-sql-gateway` → `flink-init` (submits every `flink/jobs/*.sql` —
   each runs as a long-lived streaming job)
8. `kafka-ui` (cosmetic)

**Sources**

1. `postgres-src` (logical decoding + `wal_level=logical`; seed SQL applied
   from `postgres/init/`)
2. `mongodb` → `mongodb-init` (initialises `rs0` — required by mongodb-cdc)
3. `mssql` → `mssql-init` (enables Agent + creates CDC capture instances on
   `atm_telemetry.dbo.atm_sessions`)

**Governance**

1. `datahub-mysql` + `datahub-elasticsearch`
2. `datahub-gms` (runs Ebean DDL on first start — wait up to 15 min cold)
3. `datahub-upgrade` (SystemUpdate provisions Kafka topics + root user)
4. `datahub-frontend`, `datahub-actions`, `ol-datahub-bridge`
5. `redis`
6. `superset-init` (Alembic migrations + admin user + Trino DB registration)
7. `superset` + `superset-worker`

---

## 3. Compute / catalog contract

Every module agrees on the same Trino + Polaris coordinates:

| Property | Value |
|----------|-------|
| Trino host | `trino:8080` (intra-network) / `localhost:8080` (host) |
| Trino catalog | `iceberg` (Trino-side name for the Polaris-backed Iceberg connector) |
| Polaris REST | `http://polaris:8181/api/catalog` |
| Polaris mgmt | `http://polaris:8181/api/management/v1` |
| Polaris catalog | `ebc_lakehouse` |
| Polaris realm | `POLARIS` |
| Auth | OAuth2 client_credentials · `root` / `s3cr3t` · scope `PRINCIPAL_ROLE:ALL` |
| Storage root | `s3://ebc-lakehouse/` on MinIO at `http://minio:9000` |
| Medallion namespaces | `bronze`, `silver`, `gold`, `serving` |
| Flink JobManager | `http://flink-jobmanager:8081` (intra) / `http://localhost:8881` (host) |
| Flink SQL Gateway | `http://flink-sql-gateway:8083` (intra) / `http://localhost:8883` (host) |

dbt-trino, Airflow's `SQLExecuteQueryOperator`, Superset, DataHub
(`recipes/trino.yaml` and `recipes/iceberg.yaml`), and Flink's Iceberg
connector all read these same values — change them in one place and
propagate via `.env` + the compose `environment:` blocks.

---

## 4. Extensibility

### Add a new data source

1. Append a service block to `compose/docker-compose.sources.yml` that:
   - joins the default external network (inherited automatically),
   - declares a stable hostname (Flink CDC connector configs use it),
   - has a `healthcheck` so `init_sources.sh` can gate on it,
   - mounts its bootstrap SQL/scripts under a host-mounted init directory.
2. Drop a Flink CDC job into `flink/jobs/1N_cdc_<name>.sql` that:
   - declares a `CREATE TEMPORARY TABLE … WITH ('connector' = '<x>-cdc', …)`
     against the new source hostname,
   - declares a matching `upsert-kafka` sink with the
     `ebc.<schema>.<table>` topic naming convention,
   - ends with an `INSERT INTO … SELECT * FROM …` that wires them.
3. Drop a sink job into `flink/jobs/2N_sink_<name>.sql` that creates the
   Iceberg target table under `polaris.bronze.<table>` and INSERTs from
   the Kafka topic.
4. Add a dbt source entry to `dbt/ebc_lakehouse/models/sources.yml` and a
   `stg_<table>.sql` silver model.
5. Re-run `scripts/init_sources.sh` then `scripts/init_flink.sh`. Both are
   idempotent and only touch the new objects.

### Add a new BI / consumer tool

1. Append a service block to `compose/docker-compose.governance.yml`. It
   should:
   - join the default external network,
   - connect to Trino at `trino:8080` using the tool's native dialect,
   - own its own metadata DB volume (don't piggy-back on `superset-data`).
2. (Optional) Pre-register Trino as a data source via the tool's bootstrap
   mechanism — model after `superset/register_databases.py`.
3. Add a healthcheck and wire it into `scripts/init_governance.sh` if you
   want it gated.

### Add a new compute engine (e.g., Spark)

1. Append the engine to `compose/docker-compose.core.yml`.
2. Mount or generate an Iceberg / Polaris REST catalog config that points
   at `http://polaris:8181/api/catalog` with the same OAuth2 credentials.
3. Grant the engine's principal the `CATALOG_MANAGE_CONTENT` privilege via
   `polaris/catalog-init/init_polaris.py` (or extend that script with a new
   principal role).

### Add a new DataHub metadata source

1. Drop a recipe into `recipes/<name>.yaml` (model after `recipes/trino.yaml`).
2. Invoke `datahub ingest -c recipes/<name>.yaml` from inside
   `ebc-datahub-actions` or any container that has `acryl-datahub` installed.

---

## 5. Tear-down

```bash
# Stop a single module (preserves volumes)
docker compose -f compose/docker-compose.governance.yml down

# Stop everything
docker compose down

# Stop everything + delete persistent volumes
docker compose down -v

# Drop the shared network too
docker network rm ebc-lakehouse-network
```

---

## 6. Troubleshooting

| Symptom | Fix |
|---------|-----|
| Kafka container fails with "missing meta.properties" | KRaft storage not formatted. Wipe the `kafka-data` volume — the Confluent image auto-formats on first boot using `CLUSTER_ID`. |
| Polaris won't start | Check `polaris-postgres` healthy and that `polaris-bootstrap` exited 0. The server refuses to boot against an un-bootstrapped realm. |
| Flink sink "Failed to create directory for shared state" | The named `flink-state` volume is root-owned and the `flink` user can't `mkdir` under it. Checkpoint dir defaults to `/tmp/flink-checkpoints` (container-local, writable) — fine for dev. For prod, switch to S3/MinIO checkpoint storage. |
| Flink `NoResourceAvailableException` | TaskManager slot exhaustion. Bump `taskmanager.numberOfTaskSlots` in `compose/docker-compose.core.yml` (default 16) and force-recreate the TM. |
| Iceberg-Flink "Failed to load Hadoop classes" | The `flink-shaded-hadoop-2-uber` JAR is missing from `/opt/flink/lib`. Rebuild the Flink image (`docker compose -f compose/docker-compose.core.yml build flink-jobmanager flink-taskmanager flink-sql-gateway`) and force-recreate the containers. |
| Trino "catalog iceberg not found" | Trino reads `/etc/trino/catalog/iceberg.properties` at boot. Confirm the volume mount and `docker compose -f compose/docker-compose.core.yml restart trino`. |
| Flink CDC can't reach `postgres-src` / `mongodb` / `mssql` | Sources module is not running, or the Flink job uses `localhost` instead of the container hostname. Always use container hostnames. Re-run `scripts/init_flink.sh` after `scripts/init_sources.sh`. |
| Superset "no module named trino" | Image not rebuilt after the ClickHouse → Trino switch. `docker compose -f compose/docker-compose.governance.yml build superset` and re-up. |
| DataHub ingestion sees no Trino schemas | Trino's `iceberg.properties` needs `iceberg.rest-catalog.vended-credentials-enabled=true` and the ingesting principal must have `CATALOG_MANAGE_CONTENT`. |
