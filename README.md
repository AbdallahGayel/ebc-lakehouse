# EBC Medallion Lakehouse
## Flink CDC + Kafka + Iceberg + MinIO + Polaris + Trino + dbt + Airflow + Superset + DataHub

**Egyptian Banks Company (EBC) · National Payment Infrastructure**
**v4.0 — Flink CDC edition (Kafka Connect / Debezium / Cassandra / ClickHouse retired)**

---

## Architecture

```
PostgreSQL · MongoDB · MS SQL Server      (sources)
  │  wal_level=logical · MongoDB rs0 · MSSQL Agent + CDC capture instances
  ▼
Apache Flink 1.20  ·  postgres-cdc · mongodb-cdc · sqlserver-cdc
  │  Watermarks · upsert mode (equality-field-columns)
  ▼
Apache Kafka 3.7 (KRaft)  ·  Confluent Schema Registry (Avro)
  │  ebc.<schema>.<table> topics · 6 partitions · upsert-kafka format
  ▼
Apache Flink — Iceberg sink jobs  →  Apache Polaris (Iceberg REST)
  │  Snappy Parquet on MinIO · upsert · checkpoint every 30s (EXACTLY_ONCE)
  │  OAuth2 client_credentials (root / s3cr3t · scope PRINCIPAL_ROLE:ALL)
  ▼
Apache Iceberg Lakehouse on MinIO  (Source of Truth)
  ├── BRONZE   bronze.*    ← latest-state-per-PK, written by Flink
  ├── SILVER   silver.*    ← dbt-trino incremental MERGE, ebc_dbt_silver DAG
  ├── GOLD     gold.*      ← dbt-trino full-refresh marts, ebc_dbt_gold DAG
  └── SERVING  serving.*   ← dbt-trino incremental MERGE, ebc_dbt_serving DAG
  ▼
Trino 455  (SQL compute over Iceberg)
  │  iceberg connector → Polaris REST · native S3 file system
  │  dbt-trino · Airflow SQLExecuteQueryOperator · BI queries
  ▼
Apache Superset 4.1  +  DataHub 1.5  +  OpenLineage
```

**Design principles:**
- **Trino** is the *compute*; **Polaris** is the *catalog*; **MinIO** is the *storage*.
- **Flink** owns every streaming path — both CDC source (DB → Kafka) and sink
  (Kafka → Iceberg). Kafka Connect / Debezium have been removed.
- Every layer of the medallion is a first-class Iceberg table.
- Source DBs are exactly three (Postgres / Mongo / MSSQL) so every supported
  CDC pattern (logical-decoding / change-stream / CDC-capture-instance) is
  exercised end-to-end.

---

## Services & Ports

| Service                 | URL                                       | Credentials                |
|-------------------------|-------------------------------------------|----------------------------|
| Airflow                 | http://localhost:8085                     | admin / admin              |
| Superset                | http://localhost:8088                     | admin / admin              |
| Trino UI                | http://localhost:8080                     | user: `ebc_user` (no auth) |
| Flink UI (JobManager)   | http://localhost:8881                     | —                          |
| Flink SQL Gateway       | http://localhost:8883                     | —                          |
| Polaris (Iceberg REST)  | http://localhost:8181/api/catalog         | OAuth2 root / s3cr3t       |
| Polaris (Management)    | http://localhost:8181/api/management/v1   | OAuth2 root / s3cr3t       |
| Polaris (Health)        | http://localhost:8182/q/health            | —                          |
| MinIO Console           | http://localhost:9001                     | minioadmin / minioadmin    |
| Kafka UI                | http://localhost:8090                     | —                          |
| Schema Registry         | http://localhost:8081                     | —                          |
| DataHub                 | http://localhost:9002                     | datahub / datahub          |
| MS SQL Server           | localhost:1433                            | sa / `EbcAtm_S3cret!`      |

---

## Prerequisites

- **Docker Desktop 4.x** with ≥ 24 GB RAM (Polaris + Trino + Flink + DataHub
  together comfortably hit 16 GB).
- **Docker Compose ≥ 2.20** (for the root `include:` directive).
- **bash 5.x** (Git-bash on Windows, native on macOS/Linux).
- **Python 3.11+** only if you want to run ancillary scripts on the host.

No third-party JARs need to be pre-downloaded — the Flink image
(`flink/Dockerfile`) pulls every connector (postgres-cdc, mongodb-cdc,
sqlserver-cdc, flink-sql-connector-kafka, iceberg-flink-runtime,
iceberg-aws-bundle, flink-shaded-hadoop-2-uber) at build time.

---

## Quick Start

```bash
# 1. One-shot bring-up (Core → Sources → Flink re-submit → Governance)
bash scripts/init_all.sh

# 2. Trigger the medallion pipeline
docker exec ebc-airflow-webserver airflow dags trigger ebc_dbt_silver

# 3. Verify end-to-end (counts, freshness, layer alignment)
bash scripts/verify.sh
```

After step 2 the chain is:
`ebc_dbt_silver` → `ebc_dbt_gold` → `ebc_dbt_serving`
(each TriggerDagRunOperator'd by the previous one; see `docs/04_RUNBOOK.md`).

---

## Bootstrap Order

`scripts/init_all.sh` enforces this sequence; if you `docker compose up`
manually, respect it:

1. `kafka` (KRaft) → `schema-registry`
2. `minio` → `minio-init`
3. `polaris-postgres` → `polaris-bootstrap` → `polaris` → `polaris-catalog-init`
4. `trino` → `trino-init` (creates schemas + serving DDL)
5. `postgres-meta` → `airflow-init` → `airflow-scheduler` + `airflow-webserver`
6. `flink-jobmanager` → `flink-taskmanager` → `flink-sql-gateway` → `flink-init`
7. `postgres-src` / `mongodb` / `mssql` (Sources module)
8. `flink-init` (re-run so CDC connectors can attach to the sources)
9. `datahub-mysql` / `datahub-elasticsearch` → `datahub-gms` →
   `datahub-upgrade` → `datahub-frontend` / `datahub-actions` /
   `ol-datahub-bridge`
10. `redis` → `superset-init` → `superset` + `superset-worker`

---

## Query Examples (Trino)

```sql
-- Executive overview from the Gold layer
SELECT *
FROM   iceberg.gold.mart_daily_txn_volume
WHERE  report_date >= current_date - INTERVAL '7' DAY;

-- IPN SLA compliance from Silver staging
SELECT count(*)                                                AS txn_count,
       avg(processing_time_ms)                                 AS avg_ms,
       count_if(met_sla = TRUE) * 100.0 / count(*)             AS sla_pct
FROM   iceberg.silver.stg_ipn_transactions;

-- Per-scheme totals from Serving (BI-facing)
SELECT scheme, sum(txn_count) AS total_txns, sum(approved_count) AS approved
FROM   iceberg.serving.daily_txn_volume
GROUP  BY scheme
ORDER  BY scheme;

-- Iceberg maintenance (compaction + retention) — the dbt DAGs run this
-- automatically on every layer after each refresh.
ALTER TABLE iceberg.gold.mart_daily_txn_volume EXECUTE optimize;
ALTER TABLE iceberg.gold.mart_daily_txn_volume
            EXECUTE expire_snapshots(retention_threshold => '7d');
```

---

## File Structure

```
ebc-lakehouse/
├── docker-compose.yml                  # root include: of the 3 module files
├── .env
├── README.md                           # ← you are here
├── compose/
│   ├── README.md                       # per-module compose docs
│   ├── docker-compose.core.yml
│   ├── docker-compose.sources.yml
│   └── docker-compose.governance.yml
├── docs/
│   ├── 00_OVERVIEW.md
│   ├── 01_PREREQUISITES.md
│   ├── 02_QUICKSTART.md
│   ├── 03_ARCHITECTURE.md
│   ├── 04_RUNBOOK.md
│   ├── 05_VALIDATION.md
│   └── 06_REFERENCE.md
├── scripts/
│   ├── _lib.sh                         # log_*, dc(), ensure_network()
│   ├── init_all.sh                     # orchestrator: core→sources→flink→gov
│   ├── init_core.sh
│   ├── init_sources.sh
│   ├── init_flink.sh
│   ├── init_governance.sh
│   ├── flink/submit_flink_jobs.sh      # POST flink/jobs/*.sql to SQL Gateway
│   ├── verify.sh                       # Bronze→Silver→Gold→Serving e2e check
│   ├── generate_continuous_traffic.sh  # synthetic CDC traffic generator
│   ├── seed_wallet_events.sh           # Mongo seeder
│   └── validate_iceberg_catalog.py     # Polaris/Iceberg catalog sanity check
│                                       # (DataHub metadata is populated by
│                                       #  recipes/*.yaml via datahub-actions,
│                                       #  not by a hand-written shell script)
├── flink/
│   ├── Dockerfile                      # apache/flink:1.20 + cdc + iceberg JARs
│   └── jobs/
│       ├── 01_register_polaris_catalog.sql
│       ├── 10_cdc_postgres.sql         # ach + meeza + ipn  → ebc.public.*
│       ├── 11_cdc_mongodb.sql          # wallet_events      → ebc.meeza_digital.*
│       ├── 12_cdc_sqlserver.sql        # atm_sessions       → ebc.dbo.*
│       └── 20-24_sink_*.sql            # 5 × Kafka → Iceberg sinks (upsert)
├── polaris/
│   ├── bootstrap/                      # realm seeding
│   └── catalog-init/init_polaris.py    # OAuth2 → catalog + namespaces + roles
├── trino/
│   ├── etc/                            # Trino server + catalog/ + access-control
│   └── init/sql/                       # schemas, serving DDL, pipeline_metrics
├── postgres/init/                      # postgres-src seed data
├── mongodb/init/                       # rs0 init + wallet_events seed
├── mssql/init/                         # CDC capture instances + atm_sessions seed
├── airflow/
│   ├── Dockerfile                      # apache-airflow + dbt-trino + trino + pymongo
│   └── dags/
│       ├── ebc_dbt_silver.py           # Bronze → Silver (hourly, scheduled)
│       ├── ebc_dbt_gold.py             # Silver → Gold (triggered by silver)
│       └── ebc_dbt_serving.py          # Gold → Serving + docs (triggered by gold)
├── dbt/ebc_lakehouse/
│   ├── dbt_project.yml                 # Iceberg materializations + tags
│   ├── profiles.yml                    # dbt-trino → iceberg catalog
│   └── models/
│       ├── sources.yml                 # bronze.* dbt sources
│       ├── silver/  (5 × stg_*.sql)    # incremental MERGE staging
│       ├── gold/    (3 × mart_*.sql)   # full-refresh aggregates
│       ├── serving/ (3 × *.sql)        # incremental MERGE BI-facing
│       └── semantic/                   # MetricFlow (opt-in, excluded by default)
├── superset/
│   ├── Dockerfile                      # superset + trino[sqlalchemy]
│   ├── superset_config.py
│   └── register_databases.py           # Trino DB pre-registration
├── observability/
│   ├── ebc_openlineage_emitter.py
│   ├── ol_to_datahub_bridge.py
│   └── datahub/                        # bootstrap + ingestion recipes
└── recipes/                            # DataHub CLI recipes
    └── trino.yaml · iceberg.yaml · dbt.yaml · postgres.yaml · …
```

---

## Version Matrix

| Component                 | Version                  |
|---------------------------|--------------------------|
| Apache Kafka              | 3.7 (Confluent 7.7, KRaft) |
| Confluent Schema Registry | 7.7                      |
| MinIO                     | RELEASE.2025-*           |
| Apache Polaris            | latest (Quarkus build)   |
| Apache Iceberg            | 1.7                      |
| Apache Flink              | 1.20.0 (Java 17)         |
| Flink CDC connectors      | 3.2.0 (postgres / mongodb / sqlserver) |
| Iceberg-Flink runtime     | 1.7 (incl. iceberg-aws-bundle) |
| Trino                     | 455                      |
| Apache Airflow            | 2.10.3                   |
| dbt-core                  | 1.8                      |
| dbt-trino                 | 1.8                      |
| Apache Superset           | 4.1.1                    |
| DataHub                   | 1.5.0.3                  |
| PostgreSQL (src + meta + polaris) | 16               |
| MongoDB                   | 7.0                      |
| MS SQL Server             | 2022 Developer           |

---

## Troubleshooting

See `compose/README.md` for the full troubleshooting matrix. Most common
issues this iteration:

| Symptom | Fix |
|---------|-----|
| Silver DAG fails on `check_bronze_freshness` | Source DBs idle → bronze tables stale. Either start `scripts/generate_continuous_traffic.sh` or bump `EBC_BRONZE_SLA_HOURS` (default 6h) on the airflow-scheduler service. |
| Silver dbt run fails with `column __op does not exist` | A stale silver model still references the old Debezium envelope. The 5 silver models were rewritten in v4.0 to use natural PK + event-time — re-pull the repo. |
| Flink jobs FAILED with `NoResourceAvailableException` | TaskManager slot exhaustion. Default is 16 slots — bump in `compose/docker-compose.core.yml` and force-recreate the TM container. |
| Flink Iceberg sink commits 0 to Iceberg | Checkpointing not enabled in the session. The submitter sets it explicitly (`scripts/flink/submit_flink_jobs.sh` — `execution.checkpointing.interval=30s` + `EXACTLY_ONCE`). If you launch jobs manually, copy that session block. |
| Trino `Access Denied` for `admin` user | The file-based access-control rules only grant query rights to `root`, `ebc_admin`, `ebc_user`, `ebc_engineer`, `ebc_bi`. Use one of those — see `trino/etc/rules.json`. |

---

EBC Medallion Lakehouse · v4.0 · Egyptian Banks Company · Confidential
