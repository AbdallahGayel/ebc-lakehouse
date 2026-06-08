# 03 · Architecture

How the pieces fit together — storage, compute, catalog, and the medallion
flow from source DB to BI dashboard.

---

## 1. Design principles

| Principle | How it's realised |
|-----------|-------------------|
| **Storage / compute separation** | MinIO holds data; Polaris holds the catalog; Trino + Flink are independent compute. Adding Spark = adding a new principal in Polaris, no data movement. |
| **One source of truth** | Iceberg is the only place where data lives. Bronze is the latest-state-per-PK CDC mirror; everything downstream is a derivation. |
| **Streaming-first ingest, batch-shaped consumption** | Flink owns the source→bronze edge (sub-minute latency). Airflow + dbt own bronze→silver→gold→serving (hourly). BI users see consistent batch snapshots; ops users see live Kafka topics. |
| **Schema-on-write at every Iceberg boundary** | Avro Schema Registry enforces compatibility on Kafka; Polaris enforces Iceberg schema on Bronze writes; dbt's `on_schema_change: append_new_columns` enforces Silver. |
| **Idempotent everything** | Init scripts, Flink job submissions, dbt models — all are safe to re-run. |
| **One CDC pattern per source type** | The three source DBs (Postgres / Mongo / SQL Server) are chosen so every Flink CDC variant (logical decoding / change stream / capture instance) is exercised — anything new fits an existing pattern. |

## 2. Component map

```
┌──────────────────── SOURCES (compose/sources) ─────────────────────┐
│  postgres-src    mongodb (rs0)    mssql (Agent + capture inst.)    │
│   ach            wallet           atm_sessions                     │
│   meeza          events                                            │
│   ipn                                                              │
└──────────┬─────────────┬──────────────────┬────────────────────────┘
           │             │                  │  flink-cdc 3.2 (snapshot + stream)
           ▼             ▼                  ▼
  ┌─────────────────── FLINK CDC SOURCE JOBS ─────────────────────────┐
  │  flink/jobs/10_cdc_postgres.sql  → 3 jobs → 3 topics              │
  │  flink/jobs/11_cdc_mongodb.sql   → 1 job  → 1 topic               │
  │  flink/jobs/12_cdc_sqlserver.sql → 1 job  → 1 topic               │
  └────────────────────────────┬──────────────────────────────────────┘
                               │  upsert-kafka (avro-confluent format)
                               ▼
  ┌─────────────────────── KAFKA (KRaft) ─────────────────────────────┐
  │  ebc.public.ach_transactions / meeza_authorisations / ipn_*       │
  │  ebc.meeza_digital.wallet_events                                  │
  │  ebc.dbo.atm_sessions                                             │
  │  Schema Registry: 10 subjects (5 × key + 5 × value)               │
  └────────────────────────────┬──────────────────────────────────────┘
                               │  upsert-kafka source → Iceberg sink
                               ▼
  ┌──────────────── FLINK ICEBERG SINK JOBS ──────────────────────────┐
  │  flink/jobs/20-24_sink_*.sql → 5 jobs                             │
  │  CREATE TABLE + INSERT INTO polaris.bronze.<table>                │
  │  upsert-enabled + equality-field-columns + EXACTLY_ONCE 30 s ckpt │
  └────────────────────────────┬──────────────────────────────────────┘
                               │  Iceberg Flink runtime → Polaris REST
                               ▼
  ┌─────────────────── POLARIS (Iceberg REST catalog) ────────────────┐
  │  catalog: ebc_lakehouse                                           │
  │  namespaces: bronze / silver / gold / serving                     │
  │  storage: s3://ebc-lakehouse/ on MinIO                            │
  │  auth: OAuth2 client_credentials, scope PRINCIPAL_ROLE:ALL        │
  └────────────────────────────┬──────────────────────────────────────┘
                               │  multi-engine: trino + flink + dbt
                               ▼
  ┌─── TRINO 455 (SQL compute) ──┐    ┌── AIRFLOW (orchestration) ───┐
  │  iceberg connector → Polaris │ ◀──┤  ebc_dbt_silver (hourly)     │
  │  file-based ACL (rules.json) │    │   → ebc_dbt_gold             │
  │  trino/init/sql/* (schemas + │    │     → ebc_dbt_serving        │
  │   serving DDL + ops tables)  │    └──────────────┬───────────────┘
  └──────────────┬───────────────┘                   │ dbt run / test / maintenance
                 │                                   │
                 ▼                                   ▼
  ┌──────────────────────── ICEBERG MEDALLION ON MinIO ───────────────┐
  │  BRONZE   bronze.*    ← Flink upsert (latest-state-per-PK)        │
  │  SILVER   silver.*    ← dbt incremental MERGE (PK + event-time)   │
  │  GOLD     gold.*      ← dbt full-refresh marts                    │
  │  SERVING  serving.*   ← dbt incremental MERGE (BI-facing)         │
  └────────────────────────────┬──────────────────────────────────────┘
                               │  trino queries
                               ▼
  ┌──── SUPERSET (BI) ────┐  ┌──── DATAHUB (catalog + lineage) ────┐
  │  dashboards over     │  │  recipes/ingest: trino, iceberg,    │
  │  iceberg.serving.*   │  │  postgres, mongodb, kafka, dbt      │
  └──────────────────────┘  │  + OpenLineage Airflow events       │
                            └─────────────────────────────────────┘
```

## 3. Medallion contract

| Layer | Owner | Materialisation | Maintenance | Refresh trigger |
|-------|-------|-----------------|-------------|-----------------|
| **bronze** | Flink Iceberg sink | upsert (equality-field-columns) | n/a — Flink compacts on checkpoint | Continuous (every 30 s ckpt) |
| **silver** | dbt-trino | `incremental` + `merge` on natural PK | `ALTER TABLE … EXECUTE optimize` + `expire_snapshots('7d')` | `ebc_dbt_silver` DAG hourly |
| **gold** | dbt-trino | `table` (full refresh) | optimize + expire | triggered by silver |
| **serving** | dbt-trino | `incremental` + `merge` on natural BI key, last-7-day window | optimize + expire | triggered by gold |

**Why incremental at silver + serving but full-refresh at gold:**
- Silver mirrors bronze 1:1 in shape; an incremental MERGE matches bronze's
  upsert semantics. Filter is `WHERE <event_time> > (max(<event_time>) - 1h)`
  to absorb late upserts from Flink's checkpoint window.
- Gold marts are small aggregates (tens to thousands of rows). Full refresh
  is simpler and guarantees the mart matches silver exactly — no drift.
- Serving copies gold marts with `current_timestamp(6) AS refreshed_at` for
  cache-busting. Incremental MERGE keyed on the natural BI key
  (`report_date + scheme + channel`, etc.) keeps the hot last-7-day window
  fresh without rewriting historical rows.

## 4. Bronze schema convention

Bronze tables carry **only the source row payload** — no Debezium envelope,
no `__op` / `__ts_ms`. Flink's upsert-kafka format delivers the row as-is
and Flink's Iceberg sink uses `equality-field-columns` to apply deletes.
The latest version per PK is always in the table; deletes are physically
removed on Iceberg compaction.

This means downstream silver models can dedupe on `(natural_pk)` ordered by
the **source-side event-time column** (e.g. `created_at`, `auth_timestamp`)
rather than synthetic Debezium offsets. See each `stg_<table>.sql` for the
exact dedup pattern.

## 5. DAG topology

```
                       ┌────────────────────────┐
                       │  ebc_dbt_silver        │  @hourly
                       │  (schedule_interval)   │
                       └───────────┬────────────┘
                                   │
   ┌──── check_bronze_freshness ───┤
   │                               │
   │   (fail-fast SLA gate, with   │
   │    historical-bypass for      │
   │    idle tables > 24h)         │
   │                               ▼
   │           ┌── dbt_run_silver  (tag:silver, --fail-fast)
   │           │
   │           ▼
   │       dbt_test_silver
   │           │
   │           ▼
   │       maintain_silver         (optimize + expire_snapshots, TaskGroup)
   │           │
   │           ▼
   │       record_pipeline_metrics (INSERT INTO iceberg.serving.pipeline_metrics)
   │           │
   │           ▼
   │       trigger_dbt_gold ───────►┌────────────────────────┐
   │                                │  ebc_dbt_gold          │  (triggered)
   │                                └───────────┬────────────┘
   │                                            │
   │                                  dbt_run_gold (tag:gold)
   │                                            │
   │                                  dbt_test_gold
   │                                            │
   │                                  maintain_gold (TaskGroup)
   │                                            │
   │                                  trigger_dbt_serving ──►┌──────────────────┐
   │                                                         │ ebc_dbt_serving  │
   │                                                         └──────┬───────────┘
   │                                                                │
   │                                                      dbt_run_serving
   │                                                       (tag:serving
   │                                                        --exclude tag:semantic)
   │                                                                │
   │                                                      dbt_test_serving
   │                                                                │
   │                                                      maintain_serving (TaskGroup)
   │                                                                │
   │                                                      dbt_generate_docs
   │                                                       (final manifest build)
```

## 6. Polaris auth model

| Role / Principal | Privileges | Used by |
|------------------|------------|---------|
| `service_admin` | `MANAGE_GRANTS` on catalog | Bootstrap only |
| `data_engineer` (catalog role) | `CATALOG_MANAGE_CONTENT` on `bronze`/`silver`/`gold`/`serving` | Flink, dbt-trino, Trino reads + writes |
| Principal `root` / `s3cr3t` | bound to all roles via `PRINCIPAL_ROLE:ALL` | Catch-all for the POC; split per principal in production |

The OAuth2 token is fetched once per Flink job and refreshed via Polaris's
token-refresh endpoint. dbt-trino + Trino fetch one token per session.

## 7. Trino access control

`trino/etc/rules.json` is a file-based ACL that maps users → catalogs +
schemas + tables. The four POC roles:

| User | Catalogs | Schemas | Privileges |
|------|----------|---------|------------|
| `root` / `ebc_admin` / `ebc_user` | `*` | `*` | all |
| `ebc_engineer` | `iceberg`, `system`, `jmx`, `memory` | `silver` `gold` `serving` (owner) · `bronze` (read) | all on owned, read on bronze |
| `ebc_bi` | `iceberg`, `system`, `jmx`, `memory` | `*` | read-only |
| anyone else | — | — | denied |

## 8. Storage layout on MinIO

```
s3://ebc-lakehouse/
├── bronze/
│   ├── ach_transactions/data/<part>/<file>.parquet
│   ├── ach_transactions/metadata/<snapshot>.metadata.json
│   └── … (one prefix per Iceberg table)
├── silver/
│   ├── stg_ach_transactions/…
│   └── …
├── gold/
│   ├── mart_daily_txn_volume/…
│   └── …
└── serving/
    ├── daily_txn_volume/…
    ├── scheme_performance/…
    ├── settlement_summary/…
    └── pipeline_metrics/…
```

Each prefix is owned by exactly one Polaris namespace; namespace `location`
properties enforce the mapping (`init_polaris.py`).

## 9. Where every config lives

| Concern | File |
|---------|------|
| Compose modules | `compose/docker-compose.{core,sources,governance}.yml` |
| Flink images + JARs | `flink/Dockerfile` |
| Flink streaming jobs | `flink/jobs/*.sql` |
| Flink job submitter | `scripts/flink/submit_flink_jobs.sh` |
| Polaris realm bootstrap | `polaris/bootstrap/` |
| Polaris catalog + namespace init | `polaris/catalog-init/init_polaris.py` |
| Trino server config | `trino/etc/{config,jvm,node,log}.properties` |
| Trino Iceberg catalog | `trino/etc/catalog/iceberg.properties` |
| Trino ACL | `trino/etc/rules.json` |
| Trino schema + serving DDL | `trino/init/sql/*.sql` |
| Airflow DAGs | `airflow/dags/ebc_dbt_{silver,gold,serving}.py` |
| dbt project | `dbt/ebc_lakehouse/dbt_project.yml` + `profiles.yml` |
| dbt models | `dbt/ebc_lakehouse/models/{sources.yml,silver,gold,serving,semantic}` |
| Superset bootstrap | `superset/{Dockerfile,superset_config.py,register_databases.py}` |
| DataHub ingestion recipes | `recipes/*.yaml` |
| OpenLineage emitter | `observability/ebc_openlineage_emitter.py` |
| OpenLineage→DataHub bridge | `observability/ol_to_datahub_bridge.py` |
