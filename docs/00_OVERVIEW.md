# 00 · Executive Overview

**EBC Lakehouse · Egyptian Banks Company · National Payment Infrastructure**
**v4.0 — Flink CDC edition**

---

## 1. Purpose

The EBC Lakehouse is a self-contained, on-premises lakehouse platform that
unifies real-time data ingestion, transformation, governance, and analytics
for Egyptian Banks Company's payment infrastructure. It covers the three
critical payment rails — **EG-ACH** (bank-to-bank), **Meeza** (card), and
**IPN-InstaPay** (real-time P2P) — plus the **Meeza Digital wallet** and
the **123 ATM network**, all from a single Iceberg-on-MinIO source of truth.

## 2. Problem statement

Before this platform, EBC's analytics looked like:

- **Data silos** — every payment system had its own DB and its own batch
  export, no unified view.
- **Latency** — overnight batch cycles delayed every operational and
  regulatory dashboard by 24 h.
- **Brittle integration** — hand-rolled ETL jobs broke on every schema
  change at the source.
- **Compliance gaps** — no end-to-end lineage; audits took weeks to
  reconstruct.
- **Vendor lock-in** — the previous warehouse coupled storage and compute,
  so scaling either meant paying for both.

## 3. Solution at a glance

| Stage | Component(s) | Purpose |
|-------|--------------|---------|
| **Sources** | PostgreSQL · MongoDB · MS SQL Server | Operational DBs for payment + wallet + ATM systems (one of each so all three CDC patterns are exercised). |
| **Capture** | Apache Flink 1.20 + Flink-CDC 3.2 (postgres-cdc / mongodb-cdc / sqlserver-cdc) | Continuous, zero-lag change data capture, including snapshot + streaming phases. Replaces the previous Debezium / Kafka-Connect path. |
| **Streaming** | Apache Kafka 3.7 (KRaft, no Zookeeper) · Schema Registry · Avro · `upsert-kafka` | Schema-governed, partition-keyed event log. Topic-per-table. |
| **Sink** | Apache Flink → Apache Iceberg (upsert mode, equality-field-columns) | Streaming MERGE into Bronze. EXACTLY_ONCE checkpoints every 30 s. |
| **Catalog** | Apache Polaris (Iceberg REST) | Multi-engine governance: Trino, Flink, Spark all read the same catalog. |
| **Storage** | Apache Iceberg 1.7 on MinIO (S3) | Medallion (bronze/silver/gold/serving) on object storage. |
| **Compute** | Trino 455 + dbt-trino 1.8 | SQL transformations + interactive analytics. |
| **Orchestrate** | Apache Airflow 2.10 | Three dbt DAGs (`silver` → `gold` → `serving`). |
| **BI** | Apache Superset 4.1 | Dashboards over Trino. |
| **Governance** | DataHub 1.5 + OpenLineage | Catalog, lineage, ownership. |

## 4. Success criteria

| Metric | Baseline | Target | Status |
|--------|----------|--------|--------|
| Pipeline latency (source → bronze) | 24 h batch | < 60 s | ✅ ~30 s (one Flink checkpoint) |
| Pipeline latency (bronze → serving) | 24 h batch | < 1 h | ✅ Hourly Silver DAG → triggers Gold → triggers Serving |
| Data availability | 95 % | 99.9 % | ✅ Iceberg snapshots + retries on every dbt model |
| Lineage capture | manual | automatic | ✅ DataHub + OpenLineage Airflow plugin |
| Storage / compute coupling | tightly coupled | decoupled | ✅ Polaris catalog + MinIO storage, Trino + Flink as separate engines |
| Vendor lock-in | proprietary warehouse | OSS-only | ✅ Every component is Apache-licensed |

## 5. Why these technologies

| Decision | Why |
|----------|-----|
| **Flink CDC** (not Kafka Connect + Debezium) | One engine for ingest *and* sink. Flink runs the snapshot, streams from the binlog/oplog/CDC tables, and writes Iceberg through the same SQL Gateway — no JSON connector files, no DLQ topic, no separate operator. |
| **Polaris** (not Hive Metastore / Glue / Iceberg-REST fixture) | Iceberg-native REST catalog with multi-engine OAuth2 auth + vended credentials → MinIO. Trino, Flink, Spark all see the same tables under the same access policy. |
| **Trino + dbt-trino** (not Spark / ClickHouse / Presto) | dbt-trino has first-class support for Iceberg's `MERGE`, partition transforms, and table properties. Trino's `iceberg` connector is the reference implementation. |
| **MS SQL Server** (not Cassandra) | Cassandra has no native Flink CDC connector. Switching to SQL Server lets the ATM stream join the same Flink CDC pipeline as Postgres and Mongo, removing a special case. |
| **MinIO** (not direct AWS S3) | Same S3 API, runs locally, no cloud spend during the POC. Production swap is a single endpoint change. |
| **DataHub** (not Marquez alone) | Marquez covers lineage only; DataHub covers lineage + catalog + ownership + tests. The OpenLineage→DataHub bridge gives lineage events alongside catalog. |

## 6. Documentation map

| File | What you'll find |
|------|------------------|
| `00_OVERVIEW.md` | (this file) Why the platform exists + headline architecture. |
| `01_PREREQUISITES.md` | Host requirements, ports, image sizes, credentials. |
| `02_QUICKSTART.md` | Bring the stack up in ~10 minutes. |
| `03_ARCHITECTURE.md` | Deep dive: medallion layers, Flink CDC + sink topology, Polaris auth, dbt model layout. |
| `04_RUNBOOK.md` | Day-to-day ops: init order, DAG triggers, common failures + fixes. |
| `05_VALIDATION.md` | End-to-end checks: row counts, freshness, dbt tests, Iceberg snapshots. |
| `06_REFERENCE.md` | Standards (naming, partitioning, tags), and reference data (sample queries, schema dictionaries, port matrix). |
| `compose/README.md` | Per-module compose: which services, which order, which volumes. |

## 7. What's explicitly *not* in this stack (anymore)

| Component | Replaced by | Reason |
|-----------|-------------|--------|
| ClickHouse | Trino on Iceberg | Trino covers OLAP queries on Iceberg directly; no need for a separate warehouse. |
| Kafka Connect + Debezium | Flink CDC | Flink handles both CDC source and Iceberg sink — one less moving part. |
| Cassandra | MS SQL Server | Cassandra has no native Flink CDC connector. |
| Zookeeper | Kafka KRaft | Removed in Confluent ≥ 7.5; KRaft is the supported path. |
| `iceberg-sink-*.json` Kafka Connect configs | `flink/jobs/2N_sink_*.sql` | Same target, simpler topology. |
| Hand-rolled `MERGE INTO serving.*` in Gold DAG | `dbt run --select tag:serving` (incremental MERGE strategy) | Keeps the upsert logic colocated with the model SQL. |
| Old `start.sh` + `Phase 1 / Phase 2` model | `scripts/init_all.sh` + Core / Sources / Governance modules | Modular composition replaces phased monolith. |
