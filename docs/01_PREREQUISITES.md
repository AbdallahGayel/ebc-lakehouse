# 01 · Prerequisites

Everything needed to bring the stack up on a developer laptop or a single
on-premises VM.

---

## 1. Host requirements

| Resource | Minimum | Recommended | Notes |
|----------|---------|-------------|-------|
| CPU | 8 vCPU | 12 vCPU | Flink TM defaults to 16 slots; Polaris + Trino + DataHub are the heavy sitters. |
| RAM | 16 GB | 24 GB+ | Polaris ~2 GB, Trino ~4 GB, DataHub-GMS ~4 GB, ES ~2 GB, Flink JM+TM ~4 GB. |
| Disk | 30 GB free | 60 GB+ | Iceberg data (MinIO), Kafka segments, container images. |
| OS | Linux / macOS / Windows + WSL2 | Linux native | WSL2 works; native Docker on macOS is fine. |
| Docker Engine | ≥ 24.x | latest | Required for `compose.include`. |
| Docker Compose | ≥ 2.20 | latest | `include:` directive is 2.20+. |
| bash | 5.x | 5.x | Git-bash on Windows is fine. |
| Python | 3.11 (optional) | 3.12 | Only needed for `scripts/validate_iceberg_catalog.py` on the host. |

The stack itself runs entirely inside containers — there's no Python or JVM
dependency on the host beyond what Docker needs.

## 2. Network ports

Every published port (`host:container`):

| Service | Host port | Container port | Purpose |
|---------|----------:|----------------|---------|
| Kafka (PLAINTEXT_HOST) | 9092 | 9092 | broker (host clients) |
| Schema Registry | 8081 | 8081 | Avro schemas |
| Kafka UI | 8090 | 8080 | broker UI |
| MinIO API | 9000 | 9000 | S3 |
| MinIO Console | 9001 | 9001 | UI |
| Polaris (REST) | 8181 | 8181 | catalog + management |
| Polaris (Health) | 8182 | 8182 | Quarkus probes |
| Trino | 8080 | 8080 | UI + JDBC |
| Flink JobManager | 8881 | 8081 | UI + REST |
| Flink SQL Gateway | 8883 | 8083 | SQL submission |
| Airflow | 8085 | 8080 | UI |
| Superset | 8088 | 8088 | UI |
| DataHub Frontend | 9002 | 9002 | UI |
| DataHub GMS | 8082 | 8080 | API (intra) |
| Postgres-src | 5433 | 5432 | source DB |
| Postgres-meta | 5434 | 5432 | Airflow / Superset meta |
| Polaris-postgres | 5435 | 5432 | Polaris realm DB |
| MongoDB | 27017 | 27017 | source DB |
| MS SQL Server | 1433 | 1433 | source DB |

If any of these conflict with services on your host, edit the `ports:`
block in the relevant `compose/docker-compose.*.yml` — every other config
file uses the container hostname:port pair, so host re-mapping is safe.

## 3. Default credentials

| Service | User | Password / token |
|---------|------|------------------|
| Airflow | `admin` | `admin` |
| Superset | `admin` | `admin` |
| DataHub | `datahub` | `datahub` |
| MinIO | `minioadmin` | `minioadmin` |
| Trino | `ebc_admin` / `ebc_user` / `ebc_engineer` / `ebc_bi` / `root` | none (file-based ACL) |
| Polaris | `root` | `s3cr3t` (OAuth2 client_credentials, scope `PRINCIPAL_ROLE:ALL`) |
| Postgres-src | `ebc_src` | `ebc_src_pass` |
| Postgres-meta | `airflow` | `airflow` |
| Polaris-postgres | `polaris` | `polaris` |
| MongoDB | (no auth in POC) | — |
| MS SQL Server | `sa` | `EbcAtm_S3cret!` |

All of the above are intended for the POC. For production, regenerate via
`.env` and rotate immediately.

## 4. Image sizes

First pull is ~12–14 GB. Subsequent ups are cached. The biggest images:

| Image | Approx. size |
|-------|-------------:|
| `apache/flink:1.20.0-java17` (after the project Dockerfile adds CDC + Iceberg JARs) | ~2.2 GB |
| `acryldata/datahub-gms` + `datahub-frontend` | ~2.0 GB combined |
| `mcr.microsoft.com/mssql/server:2022-latest` | ~1.5 GB |
| `apache/superset:4.1.1` (after Trino dialect install) | ~1.2 GB |
| `trinodb/trino:455` | ~1.0 GB |
| `apache/airflow:2.10.3-python3.12` (after `dbt-trino` install) | ~1.0 GB |
| `apache/polaris-quarkus-server:latest` | ~0.5 GB |
| `confluentinc/cp-kafka:7.7.0` + `cp-schema-registry:7.7.0` | ~1.2 GB combined |

## 5. No external JAR downloads

The Flink image (`flink/Dockerfile`) pulls every required JAR at *build*
time, baked into the image — no manual JAR drops, no plugin directories:

- `flink-sql-connector-postgres-cdc-3.2.0.jar`
- `flink-sql-connector-mongodb-cdc-3.2.0.jar`
- `flink-sql-connector-sqlserver-cdc-3.2.0.jar`
- `flink-sql-connector-kafka-3.3.0-1.20.jar`
- `flink-sql-avro-confluent-registry-1.20.0.jar`
- `iceberg-flink-runtime-1.20-1.7.0.jar`
- `iceberg-aws-bundle-1.7.0.jar`
- `flink-shaded-hadoop-2-uber-2.8.3-10.0.jar`

If `init_core.sh` reports `Failed to load Hadoop classes` or
`NoClassDefFoundError: org/apache/iceberg/...`, the Flink image hasn't been
rebuilt since a Dockerfile edit. Force a rebuild:

```bash
docker compose -f compose/docker-compose.core.yml build \
    flink-jobmanager flink-taskmanager flink-sql-gateway
docker compose -f compose/docker-compose.core.yml up -d --force-recreate \
    flink-jobmanager flink-taskmanager flink-sql-gateway
```

## 6. Verifying readiness

A 60-second sanity check after `bash scripts/init_all.sh`:

```bash
docker ps --format 'table {{.Names}}\t{{.Status}}' | sort
# expect ~25 containers, none in (unhealthy) state

curl -sf http://localhost:8181/q/health             # polaris
curl -sf http://localhost:8080/v1/info              # trino
curl -sf http://localhost:8881/overview             # flink JM
curl -sf http://localhost:8085/health               # airflow
curl -sf http://localhost:8088/health               # superset
curl -sf http://localhost:9002/health               # datahub frontend
```

For the in-depth post-bring-up checklist, see `05_VALIDATION.md`.
