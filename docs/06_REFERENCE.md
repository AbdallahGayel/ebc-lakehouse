# 06 · Reference & Standards

Conventions for naming, partitioning, tagging, and the reference data
(sample queries, schema dictionaries, ports) needed to operate or extend
the platform.

---

## 1. Naming conventions

### Kafka topics

```
ebc.<source_schema>.<source_table>
```

| Source | Schema | Topic |
|--------|--------|-------|
| Postgres `ebc_sources.public.ach_transactions` | `public` | `ebc.public.ach_transactions` |
| Mongo `meeza_digital.wallet_events` | `meeza_digital` | `ebc.meeza_digital.wallet_events` |
| MSSQL `atm_telemetry.dbo.atm_sessions` | `dbo` | `ebc.dbo.atm_sessions` |

The `ebc.` prefix is the global namespace; the schema part lets us add a
second Postgres database (`ebc.public.x`, `ebc.warehouse.y`) without
collision.

### Iceberg tables

| Layer | Convention | Example |
|-------|-----------|---------|
| bronze | `bronze.<source_table>` | `bronze.ach_transactions` |
| silver | `silver.stg_<source_table>` | `silver.stg_ach_transactions` |
| gold | `gold.mart_<business_domain>` | `gold.mart_daily_txn_volume` |
| serving | `serving.<business_facing_name>` | `serving.daily_txn_volume` |

Silver mirrors bronze names with the `stg_` prefix because dbt
conventionally uses it for staging models. Gold is the only layer with
business-domain naming (`mart_*`). Serving drops the `mart_` because BI
users see those table names directly.

### Airflow DAGs

```
ebc_<owner>_<layer>
```

| DAG | Owner | Layer |
|-----|-------|-------|
| `ebc_dbt_silver` | dbt | silver |
| `ebc_dbt_gold` | dbt | gold |
| `ebc_dbt_serving` | dbt | serving |

### Flink jobs

```
flink/jobs/<order>_<kind>_<source>.sql
```

| Range | Kind | Files |
|-------|------|-------|
| `00–09` | catalog setup | `01_register_polaris_catalog.sql` |
| `10–19` | CDC source jobs | `10_cdc_postgres.sql`, `11_cdc_mongodb.sql`, `12_cdc_sqlserver.sql` |
| `20–29` | Iceberg sink jobs | `20_sink_ach_transactions.sql`, …, `24_sink_atm_sessions.sql` |

Submission order is lex, so the `<order>` prefix is meaningful.

### Polaris namespaces / catalog roles

| Entity | Convention | Example |
|--------|-----------|---------|
| catalog | `ebc_<environment>` | `ebc_lakehouse` |
| namespace | layer name | `bronze` `silver` `gold` `serving` |
| principal | tool name | `flink` `dbt` `trino` `superset` |
| catalog role | `<persona>_role` | `data_engineer_role`, `bi_reader_role` |

## 2. Partitioning

| Layer | Partition column | Transform | Why |
|-------|------------------|-----------|-----|
| bronze | (none) | — | Flink upsert sink writes whatever Iceberg's default file layout dictates; partitioning here would multiply small files per checkpoint. Add post-hoc via `ALTER TABLE` if needed. |
| silver | per-table event-time | `day(<event_ts>)` | Daily files match the source's natural cadence; queries scoped to a day prune cleanly. |
| gold | per-mart report date | `month(report_date)` | Marts have lower cardinality; monthly partitions keep file counts in the tens. |
| serving | per-table primary date | `month(<report_date>)` | Matches Gold so the dbt MERGE can prune the unchanged partitions. |

The partition transform is declared in each model's `config(partitioned_by =
['day(col)'])` block. Flink's `PARTITIONED BY` clause does **not** accept
transform functions like `days(col)` — see `flink/jobs/20_*.sql` which
omits the clause and relies on Trino's later `ALTER TABLE … SET PROPERTIES
partitioning = ARRAY['day(created_at)']` for production tuning.

## 3. Tagging (dbt)

Every model carries at least one tag for `--select`:

| Tag | Meaning | Used by |
|-----|---------|---------|
| `silver` | Silver staging table | `ebc_dbt_silver` |
| `gold` | Gold mart | `ebc_dbt_gold` |
| `serving` | BI-facing serving table | `ebc_dbt_serving` |
| `semantic` | MetricFlow time-spine / semantic model | (excluded by default) |
| `ebc` | All EBC models | Org-wide selection |

The serving DAG uses `--select tag:serving --exclude tag:semantic` so the
MetricFlow models (which require `mf` CLI) don't accidentally run.

## 4. Iceberg table properties (defaults)

Set globally in `dbt_project.yml`:

```yaml
models:
  ebc_lakehouse:
    +materialized: table
    +properties:
      format:          "'PARQUET'"
      format_version:  "2"
```

Layer-specific overrides in the model `config(…)`:

```sql
{{ config(
    materialized          = 'incremental',
    incremental_strategy  = 'merge',
    unique_key            = 'txn_id',
    on_schema_change      = 'append_new_columns',
    partitioned_by        = ['day(source_created_at)']
) }}
```

For Flink sinks (in `flink/jobs/2N_sink_*.sql`):

```sql
'format-version'                  = '2',
'write.upsert.enabled'            = 'true',
'write.parquet.compression-codec' = 'snappy'
```

## 5. Sample queries

```sql
-- Daily transaction volume across all rails
SELECT report_date, scheme, txn_count, total_amount_egp
FROM   iceberg.serving.daily_txn_volume
WHERE  report_date >= current_date - INTERVAL '7' DAY
ORDER  BY report_date DESC, scheme;

-- IPN SLA compliance
SELECT count(*)                                           AS total_txns,
       count_if(met_sla)                                  AS met,
       round(count_if(met_sla) * 100.0 / count(*), 2)     AS pct
FROM   iceberg.silver.stg_ipn_transactions;

-- ATM uptime by network
SELECT network_id, count(*) AS sessions,
       count_if(txn_status = 'APPROVED') AS approved
FROM   iceberg.silver.stg_atm_sessions
GROUP  BY network_id;

-- Settlement net position (bank to bank)
SELECT originating_bank_id, receiving_bank_id,
       net_settled_egp, settlement_rate_pct
FROM   iceberg.serving.settlement_summary
WHERE  settlement_date = current_date - INTERVAL '1' DAY
ORDER  BY net_settled_egp DESC
LIMIT  20;

-- Iceberg snapshot history (most-recent first)
SELECT committed_at, snapshot_id, operation, summary
FROM   "iceberg.bronze.ach_transactions$snapshots"
ORDER  BY committed_at DESC
LIMIT  10;
```

## 6. Operational standards

| Concern | Standard |
|---------|----------|
| **Idempotency** | Every init script must be safe to re-run. Use `CREATE … IF NOT EXISTS`, `ON CONFLICT DO NOTHING`, `MERGE`. |
| **Healthchecks** | Every long-running service in compose has a `healthcheck:`. Init scripts gate on `condition: service_healthy`. |
| **No interactive auth** | All credentials are env vars (OAuth2 client_credentials, basic auth, no MFA). PoC only — production rotates. |
| **Logging** | Stdout/stderr only; no in-container log files. Aggregate via `docker logs` or a forwarder. |
| **State** | Container-local under `/tmp` is acceptable for dev (Flink checkpoints); named volumes for everything else. Production should use S3/RDS. |
| **Idempotent Flink job submission** | `scripts/init_flink.sh` cancels any matching prior job before resubmitting (job name = `INSERT INTO`'s target table). |
| **One PR-able file per concern** | Compose files per module; SQL files per Flink job; one dbt model per `stg_*` / `mart_*` / serving table; one DAG per medallion layer. |

## 7. Security checklist (before production)

- [ ] Rotate every default password in `.env` (Airflow / Superset / DataHub
      admins, Postgres / Mongo / SQL Server SAs, Polaris root).
- [ ] Switch MinIO to IAM-based access via Vault or SOPS.
- [ ] Enable TLS on Trino (`trino/etc/config.properties`).
- [ ] Replace file-based ACL in `trino/etc/rules.json` with OAuth2 +
      catalog-role bindings in Polaris.
- [ ] Move Flink checkpoint dir from `/tmp/flink-checkpoints` to
      `s3a://ebc-lakehouse/checkpoints/` and add IAM auth to the JM/TM.
- [ ] Re-issue every Polaris principal with rotated client secrets;
      shorten the OAuth2 token TTL and verify refresh works on every
      engine.
- [ ] Put DataHub frontend behind SSO.
- [ ] Enable Kafka SASL/SSL and re-issue every Flink job with secured
      bootstrap.
- [ ] Encrypt Postgres / MongoDB / SQL Server volumes at rest.

## 8. Port reference

| Service | Host | Container | Protocol |
|---------|-----:|----------:|----------|
| Kafka (PLAINTEXT_HOST) | 9092 | 9092 | Kafka wire |
| Kafka (PLAINTEXT internal) | — | 29092 | Kafka wire |
| Schema Registry | 8081 | 8081 | HTTP/REST |
| Kafka UI | 8090 | 8080 | HTTP |
| MinIO API | 9000 | 9000 | S3 |
| MinIO Console | 9001 | 9001 | HTTP |
| Polaris REST + mgmt | 8181 | 8181 | HTTP |
| Polaris Health | 8182 | 8182 | HTTP |
| Trino | 8080 | 8080 | HTTP + JDBC |
| Flink JobManager | 8881 | 8081 | HTTP REST |
| Flink SQL Gateway | 8883 | 8083 | HTTP REST |
| Airflow webserver | 8085 | 8080 | HTTP |
| Superset | 8088 | 8088 | HTTP |
| DataHub Frontend | 9002 | 9002 | HTTP |
| DataHub GMS | 8082 | 8080 | HTTP |
| Postgres-src | 5433 | 5432 | PostgreSQL wire |
| Postgres-meta | 5434 | 5432 | PostgreSQL wire |
| Polaris-postgres | 5435 | 5432 | PostgreSQL wire |
| MongoDB | 27017 | 27017 | MongoDB wire |
| MS SQL Server | 1433 | 1433 | TDS |

## 9. Source-data dictionary

### Postgres `ebc_sources`

| Table | PK | Event-time | Cardinality (seeded) |
|-------|-----|-----------|---------------------:|
| `ach_transactions` | `txn_id` | `created_at` | 2,000 |
| `meeza_authorisations` | `auth_id` | `auth_timestamp` | 4,000 |
| `ipn_transactions` | `txn_id` | `initiated_at` | 6,000 |

### MongoDB `meeza_digital`

| Collection | PK | Event-time | Cardinality (seeded) |
|------------|-----|-----------|---------------------:|
| `wallet_events` | `_id` | `event_ts` | 500 |

### MS SQL Server `atm_telemetry`

| Table | PK | Event-time | Cardinality (seeded) |
|-------|-----|-----------|---------------------:|
| `dbo.atm_sessions` | (`atm_id`, `session_id`) | `updated_at` | 20 |

## 10. dbt model registry

```
dbt/ebc_lakehouse/models/
├── sources.yml          # bronze.* sources (loaded_at_field per table)
├── silver/
│   ├── schema.yml       # unique_final, not_null, accepted_values per col
│   ├── stg_ach_transactions.sql
│   ├── stg_meeza_authorisations.sql
│   ├── stg_ipn_transactions.sql
│   ├── stg_meeza_digital_wallet.sql
│   └── stg_atm_sessions.sql
├── gold/
│   ├── schema.yml
│   ├── mart_daily_txn_volume.sql       # cross-rail daily volume
│   ├── mart_scheme_performance.sql     # per-scheme + per-channel approval rates
│   └── mart_settlement_summary.sql     # net settlement per bank pair (ACH)
├── serving/
│   ├── schema.yml
│   ├── daily_txn_volume.sql            # MERGE-upserted copy of mart_daily_txn_volume
│   ├── scheme_performance.sql
│   └── settlement_summary.sql
└── semantic/                           # opt-in (excluded by serving DAG)
    ├── _metrics.yml
    ├── _schema.yml
    ├── _semantic_models.yml
    └── metricflow_time_spine.sql
```

## 11. Glossary

| Term | Meaning |
|------|---------|
| **Bronze** | Raw mirror of source rows in Iceberg, written continuously by Flink. |
| **Silver** | Cleaned + typed + deduplicated staging, owned by dbt. |
| **Gold** | Business-domain aggregates (full-refresh marts). |
| **Serving** | BI-optimised, MERGE-upserted projections of Gold (last-7-day hot window). |
| **CDC** | Change Data Capture — streaming row-level changes from a source DB. |
| **upsert-kafka** | Flink connector that treats a Kafka topic as a compacted, key-versioned table. |
| **equality-field-columns** | Iceberg sink property naming the columns Flink uses to identify deletes. |
| **Polaris** | Apache Iceberg's REST catalog implementation with OAuth2 + vended credentials. |
| **Vended credentials** | Polaris-issued, short-lived S3 credentials that scope a client's access to specific table prefixes. |
| **MERGE strategy** | dbt-trino's incremental strategy that emits `MERGE INTO` against the Iceberg table. |
| **TaskGroup** | Airflow construct that visually groups related tasks in the UI without affecting scheduling. |
