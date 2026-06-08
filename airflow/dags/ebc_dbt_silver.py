# =============================================================================
# airflow/dags/ebc_dbt_silver.py
# DAG: ebc_dbt_silver
#
# Bronze → Silver layer. Runs hourly.
#
#   freshness_gate   →   dbt_run_silver  →  dbt_test_silver
#                                          ↘
#                          maintain_silver  →  record_pipeline_metrics
#                                                      ↓
#                                              trigger_dbt_gold
#
# The freshness gate compares each Bronze table's latest event timestamp to
# an SLA threshold. Tables idle longer than EBC_BRONZE_HISTORICAL_HOURS are
# treated as backfills (warn, don't fail) so a dormant source can't wedge
# the whole pipeline.
#
# Why no Debezium-envelope handling here anymore: the Bronze layer is now
# fed by Flink CDC + upsert-kafka (see flink/jobs/10-12_cdc_*.sql), which
# delivers the source row payload directly. The Silver dbt models dedupe
# on natural PK + event-time instead of __op/__ts_ms.
# =============================================================================
from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta

import pendulum

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.utils.task_group import TaskGroup

log = logging.getLogger(__name__)

# ─── Shared config ──────────────────────────────────────────────────────────
DBT_DIR = '/opt/airflow/dbt/ebc_lakehouse'
DBT_CMD = '/home/airflow/.local/bin/dbt'

DEFAULT_ARGS = {
    'owner':            'ebc-data-engineering',
    'depends_on_past':  False,
    'retries':          2,
    'retry_delay':      timedelta(minutes=5),
    'email_on_failure': False,
    'email':            ['data-ops@ebc.eg'],
    'sla':              timedelta(hours=1),
}

# Freshness gate thresholds (override via env on the airflow-scheduler service).
#   ACTIVE_SLA_HOURS    — a streaming table must see an event within this window.
#   HISTORICAL_CUTOFF_H — idle > this → treated as backfill, warn + pass.
ACTIVE_SLA_HOURS    = float(os.environ.get('EBC_BRONZE_SLA_HOURS',        '6'))
HISTORICAL_CUTOFF_H = float(os.environ.get('EBC_BRONZE_HISTORICAL_HOURS', '24'))

# (catalog, schema, table, event-time column used for the freshness check)
BRONZE_TABLES = [
    ('iceberg', 'bronze', 'ach_transactions',            'created_at'),
    ('iceberg', 'bronze', 'meeza_authorisations',        'auth_timestamp'),
    ('iceberg', 'bronze', 'ipn_transactions',            'initiated_at'),
    ('iceberg', 'bronze', 'meeza_digital_wallet_events', 'event_ts'),
    ('iceberg', 'bronze', 'atm_sessions',                'updated_at'),
]

SILVER_TABLES = [
    'stg_ach_transactions',
    'stg_meeza_authorisations',
    'stg_ipn_transactions',
    'stg_meeza_digital_wallet',
    'stg_atm_sessions',
]

# Map source-topic → (bronze table, silver table) for pipeline_metrics inserts.
PIPELINE_PAIRS = [
    ('ebc.public.ach_transactions',     'iceberg.bronze.ach_transactions',            'iceberg.silver.stg_ach_transactions'),
    ('ebc.public.meeza_authorisations', 'iceberg.bronze.meeza_authorisations',        'iceberg.silver.stg_meeza_authorisations'),
    ('ebc.public.ipn_transactions',     'iceberg.bronze.ipn_transactions',            'iceberg.silver.stg_ipn_transactions'),
    ('ebc.meeza_digital.wallet_events', 'iceberg.bronze.meeza_digital_wallet_events', 'iceberg.silver.stg_meeza_digital_wallet'),
    ('ebc.dbo.atm_sessions',            'iceberg.bronze.atm_sessions',                'iceberg.silver.stg_atm_sessions'),
]


def _trino_client():
    """Lazy-import + connect: keeps the DAG file parseable without trino."""
    from trino.dbapi import connect
    return connect(
        host        = os.environ.get('TRINO_HOST',    'trino'),
        port        = int(os.environ.get('TRINO_PORT', '8080')),
        user        = os.environ.get('TRINO_USER',    'ebc_user'),
        catalog     = os.environ.get('TRINO_CATALOG', 'iceberg'),
        schema      = 'bronze',
        http_scheme = 'http',
    )


# ─── Tasks ───────────────────────────────────────────────────────────────────
def check_bronze_freshness(**_):
    """
    Three-way classification per Bronze table:

      • EMPTY            → fail. The Flink sink never produced any rows.
      • HISTORICAL       → warn + pass. Last event older than
                           HISTORICAL_CUTOFF_H — treat as a static backfill.
      • ACTIVE-AND-STALE → fail. Lag is between SLA and HISTORICAL — the
                           stream is degraded.
      • FRESH            → pass silently.
    """
    conn = _trino_client()
    cur  = conn.cursor()
    failures, historical = [], []

    for catalog, schema, table, ts_col in BRONZE_TABLES:
        fq = f'"{catalog}"."{schema}"."{table}"'
        try:
            cur.execute(f"SELECT count(*), max({ts_col}) FROM {fq}")
            row_count, last_ts = cur.fetchone()
        except Exception as exc:
            failures.append(f"{schema}.{table}: check failed — {exc}")
            continue

        if not row_count:
            failures.append(f"{schema}.{table}: EMPTY — Flink sink may not have run")
            continue
        if last_ts is None:
            failures.append(f"{schema}.{table}: {ts_col} is NULL")
            continue

        last_ts_dt = (
            pendulum.instance(last_ts) if isinstance(last_ts, datetime)
            else pendulum.parse(str(last_ts))
        )
        lag_h = (pendulum.now('UTC') - last_ts_dt).total_seconds() / 3600

        if lag_h <= ACTIVE_SLA_HOURS:
            log.info("%s.%s: %d rows, last event %.2fh ago — FRESH",
                     schema, table, row_count, lag_h)
        elif HISTORICAL_CUTOFF_H is not None and lag_h > HISTORICAL_CUTOFF_H:
            log.warning("%s.%s: last event %.1fh ago (> %.0fh) — HISTORICAL backfill, bypassing SLA",
                        schema, table, lag_h, HISTORICAL_CUTOFF_H)
            historical.append(f"{schema}.{table} (last: {last_ts})")
        else:
            failures.append(
                f"{schema}.{table}: lag {lag_h:.1f}h exceeds {ACTIVE_SLA_HOURS:.0f}h SLA "
                f"(last: {last_ts})"
            )

    if failures:
        raise ValueError(
            "Bronze freshness FAILED:\n  • " + "\n  • ".join(failures)
            + (("\n\nHistorical bypasses:\n  • " + "\n  • ".join(historical))
               if historical else "")
        )
    if historical:
        log.warning("Freshness passed with %d historical bypass(es)", len(historical))
    else:
        log.info("All Bronze tables FRESH. Proceeding to Silver.")


def record_pipeline_metrics(**kwargs):
    """
    Insert one row per source topic into serving.pipeline_metrics for this
    run. Used by Superset dashboards to detect silent loss between Bronze
    and Silver. Table is pre-created in trino-init.
    """
    run_id     = kwargs['run_id']
    batch_date = datetime.strptime(kwargs['ds'], '%Y-%m-%d').date()

    cur = _trino_client().cursor()

    def _count(fq: str) -> int:
        try:
            cur.execute(f"SELECT count(*) FROM {fq}")
            return cur.fetchone()[0] or 0
        except Exception as exc:
            log.warning("count failed for %s: %s", fq, exc)
            return 0

    rows = []
    for topic, bronze_tbl, silver_tbl in PIPELINE_PAIRS:
        b, s = _count(bronze_tbl), _count(silver_tbl)
        rows.append(
            f"('{run_id.replace(chr(39), chr(39)*2)}', "
            f"'{topic.replace(chr(39), chr(39)*2)}', 0, {b}, {s}, 0.0, "
            f"DATE '{batch_date}', current_timestamp(6))"
        )

    cur.execute(
        "INSERT INTO iceberg.serving.pipeline_metrics "
        "(run_id, source_topic, kafka_offset, bronze_row_count, "
        " silver_row_count, sink_lag_seconds, batch_date, recorded_at) "
        "VALUES " + ",".join(rows)
    )
    cur.fetchall()
    log.info("pipeline_metrics: inserted %d rows for run %s", len(rows), run_id)


# ─── DAG ─────────────────────────────────────────────────────────────────────
with DAG(
    dag_id            = 'ebc_dbt_silver',
    description       = 'Bronze → Silver: freshness gate + dbt incremental MERGE + maintenance',
    default_args      = DEFAULT_ARGS,
    schedule_interval = '@hourly',
    start_date        = datetime(2025, 1, 1),
    catchup           = False,
    max_active_runs   = 1,
    tags              = ['ebc', 'silver', 'dbt', 'trino', 'iceberg'],
    doc_md            = __doc__,
) as dag:

    freshness_gate = PythonOperator(
        task_id         = 'check_bronze_freshness',
        python_callable = check_bronze_freshness,
        doc_md          = "Hard-fail the run if any active Bronze table is stale beyond SLA.",
    )

    dbt_run_silver = BashOperator(
        task_id      = 'dbt_run_silver',
        bash_command = (
            f'cd {DBT_DIR} && {DBT_CMD} run '
            f'--select tag:silver --profiles-dir . --fail-fast'
        ),
        doc_md = "Incremental MERGE of all silver staging tables (tag:silver).",
    )

    dbt_test_silver = BashOperator(
        task_id      = 'dbt_test_silver',
        bash_command = (
            f'cd {DBT_DIR} && {DBT_CMD} test '
            f'--select tag:silver --profiles-dir .'
        ),
        # tests are diagnostic — don't block maintenance/downstream on test
        # warnings, but surface failures in logs.
        trigger_rule = 'all_done',
    )

    # ── Iceberg housekeeping for the Silver layer ────────────────────────────
    # `optimize` rewrites small files into larger ones, `expire_snapshots`
    # drops history older than 7 days. Both procedures live on Trino's
    # Iceberg connector. Group keeps the UI tidy.
    with TaskGroup(group_id='maintain_silver') as maintain_silver:
        SQLExecuteQueryOperator(
            task_id    = 'optimize',
            conn_id    = 'trino_default',
            autocommit = True,
            sql        = [
                f"ALTER TABLE iceberg.silver.{t} EXECUTE optimize"
                for t in SILVER_TABLES
            ],
        )
        SQLExecuteQueryOperator(
            task_id    = 'expire_snapshots',
            conn_id    = 'trino_default',
            autocommit = True,
            sql        = [
                f"ALTER TABLE iceberg.silver.{t} "
                f"EXECUTE expire_snapshots(retention_threshold => '7d')"
                for t in SILVER_TABLES
            ],
        )

    record_metrics = PythonOperator(
        task_id         = 'record_pipeline_metrics',
        python_callable = record_pipeline_metrics,
    )

    trigger_gold = TriggerDagRunOperator(
        task_id             = 'trigger_dbt_gold',
        trigger_dag_id      = 'ebc_dbt_gold',
        wait_for_completion = False,
        reset_dag_run       = True,
    )

    (
        freshness_gate
        >> dbt_run_silver
        >> dbt_test_silver
        >> maintain_silver
        >> record_metrics
        >> trigger_gold
    )
