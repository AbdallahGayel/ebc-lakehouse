# =============================================================================
# airflow/dags/ebc_dbt_serving.py
# DAG: ebc_dbt_serving
# Trigger: ebc_dbt_gold → trigger_dbt_serving (TriggerDagRunOperator).
#
# Gold → Serving layer.
#
#   dbt_run_serving  →  dbt_test_serving  →  maintain_serving
#                                                    ↓
#                                            dbt_generate_docs
#
# The serving models are dbt incremental + merge with the natural business
# key (see dbt/.../models/serving/*.sql), so a single `dbt run --select
# tag:serving` is equivalent to the hand-rolled MERGE statements that used
# to live in ebc_dbt_gold — and keeps the upsert logic colocated with the
# model SQL.
#
# dbt_generate_docs runs at the very end of the medallion chain (here) so
# the manifest covers Bronze + Silver + Gold + Serving in one pass.
# =============================================================================
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.utils.task_group import TaskGroup

DBT_DIR = '/opt/airflow/dbt/ebc_lakehouse'
DBT_CMD = '/home/airflow/.local/bin/dbt'

DEFAULT_ARGS = {
    'owner':            'ebc-data-engineering',
    'depends_on_past':  False,
    'retries':          2,
    'retry_delay':      timedelta(minutes=5),
    'email_on_failure': True,
    'email':            ['data-ops@ebc.eg'],
    'sla':              timedelta(hours=1),
}

SERVING_TABLES = [
    'daily_txn_volume',
    'scheme_performance',
    'settlement_summary',
]


with DAG(
    dag_id            = 'ebc_dbt_serving',
    description       = 'Gold → Serving: dbt incremental MERGE + Iceberg maintenance + docs',
    default_args      = DEFAULT_ARGS,
    schedule_interval = None,
    start_date        = datetime(2025, 1, 1),
    catchup           = False,
    max_active_runs   = 1,
    tags              = ['ebc', 'serving', 'dbt', 'trino', 'iceberg'],
    doc_md            = __doc__,
) as dag:

    # `tag:serving --exclude tag:semantic` runs the BI-facing copies of the
    # Gold marts but skips the MetricFlow time-spine / semantic models. The
    # semantic layer DAG was removed in this revision; if those models are
    # ever needed again, drop the `--exclude` filter or build a dedicated DAG.
    dbt_run_serving = BashOperator(
        task_id      = 'dbt_run_serving',
        bash_command = (
            f'cd {DBT_DIR} && {DBT_CMD} run '
            f'--select tag:serving --exclude tag:semantic '
            f'--profiles-dir . --fail-fast'
        ),
        doc_md = (
            "Incremental MERGE upsert of BI-facing tables under "
            "iceberg.serving (keyed on the natural business key — "
            "see each serving/*.sql for `unique_key`)."
        ),
    )

    dbt_test_serving = BashOperator(
        task_id      = 'dbt_test_serving',
        bash_command = (
            f'cd {DBT_DIR} && {DBT_CMD} test '
            f'--select tag:serving --exclude tag:semantic '
            f'--profiles-dir .'
        ),
        trigger_rule = 'all_done',
    )

    with TaskGroup(group_id='maintain_serving') as maintain_serving:
        SQLExecuteQueryOperator(
            task_id    = 'optimize',
            conn_id    = 'trino_default',
            autocommit = True,
            sql        = [
                f"ALTER TABLE iceberg.serving.{t} EXECUTE optimize"
                for t in SERVING_TABLES
            ],
        )
        SQLExecuteQueryOperator(
            task_id    = 'expire_snapshots',
            conn_id    = 'trino_default',
            autocommit = True,
            sql        = [
                f"ALTER TABLE iceberg.serving.{t} "
                f"EXECUTE expire_snapshots(retention_threshold => '7d')"
                for t in SERVING_TABLES
            ],
        )

    # End-of-chain docs regen covers Bronze → Silver → Gold → Serving so
    # that lineage and tests in the docs site reflect the latest state.
    dbt_generate_docs = BashOperator(
        task_id      = 'dbt_generate_docs',
        bash_command = f'cd {DBT_DIR} && {DBT_CMD} docs generate --profiles-dir .',
        trigger_rule = 'all_done',
    )

    dbt_run_serving >> dbt_test_serving >> maintain_serving >> dbt_generate_docs
