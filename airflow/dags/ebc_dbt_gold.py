# =============================================================================
# airflow/dags/ebc_dbt_gold.py
# DAG: ebc_dbt_gold
# Trigger: ebc_dbt_silver → trigger_dbt_gold (TriggerDagRunOperator).
#
# Silver → Gold layer.
#
#   dbt_run_gold  →  dbt_test_gold  →  maintain_gold  →  trigger_dbt_serving
#
# Gold marts are full-refresh `table` materialisations (low-cardinality
# aggregations over Silver), so there's no incremental state to manage —
# every run rebuilds the table from scratch. Maintenance is just optimize +
# expire_snapshots.
#
# The hand-rolled MERGE-into-serving and the maintain_serving block that
# used to live here have been moved into the dedicated ebc_dbt_serving DAG,
# where they're owned by the dbt-trino incremental MERGE strategy on the
# serving models (a duplicate SQL MERGE here would only drift from the
# model SQL).
# =============================================================================
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
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

GOLD_TABLES = [
    'mart_daily_txn_volume',
    'mart_scheme_performance',
    'mart_settlement_summary',
]


with DAG(
    dag_id            = 'ebc_dbt_gold',
    description       = 'Silver → Gold: dbt full-refresh marts + Iceberg maintenance',
    default_args      = DEFAULT_ARGS,
    schedule_interval = None,
    start_date        = datetime(2025, 1, 1),
    catchup           = False,
    max_active_runs   = 1,
    tags              = ['ebc', 'gold', 'dbt', 'trino', 'iceberg'],
    doc_md            = __doc__,
) as dag:

    dbt_run_gold = BashOperator(
        task_id      = 'dbt_run_gold',
        bash_command = (
            f'cd {DBT_DIR} && {DBT_CMD} run '
            f'--select tag:gold --profiles-dir . --fail-fast'
        ),
        doc_md = "Full rebuild of mart_* tables under iceberg.gold.",
    )

    dbt_test_gold = BashOperator(
        task_id      = 'dbt_test_gold',
        bash_command = (
            f'cd {DBT_DIR} && {DBT_CMD} test '
            f'--select tag:gold --profiles-dir .'
        ),
        trigger_rule = 'all_done',
    )

    with TaskGroup(group_id='maintain_gold') as maintain_gold:
        SQLExecuteQueryOperator(
            task_id    = 'optimize',
            conn_id    = 'trino_default',
            autocommit = True,
            sql        = [
                f"ALTER TABLE iceberg.gold.{t} EXECUTE optimize"
                for t in GOLD_TABLES
            ],
        )
        SQLExecuteQueryOperator(
            task_id    = 'expire_snapshots',
            conn_id    = 'trino_default',
            autocommit = True,
            sql        = [
                f"ALTER TABLE iceberg.gold.{t} "
                f"EXECUTE expire_snapshots(retention_threshold => '7d')"
                for t in GOLD_TABLES
            ],
        )

    trigger_serving = TriggerDagRunOperator(
        task_id             = 'trigger_dbt_serving',
        trigger_dag_id      = 'ebc_dbt_serving',
        wait_for_completion = False,
        reset_dag_run       = True,
    )

    dbt_run_gold >> dbt_test_gold >> maintain_gold >> trigger_serving
