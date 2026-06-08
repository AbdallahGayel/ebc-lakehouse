#!/usr/bin/env python3
# observability/ebc_openlineage_emitter.py
# Custom OpenLineage emitters: Kafka→Bronze, ClickHouse serving inserts

from __future__ import annotations
import json, logging, os
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from openlineage.client import OpenLineageClient
from openlineage.client.run import (
    Dataset, DatasetFacets, InputDataset, Job, OutputDataset,
    Run, RunEvent, RunFacets, RunState,
)
from openlineage.client.facet import (
    DataSourceDatasetFacet, DocumentationDatasetFacet, OwnershipDatasetFacet,
    OwnershipDatasetFacetOwners, SchemaDatasetFacet, SchemaField, SqlJobFacet,
)

log = logging.getLogger(__name__)
NAMESPACE   = os.getenv("OPENLINEAGE_NAMESPACE", "ebc-lakehouse")
MARQUEZ_URL = os.getenv("OPENLINEAGE_URL",       "http://marquez:5000")

EBC_OWNERS = OwnershipDatasetFacet(owners=[
    OwnershipDatasetFacetOwners(name="data-engineering@ebc.eg", type="TEAM"),
    OwnershipDatasetFacetOwners(name="data-ops@ebc.eg",         type="GROUP"),
])

def _client() -> OpenLineageClient:
    from openlineage.client.transport.http import HttpConfig, HttpTransport
    return OpenLineageClient(transport=HttpTransport(HttpConfig.from_dict({
        "url": MARQUEZ_URL, "endpoint": "api/v1/lineage", "verify": False,
    })))


# ── Kafka → Bronze lineage ────────────────────────────────────────────────────
KAFKA_BRONZE_PAIRS = [
    ("ebc.public.ach_transactions",     "bronze.ach_transactions"),
    ("ebc.public.meeza_authorisations", "bronze.meeza_authorisations"),
    ("ebc.public.ipn_transactions",     "bronze.ipn_transactions"),
]

def emit_kafka_bronze_lineage(run_id: str | None = None) -> None:
    client = _client()
    run_id = run_id or str(uuid4())
    now    = datetime.now(timezone.utc).isoformat()
    for topic, bronze_table in KAFKA_BRONZE_PAIRS:
        src = topic.split(".")[-1]
        for state in (RunState.START, RunState.COMPLETE):
            client.emit(RunEvent(
                eventType = state,
                eventTime = now,
                run       = Run(runId=run_id),
                job       = Job(namespace=NAMESPACE, name=f"kafka_iceberg_sink.{src}"),
                inputs    = [InputDataset(
                    namespace = f"kafka://kafka:29092",
                    name      = topic,
                    facets    = DatasetFacets(ownership=EBC_OWNERS,
                        dataSource=DataSourceDatasetFacet(
                            name=f"kafka-ebc-{src}",
                            uri=f"kafka://kafka:29092/{topic}"))
                )],
                outputs   = [OutputDataset(
                    namespace = f"iceberg://iceberg-rest:8181/ebc",
                    name      = bronze_table,
                    facets    = DatasetFacets(ownership=EBC_OWNERS,
                        dataSource=DataSourceDatasetFacet(
                            name="iceberg-minio-bronze",
                            uri=f"s3a://ebc-lakehouse/bronze/{src}"))
                )],
                producer  = "kafka-connect-iceberg-sink",
            ))
    log.info("Kafka→Bronze OL lineage emitted for %d topics", len(KAFKA_BRONZE_PAIRS))


# ── ClickHouse lineage emitter ────────────────────────────────────────────────
class ClickHouseLineageEmitter:
    def __init__(self, task_id, input_dataset, output_dataset, sql, dag_run_id, job_name=None):
        self.task_id        = task_id
        self.input_dataset  = input_dataset
        self.output_dataset = output_dataset
        self.sql            = sql
        self.dag_run_id     = dag_run_id
        self.job_name       = job_name or f"ebc_dbt_gold.{task_id}"
        self.run_id         = str(uuid4())
        self._client        = _client()

    def _ds(self, name, is_input=True) -> Any:
        db = name.split(".")[0]
        facets = DatasetFacets(ownership=EBC_OWNERS,
            dataSource=DataSourceDatasetFacet(
                name="clickhouse-ebc",
                uri=f"clickhouse://clickhouse:9000/{db}"))
        cls = InputDataset if is_input else OutputDataset
        return cls(namespace=NAMESPACE, name=name, facets=facets)

    def _emit(self, state: RunState, error: str | None = None):
        from openlineage.client.facet import ErrorMessageRunFacet
        rf = {}
        if error:
            rf["errorMessage"] = ErrorMessageRunFacet(message=error, programmingLanguage="SQL")
        self._client.emit(RunEvent(
            eventType = state,
            eventTime = datetime.now(timezone.utc).isoformat(),
            run       = Run(runId=self.run_id, facets=RunFacets(**rf)),
            job       = Job(namespace=NAMESPACE, name=self.job_name,
                            facets={"sql": SqlJobFacet(query=self.sql)}),
            inputs    = [self._ds(self.input_dataset, is_input=True)],
            outputs   = [self._ds(self.output_dataset, is_input=False)],
            producer  = "ebc-airflow-gold-dag",
        ))

    def execute_and_emit(self, client) -> None:
        self._emit(RunState.START)
        try:
            client.execute(self.sql)
            self._emit(RunState.COMPLETE)
        except Exception as exc:
            self._emit(RunState.FAIL, error=str(exc))
            raise


if __name__ == "__main__":
    import argparse, logging as _log
    _log.basicConfig(level=_log.INFO)
    p = argparse.ArgumentParser()
    p.add_argument("--bronze", action="store_true")
    args = p.parse_args()
    if args.bronze:
        emit_kafka_bronze_lineage()
