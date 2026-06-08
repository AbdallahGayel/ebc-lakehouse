#!/usr/bin/env python3
# observability/ol_to_datahub_bridge.py
# Kafka consumer: OpenLineage events → DataHub MCE via GMS REST
from __future__ import annotations
import json, logging, os, time
from datetime import datetime, timezone

from kafka import KafkaConsumer
from kafka.errors import KafkaError
import requests

log = logging.getLogger(__name__)
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP",   "kafka:29092")
OL_KAFKA_TOPIC  = os.getenv("OL_KAFKA_TOPIC",    "openlineage.events")
DATAHUB_GMS_URL = os.getenv("DATAHUB_GMS_URL",   "http://datahub-gms:8080")
POLL_INTERVAL_S = int(os.getenv("POLL_INTERVAL_S", "30"))
CONSUMER_GROUP  = "ebc-ol-datahub-bridge"
NAMESPACE       = "ebc-lakehouse"

PLATFORM_MAP = {
    "kafka://":      "kafka",
    "iceberg://":    "iceberg",
    "clickhouse://": "clickhouse",
    "postgresql://": "postgres",
}

def _headers():
    h = {"Content-Type": "application/json"}
    tok = os.getenv("DATAHUB_PAT_TOKEN", "")
    if tok: h["Authorization"] = f"Bearer {tok}"
    return h

def _urn(namespace: str, name: str) -> str:
    plat = "clickhouse"
    for pfx, p in PLATFORM_MAP.items():
        if namespace.startswith(pfx): plat = p; break
    return f"urn:li:dataset:(urn:li:dataPlatform:{plat},{name.replace('.','/')},PROD)"

def _emit_mcp(entity_urn: str, aspect_name: str, aspect: dict) -> bool:
    try:
        r = requests.post(
            f"{DATAHUB_GMS_URL}/aspects?action=ingestProposal",
            json={"proposal": {
                "entityType": entity_urn.split(":")[1],
                "entityUrn":  entity_urn,
                "changeType": "UPSERT",
                "aspectName": aspect_name,
                "aspect": {"contentType": "application/json",
                            "value": json.dumps(aspect)},
            }},
            headers=_headers(), timeout=15,
        )
        r.raise_for_status()
        return True
    except Exception as exc:
        log.error("MCP emit failed %s/%s: %s", entity_urn, aspect_name, exc)
        return False

def process_event(ev: dict) -> None:
    event_type = ev.get("eventType", "")
    inputs     = ev.get("inputs",  [])
    outputs    = ev.get("outputs", [])
    ts_ms      = int(datetime.now(timezone.utc).timestamp() * 1000)
    actor      = "urn:li:corpuser:ebc-pipeline"

    # Emit UpstreamLineage for each output
    if event_type == "COMPLETE" and inputs and outputs:
        for out in outputs:
            out_urn   = _urn(out.get("namespace", NAMESPACE), out.get("name", ""))
            upstreams = [{"auditStamp": {"time": ts_ms, "actor": actor},
                          "dataset": _urn(i.get("namespace", NAMESPACE), i.get("name","")),
                          "type": "TRANSFORMED"} for i in inputs]
            _emit_mcp(out_urn, "upstreamLineage", {"upstreams": upstreams})

    # Emit DataProcessInstance run record
    run_id = ev.get("run", {}).get("runId", "")
    if run_id:
        state_map = {"START": "STARTED", "COMPLETE": "COMPLETE", "FAIL": "FAILED"}
        run_urn   = f"urn:li:dataProcessInstance:{run_id}"
        _emit_mcp(run_urn, "dataProcessInstanceRunEvent", {
            "timestampMillis": ts_ms,
            "status": state_map.get(event_type, "COMPLETE"),
        })

    log.info("OL %s → DataHub: job=%s in=%d out=%d",
             event_type, ev.get("job",{}).get("name",""), len(inputs), len(outputs))

def run_bridge():
    log.info("Bridge starting. topic=%s gms=%s", OL_KAFKA_TOPIC, DATAHUB_GMS_URL)
    consumer = KafkaConsumer(
        OL_KAFKA_TOPIC,
        bootstrap_servers     = KAFKA_BOOTSTRAP.split(","),
        group_id              = CONSUMER_GROUP,
        auto_offset_reset     = "earliest",
        enable_auto_commit    = True,
        value_deserializer    = lambda m: json.loads(m.decode("utf-8")),
        consumer_timeout_ms   = POLL_INTERVAL_S * 1000,
    )
    errs = 0
    while True:
        try:
            for msg in consumer:
                try:
                    process_event(msg.value)
                    errs = 0
                except Exception as exc:
                    errs += 1
                    log.error("Event process error (offset=%d): %s", msg.offset, exc)
                    if errs > 10:
                        raise SystemExit(1)
        except KafkaError as ke:
            log.warning("Kafka error: %s — retrying in %ds", ke, POLL_INTERVAL_S)
            time.sleep(POLL_INTERVAL_S)
        except StopIteration:
            time.sleep(POLL_INTERVAL_S)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
        format="%(asctime)s [%(levelname)-7s] %(name)s — %(message)s")
    run_bridge()
