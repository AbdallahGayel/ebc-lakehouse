#!/usr/bin/env bash
# =============================================================================
# scripts/init_all.sh
# Top-level orchestrator. Brings up the stack in the correct order:
#   1. Core       — Kafka KRaft + Schema Registry + MinIO + Polaris + Trino +
#                   Airflow + postgres-meta + Flink (incl. job submission).
#                   No Kafka Connect — every CDC source is owned by Flink.
#   2. Sources    — postgres-src, mongodb, mssql (replaces Cassandra)
#   3. Flink jobs — re-submit jobs now that the sources are up (Flink CDC
#                   needs its sources reachable before it can start a job)
#   4. Governance — DataHub + Superset
#
# Each step is idempotent. To bring up a single module:
#   scripts/init_core.sh        — Core + Trino DDL + Flink first-pass submit
#   scripts/init_sources.sh     — source systems
#   scripts/init_flink.sh       — re-submit Flink jobs (sources reachable now)
#   scripts/init_governance.sh  — DataHub + Superset
# =============================================================================
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=_lib.sh
source "${SCRIPT_DIR}/_lib.sh"

log_section "EBC Lakehouse — Full-stack bootstrap"

ensure_network

bash "${SCRIPT_DIR}/init_core.sh"
bash "${SCRIPT_DIR}/init_sources.sh"
# Sources just came up — re-submit Flink jobs so the CDC connectors actually
# attach to postgres-src / mongodb / mssql instead of failing on first try.
bash "${SCRIPT_DIR}/init_flink.sh"
bash "${SCRIPT_DIR}/init_governance.sh"

log_section "Bootstrap complete"
log_ok "Trino         →  http://localhost:8080"
log_ok "Flink UI      →  http://localhost:8881"
log_ok "Polaris       →  http://localhost:8181/api/catalog"
log_ok "Airflow       →  http://localhost:8085   (admin / admin)"
log_ok "Superset      →  http://localhost:8088   (admin / admin)"
log_ok "DataHub       →  http://localhost:9002   (datahub / datahub)"
log_ok "MinIO         →  http://localhost:9001   (minioadmin / minioadmin)"
log_ok "MS SQL Server →  localhost:1433          (sa / EbcAtm_S3cret!)"
log_ok "Kafka UI      →  http://localhost:8090"
