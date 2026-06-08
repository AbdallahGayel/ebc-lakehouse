#!/usr/bin/env bash
# =============================================================================
# scripts/init_governance.sh
# Brings up the Governance & BI module:
#   • Redis
#   • DataHub MySQL + Elasticsearch + GMS + SystemUpdate + Frontend + Actions
#   • OpenLineage → DataHub bridge
#   • Apache Superset (init / web / worker) with Trino pre-registered
#
# Requires Core to be up (postgres-meta, trino, kafka). Idempotent.
# =============================================================================
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
# shellcheck source=_lib.sh
source "${SCRIPT_DIR}/_lib.sh"

GOV_COMPOSE="${ROOT_DIR}/compose/docker-compose.governance.yml"

log_section "Data Governance & BI — Initialisation"

ensure_network

# Sanity: Core must be up — Superset and DataHub both depend on postgres-meta,
# trino, and kafka, all of which live in Core.
for container in ebc-postgres-meta ebc-trino ebc-kafka; do
    if ! docker ps --format '{{.Names}}' | grep -qx "${container}"; then
        die "${container} is not running — bring up Core first (scripts/init_core.sh)"
    fi
done
log_ok "Core dependencies are running (postgres-meta, trino, kafka)"

# Build Superset image first so the long pip install doesn't block later steps.
log "Building Superset image..."
dc "${GOV_COMPOSE}" build superset-init

# ── DataHub backend ──────────────────────────────────────────────────────────
log_section "Governance 1/4 — DataHub backend (MySQL + Elasticsearch)"
start_service "${GOV_COMPOSE}" datahub-mysql         180
start_service "${GOV_COMPOSE}" datahub-elasticsearch 300

# ── DataHub SystemUpdate (Kafka topics + bootstrap MCPs) ─────────────────────
log_section "Governance 2/4 — DataHub SystemUpdate"
run_init "${GOV_COMPOSE}" datahub-upgrade 600

# ── DataHub GMS ──────────────────────────────────────────────────────────────
log_section "Governance 3/4 — DataHub GMS (Ebean DDL + ES indices)"
start_service "${GOV_COMPOSE}" datahub-gms 900

dc "${GOV_COMPOSE}" up -d --no-deps datahub-frontend datahub-actions ol-datahub-bridge
wait_healthy "${GOV_COMPOSE}" datahub-frontend 180

# ── Superset ─────────────────────────────────────────────────────────────────
log_section "Governance 4/4 — Apache Superset"
start_service "${GOV_COMPOSE}" redis 60
run_init      "${GOV_COMPOSE}" superset-init 600
start_service "${GOV_COMPOSE}" superset 300
dc "${GOV_COMPOSE}" up -d --no-deps superset-worker

log_section "Governance module is up"
log_ok "Superset →  http://localhost:8088   (admin / admin)"
log_ok "DataHub  →  http://localhost:9002   (datahub / datahub)"
