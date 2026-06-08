#!/usr/bin/env bash
# =============================================================================
# scripts/init_sources.sh
# Brings up the Data Sources module and runs per-source bootstrap:
#   • postgres-src   — schema + seed data (../postgres/init/*.sql)
#   • mongodb        — replica-set rs0 initiation
#   • mssql          — ebc_atm DB + dbo.atm_sessions table + CDC enabled +
#                      seed data (../mssql/init/*.sql). Replaces Cassandra.
#
# Idempotent. Safe to re-run after adding a new source service to
# compose/docker-compose.sources.yml; only new services will start.
# =============================================================================
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
# shellcheck source=_lib.sh
source "${SCRIPT_DIR}/_lib.sh"

SRC_COMPOSE="${ROOT_DIR}/compose/docker-compose.sources.yml"

log_section "Data Sources — Initialisation"

ensure_network

# ── PostgreSQL ───────────────────────────────────────────────────────────────
log_section "Sources 1/3 — PostgreSQL (logical decoding)"
start_service "${SRC_COMPOSE}" postgres-src 120

log "  Validating ebc_sources table count..."
TBL_COUNT=$(dc "${SRC_COMPOSE}" exec -T postgres-src \
    psql -U ebc_src -d ebc_sources -tAc \
        "SELECT COUNT(*) FROM information_schema.tables
         WHERE table_schema='public' AND table_type='BASE TABLE';" \
    2>/dev/null | tr -d '[:space:]' || echo '0')
if [[ "${TBL_COUNT:-0}" -ge 3 ]]; then
    log_ok "  postgres-src: ${TBL_COUNT} tables found in ebc_sources"
else
    log_warn "  postgres-src: only ${TBL_COUNT} tables — seed data may be missing"
fi

# ── MongoDB ──────────────────────────────────────────────────────────────────
log_section "Sources 2/3 — MongoDB (replica set rs0)"
start_service "${SRC_COMPOSE}" mongodb       180
dc "${SRC_COMPOSE}" up -d --no-deps mongodb-init
log "  Validating rs0 PRIMARY..."
RETRIES=0
while [[ ${RETRIES} -lt 12 ]]; do
    PRIMARY=$(dc "${SRC_COMPOSE}" exec -T mongodb mongosh --quiet --eval \
        'rs.status().members.filter(m=>m.stateStr==="PRIMARY").length' \
        2>/dev/null || echo '0')
    if [[ "${PRIMARY// /}" == '1' ]]; then
        log_ok "  MongoDB rs0 has a PRIMARY member"
        break
    fi
    RETRIES=$(( RETRIES + 1 ))
    sleep 10
    log "    ↻ Waiting for rs0 PRIMARY (${RETRIES}/12)"
done

# ── MS SQL Server (ATM / 123 Network — replaces Cassandra) ───────────────────
log_section "Sources 3/3 — MS SQL Server (CDC-enabled)"
start_service "${SRC_COMPOSE}" mssql       180
run_init      "${SRC_COMPOSE}" mssql-init  240
log "  Validating dbo.atm_sessions row count + CDC capture state..."
dc "${SRC_COMPOSE}" exec -T mssql /opt/mssql-tools18/bin/sqlcmd \
    -S localhost -U sa -P 'EbcAtm_S3cret!' -C -d ebc_atm \
    -Q "SELECT
            (SELECT COUNT(*) FROM dbo.atm_sessions)               AS atm_rows,
            (SELECT is_tracked_by_cdc FROM sys.tables
             WHERE name='atm_sessions' AND schema_id=SCHEMA_ID('dbo')) AS cdc_on" \
    -W -h-1 || log_warn "  validation query failed (continuing)"

log_section "Sources module is up"
log_ok "PostgreSQL src  →  localhost:5433  (ebc_src / ebc_src_pass)"
log_ok "MongoDB         →  localhost:27017 (rs0 PRIMARY)"
log_ok "MS SQL Server   →  localhost:1433  (sa / EbcAtm_S3cret!)"
