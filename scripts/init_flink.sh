#!/usr/bin/env bash
# =============================================================================
# scripts/init_flink.sh
#
# Stand-alone wrapper around the compose `flink-init` service. Use it to
# (re-)submit every flink/jobs/*.sql without restarting the whole Core
# module. Idempotent: cancels stale jobs that share a name with one we're
# about to (re-)submit, so editing a SQL file + running this is a clean
# rolling update.
# =============================================================================
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
# shellcheck source=_lib.sh
source "${SCRIPT_DIR}/_lib.sh"

CORE_COMPOSE="${ROOT_DIR}/compose/docker-compose.core.yml"

log_section "Flink — submitting streaming jobs"

ensure_network

# Wait for prerequisites that init_core.sh should already have started.
# Use `docker` directly to match the convention in init_governance.sh — every
# other script either calls `docker compose` via the `dc()` helper from _lib.sh,
# or `docker ps`/`docker inspect` against named containers. No $DOCKER var.
for service in flink-jobmanager flink-taskmanager flink-sql-gateway; do
    if ! docker ps --format '{{.Names}}' 2>/dev/null | grep -qx "ebc-${service}"; then
        die "${service} is not running — bring up Core first (scripts/init_core.sh)"
    fi
done

# Rebuild + run the one-shot submitter.
dc "${CORE_COMPOSE}" build flink-init
run_init "${CORE_COMPOSE}" flink-init 300

log_section "Flink job submission complete"
log_ok "Flink Web UI       →  http://localhost:8881  (host port; container listens on 8081)"
log_ok "Flink SQL Gateway  →  http://localhost:8883  (host port; container listens on 8083)"
