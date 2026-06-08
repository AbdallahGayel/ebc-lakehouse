# =============================================================================
# scripts/_lib.sh
# Shared helpers for the modular init scripts. Source this file:
#   source "$(dirname "$0")/_lib.sh"
# =============================================================================
# shellcheck shell=bash

set -euo pipefail

# ── Colours ───────────────────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
BLUE='\033[0;34m'; CYAN='\033[0;36m'; BOLD='\033[1m'; NC='\033[0m'

# ── Logging ───────────────────────────────────────────────────────────────────
_ts()       { date '+%Y-%m-%d %H:%M:%S'; }
log()       { echo -e "$(_ts)  ${BLUE}[INFO]${NC}  $*"; }
log_ok()    { echo -e "$(_ts)  ${GREEN}[ OK ]${NC}  $*"; }
log_warn()  { echo -e "$(_ts)  ${YELLOW}[WARN]${NC}  $*"; }
log_error() { echo -e "$(_ts)  ${RED}[ERR ]${NC}  $*" >&2; }

log_section() {
    local line='════════════════════════════════════════════════════════════════════'
    echo ""
    echo -e "${CYAN}${BOLD}  ╔${line}╗${NC}"
    printf "  ${CYAN}${BOLD}║  %-66s║${NC}\n" "$*"
    echo -e "${CYAN}${BOLD}  ╚${line}╝${NC}"
    echo ""
}

die() { log_error "$*"; exit 1; }

# ── Shared external network ───────────────────────────────────────────────────
readonly EBC_NETWORK="${EBC_NETWORK:-ebc-lakehouse-network}"

ensure_network() {
    if docker network inspect "${EBC_NETWORK}" &>/dev/null; then
        log_ok "Network '${EBC_NETWORK}' already exists"
    else
        log "Creating external network '${EBC_NETWORK}'"
        docker network create --driver bridge "${EBC_NETWORK}" >/dev/null \
            || die "Failed to create network '${EBC_NETWORK}'"
        log_ok "Network '${EBC_NETWORK}' created"
    fi
}

# ── Compose helpers ───────────────────────────────────────────────────────────
# Resolve the project root (the directory containing this _lib.sh's parent)
# so `dc` can always pass the project-level .env regardless of cwd. Callers
# can override by exporting EBC_ROOT_DIR before sourcing.
EBC_ROOT_DIR="${EBC_ROOT_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
EBC_ENV_FILE="${EBC_ENV_FILE:-${EBC_ROOT_DIR}/.env}"

# Usage: dc <compose-file> <subcommand...>
dc() {
    local file="$1"; shift
    if [[ -f "${EBC_ENV_FILE}" ]]; then
        docker compose --env-file "${EBC_ENV_FILE}" -f "${file}" "$@"
    else
        docker compose -f "${file}" "$@"
    fi
}

# Wait for a Docker healthcheck to report `healthy`. Works for both long-
# running services (returns once healthy) and services without healthchecks
# (returns once running).
wait_healthy() {
    local file="$1" service="$2" timeout="${3:-180}"
    local elapsed=0 interval=5

    local container
    container=$(dc "${file}" ps -q "${service}" 2>/dev/null | head -1) \
        || die "Could not resolve container for '${service}'"
    [[ -z "${container}" ]] && die "No container found for service '${service}'"

    log "  ⏳ Waiting for ${service} to be healthy (timeout: ${timeout}s)"
    while [[ ${elapsed} -lt ${timeout} ]]; do
        local health running
        health=$(docker inspect \
            --format='{{if .State.Health}}{{.State.Health.Status}}{{else}}no-hc{{end}}' \
            "${container}" 2>/dev/null || echo 'inspect-failed')
        running=$(docker inspect --format='{{.State.Running}}' \
            "${container}" 2>/dev/null || echo 'false')

        case "${health}" in
            healthy)
                log_ok "  ${service} is healthy (+${elapsed}s)"; return 0 ;;
            no-hc)
                if [[ "${running}" == "true" ]]; then
                    log_ok "  ${service} is running (no healthcheck, +${elapsed}s)"
                    return 0
                fi ;;
            unhealthy)
                dc "${file}" logs --tail=30 "${service}" || true
                die "${service} entered UNHEALTHY state" ;;
        esac
        sleep "${interval}"
        elapsed=$(( elapsed + interval ))
        log "    ↻ ${service}: ${health} (${elapsed}/${timeout}s)"
    done
    dc "${file}" logs --tail=40 "${service}" || true
    die "${service} did not become healthy within ${timeout}s"
}

# Wait for a one-shot init container to exit 0.
wait_completed() {
    local file="$1" service="$2" timeout="${3:-180}"
    local elapsed=0 interval=5

    local container
    container=$(dc "${file}" ps -q "${service}" 2>/dev/null | head -1) \
        || die "Could not resolve container for '${service}'"

    log "  ⏳ Waiting for ${service} to complete (timeout: ${timeout}s)"
    while [[ ${elapsed} -lt ${timeout} ]]; do
        local state exit_code
        state=$(docker inspect --format='{{.State.Status}}' \
            "${container}" 2>/dev/null || echo 'unknown')
        exit_code=$(docker inspect --format='{{.State.ExitCode}}' \
            "${container}" 2>/dev/null || echo '-1')

        if [[ "${state}" == "exited" ]]; then
            if [[ "${exit_code}" == "0" ]]; then
                log_ok "  ${service} completed (+${elapsed}s)"; return 0
            fi
            dc "${file}" logs --tail=50 "${service}" || true
            die "${service} exited with code ${exit_code}"
        fi

        sleep "${interval}"
        elapsed=$(( elapsed + interval ))
        log "    ↻ ${service}: ${state} (${elapsed}/${timeout}s)"
    done
    die "${service} did not complete within ${timeout}s"
}

# Bring up a long-running service and wait for it. Idempotent.
start_service() {
    local file="$1" service="$2" timeout="${3:-180}"
    log "  ▶  Starting: ${service}"
    dc "${file}" up -d --no-deps "${service}" \
        || die "Failed to start ${service}"
    wait_healthy "${file}" "${service}" "${timeout}"
}

# Bring up a one-shot init container and wait for exit 0. Idempotent.
run_init() {
    local file="$1" service="$2" timeout="${3:-180}"
    log "  ▶  Running init: ${service}"
    dc "${file}" up -d --no-deps "${service}" \
        || die "Failed to run init ${service}"
    wait_completed "${file}" "${service}" "${timeout}"
}
