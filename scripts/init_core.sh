#!/usr/bin/env bash
# =============================================================================
# scripts/init_core.sh
# Brings up the Core lakehouse module in the right order and initialises:
#   • Kafka (KRaft mode) + Schema Registry
#   • MinIO bucket layout (medallion prefixes)
#   • Polaris realm + ebc_lakehouse catalog + medallion namespaces
#   • Trino coordinator with iceberg catalog wired to Polaris
#   • Postgres metadata DB + Airflow (init/scheduler/webserver)
# Re-running is idempotent: services already healthy are left alone, init
# containers that already exited 0 are skipped.
# =============================================================================
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
# shellcheck source=_lib.sh
source "${SCRIPT_DIR}/_lib.sh"

CORE_COMPOSE="${ROOT_DIR}/compose/docker-compose.core.yml"

log_section "Core Lakehouse — Initialisation"

ensure_network

# ── Build images first so subsequent up -d calls don't stall on image build.
log "Building Core images (airflow)..."
dc "${CORE_COMPOSE}" build airflow-init airflow-scheduler airflow-webserver

# ── Streaming layer ──────────────────────────────────────────────────────────
log_section "Core 1/5 — Kafka (KRaft) + Schema Registry"
start_service "${CORE_COMPOSE}" kafka            240
start_service "${CORE_COMPOSE}" schema-registry  180

# ── Storage layer ────────────────────────────────────────────────────────────
log_section "Core 2/5 — MinIO + warehouse buckets"
start_service "${CORE_COMPOSE}" minio       60
run_init      "${CORE_COMPOSE}" minio-init  180

# ── Catalog layer ────────────────────────────────────────────────────────────
log_section "Core 3/5 — Apache Polaris (Iceberg REST catalog)"
start_service "${CORE_COMPOSE}" polaris-postgres  60
# Run polaris bootstrap but tolerate the case where the metastore is already
# bootstrapped. In that case the bootstrap tool exits non-zero with a message
# instructing the user to `purge` the metastore — for local dev we can safely
# continue and start the polaris server instead of failing the whole init.
log "  ▶  Running init: polaris-bootstrap (tolerant mode)"
set +e
dc "${CORE_COMPOSE}" up -d --no-deps polaris-bootstrap
rc=$?
set -e
if [[ ${rc} -eq 0 ]]; then
	# Poll the polaris-bootstrap container until it exits or timeout, but do
	# not call the fatal `wait_completed` helper (it would `die` on non-zero
	# exit). Capture the exit code for handling below.
	container=$(dc "${CORE_COMPOSE}" ps -q polaris-bootstrap 2>/dev/null | head -1) || container=""
	if [[ -z "${container}" ]]; then
		log_warn "Could not determine polaris-bootstrap container id; continuing"
		bootstrap_exit_code=0
	else
		bootstrap_timeout=180
		elapsed=0
		interval=5
		bootstrap_exit_code=0
		log "  ⏳ Waiting for polaris-bootstrap to complete (timeout: ${bootstrap_timeout}s)"
		while [[ ${elapsed} -lt ${bootstrap_timeout} ]]; do
			state=$(docker inspect --format='{{.State.Status}}' "${container}" 2>/dev/null || echo 'unknown')
			exit_code=$(docker inspect --format='{{.State.ExitCode}}' "${container}" 2>/dev/null || echo '-1')
			if [[ "${state}" == "exited" ]]; then
				if [[ "${exit_code}" == "0" ]]; then
					log_ok "  polaris-bootstrap completed (+${elapsed}s)"
					bootstrap_exit_code=0
				else
					bootstrap_exit_code=${exit_code}
				fi
				break
			fi
			sleep ${interval}
			elapsed=$(( elapsed + interval ))
			log "    ↻ polaris-bootstrap: ${state} (${elapsed}/${bootstrap_timeout}s)"
		done
		if [[ ${elapsed} -ge ${bootstrap_timeout} ]]; then
			log_warn "polaris-bootstrap did not complete within ${bootstrap_timeout}s; continuing"
			bootstrap_exit_code=1
		fi
	fi
else
	# Show recent logs to aid diagnosis. Don't fail the whole init here — for
	# local dev it's acceptable to continue if bootstrap fails due to the
	# metastore already being initialised. Print a warning and proceed.
	dc "${CORE_COMPOSE}" logs --tail=200 polaris-bootstrap || true
	if dc "${CORE_COMPOSE}" logs polaris-bootstrap 2>/dev/null | grep -qi "already been bootstrapped"; then
		log_warn "Polaris metastore already bootstrapped — continuing without purging"
		log_warn "If you want to re-bootstrap from scratch, run the polaris 'purge' command first."
	else
		log_warn "polaris-bootstrap failed with an unexpected error; continuing anyway (check logs above)"
	fi
fi

# If the bootstrap container exited non-zero, show logs and warn (but continue
# for local dev). Otherwise proceed to start Polaris and run the catalog init.
if [[ ${bootstrap_exit_code:-0} -ne 0 ]]; then
	dc "${CORE_COMPOSE}" logs --tail=200 polaris-bootstrap || true
	if dc "${CORE_COMPOSE}" logs polaris-bootstrap 2>/dev/null | grep -qi "already been bootstrapped"; then
		log_warn "Polaris metastore already bootstrapped — continuing without purging"
		log_warn "If you want to re-bootstrap from scratch, run the polaris 'purge' command first."
	else
		log_warn "polaris-bootstrap exited with code ${bootstrap_exit_code}; continuing anyway (check logs above)"
	fi
fi

# Start the polaris server and continue the init sequence.
start_service "${CORE_COMPOSE}" polaris           180
run_init      "${CORE_COMPOSE}" polaris-catalog-init 180

# ── Compute layer ────────────────────────────────────────────────────────────
log_section "Core 4/5 — Trino"
start_service "${CORE_COMPOSE}" trino 300

# Pre-create schemas + Serving DDL via the Trino bootstrap. Idempotent;
# runs every time and short-circuits if everything is already in place.
log_section "Core 4b/5 — Trino DDL bootstrap"
run_init "${CORE_COMPOSE}" trino-init 180

# ── Orchestration layer ──────────────────────────────────────────────────────
log_section "Core 5/5 — Airflow"
start_service "${CORE_COMPOSE}" postgres-meta 60
run_init      "${CORE_COMPOSE}" airflow-init  180
start_service "${CORE_COMPOSE}" airflow-scheduler 180
start_service "${CORE_COMPOSE}" airflow-webserver 180

# ── Optional Kafka UI ────────────────────────────────────────────────────────
log "  ▶  Starting: kafka-ui (no healthcheck)"
dc "${CORE_COMPOSE}" up -d --no-deps kafka-ui

# ── Apache Flink ─────────────────────────────────────────────────────────────
# Streaming engine that owns the entire ingest + sink path:
#   • log-based CDC for Postgres + MongoDB + SQL Server
#     (flink/jobs/10_cdc_postgres.sql, 11_cdc_mongodb.sql, 12_cdc_sqlserver.sql),
#   • Kafka → Iceberg stateful sink for all five Bronze tables
#     (flink/jobs/20_*.sql … 24_*.sql), with watermarks + Iceberg upsert.
# Kafka Connect was removed in the same change that retired Cassandra and
# introduced MS SQL Server — Flink CDC sqlserver-cdc covers the previously
# uncovered source.
log_section "Core ▸ Apache Flink"
log "Building Flink image (downloads connector JARs on first build)..."
dc "${CORE_COMPOSE}" build flink-jobmanager
start_service "${CORE_COMPOSE}" flink-jobmanager   180
start_service "${CORE_COMPOSE}" flink-taskmanager  180
start_service "${CORE_COMPOSE}" flink-sql-gateway  180
run_init      "${CORE_COMPOSE}" flink-init        300

log_section "Core module is up"
log_ok "Trino     →  http://localhost:8080"
log_ok "Flink UI  →  http://localhost:8881"
log_ok "Polaris   →  http://localhost:8181/api/catalog  (root / s3cr3t)"
log_ok "MinIO     →  http://localhost:9001              (minioadmin / minioadmin)"
log_ok "Airflow   →  http://localhost:8085              (admin / admin)"
log_ok "Kafka UI  →  http://localhost:8090"
