#!/usr/bin/env bash
# =============================================================================
# superset/docker-bootstrap.sh
# Multi-mode entrypoint for all three Superset containers.
#
# Modes:
#   init          — one-shot init: create DB, migrations, admin user, register
#                   Trino schema connections (run by superset-init service)
#   app-gunicorn  — production web server (run by superset service)
#   worker        — Celery async query worker (run by superset-worker service)
# =============================================================================
set -eo pipefail

MODE="${1:-app-gunicorn}"
export SUPERSET_CONFIG_PATH="${SUPERSET_CONFIG_PATH:-/app/superset_config.py}"
export FLASK_APP="superset.app:create_app()"

log() { echo "[superset-bootstrap] $*"; }

case "$MODE" in

  # ── Initialisation (runs once at stack start) ──────────────────────────────
  init)
    log "Mode: init"

    # 1. Create the 'superset' database in postgres-meta if it does not exist.
    #    Connect to the default 'postgres' DB first (guaranteed to exist).
    log "Ensuring superset database exists in ${DATABASE_HOST}:${DATABASE_PORT}..."
    PGPASSWORD="${DATABASE_PASSWORD}" psql \
        -h "${DATABASE_HOST}" \
        -p "${DATABASE_PORT}" \
        -U "${DATABASE_USER}" \
        -d postgres \
        -tc "SELECT 1 FROM pg_database WHERE datname='${DATABASE_DB}'" \
        | grep -q 1 \
    || PGPASSWORD="${DATABASE_PASSWORD}" psql \
        -h "${DATABASE_HOST}" \
        -p "${DATABASE_PORT}" \
        -U "${DATABASE_USER}" \
        -d postgres \
        -c "CREATE DATABASE ${DATABASE_DB} OWNER ${DATABASE_USER}"
    log "Database '${DATABASE_DB}' is ready."

    # 2. Run Alembic migrations (idempotent — safe to re-run)
    log "Running Superset DB migrations..."
    superset db upgrade

    # 3. Create admin user (exits 0 if already exists)
    log "Creating admin user (admin / admin)..."
    superset fab create-admin \
        --username  admin \
        --firstname EBC \
        --lastname  Admin \
        --email     admin@ebc.eg \
        --password  admin \
    2>&1 | grep -v "already exists" || true

    # 4. Initialise roles and permissions
    log "Initialising Superset roles and permissions..."
    superset init

    # 5. Register Trino schema connections (one per medallion namespace)
    log "Registering Trino database connections..."
    python3 /app/register_databases.py

    log "Superset initialisation complete."
    ;;

  # ── Production web server ──────────────────────────────────────────────────
  app-gunicorn)
    log "Mode: app-gunicorn (Superset web server on :8088)"
    exec gunicorn \
        --bind            "0.0.0.0:8088" \
        --workers         4 \
        --worker-class    gthread \
        --threads         20 \
        --timeout         120 \
        --keep-alive      5 \
        --limit-request-line 0 \
        --limit-request-field_size 0 \
        --access-logfile  - \
        --error-logfile   - \
        "superset.app:create_app()"
    ;;

  # ── Celery worker (async SQL Lab + scheduled reports) ─────────────────────
  worker)
    log "Mode: worker (Celery)"
    exec celery \
        --app=superset.tasks.celery_app:app \
        worker \
        --pool=prefork \
        --concurrency=4 \
        --max-tasks-per-child=128 \
        --loglevel=info
    ;;

  *)
    echo "Unknown mode: $MODE"
    echo "Usage: $0 [init|app-gunicorn|worker]"
    exit 1
    ;;
esac
