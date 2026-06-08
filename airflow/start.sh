#!/bin/bash
# =============================================================================
# airflow/start.sh
# Entrypoint for every Airflow container in the Core compose module.
#
# Modes (driven by docker compose `command:`):
#   init               — db migrate + admin user, then exit 0
#   scheduler / webserver / worker / …  — exec airflow <subcommand>
#   (no args)          — fall back to scheduler
# =============================================================================
set -euo pipefail

# ── PATH hardening ────────────────────────────────────────────────────────────
# The apache/airflow:2.10.x base image installs the CLI at
# /home/airflow/.local/bin/airflow. Older PATHs (or a stripped-down ENTRYPOINT
# context) sometimes lose that directory, so we prepend it defensively and
# also resolve the absolute path below.
export PATH="/home/airflow/.local/bin:/usr/local/bin:${PATH:-/usr/bin:/bin}"

# ── Locate the airflow binary ─────────────────────────────────────────────────
# `command -v` first (honours PATH), then fall back to known install paths.
# If we still can't find it, dump a diagnostic and exit — failing fast here is
# much friendlier than letting `airflow: command not found` loop forever.
AIRFLOW_BIN="$(command -v airflow || true)"
if [[ -z "${AIRFLOW_BIN}" ]]; then
    for candidate in \
        /home/airflow/.local/bin/airflow \
        /usr/local/bin/airflow \
        /opt/airflow/.local/bin/airflow; do
        if [[ -x "${candidate}" ]]; then
            AIRFLOW_BIN="${candidate}"
            break
        fi
    done
fi

if [[ -z "${AIRFLOW_BIN}" ]]; then
    echo "ERROR: airflow CLI not found. Searched PATH=${PATH} and standard install dirs." >&2
    echo "Hint: rebuild the airflow image (docker compose -f compose/docker-compose.core.yml build airflow-init)" >&2
    exit 127
fi
echo "[start.sh] Using airflow binary at: ${AIRFLOW_BIN}"

# ── Wait for the metadata database ────────────────────────────────────────────
until pg_isready -h postgres-meta -p 5432 -U airflow >/dev/null 2>&1; do
    echo "[start.sh] Waiting for postgres-meta:5432 …"
    sleep 3
done
echo "[start.sh] postgres-meta is accepting connections"

# ── init mode: run migrations + seed the admin user, then exit ────────────────
if [[ "${1:-}" == "init" ]] || [[ "${1:-}" == "version" ]]; then
    echo "[start.sh] Running Airflow migrations and user setup…"
    "${AIRFLOW_BIN}" db migrate

    if ! "${AIRFLOW_BIN}" users list 2>/dev/null | grep -q '^admin '; then
        "${AIRFLOW_BIN}" users create \
            --username  admin \
            --password  admin \
            --firstname EBC \
            --lastname  Admin \
            --role      Admin \
            --email     admin@ebc.eg
    else
        echo "[start.sh] Admin user already exists — skipping create"
    fi

    if [[ "${1:-}" == "init" ]]; then
        echo "[start.sh] init complete"
        exit 0
    fi
fi

# ── Default: run the supplied airflow subcommand (or scheduler) ───────────────
if [[ $# -eq 0 ]]; then
    echo "[start.sh] No command provided — defaulting to scheduler"
    exec "${AIRFLOW_BIN}" scheduler
else
    echo "[start.sh] Executing: airflow $*"
    exec "${AIRFLOW_BIN}" "$@"
fi
