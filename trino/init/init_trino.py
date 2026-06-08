#!/usr/bin/env python3
# =============================================================================
# trino/init/init_trino.py
#
# Idempotent Trino DDL bootstrap for the EBC Lakehouse. Plays the same role
# the old ClickHouse 01_create_databases.sql script used to play, but in the
# Trino + Polaris + Iceberg stack:
#
#   • Creates the four medallion schemas (bronze/silver/gold/serving)
#   • Pre-creates Serving-layer Iceberg tables with stable BI-facing DDL
#     so dbt's incremental MERGE can upsert into them on every Gold run
#   • Pre-creates operational tables (pipeline_metrics) that Airflow DAGs
#     write to as plain INSERTs
#   • Validates that every required schema + operational table exists
#
# It does NOT pre-create Bronze (owned by Kafka Connect) or Silver/Gold
# (owned by dbt as full-rebuild table materializations). See the per-layer
# reference SQL files for an explanation of each ownership boundary.
#
# Re-runs are safe — every DDL statement is CREATE … IF NOT EXISTS.
#
# Exits non-zero only if a validation query returns rows, so it can gate
# downstream services (Airflow scheduler, DataHub ingestion) cleanly.
# =============================================================================
from __future__ import annotations

import os
import pathlib
import sys
import time

from trino.dbapi import connect
from trino.exceptions import TrinoUserError

TRINO_HOST    = os.environ.get("TRINO_HOST",    "trino")
TRINO_PORT    = int(os.environ.get("TRINO_PORT", "8080"))
TRINO_USER    = os.environ.get("TRINO_USER",    "ebc_user")
TRINO_CATALOG = os.environ.get("TRINO_CATALOG", "iceberg")

SQL_DIR       = pathlib.Path(__file__).parent / "sql"
WAIT_TIMEOUT  = int(os.environ.get("TRINO_WAIT_TIMEOUT", "180"))

# Files starting with these prefixes execute DDL; "99_validate" runs the
# validation suite. Reference-only files (10_bronze_reference, 20_silver_…,
# 30_gold_…) emit a single SELECT marker — harmless, useful for logging.
DDL_PREFIXES        = ("00_", "10_", "20_", "30_", "40_")
VALIDATION_PREFIX   = "99_"


def _connect():
    """Connect as the dbt/Airflow operations user so all RBAC paths apply uniformly."""
    return connect(
        host        = TRINO_HOST,
        port        = TRINO_PORT,
        user        = TRINO_USER,
        catalog     = TRINO_CATALOG,
        http_scheme = "http",
    )


def wait_for_trino() -> None:
    """Block until Trino's coordinator accepts at least one query."""
    deadline = time.time() + WAIT_TIMEOUT
    last_err: Exception | None = None
    while time.time() < deadline:
        try:
            conn = _connect()
            cur  = conn.cursor()
            cur.execute("SELECT 1")
            cur.fetchall()
            print(f"  ✓ Trino reachable at {TRINO_HOST}:{TRINO_PORT}", flush=True)
            return
        except Exception as exc:
            last_err = exc
            time.sleep(5)
    raise SystemExit(
        f"Trino did not become reachable within {WAIT_TIMEOUT}s "
        f"(last error: {last_err})"
    )


def _split_statements(sql: str) -> list[str]:
    """
    Naive splitter: strip line-comments, split on top-level `;`. Sufficient
    for the small, well-controlled DDL files we ship. We deliberately keep
    statements simple (no string literals containing semicolons) so this
    splitter doesn't need a SQL parser.
    """
    stripped_lines = []
    for line in sql.splitlines():
        s = line.lstrip()
        if s.startswith("--") or not s.strip():
            continue
        stripped_lines.append(line)
    body = "\n".join(stripped_lines)
    return [stmt.strip() for stmt in body.split(";") if stmt.strip()]


def execute_file(conn, path: pathlib.Path, *, collect_issues: bool = False) -> list[str]:
    """
    Execute every statement in the file. Returns a list of issue strings if
    `collect_issues=True` — used by validation files where each row of the
    result indicates a failed invariant.
    """
    print(f"  ▶  {path.name}", flush=True)
    issues: list[str] = []
    cur = conn.cursor()
    for stmt in _split_statements(path.read_text(encoding="utf-8")):
        try:
            cur.execute(stmt)
            rows = cur.fetchall()
        except TrinoUserError as exc:
            # CREATE … IF NOT EXISTS shouldn't raise, but other DDL might
            # (e.g. RBAC blocking). Re-raise with file context.
            raise SystemExit(f"  ✗ {path.name}: {exc}") from exc

        if collect_issues:
            for row in rows:
                # Validation rows are single-column issue strings
                issues.append(str(row[0]))
        else:
            # Reference files emit a status marker — surface it.
            if rows and len(rows[0]) == 1 and rows[0][0]:
                print(f"     · {rows[0][0]}", flush=True)
    return issues


def main() -> int:
    print(f"Trino DDL bootstrap starting (catalog={TRINO_CATALOG}, user={TRINO_USER})", flush=True)
    wait_for_trino()
    conn = _connect()

    print("\n── Phase 1 / DDL ──────────────────────────────────────────────", flush=True)
    for path in sorted(SQL_DIR.glob("*.sql")):
        if path.name.startswith(DDL_PREFIXES):
            execute_file(conn, path)

    print("\n── Phase 2 / Validation ──────────────────────────────────────", flush=True)
    all_issues: list[str] = []
    for path in sorted(SQL_DIR.glob("*.sql")):
        if path.name.startswith(VALIDATION_PREFIX):
            all_issues.extend(execute_file(conn, path, collect_issues=True))

    print("\n── Summary ────────────────────────────────────────────────────", flush=True)
    if all_issues:
        print(f"  ✗ {len(all_issues)} validation issue(s):", flush=True)
        for issue in all_issues:
            print(f"     · {issue}", flush=True)
        return 1

    # One-line happy-path summary
    cur = conn.cursor()
    cur.execute(
        "SELECT table_schema, count(*) AS tables "
        "FROM iceberg.information_schema.tables "
        "WHERE table_schema IN ('bronze','silver','gold','serving') "
        "GROUP BY table_schema ORDER BY 1"
    )
    layout = {row[0]: row[1] for row in cur.fetchall()}
    counts = " · ".join(
        f"{layer}={layout.get(layer, 0)}" for layer in ("bronze", "silver", "gold", "serving")
    )
    print(f"  ✓ Schemas + operational tables OK. Tables per layer: {counts}", flush=True)
    print("\nTrino DDL bootstrap complete.", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
