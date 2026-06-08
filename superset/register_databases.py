#!/usr/bin/env python3
# =============================================================================
# superset/register_databases.py
# Pre-register the four EBC Trino schemas in Superset so analysts can query
# Silver / Gold / Serving / Semantic immediately without manual UI setup.
#
# Run via: superset-init container (docker-bootstrap.sh init)
# =============================================================================
import os
import sys

os.environ.setdefault("SUPERSET_CONFIG_PATH", "/app/pythonpath/superset_config.py")
os.environ.setdefault("FLASK_ENV", "production")

# Trino connection coordinates injected by docker-compose.
TRINO_HOST    = os.environ.get("TRINO_HOST",    "trino")
TRINO_PORT    = os.environ.get("TRINO_PORT",    "8080")
TRINO_USER    = os.environ.get("TRINO_USER",    "ebc_user")
TRINO_CATALOG = os.environ.get("TRINO_CATALOG", "iceberg")


def _uri(schema: str) -> str:
    # Trino SQLAlchemy URL. No password — single-tenant local Trino with auth=none.
    return f"trino://{TRINO_USER}@{TRINO_HOST}:{TRINO_PORT}/{TRINO_CATALOG}/{schema}"


DATABASES = [
    {
        "database_name": "EBC Gold — Business Marts",
        "sqlalchemy_uri": _uri("gold"),
        "description": (
            "Gold layer Iceberg tables on Polaris: mart_daily_txn_volume, "
            "mart_scheme_performance, mart_settlement_summary — aggregated KPIs "
            "ready for dashboards. Queried via Trino."
        ),
    },
    {
        "database_name": "EBC Serving — KPI Cache",
        "sqlalchemy_uri": _uri("serving"),
        "description": (
            "Serving layer: Iceberg tables refreshed by Airflow MERGE after every "
            "Gold run. Optimised + snapshot-expired weekly. Hosts pipeline_metrics."
        ),
    },
    {
        "database_name": "EBC Silver — Staging",
        "sqlalchemy_uri": _uri("silver"),
        "description": (
            "Silver layer: cleansed and deduplicated staging Iceberg tables "
            "(stg_ach_transactions, stg_meeza_authorisations, stg_ipn_transactions, "
            "stg_meeza_digital_wallet, stg_atm_sessions). Incremental MERGE upserts."
        ),
    },
    {
        "database_name": "EBC Semantic — MetricFlow",
        "sqlalchemy_uri": _uri("ebc_semantic"),
        "description": (
            "Semantic layer: dbt MetricFlow time-spine and semantic objects. "
            "Use for metric-level queries consistent with the dbt semantic layer."
        ),
    },
]

EXTRA = (
    '{"engine_params":{"connect_args":{"http_scheme":"http"}},'
    '"metadata_params":{},'
    '"schemas_allowed_for_file_upload":[]}'
)


def main() -> None:
    try:
        from superset import create_app
        from superset.extensions import db
        from superset.models.core import Database
    except ImportError as exc:
        print(f"ERROR: cannot import Superset — {exc}", file=sys.stderr)
        sys.exit(1)

    app = create_app()
    with app.app_context():
        registered = skipped = 0
        for cfg in DATABASES:
            name = cfg["database_name"]
            existing = db.session.query(Database).filter_by(database_name=name).first()
            if existing:
                # Refresh the URI in case the Trino host moved between deploys.
                existing.sqlalchemy_uri = cfg["sqlalchemy_uri"]
                existing.extra          = EXTRA
                db.session.commit()
                print(f"  [=] Refreshed: {name}")
                skipped += 1
                continue

            db.session.add(Database(
                database_name                     = cfg["database_name"],
                sqlalchemy_uri                    = cfg["sqlalchemy_uri"],
                expose_in_sqllab                  = True,
                allow_run_async                   = True,
                allow_dml                         = False,
                allow_multi_schema_metadata_fetch = False,
                extra                             = EXTRA,
            ))
            db.session.commit()
            print(f"  [+] Registered: {name}  →  {cfg['sqlalchemy_uri']}")
            registered += 1

        print(f"\nDone. Registered: {registered}  Refreshed: {skipped}")


if __name__ == "__main__":
    main()
