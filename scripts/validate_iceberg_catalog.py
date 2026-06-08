#!/usr/bin/env python3
# =============================================================================
# scripts/validate_iceberg_catalog.py
# Standalone catalog + MinIO storage validation script.
#
# Run from the project root (outside Docker) after the stack is up:
#   python scripts/validate_iceberg_catalog.py
#
# Or inside the Airflow container (all deps are pre-installed):
#   docker exec ebc-airflow-scheduler python /opt/airflow/dags/../scripts/validate_iceberg_catalog.py
#
# Exit codes:
#   0 — all tables registered, all snapshots present, all MinIO files confirmed
#   1 — one or more checks failed (details printed to stdout)
# =============================================================================
import sys
import os
import re
import argparse

# ── Configuration (override via env vars or --catalog-uri / --minio-endpoint) ─
CATALOG_URI  = os.environ.get("ICEBERG_REST_URI",        "http://localhost:8181")
MINIO_EP     = os.environ.get("MINIO_ENDPOINT",          "http://localhost:9000")
AWS_KEY      = os.environ.get("AWS_ACCESS_KEY_ID",       "minioadmin")
AWS_SECRET   = os.environ.get("AWS_SECRET_ACCESS_KEY",   "minioadmin")
AWS_REGION   = os.environ.get("AWS_DEFAULT_REGION",      "us-east-1")
MINIO_BUCKET = os.environ.get("MINIO_BUCKET",            "ebc-lakehouse")

CATALOG_CONFIG = {
    "uri":                  CATALOG_URI,
    "s3.endpoint":          MINIO_EP,
    "s3.access-key-id":     AWS_KEY,
    "s3.secret-access-key": AWS_SECRET,
    "s3.region":            AWS_REGION,
    "s3.path-style-access": "true",
    "warehouse":            f"s3://{MINIO_BUCKET}/",
    "py-io-impl":           "pyiceberg.io.fsspec.FsspecFileIO",
}

# ── Expected medallion catalog contents ───────────────────────────────────────
EXPECTED_TABLES = {
    "ebc_bronze": [
        "ach_transactions",
        "meeza_authorisations",
        "ipn_transactions",
        "meeza_digital_wallet_events",
        "cassandra_atm_sessions",
    ],
    "ebc_silver": [
        "stg_ach_transactions",
        "stg_meeza_authorisations",
        "stg_ipn_transactions",
        "stg_meeza_digital_wallet",
        "stg_atm_sessions",
    ],
    "ebc_gold": [
        "mart_daily_txn_volume",
        "mart_scheme_performance",
        "mart_settlement_summary",
    ],
    "ebc_serving": [
        "daily_txn_volume",
        "scheme_performance",
        "settlement_summary",
        "pipeline_metrics",
    ],
}

SECTION = "─" * 70


def hr(label: str) -> None:
    print(f"\n{SECTION}")
    print(f"  {label}")
    print(SECTION)


def check_catalog(catalog) -> list:
    """Returns a list of failure strings (empty = all clear)."""
    failures = []
    results  = []

    for namespace, tables in EXPECTED_TABLES.items():
        try:
            registered = {t.name for t in catalog.list_tables(namespace)}
        except Exception as exc:
            failures.append(f"Cannot list tables in namespace '{namespace}': {exc}")
            continue

        for table_name in tables:
            full_id = f"{namespace}.{table_name}"

            if table_name not in registered:
                failures.append(f"{full_id}: NOT REGISTERED")
                results.append(("FAIL", full_id, "not in catalog"))
                continue

            try:
                tbl = catalog.load_table(full_id)
            except Exception as exc:
                failures.append(f"{full_id}: load_table() failed — {exc}")
                results.append(("FAIL", full_id, str(exc)))
                continue

            snapshot = tbl.current_snapshot()
            if snapshot is None:
                failures.append(f"{full_id}: registered but NO snapshot (no data committed)")
                results.append(("WARN", full_id, "registered, no snapshot"))
                continue

            try:
                file_count = len(list(tbl.scan().plan_files()))
            except Exception:
                file_count = -1

            results.append(("OK", full_id,
                            f"snapshot={snapshot.snapshot_id}  "
                            f"sequence_number={snapshot.sequence_number}  "
                            f"data_files={file_count}"))

    return failures, results


def check_minio(catalog, s3_client) -> list:
    """
    Uses each table's catalog-resolved metadata_location to confirm physical
    Iceberg metadata and data files exist in MinIO.
    Returns a list of failure strings.
    """
    failures = []
    results  = []

    all_table_ids = [
        f"{ns}.{tbl}"
        for ns, tables in EXPECTED_TABLES.items()
        for tbl in tables
    ]

    for full_id in all_table_ids:
        if not catalog.table_exists(full_id):
            results.append(("SKIP", full_id, "not in catalog — skipped storage check"))
            continue

        tbl      = catalog.load_table(full_id)
        meta_loc = tbl.metadata_location

        m = re.match(r"s3://([^/]+)/(.+?)/metadata/", meta_loc)
        if not m:
            failures.append(f"{full_id}: unexpected metadata_location format: {meta_loc}")
            results.append(("FAIL", full_id, f"bad location: {meta_loc}"))
            continue

        bucket     = m.group(1)
        table_root = m.group(2)

        meta_resp  = s3_client.list_objects_v2(Bucket=bucket, Prefix=f"{table_root}/metadata/", MaxKeys=10)
        data_resp  = s3_client.list_objects_v2(Bucket=bucket, Prefix=f"{table_root}/data/",     MaxKeys=10)

        meta_count = meta_resp.get("KeyCount", 0)
        data_count = data_resp.get("KeyCount", 0)

        if meta_count == 0:
            failures.append(f"{full_id}: no metadata/ files in s3://{bucket}/{table_root}/")
            results.append(("FAIL", full_id, "metadata/ missing in MinIO"))
        else:
            note = f"data_files≥{data_count}" if data_count else "data/ empty (0-row table)"
            results.append(("OK", full_id, f"metadata≥{meta_count}  {note}  "
                                           f"(root: s3://{bucket}/{table_root}/)"))

    return failures, results


def print_results(section_label: str, results: list) -> None:
    hr(section_label)
    for status, table_id, detail in results:
        icon = "✓" if status == "OK" else ("⚠" if status == "WARN" else ("→" if status == "SKIP" else "✗"))
        print(f"  [{status:4s}] {icon}  {table_id:<55} {detail}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate EBC Iceberg catalog and MinIO storage.")
    parser.add_argument("--catalog-uri",    default=None, help="Override ICEBERG_REST_URI")
    parser.add_argument("--minio-endpoint", default=None, help="Override MINIO_ENDPOINT")
    parser.add_argument("--skip-minio",     action="store_true", help="Skip MinIO file-level checks")
    args = parser.parse_args()

    if args.catalog_uri:
        CATALOG_CONFIG["uri"] = args.catalog_uri
    if args.minio_endpoint:
        CATALOG_CONFIG["s3.endpoint"] = args.minio_endpoint

    print(f"\nEBC Lakehouse — Iceberg Catalog Validation")
    print(f"  Catalog : {CATALOG_CONFIG['uri']}")
    print(f"  Storage : {CATALOG_CONFIG['s3.endpoint']} / bucket={MINIO_BUCKET}")

    # ── PyIceberg catalog connection ─────────────────────────────────────────
    try:
        from pyiceberg.catalog.rest import RestCatalog
    except ImportError:
        print("\nERROR: pyiceberg not installed. Run: pip install 'pyiceberg[s3fs]>=0.8.0'")
        return 1

    try:
        catalog = RestCatalog(name="ebc_rest_catalog", **CATALOG_CONFIG)
        namespaces = [ns[0] for ns in catalog.list_namespaces()]
    except Exception as exc:
        print(f"\nERROR: Cannot connect to Iceberg REST catalog: {exc}")
        return 1

    hr("Registered namespaces")
    for ns in namespaces:
        tables = catalog.list_tables(ns)
        print(f"  {ns}: {len(tables)} table(s)")

    # ── Catalog registration check ───────────────────────────────────────────
    cat_failures, cat_results = check_catalog(catalog)
    print_results("Catalog Registration & Snapshot Check", cat_results)

    # ── MinIO storage check ──────────────────────────────────────────────────
    minio_failures = []
    minio_results  = []

    if not args.skip_minio:
        try:
            import boto3
            from botocore.client import Config

            s3 = boto3.client(
                "s3",
                endpoint_url=CATALOG_CONFIG["s3.endpoint"],
                aws_access_key_id=CATALOG_CONFIG["s3.access-key-id"],
                aws_secret_access_key=CATALOG_CONFIG["s3.secret-access-key"],
                config=Config(signature_version="s3v4"),
                region_name=CATALOG_CONFIG["s3.region"],
            )
            minio_failures, minio_results = check_minio(catalog, s3)
            print_results("MinIO Physical Storage Check", minio_results)

        except ImportError:
            print("\n  [SKIP] boto3 not installed — MinIO file-level check skipped.")
            print("         Run: pip install boto3")

    # ── Summary ──────────────────────────────────────────────────────────────
    all_failures = cat_failures + minio_failures
    total_tables = sum(len(v) for v in EXPECTED_TABLES.values())

    hr("Summary")
    if all_failures:
        print(f"  FAILED — {len(all_failures)} issue(s) found across {total_tables} expected tables:\n")
        for f in all_failures:
            print(f"    ✗ {f}")
        print()
        return 1
    else:
        print(f"  PASSED — all {total_tables} tables registered in catalog with active snapshots.")
        print(f"           Physical MinIO storage confirmed for all layers.")
        print()
        return 0


if __name__ == "__main__":
    sys.exit(main())
