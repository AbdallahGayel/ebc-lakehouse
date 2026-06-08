#!/usr/bin/env python3
# =============================================================================
# polaris/catalog-init/init_polaris.py
#
# Bootstraps the EBC Lakehouse catalog inside Apache Polaris on first run.
# Idempotent: re-runs detect existing catalog/namespaces and skip cleanly.
#
# Storage layout enforced here (one MinIO prefix per medallion layer):
#
#   ebc_bronze   →  s3://ebc-lakehouse/bronze/
#   ebc_silver   →  s3://ebc-lakehouse/silver/
#   ebc_gold     →  s3://ebc-lakehouse/gold/
#   ebc_serving  →  s3://ebc-lakehouse/serving/
#
# The namespace name stays the canonical `ebc_*` identifier so dbt/Trino
# references don't change; only the on-disk path is the short layer name.
# Tables auto-created by Kafka Connect / dbt inherit the namespace location.
#
# Self-healing behaviour:
#   • If the catalog is missing `stsUnavailable=true` → recreate it.
#   • If a namespace exists but its `location` doesn't match the target
#     short-name path → recreate it (dropping tables, which Kafka Connect
#     and dbt will re-materialise at the new location on next run).
# =============================================================================
from __future__ import annotations

import json
import os
import sys
import time
import urllib.parse

import requests

BASE_URL      = os.environ.get("POLARIS_BASE_URL",     "http://polaris:8181").rstrip("/")
REALM         = os.environ.get("POLARIS_REALM",        "POLARIS")
CLIENT_ID     = os.environ.get("POLARIS_CLIENT_ID",    "root")
CLIENT_SECRET = os.environ.get("POLARIS_CLIENT_SECRET","s3cr3t")
CATALOG_NAME  = os.environ.get("POLARIS_CATALOG",      "ebc_lakehouse")
WAREHOUSE     = os.environ.get("WAREHOUSE_LOCATION",   "s3://ebc-lakehouse/")
MINIO_EP      = os.environ.get("MINIO_ENDPOINT",       "http://minio:9000")
AWS_KEY       = os.environ.get("AWS_ACCESS_KEY_ID",    "minioadmin")
AWS_SECRET    = os.environ.get("AWS_SECRET_ACCESS_KEY","minioadmin")
AWS_REGION    = os.environ.get("AWS_REGION",           "us-east-1")

HEALTH_URL    = BASE_URL.replace(":8181", ":8182") + "/q/health/ready"
TOKEN_URL     = f"{BASE_URL}/api/catalog/v1/oauth/tokens"
MGMT_URL      = f"{BASE_URL}/api/management/v1"

# (namespace, description) — the namespace IS the layer name. Polaris derives
# storage paths as <warehouse>/<namespace>/, so naming the namespace `bronze`
# puts data at s3://ebc-lakehouse/bronze/. (We previously used `ebc_bronze`
# but Polaris rejects any location override that doesn't have the namespace
# name as the final path component — only the default derivation is allowed.)
MEDALLION_LAYERS = [
    ("bronze",  "Raw CDC data — written by Kafka Iceberg Sink connectors"),
    ("silver",  "Cleansed/deduplicated staging tables produced by dbt Silver models"),
    ("gold",    "Business-aggregate mart tables produced by dbt Gold models"),
    ("serving", "BI-optimised serving layer materialised by dbt + Airflow MERGE"),
]


def _layer_location(namespace: str) -> str:
    """Default Polaris-derived path — guaranteed to satisfy its location check."""
    return f"{WAREHOUSE.rstrip('/')}/{namespace}/"


def wait_for_polaris(timeout_s: int = 180) -> None:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        try:
            r = requests.get(HEALTH_URL, timeout=5)
            if r.status_code == 200:
                print(f"  ✓ Polaris is ready at {BASE_URL}", flush=True)
                return
        except requests.RequestException as exc:
            print(f"    Polaris not ready ({exc}). Retrying…", flush=True)
        time.sleep(5)
    raise SystemExit(f"Polaris did not become ready within {timeout_s}s ({HEALTH_URL})")


def fetch_token() -> str:
    body = urllib.parse.urlencode({
        "grant_type":    "client_credentials",
        "client_id":     CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "scope":         "PRINCIPAL_ROLE:ALL",
    })
    r = requests.post(
        TOKEN_URL, data=body,
        headers={"Content-Type": "application/x-www-form-urlencoded", "Polaris-Realm": REALM},
        timeout=10,
    )
    r.raise_for_status()
    print(f"  ✓ Acquired OAuth2 token for principal '{CLIENT_ID}'", flush=True)
    return r.json()["access_token"]


def auth_headers(token: str) -> dict[str, str]:
    return {
        "Authorization":  f"Bearer {token}",
        "Polaris-Realm":  REALM,
        "Content-Type":   "application/json",
        "Accept":         "application/json",
    }


# ── Catalog lifecycle ────────────────────────────────────────────────────────
def _catalog_needs_recreate(token: str) -> bool:
    r = requests.get(
        f"{MGMT_URL}/catalogs/{CATALOG_NAME}",
        headers=auth_headers(token), timeout=10,
    )
    if r.status_code == 404:
        return False
    r.raise_for_status()
    return not r.json().get("storageConfigInfo", {}).get("stsUnavailable", False)


def _delete_catalog(token: str) -> None:
    base = f"{BASE_URL}/api/catalog/v1/{CATALOG_NAME}/namespaces"
    ns_resp = requests.get(base, headers=auth_headers(token), timeout=10)
    if ns_resp.status_code == 200:
        for ns in ns_resp.json().get("namespaces", []):
            name = ".".join(ns)
            _drop_namespace_with_tables(token, name)
    d = requests.delete(
        f"{MGMT_URL}/catalogs/{CATALOG_NAME}",
        headers=auth_headers(token), timeout=15,
    )
    if d.status_code in (200, 204):
        print(f"  - Dropped catalog '{CATALOG_NAME}'", flush=True)


def ensure_catalog(token: str) -> None:
    if _catalog_needs_recreate(token):
        print(f"  ! Catalog '{CATALOG_NAME}' missing stsUnavailable — recreating", flush=True)
        _delete_catalog(token)

    url = f"{MGMT_URL}/catalogs"
    existing = requests.get(url, headers=auth_headers(token), timeout=10)
    existing.raise_for_status()
    if CATALOG_NAME in {c["name"] for c in existing.json().get("catalogs", [])}:
        print(f"  = Catalog '{CATALOG_NAME}' already exists with correct config", flush=True)
        return

    body = {
        "catalog": {
            "name": CATALOG_NAME,
            "type": "INTERNAL",
            "properties": {
                "default-base-location": WAREHOUSE,
                "s3.endpoint":           MINIO_EP,
                "s3.path-style-access":  "true",
                "s3.access-key-id":      AWS_KEY,
                "s3.secret-access-key":  AWS_SECRET,
                "s3.region":             AWS_REGION,
            },
            "storageConfigInfo": {
                "storageType":      "S3",
                "allowedLocations": [WAREHOUSE.rstrip("/") + "/*"],
                "endpoint":         MINIO_EP,
                "region":           AWS_REGION,
                "pathStyleAccess":  True,
                "stsUnavailable":   True,
            },
        }
    }
    r = requests.post(url, headers=auth_headers(token), data=json.dumps(body), timeout=15)
    if r.status_code not in (200, 201):
        raise SystemExit(f"Failed to create catalog: HTTP {r.status_code} — {r.text}")
    print(f"  + Created catalog '{CATALOG_NAME}' (stsUnavailable=true) → {WAREHOUSE}", flush=True)


# ── Role lifecycle ───────────────────────────────────────────────────────────
# Catalog-role layout (mirrored on the Trino side by trino/etc/rules.json):
#
#   catalog_admin → full CRUD on the catalog. Bound to service_admin (root).
#   engineer      → CATALOG_MANAGE_CONTENT (used by dbt-trino + Airflow + Connect).
#   bi_reader     → CATALOG_READ_PROPERTIES + TABLE_READ_DATA (read-only).
#
# We bind every role to service_admin in dev so the single `root` principal
# can act in either capacity. For prod, create separate Polaris principals
# (ebc_engineer / ebc_bi) and bind them to their respective principal roles.
CATALOG_ROLES = [
    ("catalog_admin", ["CATALOG_MANAGE_CONTENT"]),
    ("engineer",      ["CATALOG_MANAGE_CONTENT"]),
    ("bi_reader",     ["CATALOG_READ_PROPERTIES", "TABLE_READ_DATA"]),
]


def _ensure_catalog_role(token: str, role_name: str, privileges: list[str]) -> None:
    role_url = f"{MGMT_URL}/catalogs/{CATALOG_NAME}/catalog-roles/{role_name}"
    if requests.get(role_url, headers=auth_headers(token), timeout=10).status_code == 404:
        r = requests.post(
            f"{MGMT_URL}/catalogs/{CATALOG_NAME}/catalog-roles",
            headers=auth_headers(token),
            data=json.dumps({"catalogRole": {"name": role_name}}),
            timeout=10,
        )
        if r.status_code in (200, 201):
            print(f"  + Created catalog role '{role_name}'", flush=True)
    for priv in privileges:
        requests.put(
            f"{MGMT_URL}/catalogs/{CATALOG_NAME}/catalog-roles/{role_name}/grants",
            headers=auth_headers(token),
            data=json.dumps({"grant": {"type": "catalog", "privilege": priv}}),
            timeout=10,
        )
    # Bind to service_admin so the root principal inherits the role in dev.
    requests.put(
        f"{MGMT_URL}/principal-roles/service_admin/catalog-roles/{CATALOG_NAME}",
        headers=auth_headers(token),
        data=json.dumps({"catalogRole": {"name": role_name}}),
        timeout=10,
    )


def ensure_admin_grant(token: str) -> None:
    for name, privileges in CATALOG_ROLES:
        _ensure_catalog_role(token, name, privileges)
    print("  + Catalog roles: catalog_admin · engineer · bi_reader (bound to root)", flush=True)


# ── Namespace lifecycle (with location enforcement) ──────────────────────────
def _drop_namespace_with_tables(token: str, namespace: str) -> None:
    base = f"{BASE_URL}/api/catalog/v1/{CATALOG_NAME}/namespaces"
    t = requests.get(f"{base}/{namespace}/tables", headers=auth_headers(token), timeout=10)
    if t.status_code == 200:
        for ident in t.json().get("identifiers", []):
            requests.delete(
                f"{base}/{namespace}/tables/{ident['name']}?purgeRequested=true",
                headers=auth_headers(token), timeout=15,
            )
    requests.delete(f"{base}/{namespace}", headers=auth_headers(token), timeout=10)
    print(f"  - Dropped namespace '{namespace}' (with all tables)", flush=True)


def _namespace_location(token: str, namespace: str) -> str | None:
    r = requests.get(
        f"{BASE_URL}/api/catalog/v1/{CATALOG_NAME}/namespaces/{namespace}",
        headers=auth_headers(token), timeout=10,
    )
    if r.status_code == 404:
        return None
    r.raise_for_status()
    return (r.json().get("properties") or {}).get("location")


def ensure_namespaces(token: str) -> None:
    base = f"{BASE_URL}/api/catalog/v1/{CATALOG_NAME}/namespaces"

    for ns_name, description in MEDALLION_LAYERS:
        target_loc = _layer_location(ns_name)
        current_loc = _namespace_location(token, ns_name)

        if current_loc is not None and current_loc.rstrip("/") + "/" != target_loc:
            print(
                f"  ! Namespace '{ns_name}' location {current_loc} ≠ target "
                f"{target_loc} — recreating",
                flush=True,
            )
            _drop_namespace_with_tables(token, ns_name)
            current_loc = None

        if current_loc is not None:
            print(f"  = Namespace '{ns_name}' already at {target_loc}", flush=True)
            continue

        body = {
            "namespace":  [ns_name],
            "properties": {
                "owner":       "ebc-data-engineering",
                "description": description,
                "location":    target_loc,
                # Surface the layer name as a queryable property — used by
                # validation SQL to assert storage-layout correctness.
                "medallion-layer": ns_name,
            },
        }
        r = requests.post(base, headers=auth_headers(token), data=json.dumps(body), timeout=10)
        if r.status_code not in (200, 201):
            raise SystemExit(f"Failed to create namespace {ns_name}: HTTP {r.status_code} — {r.text}")
        print(f"  + Created namespace '{ns_name}' → {target_loc}", flush=True)


def summarise(token: str) -> None:
    base = f"{BASE_URL}/api/catalog/v1/{CATALOG_NAME}/namespaces"
    r = requests.get(base, headers=auth_headers(token), timeout=10)
    r.raise_for_status()
    print("\n── Registered namespaces ──────────────────────────────────────", flush=True)
    for ns in sorted(r.json().get("namespaces", []), key=lambda x: x[0]):
        name = ".".join(ns)
        loc  = _namespace_location(token, name) or "<no location>"
        tbl_url = f"{base}/{name}/tables"
        tr = requests.get(tbl_url, headers=auth_headers(token), timeout=10)
        tbl_count = len(tr.json().get("identifiers", [])) if tr.status_code == 200 else 0
        print(f"  {name:<14} → {loc:<40} ({tbl_count} table(s))", flush=True)


def main() -> None:
    print(f"Polaris bootstrap starting (catalog={CATALOG_NAME}, realm={REALM})", flush=True)
    wait_for_polaris()
    token = fetch_token()
    ensure_catalog(token)
    ensure_admin_grant(token)
    ensure_namespaces(token)
    summarise(token)
    print("\nPolaris catalog bootstrap complete.", flush=True)


if __name__ == "__main__":
    try:
        main()
    except requests.HTTPError as exc:
        print(f"HTTP error during bootstrap: {exc} — body: {exc.response.text}", file=sys.stderr)
        sys.exit(1)
