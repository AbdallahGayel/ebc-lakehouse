#!/usr/bin/env bash
# =============================================================================
# scripts/verify.sh
# EBC Lakehouse — end-to-end verification queries
# Runs a set of SQL checks across Bronze → Silver → Gold → Serving in Trino
# Usage: bash scripts/verify.sh
# =============================================================================
set -euo pipefail

GREEN='\033[0;32m'; YELLOW='\033[1;33m'; RED='\033[0;31m'; CYAN='\033[0;36m'; BOLD='\033[1m'; NC='\033[0m'

log()    { echo -e "${GREEN}[INFO]${NC}  $*"; }
warn()   { echo -e "${YELLOW}[WARN]${NC}  $*"; }
header() { echo -e "\n${BOLD}${CYAN}$*${NC}"; echo "──────────────────────────────────────────────────────────"; }

TRINO="docker exec -e JAVA_TOOL_OPTIONS= ebc-trino trino --user ebc_user --catalog iceberg"

run_query() {
    local label="$1"
    local sql="$2"
    echo -e "\n${BOLD}$label${NC}"
    $TRINO --execute "$sql" 2>/dev/null || warn "Query failed or table not yet populated"
}

header "EBC Lakehouse — End-to-End Verification (Trino)"

# ── Bronze Layer ───────────────────────────────────────────────────────────────
header "BRONZE LAYER — Iceberg tables written by Flink CDC Sinks"

run_query "Bronze row counts" "
SELECT
    'ach_transactions'     AS table_name, count(*) AS row_count FROM bronze.ach_transactions
UNION ALL SELECT
    'meeza_authorisations' AS table_name, count(*) AS row_count FROM bronze.meeza_authorisations
UNION ALL SELECT
    'ipn_transactions'     AS table_name, count(*) AS row_count FROM bronze.ipn_transactions
UNION ALL SELECT
    'meeza_digital_wallet_events' AS table_name, count(*) AS row_count FROM bronze.meeza_digital_wallet_events
UNION ALL SELECT
    'atm_sessions'         AS table_name, count(*) AS row_count FROM bronze.atm_sessions
"

# ── Silver Layer ───────────────────────────────────────────────────────────────
header "SILVER LAYER — dbt cleansed and deduplicated"

run_query "Silver row counts" "
SELECT
    'stg_ach_transactions'     AS model, count(*) AS rows FROM silver.stg_ach_transactions
UNION ALL SELECT
    'stg_meeza_authorisations' AS model, count(*) AS rows FROM silver.stg_meeza_authorisations
UNION ALL SELECT
    'stg_ipn_transactions'     AS model, count(*) AS rows FROM silver.stg_ipn_transactions
UNION ALL SELECT
    'stg_meeza_digital_wallet' AS model, count(*) AS rows FROM silver.stg_meeza_digital_wallet
UNION ALL SELECT
    'stg_atm_sessions'         AS model, count(*) AS rows FROM silver.stg_atm_sessions
"

run_query "Silver Wallet — status distribution" "
SELECT txn_status, count(*) AS cnt, round(sum(amount_egp)/1e6, 2) AS total_egp_millions
FROM silver.stg_meeza_digital_wallet
GROUP BY txn_status ORDER BY cnt DESC
"

# ── Gold Layer ─────────────────────────────────────────────────────────────────
header "GOLD LAYER — dbt business aggregations"

run_query "Gold — daily txn volume (last 7 days)" "
SELECT report_date, scheme,
       txn_count AS txns,
       round(total_amount_egp/1e6, 2)   AS egp_millions,
       round(approved_count * 100.0 / nullif(txn_count,0), 1) AS approval_pct
FROM gold.mart_daily_txn_volume
ORDER BY report_date DESC, scheme LIMIT 21
"

run_query "Gold — ACH net settlement (top 10 bank pairs)" "
SELECT originating_bank_id, receiving_bank_id,
       settled_count, round(net_settled_egp/1e6, 2) AS net_egp_millions,
       settlement_rate_pct
FROM gold.mart_settlement_summary
ORDER BY net_settled_egp DESC LIMIT 10
"

# ── Serving Layer ──────────────────────────────────────────────────────────────
header "SERVING LAYER — Stable BI-facing schemas"

run_query "serving.daily_txn_volume (last 10 rows)" "
SELECT * FROM serving.daily_txn_volume LIMIT 10
"

# ── Flink Job status ───────────────────────────────────────────────────────────
header "FLINK JOB STATUS"

curl -sf "http://localhost:8881/jobs/overview" | \
    python3 -c "
import json, sys
data = json.load(sys.stdin)
for job in data.get('jobs', []):
    state = job.get('state')
    name = job.get('name')
    if state == 'RUNNING':
        print(f'  ${GREEN}✓${NC} {name}: {state}')
    else:
        print(f'  ${RED}✗${NC} {name}: {state}')
" 2>/dev/null || warn "Failed to fetch Flink job status overview"

echo ""
log "Verification complete."