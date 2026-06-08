#!/usr/bin/env bash
# =============================================================================
# scripts/generate_continuous_traffic.sh
# EBC Lakehouse — Continuous Traffic Generator
# Automatically generates fresh CDC events across all 5 databases.
# =============================================================================

GREEN='\033[0;32m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

echo -e "${CYAN}=======================================================${NC}"
echo -e "${BOLD}   EBC Lakehouse - Continuous Traffic Generator${NC}"
echo -e "${CYAN}=======================================================${NC}"
echo -e "This script will push fresh data to all 5 sources every 60 seconds."
echo -e "Press ${RED}[CTRL+C]${NC} at any time to stop it gracefully.\n"

# Trap CTRL+C to exit cleanly
trap "echo -e '\n${GREEN}Stopping traffic generator...${NC}'; exit 0" SIGINT

INTERVAL=60 # Sleep time in seconds

while true; do
    TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S')
    echo -e "${GREEN}[$TIMESTAMP]${NC} Pushing fresh events to Postgres, Mongo, and MS SQL Server..."

    # 1. PostgreSQL (ACH, Meeza Cards, IPN)
    docker exec -i ebc-postgres-src psql -U ebc_src -d ebc_sources -q -c "
      UPDATE public.ach_transactions SET created_at = NOW() WHERE txn_id IN (SELECT txn_id FROM public.ach_transactions LIMIT 1);
      UPDATE public.meeza_authorisations SET auth_timestamp = NOW() WHERE auth_id IN (SELECT auth_id FROM public.meeza_authorisations LIMIT 1);
      UPDATE public.ipn_transactions SET initiated_at = NOW() WHERE txn_id IN (SELECT txn_id FROM public.ipn_transactions LIMIT 1);
    "

    # 2. MongoDB (Meeza Digital Wallet)
    docker exec -i ebc-mongodb mongosh --quiet meeza_digital --eval "
      db.wallet_events.insertOne({
          wallet_id: 'WLT' + Math.floor(Math.random() * 100000).toString().padStart(8, '0'),
          event_type: 'WALLET_TRANSFER',
          channel: 'MOBILE_APP',
          issuing_bank_id: 'CIB',
          amount_egp: Math.round(Math.random() * 1000 * 100) / 100,
          status: 'COMPLETED',
          event_ts: new Date(),
          _source_system: 'continuous_traffic_gen'
      });
    "

    # 3. MS SQL Server (ATM Network)
    docker exec -i ebc-mssql /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P 'EbcAtm_S3cret!' -C -d ebc_atm -Q "
      INSERT INTO dbo.atm_sessions (atm_id, session_date, session_ts, card_token, issuing_bank_id, txn_type, amount_egp, status, updated_at)
      VALUES ('ATM-AUTO', CAST(GETDATE() AS DATE), GETDATE(), 'tk_auto', 'CIB', 'WITHDRAWAL', 300, 'APPROVED', GETDATE());
    "

    echo -e "  -> Batch complete. Sleeping for $INTERVAL seconds...\n"
    sleep $INTERVAL
done