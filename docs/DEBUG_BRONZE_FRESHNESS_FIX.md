# Airflow DAG Failure Debug & Fix: Bronze Freshness Gate

## Issue Summary
**DAG**: `ebc_dbt_silver` (scheduled hourly)  
**Task**: `check_bronze_freshness` (attempt 5 of 7)  
**Error**: `ValueError: Bronze freshness FAILED`

```
ValueError: Bronze freshness FAILED:
  • bronze.ach_transactions: EMPTY — Flink sink may not have run
  • bronze.meeza_authorisations: EMPTY — Flink sink may not have run
  • bronze.ipn_transactions: EMPTY — Flink sink may not have run
  • bronze.meeza_digital_wallet_events: EMPTY — Flink sink may not have run

Historical bypasses:
  • bronze.atm_sessions (last: 2026-04-18 12:00:00)
```

## Root Cause Analysis

### 1. Why Bronze tables are EMPTY
Flink CDC (Change Data Capture) architecture:
```
PostgreSQL/MongoDB/MSSQL  ──[Flink CDC]──>  Kafka  ──[Flink Iceberg Sink]──>  Bronze Iceberg tables
```

**Prerequisites for CDC to work**:
- Source database must have **changes** (INSERT/UPDATE/DELETE)
- Flink jobs must be **running** (✓ verified running)
- Kafka topics must exist (✓ verified)
- Polaris catalog must be accessible (✓ verified)

**Actual situation**: Source databases were **IDLE** — no changes to capture = no CDC events = Flink sinks write nothing = Bronze tables remain EMPTY.

### 2. Freshness Gate Logic (airflow/dags/ebc_dbt_silver.py lines 54-160)

```python
ACTIVE_SLA_HOURS    = 6      # streaming table must see event within 6h
HISTORICAL_CUTOFF_H = 24     # if idle >24h, treat as static backfill (warn + pass)

# Classification per table:
if row_count == 0:
    FAIL  # EMPTY — no data at all
elif lag_h <= 6:
    PASS  # FRESH — recent activity
elif lag_h > 24:
    WARN  # HISTORICAL — old backfill, don't fail pipeline
else (6 < lag_h <= 24):
    FAIL  # ACTIVE-AND-STALE — stream degraded
```

**Result**:
- 4 tables: EMPTY (0 rows) → **FAIL** ✗
- 1 table (atm_sessions): 1197h old → **HISTORICAL bypass** ⚠

## Solution Applied

### Step 1: Start Continuous Traffic Generator ✅
**Command executed**:
```bash
bash scripts/generate_continuous_traffic.sh
```

**What it does**:
- Connects to source databases (PostgreSQL, MongoDB, MSSQL)
- Generates synthetic CDC events: INSERT/UPDATE on existing rows
- Creates realistic data changes every 1-5 seconds
- **Expected result**: Flink CDC jobs capture events → Kafka topics receive messages → Iceberg sinks populate Bronze

### Step 2: Manually Trigger DAG ✅
**Command executed**:
```bash
docker exec ebc-airflow-webserver airflow dags trigger ebc_dbt_silver --exec-date "2026-06-07"
```

**Output**:
```
DAG run created: manual__2026-06-07T00:00:00+00:00  [QUEUED]
```

This triggers a new instance immediately (doesn't wait for 1h schedule).

## Expected Recovery Timeline

| Time | Action | Expected State |
|------|--------|-----------------|
| T+0s | Traffic generator starts | Synthetic events begin in source DBs |
| T+5s | Flink CDC captures changes | Kafka topics receive change events |
| T+10s | Flink Iceberg sink processes | Batch write to Bronze tables |
| T+15s | DAG task executes freshness check | Bronze tables have row_count > 0 |
| T+20s | Task passes (or FRESH logged) | Silver layer begins dbt transforms |

## Manual Verification Steps

### 1. Check Bronze Table Row Counts (via Trino)
```sql
SELECT 'ach_transactions' as table_name, COUNT(*) FROM iceberg.bronze.ach_transactions
UNION ALL SELECT 'meeza_authorisations', COUNT(*) FROM iceberg.bronze.meeza_authorisations
UNION ALL SELECT 'ipn_transactions', COUNT(*) FROM iceberg.bronze.ipn_transactions
UNION ALL SELECT 'meeza_digital_wallet_events', COUNT(*) FROM iceberg.bronze.meeza_digital_wallet_events;
```
**Expected**: row_count > 0 for all

### 2. Check Flink Job Status
```bash
curl -s http://localhost:8881/api/v1/jobs | jq '.jobs[] | {id, name, state}'
```
**Expected**: All CDC and sink jobs in state `RUNNING`

### 3. Check Kafka Topics
```bash
docker exec ebc-kafka kafka-topics --bootstrap-server localhost:9092 --list | grep ebc.
```
**Expected**: Topics like `ebc.public.ach_transactions` have increasing offset counts

### 4. Monitor Airflow DAG Run
Visit: **http://localhost:8085/dags/ebc_dbt_silver/grid**
- Find run: `manual__2026-06-07T00:00:00+00:00`
- Watch task state flow: `queued` → `running` → `success` ✓

## Alternative: Increase SLA Threshold (Not Recommended)

If you need the DAG to pass **immediately** while traffic generator primes the system:

```bash
# In compose/docker-compose.core.yml, edit airflow-scheduler service:
environment:
  - EBC_BRONZE_SLA_HOURS=72          # extend from 6h to 72h
  - EBC_BRONZE_HISTORICAL_HOURS=24   # keep backfill cutoff
```

Then restart:
```bash
docker compose -f compose/docker-compose.core.yml up -d --force-recreate ebc-airflow-scheduler
```

**⚠ Not recommended**: Masks real data freshness problems in production. Use only for debugging.

## Cleanup (If Needed)

Stop traffic generator:
```bash
pkill -f generate_continuous_traffic.sh
```

## Files Modified/Created
- ✓ Traffic generator: running (detached process)
- ✓ DAG trigger: issued
- 📄 This document: `DEBUG_BRONZE_FRESHNESS_FIX.md`

## Next Observation Points
1. **Airflow UI**: Check `check_bronze_freshness` task logs → should show "FRESH" or "HISTORICAL bypass"
2. **Flink UI** (http://localhost:8881): Verify job checkpoints increasing (sign of active processing)
3. **MinIO**: Verify Bronze Iceberg table size increasing (http://localhost:9001)

## References
- README.md: Troubleshooting section → "Silver DAG fails on check_bronze_freshness"
- docs/04_RUNBOOK.md: DAG execution flow
- airflow/dags/ebc_dbt_silver.py: lines 54-160 (freshness gate logic)
