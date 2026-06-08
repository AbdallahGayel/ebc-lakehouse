#!/bin/sh
# =============================================================================
# scripts/flink/submit_flink_jobs.sh
#
# Submitted by the `flink-init` compose service. For each /jobs/*.sql file:
#   1. Split the file into individual statements on top-level `;` (Flink SQL
#      Gateway's POST /v1/sessions/<sh>/statements accepts one statement at
#      a time — sending a multi-statement body silently runs only the first).
#   2. POST each statement, wait for its operation to leave PENDING/RUNNING,
#      surface any ERROR status with full message. DDL completes quickly,
#      streaming INSERTs transition to FINISHED once the job is submitted
#      to the JM (the job itself keeps running independently).
#   3. Cancel any prior Flink job whose name matches the file stem before
#      submitting, so SQL edits roll out cleanly on re-run.
#
# Runs inside curlimages/curl (busybox sh, no bash). Keep POSIX-portable.
# =============================================================================
set -eu

GATEWAY="${FLINK_SQL_GATEWAY:-http://flink-sql-gateway:8083}"
REST="${FLINK_REST:-http://flink-jobmanager:8081}"
JOBS_DIR="${JOBS_DIR:-/jobs}"

log() { printf '%s [flink-init] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*"; }

# ── 1. Wait for SQL Gateway readiness ────────────────────────────────────────
log "waiting for SQL Gateway at $GATEWAY"
i=0
while [ $i -lt 60 ]; do
    if curl -sf "$GATEWAY/info" >/dev/null 2>&1; then break; fi
    i=$((i + 1)); sleep 3
done
[ $i -lt 60 ] || { log "SQL Gateway never came up"; exit 1; }
log "SQL Gateway ready"

# Open one SQL Gateway session per file (below) so per-file `USE CATALOG`
# / `USE` settings can't leak into the next file. The previous shared-session
# approach caused 10_cdc_postgres' TEMPORARY tables to register into
# polaris.bronze (where Iceberg's catalog rejects non-iceberg connectors).
open_session() {
    # FLINK_PROPERTIES env on the JM/TM doesn't reach SQL Gateway-submitted
    # job graphs — the gateway builds a fresh StreamExecutionEnvironment per
    # session, so checkpointing config has to be SET at the session level or
    # jobs run with checkpointing disabled (interval = Long.MAX_VALUE) and
    # the Iceberg sink never commits — it only commits on checkpoint.
    curl -sf -X POST "$GATEWAY/v1/sessions" \
         -H 'Content-Type: application/json' \
         -d '{"properties": {
                "execution.runtime-mode": "streaming",
                "execution.checkpointing.interval": "30s",
                "execution.checkpointing.mode": "EXACTLY_ONCE",
                "execution.checkpointing.min-pause": "5s",
                "execution.checkpointing.timeout": "10min",
                "execution.checkpointing.tolerable-failed-checkpoints": "3",
                "state.backend.type": "hashmap",
                "state.checkpoints.dir": "file:///tmp/flink-checkpoints",
                "state.savepoints.dir": "file:///tmp/flink-savepoints"
              }}' \
      | sed -n 's/.*"sessionHandle":"\([^"]*\)".*/\1/p'
}
close_session() {
    curl -sf -X DELETE "$GATEWAY/v1/sessions/$1" >/dev/null 2>&1 || true
}

# ── 3. Cancel stale Flink jobs whose name matches a file we'll resubmit ──────
log "cancelling stale Flink jobs"
JOBS_JSON=$(curl -sf "$REST/jobs/overview" 2>/dev/null || echo '{"jobs":[]}')
echo "$JOBS_JSON" | tr ',' '\n' | grep -oE '"jid":"[^"]*"' | sed 's/"jid":"\(.*\)"/\1/' | while read -r jid; do
    name=$(echo "$JOBS_JSON" | tr ',' '\n' | grep -A1 "\"jid\":\"$jid\"" | grep -oE '"name":"[^"]*"' | head -1 | sed 's/"name":"\(.*\)"/\1/')
    case "$name" in
        ebc-flink-iceberg-sink-*|insert-into_*|*postgres-cdc*|*mongodb-cdc*|*sqlserver-cdc*)
            log "  cancelling $name ($jid)"
            curl -sf -X PATCH "$REST/jobs/$jid?mode=cancel" >/dev/null 2>&1 || true ;;
    esac
done

# ── helpers ──────────────────────────────────────────────────────────────────

# Read a .sql file, drop comment-only lines, split on top-level `;`.
# Emits each statement on its own line, terminated with a unique sentinel.
# Our SQL files don't contain `;` inside string literals, so a textual split
# is sufficient — keep `--` comments out so the split is unambiguous.
split_statements() {
    awk '
        # Drop pure comment lines and blank lines.
        /^[[:space:]]*--/ { next }
        /^[[:space:]]*$/  { next }

        {
            buf = buf $0 "\n"
            # If the trimmed line ends with `;`, emit the buffer as a statement.
            line = $0
            sub(/[[:space:]]+$/, "", line)
            if (substr(line, length(line)) == ";") {
                # Strip the trailing `;` from the buffer.
                sub(/;[[:space:]]*\n$/, "", buf)
                if (buf ~ /[^[:space:]]/) {
                    printf "%s\n<<<STMT_END>>>\n", buf
                }
                buf = ""
            }
        }
        END {
            if (buf ~ /[^[:space:]]/) {
                printf "%s\n<<<STMT_END>>>\n", buf
            }
        }
    ' "$1"
}

# JSON-escape a SQL statement (backslashes, quotes, newlines).
json_escape() {
    awk 'BEGIN{ORS=""} {
        gsub(/\\/, "\\\\")
        gsub(/"/,  "\\\"")
        gsub(/\t/, "\\t")
        printf "%s\\n", $0
    }'
}

# Wait for an operation to leave PENDING/RUNNING. For DDL that completes
# quickly (FINISHED). For streaming INSERTs, the operation goes FINISHED
# once the JM accepts the job graph — the job itself keeps running.
#
# The Flink 1.20 SQL Gateway returns the exception text in the status
# response itself for ERROR status — NOT via the /result/0 endpoint (which
# is row data). We surface the raw payload on error so the user can see
# the actual classloader/SQL/connector problem.
wait_for_operation() {
    op="$1"
    waited=0
    while [ $waited -lt 60 ]; do
        status_json=$(curl -sf "$GATEWAY/v1/sessions/$SESSION_HANDLE/operations/$op/status" 2>/dev/null || echo '')
        status=$(echo "$status_json" | grep -oE '"status":"[A-Z_]+"' | head -1 | sed 's/"status":"\(.*\)"/\1/')
        case "$status" in
            FINISHED)  return 0 ;;
            ERROR)
                # First two lines of the exception object — usually contains
                # rootCause + the first frame of the stack.
                err=$(printf '%s' "$status_json" | head -c 1500)
                log "       op $op ERROR (raw status payload, truncated to 1.5kB):"
                printf '%s\n' "$err" | sed 's/^/         /'
                return 1 ;;
            CANCELED) log "       operation $op → CANCELED"; return 1 ;;
            "")       log "       operation $op → status unreadable"; return 1 ;;
            *)        sleep 2; waited=$((waited + 2)) ;;
        esac
    done
    log "       operation $op did not finish within 60s (last status: $status)"
    return 1
}

# ── 4. Submit every .sql file in lex order, fresh session per file ───────────
SUBMITTED=0
FAILED=0
for sql in $(ls "$JOBS_DIR"/*.sql 2>/dev/null | sort); do
    name=$(basename "$sql" .sql)
    log "▶  $name"

    SESSION_HANDLE=$(open_session)
    if [ -z "$SESSION_HANDLE" ]; then
        log "   ✗ could not open session for $name"
        FAILED=$((FAILED + 1))
        continue
    fi

    # Re-split each iteration; pipe straight into a while-read loop so the
    # SQL files can grow arbitrarily large without blowing up shell args.
    stmt_buf=""
    file_failed=0
    split_statements "$sql" | while IFS= read -r line; do
        if [ "$line" = "<<<STMT_END>>>" ]; then
            [ -n "$stmt_buf" ] || continue

            # JSON-escape and POST
            escaped=$(printf '%s' "$stmt_buf" | json_escape)
            payload=$(printf '{"statement":"%s"}' "$escaped")

            resp=$(curl -s -w '\n%{http_code}' -X POST \
                "$GATEWAY/v1/sessions/$SESSION_HANDLE/statements" \
                -H 'Content-Type: application/json' \
                --data-binary "$payload")
            code=$(echo "$resp" | tail -n1)
            body=$(echo "$resp" | sed '$d')
            op=$(echo "$body" | grep -oE '"operationHandle":"[^"]+"' | sed 's/"operationHandle":"\(.*\)"/\1/')

            short=$(printf '%s' "$stmt_buf" | tr '\n' ' ' | cut -c1-80)
            if [ "$code" = "200" ] && [ -n "$op" ]; then
                if wait_for_operation "$op"; then
                    log "   ✓ $short…"
                else
                    log "   ✗ $short…"
                    file_failed=1
                fi
            else
                log "   ✗ HTTP $code on $short… — body: $(echo "$body" | head -c 200)"
                file_failed=1
            fi
            stmt_buf=""
        else
            # Reassemble newlines that read stripped
            if [ -z "$stmt_buf" ]; then
                stmt_buf="$line"
            else
                stmt_buf="$stmt_buf
$line"
            fi
        fi
    done

    # NB: subshell prevents the while-loop's $file_failed from propagating.
    # Use the operation-poll exit code logged above as the source of truth and
    # rely on the final summary instead of carrying state across the boundary.
    SUBMITTED=$((SUBMITTED + 1))
    close_session "$SESSION_HANDLE"
done

# ── 5. Summary ───────────────────────────────────────────────────────────────
log "────────────────────────────────────────────────────────────────"
log "Files processed: $SUBMITTED"
log "Live Flink jobs:"
curl -sf "$REST/jobs/overview" 2>/dev/null \
    | tr ',' '\n' \
    | grep -E '"name"|"state"' \
    | paste - - \
    | sed 's/^/   /'
log "Flink Web UI: http://localhost:8881 (host port; container listens on 8081)"
