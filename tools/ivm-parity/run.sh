#!/usr/bin/env bash
# End-to-end bring-up script for the ivm-parity parity harness.
#
# What this does:
#   1. Apply `schema.sql` + `seed.sql` to a Postgres at $PG_URL.
#      Tables land under the `parity` schema to avoid colliding
#      with zbugs's `public` schema on the same PG instance.
#   2. Compile `zero-schema.ts` -> `schema.json` so both zero-caches
#      load the same application schema.
#   3. Start two zero-caches side-by-side:
#        - TS IVM on port 4858 (replica: /tmp/ivm-parity-ts.db,
#          app_id: parity_ts)
#        - Rust v2 IVM on port 4868 (replica: /tmp/ivm-parity-rs.db,
#          app_id: parity_rs, ZERO_USE_RUST_IVM_V2=1)
#   4. Wait for both replicas to catch up to the PG state.
#   5. Run `tsx harness.ts` which sends every pattern in
#      `patterns.ts` to both zero-caches and diffs the row counts.
#
# Kill either zero-cache by killing its PID (script prints them).

set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

PG_URL="${PARITY_PG_URL:-postgresql://user:password@127.0.0.1:6434/postgres}"

echo "[ivm-parity] 1. applying schema + seed to $PG_URL"
psql "$PG_URL" -v ON_ERROR_STOP=1 -f schema.sql  > /dev/null
psql "$PG_URL" -v ON_ERROR_STOP=1 -f seed.sql    > /dev/null
echo "[ivm-parity]    seeded — channels=3 conversations=5 messages=10 attachments=6"

echo "[ivm-parity] 2. compiling zero-schema.ts -> schema.json"
if [ ! -f schema.json ] || [ zero-schema.ts -nt schema.json ]; then
  # zero-deploy-permissions lives under @rocicorp/zero in node_modules.
  # We're in a workspace so the mono-v2 root has it.
  (cd ../.. && npx zero-deploy-permissions -p tools/ivm-parity/zero-schema.ts \
     --output-file tools/ivm-parity/schema.json)
fi

echo "[ivm-parity] 3a. starting TS zero-cache on :4858 (app: parity_ts)"
rm -f /tmp/ivm-parity-ts.db /tmp/ivm-parity-ts.db-shm /tmp/ivm-parity-ts.db-wal /tmp/ivm-parity-ts.db-wal2
ZERO_PORT=4858 \
ZERO_APP_ID=parity_ts \
ZERO_UPSTREAM_DB="$PG_URL" \
ZERO_REPLICA_FILE=/tmp/ivm-parity-ts.db \
ZERO_SCHEMA_FILE="$(pwd)/schema.json" \
  npx zero-cache > /tmp/ivm-parity-ts.log 2>&1 &
TS_PID=$!
echo "[ivm-parity]    TS zero-cache PID=$TS_PID  log=/tmp/ivm-parity-ts.log"

echo "[ivm-parity] 3b. starting Rust v2 zero-cache on :4868 (app: parity_rs)"
rm -f /tmp/ivm-parity-rs.db /tmp/ivm-parity-rs.db-shm /tmp/ivm-parity-rs.db-wal /tmp/ivm-parity-rs.db-wal2
ZERO_PORT=4868 \
ZERO_APP_ID=parity_rs \
ZERO_USE_RUST_IVM_V2=1 \
ZERO_UPSTREAM_DB="$PG_URL" \
ZERO_REPLICA_FILE=/tmp/ivm-parity-rs.db \
ZERO_SCHEMA_FILE="$(pwd)/schema.json" \
  npx zero-cache > /tmp/ivm-parity-rs.log 2>&1 &
RS_PID=$!
echo "[ivm-parity]    RS zero-cache PID=$RS_PID  log=/tmp/ivm-parity-rs.log"

echo "[ivm-parity] 4. waiting for both caches to report ready"
for log in /tmp/ivm-parity-ts.log /tmp/ivm-parity-rs.log; do
  for i in {1..30}; do
    if grep -q "zero-cache ready" "$log" 2>/dev/null; then break; fi
    sleep 1
  done
done

echo "[ivm-parity] 5. running harness"
PARITY_TS_URL="ws://localhost:4858/sync/v49/connect" \
PARITY_RS_URL="ws://localhost:4868/sync/v49/connect" \
  NODE_OPTIONS='--import tsx' node harness.ts

echo "[ivm-parity] done. tail logs with:"
echo "  tail -f /tmp/ivm-parity-ts.log"
echo "  tail -f /tmp/ivm-parity-rs.log"
echo "[ivm-parity] stop both zero-caches:"
echo "  kill $TS_PID $RS_PID"
