#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
RUST_MANIFEST="$ROOT_DIR/services/expense-rs/Cargo.toml"
TEST_TMP_DIR="$ROOT_DIR/.tmp/step1-smoke"
DB_PATH="$TEST_TMP_DIR/expense-smoke.db"
API_PORT="18081"
WORKER_PORT="18082"
API_PID=""
WORKER_PID=""

mkdir -p "$TEST_TMP_DIR"
rm -f "$DB_PATH"

cleanup() {
  if [[ -n "$API_PID" ]] && kill -0 "$API_PID" 2>/dev/null; then
    kill "$API_PID" || true
    wait "$API_PID" 2>/dev/null || true
  fi
  if [[ -n "$WORKER_PID" ]] && kill -0 "$WORKER_PID" 2>/dev/null; then
    kill "$WORKER_PID" || true
    wait "$WORKER_PID" 2>/dev/null || true
  fi
}
trap cleanup EXIT

wait_for_url() {
  local url="$1"
  local pid="$2"
  local logfile="$3"
  local retries="${4:-40}"
  for _ in $(seq 1 "$retries"); do
    if ! kill -0 "$pid" 2>/dev/null; then
      echo "Process $pid exited before $url became ready. Log tail:" >&2
      tail -n 80 "$logfile" >&2 || true
      return 1
    fi
    if curl -sSf "$url" >/dev/null; then
      return 0
    fi
    sleep 0.5
  done
  echo "Timed out waiting for $url. Log tail:" >&2
  tail -n 80 "$logfile" >&2 || true
  return 1
}

echo "[step1-smoke] starting API"
cargo run -p api --manifest-path "$RUST_MANIFEST" -- --db-path "$DB_PATH" --port "$API_PORT" --migrate \
  >"$TEST_TMP_DIR/api.log" 2>&1 &
API_PID="$!"

wait_for_url "http://127.0.0.1:${API_PORT}/health" "$API_PID" "$TEST_TMP_DIR/api.log" 40
API_HEALTH="$(curl -sSf "http://127.0.0.1:${API_PORT}/health")"
[[ "$API_HEALTH" == *'"service":"expense-api"'* ]]
[[ "$API_HEALTH" == *'"status":"ok"'* ]]

DIAG="$(curl -sSf "http://127.0.0.1:${API_PORT}/api/v1/diagnostics")"
[[ "$DIAG" == *'"service":"expense-api"'* ]]
[[ "$DIAG" == *'"sqlite":"ok"'* ]]

echo "[step1-smoke] starting worker"
cargo run -p worker --manifest-path "$RUST_MANIFEST" -- --db-path "$DB_PATH" --port "$WORKER_PORT" --migrate --poll-seconds 1 \
  >"$TEST_TMP_DIR/worker.log" 2>&1 &
WORKER_PID="$!"

wait_for_url "http://127.0.0.1:${WORKER_PORT}/health" "$WORKER_PID" "$TEST_TMP_DIR/worker.log" 40
WORKER_HEALTH="$(curl -sSf "http://127.0.0.1:${WORKER_PORT}/health")"
[[ "$WORKER_HEALTH" == *'"service":"expense-worker"'* ]]
[[ "$WORKER_HEALTH" == *'"status":"ok"'* ]]

if command -v sqlite3 >/dev/null 2>&1; then
  echo "[step1-smoke] validating migrated tables"
  TABLE_COUNT="$(sqlite3 "$DB_PATH" "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name IN ('connections','transactions','job_runs','audit_events');")"
  [[ "$TABLE_COUNT" == "4" ]]
else
  echo "[step1-smoke] sqlite3 not found; skipping explicit table-count assertion"
fi

if [[ ! -f "$DB_PATH" ]]; then
  echo "Expected DB file not found at $DB_PATH" >&2
  exit 1
fi

echo "[step1-smoke] PASS"
