#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
RUST_MANIFEST="$ROOT_DIR/services/expense-rs/Cargo.toml"
TEST_TMP_DIR="$ROOT_DIR/.tmp/step1-stress"
DB_PATH="$TEST_TMP_DIR/expense-stress.db"
API_PORT="19081"
WORKER_PORT="19082"
HEALTH_PROBES="${HEALTH_PROBES:-25}"
RESTART_CYCLES="${RESTART_CYCLES:-4}"
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
  local retries="${4:-60}"
  for _ in $(seq 1 "$retries"); do
    if ! kill -0 "$pid" 2>/dev/null; then
      echo "Process $pid exited before $url became ready. Log tail:" >&2
      tail -n 120 "$logfile" >&2 || true
      return 1
    fi
    if curl -sSf "$url" >/dev/null; then
      return 0
    fi
    sleep 0.25
  done
  echo "Timed out waiting for $url. Log tail:" >&2
  tail -n 120 "$logfile" >&2 || true
  return 1
}

start_api() {
  cargo run -p api --manifest-path "$RUST_MANIFEST" -- --db-path "$DB_PATH" --port "$API_PORT" --migrate true \
    >"$TEST_TMP_DIR/api.log" 2>&1 &
  API_PID="$!"
  wait_for_url "http://127.0.0.1:${API_PORT}/health" "$API_PID" "$TEST_TMP_DIR/api.log"
}

start_worker() {
  cargo run -p worker --manifest-path "$RUST_MANIFEST" -- --db-path "$DB_PATH" --port "$WORKER_PORT" --migrate true --poll-seconds 1 \
    >"$TEST_TMP_DIR/worker.log" 2>&1 &
  WORKER_PID="$!"
  wait_for_url "http://127.0.0.1:${WORKER_PORT}/health" "$WORKER_PID" "$TEST_TMP_DIR/worker.log"
}

stop_api() {
  if [[ -n "$API_PID" ]] && kill -0 "$API_PID" 2>/dev/null; then
    kill "$API_PID"
    wait "$API_PID" 2>/dev/null || true
  fi
  API_PID=""
}

stop_worker() {
  if [[ -n "$WORKER_PID" ]] && kill -0 "$WORKER_PID" 2>/dev/null; then
    kill "$WORKER_PID"
    wait "$WORKER_PID" 2>/dev/null || true
  fi
  WORKER_PID=""
}

assert_not_running() {
  local pid="$1"
  local name="$2"
  if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
    echo "$name should not be running" >&2
    return 1
  fi
}

run_probes() {
  echo "[step1-stress] running ${HEALTH_PROBES} API probes"
  for _ in $(seq 1 "$HEALTH_PROBES"); do
    curl -sSf "http://127.0.0.1:${API_PORT}/health" >/dev/null
    curl -sSf "http://127.0.0.1:${API_PORT}/api/v1/diagnostics" >/dev/null
  done

  echo "[step1-stress] running ${HEALTH_PROBES} Worker probes"
  for _ in $(seq 1 "$HEALTH_PROBES"); do
    curl -sSf "http://127.0.0.1:${WORKER_PORT}/health" >/dev/null
  done
}

run_invalid_path_failure_checks() {
  local invalid_db="/dev/null/expense-invalid.db"

  echo "[step1-stress] validating API fails with invalid db path"
  if cargo run -p api --manifest-path "$RUST_MANIFEST" -- --db-path "$invalid_db" --port 19091 --migrate true >"$TEST_TMP_DIR/api-invalid.log" 2>&1; then
    echo "API unexpectedly started with invalid db path" >&2
    return 1
  fi

  echo "[step1-stress] validating Worker fails with invalid db path"
  if cargo run -p worker --manifest-path "$RUST_MANIFEST" -- --db-path "$invalid_db" --port 19092 --migrate true >"$TEST_TMP_DIR/worker-invalid.log" 2>&1; then
    echo "Worker unexpectedly started with invalid db path" >&2
    return 1
  fi
}

echo "[step1-stress] starting churn test with ${RESTART_CYCLES} restart cycles"
for cycle in $(seq 1 "$RESTART_CYCLES"); do
  echo "[step1-stress] cycle ${cycle}: start services"
  start_api
  start_worker
  run_probes

  echo "[step1-stress] cycle ${cycle}: stop services"
  stop_worker
  stop_api
  assert_not_running "$WORKER_PID" "worker"
  assert_not_running "$API_PID" "api"
done

run_invalid_path_failure_checks

echo "[step1-stress] PASS"
