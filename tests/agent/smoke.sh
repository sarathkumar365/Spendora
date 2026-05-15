#!/usr/bin/env bash
# Smoke test for the financial-awareness agent endpoints.
# Requires the local API to be running on the configured port (default 8081).
#
# Usage:
#   npm run test:agent
#   API_PORT=8082 bash tests/agent/smoke.sh
#
# What it checks:
#   1. GET  /api/v1/agent/context returns provider + accounts + data_range
#   2. POST /api/v1/agent/chat opens an SSE stream with a known set of event kinds

set -euo pipefail

API_HOST="${API_HOST:-127.0.0.1}"
API_PORT="${API_PORT:-8081}"
BASE="http://${API_HOST}:${API_PORT}"

red()   { printf '\033[31m%s\033[0m\n' "$*"; }
green() { printf '\033[32m%s\033[0m\n' "$*"; }
info()  { printf '\033[36m• %s\033[0m\n' "$*"; }

fail() { red "FAIL: $*"; exit 1; }

info "Probing ${BASE}/api/v1/health"
if ! curl -sf --max-time 2 "${BASE}/api/v1/health" >/dev/null; then
  fail "API not reachable at ${BASE}. Start it with: npm run rs:api or npm run tauri:dev"
fi

info "GET /api/v1/agent/context"
ctx_resp=$(curl -sf --max-time 5 "${BASE}/api/v1/agent/context") || \
  fail "/agent/context failed — is the LLM provider configured?"

echo "$ctx_resp" | grep -q '"provider"'            || fail "context missing 'provider'"
echo "$ctx_resp" | grep -q '"registered_tools"'    || fail "context missing 'registered_tools'"
echo "$ctx_resp" | grep -q '"accounts"'            || fail "context missing 'accounts'"
echo "$ctx_resp" | grep -q '"data_range"'          || fail "context missing 'data_range'"
green "✓ context payload looks well-formed"

info "POST /api/v1/agent/chat (streaming)"
tmpfile=$(mktemp)
trap 'rm -f "$tmpfile"' EXIT

# 60s budget — plenty for a "list my accounts" answer.
curl -sN --max-time 60 \
  -H "Content-Type: application/json" \
  -H "Accept: text/event-stream" \
  --data '{"message":"List my accounts. Be brief.","history":[]}' \
  "${BASE}/api/v1/agent/chat" > "$tmpfile"

grep -q '^event: started'           "$tmpfile" || fail "stream missing 'started' event"
grep -q '^event: tool_call_start'   "$tmpfile" || fail "stream missing 'tool_call_start' event"
grep -q '^event: tool_call_result'  "$tmpfile" || fail "stream missing 'tool_call_result' event"
grep -q '^event: assistant_message' "$tmpfile" || fail "stream missing 'assistant_message' event"
grep -q '^event: done'              "$tmpfile" || fail "stream missing 'done' event"
green "✓ SSE stream emitted started/tool_*/assistant_message/done"

# --- Audit endpoints (Phase 6) ---

info "GET /api/v1/audit/summary"
summary=$(curl -sf --max-time 5 "${BASE}/api/v1/audit/summary") || fail "/audit/summary failed"
echo "$summary" | grep -q '"total_cost_micros"'   || fail "summary missing 'total_cost_micros'"
echo "$summary" | grep -q '"total_cost_dollars"'  || fail "summary missing 'total_cost_dollars'"
echo "$summary" | grep -q '"llm_call_count"'      || fail "summary missing 'llm_call_count'"
green "✓ audit summary endpoint responding"

info "GET /api/v1/audit/conversations"
conv=$(curl -sf --max-time 5 "${BASE}/api/v1/audit/conversations?limit=5") || fail "/audit/conversations failed"
# After running the chat above, at least one conversation row should exist.
echo "$conv" | grep -q '"conversation_id"' || fail "no conversations recorded — runtime instrumentation may be broken"
green "✓ audit conversations endpoint shows recent runs"

info "GET /api/v1/audit/runs"
runs=$(curl -sf --max-time 5 "${BASE}/api/v1/audit/runs?limit=5") || fail "/audit/runs failed"
echo "$runs" | grep -q '"run_id"' || fail "no completed runs — run_ended audit event may be missing"
green "✓ audit runs endpoint shows recent run_ended rows"

green "All agent smoke checks passed."
