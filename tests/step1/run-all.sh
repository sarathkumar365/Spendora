#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

echo "[step1-tests] running Rust unit tests"
cargo test --workspace --manifest-path services/expense-rs/Cargo.toml

echo "[step1-tests] running UI build test"
npm run build --workspace expense-desktop-ui

echo "[step1-tests] running API/worker smoke test"
bash tests/step1/smoke.sh

echo "[step1-tests] running stress-lite test"
bash tests/step1/stress-lite.sh

echo "[step1-tests] PASS"
