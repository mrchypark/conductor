#!/usr/bin/env bash
set -euo pipefail

echo "[1/8] cargo fmt --all --check"
cargo fmt --all --check

echo "[2/8] cargo clippy --workspace --all-targets -- -D warnings"
cargo clippy --workspace --all-targets -- -D warnings

echo "[3/8] cargo test --workspace"
cargo test --workspace

echo "[4/8] order_sync example"
cargo run -p workflow-engine --example order_sync

echo "[5/8] branch routing e2e"
cargo test -p workflow-engine branch_ -- --nocapture

echo "[6/8] sleep timer e2e"
cargo test -p workflow-engine sleep_timer_fires_and_schedules_downstream_task -- --exact

echo "[7/8] order_sync sleep-driven e2e"
cargo test -p workflow-engine http_order_sync_flow_runs_end_to_end_with_sleep_between_tasks -- --exact

echo "[8/8] batch ingest e2e"
cargo test -p workflow-engine batch_event_ingest_returns_per_item_dedupe_results -- --exact

echo "MVP verification complete."
