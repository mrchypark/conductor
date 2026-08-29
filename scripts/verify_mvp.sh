#!/usr/bin/env bash
set -euo pipefail

echo "[1/11] go test ./..."
go test ./...

echo "[2/11] go test -race ./..."
go test -race ./...

echo "[3/11] go vet ./..."
go vet ./...

echo "[4/11] cargo fmt --all --check (legacy reference workspace)"
cargo fmt --all --check

echo "[5/11] cargo clippy --workspace --all-targets -- -D warnings (legacy reference workspace)"
cargo clippy --workspace --all-targets -- -D warnings

echo "[6/11] cargo test --workspace (legacy reference workspace)"
cargo test --workspace

echo "[7/11] order_sync example (legacy reference workspace)"
cargo run -p workflow-engine --example order_sync

echo "[8/11] branch routing e2e (legacy reference workspace)"
cargo test -p workflow-engine branch_ -- --nocapture

echo "[9/11] sleep timer e2e (legacy reference workspace)"
cargo test -p workflow-engine sleep_timer_fires_and_schedules_downstream_task -- --exact

echo "[10/11] order_sync sleep-driven e2e (legacy reference workspace)"
cargo test -p workflow-engine http_order_sync_flow_runs_end_to_end_with_sleep_between_tasks -- --exact
echo "[11/11] batch ingest e2e (legacy reference workspace)"
cargo test -p workflow-engine batch_event_ingest_returns_per_item_dedupe_results -- --exact

echo "MVP verification complete."
