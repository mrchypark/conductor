# Handler Manifest And Sync DSL Slice Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

> **Compatibility amendment:** The proposed `wait_for_event`/`emit_event` APIs were removed before release and have no compatibility runtime. Published versions remain immutable and inspectable, but unsupported stored JSON is diagnostic-only and cannot execute. Versions published before routing snapshots are ineligible for new runs. Do not silently reinterpret stored JSON.

**Goal:** Make handler routing metadata explicit and narrow conductor's DSL/runtime to synchronous execution primitives: `task`, `sleep`, and `branch`.

**Architecture:** The change keeps conductor's existing durable runtime shape while tightening the contract between workflow definitions and workers. `workflow-definition` will carry richer `TaskMetadata`, `workflow-worker` will remain the source of manifest defaults, and `workflow-engine` will resolve and snapshot effective routing at publication. Authoring, publication, execution, and recovery exclude `wait_for_event` and `emit_event`.

**Tech Stack:** Rust workspace, `axum`, `serde`, `hiqlite`, existing integration tests in `crates/workflow-engine/tests`

All paths and commands below are relative to the repository root.

---

### Task 1: Enrich Task Metadata And Queueing Contract

**Files:**
- Modify: `crates/workflow-definition/src/dsl.rs`
- Modify: `crates/workflow-definition/src/validate.rs`
- Modify: `crates/workflow-definition/src/lib.rs`
- Modify: `crates/workflow-sdk/src/builder.rs`
- Modify: `crates/workflow-sdk/src/compile.rs`
- Modify: `crates/workflow-worker/src/registry.rs`
- Modify: `crates/workflow-engine/src/definition_store.rs`
- Modify: `crates/workflow-engine/src/runtime/tasks.rs`
- Test: `crates/workflow-engine/tests/definition_publish.rs`
- Test: `crates/workflow-engine/tests/event_to_task.rs`

**Step 1: Write the failing tests**

Add tests that assert:

- published task metadata can carry explicit `capability`, `queue`, and `timeout_seconds`
- publish rejects task metadata whose explicit `capability` conflicts with the handler registry
- publication uses explicit per-step `queue` and `capability` overrides when present
- publication fills omitted routing values from registry defaults and persists the resolved snapshot
- runtime queueing continues to use that snapshot even if live registry defaults later change

Suggested test names:

```rust
#[tokio::test]
async fn definition_publish_preserves_explicit_task_routing_metadata() {}

#[tokio::test]
async fn definition_publish_rejects_task_capability_mismatch() {}

#[tokio::test]
async fn event_queueing_uses_explicit_task_metadata_over_registry_defaults() {}

#[tokio::test]
async fn published_routing_snapshot_ignores_later_registry_default_changes() {}
```

**Step 2: Run tests to verify they fail**

Run:

```bash
cargo test -p workflow-engine definition_publish_preserves_explicit_task_routing_metadata -- --exact
cargo test -p workflow-engine definition_publish_rejects_task_capability_mismatch -- --exact
cargo test -p workflow-engine event_queueing_uses_explicit_task_metadata_over_registry_defaults -- --exact
cargo test -p workflow-engine published_routing_snapshot_ignores_later_registry_default_changes -- --exact
```

Expected:

- tests fail because `TaskMetadata` does not yet expose those fields and the engine cannot validate or queue from them

**Step 3: Write the minimal implementation**

Implement:

- extend `TaskMetadata` with optional `capability`, `queue`, and `timeout_seconds`
- add matching builder support in `WorkflowBuilder`
- update `compile_workflow` to emit richer task metadata
- extend publish validation so handler existence and explicit capability compatibility are checked against `WorkerRegistry`
- resolve omitted capability, queue, timeout, and routing inputs from `WorkerRegistry` during publication and persist them in the immutable published version
- update runtime task queueing to use only the published routing snapshot rather than live defaults
- mark pre-snapshot published versions ineligible for new runs until explicitly republished

Keep `WorkerRegistry` as the manifest default source in this slice. Do not add persistence for standalone manifests yet.

**Step 4: Run tests to verify they pass**

Run:

```bash
cargo test -p workflow-engine definition_publish_preserves_explicit_task_routing_metadata -- --exact
cargo test -p workflow-engine definition_publish_rejects_task_capability_mismatch -- --exact
cargo test -p workflow-engine event_queueing_uses_explicit_task_metadata_over_registry_defaults -- --exact
cargo test -p workflow-engine published_routing_snapshot_ignores_later_registry_default_changes -- --exact
```

Expected:

- all four tests pass

**Step 5: Run focused regression coverage**

Run:

```bash
cargo test -p workflow-definition
cargo test -p workflow-engine definition_publish -- --nocapture
cargo test -p workflow-engine event_creates_runnable_task_that_worker_can_lease_and_complete -- --exact
```

Expected:

- definition serialization and publish tests still pass
- event-to-task flow still passes with richer metadata

**Step 6: Commit**

```bash
git add crates/workflow-definition/src/dsl.rs crates/workflow-definition/src/validate.rs crates/workflow-definition/src/lib.rs crates/workflow-sdk/src/builder.rs crates/workflow-sdk/src/compile.rs crates/workflow-worker/src/registry.rs crates/workflow-engine/src/definition_store.rs crates/workflow-engine/src/runtime/tasks.rs crates/workflow-engine/tests/definition_publish.rs crates/workflow-engine/tests/event_to_task.rs
git commit -m "feat: enrich task routing metadata"
```

### Task 2: Remove Asynchronous Step Kinds From DSL And Runtime

**Files:**
- Modify: `crates/workflow-definition/src/dsl.rs`
- Modify: `crates/workflow-definition/src/validate.rs`
- Modify: `crates/workflow-definition/src/lib.rs`
- Modify: `crates/workflow-sdk/src/builder.rs`
- Modify: `crates/workflow-sdk/src/compile.rs`
- Modify: `crates/workflow-engine/src/runtime/tasks.rs`
- Modify: `crates/workflow-engine/src/runtime/events.rs`
- Modify: `crates/workflow-engine/src/runtime/recovery.rs`
- Modify: `crates/workflow-engine/examples/order_sync.rs`
- Modify: `crates/workflow-engine/tests/definition_publish.rs`
- Modify: `crates/workflow-engine/tests/order_sync_e2e.rs`
- Modify: `crates/workflow-engine/tests/wait_resume.rs`
- Modify: `crates/workflow-engine/tests/wait_timeout.rs`
- Modify: `crates/workflow-engine/tests/branch_emit.rs`
- Modify: `docs/spec/mvp-verification-matrix.md`
- Modify: `docs/spec/rust-durable-workflow-engine-design.md`

**Step 1: Write the failing tests**

Replace async-step coverage with tests that assert:

- definitions containing `wait_for_event` or `emit_event` are rejected at compile-time or validation-time depending on the remaining surface
- `branch` still works without `emit_event` downstream
- end-to-end example uses only synchronous primitives

Suggested test names:

```rust
#[test]
fn definition_rejects_wait_for_event_step_kind() {}

#[test]
fn definition_rejects_emit_event_step_kind() {}

#[tokio::test]
async fn branch_false_path_finishes_without_async_step_creation() {}
```

**Step 2: Run tests to verify they fail**

Run:

```bash
cargo test -p workflow-definition definition_rejects_wait_for_event_step_kind -- --exact
cargo test -p workflow-definition definition_rejects_emit_event_step_kind -- --exact
cargo test -p workflow-engine branch_false_path_finishes_without_async_step_creation -- --exact
```

Expected:

- tests fail because the async step kinds still exist and current tests/examples still depend on them

**Step 3: Write the minimal implementation**

Implement for the current `0.2.0` surface:

- remove `WaitForEvent` and `EmitEvent` from current authoring/public `StepKind`
- reject unsupported stored definitions during new publication and new-run selection
- remove builder helpers for those step kinds
- keep retained unsupported JSON losslessly inspectable without decoding it into an executable graph
- remove waiter scheduling and recovery paths; no compatibility runtime exists
- rewrite example and tests to use only `task`, `sleep`, and `branch`

Do not remove `timers`; they still serve retry and `sleep`.

**Step 4: Run tests to verify they pass**

Run:

```bash
cargo test -p workflow-definition
cargo test -p workflow-engine branch_false_path_finishes_without_async_step_creation -- --exact
```

Expected:

- definition tests pass with narrowed step kinds
- branch coverage passes without async step kinds
- focused inspection tests prove unsupported JSON remains visible while publication, new-run selection, and execution reject it

**Step 5: Run focused regression coverage**

Run:

```bash
cargo test -p workflow-engine sleep_timer_fires_and_schedules_downstream_task -- --exact
cargo test -p workflow-engine definition_publish -- --nocapture
cargo test -p workflow-engine event_creates_runnable_task_that_worker_can_lease_and_complete -- --exact
```

Expected:

- `sleep` still works
- publish flow still works
- basic event-triggered task execution still works

**Step 6: Commit**

```bash
git add crates/workflow-definition/src/dsl.rs crates/workflow-definition/src/validate.rs crates/workflow-definition/src/lib.rs crates/workflow-sdk/src/builder.rs crates/workflow-sdk/src/compile.rs crates/workflow-engine/src/runtime/tasks.rs crates/workflow-engine/src/runtime/events.rs crates/workflow-engine/src/runtime/recovery.rs crates/workflow-engine/examples/order_sync.rs crates/workflow-engine/tests/definition_publish.rs crates/workflow-engine/tests/order_sync_e2e.rs crates/workflow-engine/tests/wait_resume.rs crates/workflow-engine/tests/wait_timeout.rs crates/workflow-engine/tests/branch_emit.rs docs/spec/mvp-verification-matrix.md docs/spec/rust-durable-workflow-engine-design.md
git commit -m "refactor: narrow workflow DSL to sync execution"
```

### Task 3: Final Consistency Sweep

**Files:**
- Modify: `README.md`
- Modify: `scripts/verify_mvp.sh`
- Modify: `docs/spec/tork-utask-code-review-for-conductor.md`
- Test: workspace-wide checks

**Step 1: Write the failing test or verification expectation**

Define the final verification target:

- workspace compiles and tests with the narrowed DSL
- top-level docs no longer advertise removed async primitives
- verification script no longer references removed tests

**Step 2: Run current verification to expose failures**

Run:

```bash
bash scripts/verify_mvp.sh
```

Expected:

- failures or stale references appear until docs and verification commands are updated

**Step 3: Write the minimal implementation**

Implement:

- update README wording to synchronous execution scope
- update verification script to current surviving tests
- align analysis/spec docs with the narrowed DSL and scope

**Step 4: Run verification to prove completion**

Run:

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
bash scripts/verify_mvp.sh
```

Expected:

- all commands succeed

**Step 5: Commit**

```bash
git add README.md scripts/verify_mvp.sh docs/spec/tork-utask-code-review-for-conductor.md
git commit -m "docs: align conductor with sync execution scope"
```
