# Rust Durable Workflow Engine MVP Implementation Plan

> Historical, pre-release plan: sections that propose `wait_for_event`, `emit_event`, or waiter-driven recovery are retained only as design provenance. Those APIs were removed before release and have no authoring, execution, recovery, or compatibility surface. Current `0.2.0` policy is defined in [`rust-durable-workflow-engine-design.md`](../spec/rust-durable-workflow-engine-design.md).

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Historical goal:** Build the originally proposed MVP with a Rust-first SDK, DSL-backed runtime, `hiqlite` persistence, in-process pull-worker execution, durable waits, and inspection APIs. Current scope excludes the proposed asynchronous wait/emit pieces.

**Architecture:** Start from stable shared contracts and move inward: define the DSL and worker protocol first, then build the engine state model on top of `hiqlite`, then add the SDK compiler and in-process worker adapter, and only after that expose HTTP APIs and recovery flows. Each slice must leave the repository in a runnable, testable state with one narrow end-to-end behavior proven before the next slice expands scope.

**Tech Stack:** Rust workspace, `hiqlite`, `axum`, `serde`, `thiserror`, `uuid`, `time`, `tokio`, `tracing`, `cel` via an internal adapter trait.

---

## Chunk 1: Workspace And Shared Contracts

### Planned File Structure

- `Cargo.toml`
  - workspace root with common dependencies and crate members
- `rust-toolchain.toml`
  - pinned toolchain for reproducible builds
- `crates/workflow-definition/src/lib.rs`
  - public DSL types and validation entrypoints
- `crates/workflow-definition/src/dsl.rs`
  - `FlowDefinition` schema and step kinds
- `crates/workflow-definition/src/validate.rs`
  - shape, graph, and metadata validation
- `crates/workflow-protocol/src/lib.rs`
  - public worker protocol exports
- `crates/workflow-protocol/src/messages.rs`
  - `PollTask`, `HeartbeatLease`, `CompleteTask`, `FailTask`, `EmitEvent`, `GetRun`
- `crates/workflow-protocol/src/types.rs`
  - attempt ids, fencing tokens, task payloads, error payloads
- `crates/workflow-sdk/src/lib.rs`
  - user-facing workflow builder entrypoint
- `crates/workflow-sdk/src/builder.rs`
  - Rust-first authoring API
- `crates/workflow-sdk/src/compile.rs`
  - builder to DSL compilation
- `crates/workflow-worker/src/lib.rs`
  - worker runtime exports
- `crates/workflow-worker/src/registry.rs`
  - handler registration and metadata manifest
- `crates/workflow-worker/src/in_process.rs`
  - in-process adapter over protocol types
- `crates/workflow-engine/src/lib.rs`
  - engine application exports
- `crates/workflow-engine/src/app.rs`
  - high-level engine composition
- `crates/workflow-engine/src/definition_store.rs`
  - draft/publish persistence API
- `crates/workflow-engine/src/expression/adapter.rs`
  - CEL adapter trait
- `crates/workflow-engine/src/expression/noop.rs`
  - stub implementation for early slices
- `crates/workflow-engine/src/persistence/mod.rs`
  - store trait exports
- `crates/workflow-engine/src/persistence/schema.rs`
  - `hiqlite` schema creation
- `crates/workflow-engine/src/persistence/hiqlite.rs`
  - concrete durable store
- `crates/workflow-engine/src/runtime/mod.rs`
  - orchestration runtime exports
- `crates/workflow-engine/src/runtime/events.rs`
  - ingest and run creation
- `crates/workflow-engine/src/runtime/tasks.rs`
  - lease, completion, failure, retry scheduling
- `crates/workflow-engine/src/runtime/waits.rs`
  - timers and `wait_for_event`
- `crates/workflow-engine/src/runtime/recovery.rs`
  - recovery scanner
- `crates/workflow-engine/src/api/mod.rs`
  - HTTP router exports
- `crates/workflow-engine/src/api/workflows.rs`
  - draft, validate, publish, version inspection
- `crates/workflow-engine/src/api/events.rs`
  - event ingest endpoint
- `crates/workflow-engine/src/api/runs.rs`
  - run inspection and cancellation
- `crates/workflow-engine/tests/definition_publish.rs`
  - publish/validate integration tests
- `crates/workflow-engine/tests/event_to_task.rs`
  - event ingest to task lease integration tests
- `crates/workflow-engine/tests/wait_resume.rs`
  - durable wait and resume integration tests
- `crates/workflow-engine/tests/recovery.rs`
  - crash recovery integration tests
- `examples/order_sync.rs`
  - sample Rust-authored workflow for manual testing

### Execution Strategy

- Critical path:
  - Task 1 -> Task 2 -> Task 4 -> Task 5 -> Task 6 -> Task 7
- Parallelizable after Task 2:
  - Task 3 can proceed in parallel with Task 4 once shared DSL and protocol types are stable.
- Parallelizable after Task 5:
  - Task 7 API handlers and Task 8 examples can proceed in parallel if runtime traits are already fixed.
- Deferred on purpose:
  - any remote worker transport
  - polling connectors
  - editor support
  - `.codex/context/ACTIVE_TASK.md` updates

### Task 1: Bootstrap Rust Workspace

**Goal:** Create a compileable Rust workspace with the five MVP crates and a single root command that runs formatting, lint, and tests.

**Files:**
- Create: `Cargo.toml`
- Create: `rust-toolchain.toml`
- Create: `crates/workflow-definition/src/lib.rs`
- Create: `crates/workflow-protocol/src/lib.rs`
- Create: `crates/workflow-sdk/src/lib.rs`
- Create: `crates/workflow-worker/src/lib.rs`
- Create: `crates/workflow-engine/src/lib.rs`

**Depends on:** none

**Verification:** `cargo test --workspace` completes with zero tests or crate smoke tests passing.

- [ ] **Step 1: Create the workspace manifest**

Add a root `Cargo.toml` with these members:

```toml
[workspace]
members = [
  "crates/workflow-definition",
  "crates/workflow-protocol",
  "crates/workflow-sdk",
  "crates/workflow-worker",
  "crates/workflow-engine",
]
resolver = "2"
```

- [ ] **Step 2: Add minimal crate manifests and `lib.rs` files**

Each crate should export one placeholder function or type so the workspace compiles before any runtime logic exists.

- [ ] **Step 3: Add a root smoke test command**

Run: `cargo test --workspace`

Expected: all five crates build successfully and test output ends in `test result: ok`.

- [ ] **Step 4: Add a root developer command block to `README.md`**

Document the baseline commands:

```bash
cargo fmt --all
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

- [ ] **Step 5: Commit**

```bash
git add Cargo.toml rust-toolchain.toml README.md crates
git commit -m "chore: bootstrap workflow engine workspace"
```

### Task 2: Define DSL And Protocol Contracts

**Goal:** Lock the shared public contracts before engine behavior spreads across crates.

**Files:**
- Create: `crates/workflow-definition/src/dsl.rs`
- Create: `crates/workflow-definition/src/validate.rs`
- Modify: `crates/workflow-definition/src/lib.rs`
- Create: `crates/workflow-protocol/src/messages.rs`
- Create: `crates/workflow-protocol/src/types.rs`
- Modify: `crates/workflow-protocol/src/lib.rs`

**Depends on:** Task 1

**Verification:** unit tests prove DSL serialization, graph validation, and protocol idempotency field presence.

- [ ] **Step 1: Write failing DSL round-trip and validation tests**

Create tests inside `crates/workflow-definition/src/lib.rs` like:

```rust
#[test]
fn definition_roundtrip_serializes() {}

#[test]
fn definition_rejects_unknown_edge_target() {}
```

Run: `cargo test -p workflow-definition`

Expected: FAIL because `FlowDefinition` and validation entrypoints do not exist yet.

- [ ] **Step 2: Implement the MVP `FlowDefinition` schema**

Include exact MVP step kinds from the spec:

```rust
pub enum StepKind {
    Task,
    Sleep,
    WaitForEvent,
    Branch,
    EmitEvent,
}
```

- [ ] **Step 3: Implement shape and graph validation**

Cover at least:
- duplicate `step_id`
- missing edge endpoints
- unsupported step kind metadata
- missing trigger

- [ ] **Step 4: Write failing worker protocol tests**

Create tests in `crates/workflow-protocol/src/lib.rs` like:

```rust
#[test]
fn complete_task_contains_attempt_and_fencing_fields() {}
```

Run: `cargo test -p workflow-protocol`

Expected: FAIL because protocol messages are not defined yet.

- [ ] **Step 5: Implement worker protocol DTOs**

Add request/response structs for:
- `PollTask`
- `HeartbeatLease`
- `CompleteTask`
- `FailTask`
- `EmitEvent`
- `GetRun`

Each task-mutating request must include `task_id`, `attempt_id`, `worker_id`, and `fencing_token`.

- [ ] **Step 6: Re-run shared contract tests**

Run:

```bash
cargo test -p workflow-definition
cargo test -p workflow-protocol
```

Expected: PASS for both crates.

- [ ] **Step 7: Commit**

```bash
git add crates/workflow-definition crates/workflow-protocol
git commit -m "feat: add workflow DSL and worker protocol contracts"
```

## Chunk 2: Authoring And Durable Runtime Core

### Task 3: Build The Rust SDK Compiler And Handler Manifest

**Goal:** Prove that Rust authoring produces the exact DSL shape the runtime consumes and that handler metadata can be validated without executable code.

**Files:**
- Create: `crates/workflow-sdk/src/builder.rs`
- Create: `crates/workflow-sdk/src/compile.rs`
- Modify: `crates/workflow-sdk/src/lib.rs`
- Create: `crates/workflow-worker/src/registry.rs`
- Modify: `crates/workflow-worker/src/lib.rs`
- Create: `crates/workflow-engine/tests/definition_publish.rs`

**Depends on:** Task 2

**Verification:** an integration test publishes a Rust-authored definition after builder compilation and handler manifest validation.

- [ ] **Step 1: Write the failing compile-to-DSL integration test**

Create `crates/workflow-engine/tests/definition_publish.rs` with a test like:

```rust
#[tokio::test]
async fn rust_builder_compiles_and_validates_before_publish() {}
```

Run: `cargo test -p workflow-engine rust_builder_compiles_and_validates_before_publish -- --exact`

Expected: FAIL because builder compilation and manifest validation do not exist.

- [ ] **Step 2: Implement the SDK builder surface**

Support only the MVP authoring surface:
- event trigger
- task step
- sleep step
- wait-for-event step
- branch step
- emit-event step

- [ ] **Step 3: Implement builder-to-DSL compilation**

Compiler output must populate:
- `workflow_id`
- `workflow_version`
- steps
- edges
- defaults

- [ ] **Step 4: Implement handler registration metadata**

Add a registration API like:

```rust
worker.register("orders.fetch", handlers::fetch_order);
```

Generate a manifest entry with:
- handler id
- capability
- default queue
- optional timeout hints

- [ ] **Step 5: Implement publish-time handler validation**

The engine-side validation only needs metadata lookup, not executable invocation.

- [ ] **Step 6: Re-run the compile/publish test**

Run: `cargo test -p workflow-engine rust_builder_compiles_and_validates_before_publish -- --exact`

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add crates/workflow-sdk crates/workflow-worker crates/workflow-engine/tests/definition_publish.rs
git commit -m "feat: compile rust workflows into validated DSL definitions"
```

### Task 4: Implement `hiqlite` Persistence And Definition Publish State

**Goal:** Create the durable storage layer and prove immutable definition versioning before any runtime execution logic starts mutating state.

**Files:**
- Create: `crates/workflow-engine/src/definition_store.rs`
- Create: `crates/workflow-engine/src/persistence/mod.rs`
- Create: `crates/workflow-engine/src/persistence/schema.rs`
- Create: `crates/workflow-engine/src/persistence/hiqlite.rs`
- Modify: `crates/workflow-engine/src/lib.rs`
- Modify: `crates/workflow-engine/src/app.rs`
- Modify: `crates/workflow-engine/tests/definition_publish.rs`

**Depends on:** Task 2

**Verification:** integration tests prove draft replace, draft validate, immutable publish, and run pinning metadata persistence.

- [ ] **Step 1: Write failing persistence tests for definition lifecycle**

Add tests like:

```rust
#[tokio::test]
async fn publish_creates_new_immutable_workflow_version() {}

#[tokio::test]
async fn latest_published_version_is_used_for_default_lookup() {}
```

Run: `cargo test -p workflow-engine definition_publish -- --nocapture`

Expected: FAIL because `hiqlite` schema and stores are not implemented.

- [ ] **Step 2: Create `hiqlite` schema creation code**

Create tables for:
- `workflow_definitions`
- `workflow_runs`
- `step_instances`
- `task_queue`
- `timers`
- `waiters`
- `leases`
- `event_inbox`
- `transition_log`

- [ ] **Step 3: Implement definition store operations**

Support only:
- replace draft
- validate draft
- publish draft
- fetch latest published version
- list published versions

- [ ] **Step 4: Add content-hash guarding on publish**

Publish must fail if validation was done against stale draft content.

- [ ] **Step 5: Re-run definition lifecycle tests**

Run: `cargo test -p workflow-engine definition_publish -- --nocapture`

Expected: PASS for immutable versioning flows.

- [ ] **Step 6: Commit**

```bash
git add crates/workflow-engine/src crates/workflow-engine/tests/definition_publish.rs
git commit -m "feat: add hiqlite definition persistence and publish lifecycle"
```

### Task 5: Implement Task Leasing And In-Process Worker Execution

**Goal:** Prove the pull protocol is real by running one in-process worker through the same lease, heartbeat, completion, and failure semantics required for future remote workers.

**Files:**
- Create: `crates/workflow-worker/src/in_process.rs`
- Create: `crates/workflow-engine/src/runtime/mod.rs`
- Create: `crates/workflow-engine/src/runtime/tasks.rs`
- Modify: `crates/workflow-engine/src/app.rs`
- Create: `crates/workflow-engine/tests/event_to_task.rs`

**Depends on:** Task 3, Task 4

**Verification:** an integration test ingests one event, leases one task, completes it through protocol DTOs, and observes durable step completion.

- [ ] **Step 1: Write the failing event-to-task lease test**

Create `crates/workflow-engine/tests/event_to_task.rs` with:

```rust
#[tokio::test]
async fn event_creates_runnable_task_that_worker_can_lease_and_complete() {}
```

Run: `cargo test -p workflow-engine event_creates_runnable_task_that_worker_can_lease_and_complete -- --exact`

Expected: FAIL because runtime task scheduling and in-process worker adapter are missing.

- [ ] **Step 2: Implement task queue read/lease transitions**

Add store methods for:
- enqueue task
- poll next eligible task
- create lease
- heartbeat lease
- reject stale completion

- [ ] **Step 3: Implement attempt and fencing token progression**

Guarantee a new `(attempt_id, fencing_token)` on each accepted lease and idempotent handling for duplicate protocol messages.

- [ ] **Step 4: Implement the in-process worker adapter**

The adapter must call engine APIs with protocol request structs, not private function shortcuts.

- [ ] **Step 5: Implement task completion and failure state transitions**

Cover:
- successful output persistence
- failure payload persistence
- retry timer insertion stub for later use

- [ ] **Step 6: Re-run the event-to-task test**

Run: `cargo test -p workflow-engine event_creates_runnable_task_that_worker_can_lease_and_complete -- --exact`

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add crates/workflow-worker/src/in_process.rs crates/workflow-engine/src/runtime crates/workflow-engine/tests/event_to_task.rs
git commit -m "feat: add lease-based task execution through in-process worker adapter"
```

## Chunk 3: Durable Waits, Recovery, And HTTP Surface

### Task 6: Implement Event Ingest, Durable Waits, Timers, And Recovery

**Goal:** Prove the product’s main value: long-lived runs survive process loss, wait durably, and resume from matching external events.

**Files:**
- Create: `crates/workflow-engine/src/runtime/events.rs`
- Create: `crates/workflow-engine/src/runtime/waits.rs`
- Create: `crates/workflow-engine/src/runtime/recovery.rs`
- Modify: `crates/workflow-engine/src/runtime/tasks.rs`
- Create: `crates/workflow-engine/tests/wait_resume.rs`
- Create: `crates/workflow-engine/tests/recovery.rs`

**Depends on:** Task 5

**Verification:** one test proves `wait_for_event` resume semantics; another proves overdue timers and expired leases are recovered without duplicate downstream work.

- [ ] **Step 1: Write the failing wait-resume integration test**

Create `crates/workflow-engine/tests/wait_resume.rs` with:

```rust
#[tokio::test]
async fn waiting_run_resumes_when_matching_event_arrives() {}
```

Run: `cargo test -p workflow-engine waiting_run_resumes_when_matching_event_arrives -- --exact`

Expected: FAIL because wait registration and event-based resume do not exist.

- [ ] **Step 2: Implement crash-closed event ingest**

Persist `event_inbox` records with:
- `received`
- `applied`
- `failed`

Only acknowledge success after downstream effects are durable.

- [ ] **Step 3: Implement durable `sleep` and `wait_for_event`**

Add one-way transitions for:
- timer `pending -> fired | cancelled`
- waiter `waiting -> resumed | timed_out | cancelled`

- [ ] **Step 4: Implement retry timers and wait timeouts**

Use deterministic backoff from the definition retry policy and record timeout classification explicitly.

- [ ] **Step 5: Write the failing recovery test**

Create `crates/workflow-engine/tests/recovery.rs` with:

```rust
#[tokio::test]
async fn recovery_requeues_expired_leases_and_fires_due_timers_once() {}
```

Run: `cargo test -p workflow-engine recovery_requeues_expired_leases_and_fires_due_timers_once -- --exact`

Expected: FAIL because recovery scanning does not exist.

- [ ] **Step 6: Implement recovery scanning**

Support only:
- requeue expired lease if no newer attempt exists
- fire overdue timer still in `pending`
- time out waiter still in `waiting`
- re-apply event still in `received`

- [ ] **Step 7: Re-run durable wait tests**

Run:

```bash
cargo test -p workflow-engine waiting_run_resumes_when_matching_event_arrives -- --exact
cargo test -p workflow-engine recovery_requeues_expired_leases_and_fires_due_timers_once -- --exact
```

Expected: PASS for both.

- [ ] **Step 8: Commit**

```bash
git add crates/workflow-engine/src/runtime crates/workflow-engine/tests/wait_resume.rs crates/workflow-engine/tests/recovery.rs
git commit -m "feat: add durable waits timers and recovery scanning"
```

### Task 7: Expose Definition, Event, Run Inspection, And Cancel APIs

**Goal:** Add the narrow MVP HTTP surface needed for code-first publishing, event ingress, run inspection, and cancellation.

**Files:**
- Create: `crates/workflow-engine/src/api/mod.rs`
- Create: `crates/workflow-engine/src/api/workflows.rs`
- Create: `crates/workflow-engine/src/api/events.rs`
- Create: `crates/workflow-engine/src/api/runs.rs`
- Modify: `crates/workflow-engine/src/app.rs`
- Modify: `crates/workflow-engine/src/lib.rs`

**Depends on:** Task 4, Task 5, Task 6

**Verification:** HTTP integration tests cover draft publish, event ingest, run inspection, wait inspection, and run cancellation barrier behavior.

- [ ] **Step 1: Write failing HTTP tests for the MVP surface**

Add tests inside `crates/workflow-engine/tests/definition_publish.rs` and `crates/workflow-engine/tests/wait_resume.rs` for:
- `PUT /v1/workflows/{workflow_id}/draft`
- `POST /v1/workflows/{workflow_id}/draft:validate`
- `POST /v1/workflows/{workflow_id}/draft:publish`
- `POST /v1/events`
- `GET /v1/runs/{run_id}`
- `GET /v1/runs/{run_id}/steps`
- `GET /v1/runs/{run_id}/transitions`
- `GET /v1/runs/{run_id}/waits`
- `POST /v1/runs/{run_id}:cancel`

Run: `cargo test -p workflow-engine --test definition_publish --test wait_resume`

Expected: FAIL because the HTTP router does not exist.

- [ ] **Step 2: Implement workflow definition endpoints**

Keep handlers thin. All business rules stay in store/runtime services.

- [ ] **Step 3: Implement event ingest and run inspection endpoints**

Batch event ingest should return per-item results even if the first implementation accepts batch size `1`.

- [ ] **Step 4: Implement cancellation barrier behavior**

After `cancel_requested`, no new downstream tasks, timers, or waits may be scheduled.

- [ ] **Step 5: Re-run API integration tests**

Run: `cargo test -p workflow-engine --test definition_publish --test wait_resume`

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/workflow-engine/src/api crates/workflow-engine/src/app.rs crates/workflow-engine/tests
git commit -m "feat: add MVP control-plane and run inspection APIs"
```

### Task 8: Add End-To-End Example And Final MVP Verification

**Goal:** Leave the repository with one runnable example and one command sequence that proves the MVP success criteria from the spec.

**Files:**
- Create: `examples/order_sync.rs`
- Modify: `README.md`
- Modify: `crates/workflow-engine/tests/event_to_task.rs`
- Modify: `crates/workflow-engine/tests/wait_resume.rs`
- Modify: `crates/workflow-engine/tests/recovery.rs`

**Depends on:** Task 7

**Verification:** a final integration suite proves event start, task execution, wait registration, matching event resume, retry timer behavior, and recovery from expired leases.

- [ ] **Step 1: Add a sample workflow and handler registration**

The example should include:
- one event trigger
- one task
- one `wait_for_event`
- one notification task

- [ ] **Step 2: Add a README quickstart**

Document exactly how to:
- run the engine
- publish the example definition
- post the start event
- post the resume event
- inspect the run

- [ ] **Step 3: Add one final end-to-end happy-path test**

Use a single test name:

```rust
#[tokio::test]
async fn order_sync_flow_runs_from_event_to_wait_resume_to_completion() {}
```

Run: `cargo test -p workflow-engine order_sync_flow_runs_from_event_to_wait_resume_to_completion -- --exact`

Expected: PASS.

- [ ] **Step 4: Run full MVP verification**

Run:

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

Expected:
- no formatting diffs
- no clippy warnings
- all workspace tests pass

- [ ] **Step 5: Commit**

```bash
git add README.md examples crates/workflow-engine/tests
git commit -m "feat: verify end-to-end durable workflow engine MVP"
```

## Open Implementation Choices To Confirm Before Execution

- Pick the concrete `hiqlite` schema migration mechanism:
  - simplest acceptable MVP choice is startup schema bootstrap in `crates/workflow-engine/src/persistence/schema.rs`
- Pick the first CEL adapter shape:
  - start with `ExpressionAdapter` trait plus a no-op validator stub, then wire the real `cel` crate in the first slice that truly needs expression evaluation
- Pick the first HTTP app boundary:
  - expose an `axum::Router` from `crates/workflow-engine/src/api/mod.rs` and keep runtime services framework-agnostic

## Suggested Delegation Map

- Local critical path:
  - Task 2
  - Task 4
  - Task 5
  - Task 6
- Safe to delegate once contracts are fixed:
  - Task 3 SDK builder and manifest wiring
  - Task 7 HTTP endpoint shell and request/response mapping
  - Task 8 example and README quickstart

## Exit Criteria

- Published definitions are immutable and pinned by run version.
- The in-process worker crosses the same logical protocol boundary required for remote workers later.
- Duplicate event delivery does not duplicate run effects.
- `wait_for_event` and timers survive restarts and recover correctly.
- The repository contains one end-to-end example and a passing workspace test suite.
