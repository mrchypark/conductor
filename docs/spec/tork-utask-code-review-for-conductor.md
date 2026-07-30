# Tork + µTask Code Review For conductor

This document reviews two external workflow/orchestration projects at code level and filters what conductor should copy, adapt, defer, or reject.

- `conductor` baseline: `9190ed0a309284e6812dea4ff5a0d5f3e5da1b9f`
- `tork` analyzed at: `cfac641af6769a3a76f48eb1e5ea716a5ed8f185`
- `utask` analyzed at: `0db718505f4ffd7d45318b7a079c57cd00d3e1ba`

Reviewed code anchors:

- `conductor`: `crates/workflow-definition/src/dsl.rs`, `crates/workflow-definition/src/validate.rs`, `crates/workflow-engine/src/definition_store.rs`, `crates/workflow-engine/src/runtime/events.rs`, `crates/workflow-engine/src/runtime/tasks.rs`, `crates/workflow-engine/src/runtime/recovery.rs`, `crates/workflow-engine/src/persistence/schema.rs`, `crates/workflow-worker/src/registry.rs`, `crates/workflow-worker/src/in_process.rs`
- `tork`: `engine/engine.go`, `engine/worker.go`, `internal/coordinator/coordinator.go`, `internal/coordinator/scheduler/scheduler.go`, `internal/coordinator/handlers/error.go`, `internal/coordinator/handlers/completed.go`, `internal/coordinator/handlers/heartbeat.go`, `internal/coordinator/handlers/redelivered.go`, `task.go`, `job.go`, `broker/broker.go`
- `utask`: `engine/engine.go`, `engine/collector_instance.go`, `engine/collector_retry.go`, `engine/collector_autorun.go`, `engine/collector_garbage.go`, `models/tasktemplate/template.go`, `models/tasktemplate/validate.go`, `models/task/task.go`, `models/resolution/resolution.go`, `engine/step/step.go`, `engine/step/apply.go`, `engine/values/values.go`, `pkg/plugins/taskplugin/taskplugin.go`, `pkg/plugins/plugins.go`, `pkg/plugins/builtin/callback/callback.go`, `pkg/plugins/builtin/subtask/subtask.go`, `pkg/plugins/builtin/batch/batch.go`

## Executive Summary

The strongest immediate lessons are not "copy their architecture". conductor's current direction is already better aligned with a durable event-driven engine:

- conductor already has the right core boundary: Rust authoring -> stable DSL -> durable runtime tables -> pull worker protocol.
- `tork` is useful mainly for worker-facing operations, queue/routing ergonomics, and multi-mode deployment shape.
- `utask` is useful mainly for authoring validation, rich persisted execution state, plugin metadata, retention/security hygiene, and human-in-the-loop patterns.

The strongest near-term additions for conductor are:

1. Make handler metadata a first-class published artifact, not only an in-memory registry lookup.
2. Add a real worker/node operational surface: registration, heartbeat, inventory, queue/capability inspection.
3. Add retention and housekeeping for `event_inbox`, `transition_log`, `timers`, and completed runs.
4. Deepen authoring validation beyond graph shape into reference validation, expression validation, and schema-aware linting.
5. Keep async interaction patterns such as callback wait outside conductor and under the higher-level task manager; only subflow and bounded fan-out remain possible future conductor concerns.

The strongest things to reject are:

- `tork`'s broker-first architecture with RabbitMQ as dispatch backbone
- `tork`'s container-job execution model as conductor's core abstraction
- `utask`'s monolithic same-process execution model
- `utask`'s `text/template` + Sprig + JS expression stack
- `utask`'s product-specific requester/resolver/watcher authorization model inside the engine core

## conductor Baseline

conductor today has the correct foundation for a durable workflow engine:

- `FlowDefinition` is a stable runtime DSL with small step kinds: `task`, `sleep`, and `branch`.
- runtime durability is explicit in `workflow_runs`, `step_instances`, `task_queue`, `timers`, `leases`, `event_inbox`, and `transition_log`.
- worker interaction is pull-based, lease-guarded, and fenced through `attempt_id` and `fencing_token`.
- retries are timer-driven, not in-memory sleeps.
- `sleep` is durable first-class state, not a sleeping thread.
- publish-time handler validation already exists in seed form through `WorkerRegistry`.

Current limitations that matter for this review:

- `TaskMetadata` only stores `handler`; capability, queue, timeout hints, and schemas are not in the published DSL.
- handler metadata is validated against a local registry, but that manifest is not yet a first-class persisted or transportable object.
- branch expressions are still a very small evaluator.
- operational/admin surface is intentionally narrow.
- retention, archival, and long-term inspection policy are mostly absent.

This means conductor does not need a new architecture. It needs sharper metadata, operations, and authoring surfaces around the architecture it already chose.

## What To Learn From Tork

### 1. Strong worker/coordinator operating model

Relevant code:

- `engine/engine.go`
- `engine/worker.go`
- `internal/coordinator/coordinator.go`
- `internal/coordinator/handlers/heartbeat.go`
- `broker/broker.go`

Why it matters:

- `tork` makes the runtime topology very obvious: `standalone`, `coordinator`, and `worker`.
- The coordinator is operationally rich: API, queue subscriptions, node heartbeats, redelivery handling, progress/log handling.
- Heartbeats update durable node records instead of being purely ephemeral.

What conductor should take:

- Add durable worker/node records and heartbeat APIs.
- Expose worker inventory and health separately from run/task state.
- Keep the same logical deployment modes as documentation and packaging concepts, even if MVP stays in-process first.

How to adapt it:

- Do not copy the broker subscription model.
- Implement worker registration/heartbeat on top of the existing pull protocol and `hiqlite`, not via RabbitMQ topics.

Recommendation:

- `Adopt now`, but only the operational model, not the transport architecture.

### 2. Routing defaults and scheduling ergonomics

Relevant code:

- `task.go`
- `job.go`
- `internal/coordinator/scheduler/scheduler.go`

Why it matters:

- `tork` pushes queue, retry, timeout, priority, and limits into the task/job model and applies defaults when scheduling.
- The scheduler centralizes those defaults before dispatch.

What conductor should take:

- Promote handler-derived metadata into explicit workflow metadata.
- Allow workflow-level defaults plus optional per-step overrides for:
  - queue
  - capability
  - run timeout hint
  - retry override

What conductor should not copy:

- CPU/memory/container limits as a core workflow definition concern.
- runtime-specific fields like image, mount, sidecar, shell command inside the durable DSL.

Recommendation:

- `Adopt now` for queue/capability/timeout/retry metadata.
- `Reject` runtime-specific execution fields from the workflow DSL.

### 3. Composite control-flow shapes are useful product hints

Relevant code:

- `task.go`
- `internal/coordinator/scheduler/scheduler.go`
- `internal/coordinator/handlers/completed.go`

Why it matters:

- `tork` has practical composite shapes: `parallel`, `each`, `subjob`.
- Completion handlers show one workable way to coordinate parent-child completion and concurrency windows.

What conductor should take:

- Treat these as evidence that users will need:
  - bounded fan-out/fan-in
  - reusable subflow/subworkflow
  - batch child orchestration

What conductor should not copy:

- Parent-child orchestration through ad hoc task rows and coordinator handlers.
- Composite logic encoded in scheduler branches rather than in stable DSL semantics.

Recommendation:

- `Defer to V1`.
- Model as explicit DSL step kinds or compile-time macros, not as coordinator-only special cases.

### 4. Middleware and operational extension points

Relevant code:

- `engine/coordinator.go`
- `engine/worker.go`

Why it matters:

- `tork` has clear extension hooks for web, task, job, node, and log middleware.
- That keeps observability, auth, redaction, webhooks, and policy from leaking into the engine core.

What conductor should take:

- Introduce a small policy/interceptor layer around:
  - API requests
  - event ingest
  - task lease/complete/fail
  - transition recording
  - run cancellation

Recommendation:

- `Adopt now`, but keep the number of hook points small.

### 5. What conductor should explicitly reject from Tork

Relevant code:

- `broker/broker.go`
- `engine/datastore.go`
- `engine/locker.go`
- `internal/coordinator/handlers/error.go`
- `internal/coordinator/handlers/redelivered.go`

Reject these patterns:

- Broker-first dispatch.
  - conductor should keep `hiqlite` tables as the durable source of truth for runnable work.
  - RabbitMQ-style redelivery queues would weaken the current "one authoritative queue state" design.
- Retry by cloning a new task row and republishing.
  - conductor's current timer-driven retry is cleaner and more auditable.
- Generic container-task abstraction as the root model.
  - conductor should stay workflow-state-first, not container-job-first.
- A second lock service beside the main source of truth.
  - `tork` separates datastore and locker; conductor should avoid reintroducing cross-store coordination unless throughput forces it later.

## What To Learn From µTask

### 1. Rich persisted execution state is a real product advantage

Relevant code:

- `models/resolution/resolution.go`
- `engine/engine.go`
- `engine/collector_instance.go`
- `engine/collector_retry.go`
- `engine/collector_autorun.go`

Why it matters:

- `utask` persists a separate execution object (`Resolution`) with explicit runtime state, retry timing, ownership, encrypted execution payloads, and compressed steps.
- It treats orchestration state as a first-class domain object, not an incidental byproduct.

What conductor should take:

- Keep leaning into explicit runtime records instead of collapsing everything into "task queue rows".
- Add a richer run/step/wait failure vocabulary for operator understanding, even if the state machine stays smaller than µTask's.
- Add housekeeping and retention policy as first-class runtime behavior.

What conductor should not copy:

- µTask's very broad state taxonomy wholesale.
- same-process execution ownership as the main scaling model.

Recommendation:

- `Adopt now` as a design principle.
- `Adapt selectively` for state naming and admin semantics.

### 2. Authoring validation is much deeper than graph validation

Relevant code:

- `models/tasktemplate/template.go`
- `models/tasktemplate/validate.go`
- `engine/step/step.go`
- `engine/values/values.go`

Why it matters:

- µTask validates not only the shape of a template, but also references inside templated expressions.
- It lints access to step outputs and metadata against JSON schema.
- That catches a large class of authoring mistakes before execution.

What conductor should take:

- Extend publish validation to include:
  - referenced step ids inside expressions
  - event field references
  - output-schema-aware path validation
  - wait-match expression validation
  - branch expression validation
  - handler manifest existence and schema compatibility

What conductor should not copy:

- the actual templating stack: Go `text/template`, Sprig, and embedded JS are too open-ended and hard to reason about durably.

Recommendation:

- `Adopt now`, but implement it with CEL and schema-aware validation, not with template strings.

### 3. Plugin contracts should carry metadata, not just execution functions

Relevant code:

- `pkg/plugins/taskplugin/taskplugin.go`
- `pkg/plugins/plugins.go`

Why it matters:

- µTask plugins expose config validation, context factories, resource hints, metadata schema, and tags.
- That is a much better shape than a raw handler callback once the system grows beyond local Rust functions.

What conductor should take:

- Evolve the current `WorkerRegistry` into a true manifest surface containing at least:
  - handler id
  - capability
  - default queue
  - timeout hint
  - input schema hint
  - output schema hint
  - optional retry/classification hints

- Publish or register that manifest explicitly instead of only checking in-memory registrations.

Why this fits conductor especially well:

- conductor already has the seed of this in `WorkerRegistry`.
- µTask shows why metadata-rich executors matter once you want validation, editor support, remote workers, and plugins/connectors.

Recommendation:

- `Adopt now`.

### 4. Async callback and human-in-the-loop patterns are product needs, but not conductor-core needs

Relevant code:

- `pkg/plugins/builtin/callback/callback.go`
- `pkg/plugins/builtin/subtask/subtask.go`
- `pkg/plugins/builtin/batch/batch.go`

Why it matters:

- µTask demonstrates a real production need for:
  - callback creation + callback wait
  - spawning child tasks/subtasks
  - batching child work and resuming the parent when all children finish

What conductor should take:

- Introduce explicit roadmap items for:
  - subflow step
  - bounded batch/fan-out step
  - parent completion semantics on child completion when the child work is still synchronously owned by conductor

What conductor should not copy:

- product-specific task authorization semantics from requester/resolver/watcher.
- reusing one product model for both workflow state and human workflow permissions.

Recommendation:

- `Reject` callback wait in conductor core under the current boundary.
- `Defer to V1` for subflow and bounded fan-out only if they preserve self-contained completion semantics.

### 5. Recovery and housekeeping are not optional extras

Relevant code:

- `engine/collector_instance.go`
- `engine/collector_retry.go`
- `engine/collector_autorun.go`
- `engine/collector_garbage.go`

Why it matters:

- µTask makes collectors explicit: retry collection, crashed-instance collection, autorun collection, garbage collection.
- That makes operational responsibilities visible instead of implicit.

What conductor should take:

- Make runtime maintenance jobs explicit:
  - expired lease requeue scanner
  - due timer scanner
  - pending event re-apply scanner
  - retention/compaction scanner
  - possibly orphaned/cancelled run cleanup

Why this is still useful even though conductor already has recovery:

- conductor has the mechanics in `recovery_scan_at`, but not the productized operational framing.
- µTask is a good reminder to turn these into deliberate maintenance responsibilities with policy, retention, and observability.

Recommendation:

- `Adopt now`.

### 6. Encryption, compression, and data governance

Relevant code:

- `models/task/task.go`
- `models/resolution/resolution.go`
- `sql/schema.sql`

Why it matters:

- µTask encrypts task input/result and resolution payloads.
- It compresses stored step blobs.
- It is opinionated about retention and key rotation.

What conductor should take:

- Define retention and privacy policy for:
  - event payloads
  - step inputs/outputs
  - failures/details
  - transition payloads

- Decide early whether the engine needs:
  - payload encryption at rest
  - selective redaction
  - payload truncation
  - archival/export before deletion

What conductor should not copy:

- encrypting everything by default without first clarifying product requirements and operator ergonomics.

Recommendation:

- `Plan now`, `implement when product requirements are fixed`.

### 7. What conductor should explicitly reject from µTask

Reject these patterns:

- Same-runtime execution as the main execution model.
  - conductor's pull-worker boundary is the better long-term decision.
- Templating and expression sprawl.
  - CEL should remain the one expression language.
- Engine core coupled to user identity, resolver rights, watchers, and comments.
  - those belong in application/product layers above the engine.
- Huge mutable per-resolution in-memory orchestration logic as the main abstraction.
  - conductor should preserve a smaller, more explicit durable transition model.

## Cross-Project Synthesis

### Things Both Projects Confirm

Both projects, despite very different designs, reinforce the same conclusions:

- users will ask for subflows, batching, fan-out, and callback-style async interactions
- operators need inventory and health for workers/instances, not just workflow state
- retry, timeout, and recovery need explicit policy and explicit inspection surfaces
- authoring validation has to go beyond syntax
- housekeeping, retention, and compaction eventually become core platform work

### Things Only conductor Already Gets Right

conductor should stay confident on these points:

- durable wait/timer model instead of sleeping workers
- pull-worker protocol as the stable boundary
- fenced leases and stale write rejection
- timer-driven retry instead of task cloning
- code-first authoring compiled into a runtime DSL

Neither `tork` nor `utask` is a better template for those fundamentals.

## Recommended Changes For conductor

### P0: Do Next

1. Make handler manifests first-class.
   - Extend `workflow-definition` task metadata to optionally include `capability`, `queue`, `timeout_seconds`, and schema hints.
   - Persist or publish handler manifests instead of validating only against a live local registry.
   - Keep runtime scheduling able to fall back to manifest defaults.

2. Add worker/node operational records.
   - New durable table for worker registrations/heartbeats.
   - Add APIs for listing active workers, queues, capabilities, and lease ownership.
   - Expose "who can run this task" and "who currently owns this lease" for debugging.

3. Add retention and housekeeping policy.
   - Define default retention windows for runs, transitions, events, and timers.
   - Add a maintenance loop or API-triggered compaction path.
   - Decide what inspection data must survive versus what can be pruned.

4. Deepen publish validation.
   - Validate expression references against known step ids and known payload/schema paths.
   - Validate wait-match expressions once CEL matching is enabled.
   - Validate handler schema compatibility when available.

### P1: Build Soon After

1. Enrich inspection APIs.
   - `GET /runs/{id}/transitions`
   - `GET /runs/{id}/leases`
   - `GET /workers`
   - `GET /queues`

2. Add explicit timeout taxonomy.
   - Distinguish schedule timeout, task timeout, run timeout, and cancellation in persisted failure details.

3. Add scheduled triggers.
   - Learn from `tork` scheduled jobs, but model them as trigger types or external scheduler->event ingress, not as a second execution system.

4. Add policy/interceptor hooks.
   - request auth
   - event ingest policy
   - transition observation
   - redaction/logging/metrics

### P2: V1 Surface

1. Subflow step.
2. Bounded fan-out/fan-in step.
3. Callback wait step.
4. Connector/plugin packaging with manifest metadata.
5. Optional payload redaction/encryption/archival.

## Concrete Crate-Level Implications

### `crates/workflow-definition`

Add or plan:

- richer `TaskMetadata`
- optional per-step retry override
- scheduled trigger shape
- callback/subflow/fan-out step shapes for V1
- schema references for input/output validation

### `crates/workflow-sdk`

Add or plan:

- manifest emission alongside workflow definitions
- builders for queue/capability/timeout metadata
- builders for future callback/subflow/fan-out step kinds

### `crates/workflow-worker`

Add or plan:

- explicit manifest export/registration API
- worker heartbeat and identity DTOs
- optional handler schema declarations

### `crates/workflow-engine`

Add or plan:

- worker/node persistence tables
- inspection APIs for workers/leases/queues
- retention scanners
- richer failure/timeout classification
- stronger publish validation using manifest + schema + CEL checks

## Final Recommendation

The correct move is not to turn conductor into either `tork` or `utask`.

Use `tork` as an operations and distributed-worker reference:

- queue/capability ergonomics
- worker inventory
- deployment topology
- extension hooks

Use `utask` as an authoring and state-governance reference:

- validation depth
- manifest-rich executors
- explicit recovery/collector responsibilities
- callback/subtask/batch patterns
- retention/security hygiene

Keep conductor's current core intact:

- DSL-backed runtime
- durable `hiqlite` state machine
- pull workers
- leases + timers + transition log

That core is already the best part of the current design. The next gains are around metadata, operations, and authoring correctness.
