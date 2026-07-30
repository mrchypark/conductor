# Rust Durable Workflow Engine Design

## Goal

Design a new event-driven durable workflow engine with these constraints:

- `hiqlite` is the only durable backend in MVP.
- Workflow definitions are authored in Rust code first.
- The runtime executes a stable intermediate `FlowDefinition DSL`, not arbitrary user code directly.
- The execution model is built around a future `pull worker protocol`.
- The first local worker uses the same protocol surface through an in-process adapter.
- Durable timers are first-class, including very long `sleep` delays between synchronous execution steps.

This design explicitly does **not** attempt to clone all of Temporal or Inngest semantics in the first release.

## Spec Maintenance

This document is a living design record for the engine.

- Every design Q&A that changes scope, constraints, architecture, or terminology should be reflected here at the end of that discussion.
- `MVP` and `V1` boundaries must stay explicit as decisions evolve.
- Implementation planning should follow the latest version of this document, not stale chat context.

Companion specs for the narrowed sync-only boundary:

- [`conductor-user-scenarios.md`](conductor-user-scenarios.md)
- [`conductor-interface-contract.md`](conductor-interface-contract.md)
- [`conductor-api-shape-draft.md`](conductor-api-shape-draft.md)
- [`conductor-api-gap-analysis.md`](conductor-api-gap-analysis.md)

## Design Status

This design is now the agreed baseline for implementation planning.

- the core MVP architecture is fixed
- the V1 expansion boundary is fixed
- the `Tasks` integration boundary is fixed as event-triggered synchronous execution under a higher-level task manager

Further discussion may refine defaults and operations, but should not reopen the core architecture unless a new product constraint appears.

## Current Implementation Status (2026-03-22)

This document remains the architecture baseline. The current branch implements and verifies an MVP subset of that baseline.

Implemented and covered by integration/E2E tests:

- definition draft, validate, publish, version listing, and run inspection APIs
- in-process pull-worker execution over the protocol DTOs
- `task`, `sleep`, and `branch` step execution
- batch event ingest with dedupe responses
- retry timer, sleep timer, and lease recovery paths

Implemented with an intentionally narrower MVP surface than the long-term design target:

- `branch` evaluation currently supports `true`, `false`, and `event.data.* ==/!= <json literal>` rather than a general CEL evaluator
- the HTTP cancellation endpoint currently uses `POST /v1/runs/{run_id}/cancel`; the colon-verb form remains a documented target shape rather than the current transport contract

Scope change on 2026-04-04:

- conductor now owns only self-contained synchronous execution tasks plus durable `sleep`
- asynchronous external waits, callback resumes, and `emit_event` orchestration moved out of conductor scope and up to the higher-level task manager
- older sections below that discuss `wait_for_event`, `emit_event`, or waiter tables are historical, unshipped design provenance only; those APIs were removed before release and have no authoring, execution, recovery, or compatibility surface

### Confirmed `0.2.0` Compatibility And Publication Policy

The proposed `wait_for_event` and `emit_event` APIs were removed before release. They must not be reintroduced through stored-definition decoding or silently reinterpreted as sync-only definitions. The broader pre-1.0 wire and publication contract changes use the `0.2.0` boundary.

The confirmed lifecycle policy is:

- every retained published version remains immutable and inspectable, whether or not it is eligible for new runs
- stored JSON outside the current authoring schema remains visible as a lossless diagnostic record but is not executable
- new publications reject unsupported step kinds, and new runs never select an unsupported definition
- capability, queue, timeout, and all other registry-derived routing inputs are resolved and snapshotted into the immutable published version
- changing live registry defaults affects only a later publication; it never changes an existing published version or any run pinned to it
- versions published before routing snapshots exist remain inspectable but non-executable and ineligible for new runs until an author explicitly republishes them as a new version
- destructive definition removal is blocked while active runs reference the version

This section is policy and remediation target, not a claim that every guard is implemented. The verification matrix must carry the matching storage, API, concurrency, recovery, and migration evidence before the lifecycle can be described as complete.

Still intended by the design, but not yet proven by the current branch:

- publish-time CEL binding/type validation
- payload-aware event matching over `event`, `steps`, `workflow`, and `run` bindings
- crash-closed atomic apply semantics for every current event/timer side effect

The verification matrix for the current branch lives in [`docs/spec/mvp-verification-matrix.md`](mvp-verification-matrix.md).

## Product Direction

The engine has three planes:

- `Control plane`: stores workflow definitions, accepts events, schedules runs, manages durable state transitions, and exposes the worker protocol.
- `Worker plane`: pulls runnable tasks, executes handlers/connectors, heartbeats leases, and completes or fails tasks.
- `Authoring plane`: starts as a Rust SDK that compiles to the common DSL. A visual editor can be added later by emitting the same DSL.

The core architecture is therefore:

- `Code-first engine, editor later`
- `DSL-backed runtime`
- `hiqlite` as source of truth
- `pull worker protocol` as the stable execution boundary

## Why This Direction

This path keeps the first version small enough to build while avoiding a dead-end local-only executor.

- A code-first SDK gives immediate developer ergonomics and testability.
- A DSL-backed runtime keeps the engine independent from the SDK surface.
- A shared pull protocol prevents a second execution model from appearing later.
- `hiqlite` lets the engine model durable state transitions, timers, leases, and recovery without introducing cross-store coordination in MVP.

## Non-Goals

These are intentionally out of scope for MVP:

- Visual node editor
- Polling connectors
- Arbitrary dynamic graph mutation during execution
- Transparent multi-language worker SDKs
- Parallel fan-out/fan-in
- Child workflows or nested subflows
- Human approval UI
- Marketplace-grade connector system

## High-Level Architecture

### Definition Model

Users write workflows in Rust. The Rust SDK compiles them into a stable `FlowDefinition DSL`.

The runtime consumes only the DSL. This is the key contract that later allows:

- Rust code authoring
- visual editor authoring
- generated definitions from other tools

without changing runtime semantics.

### Runtime Model

Execution is represented as durable state transitions stored in `hiqlite`.

The engine never relies on in-memory progress to preserve correctness. Every meaningful transition is stored durably:

- event received
- run created
- task leased
- task completed
- task failed
- timer scheduled
- timer fired
- run completed or failed

### Worker Model

Workers use a pull model:

- poll task
- receive lease
- execute
- heartbeat
- complete or fail

The first worker runs in-process but uses the same protocol types and state rules as a future remote worker.

### Wait Model

Durable waiting in the current scope is limited to timers.

The engine supports:

- `sleep until` for very long timers
- retry backoff through durable timers

External event waits and callback resumes are intentionally out of scope for the current conductor boundary.

## FlowDefinition DSL

The minimum durable definition format is:

```json
{
  "version": "v1",
  "workflow_id": "order_sync",
  "workflow_version": 3,
  "trigger": {
    "type": "event",
    "event": "order.created"
  },
  "input": {
    "schema": "ref:order.created"
  },
  "defaults": {
    "retry_policy": "default",
    "timeout_ms": 30000
  },
  "steps": [
    {
      "id": "fetch_order",
      "kind": "task",
      "worker": "rust",
      "handler": "fetch_order",
      "input": {
        "order_id": "$.event.data.order_id"
      }
    },
    {
      "id": "cooldown",
      "kind": "sleep",
      "sleep": {
        "seconds": 30
      }
    },
    {
      "id": "notify",
      "kind": "task",
      "worker": "rust",
      "handler": "send_notification",
      "input": {
        "order": "$steps.fetch_order.output"
      }
    }
  ],
  "edges": [
    { "from": "fetch_order", "to": "cooldown" },
    { "from": "cooldown", "to": "notify" }
  ]
}
```

### MVP Step Kinds

- `task`
- `sleep`
- `branch`

### V1 Step Kinds

- all MVP step kinds
- `poll_until`
- `foreach`
- limited `parallel`
- `subflow`

## Expression Language

The engine adopts `CEL` as the shared expression language.

It is used for:

- step input mapping
- branch conditions
- event payload filtering
- reading prior step outputs and workflow input values

### Why CEL

`CEL` gives the design one portable expression language across:

- Rust SDK authoring
- runtime validation and execution
- future visual editor authoring and preview

This is preferable to a custom mini-language because the engine already needs a real expression model for branching, matching, and mapping.

### MVP CEL Profile

MVP uses a constrained `CEL` profile instead of exposing the entire language at once.

Allowed in MVP:

- field access over event input and prior step outputs
- scalar comparisons
- boolean logic
- null checks
- only a very small helper surface if a concrete MVP use case requires it

Not included in MVP:

- a large standard library surface
- arbitrary user-defined functions
- broad date, collection, or string helpers unless specifically justified

### V1 CEL Profile

V1 may expand the allowed profile with:

- richer collection and string helpers
- date and duration helpers
- reusable helper libraries
- editor-oriented validation, preview, and linting

### Publish-Time Validation Rule

The DSL stores CEL source strings plus binding metadata.

Definitions must be validated at publish time so that the engine rejects:

- unknown bindings
- unsupported functions for the active profile
- type-invalid expressions that can be proven upfront

### Rust Implementation Candidate

The current preferred Rust implementation candidate is `cel-rust/cel-rust` (`cel` crate), not because it is guaranteed to be the final choice, but because it currently shows better ecosystem signals for this project:

- active public repository with recent releases
- direct compile and execute flow
- examples for variables, functions, and concurrent execution
- alignment with the standard CEL positioning of compile once, evaluate many

The engine should still depend on an internal expression adapter trait rather than the crate directly in core runtime code.

This keeps open the option to switch implementations later if compatibility, performance, or publish-time checking proves insufficient.

## CEL Binding Model and Validation Detail

The expression system needs a stable binding model so that SDK authoring, publish validation, runtime evaluation, and a future editor all agree on what names exist and what they mean.

### Binding Roots

MVP exposes these top-level binding roots:

- `event`
- `steps`
- `workflow`
- `run`
- `env`

### Root Semantics

`event`
- the triggering event payload

`steps`
- prior step outputs for the current run, indexed by stable `step_id`

`workflow`
- static workflow metadata such as `workflow_id` and `workflow_version`

`run`
- runtime metadata such as `run_id`, attempt counters where relevant, and timestamps that are safe to expose

`env`
- controlled engine-provided environment values only if explicitly whitelisted for MVP

MVP should avoid exposing arbitrary process-global mutable context.

### Evaluation Context by Use Case

Not every root needs to be available everywhere.

`step input mapping`
- `event`
- `steps`
- `workflow`
- `run`

`branch condition`
- `event`
- `steps`
- `workflow`
- `run`

### Binding Stability Rules

- `step_id` is part of the public binding surface once published
- renaming a step changes the binding path and is therefore a versioned definition change
- CEL expressions must refer to stable published names only

This is why immutable published versions are important.

### Recommended Shape

Examples:

```cel
event.data.order_id
steps.fetch_order.output.id
steps.fetch_order.output.status == "paid"
event.data.order_id == steps.fetch_order.output.id
```

The runtime should prefer explicit object shapes over implicit magic names.

For example, exposing `steps.<id>.output` is clearer than flattening step outputs into the top-level scope.

### Validation Phases

Publish validation should happen in layers.

1. syntax validation
2. binding validation
3. allowed-function validation for the active CEL profile
4. type validation where static types are known
5. reference validation against known step ids and handler metadata

Definitions should be rejected at publish time if they fail any deterministic validation phase.

### Type Model

MVP should keep the type model intentionally shallow.

- event payloads are treated as JSON-like dynamic values
- step outputs are treated as JSON-like dynamic values unless a handler declares stronger metadata
- workflow and run metadata can be strongly typed by the engine

This means MVP publish validation can catch:

- unknown bindings
- unsupported helper usage
- obviously invalid field paths against engine-owned metadata

But it should not try to prove every user payload shape statically.

### Function Surface

MVP should support only a small function surface, if any.

Good candidates:

- equality and inequality
- boolean composition
- null checks
- maybe selected string helpers such as `startsWith` only when a concrete use case appears

MVP should avoid:

- broad date math
- large collection helper sets
- user-defined functions
- engine-injected side-effecting helpers

### Runtime Evaluation Rules

- expressions are compiled and validated at publish time where possible
- runtime evaluation receives a typed binding context plus dynamic payload objects
- evaluation errors at runtime should fail the current step deterministically and record a structured error

### Security and Determinism

Expressions must remain pure.

- no I/O
- no clock access unless the engine explicitly injects a deterministic timestamp field
- no random values
- no mutation

The same expression with the same binding context should always evaluate to the same result.

## Durable State Model

The runtime state is separate from the static definition.

### Core Tables

- `workflow_definitions`
- `workflow_runs`
- `step_instances`
- `task_queue`
- `timers`
- `leases`
- `event_inbox`
- `transition_log`

### Table Purposes

`workflow_definitions`
- stores published DSL versions

`workflow_runs`
- stores one workflow execution per triggering input

`step_instances`
- stores per-step execution attempts, input, output, error, and status

`task_queue`
- durable source of runnable tasks in MVP

`timers`
- durable source of wake-ups for sleep and retry timeouts

`leases`
- stores active execution claims with fencing tokens and expiry

`event_inbox`
- stores ingested external events durably and supports idempotent processing

`transition_log`
- stores the state transitions needed for debugging, replay support later, and UI inspection

## Versioning and Publish Semantics

Workflow definitions are versioned immutably once published.

### Core Rule

- Every publish creates a new immutable `workflow_version`.
- Running executions stay pinned to the exact version they started with.
- New events target the latest `published` version by default.

This avoids mid-flight semantic changes and keeps debugging explainable.

### Definition States

MVP supports:

- `draft`
- `published`

V1 may add:

- `deprecated`
- `archived`

### Publish Lifecycle

1. Author edits a draft definition.
2. The system validates:
   - DSL shape
   - CEL bindings and allowed profile
   - handler references
   - step graph integrity
3. The authoring schema admits only supported step kinds; unsupported stored definitions cannot be published.
4. If valid, publish resolves explicit routing plus registry defaults and snapshots the effective capability, queue, timeout, and routing inputs into a new immutable `workflow_version`.
5. Prior published versions remain immutable and inspectable, and active runs remain pinned to them.
6. New runs use the latest eligible published version. Administrative replay or test flows may choose another eligible version, but cannot bypass deprecation or pre-snapshot eligibility gates.
7. A version published before routing snapshots exist becomes eligible for new runs only through explicit republish as a new version.

### Run Pinning

When a run starts, it stores:

- `workflow_id`
- `workflow_version`

All later step scheduling, sleeps, and retries use that same pinned version.

The engine must never silently migrate an in-flight run to a newer definition version.

### Breaking Change Model

For MVP, the engine should treat these as effectively breaking from the point of view of running executions:

- removing a step that a pinned run may still reach
- changing step ids
- changing binding names relied on by CEL expressions
- removing or renaming handlers

At the package and public API boundary, the narrowed pre-1.0 authoring surface and changed inspection contract are released at the `0.2.0` boundary.

Because published definitions are immutable, the engine avoids this problem by pinning each run to the version it started with instead of trying to patch old runs forward.

Retention is not execution eligibility. The engine preserves old versions for inspection while refusing to start new runs from unsupported or pre-routing-snapshot versions. Unsupported definitions have no execution path. Destructive removal remains blocked while any active run references a supported published version.

### Administrative Simplicity

MVP should avoid complex migration tooling.

Allowed in MVP:

- create draft
- validate draft
- publish new version
- list published versions
- inspect which version a run is pinned to

Not required in MVP:

- live migration of running runs
- definition diff tooling
- version rollback semantics beyond publishing a new corrected version

### Recommended Storage Shape

`workflow_definitions` should include at least:

- `workflow_id`
- `workflow_version`
- `status`
- `definition_json`
- `created_at`
- `published_at`
- snapshotted effective routing metadata or an equivalent immutable publication payload
- explicit new-run eligibility/compatibility metadata

There should be at most one latest eligible `published` version per `workflow_id` for default routing. Ineligible retained versions remain addressable for inspection and pinned active-run recovery.

## API Surface for Definition Publish and Run Inspection

The MVP API should stay narrow and operationally obvious.

### Recommendation

Use a minimal REST API.

Do not start with:

- CRUD-heavy administration endpoints for every internal table
- GraphQL-style flexible querying
- editor-oriented fine-grained patch APIs

The MVP only needs to support:

- creating or replacing a draft definition
- validating a draft
- publishing a draft into an immutable version
- listing versions
- inspecting runs and their execution state

### Definition APIs

Recommended MVP endpoints:

- `PUT /v1/workflows/{workflow_id}/draft`
  - create or replace the current draft payload for a workflow
- `POST /v1/workflows/{workflow_id}/draft:validate`
  - run DSL, CEL, graph, and handler validation
- `POST /v1/workflows/{workflow_id}/draft:publish`
  - create a new immutable published version from the current valid draft with resolved routing defaults snapshotted
- `GET /v1/workflows/{workflow_id}`
  - return summary metadata including current draft and latest published version info
- `GET /v1/workflows/{workflow_id}/versions`
  - list immutable published versions, including retained versions that are not eligible for new runs
- `GET /v1/workflows/{workflow_id}/versions/{workflow_version}`
  - return the concrete published definition and metadata

### Definition API Notes

- `PUT draft` is intentionally coarse-grained because the first authoring path is code-first.
- validation results should be structured and machine-readable so the future editor can reuse them.
- publish should fail if validation has not passed against the current draft content hash.

### Validation Result Shape

Validation responses should include:

- `ok`
- `errors[]`
- `warnings[]`
- `content_hash`

Each error should identify:

- location within the definition
- error code
- message
- affected binding, step, or expression if known

### Publish Result Shape

Publish responses should include:

- `workflow_id`
- `workflow_version`
- `published_at`
- `content_hash`

### Run Inspection APIs

Recommended MVP endpoints:

- `GET /v1/runs/{run_id}`
  - return run status, workflow version, timing, and top-level summary
- `GET /v1/runs/{run_id}/steps`
  - return step instances, attempts, outputs or errors, and current status
- `GET /v1/runs/{run_id}/transitions`
  - return the transition log for debugging and auditability

### Optional MVP Endpoint

If needed for operational debugging:

- `GET /v1/workflows/{workflow_id}/runs?status=...`

This should remain optional because it tends to pull the API toward a generic admin surface too early.

### Deliberately Deferred

Not required in MVP:

- delete endpoints for published versions; any future destructive removal must first prove there are no active references and satisfy the retention policy
- partial draft patch APIs
- mass search and filtering over all runs
- mutation endpoints for manual run surgery
- pause, resume, retry, or cancel admin endpoints beyond what the runtime itself needs
- GraphQL or arbitrary query APIs

### Why This Surface Fits The Product Direction

It supports:

- code-first SDK publishing
- future editor validation reuse
- operational run inspection

without forcing the project to design a full control-plane product before the runtime exists.

## Retry Policy and Failure Classification

The MVP should use a small retry model and a small failure model.

### Recommendation

Use:

- workflow-level default retry policy
- optional per-step retry override
- a small failure classification set
- explicit system-generated timeout and cancellation outcomes

Do not start with a large taxonomy or highly dynamic retry rules.

### Retry Policy Shape

Each workflow definition may declare:

- `defaults.retry_policy`

Each step may optionally override it.

Recommended MVP fields:

- `max_attempts`
- `initial_interval`
- `backoff_coefficient`
- `max_interval`

Example:

```json
{
  "defaults": {
    "retry_policy": {
      "max_attempts": 5,
      "initial_interval": "5s",
      "backoff_coefficient": 2.0,
      "max_interval": "5m"
    }
  }
}
```

This is enough to support useful retries without turning policy evaluation into its own language.

### Retry Scheduling Rule

When a retryable failure occurs:

1. the current attempt is recorded
2. the next retry time is computed from the active retry policy
3. a durable retry timer is inserted
4. when the timer fires, a new task attempt is created

Retries are therefore timer-driven, not in-memory sleep driven.

### Failure Model

MVP should classify step outcomes into these execution-level categories:

- `application_error`
- `infrastructure_error`
- `cancelled`
- `timed_out`

And these terminal workflow-level states:

- `completed`
- `failed`
- `cancelled`

This keeps the system understandable without losing important distinctions.

### Meaning of Each Failure Class

`application_error`
- the handler ran and returned a business or code error
- may be retryable or non-retryable

`infrastructure_error`
- the engine could not trust the execution result
- examples: worker lost, lease expired mid-run, transport failure, storage or dependency outage surfaced by the runtime
- retryable by default in MVP

`cancelled`
- cancellation was requested and cooperatively honored
- not retried

`timed_out`
- the engine or task timeout was exceeded
- retryability depends on the active retry policy and timeout type

### Error Payload Shape

`FailTask` should carry a structured payload such as:

```json
{
  "classification": "application_error",
  "code": "PAYMENT_API_TIMEOUT",
  "message": "upstream request timed out",
  "retryable": true,
  "details": {
    "provider": "stripe"
  }
}
```

The runtime may also synthesize failure payloads for:

- lease expiry
- heartbeat timeout
- wait timeout
- cancellation

### Retry Decision Rule

MVP retry behavior should be simple:

- if `classification` is `cancelled`, do not retry
- if `classification` is `infrastructure_error`, retry while attempts remain
- if `classification` is `timed_out`, consult the active retry policy and timeout type
- if `classification` is `application_error`, honor the explicit `retryable` flag

If the worker does not supply `retryable` for an `application_error`, the engine should default to non-retryable.

### Timeouts

MVP should distinguish at least:

- `task_start_timeout`
- `task_run_timeout`

`task_start_timeout`
- task sat too long without successful execution progress after leasing logic or queueing conditions

`task_run_timeout`
- handler exceeded its allowed execution window

These timeout sources should be recorded explicitly because they imply different operator actions and retry behavior.

### Suggested MVP Defaults

- default retry policy at workflow level
- per-step override optional
- no jitter in MVP
- retry backoff deterministic from policy and attempt number

Jitter can be added in V1 if load smoothing becomes necessary.

### What To Defer

Do not include in MVP:

- a large multi-axis failure taxonomy
- policy expressions written in CEL
- rate-limit-specific retry classes
- circuit-breaker semantics
- manual retry administration APIs
- rollback or compensation semantics

### Why This Model Fits

It matches the rest of the design:

- durable retries via timers
- at-least-once worker delivery
- cooperative cancellation
- explainable run inspection

without turning failure handling into a second orchestration language.

## Handler Registration and SDK Packaging Boundaries

The packaging model should preserve the future boundaries already chosen in the architecture.

### Recommendation

Use a small set of split crates, not one giant SDK crate and not a fully exploded plugin ecosystem in MVP.

Recommended MVP package boundaries:

- `workflow-definition`
- `workflow-protocol`
- `workflow-sdk`
- `workflow-worker`
- `workflow-engine`

This is the smallest split that keeps the runtime, SDK, and worker protocol decoupled.

### Crate Responsibilities

`workflow-definition`
- owns the `FlowDefinition DSL`
- owns definition schema types
- owns workflow version metadata
- owns validation result types shared across SDK and engine

`workflow-protocol`
- owns pull worker protocol request and response types
- owns failure payload types
- owns run inspection DTOs
- should not depend on engine internals

`workflow-sdk`
- user-facing Rust authoring API
- compiles workflow code into `FlowDefinition`
- exposes publish helpers against the control-plane API
- should not contain worker execution internals

`workflow-worker`
- handler registration
- in-process worker adapter
- remote worker polling loop later
- execution context passed to handlers

`workflow-engine`
- event ingest
- run creation
- scheduling
- timers, leases, retries, inspection
- control-plane API implementation

### Why Not One Big Crate

A single catch-all SDK crate looks fast initially, but it blurs these critical boundaries:

- authoring versus execution
- engine state model versus worker runtime
- protocol DTOs versus internal persistence models

That makes future remote workers and editor support harder, not easier.

### Why Not Over-Split In MVP

MVP should not introduce separate connector SDKs, plugin loaders, or marketplace packaging boundaries yet.

Those would create framework surface before the base runtime is proven.

### Handler Identity

Each executable task step should reference a stable handler identifier:

- `handler`
- optionally `capability`
- optionally `queue`

Example:

```json
{
  "id": "fetch_order",
  "kind": "task",
  "handler": "orders.fetch",
  "capability": "rust",
  "queue": "default"
}
```

The handler identifier is part of the published workflow definition.

### Handler Registration Model

Workers register handlers locally in code.

Conceptually:

```rust
worker.register("orders.fetch", handlers::fetch_order);
worker.register("orders.notify", handlers::send_notification);
```

Registration binds:

- handler id
- Rust function or async function
- capability
- optional queue preference
- optional handler metadata such as timeout hints or input/output schema hints

### Important Boundary

The engine should know handler metadata, not handler implementations.

That means:

- published definitions refer to stable handler ids
- worker processes provide implementations for those ids at runtime
- the engine never stores or executes arbitrary user function pointers

This is what allows future remote workers.

### Publish-Time Validation Rule

MVP publish validation should confirm that each referenced handler id has matching declared metadata in the local authoring context or published manifest.

The engine does not need the executable implementation at publish time.

Instead, it needs enough metadata to validate:

- handler id exists
- capability requirement is known
- queue is valid if specified
- optional input/output schema hints are structurally valid

Publication must also materialize the effective values after explicit definition metadata is combined with manifest defaults. Those resolved values belong to the immutable published version; runtime execution must not look them up again from a mutable live registry.

### Handler Metadata Manifest

To keep the engine and worker decoupled, the SDK should be able to produce a handler manifest alongside workflow definitions.

The manifest may include:

- handler id
- capability
- default queue
- optional timeout hints
- optional schema metadata

This manifest is a better validation artifact than trying to make the engine inspect compiled Rust code.

### In-Process MVP Path

For MVP, the same binary may contain:

- workflow definitions
- handler manifest
- worker registration
- engine runtime

But the architectural rule remains:

- the in-process path must still cross the protocol boundary logically
- the engine must depend on protocol and metadata, not direct handler calls

### V1 Expansion

V1 may add:

- remote worker packages
- connector packages
- generated manifests
- per-handler observability metadata

But these should extend the same boundaries rather than replace them.

## Event Ingest API Shape and Idempotency Contract

Event ingest is the external entry point into the runtime.

### Recommendation

MVP should expose a simple HTTP ingest API with a stable event envelope, explicit idempotency, and first-class batch support.

Recommended MVP endpoint:

- `POST /v1/events`

The logical contract should be the same whether the request contains one event or many.

### Event Envelope

Recommended shape:

```json
{
  "id": "evt_01",
  "name": "order.created",
  "source": "app.orders",
  "time": "2026-03-22T01:02:03Z",
  "idempotency_key": "orders:123:create",
  "data": {
    "order_id": "123"
  },
  "meta": {
    "tenant": "default"
  }
}
```

### Envelope Rules

- `name` is required
- `data` is required and JSON-like
- `id` is optional but strongly recommended
- `idempotency_key` is optional but strongly recommended for producer-controlled deduplication
- `source` is optional in MVP but recommended for observability and future routing
- `meta` is engine-opaque except for explicitly reserved fields

MVP uses a single logical namespace by default.

- no strong tenant isolation in MVP
- no tenant-aware policy engine in MVP
- `meta.tenant` may still be carried as opaque metadata for future use, but it does not imply hard runtime isolation yet

### Ingest Response

Recommended response shape:

```json
{
  "accepted": true,
  "event_id": "evt_01",
  "deduplicated": false
}
```

If the event is recognized as a duplicate inside the active idempotency window:

```json
{
  "accepted": true,
  "event_id": "evt_01",
  "deduplicated": true
}
```

For batch requests, the response should return a per-item acceptance result rather than failing the whole batch on one duplicate or validation error.

### Idempotency Rule

MVP should support two idempotency paths:

- producer-supplied `idempotency_key`
- fallback to `id` when present

The engine should compute and persist a dedupe record before any workflow run creation.

Recommended dedupe precedence:

1. if `idempotency_key` exists, dedupe on `(source, name, idempotency_key)`
2. else if both `source` and `id` exist, dedupe on `(source, id)`
3. else treat the event as non-idempotent and accept it normally

### Idempotency Window

MVP should use a bounded retention window for dedupe records, not infinite history.

Default recommendation:

- dedupe retention: `90 days`

This is an operational default, not a semantic guarantee forever.

### Persistence Rule

Ingest must be durable before execution side effects occur.

The engine should:

1. validate the envelope
2. check dedupe state
3. persist the event in `event_inbox`
4. create matching eligible new runs
5. record transitions

These steps must be crash-closed.

The engine should not mark an event as fully consumed until all derived runtime effects for that ingest operation are durably recorded.

### Crash-Closed Ingest Rule

MVP should model event ingest as a small durable state machine.

Recommended `event_inbox.processing_state` values:

- `received`
- `applied`
- `failed`

Rules:

- dedupe lookup may find an existing event record
- only an event in `applied` state counts as a completed prior ingest
- if an event exists in `received` state after a crash, recovery must retry applying it
- ingest acknowledgement may return success only after the event is durably stored and either applied or explicitly recognized as an already-applied duplicate

This prevents the failure mode where dedupe state is written but run creation is lost.

### Single-Fire Invariants

Timers and event ingest must have one-way durable state transitions.

Recommended examples:

- timer: `pending -> fired | cancelled`
- event ingest: `received -> applied | failed`

These transitions must be atomic with the side effects they authorize.

Examples:

- a timer may move to `fired` only in the same transaction that creates the next runnable task

Recovery must respect these one-way state transitions and never create downstream work from a timer already in a terminal state.

### Matching Behavior

In the current conductor scope, an ingested event only triggers new workflow runs.

Event ingest is not a resume signal for in-flight workflows. If an external async system must later advance orchestration, that decision belongs to the higher-level task manager, which can emit a fresh start event or invoke conductor again.

### Deferred For V1

Do not include in MVP:

- arbitrary event search APIs
- replay-from-inbox administration APIs
- broker-specific protocols
- multi-tenant policy engines
- partition key routing semantics beyond basic metadata passthrough

## Execution Timeout Ownership Between Engine and Worker

Timeout ownership must be explicit or the engine and worker will disagree about who is allowed to end work.

### Recommendation

The engine owns authoritative timeout policy and deadline computation.

Workers are informed of deadlines and are expected to cooperate, but workers do not define or extend authoritative deadlines in MVP.

### Timeout Types

MVP should model at least:

- `task_schedule_timeout`
- `task_run_timeout`

`task_schedule_timeout`
- maximum allowed delay from task becoming runnable to actual execution progress

`task_run_timeout`
- maximum allowed execution window once a worker begins the task

### Ownership Rule

The engine determines:

- whether a timeout exists
- the computed deadline
- whether a timeout has expired
- the resulting failure classification and retry behavior

The worker receives:

- timeout metadata
- absolute deadline where relevant

The worker should stop cooperatively when it sees a passed deadline, but the engine remains authoritative.

Heartbeats preserve lease ownership only.

Heartbeats do **not** extend `task_run_timeout` in MVP.

### Lease vs Timeout

Lease expiry is not the same as task timeout.

- lease expiry means execution ownership is no longer trusted
- task timeout means the business execution window was exceeded

The runtime must record these separately.

### Why The Engine Owns Timeouts

This preserves consistency across:

- in-process workers
- future remote workers
- retries
- cancellation
- run inspection

Without engine ownership, the same workflow could behave differently depending on worker implementation quality.

### Deferred For V1

Do not include in MVP:

- worker-requested timeout extension APIs
- dynamic timeout policies written in CEL
- per-heartbeat negotiated deadlines

## Run Cancellation and Administrative Control Surface

MVP should include a small administrative control surface, not a full workflow operations console.

### Recommendation

Include minimal run cancellation in MVP and defer pause/resume and manual run surgery to V1.

Recommended MVP endpoint:

- `POST /v1/runs/{run_id}:cancel`

Optional request body:

```json
{
  "reason": "user_requested",
  "requested_by": "admin@example.com"
}
```

### Cancellation Semantics

Cancellation is cooperative.

When cancellation is requested:

1. the run is marked `cancel_requested`
2. pending timers and waits are marked so they will not schedule new work
3. running tasks observe cancellation on heartbeat or the next protocol interaction
4. workers stop safely and report `cancelled`
5. when all active work is reconciled, the run becomes terminal `cancelled`

### Scheduling Barrier Rule

`cancel_requested` is a hard scheduling barrier.

That means:

- no new downstream tasks, timers, or waits may be created once cancellation is observed in the authoritative run state
- a late `CompleteTask` may reconcile the current attempt outcome for audit purposes, but it must not schedule additional work when `cancel_requested` is already set
- timeout processing and wait resume must also honor the barrier before creating new runnable work

### Important Rule

Cancellation must not mutate the published workflow definition or rewrite history.

It only changes runtime state for the targeted run.

### Inspection Requirements

Run inspection should expose:

- whether cancellation was requested
- who requested it if available
- when it was requested
- whether active work is still draining

### MVP Administrative Surface

Include:

- cancel run
- inspect run
- inspect steps
- inspect waits
- inspect transitions

Do not include in MVP:

- pause run
- resume run
- manual task requeue
- force-complete step
- manual state patching
- replay arbitrary historical runs

### V1 Administrative Surface

V1 may add:

- pause and resume
- richer retry and replay controls
- run search and bulk operations
- operator-safe manual recovery tooling

## Pull Worker Protocol

The minimum protocol surface is:

- `PollTask`
- `HeartbeatLease`
- `CompleteTask`
- `FailTask`
- `EmitEvent`
- `GetRun`

### Protocol Rules

- Workers never write runtime tables directly.
- Task execution is `at-least-once`.
- Completion and failure are guarded by fencing tokens.
- Lost workers are handled by lease expiry plus recovery scanning.
- The in-process worker uses the same protocol requests and responses as the remote worker path.
- External side effects executed by handlers are also `at-least-once` unless the handler or target system uses an explicit idempotency key.

## Worker Contract Detail

The worker model is pull-based and lease-guarded.

### Worker Identity

Each poll request carries:

- `worker_id`: unique instance identity for auditing and lease ownership
- `worker_name`: stable logical worker name
- `queues`: named queues the worker is willing to consume
- `capabilities`: execution features or handler families the worker supports
- `max_inflight`: maximum tasks the worker is willing to hold concurrently

MVP does not require a separate persistent registration step. Worker identity is supplied on each poll request.

### Routing Model

Routing uses both queues and capabilities.

- `queue` controls coarse routing and isolation
- `capability` controls execution compatibility

Examples:

- queue: `default`, `io-bound`, `priority-payments`
- capability: `rust`, `http`, `connector:slack`

A task is eligible only when both its queue and capability requirements match the worker request.

The task's queue, capability, timeout, and related routing requirements come from the routing snapshot stored in its published workflow version. Live registry changes influence only a subsequent publication and never retroactively change queued work or a run already in progress.

### Polling

`PollTask` should support long polling.

Recommended MVP behavior:

- protocol supports batching
- initial implementation may process one task at a time per worker loop for simplicity
- worker controls demand through `max_inflight`

### Leases and Heartbeats

Recommended starting values for MVP:

- lease TTL: `30s`
- heartbeat target: every `10s`
- renew before one third of lease life remains

Workers should not wait until the edge of lease expiry to renew. Recovery must assume heartbeats can be delayed briefly, so the system should tolerate minor jitter.

Each successful lease acquisition creates a new `attempt_id` and a new monotonic `fencing_token` for that task.

These values are the authoritative identity of the active execution attempt.

### Completion and Failure

Every completion or failure request must include:

- `task_id`
- `attempt_id`
- `worker_id`
- `fencing_token`

This prevents stale workers from overwriting a task already re-leased elsewhere.

Duplicate `HeartbeatLease`, `CompleteTask`, or `FailTask` requests for the same accepted `(task_id, attempt_id, fencing_token)` must be handled idempotently.

Late requests from an older attempt must be rejected without changing runtime state.

### Cancellation Model

MVP uses cooperative cancellation only.

- the engine can mark a task or run as `cancel_requested`
- workers learn this through heartbeat or the next protocol interaction
- workers are expected to stop safely and report failure or cancellation

MVP does not attempt hard preemption.

### Timeouts and Long-Running Work

Long-running work must remain lease-based, not thread-based.

- workers heartbeat while running
- lease expiry makes the task eligible for recovery
- lease renewal does not change the authoritative `task_run_timeout`
- future versions may support explicit timeout extension APIs, but MVP does not

### Inspection

Run and task inspection should expose:

- current worker owner if leased
- lease expiry
- attempt count
- cancellation requested state
- queue and capability selection

## Execution Semantics

### Event to Run

1. Receive event.
2. Persist it in `event_inbox` with `processing_state = received`.
3. Match workflow definitions.
4. Create `workflow_runs`.
5. Create initial `step_instances`.
6. Insert initial `task_queue` entries.
7. Record transitions.
8. Mark the event as `applied` in the same durable apply transaction.

### Task Lease

1. Worker calls `PollTask`.
2. Engine finds a ready task.
3. Engine atomically marks the task leased and stores a lease.
4. Engine increments the task attempt identity and fencing token.
5. Worker receives the task plus `attempt_id` and `fencing_token`.

### Task Success

1. Worker calls `CompleteTask`.
2. Engine validates `task_id`, `attempt_id`, and `fencing_token`.
3. Engine stores output and marks the step completed.
4. Engine checks whether the run is already `cancel_requested`.
5. If not cancelled, the engine evaluates outgoing edges.
6. If not cancelled, the engine creates next tasks or timers.
7. Engine completes the run only if the terminal predicate is satisfied.

### Task Failure

1. Worker calls `FailTask`.
2. Engine validates `task_id`, `attempt_id`, and `fencing_token`.
3. Engine stores the error and attempt metadata.
4. Engine either schedules a retry timer or fails the run.

### Event-Based Start

1. External event arrives.
2. Engine persists the event with `processing_state = received`.
3. Engine matches published workflow triggers.
4. New workflow runs are created from matching definitions.
5. First runnable tasks are inserted in the same atomic transition.
6. The event is marked `applied`.

### Recovery

A recovery scanner periodically:

- requeues tasks whose leases expired
- fires overdue timers
- resumes runs left in intermediate states

Recovery must respect the same one-way state transitions as the live path.

It may only:

- requeue a leased task if the active lease expired and no newer attempt has been accepted
- fire a timer still in `pending`
- re-apply an event still in `received`

### Terminal Run Predicate

A run may enter a terminal state only when all of these are true:

- there are no ready tasks
- there are no leased tasks with a still-active attempt
- there are no pending timers
- there is no retry timer still pending
- there is no cancellation drain work remaining

The terminal state then becomes one of:

- `completed`
- `failed`
- `cancelled`

## Positioning Against Existing Systems

This design is not identical to Airflow or Camunda.

### Compared with Airflow

It is intentionally different from Airflow in core operating model.

- Airflow is scheduler-first and DAG-run oriented, with the scheduler monitoring DAGs and triggering runnable task instances through an executor.
- This engine is event-first and durable-wait oriented.
- Airflow is a strong fit for batch scheduling and data pipelines.
- This engine is optimized for long-lived event-driven orchestration, external signals, retries, and resumable waits.

In short, this is not an Airflow-like batch orchestrator with durable waits added later.

### Compared with Camunda

It is closer to Camunda 8 in execution shape.

- both create runnable work items and let workers pull, execute, and report completion or failure
- both support event-driven progression and long-lived process state
- both separate orchestration from worker-side business logic

However, this design still differs materially:

- Camunda is BPMN-first and modeler-first
- this engine is code-first now, editor later
- Camunda uses a broad process variable model
- this engine prefers explicit step outputs plus CEL bindings
- Camunda already includes mature human-task and BPMN semantics
- this engine deliberately keeps MVP smaller and narrower

### Practical Summary

- closer to Camunda than Airflow in runtime semantics
- more code-centric than Camunda in authoring
- narrower and more programmable than BPMN-heavy systems
- more explicitly durable-wait oriented than Airflow

## What Would Change If This Became BPMN-First

Moving closer to BPMN would change the authoring and execution model in meaningful ways.

### Main Shift

The current design treats workflow structure as a compact internal DSL compiled from Rust.

A BPMN-first design would instead treat the process model itself as the primary source of truth. That means the engine would need to execute concepts closer to BPMN tokens and BPMN element semantics rather than a smaller custom step graph.

### Additional Modeling Concepts

A BPMN-first direction would likely introduce first-class support for:

- start events and end events
- intermediate catch and throw events
- boundary events
- receive tasks
- service tasks
- exclusive, parallel, inclusive, and event-based gateways
- subprocesses and possibly event subprocesses

This is a much larger semantic surface than the current MVP step model.

### Variable Model Changes

The current design prefers explicit step outputs plus CEL bindings.

A BPMN-first design usually pushes toward a broader process-variable model:

- a shared process context
- scoped variables
- variable propagation rules across tasks and subprocesses
- more implicit data flow

That increases flexibility for modelers, but also makes debugging and compatibility more subtle.

### Wait and Event Semantics

The current design models waiting through `sleep` and durable timers.

A BPMN-first design would want to represent waits using BPMN concepts such as:

- message catch events
- timer catch events
- receive tasks
- boundary timer or message events
- event-based gateways

This makes the engine friendlier to model-driven authoring, but it also means implementing more edge-case execution semantics.

### Worker Mapping Changes

Today a worker executes explicit `task` steps.

In a BPMN-first design, the runtime would likely map:

- `service task` -> external worker job
- `receive task` -> event wait registration
- `timer catch event` -> durable timer
- `event-based gateway` -> multiple competing wait registrations

This is feasible, but it broadens the runtime from a compact durable workflow engine into a BPMN execution engine.

### Authoring Changes

The biggest user-visible change is that the visual model becomes primary.

- current direction: code-first now, editor later
- BPMN-first direction: modeler-first now, code becomes an implementation detail behind service tasks or connectors

That is a different product choice, not just a syntax change.

### Recommended Interpretation

If the goal is to stay programmable, small, and durable-wait oriented, the current design should remain code-first with an editor-ready custom DSL.

If the goal is business-readable process modeling, shared language with analysts, and standards-based diagrams, then BPMN-first is the right direction, but it should be treated as a deliberate product pivot with a larger scope.

## Integration Boundary with the Tasks Platform

This engine can integrate with a separate `Tasks` repository, but it should not absorb the Tasks domain model or become the direct source of truth for Tasks records.

### Recommendation

Integrate as a loosely coupled orchestration capability.

That means:

- `Tasks` remains the system of record for business records, workflow templates, permissions, and user-facing operational state
- the durable workflow engine owns synchronous execution state, durable sleep/retry timers, and execution history
- integration happens through domain events and command-style API calls, not shared direct table mutation

The preferred product boundary is:

- build the durable workflow engine as a separate product or separately deployable service
- integrate it with `Tasks` through ready-to-start event triggers and synchronous task handlers that invoke command APIs

### Why This Fits Tasks

The Tasks specs and codebase already point in this direction:

- workflow is treated as a cross-cutting capability rather than the root business object
- native entities and generic records coexist
- workflow templates and transition validation remain module-aware
- API and domain services are the mutation boundary

This means the engine naturally fits as a platform capability around the existing app, not as a replacement for its core record model.

### What Should Be Shared

Good shared concepts:

- workflow or automation bindings from Tasks objects to engine workflow definitions
- event envelopes emitted from Tasks domain actions
- command contracts the engine can call back into
- projected execution summaries visible in Tasks

Examples:

- issue created -> start an orchestration run
- issue moved to `waiting_customer` -> arm a timeout and reminder flow
- approval request created -> wait for external approval event and then call back into Tasks to transition the record

The third example is a higher-level task-manager flow: the task manager owns the approval wait and starts a new eligible conductor run when the approval is ready. Conductor has no wait/callback compatibility execution.

### What Should Not Be Shared

Do not share these as a single merged runtime:

- direct record persistence
- authorization evaluation
- native business semantics such as what issue closure or approval means
- direct DB writes from the engine into Tasks tables

Those belong to the Tasks domain services and API layer.

### Recommended Technical Shape

Preferred first integration:

1. `Tasks` emits domain events when records are created, moved, assigned, or commented
2. the upper task manager converts ready work into an eligible conductor start event
3. conductor runs self-contained synchronous tasks and durable timers; task handlers may call Tasks through its command/API boundary
4. Tasks records activities and resulting state changes in its own persistence model

This keeps both systems honest:

- the engine never bypasses Tasks validation and auth
- Tasks never has to implement conductor's durable sleep, retry, and execution semantics itself

### Suitable First Use Cases

Good candidates for integration:

- SLA timers and escalation
- follow-up reminders
- approval flows with long external waits
- connector-driven side effects such as Slack, email, or external callbacks
- multi-step operational playbooks that touch Tasks records over time

Long external waits and callbacks in this list are higher-level task-manager use cases, not conductor workflow steps. Conductor participates only in ready, self-contained execution slices.

### Poor First Use Cases

Bad candidates for first integration:

- replacing the current issue transition validation path wholesale
- moving all workflow template semantics into the engine
- making generic `records` the hidden universal runtime subject for everything

Those would erase the current product boundary inside Tasks.

### Practical Product Framing

`Tasks workflow`
- object lifecycle and operator-facing status flow

`Durable engine workflow`
- asynchronous orchestration around those objects

These two workflow concepts should be related, but not conflated.

## MVP Scope

MVP exists to prove the durable execution model, not to maximize feature surface.

### MVP Includes

- Rust SDK for workflow definition
- compilation to `FlowDefinition DSL`
- definition publishing and versioning
- event-triggered workflows
- `task`, `sleep`, and `branch`
- `hiqlite` as the only backend
- durable task queue in `hiqlite`
- pull worker protocol
- in-process worker adapter using the same protocol surface
- lease heartbeat and recovery
- idempotent event ingest
- retry with backoff timers
- very long timer support
- run and step inspection API

### MVP Excludes

- visual editor
- polling connectors
- remote worker transports
- generalized connector SDK
- parallel DAG execution
- nested workflows
- built-in human task inbox

### MVP Success Criteria

- a workflow can wait for hours or days without process residency
- a crashed worker does not lose workflow progress
- duplicate event delivery is absorbed safely
- in-process execution and future remote execution share the same protocol semantics
- current definitions execute only from immutable, routing-snapshotted published versions eligible for new runs

## V1 Scope

V1 adds the first expansion layer once MVP semantics are stable.

### V1 Includes

- remote workers over HTTP using the existing pull protocol
- worker capability routing and named queues
- polling connectors as a second wait/resume model (historical extension idea requiring a later explicit scope decision)
- connector registry and packaged built-in connectors
- visual editor that emits the same `FlowDefinition DSL`
- more expressive branching and data mapping
- limited parallel execution with join semantics
- richer administrative controls including pause/resume
- richer observability and run inspection

The preferred transport rollout is:

- `MVP`: in-process worker execution only; HTTP is used for control-plane and event ingest APIs only
- `V1`: add remote workers over HTTP
- later V1 slice: optionally add gRPC as an additional remote worker transport

The worker SDK should hide this transport choice behind the same logical worker interface so user code is not rewritten when gRPC is added.

### V1 Does Not Yet Require

- arbitrary replay of user code
- full Temporal-style deterministic workflow replay
- marketplace-grade extension ecosystem
- unrestricted dynamic graph mutation

## Risks

### Risk: Building a poor local-only shortcut

If the in-process worker is allowed to bypass the protocol, future remote workers will force a second execution model. The design avoids this by requiring the same protocol types and semantics from day one.

### Risk: hiqlite bottleneck on queue-heavy workloads

MVP is intentionally optimized for correctness and architecture validation, not maximum throughput. If queue throughput becomes a bottleneck, a later version can introduce a fast dispatch layer while preserving `hiqlite` as source of truth.

### Risk: DSL drift between SDK and future editor

The SDK must compile to the same published DSL schema the editor will emit. The engine must reject unpublished or incompatible definitions.

### Risk: Long wait complexity

Very long delays are easy to model incorrectly if they are treated like sleeping threads. The current scope avoids this by storing `sleep` and retry wake-ups durably in `timers`.

### Risk: CEL surface area growing too quickly

Adopting `CEL` does not justify exposing the whole language in MVP. The language choice is fixed early, but the supported profile must remain intentionally small until the runtime, validation, and debugging model are proven.

### Historical Risk: weak callback and binding contracts with external systems

The unshipped async proposal would have required versioned, idempotent, and precondition-aware bindings, emitted events, and callback commands. Current definitions instead keep external async orchestration in the higher-level task manager.

## Recommended Build Order

1. Define the `FlowDefinition DSL` schema and publish model.
2. Define the constrained `CEL` profile and binding model.
3. Define runtime tables and state transition rules.
4. Define the pull worker protocol types.
5. Build the in-process worker adapter against that protocol.
6. Implement event ingest, task leasing, and timer firing.
7. Add recovery scanning and inspection APIs.
8. Add remote workers and connectors in V1.

## Next Design Decisions

The core MVP and V1 design boundaries are now defined.

The remaining questions are product and operations choices rather than missing architecture:

1. multi-tenant model and namespace strategy
2. default retention windows for events, dedupe records, and transitions
3. initial transport choice for remote workers in V1 (`HTTP` or `gRPC`)
4. whether batch event ingest is first-class in MVP or deferred

These can be answered without changing the core runtime model.

## Open Questions For Review

The current design assumes these defaults unless changed later:

- single logical namespace in MVP
- long-audit retention posture in MVP, with dedupe retained for `90 days`
- minimal run cancellation in MVP
- `MVP` remote execution path is HTTP-oriented and `V1` adds gRPC as an additional transport
- batch event ingest is first-class in MVP

If product requirements differ here, the architecture can absorb it, but the defaults should be confirmed before implementation planning.

## Recommendation

Proceed with:

- `MVP`: hiqlite-only, code-first, DSL-backed, pull-protocol-based synchronous execution with durable timers
- `V1`: remote workers, editor-ready authoring surface, and expanded synchronous execution features; any new external wait/resume model requires a later explicit scope decision

This keeps the engine small enough to build while preserving the most important future boundary: the worker protocol and DSL-backed runtime.
