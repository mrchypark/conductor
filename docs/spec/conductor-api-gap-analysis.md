# conductor API Gap Analysis

## Goal

Compare the current implementation surface with the target shape described in [`conductor-api-shape-draft.md`](conductor-api-shape-draft.md).

This document is intentionally practical. It answers:

- what already matches
- what is partially aligned
- what is still missing
- what should remain out of scope

## Scope

Compared surfaces:

- current control-plane HTTP routes in [`crates/workflow-engine/src/api/mod.rs`](../../crates/workflow-engine/src/api/mod.rs)
- current HTTP handlers in [`crates/workflow-engine/src/api/workflows.rs`](../../crates/workflow-engine/src/api/workflows.rs), [`crates/workflow-engine/src/api/events.rs`](../../crates/workflow-engine/src/api/events.rs), [`crates/workflow-engine/src/api/runs.rs`](../../crates/workflow-engine/src/api/runs.rs)
- current DTOs in [`crates/workflow-engine/src/definition_store.rs`](../../crates/workflow-engine/src/definition_store.rs), [`crates/workflow-engine/src/runtime/events.rs`](../../crates/workflow-engine/src/runtime/events.rs), [`crates/workflow-engine/src/runtime/tasks.rs`](../../crates/workflow-engine/src/runtime/tasks.rs)
- current worker protocol messages in [`crates/workflow-protocol/src/messages.rs`](../../crates/workflow-protocol/src/messages.rs)

## Summary

The current implementation is already strong on:

- definition draft and publish
- event-driven run start
- run, step, and transition inspection
- worker pull protocol DTOs
- cancellation

The main gaps are:

1. there is no explicit API-level distinction yet between work being `requested` and becoming `ready` for `conductor`
2. worker protocol is DTO-stable but not exposed as a dedicated HTTP worker plane
3. error envelopes are string-based instead of normalized structured codes
4. inspection is sufficient for MVP but still thin for routing and operational debugging
5. a secondary direct-run API does not exist yet for testing or operator shortcuts

## Current State By Surface

## 1. Definition APIs

### Target

From the draft:

- `PUT /v1/workflows/{workflow_id}/draft`
- `POST /v1/workflows/{workflow_id}/draft:validate`
- `POST /v1/workflows/{workflow_id}/draft:publish`
- `GET /v1/workflows/{workflow_id}`
- `GET /v1/workflows/{workflow_id}/versions`
- `GET /v1/workflows/{workflow_id}/versions/{workflow_version}`

### Current implementation

Implemented exactly in [`crates/workflow-engine/src/api/mod.rs`](../../crates/workflow-engine/src/api/mod.rs).

### Gap assessment

- Status: `Aligned`

### Notes

The definition lifecycle is already close to the desired upper task manager contract.

The main remaining issue is not route shape. It is response normalization:

- successful responses currently return full stored definition records
- errors are returned as `(StatusCode, String)` rather than a structured error envelope

## 2. Run Start API

### Target

Preferred target in the draft:

- primary: `POST /v1/events` with ready-to-start events
- secondary: `POST /v1/workflows/{workflow_id}/runs` for testing, replay, or operator actions

### Current implementation

Current run start is only available through:

- `POST /v1/events`

Implemented in [`crates/workflow-engine/src/api/events.rs`](../../crates/workflow-engine/src/api/events.rs)

Current request DTO:

```json
{
  "events": [
    {
      "id": "evt-1",
      "name": "order.created",
      "idempotency_key": "evt-1",
      "data": {}
    }
  ]
}
```

Current response DTO:

```json
{
  "accepted": true,
  "event_id": "event_123",
  "deduplicated": false,
  "started_runs": [
    {
      "run_id": "run_123",
      "workflow_id": "order-sync",
      "workflow_version": 3
    }
  ]
}
```

### Gap assessment

- Status: `Partially aligned`

### Real gap

The current API can start runs, and that is directionally correct for an event-driven engine. The real gap is that the documented API does not yet make the business distinction between `requested` work and `ready` work explicit.

That creates two boundary problems and one optional ergonomics gap:

1. the upper task manager could be tempted to treat any request event as a start event
2. readiness semantics are not formalized in the contract
3. direct run creation for testing or operator use still has no dedicated route

### Recommendation

- keep `POST /v1/events` as the primary start path
- document `*_requested` vs `*_ready` event semantics explicitly
- add `POST /v1/workflows/{workflow_id}/runs` only as a secondary path

### Priority

- `P0 for boundary clarity`

## 3. Workflow Summary API

### Target

The draft assumes inspection of:

- latest published definition
- published versions
- run state

### Current implementation

`GET /v1/workflows/{workflow_id}` returns [`WorkflowSummary`](../../crates/workflow-engine/src/definition_store.rs) with:

- `workflow_id`
- `draft`
- `latest_published`

### Gap assessment

- Status: `Aligned`

### Notes

This shape is already useful for the upper task manager.

The only meaningful next step is to normalize errors and possibly add lightweight metadata-only views later if payload size becomes a concern.

## 4. Run Inspection APIs

### Target

Draft target:

- `GET /v1/runs/{run_id}`
- `GET /v1/runs/{run_id}/steps`
- `GET /v1/runs/{run_id}/transitions`
- `POST /v1/runs/{run_id}/cancel`

### Current implementation

Implemented exactly in [`crates/workflow-engine/src/api/mod.rs`](../../crates/workflow-engine/src/api/mod.rs) and [`crates/workflow-engine/src/api/runs.rs`](../../crates/workflow-engine/src/api/runs.rs).

Current DTOs:

- [`WorkflowRunRecord`](../../crates/workflow-engine/src/definition_store.rs)
- [`StepInstanceRecord`](../../crates/workflow-engine/src/runtime/tasks.rs)
- [`TransitionRecord`](../../crates/workflow-engine/src/runtime/tasks.rs)

### Gap assessment

- Status: `Aligned`

### Notes

This surface already fits the sync-only boundary well.

One limitation remains:

- the current run record is execution-focused and intentionally thin
- it does not include aggregated final output or routing metadata summary

That is acceptable for now because the upper task manager can derive meaning from step records and transitions.

## 5. Cancellation API

### Target

Draft target:

- `POST /v1/runs/{run_id}/cancel`

### Current implementation

Implemented exactly with:

- optional `reason`
- optional `requested_by`

### Gap assessment

- Status: `Aligned`

### Notes

The route shape is already good enough for the current boundary.

## 6. Worker Protocol

### Target

The draft separates worker interaction into a worker plane with:

- `poll`
- `heartbeat`
- `complete`
- `fail`

Recommended future HTTP mapping:

- `POST /v1/worker/tasks:poll`
- `POST /v1/worker/tasks:heartbeat`
- `POST /v1/worker/tasks:complete`
- `POST /v1/worker/tasks:fail`

### Current implementation

The stable DTOs already exist in [`crates/workflow-protocol/src/messages.rs`](../../crates/workflow-protocol/src/messages.rs):

- `PollTask`
- `PollTaskResponse`
- `HeartbeatLease`
- `CompleteTask`
- `FailTask`

And the engine already exposes matching application methods through [`WorkflowEngineApp`](../../crates/workflow-engine/src/app.rs):

- `poll_task`
- `heartbeat_lease`
- `complete_task`
- `fail_task`

### Gap assessment

- Status: `Partially aligned`

### Real gap

The contract is stable at the DTO and application boundary, but not yet exposed as a dedicated HTTP worker plane.

This means:

- in-process workers are first-class
- future remote workers still need transport wiring

### Recommendation

- keep protocol DTOs as the stable source of truth
- add a transport adapter later instead of redesigning the message shapes

### Priority

- `P1`

## 7. Error Shape

### Target

Draft target:

```json
{
  "error": {
    "code": "handler_manifest_mismatch",
    "message": "..."
  }
}
```

### Current implementation

Handlers currently return:

- status code
- plain string body

Examples:

- `409 Conflict` with `error.to_string()`
- `404 Not Found` with `"run not found"`

### Gap assessment

- Status: `Missing`

### Consequence

This is acceptable for tests and MVP development, but it is weak for:

- upper task manager retry policy
- client-side error handling
- operator dashboards
- long-term public API stability

### Recommendation

- introduce a normalized error envelope
- preserve current status code mapping
- derive machine-readable `code` from `WorkflowEngineError`, `PublishDraftError`, and `TaskRuntimeError`

### Priority

- `P1`

## 8. Idempotency Model

### Target

Draft target:

- explicit `idempotency_key` on ready-event ingest remains the primary dedupe path
- direct run start, if added, may also accept its own optional `idempotency_key`

### Current implementation

Current idempotency behavior exists only in event ingest:

- `EventEnvelope.idempotency_key`
- dedupe against `event_inbox`

### Gap assessment

- Status: `Partially aligned`

### Real gap

There is no direct-run-specific idempotency contract because explicit run start does not exist yet.

### Recommendation

- keep event-ingest idempotency as the primary product-path dedupe model
- when adding `POST /v1/workflows/{workflow_id}/runs`, include optional `idempotency_key`
- define `200 existing` vs `201 created` semantics clearly

## 9. Version Selection Semantics

### Target

Draft target:

- explicit version optional
- latest eligible published version used when omitted
- unsupported definitions are never eligible for new runs; the proposed `wait_for_event`/`emit_event` APIs were removed before release
- versions published before routing defaults were snapshotted are not eligible until explicitly republished
- retained malformed or unsupported versions remain immutable and inspectable even when ineligible

### Current implementation

Current behavior:

- event-based start selects latest supported published definition
- published-definition compatibility already falls back when the newest stored version is unsupported

### Gap assessment

- Status: `Partially aligned`

### Real gap

Unsupported-definition filtering exists internally, but the complete lifecycle contract is not yet verified. The remaining work must distinguish retention from new-run eligibility, persist the resolved routing snapshot, reject pre-snapshot versions for new runs until explicit republish, and block destructive removal while active references exist. Unsupported definitions have no compatibility execution path. Direct run start, if added, must enforce the same eligibility rules.

## 10. Inspection Depth

### Target

The draft wants inspection to explain:

- definition version
- current or failed step
- transition history
- routing and timeout rules that applied

### Current implementation

Current inspection gives:

- run
- steps
- transitions

It does not yet expose:

- task routing summary
- worker inventory
- queue inspection
- per-run effective manifest view

### Gap assessment

- Status: `Partial but acceptable`

### Recommendation

Keep current inspection surface for MVP. Expand later only if operational pressure justifies it.

## Recommended Implementation Order

### P0

1. Formalize `requested` vs `ready` semantics in the API contract
2. Keep ready-event ingest as the primary start path
3. Make examples and product docs use `*_ready` style start events

### P1

1. Add structured error envelope
2. Add worker-plane HTTP adapter if remote workers become a real need
3. Add secondary direct-run route:
   - `POST /v1/workflows/{workflow_id}/runs`
4. Add direct-run idempotency semantics

### P2

1. Add richer operational inspection only after worker inventory exists
2. Add metadata-only inspection variants if payload size becomes a problem

## Explicit Non-Goals

This gap analysis does not recommend:

- reintroducing callback resume APIs
- adding waiter inspection APIs
- turning event ingest into a general async orchestration surface
- widening `conductor` beyond sync execution plus durable timers

## Final Assessment

The current API is already structurally close to the target design.

The most important missing piece is not a broad redesign. It is clearer start semantics:

- requested work and ready-to-start work must be distinguished explicitly

Only after that, the most useful optional addition is:

- a secondary explicit direct run-start route for testing and operator use

After that, the next useful improvements are:

- structured error envelopes
- eventual worker transport exposure for remote workers
