# conductor API Shape Draft

## Goal

Describe the recommended API surface for the narrowed sync-only `conductor` boundary.

This draft separates:

- current implemented control-plane HTTP
- recommended target upper-task-manager API
- worker protocol surface

Gap analysis companion:

- [`conductor-api-gap-analysis.md`](conductor-api-gap-analysis.md)

## Design Principles

- keep the upper task manager API small and business-neutral
- keep worker execution as a separate pull protocol
- do not leak async orchestration semantics into the `conductor` API
- prefer explicit versioning and inspection over implicit behavior
- treat ready-to-start events as the primary product start path

## API Planes

### Control Plane

Used by the upper task manager to:

- publish definitions
- start runs
- inspect runs
- cancel runs

### Worker Plane

Used by workers to:

- poll runnable tasks
- heartbeat leases
- complete tasks
- fail tasks

### Inspection Plane

Used by developers and operators to:

- inspect definitions
- inspect runs, steps, and transitions
- understand routing and failure outcomes

## Current Implemented HTTP Surface

The current branch already exposes these control-plane routes:

### Workflow definition routes

- `PUT /v1/workflows/{workflow_id}/draft`
- `POST /v1/workflows/{workflow_id}/draft:validate`
- `POST /v1/workflows/{workflow_id}/draft:publish`
- `GET /v1/workflows/{workflow_id}`
- `GET /v1/workflows/{workflow_id}/versions`
- `GET /v1/workflows/{workflow_id}/versions/{workflow_version}`

Published-version history is ordered by signed `workflow_version` and uses the exclusive `after_version` keyset. `limit` defaults to 100 and must be between 1 and 100. Continue by sending the last returned record's `workflow_version`; this preserves retained negative, above-`u32`, and diagnostic rows. Invalid limits or cursors return `400 Bad Request`, and the exact-version GET is unchanged.

### Event-based start route

- `POST /v1/events`

This is the currently implemented run-start path. For the narrowed sync-only boundary, it is also the preferred primary product path when the upper task manager emits a ready-to-start event.

### Run inspection and control routes

- `GET /v1/runs/{run_id}`
- `GET /v1/runs/{run_id}/steps`
- `GET /v1/runs/{run_id}/transitions`
- `POST /v1/runs/{run_id}/cancel`

## Recommended Target Upper Task Manager API

For the upper task manager, the preferred long-term surface is event ingest for ready-to-start business events, with direct run start available only as a secondary administrative or testing path.

### Definitions

#### `PUT /v1/workflows/{workflow_id}/draft`

Purpose:

- replace the current draft definition

Request body:

```json
{
  "workflow_id": "order-sync",
  "workflow_version": 0,
  "trigger": {
    "type": "event",
    "event_type": "order.created"
  },
  "steps": [],
  "edges": []
}
```

Response:

- stored draft record

#### `POST /v1/workflows/{workflow_id}/draft:validate`

Purpose:

- validate the current draft and produce a content hash

Response:

```json
{
  "ok": true,
  "errors": [],
  "warnings": [],
  "content_hash": "sha256:..."
}
```

#### `POST /v1/workflows/{workflow_id}/draft:publish`

Purpose:

- publish the validated draft as an immutable version

Request:

```json
{
  "content_hash": "sha256:..."
}
```

Response:

- published definition record

### Primary start route

#### `POST /v1/events`

Purpose:

- ingest ready-to-start events from the upper task manager

Recommended event taxonomy:

- `*_requested` stays in the upper task manager
- `*_ready` is the event family that should normally enter `conductor`

Example:

```json
{
  "events": [
    {
      "id": "evt-order-sync-ready-1",
      "name": "order_sync_ready",
      "idempotency_key": "order-sync-ready-ord-123",
      "data": {
        "order_id": "ord-123"
      }
    }
  ]
}
```

Semantics:

- event ingest starts new runs
- it does not resume pending waits
- readiness is determined before the event reaches `conductor`

### Secondary direct run-start route

#### `POST /v1/workflows/{workflow_id}/runs`

Purpose:

- allow direct run creation for testing, replay, operator actions, or internal shortcuts
- this is not the preferred product boundary for normal business flow starts

Request:

```json
{
  "workflow_version": 3,
  "input": {
    "order_id": "ord-123"
  },
  "idempotency_key": "tm-order-123-sync"
}
```

Semantics:

- `workflow_version` optional
- when omitted, the latest published version eligible for new runs is used
- an explicit version must also be eligible for new runs; retained unsupported or pre-routing-snapshot versions remain inspectable but cannot be selected
- `idempotency_key` optional

Response:

```json
{
  "run_id": "run_123",
  "workflow_id": "order-sync",
  "workflow_version": 3,
  "status": "running"
}
```

Status mapping:

- `201 Created` for new run
- `200 OK` if idempotency resolves to an existing run
- `404 Not Found` if no published definition exists
- `409 Conflict` if the selected published version is invalid for execution

### Run inspection routes

#### `GET /v1/runs/{run_id}`

Returns:

- run id
- workflow id
- workflow version
- status
- cancellation metadata if present
- created timestamp

#### `GET /v1/runs/{run_id}/steps`

Returns:

- ordered step instances
- step status
- attempt count
- input, output, and error where available

Query contract:

- `limit` defaults to 100 and must be between 1 and 100
- results are ordered by `(created_at, step_instance_id)`
- `after_created_at` and `after_step_instance_id` form an exclusive composite keyset and must be supplied together
- continue by copying both cursor values from the last returned record

#### `GET /v1/runs/{run_id}/transitions`

Returns:

- transition log for debugging and auditability

Query contract:

- `limit` defaults to 100 and must be between 1 and 100
- results are ordered by `(created_at, transition_id)`
- `after_created_at` and `after_transition_id` form an exclusive composite keyset and must be supplied together
- continue by copying both cursor values from the last returned record

Both run-history routes return `400 Bad Request` for malformed values, out-of-range limits, or half-cursors. Omitted query fields return the first 100 records; the service does not silently clamp invalid limits.

#### `POST /v1/runs/{run_id}/cancel`

Purpose:

- request cancellation of a running workflow

Request:

```json
{
  "reason": "operator_requested",
  "requested_by": "task-manager"
}
```

## Worker Protocol Surface

The worker-facing contract is DTO-first and may be exposed over HTTP, gRPC, or an in-process adapter. The stable message shapes are:

### `PollTask`

```json
{
  "worker_id": "worker-1",
  "worker_name": "sync-worker-1",
  "queues": ["default"],
  "capabilities": ["task"],
  "max_inflight": 1
}
```

### `PollTaskResponse`

Assigned:

```json
{
  "status": "assigned",
  "task_id": "task_123",
  "run_id": "run_123",
  "attempt_id": "attempt_123",
  "fencing_token": 7,
  "workflow_id": "order-sync",
  "workflow_version": 3,
  "step_id": "fetch_order",
  "step_kind": "task",
  "handler_id": "orders.fetch",
  "input": {
    "order_id": "ord-123"
  }
}
```

No task available:

```json
{
  "status": "no_task_available"
}
```

### `HeartbeatLease`

Carries the current lease identity:

- `task_id`
- `attempt_id`
- `fencing_token`

### `CompleteTask`

Carries:

- lease identity
- result payload

### `FailTask`

Carries:

- lease identity
- failure payload

## Recommended Worker Transport Mapping

If HTTP is used later, the worker routes should stay separate from upper-task-manager routes.

Recommended shape:

- `POST /v1/worker/tasks:poll`
- `POST /v1/worker/tasks:heartbeat`
- `POST /v1/worker/tasks:complete`
- `POST /v1/worker/tasks:fail`

Reason:

- keeps control-plane and worker-plane concerns separate
- makes auth and rate-limiting policy easier to split later

## Error Shape Draft

Recommended normalized error envelope:

```json
{
  "error": {
    "code": "handler_manifest_mismatch",
    "message": "handler metadata does not match published step requirements"
  }
}
```

Recommended error code families:

- `draft_not_found`
- `draft_not_validated`
- `stale_draft_content`
- `handler_manifest_mismatch`
- `task_timeout_out_of_range`
- `published_definition_not_found`
- `run_not_found`
- `lease_conflict`
- `unsupported_step_kind`

## API Boundaries To Avoid

Do not add these to the `conductor` API surface:

- callback resume endpoints
- waiter inspection endpoints
- human approval endpoints
- product-specific business transition endpoints
- generic external event bus semantics

## Summary

The preferred API shape is:

- small control-plane HTTP for definitions, run start, inspection, and cancellation
- separate worker pull protocol for execution
- explicit support for sync-only task execution, `sleep`, and `branch`
- no async callback-resume semantics in the engine-facing API
