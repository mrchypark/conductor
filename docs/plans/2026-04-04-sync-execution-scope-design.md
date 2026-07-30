# conductor Sync Execution Scope Design

> Compatibility amendment: the proposed `wait_for_event` and `emit_event` APIs were removed before release. They are absent from authoring, publication, execution, and recovery, and there is no compatibility runtime. Retained unsupported JSON remains immutable and inspectable as diagnostic data only.

## Goal

Refocus conductor as a durable engine for self-contained code execution tasks under a higher-level task manager.

## Confirmed Responsibility Boundary

`conductor` is responsible for:

- workflow definition storage and publish
- durable run creation and state transitions
- task lease, heartbeat, completion, failure, retry, timeout, and recovery
- synchronous code execution against local code or synchronous external systems
- durable delayed re-execution through `sleep`
- internal branching based on currently available input or prior step outputs

`conductor` is not responsible for:

- external asynchronous event waiting
- webhook/callback correlation and resume
- human approval or task inbox semantics
- business object ownership and product state machines
- broad plugin marketplace concerns in this slice

## DSL Direction

The workflow DSL should narrow to primitives that fit the confirmed boundary:

- keep `task`
- keep `sleep`
- keep `branch`
- remove `wait_for_event` from current authoring, publication, and new-run selection
- remove `emit_event` from current authoring, publication, and new-run selection

### Why `sleep` stays

`sleep` is a durable delay primitive, not an asynchronous integration primitive. It lets the engine own "run this code later" without depending on process residency.

### Why `wait_for_event` goes

`wait_for_event` models asynchronous interaction with external systems. That responsibility now belongs to the higher-level task manager, which can decide when to start a new run or invoke the next conductor workflow.

### Why `emit_event` goes

If conductor needs to synchronously notify or call an external system, that should happen inside a `task` handler. A dedicated `emit_event` step widens the engine toward external orchestration semantics that no longer belong inside conductor.

## First Implementation Slice

The first implementation slice is intentionally narrow:

1. enrich `TaskMetadata`
2. make handler metadata an explicit manifest-like contract
3. use that contract at publish and queueing time
4. begin narrowing the DSL by removing `wait_for_event` and `emit_event`

This slice does not include:

- worker/node inventory
- retention and archival
- policy/interceptor hooks
- callback/subflow/batch primitives

## Component Interface Shape

### `workflow-definition`

Represents the stable execution contract.

For `task` steps, metadata should explicitly carry execution routing and timeout hints:

- `handler`
- `capability`
- `queue`
- `timeout_seconds`

These fields may be optional in the draft DSL when defaults come from the worker manifest, but publication must resolve them into an immutable routing snapshot. That published snapshot alone determines execution routing.

### `workflow-worker`

Represents executable handler capability.

The current `WorkerRegistry` already contains the seed of the right abstraction. It should act as the source of handler manifest metadata:

- handler id
- capability
- default queue
- timeout hint

### `workflow-engine`

Validates that published workflows only reference executable handlers, snapshots effective routing metadata at publication, and schedules queueable work from that snapshot instead of mutable local defaults.

## Persistence Direction

Keep these runtime tables as core:

- `workflow_runs`
- `step_instances`
- `task_queue`
- `timers`
- `leases`
- `transition_log`

Do not include asynchronous-wait execution state:

- current publications and runs do not create `waiters`
- unsupported stored definitions are inspectable diagnostics but are never executable
- event ingest starts eligible workflows; it does not resume an in-workflow asynchronous wait

`timers` stay important for:

- retry scheduling
- `sleep` wake-up scheduling

## Testing Direction

The first slice should prove:

- publish uses manifest metadata correctly
- publication snapshots effective `capability`, `queue`, `timeout_seconds`, and routing inputs after registry defaults fill omitted fields; runtime queueing reads the snapshot
- `timeout_seconds` behaves as a step-level deadline, so retries and lease recovery consume the same timeout budget
- unsupported step kinds are absent from the SDK, public definition, publication, runtime, and recovery surfaces
- unsupported and pre-snapshot definitions remain inspectable without becoming eligible for new runs
- existing `task`, `sleep`, and `branch` behavior still works

## Review Notes Applied

This design narrows the earlier review document in two explicit ways:

- the first implementation slice is frozen to one cross-cutting goal: handler metadata + DSL narrowing
- broad interceptor/policy extension points are intentionally out of scope
