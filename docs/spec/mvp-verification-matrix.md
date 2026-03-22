# MVP Verification Matrix

This file maps the current branch's documented MVP claims to the tests that actually prove them.

It is intentionally narrower than the architecture baseline in [`rust-durable-workflow-engine-design.md`](/Users/cypark/.codex/worktrees/0637/conductor/docs/spec/rust-durable-workflow-engine-design.md). If a behavior is described in the baseline but not listed here, it should be treated as `designed` rather than `currently verified`.

## Verification Principles

- Every user-visible MVP claim should point to at least one integration or E2E test.
- Happy-path claims should also assert the durable state that proves the behavior, not only final status.
- Cancellation and recovery tests should prove the negative case, not only the nominal completion path.
- This matrix should be updated when either the spec surface or the test surface changes.

## Verified MVP Surface

| Claim | Evidence |
| --- | --- |
| Draft replace, validate, publish, version list, and version fetch work over HTTP | [`crates/workflow-engine/tests/definition_publish.rs`](/Users/cypark/.codex/worktrees/0637/conductor/crates/workflow-engine/tests/definition_publish.rs) |
| Publish rejects missing handler metadata and stale/unvalidated drafts with explicit failure modes | [`crates/workflow-engine/tests/definition_publish.rs`](/Users/cypark/.codex/worktrees/0637/conductor/crates/workflow-engine/tests/definition_publish.rs) |
| Event ingest creates a run and runnable task that a worker can lease and complete | [`crates/workflow-engine/tests/event_to_task.rs`](/Users/cypark/.codex/worktrees/0637/conductor/crates/workflow-engine/tests/event_to_task.rs) |
| Waiters register durably and resume on a matching event type | [`crates/workflow-engine/tests/wait_resume.rs`](/Users/cypark/.codex/worktrees/0637/conductor/crates/workflow-engine/tests/wait_resume.rs) |
| Cancellation blocks downstream execution and keeps the waiter from resuming | [`crates/workflow-engine/tests/wait_resume.rs`](/Users/cypark/.codex/worktrees/0637/conductor/crates/workflow-engine/tests/wait_resume.rs) |
| Retry timers fire once and expired leases are requeued | [`crates/workflow-engine/tests/recovery.rs`](/Users/cypark/.codex/worktrees/0637/conductor/crates/workflow-engine/tests/recovery.rs) |
| Stale lease completion is rejected after reassignment | [`crates/workflow-engine/tests/recovery.rs`](/Users/cypark/.codex/worktrees/0637/conductor/crates/workflow-engine/tests/recovery.rs) |
| Sleep timers resume downstream work | [`crates/workflow-engine/tests/sleep_timer.rs`](/Users/cypark/.codex/worktrees/0637/conductor/crates/workflow-engine/tests/sleep_timer.rs) |
| Wait timeout fails the step and the run | [`crates/workflow-engine/tests/wait_timeout.rs`](/Users/cypark/.codex/worktrees/0637/conductor/crates/workflow-engine/tests/wait_timeout.rs) |
| `branch` can take the true path and emit a follow-up event | [`crates/workflow-engine/tests/branch_emit.rs`](/Users/cypark/.codex/worktrees/0637/conductor/crates/workflow-engine/tests/branch_emit.rs) |
| `branch` can take the false path and skip `emit_event` | [`crates/workflow-engine/tests/branch_emit.rs`](/Users/cypark/.codex/worktrees/0637/conductor/crates/workflow-engine/tests/branch_emit.rs) |
| Batch event ingest returns per-item dedupe results | [`crates/workflow-engine/tests/batch_ingest.rs`](/Users/cypark/.codex/worktrees/0637/conductor/crates/workflow-engine/tests/batch_ingest.rs) |
| Full HTTP order-sync flow preserves payloads across fetch, wait resume, and notify | [`crates/workflow-engine/tests/order_sync_e2e.rs`](/Users/cypark/.codex/worktrees/0637/conductor/crates/workflow-engine/tests/order_sync_e2e.rs) |

## Verification Command Surface

Primary sweep:

```bash
bash scripts/verify_mvp.sh
```

The script runs:

- formatter check
- clippy on the whole workspace
- the full workspace test suite
- the `order_sync` example
- focused E2E checks for branch/emit, sleep timers, wait timeout, and batch ingest

## Known Designed-But-Not-Yet-Verified Items

These remain part of the architecture direction, but the current branch does not yet prove them:

- general CEL parsing, binding validation, and publish-time type checking
- payload-aware `wait_for_event.match.expr` semantics
- full crash-closed atomicity for event apply, timer firing, and waiter timeout/resume races
- remote worker transport compatibility beyond the in-process adapter

Any PR or release note should avoid claiming those items are finished until they have matching tests in this matrix.
