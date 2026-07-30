# MVP Verification Matrix

This file maps the current branch's documented MVP claims to the tests that actually prove them.

It is intentionally narrower than the architecture baseline in [`rust-durable-workflow-engine-design.md`](rust-durable-workflow-engine-design.md). If a behavior is described in the baseline but not listed here, it should be treated as `designed` rather than `currently verified`.

## Verification Principles

- Every user-visible MVP claim should point to at least one integration or E2E test.
- Happy-path claims should also assert the durable state that proves the behavior, not only final status.
- Cancellation and recovery tests should prove the negative case, not only the nominal completion path.
- This matrix should be updated when either the spec surface or the test surface changes.

## Verified MVP Surface

| Claim | Evidence |
| --- | --- |
| Draft replace, validate, publish, version list, and version fetch work over HTTP | [`crates/workflow-engine/tests/definition_publish.rs`](../../crates/workflow-engine/tests/definition_publish.rs) |
| Publish rejects missing handler metadata, capability mismatches, invalid task timeouts, and stale/unvalidated drafts with explicit failure modes | [`crates/workflow-engine/tests/definition_publish.rs`](../../crates/workflow-engine/tests/definition_publish.rs) |
| Event ingest creates a run and runnable task that a worker can lease and complete, while explicit task routing metadata overrides registry defaults | [`crates/workflow-engine/tests/event_to_task.rs`](../../crates/workflow-engine/tests/event_to_task.rs) |
| Retry timers fire once and expired leases are requeued | [`crates/workflow-engine/tests/recovery.rs`](../../crates/workflow-engine/tests/recovery.rs) |
| Stale lease completion is rejected after reassignment | [`crates/workflow-engine/tests/recovery.rs`](../../crates/workflow-engine/tests/recovery.rs) |
| Sleep timers resume downstream work | [`crates/workflow-engine/tests/sleep_timer.rs`](../../crates/workflow-engine/tests/sleep_timer.rs) |
| `branch` can take the true path and schedule a follow-up task in the same workflow | [`crates/workflow-engine/tests/branch_emit.rs`](../../crates/workflow-engine/tests/branch_emit.rs) |
| `branch` can take the false path and finish without creating extra runnable work | [`crates/workflow-engine/tests/branch_emit.rs`](../../crates/workflow-engine/tests/branch_emit.rs) |
| Batch event ingest returns per-item dedupe results | [`crates/workflow-engine/tests/batch_ingest.rs`](../../crates/workflow-engine/tests/batch_ingest.rs) |
| Full HTTP order-sync flow preserves payloads across fetch, durable sleep, and notify | [`crates/workflow-engine/tests/order_sync_e2e.rs`](../../crates/workflow-engine/tests/order_sync_e2e.rs) |

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
- focused E2E checks for branch routing, sleep timers, `order_sync`, and batch ingest

## Known Designed-But-Not-Yet-Verified Items

These remain part of the architecture direction, but the current branch does not yet prove them:

- general CEL parsing, binding validation, and publish-time type checking
- full crash-closed atomicity for event apply and timer firing races
- remote worker transport compatibility beyond the in-process adapter
- any async external wait or callback-resume semantics, which are now out of current conductor scope
- the complete `0.2.0` definition lifecycle: immutable raw inspection of malformed or unsupported rows, publication/run eligibility gates, persisted routing-default snapshots, and active-reference deletion guards; unsupported definitions have no execution path

Any PR or release note should avoid claiming those items are finished until they have matching tests in this matrix.
