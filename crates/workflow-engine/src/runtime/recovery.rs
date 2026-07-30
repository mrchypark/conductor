use serde_json::json;

use super::{
    tasks::{TIMER_KIND_RETRY, TIMER_KIND_SLEEP},
    TaskRuntime, TaskRuntimeError,
};
use crate::{
    persistence::{
        ContinuationQuarantineResult, ContinuationQuarantineStaleReason, TaskOutcomeApplyResult,
        TimerClaimResult,
    },
    stored_definition::StoredDefinitionClassification,
};

impl TaskRuntime {
    pub async fn recovery_scan_at(&self, now: i64) -> Result<(), TaskRuntimeError> {
        let timed_out_tasks = self.persistence.list_timed_out_tasks(now).await?;
        for task in timed_out_tasks {
            match self
                .persistence
                .expire_task_if_due(&task.task_id, now)
                .await?
            {
                TaskOutcomeApplyResult::Applied
                | TaskOutcomeApplyResult::AlreadyApplied
                | TaskOutcomeApplyResult::Stale => {}
                TaskOutcomeApplyResult::Expired => {
                    return Err(TaskRuntimeError::Persistence(
                        crate::persistence::PersistenceError::Invariant(format!(
                            "timeout claim for task '{}' unexpectedly remained expired",
                            task.task_id
                        )),
                    ));
                }
            }
        }

        let requeued = self.persistence.requeue_expired_leases(now).await?;
        for task in requeued {
            self.record_transition(
                &task.run_id,
                "lease_requeued",
                Some(json!({
                    "task_id": task.task_id,
                    "step_id": task.step_id,
                })),
                now,
            )
            .await?;
        }

        let continuations = self
            .persistence
            .list_pending_completed_task_continuations(now)
            .await?;
        for continuation in continuations {
            self.apply_task_continuation(continuation, now).await?;
        }

        let due_timers = self.persistence.list_due_timers(now).await?;
        for timer in due_timers {
            match timer.timer_kind.as_str() {
                TIMER_KIND_RETRY => {
                    let task_id = timer
                        .payload
                        .as_ref()
                        .and_then(|value| value.get("task_id"))
                        .and_then(|value| value.as_str());
                    let task_id = task_id.ok_or_else(|| {
                        TaskRuntimeError::Persistence(
                            crate::persistence::PersistenceError::Invariant(
                                "retry timer payload missing task_id".to_owned(),
                            ),
                        )
                    })?;
                    self.persistence
                        .apply_retry_timer_if_due(&timer.timer_id, task_id, now)
                        .await?;
                }
                TIMER_KIND_SLEEP => {
                    let step = self
                        .persistence
                        .get_step_instance(&timer.run_id, &timer.step_id)
                        .await?
                        .ok_or_else(|| TaskRuntimeError::StepInstanceNotFound {
                            run_id: timer.run_id.clone(),
                            step_id: timer.step_id.clone(),
                        })?;
                    let output = timer
                        .payload
                        .as_ref()
                        .and_then(|value| value.get("input"))
                        .cloned()
                        .or(step.input)
                        .unwrap_or_else(|| json!({}));
                    if self
                        .persistence
                        .claim_sleep_timer_if_due(&timer.timer_id, &output, now)
                        .await?
                        == TimerClaimResult::NotClaimable
                    {
                        continue;
                    }
                    let run = self
                        .persistence
                        .get_run(&timer.run_id)
                        .await?
                        .ok_or_else(|| TaskRuntimeError::RunNotFound {
                            run_id: timer.run_id.clone(),
                        })?;
                    if run.status != super::tasks::RUN_STATUS_RUNNING {
                        continue;
                    }
                    let definition = match self
                        .load_pinned_execution_definition(&run.workflow_id, run.workflow_version)
                        .await
                    {
                        Ok(definition) => definition,
                        Err(_) => {
                            self.quarantine_timer_effect(
                                &timer.timer_id,
                                "pinned_definition_invalid_or_missing",
                                now,
                            )
                            .await?;
                            continue;
                        }
                    };
                    match definition.classification() {
                        StoredDefinitionClassification::SnapshottedV1 => {
                            self.schedule_downstream_steps(
                                &timer.run_id,
                                &definition,
                                &timer.step_id,
                                output,
                                now,
                            )
                            .await?;
                        }
                        StoredDefinitionClassification::PreSnapshot => {
                            self.quarantine_timer_effect(
                                &timer.timer_id,
                                "pre_snapshot_execution_metadata_missing",
                                now,
                            )
                            .await?;
                            continue;
                        }
                    }
                    match self
                        .persistence
                        .mark_timer_effect_applied(&timer.timer_id, now)
                        .await?
                    {
                        super::MutationApplyResult::Applied
                        | super::MutationApplyResult::AlreadyApplied => {
                            self.reconcile_run_status(&timer.run_id, now).await?;
                        }
                        super::MutationApplyResult::Stale => {}
                    }
                }
                _ => {}
            }
        }

        let pending_events = self.persistence.list_received_events().await?;
        for event in pending_events {
            self.persistence
                .ensure_event_received_transition(&event.event_id, &event.event_type, now)
                .await?;
            self.apply_event(&event.event_id, &event.event_type, &event.payload, now)
                .await?;
            self.persistence
                .mark_event_applied(&event.event_id, now)
                .await?;
        }
        Ok(())
    }

    async fn quarantine_timer_effect(
        &self,
        timer_id: &str,
        code: &str,
        now: i64,
    ) -> Result<(), TaskRuntimeError> {
        match self
            .persistence
            .quarantine_timer_effect(timer_id, code, now)
            .await?
        {
            ContinuationQuarantineResult::Applied
            | ContinuationQuarantineResult::AlreadyApplied
            | ContinuationQuarantineResult::Stale(
                ContinuationQuarantineStaleReason::RunNotRunning { .. },
            ) => Ok(()),
            ContinuationQuarantineResult::Stale(reason) => Err(TaskRuntimeError::Persistence(
                crate::persistence::PersistenceError::Invariant(format!(
                    "timer effect '{timer_id}' could not be quarantined: {reason:?}"
                )),
            )),
            ContinuationQuarantineResult::Conflict { existing_state } => {
                Err(TaskRuntimeError::Persistence(
                    crate::persistence::PersistenceError::Invariant(format!(
                        "timer effect '{timer_id}' has conflicting quarantine state {existing_state:?}"
                    )),
                ))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use serde_json::json;
    use tempfile::TempDir;
    use workflow_definition::{
        Edge, EventTrigger, FlowDefinition, SleepMetadata, StepDefinition, StepKind, StepMetadata,
        TaskMetadata, Trigger, WorkflowDefaults,
    };
    use workflow_protocol::{
        CompleteTask, PollTask, PollTaskResponse, TaskMutationContext, TaskOutput,
    };
    use workflow_worker::{Capability, WorkerRegistry};

    use crate::{
        definition_store::{StoredWorkflowDefinition, WorkflowRunRecord},
        persistence::{HiqlitePersistence, TaskOutcomeApplyResult, TimerClaimResult},
        runtime::{
            tasks::{
                StepInstanceRecord, TimerRecord, RUN_STATUS_RUNNING, STEP_STATUS_SLEEPING,
                TIMER_EFFECT_STATE_PENDING, TIMER_KIND_RETRY, TIMER_KIND_SLEEP, TIMER_STATUS_FIRED,
                TIMER_STATUS_PENDING,
            },
            TaskRuntime, TaskRuntimeError,
        },
        stored_definition::{PublicationExecutionMetadata, ResolvedTaskRoute},
    };

    #[tokio::test]
    async fn recovery_drains_directly_persisted_completion_once() {
        let (tempdir, persistence, registry, runtime, lease, now) =
            pending_completion_fixture("run-continuation-recovery").await;

        assert_eq!(
            persistence
                .complete_task_with_continuation(
                    &lease,
                    &TaskOutput {
                        payload: json!({ "order_id": "ord-123", "ready": true }),
                    },
                    now + 1,
                )
                .await
                .expect("direct completion should commit"),
            TaskOutcomeApplyResult::Applied
        );

        let recovered = TaskRuntime::new(persistence.clone(), registry);
        recovered
            .recovery_scan_at(now + 2)
            .await
            .expect("fresh runtime should drain continuation");
        recovered
            .recovery_scan_at(now + 3)
            .await
            .expect("second recovery should remain idempotent");

        assert_continuation_scheduled_child_once(&persistence, &recovered, &lease.task_id).await;
        drop(tempdir);
        drop(runtime);
    }

    #[tokio::test]
    async fn recovery_replays_child_inserted_before_continuation_marker_without_duplicates() {
        let (tempdir, persistence, registry, runtime, lease, now) =
            pending_completion_fixture("run-continuation-replay").await;
        let output = json!({ "order_id": "ord-456", "ready": true });
        assert_eq!(
            persistence
                .complete_task_with_continuation(
                    &lease,
                    &TaskOutput {
                        payload: output.clone(),
                    },
                    now + 1,
                )
                .await
                .expect("direct completion should commit"),
            TaskOutcomeApplyResult::Applied
        );

        let definition = runtime
            .load_current_execution_definition("continuation-flow", 1)
            .await
            .expect("published definition should load");
        runtime
            .schedule_downstream_steps(
                "run-continuation-replay",
                &definition,
                "prepare",
                output,
                now + 1,
            )
            .await
            .expect("child should be inserted before simulated crash");

        let recovered = TaskRuntime::new(persistence.clone(), registry);
        recovered
            .recovery_scan_at(now + 2)
            .await
            .expect("fresh runtime should finish pending marker");
        recovered
            .recovery_scan_at(now + 3)
            .await
            .expect("second recovery should remain idempotent");

        assert_continuation_scheduled_child_once(&persistence, &recovered, &lease.task_id).await;
        drop(tempdir);
    }

    #[tokio::test]
    async fn completion_at_timeout_is_terminal_and_late_replay_stays_stale() {
        let (_tempdir, persistence, _registry, runtime, lease, _now) =
            pending_completion_fixture("run-timeout-equality-runtime").await;
        let deadline = persistence
            .get_task_snapshot(&lease.task_id)
            .await
            .expect("task should load")
            .expect("task should exist")
            .timeout_at
            .expect("fixture task should have a timeout");

        for completion_at in [deadline, deadline + 1] {
            let error = runtime
                .complete_task_at(
                    CompleteTask {
                        lease: lease.clone(),
                        result: TaskOutput {
                            payload: json!({ "late": true }),
                        },
                    },
                    completion_at,
                )
                .await
                .expect_err("completion at or after the deadline should be stale");
            assert!(matches!(
                error,
                TaskRuntimeError::StaleLease { ref task_id } if task_id == &lease.task_id
            ));
        }

        assert_eq!(
            persistence
                .get_run("run-timeout-equality-runtime")
                .await
                .expect("run should load")
                .expect("run should exist")
                .status,
            "failed"
        );
        let transitions = persistence
            .list_transitions("run-timeout-equality-runtime")
            .await
            .expect("transitions should load");
        assert_eq!(
            transitions
                .iter()
                .filter(|transition| transition.transition_type == "task_timed_out")
                .count(),
            1
        );
        assert_eq!(
            transitions
                .iter()
                .filter(|transition| transition.transition_type == "run_failed")
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn concurrent_scanners_apply_one_sleep_effect_and_stop_rescanning_it() {
        let (_tempdir, persistence, runtime, now) =
            pending_sleep_fixture("run-concurrent-sleep").await;
        let left = runtime.clone();
        let right = runtime.clone();
        let (left_result, right_result) =
            tokio::join!(left.recovery_scan_at(now), right.recovery_scan_at(now));
        left_result.expect("left scanner should succeed");
        right_result.expect("right scanner should succeed");

        let steps = persistence
            .list_step_instances("run-concurrent-sleep")
            .await
            .expect("steps should load");
        assert_eq!(steps.len(), 2);
        assert_eq!(
            steps
                .iter()
                .filter(|step| step.step_id == "send_notification")
                .count(),
            1
        );
        let transitions = persistence
            .list_transitions("run-concurrent-sleep")
            .await
            .expect("transitions should load");
        assert_eq!(
            transitions
                .iter()
                .filter(|transition| transition.transition_type == "timer_fired")
                .count(),
            1
        );
        assert_eq!(
            transitions
                .iter()
                .filter(|transition| {
                    transition.transition_type == "task_queued"
                        && transition
                            .payload
                            .as_ref()
                            .and_then(|payload| payload.get("step_id"))
                            .and_then(|step_id| step_id.as_str())
                            == Some("send_notification")
                })
                .count(),
            1
        );
        assert!(persistence
            .list_due_timers(now + 1)
            .await
            .expect("applied timer should not remain due")
            .is_empty());
    }

    #[tokio::test]
    async fn due_timer_scan_is_deterministic_and_bounded_to_one_hundred_rows() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let persistence = HiqlitePersistence::for_test(tempdir.path())
            .await
            .expect("persistence should start");
        let now = 1_700_000_000;
        for index in (0..105).rev() {
            persistence
                .insert_timer(&TimerRecord {
                    timer_id: format!("timer-{index:03}"),
                    run_id: "run-batch".to_owned(),
                    step_id: format!("step-{index:03}"),
                    timer_kind: TIMER_KIND_SLEEP.to_owned(),
                    due_at: now,
                    status: TIMER_STATUS_PENDING.to_owned(),
                    effect_state: Some(TIMER_EFFECT_STATE_PENDING.to_owned()),
                    payload: None,
                    created_at: now,
                    updated_at: now,
                })
                .await
                .expect("timer should insert");
        }

        let due = persistence
            .list_due_timers(now)
            .await
            .expect("due timers should load");
        assert_eq!(due.len(), 100);
        assert_eq!(
            due.first().map(|timer| timer.timer_id.as_str()),
            Some("timer-000")
        );
        assert_eq!(
            due.last().map(|timer| timer.timer_id.as_str()),
            Some("timer-099")
        );
    }

    #[tokio::test]
    async fn cancellation_after_sleep_claim_suppresses_effect_without_descendants() {
        let (_tempdir, persistence, runtime, now) =
            pending_sleep_fixture("run-cancel-after-sleep-claim").await;
        assert_eq!(
            persistence
                .claim_sleep_timer_if_due(
                    "timer-sleep-run-cancel-after-sleep-claim",
                    &json!({ "order_id": "ord-cancel" }),
                    now,
                )
                .await
                .expect("sleep claim should commit"),
            TimerClaimResult::Claimed
        );

        runtime
            .cancel_run(
                "run-cancel-after-sleep-claim",
                Some("cancel_after_claim"),
                Some("test"),
            )
            .await
            .expect("cancellation should commit");
        runtime
            .recovery_scan_at(now + 1)
            .await
            .expect("recovery should ignore suppressed effect");

        let steps = persistence
            .list_step_instances("run-cancel-after-sleep-claim")
            .await
            .expect("steps should load");
        assert_eq!(steps.len(), 1);
        assert_eq!(
            persistence
                .get_run("run-cancel-after-sleep-claim")
                .await
                .expect("run should load")
                .expect("run should exist")
                .status,
            "cancelled"
        );
        assert!(persistence
            .list_due_timers(now + 1)
            .await
            .expect("suppressed timer should not remain due")
            .is_empty());
    }

    #[tokio::test]
    async fn claimed_sleep_timer_keeps_run_running_until_recovery_applies_its_effect() {
        let (_tempdir, persistence, runtime, now) =
            pending_sleep_fixture("run-claimed-sleep-replay").await;
        assert_eq!(
            persistence
                .claim_sleep_timer_if_due(
                    "timer-sleep-run-claimed-sleep-replay",
                    &json!({ "order_id": "ord-replay" }),
                    now,
                )
                .await
                .expect("sleep timer should be claimed before its effect is applied"),
            TimerClaimResult::Claimed
        );
        assert!(persistence
            .run_has_open_work("run-claimed-sleep-replay")
            .await
            .expect("fired unapplied timer should count as open work"));

        runtime
            .reconcile_run_status("run-claimed-sleep-replay", now + 1)
            .await
            .expect("reconciliation should leave the run open");
        assert_eq!(
            persistence
                .get_run("run-claimed-sleep-replay")
                .await
                .expect("run should load")
                .expect("run should exist")
                .status,
            RUN_STATUS_RUNNING
        );

        runtime
            .recovery_scan_at(now + 2)
            .await
            .expect("recovery should replay the claimed timer effect");
        let steps = persistence
            .list_step_instances("run-claimed-sleep-replay")
            .await
            .expect("steps should load");
        assert_eq!(steps.len(), 2);
        assert_eq!(
            steps
                .iter()
                .filter(|step| step.step_id == "send_notification")
                .count(),
            1
        );
        assert!(persistence
            .list_due_timers(now + 2)
            .await
            .expect("applied timer should leave the recovery queue")
            .is_empty());
    }

    #[tokio::test]
    async fn cancellation_before_completion_cas_cannot_emit_completed_transition() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let persistence = HiqlitePersistence::for_test(tempdir.path())
            .await
            .expect("persistence should start");
        let runtime = TaskRuntime::new(persistence.clone(), WorkerRegistry::new());
        let now = crate::time::now_unix_seconds();
        persistence
            .insert_run(&WorkflowRunRecord {
                run_id: "run-cancel-before-complete-cas".to_owned(),
                workflow_id: "empty-flow".to_owned(),
                workflow_version: 1,
                status: RUN_STATUS_RUNNING.to_owned(),
                cancel_reason: None,
                cancel_requested_by: None,
                cancel_requested_at: None,
                created_at: now,
            })
            .await
            .expect("run should insert");

        runtime
            .cancel_run(
                "run-cancel-before-complete-cas",
                Some("race_winner"),
                Some("test"),
            )
            .await
            .expect("cancellation should commit first");
        runtime
            .reconcile_run_status("run-cancel-before-complete-cas", now + 1)
            .await
            .expect("losing completion CAS should be harmless");

        assert_eq!(
            persistence
                .get_run("run-cancel-before-complete-cas")
                .await
                .expect("run should load")
                .expect("run should exist")
                .status,
            "cancelled"
        );
        let transitions = persistence
            .list_transitions("run-cancel-before-complete-cas")
            .await
            .expect("transitions should load");
        assert_eq!(
            transitions
                .iter()
                .filter(|transition| transition.transition_type == "run_completed")
                .count(),
            0
        );
    }

    #[tokio::test]
    async fn cancellation_after_continuation_read_suppresses_without_downstream_work() {
        let (_tempdir, persistence, _registry, runtime, lease, now) =
            pending_completion_fixture("run-cancel-after-continuation-read").await;
        persistence
            .complete_task_with_continuation(
                &lease,
                &TaskOutput {
                    payload: json!({ "order_id": "ord-cancel" }),
                },
                now + 1,
            )
            .await
            .expect("completion should commit");
        let continuation = persistence
            .get_pending_completed_task_continuation(&lease.task_id)
            .await
            .expect("continuation should load")
            .expect("continuation should be pending");

        runtime
            .cancel_run(
                "run-cancel-after-continuation-read",
                Some("cancel_after_read"),
                Some("test"),
            )
            .await
            .expect("cancellation should commit");
        runtime
            .apply_task_continuation(continuation, now + 2)
            .await
            .expect("suppressed continuation replay should be harmless");

        let steps = persistence
            .list_step_instances("run-cancel-after-continuation-read")
            .await
            .expect("steps should load");
        assert_eq!(steps.len(), 1);
        assert_eq!(
            persistence
                .get_run("run-cancel-after-continuation-read")
                .await
                .expect("run should load")
                .expect("run should exist")
                .status,
            "cancelled"
        );
        assert!(persistence
            .list_pending_completed_task_continuations(now + 2)
            .await
            .expect("continuations should load")
            .is_empty());
        let transitions = persistence
            .list_transitions("run-cancel-after-continuation-read")
            .await
            .expect("transitions should load");
        assert_eq!(
            transitions
                .iter()
                .filter(|transition| transition.transition_type == "run_cancelled")
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn recovery_returns_invariant_error_for_malformed_retry_timer_payload() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let persistence = HiqlitePersistence::for_test(tempdir.path())
            .await
            .expect("persistence should start");
        let runtime = TaskRuntime::new(persistence.clone(), WorkerRegistry::new());
        let now = 1_700_000_000;

        persistence
            .insert_timer(&TimerRecord {
                timer_id: "timer-1".to_owned(),
                run_id: "run-1".to_owned(),
                step_id: "step-1".to_owned(),
                timer_kind: TIMER_KIND_RETRY.to_owned(),
                due_at: now,
                status: TIMER_STATUS_PENDING.to_owned(),
                effect_state: Some(TIMER_EFFECT_STATE_PENDING.to_owned()),
                payload: Some(json!({})),
                created_at: now,
                updated_at: now,
            })
            .await
            .expect("timer should insert");

        let error = runtime
            .recovery_scan_at(now)
            .await
            .expect_err("malformed retry timer should return an invariant error");

        assert!(matches!(
            error,
            TaskRuntimeError::Persistence(crate::persistence::PersistenceError::Invariant(ref message))
                if message == "retry timer payload missing task_id"
        ));
    }

    #[tokio::test]
    async fn recovery_repairs_fired_sleep_timer_missing_downstream_work() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let persistence = HiqlitePersistence::for_test(tempdir.path())
            .await
            .expect("persistence should start");
        let registry = WorkerRegistry::new();
        registry
            .define_handler("notifications.send")
            .capability(Capability::Task)
            .default_queue("default");
        let runtime = TaskRuntime::new(persistence.clone(), registry);
        let now = 1_700_000_000;

        insert_snapshotted_definition(
            &persistence,
            &StoredWorkflowDefinition {
                workflow_id: "sleep-flow".to_owned(),
                workflow_version: 1,
                definition: sleep_definition(),
                content_hash: "sleep-flow-v1".to_owned(),
                created_at: now,
                published_at: Some(now),
            },
        )
        .await;
        persistence
            .insert_run(&WorkflowRunRecord {
                run_id: "run-sleep".to_owned(),
                workflow_id: "sleep-flow".to_owned(),
                workflow_version: 1,
                status: RUN_STATUS_RUNNING.to_owned(),
                cancel_reason: None,
                cancel_requested_by: None,
                cancel_requested_at: None,
                created_at: now,
            })
            .await
            .expect("run should insert");
        persistence
            .insert_step_instance(&StepInstanceRecord {
                step_instance_id: "step-sleep".to_owned(),
                run_id: "run-sleep".to_owned(),
                step_id: "sleep_before_notify".to_owned(),
                status: STEP_STATUS_SLEEPING.to_owned(),
                attempt_count: 0,
                input: Some(json!({ "order_id": "ord-123" })),
                output: None,
                error: None,
                created_at: now,
                updated_at: now,
            })
            .await
            .expect("sleep step should insert");
        persistence
            .insert_timer(&TimerRecord {
                timer_id: "timer-sleep".to_owned(),
                run_id: "run-sleep".to_owned(),
                step_id: "sleep_before_notify".to_owned(),
                timer_kind: TIMER_KIND_SLEEP.to_owned(),
                due_at: now,
                status: TIMER_STATUS_FIRED.to_owned(),
                effect_state: Some(TIMER_EFFECT_STATE_PENDING.to_owned()),
                payload: Some(json!({ "input": { "order_id": "ord-123" } })),
                created_at: now,
                updated_at: now,
            })
            .await
            .expect("fired timer should insert");

        runtime
            .reconcile_run_status("run-sleep", now + 1)
            .await
            .expect("fired timer with a pending effect should keep the run open");
        assert_eq!(
            persistence
                .get_run("run-sleep")
                .await
                .expect("run should load")
                .expect("run should exist")
                .status,
            RUN_STATUS_RUNNING
        );

        runtime
            .recovery_scan_at(now + 30)
            .await
            .expect("recovery should repair fired sleep timer");

        let steps = persistence
            .list_step_instances("run-sleep")
            .await
            .expect("steps should load");
        assert_eq!(steps.len(), 2);
        assert_eq!(steps[0].status, "completed");
        assert_eq!(steps[1].step_id, "send_notification");
        assert_eq!(steps[1].status, "queued");
    }

    async fn pending_completion_fixture(
        run_id: &str,
    ) -> (
        TempDir,
        HiqlitePersistence,
        WorkerRegistry,
        TaskRuntime,
        TaskMutationContext,
        i64,
    ) {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let persistence = HiqlitePersistence::for_test(tempdir.path())
            .await
            .expect("persistence should start");
        let registry = WorkerRegistry::new();
        for handler in ["continuation.prepare", "continuation.finish"] {
            registry
                .define_handler(handler)
                .capability(Capability::Task)
                .default_queue("continuation");
        }
        let runtime = TaskRuntime::new(persistence.clone(), registry.clone());
        let now = crate::time::now_unix_seconds();
        let definition = continuation_definition();
        insert_snapshotted_definition(
            &persistence,
            &StoredWorkflowDefinition {
                workflow_id: definition.workflow_id.clone(),
                workflow_version: definition.workflow_version,
                definition: definition.clone(),
                content_hash: "continuation-flow-v1".to_owned(),
                created_at: now,
                published_at: Some(now),
            },
        )
        .await;
        persistence
            .insert_run(&WorkflowRunRecord {
                run_id: run_id.to_owned(),
                workflow_id: definition.workflow_id.clone(),
                workflow_version: definition.workflow_version,
                status: RUN_STATUS_RUNNING.to_owned(),
                cancel_reason: None,
                cancel_requested_by: None,
                cancel_requested_at: None,
                created_at: now,
            })
            .await
            .expect("run should insert");
        let stored_definition = runtime
            .load_current_execution_definition(&definition.workflow_id, definition.workflow_version)
            .await
            .expect("snapshotted definition should load");
        runtime
            .schedule_root_steps(
                run_id,
                &stored_definition,
                json!({ "order_id": "ord-123" }),
                now,
            )
            .await
            .expect("root should schedule");

        let assigned = runtime
            .poll_task(PollTask {
                worker_id: "continuation-worker".to_owned(),
                worker_name: "continuation-worker".to_owned(),
                queues: vec!["continuation".to_owned()],
                capabilities: vec![Capability::Task.as_str().to_owned()],
                max_inflight: 1,
            })
            .await
            .expect("root should poll");
        let assigned = match assigned {
            PollTaskResponse::Assigned(task) => *task,
            PollTaskResponse::NoTaskAvailable => panic!("expected root task"),
        };
        let lease = TaskMutationContext {
            task_id: assigned.task_id,
            attempt_id: assigned.attempt_id,
            worker_id: "continuation-worker".to_owned(),
            fencing_token: assigned.fencing_token,
        };

        (tempdir, persistence, registry, runtime, lease, now)
    }

    async fn pending_sleep_fixture(
        run_id: &str,
    ) -> (TempDir, HiqlitePersistence, TaskRuntime, i64) {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let persistence = HiqlitePersistence::for_test(tempdir.path())
            .await
            .expect("persistence should start");
        let registry = WorkerRegistry::new();
        registry
            .define_handler("notifications.send")
            .capability(Capability::Task)
            .default_queue("default");
        let runtime = TaskRuntime::new(persistence.clone(), registry);
        let now = crate::time::now_unix_seconds();
        insert_snapshotted_definition(
            &persistence,
            &StoredWorkflowDefinition {
                workflow_id: "sleep-flow".to_owned(),
                workflow_version: 1,
                definition: sleep_definition(),
                content_hash: "sleep-flow-v1".to_owned(),
                created_at: now,
                published_at: Some(now),
            },
        )
        .await;
        persistence
            .insert_run(&WorkflowRunRecord {
                run_id: run_id.to_owned(),
                workflow_id: "sleep-flow".to_owned(),
                workflow_version: 1,
                status: RUN_STATUS_RUNNING.to_owned(),
                cancel_reason: None,
                cancel_requested_by: None,
                cancel_requested_at: None,
                created_at: now,
            })
            .await
            .expect("run should insert");
        persistence
            .insert_step_instance(&StepInstanceRecord {
                step_instance_id: format!("step-sleep-{run_id}"),
                run_id: run_id.to_owned(),
                step_id: "sleep_before_notify".to_owned(),
                status: STEP_STATUS_SLEEPING.to_owned(),
                attempt_count: 0,
                input: Some(json!({ "order_id": "ord-cancel" })),
                output: None,
                error: None,
                created_at: now,
                updated_at: now,
            })
            .await
            .expect("sleep step should insert");
        persistence
            .insert_timer(&TimerRecord {
                timer_id: format!("timer-sleep-{run_id}"),
                run_id: run_id.to_owned(),
                step_id: "sleep_before_notify".to_owned(),
                timer_kind: TIMER_KIND_SLEEP.to_owned(),
                due_at: now,
                status: TIMER_STATUS_PENDING.to_owned(),
                effect_state: Some(TIMER_EFFECT_STATE_PENDING.to_owned()),
                payload: Some(json!({ "input": { "order_id": "ord-cancel" } })),
                created_at: now,
                updated_at: now,
            })
            .await
            .expect("sleep timer should insert");
        (tempdir, persistence, runtime, now)
    }

    async fn assert_continuation_scheduled_child_once(
        persistence: &HiqlitePersistence,
        runtime: &TaskRuntime,
        completed_task_id: &str,
    ) {
        let steps = persistence
            .list_step_instances(
                &persistence
                    .get_task_snapshot(completed_task_id)
                    .await
                    .expect("task should load")
                    .expect("completed task should exist")
                    .run_id,
            )
            .await
            .expect("steps should load");
        assert_eq!(steps.len(), 2);
        assert_eq!(
            steps.iter().filter(|step| step.step_id == "finish").count(),
            1
        );
        assert!(persistence
            .list_pending_completed_task_continuations(0)
            .await
            .expect("continuations should load")
            .is_empty());

        let run_id = &steps[0].run_id;
        let transitions = persistence
            .list_transitions(run_id)
            .await
            .expect("transitions should load");
        assert_eq!(
            transitions
                .iter()
                .filter(|transition| {
                    transition.transition_type == "task_queued"
                        && transition
                            .payload
                            .as_ref()
                            .and_then(|payload| payload.get("step_id"))
                            .and_then(|step_id| step_id.as_str())
                            == Some("finish")
                })
                .count(),
            1
        );

        let child = runtime
            .poll_task(PollTask {
                worker_id: "child-worker".to_owned(),
                worker_name: "child-worker".to_owned(),
                queues: vec!["continuation".to_owned()],
                capabilities: vec![Capability::Task.as_str().to_owned()],
                max_inflight: 1,
            })
            .await
            .expect("child should poll");
        assert!(matches!(child, PollTaskResponse::Assigned(_)));
        let duplicate = runtime
            .poll_task(PollTask {
                worker_id: "duplicate-worker".to_owned(),
                worker_name: "duplicate-worker".to_owned(),
                queues: vec!["continuation".to_owned()],
                capabilities: vec![Capability::Task.as_str().to_owned()],
                max_inflight: 1,
            })
            .await
            .expect("duplicate poll should succeed");
        assert!(matches!(duplicate, PollTaskResponse::NoTaskAvailable));
    }

    fn continuation_definition() -> FlowDefinition {
        FlowDefinition {
            workflow_id: "continuation-flow".to_owned(),
            workflow_version: 1,
            defaults: WorkflowDefaults::default(),
            trigger: Some(Trigger::Event(EventTrigger {
                event_type: "continuation.ready".to_owned(),
            })),
            steps: vec![
                StepDefinition {
                    id: "prepare".to_owned(),
                    kind: StepKind::Task,
                    metadata: StepMetadata {
                        task: Some(TaskMetadata {
                            handler: "continuation.prepare".to_owned(),
                            capability: None,
                            queue: None,
                            timeout_seconds: Some(60),
                        }),
                        ..StepMetadata::default()
                    },
                },
                StepDefinition {
                    id: "finish".to_owned(),
                    kind: StepKind::Task,
                    metadata: StepMetadata {
                        task: Some(TaskMetadata {
                            handler: "continuation.finish".to_owned(),
                            capability: None,
                            queue: None,
                            timeout_seconds: None,
                        }),
                        ..StepMetadata::default()
                    },
                },
            ],
            edges: vec![Edge {
                from: "prepare".to_owned(),
                to: "finish".to_owned(),
            }],
        }
    }

    fn sleep_definition() -> FlowDefinition {
        FlowDefinition {
            workflow_id: "sleep-flow".to_owned(),
            workflow_version: 1,
            defaults: WorkflowDefaults::default(),
            trigger: Some(Trigger::Event(EventTrigger {
                event_type: "reminder.ready".to_owned(),
            })),
            steps: vec![
                StepDefinition {
                    id: "sleep_before_notify".to_owned(),
                    kind: StepKind::Sleep,
                    metadata: StepMetadata {
                        sleep: Some(SleepMetadata { seconds: 30 }),
                        ..StepMetadata::default()
                    },
                },
                StepDefinition {
                    id: "send_notification".to_owned(),
                    kind: StepKind::Task,
                    metadata: StepMetadata {
                        task: Some(TaskMetadata {
                            handler: "notifications.send".to_owned(),
                            capability: None,
                            queue: None,
                            timeout_seconds: None,
                        }),
                        ..StepMetadata::default()
                    },
                },
            ],
            edges: vec![Edge {
                from: "sleep_before_notify".to_owned(),
                to: "send_notification".to_owned(),
            }],
        }
    }

    async fn insert_snapshotted_definition(
        persistence: &HiqlitePersistence,
        record: &StoredWorkflowDefinition,
    ) {
        let task_routes = record
            .definition
            .steps
            .iter()
            .filter_map(|step| {
                step.metadata.task.as_ref().map(|task| {
                    let queue = if task.handler.starts_with("continuation.") {
                        "continuation"
                    } else {
                        "default"
                    };
                    (
                        step.id.clone(),
                        ResolvedTaskRoute::new(
                            task.handler.clone(),
                            "task",
                            queue,
                            task.timeout_seconds,
                        )
                        .expect("route should be valid"),
                    )
                })
            })
            .collect::<BTreeMap<_, _>>();
        let metadata =
            PublicationExecutionMetadata::v1(task_routes).expect("metadata should be valid");
        let metadata_json = serde_json::to_string(&metadata).expect("metadata should serialize");
        persistence
            .insert_snapshotted_published_definition(record, &metadata_json)
            .await
            .expect("snapshotted definition should insert");
    }
}
