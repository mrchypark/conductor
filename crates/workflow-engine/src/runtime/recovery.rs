use serde_json::json;

use super::{
    tasks::{
        RUN_STATUS_FAILED, STEP_STATUS_COMPLETED, STEP_STATUS_QUEUED, STEP_STATUS_TIMED_OUT,
        TIMER_KIND_RETRY, TIMER_KIND_SLEEP, TIMER_KIND_WAIT_TIMEOUT, TIMER_STATUS_FIRED,
        WAITER_STATUS_TIMED_OUT,
    },
    TaskRuntime, TaskRuntimeError,
};

impl TaskRuntime {
    pub async fn recovery_scan_at(&self, now: i64) -> Result<(), TaskRuntimeError> {
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

        let due_timers = self.persistence.list_due_timers(now).await?;
        for timer in due_timers {
            if self
                .persistence
                .mark_timer_status(&timer.timer_id, TIMER_STATUS_FIRED, now)
                .await?
                != super::MutationApplyResult::Applied
            {
                continue;
            }

            self.record_transition(
                &timer.run_id,
                "timer_fired",
                Some(json!({
                    "timer_id": timer.timer_id,
                    "timer_kind": timer.timer_kind,
                    "step_id": timer.step_id,
                })),
                now,
            )
            .await?;

            match timer.timer_kind.as_str() {
                TIMER_KIND_RETRY => {
                    let task_id = timer
                        .payload
                        .as_ref()
                        .and_then(|value| value.get("task_id"))
                        .and_then(|value| value.as_str())
                        .ok_or_else(|| {
                            TaskRuntimeError::Persistence(
                                crate::persistence::PersistenceError::Invariant(
                                    "retry timer payload missing task_id".to_owned(),
                                ),
                            )
                        })?;
                    let task = self
                        .persistence
                        .get_task_snapshot(task_id)
                        .await?
                        .ok_or_else(|| {
                            TaskRuntimeError::Persistence(
                                crate::persistence::PersistenceError::Invariant(format!(
                                    "retry timer references unknown task '{task_id}'"
                                )),
                            )
                        })?;
                    self.persistence.requeue_task(&task.task_id, now).await?;
                    self.persistence
                        .update_step_instance(
                            &task.run_id,
                            &task.step_id,
                            STEP_STATUS_QUEUED,
                            None,
                            None,
                            None,
                            now,
                        )
                        .await?;
                    self.record_transition(
                        &task.run_id,
                        "task_requeued",
                        Some(json!({
                            "task_id": task.task_id,
                            "step_id": task.step_id,
                        })),
                        now,
                    )
                    .await?;
                }
                TIMER_KIND_SLEEP => {
                    let run = self
                        .persistence
                        .get_run(&timer.run_id)
                        .await?
                        .ok_or_else(|| TaskRuntimeError::RunNotFound {
                            run_id: timer.run_id.clone(),
                        })?;
                    let definition = self
                        .load_published_definition(&run.workflow_id, run.workflow_version)
                        .await?;
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
                        .or(step.input.clone())
                        .unwrap_or_else(|| json!({}));
                    self.persistence
                        .update_step_instance(
                            &timer.run_id,
                            &timer.step_id,
                            STEP_STATUS_COMPLETED,
                            None,
                            Some(&output),
                            None,
                            now,
                        )
                        .await?;
                    self.schedule_downstream_steps(
                        &timer.run_id,
                        &definition,
                        &timer.step_id,
                        output,
                        now,
                    )
                    .await?;
                }
                TIMER_KIND_WAIT_TIMEOUT => {
                    let waiter_id = timer
                        .payload
                        .as_ref()
                        .and_then(|value| value.get("waiter_id"))
                        .and_then(|value| value.as_str())
                        .ok_or_else(|| {
                            TaskRuntimeError::Persistence(
                                crate::persistence::PersistenceError::Invariant(
                                    "wait timeout timer payload missing waiter_id".to_owned(),
                                ),
                            )
                        })?;
                    if self
                        .persistence
                        .mark_waiter_status(waiter_id, WAITER_STATUS_TIMED_OUT, None, now)
                        .await?
                        == super::MutationApplyResult::Applied
                    {
                        let waiter =
                            self.persistence
                                .get_waiter(waiter_id)
                                .await?
                                .ok_or_else(|| TaskRuntimeError::WaiterNotFound {
                                    waiter_id: waiter_id.to_owned(),
                                })?;
                        self.persistence
                            .update_step_instance(
                                &waiter.run_id,
                                &waiter.step_id,
                                STEP_STATUS_TIMED_OUT,
                                None,
                                None,
                                Some(&json!({ "reason": "wait_timeout" })),
                                now,
                            )
                            .await?;
                        self.persistence
                            .update_run_status(&waiter.run_id, RUN_STATUS_FAILED, now)
                            .await?;
                        self.record_transition(
                            &waiter.run_id,
                            "waiter_timed_out",
                            Some(json!({
                                "waiter_id": waiter.waiter_id,
                                "step_id": waiter.step_id,
                            })),
                            now,
                        )
                        .await?;
                        self.record_transition(&waiter.run_id, "run_failed", None, now)
                            .await?;
                    }
                }
                _ => {}
            }
        }

        let pending_events = self.persistence.list_received_events().await?;
        for event in pending_events {
            self.apply_event(&event.event_id, &event.event_type, &event.payload, now)
                .await?;
            self.persistence
                .mark_event_applied(&event.event_id, now)
                .await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use tempfile::TempDir;
    use workflow_worker::WorkerRegistry;

    use crate::{
        persistence::HiqlitePersistence,
        runtime::{
            tasks::{TimerRecord, TIMER_KIND_RETRY, TIMER_STATUS_PENDING},
            TaskRuntime, TaskRuntimeError,
        },
    };

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
}
