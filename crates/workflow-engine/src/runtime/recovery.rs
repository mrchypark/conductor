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
                        .expect("retry timer payload should include task id");
                    let task = self
                        .persistence
                        .get_task_snapshot(task_id)
                        .await?
                        .expect("retry timer task should exist");
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
                        .expect("wait timeout timer payload should include waiter id");
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
