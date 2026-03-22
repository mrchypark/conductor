use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use workflow_definition::Trigger;

use super::{
    tasks::{
        next_runtime_id, now_unix_seconds, RUN_STATUS_RUNNING, STEP_STATUS_COMPLETED,
        TIMER_KIND_WAIT_TIMEOUT, WAITER_STATUS_RESUMED, WAITER_STATUS_WAITING,
    },
    StartedRun, TaskRuntime, TaskRuntimeError,
};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EventEnvelope {
    pub id: Option<String>,
    pub name: String,
    pub idempotency_key: Option<String>,
    pub data: Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EventIngestResult {
    pub accepted: bool,
    pub event_id: String,
    pub deduplicated: bool,
    pub started_runs: Vec<StartedRun>,
}

impl TaskRuntime {
    pub async fn ingest_event(
        &self,
        event_type: &str,
        payload: Value,
    ) -> Result<Vec<StartedRun>, TaskRuntimeError> {
        let results = self
            .ingest_events(vec![EventEnvelope {
                id: None,
                name: event_type.to_owned(),
                idempotency_key: None,
                data: payload,
            }])
            .await?;

        Ok(results
            .into_iter()
            .flat_map(|result| result.started_runs)
            .collect())
    }

    pub async fn ingest_events(
        &self,
        events: Vec<EventEnvelope>,
    ) -> Result<Vec<EventIngestResult>, TaskRuntimeError> {
        let mut results = Vec::with_capacity(events.len());

        for event in events {
            let now = now_unix_seconds();
            let event_id = event.id.clone().unwrap_or_else(|| next_runtime_id("event"));
            let dedupe_key = event
                .idempotency_key
                .clone()
                .or_else(|| event.id.clone())
                .filter(|value| !value.is_empty());

            if let Some(existing) = self
                .persistence
                .find_event_by_dedupe_key(dedupe_key.as_deref())
                .await?
            {
                if existing.processing_state == "applied" {
                    results.push(EventIngestResult {
                        accepted: true,
                        event_id: existing.event_id,
                        deduplicated: true,
                        started_runs: Vec::new(),
                    });
                    continue;
                }
            }

            self.persistence
                .insert_event_received(
                    &event_id,
                    &event.name,
                    &event.data,
                    dedupe_key.as_deref(),
                    now,
                )
                .await?;

            self.record_transition(
                "event-inbox",
                "event_received",
                Some(json!({
                    "event_id": event_id,
                    "event_type": event.name,
                })),
                now,
            )
            .await?;

            let started_runs = self
                .apply_event(&event_id, &event.name, &event.data, now)
                .await?;
            self.persistence.mark_event_applied(&event_id, now).await?;

            results.push(EventIngestResult {
                accepted: true,
                event_id,
                deduplicated: false,
                started_runs,
            });
        }

        Ok(results)
    }

    pub(crate) async fn apply_event(
        &self,
        event_id: &str,
        event_type: &str,
        payload: &Value,
        now: i64,
    ) -> Result<Vec<StartedRun>, TaskRuntimeError> {
        let published_definitions = self.persistence.list_all_published_definitions().await?;
        let mut started_runs = Vec::new();

        for definition in
            published_definitions
                .into_iter()
                .filter(|stored| match &stored.definition.trigger {
                    Some(Trigger::Event(trigger)) => trigger.event_type == event_type,
                    None => false,
                })
        {
            let run_id = next_runtime_id("run");
            let run = crate::definition_store::WorkflowRunRecord {
                run_id: run_id.clone(),
                workflow_id: definition.workflow_id.clone(),
                workflow_version: definition.workflow_version,
                status: RUN_STATUS_RUNNING.to_owned(),
                cancel_reason: None,
                cancel_requested_by: None,
                cancel_requested_at: None,
                created_at: now,
            };
            self.persistence.insert_run(&run).await?;
            self.record_transition(
                &run_id,
                "run_started",
                Some(json!({
                    "event_id": event_id,
                    "workflow_id": definition.workflow_id,
                })),
                now,
            )
            .await?;
            self.schedule_root_steps(&run_id, &definition.definition, payload.clone(), now)
                .await?;
            self.reconcile_run_status(&run_id, now).await?;

            started_runs.push(StartedRun {
                run_id,
                workflow_id: definition.workflow_id,
                workflow_version: definition.workflow_version,
            });
        }

        let matching_waiters = self
            .persistence
            .list_matching_waiters(event_type, WAITER_STATUS_WAITING)
            .await?;
        for waiter in matching_waiters {
            if self
                .persistence
                .mark_waiter_status(
                    &waiter.waiter_id,
                    WAITER_STATUS_RESUMED,
                    Some(event_id),
                    now,
                )
                .await?
                == super::MutationApplyResult::Applied
            {
                self.persistence
                    .cancel_timers_for_step(
                        &waiter.run_id,
                        &waiter.step_id,
                        TIMER_KIND_WAIT_TIMEOUT,
                        now,
                    )
                    .await?;
                self.persistence
                    .update_step_instance(
                        &waiter.run_id,
                        &waiter.step_id,
                        STEP_STATUS_COMPLETED,
                        None,
                        Some(payload),
                        None,
                        now,
                    )
                    .await?;
                let run = self
                    .persistence
                    .get_run(&waiter.run_id)
                    .await?
                    .ok_or_else(|| TaskRuntimeError::RunNotFound {
                        run_id: waiter.run_id.clone(),
                    })?;
                let definition = self
                    .load_published_definition(&run.workflow_id, run.workflow_version)
                    .await?;
                self.record_transition(
                    &waiter.run_id,
                    "waiter_resumed",
                    Some(json!({
                        "waiter_id": waiter.waiter_id,
                        "step_id": waiter.step_id,
                        "event_id": event_id,
                    })),
                    now,
                )
                .await?;
                self.schedule_downstream_steps(
                    &waiter.run_id,
                    &definition,
                    &waiter.step_id,
                    payload.clone(),
                    now,
                )
                .await?;
                self.reconcile_run_status(&waiter.run_id, now).await?;
            }
        }

        Ok(started_runs)
    }
}
