use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use thiserror::Error;
use uuid::Uuid;
use workflow_definition::{FlowDefinition, RetryPolicy, StepDefinition, StepKind};
use workflow_protocol::{
    AssignedTask, CompleteTask, FailTask, HeartbeatLease, PollTask, PollTaskResponse,
};
use workflow_worker::WorkerRegistry;

use super::EventEnvelope;
use crate::persistence::{HiqlitePersistence, PersistenceError};
use crate::time::now_unix_seconds;

pub(crate) const LEASE_TTL_SECONDS: i64 = 30;
pub(crate) const RUN_STATUS_RUNNING: &str = "running";
pub(crate) const RUN_STATUS_COMPLETED: &str = "completed";
pub(crate) const RUN_STATUS_FAILED: &str = "failed";
pub(crate) const RUN_STATUS_CANCEL_REQUESTED: &str = "cancel_requested";
pub(crate) const RUN_STATUS_CANCELLED: &str = "cancelled";

pub(crate) const STEP_STATUS_QUEUED: &str = "queued";
pub(crate) const STEP_STATUS_WAITING: &str = "waiting";
pub(crate) const STEP_STATUS_SLEEPING: &str = "sleeping";
pub(crate) const STEP_STATUS_COMPLETED: &str = "completed";
pub(crate) const STEP_STATUS_TIMED_OUT: &str = "timed_out";
pub(crate) const STEP_STATUS_RETRY_PENDING: &str = "retry_pending";

pub(crate) const TIMER_STATUS_PENDING: &str = "pending";
pub(crate) const TIMER_STATUS_FIRED: &str = "fired";

pub(crate) const WAITER_STATUS_WAITING: &str = "waiting";
pub(crate) const WAITER_STATUS_RESUMED: &str = "resumed";
pub(crate) const WAITER_STATUS_TIMED_OUT: &str = "timed_out";

pub(crate) const TIMER_KIND_SLEEP: &str = "sleep";
pub(crate) const TIMER_KIND_RETRY: &str = "retry";
pub(crate) const TIMER_KIND_WAIT_TIMEOUT: &str = "wait_timeout";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StartedRun {
    pub run_id: String,
    pub workflow_id: String,
    pub workflow_version: u32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StepInstanceRecord {
    pub step_instance_id: String,
    pub run_id: String,
    pub step_id: String,
    pub status: String,
    pub attempt_count: u32,
    pub input: Option<Value>,
    pub output: Option<Value>,
    pub error: Option<Value>,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WaiterRecord {
    pub waiter_id: String,
    pub run_id: String,
    pub step_id: String,
    pub event_type: String,
    pub status: String,
    pub timeout_at: Option<i64>,
    pub resume_event_id: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TransitionRecord {
    pub transition_id: String,
    pub run_id: String,
    pub transition_type: String,
    pub payload: Option<Value>,
    pub created_at: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QueuedTaskRecord {
    pub task_id: String,
    pub run_id: String,
    pub workflow_id: String,
    pub workflow_version: u32,
    pub step_id: String,
    pub step_kind: String,
    pub handler_id: String,
    pub capability: String,
    pub queue_name: String,
    pub input: Value,
    pub attempt_sequence: u32,
    pub created_at: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TaskLeaseRecord {
    pub task_id: String,
    pub run_id: String,
    pub workflow_id: String,
    pub workflow_version: u32,
    pub step_id: String,
    pub step_kind: String,
    pub handler_id: String,
    pub input: Value,
    pub attempt_id: String,
    pub fencing_token: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct TaskSnapshot {
    pub task_id: String,
    pub run_id: String,
    pub workflow_id: String,
    pub workflow_version: u32,
    pub step_id: String,
    pub step_kind: String,
    pub handler_id: String,
    pub capability: String,
    pub queue_name: String,
    pub input: Value,
    pub status: String,
    pub attempt_sequence: u32,
    pub active_attempt_id: Option<String>,
    pub active_fencing_token: Option<u64>,
    pub leased_by_worker_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct TimerRecord {
    pub timer_id: String,
    pub run_id: String,
    pub step_id: String,
    pub timer_kind: String,
    pub due_at: i64,
    pub status: String,
    pub payload: Option<Value>,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MutationApplyResult {
    Applied,
    AlreadyApplied,
    Stale,
}

#[derive(Debug, Error)]
pub enum TaskRuntimeError {
    #[error("published workflow references handler '{handler_id}' without worker metadata")]
    MissingHandlerMetadata { handler_id: String },
    #[error("task lease for task '{task_id}' is stale")]
    StaleLease { task_id: String },
    #[error("no published workflow version '{workflow_id}:{workflow_version}' exists")]
    PublishedWorkflowVersionNotFound {
        workflow_id: String,
        workflow_version: u32,
    },
    #[error("run '{run_id}' does not exist")]
    RunNotFound { run_id: String },
    #[error("step '{step_id}' with kind '{kind:?}' is not supported by the MVP runtime path")]
    UnsupportedStepKind { step_id: String, kind: StepKind },
    #[error("step instance '{run_id}/{step_id}' does not exist")]
    StepInstanceNotFound { run_id: String, step_id: String },
    #[error("waiter '{waiter_id}' does not exist")]
    WaiterNotFound { waiter_id: String },
    #[error("branch expression '{expression}' is not supported by the MVP runtime evaluator")]
    UnsupportedBranchExpression { expression: String },
    #[error(transparent)]
    Persistence(#[from] PersistenceError),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}

#[derive(Clone)]
pub struct TaskRuntime {
    pub(crate) persistence: HiqlitePersistence,
    pub(crate) registry: WorkerRegistry,
}

impl TaskRuntime {
    pub fn new(persistence: HiqlitePersistence, registry: WorkerRegistry) -> Self {
        Self {
            persistence,
            registry,
        }
    }

    pub async fn poll_task(&self, request: PollTask) -> Result<PollTaskResponse, TaskRuntimeError> {
        let leased = self
            .persistence
            .lease_next_task(
                &request,
                now_unix_seconds() + LEASE_TTL_SECONDS,
                now_unix_seconds(),
            )
            .await?;

        if let Some(lease) = leased {
            self.record_transition(
                &lease.run_id,
                "task_leased",
                Some(json!({
                    "task_id": lease.task_id,
                    "step_id": lease.step_id,
                    "attempt_id": lease.attempt_id,
                    "fencing_token": lease.fencing_token,
                })),
                now_unix_seconds(),
            )
            .await?;

            Ok(PollTaskResponse::Assigned(Box::new(AssignedTask {
                task_id: lease.task_id,
                run_id: lease.run_id,
                attempt_id: lease.attempt_id,
                fencing_token: lease.fencing_token,
                workflow_id: lease.workflow_id,
                workflow_version: lease.workflow_version,
                step_id: lease.step_id,
                step_kind: lease.step_kind,
                handler_id: lease.handler_id,
                input: lease.input,
            })))
        } else {
            Ok(PollTaskResponse::NoTaskAvailable)
        }
    }

    pub async fn heartbeat_lease(&self, request: HeartbeatLease) -> Result<(), TaskRuntimeError> {
        match self
            .persistence
            .heartbeat_lease(
                &request.lease,
                now_unix_seconds() + LEASE_TTL_SECONDS,
                now_unix_seconds(),
            )
            .await?
        {
            MutationApplyResult::Applied | MutationApplyResult::AlreadyApplied => Ok(()),
            MutationApplyResult::Stale => Err(TaskRuntimeError::StaleLease {
                task_id: request.lease.task_id,
            }),
        }
    }

    pub async fn complete_task(&self, request: CompleteTask) -> Result<(), TaskRuntimeError> {
        let now = now_unix_seconds();
        let snapshot = self
            .persistence
            .get_task_snapshot(&request.lease.task_id)
            .await?
            .ok_or_else(|| TaskRuntimeError::StaleLease {
                task_id: request.lease.task_id.clone(),
            })?;

        match self
            .persistence
            .complete_task(&request.lease, &request.result, now)
            .await?
        {
            MutationApplyResult::Applied => {
                self.record_transition(
                    &snapshot.run_id,
                    "task_completed",
                    Some(json!({
                        "task_id": snapshot.task_id,
                        "step_id": snapshot.step_id,
                    })),
                    now,
                )
                .await?;

                let run = self
                    .persistence
                    .get_run(&snapshot.run_id)
                    .await?
                    .ok_or_else(|| TaskRuntimeError::RunNotFound {
                        run_id: snapshot.run_id.clone(),
                    })?;
                if run.status == RUN_STATUS_RUNNING {
                    let definition = self
                        .load_published_definition(&snapshot.workflow_id, snapshot.workflow_version)
                        .await?;
                    self.schedule_downstream_steps(
                        &snapshot.run_id,
                        &definition,
                        &snapshot.step_id,
                        request.result.payload.clone(),
                        now,
                    )
                    .await?;
                }
                self.reconcile_run_status(&snapshot.run_id, now).await?;
                Ok(())
            }
            MutationApplyResult::AlreadyApplied => Ok(()),
            MutationApplyResult::Stale => Err(TaskRuntimeError::StaleLease {
                task_id: request.lease.task_id,
            }),
        }
    }

    pub async fn fail_task(&self, request: FailTask) -> Result<(), TaskRuntimeError> {
        let now = now_unix_seconds();
        let snapshot = self
            .persistence
            .get_task_snapshot(&request.lease.task_id)
            .await?
            .ok_or_else(|| TaskRuntimeError::StaleLease {
                task_id: request.lease.task_id.clone(),
            })?;
        let definition = self
            .load_published_definition(&snapshot.workflow_id, snapshot.workflow_version)
            .await?;
        let retry_policy = active_retry_policy(&definition);
        let should_retry = request.failure.retryable
            && retry_policy
                .map(|policy| snapshot.attempt_sequence < policy.max_attempts)
                .unwrap_or(false);

        match self
            .persistence
            .fail_task(
                &request.lease,
                &serde_json::to_value(&request.failure)?,
                now,
            )
            .await?
        {
            MutationApplyResult::Applied => {
                if should_retry {
                    let delay_seconds = retry_delay_seconds(
                        retry_policy.expect("retry policy should exist"),
                        snapshot.attempt_sequence,
                    );
                    let due_at = now + delay_seconds as i64;
                    self.persistence
                        .mark_task_retry_pending(&snapshot.task_id, now)
                        .await?;
                    self.persistence
                        .update_step_instance(
                            &snapshot.run_id,
                            &snapshot.step_id,
                            STEP_STATUS_RETRY_PENDING,
                            Some(snapshot.attempt_sequence),
                            None,
                            Some(&serde_json::to_value(&request.failure)?),
                            now,
                        )
                        .await?;
                    self.persistence
                        .insert_timer(&TimerRecord {
                            timer_id: next_runtime_id("timer"),
                            run_id: snapshot.run_id.clone(),
                            step_id: snapshot.step_id.clone(),
                            timer_kind: TIMER_KIND_RETRY.to_owned(),
                            due_at,
                            status: TIMER_STATUS_PENDING.to_owned(),
                            payload: Some(json!({
                                "task_id": snapshot.task_id,
                            })),
                            created_at: now,
                            updated_at: now,
                        })
                        .await?;
                    self.record_transition(
                        &snapshot.run_id,
                        "retry_timer_scheduled",
                        Some(json!({
                            "task_id": snapshot.task_id,
                            "step_id": snapshot.step_id,
                            "due_at": due_at,
                        })),
                        now,
                    )
                    .await?;
                    self.reconcile_run_status(&snapshot.run_id, now).await?;
                } else {
                    self.record_transition(
                        &snapshot.run_id,
                        "task_failed",
                        Some(json!({
                            "task_id": snapshot.task_id,
                            "step_id": snapshot.step_id,
                            "code": request.failure.code,
                        })),
                        now,
                    )
                    .await?;
                    self.persistence
                        .update_run_status(&snapshot.run_id, RUN_STATUS_FAILED, now)
                        .await?;
                    self.record_transition(&snapshot.run_id, "run_failed", None, now)
                        .await?;
                }
                Ok(())
            }
            MutationApplyResult::AlreadyApplied => Ok(()),
            MutationApplyResult::Stale => Err(TaskRuntimeError::StaleLease {
                task_id: request.lease.task_id,
            }),
        }
    }

    pub async fn list_step_instances(
        &self,
        run_id: &str,
    ) -> Result<Vec<StepInstanceRecord>, TaskRuntimeError> {
        Ok(self.persistence.list_step_instances(run_id).await?)
    }

    pub async fn list_waiters(&self, run_id: &str) -> Result<Vec<WaiterRecord>, TaskRuntimeError> {
        Ok(self.persistence.list_waiters(run_id).await?)
    }

    pub async fn list_transitions(
        &self,
        run_id: &str,
    ) -> Result<Vec<TransitionRecord>, TaskRuntimeError> {
        Ok(self.persistence.list_transitions(run_id).await?)
    }

    pub async fn cancel_run(
        &self,
        run_id: &str,
        reason: Option<&str>,
        requested_by: Option<&str>,
    ) -> Result<(), TaskRuntimeError> {
        let now = now_unix_seconds();
        let run = self.persistence.get_run(run_id).await?.ok_or_else(|| {
            TaskRuntimeError::RunNotFound {
                run_id: run_id.to_owned(),
            }
        })?;

        if matches!(
            run.status.as_str(),
            RUN_STATUS_COMPLETED
                | RUN_STATUS_FAILED
                | RUN_STATUS_CANCELLED
                | RUN_STATUS_CANCEL_REQUESTED
        ) {
            return Ok(());
        }

        let changed = self
            .persistence
            .cancel_run(run_id, reason, requested_by, now)
            .await?;
        if !changed {
            return Ok(());
        }
        self.persistence.cancel_pending_waiters(run_id, now).await?;
        self.persistence.cancel_pending_timers(run_id, now).await?;
        self.record_transition(
            run_id,
            "run_cancel_requested",
            Some(json!({
                "reason": reason,
                "requested_by": requested_by,
            })),
            now,
        )
        .await?;
        self.reconcile_run_status(run_id, now).await
    }

    pub(crate) async fn load_published_definition(
        &self,
        workflow_id: &str,
        workflow_version: u32,
    ) -> Result<FlowDefinition, TaskRuntimeError> {
        self.persistence
            .get_published_definition(workflow_id, workflow_version)
            .await?
            .map(|record| record.definition)
            .ok_or_else(|| TaskRuntimeError::PublishedWorkflowVersionNotFound {
                workflow_id: workflow_id.to_owned(),
                workflow_version,
            })
    }

    pub(crate) async fn schedule_root_steps(
        &self,
        run_id: &str,
        definition: &FlowDefinition,
        input: Value,
        now: i64,
    ) -> Result<(), TaskRuntimeError> {
        for step in root_steps(definition) {
            self.schedule_step(run_id, definition, step, input.clone(), now)
                .await?;
        }
        Ok(())
    }

    pub(crate) async fn schedule_downstream_steps(
        &self,
        run_id: &str,
        definition: &FlowDefinition,
        from_step_id: &str,
        input: Value,
        now: i64,
    ) -> Result<(), TaskRuntimeError> {
        for step in downstream_steps(definition, from_step_id) {
            self.schedule_step(run_id, definition, step, input.clone(), now)
                .await?;
        }
        Ok(())
    }

    pub(crate) async fn schedule_step(
        &self,
        run_id: &str,
        definition: &FlowDefinition,
        step: &StepDefinition,
        input: Value,
        now: i64,
    ) -> Result<(), TaskRuntimeError> {
        match step.kind {
            StepKind::Task => {
                self.schedule_task_step(run_id, definition, step, input, now)
                    .await
            }
            StepKind::WaitForEvent => self.schedule_wait_step(run_id, step, input, now).await,
            StepKind::Sleep => self.schedule_sleep_step(run_id, step, input, now).await,
            StepKind::EmitEvent => {
                self.schedule_emit_event_step(run_id, definition, step, input, now)
                    .await
            }
            StepKind::Branch => {
                self.schedule_branch_step(run_id, definition, step, input, now)
                    .await
            }
        }
    }

    async fn schedule_task_step(
        &self,
        run_id: &str,
        definition: &FlowDefinition,
        step: &StepDefinition,
        input: Value,
        now: i64,
    ) -> Result<(), TaskRuntimeError> {
        let handler_id = step
            .metadata
            .task
            .as_ref()
            .expect("validated task step should include task metadata")
            .handler
            .clone();
        let registration = self.registry.registration(&handler_id).ok_or_else(|| {
            TaskRuntimeError::MissingHandlerMetadata {
                handler_id: handler_id.clone(),
            }
        })?;

        self.persistence
            .insert_step_instance(&StepInstanceRecord {
                step_instance_id: next_runtime_id("step"),
                run_id: run_id.to_owned(),
                step_id: step.id.clone(),
                status: STEP_STATUS_QUEUED.to_owned(),
                attempt_count: 0,
                input: Some(input.clone()),
                output: None,
                error: None,
                created_at: now,
                updated_at: now,
            })
            .await?;

        self.persistence
            .enqueue_task(&QueuedTaskRecord {
                task_id: next_runtime_id("task"),
                run_id: run_id.to_owned(),
                workflow_id: definition.workflow_id.clone(),
                workflow_version: definition.workflow_version,
                step_id: step.id.clone(),
                step_kind: "task".to_owned(),
                handler_id,
                capability: registration.capability.as_str().to_owned(),
                queue_name: registration.default_queue,
                input,
                attempt_sequence: 0,
                created_at: now,
            })
            .await?;

        self.record_transition(
            run_id,
            "task_queued",
            Some(json!({ "step_id": step.id })),
            now,
        )
        .await
    }

    async fn schedule_wait_step(
        &self,
        run_id: &str,
        step: &StepDefinition,
        input: Value,
        now: i64,
    ) -> Result<(), TaskRuntimeError> {
        let metadata = step
            .metadata
            .wait_for_event
            .as_ref()
            .expect("validated wait step should include wait metadata");
        self.persistence
            .insert_step_instance(&StepInstanceRecord {
                step_instance_id: next_runtime_id("step"),
                run_id: run_id.to_owned(),
                step_id: step.id.clone(),
                status: STEP_STATUS_WAITING.to_owned(),
                attempt_count: 0,
                input: Some(input),
                output: None,
                error: None,
                created_at: now,
                updated_at: now,
            })
            .await?;

        let waiter_id = next_runtime_id("waiter");
        let timeout_at = metadata.timeout_seconds.map(|seconds| now + seconds as i64);
        self.persistence
            .insert_waiter(&WaiterRecord {
                waiter_id: waiter_id.clone(),
                run_id: run_id.to_owned(),
                step_id: step.id.clone(),
                event_type: metadata.event_type.clone(),
                status: WAITER_STATUS_WAITING.to_owned(),
                timeout_at,
                resume_event_id: None,
                created_at: now,
                updated_at: now,
            })
            .await?;

        if let Some(timeout_at) = timeout_at {
            self.persistence
                .insert_timer(&TimerRecord {
                    timer_id: next_runtime_id("timer"),
                    run_id: run_id.to_owned(),
                    step_id: step.id.clone(),
                    timer_kind: TIMER_KIND_WAIT_TIMEOUT.to_owned(),
                    due_at: timeout_at,
                    status: TIMER_STATUS_PENDING.to_owned(),
                    payload: Some(json!({ "waiter_id": waiter_id })),
                    created_at: now,
                    updated_at: now,
                })
                .await?;
        }

        self.record_transition(
            run_id,
            "waiter_registered",
            Some(json!({
                "step_id": step.id,
                "event_type": metadata.event_type,
            })),
            now,
        )
        .await
    }

    async fn schedule_sleep_step(
        &self,
        run_id: &str,
        step: &StepDefinition,
        input: Value,
        now: i64,
    ) -> Result<(), TaskRuntimeError> {
        let sleep = step
            .metadata
            .sleep
            .as_ref()
            .expect("validated sleep step should include sleep metadata");

        self.persistence
            .insert_step_instance(&StepInstanceRecord {
                step_instance_id: next_runtime_id("step"),
                run_id: run_id.to_owned(),
                step_id: step.id.clone(),
                status: STEP_STATUS_SLEEPING.to_owned(),
                attempt_count: 0,
                input: Some(input.clone()),
                output: None,
                error: None,
                created_at: now,
                updated_at: now,
            })
            .await?;

        self.persistence
            .insert_timer(&TimerRecord {
                timer_id: next_runtime_id("timer"),
                run_id: run_id.to_owned(),
                step_id: step.id.clone(),
                timer_kind: TIMER_KIND_SLEEP.to_owned(),
                due_at: now + sleep.seconds as i64,
                status: TIMER_STATUS_PENDING.to_owned(),
                payload: Some(json!({ "input": input })),
                created_at: now,
                updated_at: now,
            })
            .await?;

        self.record_transition(
            run_id,
            "timer_scheduled",
            Some(json!({
                "step_id": step.id,
                "timer_kind": TIMER_KIND_SLEEP,
            })),
            now,
        )
        .await
    }

    async fn schedule_branch_step(
        &self,
        run_id: &str,
        definition: &FlowDefinition,
        step: &StepDefinition,
        input: Value,
        now: i64,
    ) -> Result<(), TaskRuntimeError> {
        let expression = step
            .metadata
            .branch
            .as_ref()
            .expect("validated branch step should include branch metadata")
            .expression
            .clone();
        let matched = evaluate_branch_expression(&expression, &input)?;

        self.persistence
            .insert_step_instance(&StepInstanceRecord {
                step_instance_id: next_runtime_id("step"),
                run_id: run_id.to_owned(),
                step_id: step.id.clone(),
                status: STEP_STATUS_COMPLETED.to_owned(),
                attempt_count: 0,
                input: Some(input.clone()),
                output: Some(input.clone()),
                error: None,
                created_at: now,
                updated_at: now,
            })
            .await?;

        self.record_transition(
            run_id,
            "branch_evaluated",
            Some(json!({
                "step_id": step.id,
                "expression": expression,
                "matched": matched,
            })),
            now,
        )
        .await?;

        if matched {
            Box::pin(self.schedule_downstream_steps(run_id, definition, &step.id, input, now))
                .await?;
        }

        Ok(())
    }

    async fn schedule_emit_event_step(
        &self,
        run_id: &str,
        definition: &FlowDefinition,
        step: &StepDefinition,
        input: Value,
        now: i64,
    ) -> Result<(), TaskRuntimeError> {
        let event_type = step
            .metadata
            .emit_event
            .as_ref()
            .expect("validated emit step should include emit metadata")
            .event_type
            .clone();

        self.persistence
            .insert_step_instance(&StepInstanceRecord {
                step_instance_id: next_runtime_id("step"),
                run_id: run_id.to_owned(),
                step_id: step.id.clone(),
                status: STEP_STATUS_COMPLETED.to_owned(),
                attempt_count: 0,
                input: Some(input.clone()),
                output: Some(input.clone()),
                error: None,
                created_at: now,
                updated_at: now,
            })
            .await?;

        Box::pin(self.ingest_events(vec![EventEnvelope {
            id: None,
            name: event_type.clone(),
            idempotency_key: None,
            data: input.clone(),
        }]))
        .await?;

        self.record_transition(
            run_id,
            "event_emitted",
            Some(json!({
                "step_id": step.id,
                "event_type": event_type,
            })),
            now,
        )
        .await?;

        Box::pin(self.schedule_downstream_steps(run_id, definition, &step.id, input, now)).await?;

        Ok(())
    }

    pub(crate) async fn reconcile_run_status(
        &self,
        run_id: &str,
        now: i64,
    ) -> Result<(), TaskRuntimeError> {
        let run = self.persistence.get_run(run_id).await?.ok_or_else(|| {
            TaskRuntimeError::RunNotFound {
                run_id: run_id.to_owned(),
            }
        })?;

        if run.status == RUN_STATUS_FAILED {
            return Ok(());
        }

        if run.status == RUN_STATUS_CANCEL_REQUESTED
            && !self.persistence.run_has_open_work(run_id).await?
        {
            self.persistence
                .update_run_status(run_id, RUN_STATUS_CANCELLED, now)
                .await?;
            self.record_transition(run_id, "run_cancelled", None, now)
                .await?;
            return Ok(());
        }

        if run.status == RUN_STATUS_CANCEL_REQUESTED {
            return Ok(());
        }

        if self.persistence.run_has_open_work(run_id).await? {
            self.persistence
                .update_run_status(run_id, RUN_STATUS_RUNNING, now)
                .await?;
            return Ok(());
        }

        self.persistence
            .update_run_status(run_id, RUN_STATUS_COMPLETED, now)
            .await?;
        self.record_transition(run_id, "run_completed", None, now)
            .await?;
        Ok(())
    }

    pub(crate) async fn record_transition(
        &self,
        run_id: &str,
        transition_type: &str,
        payload: Option<Value>,
        now: i64,
    ) -> Result<(), TaskRuntimeError> {
        self.persistence
            .insert_transition(&TransitionRecord {
                transition_id: next_runtime_id("transition"),
                run_id: run_id.to_owned(),
                transition_type: transition_type.to_owned(),
                payload,
                created_at: now,
            })
            .await?;
        Ok(())
    }
}

pub(crate) fn root_steps(definition: &FlowDefinition) -> Vec<&StepDefinition> {
    definition
        .steps
        .iter()
        .filter(|step| !definition.edges.iter().any(|edge| edge.to == step.id))
        .collect()
}

pub(crate) fn downstream_steps<'a>(
    definition: &'a FlowDefinition,
    from_step_id: &str,
) -> Vec<&'a StepDefinition> {
    definition
        .edges
        .iter()
        .filter(|edge| edge.from == from_step_id)
        .filter_map(|edge| definition.steps.iter().find(|step| step.id == edge.to))
        .collect()
}

pub(crate) fn next_runtime_id(prefix: &str) -> String {
    format!("{prefix}-{}", Uuid::new_v4())
}

pub(crate) fn active_retry_policy(definition: &FlowDefinition) -> Option<&RetryPolicy> {
    definition.defaults.retry_policy.as_ref()
}

fn retry_delay_seconds(policy: &RetryPolicy, prior_attempt_count: u32) -> u64 {
    let multiplier = policy
        .backoff_coefficient
        .saturating_pow(prior_attempt_count.saturating_sub(1));
    let delay = policy
        .initial_interval_seconds
        .saturating_mul(multiplier as u64);
    delay.min(policy.max_interval_seconds)
}

fn evaluate_branch_expression(expression: &str, input: &Value) -> Result<bool, TaskRuntimeError> {
    let expression = expression.trim();
    match expression {
        "true" => Ok(true),
        "false" => Ok(false),
        _ => {
            if let Some((path, expected)) = expression.split_once("==") {
                let actual = resolve_expression_path(input, path.trim())?;
                let expected = parse_expression_literal(expected.trim())?;
                return Ok(actual == expected);
            }

            if let Some((path, expected)) = expression.split_once("!=") {
                let actual = resolve_expression_path(input, path.trim())?;
                let expected = parse_expression_literal(expected.trim())?;
                return Ok(actual != expected);
            }

            Err(TaskRuntimeError::UnsupportedBranchExpression {
                expression: expression.to_owned(),
            })
        }
    }
}

fn resolve_expression_path(input: &Value, path: &str) -> Result<Value, TaskRuntimeError> {
    let Some(path) = path.strip_prefix("event.data.") else {
        return Err(TaskRuntimeError::UnsupportedBranchExpression {
            expression: path.to_owned(),
        });
    };

    let mut current = input;
    for segment in path.split('.') {
        current =
            current
                .get(segment)
                .ok_or_else(|| TaskRuntimeError::UnsupportedBranchExpression {
                    expression: format!("event.data.{path}"),
                })?;
    }

    Ok(current.clone())
}

fn parse_expression_literal(literal: &str) -> Result<Value, TaskRuntimeError> {
    serde_json::from_str(literal).map_err(|_| TaskRuntimeError::UnsupportedBranchExpression {
        expression: literal.to_owned(),
    })
}
