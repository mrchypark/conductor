use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use thiserror::Error;
use uuid::Uuid;
use workflow_definition::{
    StepKind, MAX_ENGINE_TIMESTAMP_SECONDS, MAX_TASK_TIMEOUT_SECONDS, MIN_ENGINE_TIMESTAMP_SECONDS,
};
use workflow_protocol::{
    AssignedTask, CompleteTask, FailTask, HeartbeatLease, PollTask, PollTaskResponse,
};
use workflow_worker::WorkerRegistry;

use crate::persistence::{
    CompletedTaskContinuation, ContinuationQuarantineResult, ContinuationQuarantineStaleReason,
    HiqlitePersistence, PersistenceError, TaskExecutionQuarantineResult, TaskOutcomeApplyResult,
};
use crate::stored_definition::{
    CurrentExecutionCompatibilityError, CurrentExecutionContext, DecodedStoredDefinition,
    ExecutableRetryPolicy, ExecutableStepDefinition, ExecutableStepKind,
    ExecutableWorkflowDefinition, StoredDefinitionClassification,
};
use crate::time::now_unix_seconds;

pub(crate) const LEASE_TTL_SECONDS: i64 = 30;
pub(crate) const RUN_STATUS_RUNNING: &str = "running";
pub(crate) const RUN_STATUS_COMPLETED: &str = "completed";
pub(crate) const RUN_STATUS_FAILED: &str = "failed";
pub(crate) const RUN_STATUS_CANCELLED: &str = "cancelled";

pub(crate) const STEP_STATUS_QUEUED: &str = "queued";
pub(crate) const STEP_STATUS_SLEEPING: &str = "sleeping";
pub(crate) const STEP_STATUS_COMPLETED: &str = "completed";
pub(crate) const TIMER_STATUS_PENDING: &str = "pending";
#[cfg(test)]
pub(crate) const TIMER_STATUS_FIRED: &str = "fired";
pub(crate) const TIMER_EFFECT_STATE_PENDING: &str = "pending";

pub(crate) const TIMER_KIND_SLEEP: &str = "sleep";
pub(crate) const TIMER_KIND_RETRY: &str = "retry";

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
    pub timeout_at: Option<i64>,
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
    pub timeout_at: Option<i64>,
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
    pub effect_state: Option<String>,
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
    #[error(
        "task handler '{handler_id}' uses timeout_seconds '{timeout_seconds}' outside the supported range"
    )]
    InvalidTaskTimeout {
        handler_id: String,
        timeout_seconds: u64,
    },
    #[error(
        "engine timestamp '{timestamp}' is outside the supported range '{min_timestamp}'..='{max_timestamp}'"
    )]
    UnsupportedEngineTimestamp {
        timestamp: i64,
        min_timestamp: i64,
        max_timestamp: i64,
    },
    #[error("task lease for task '{task_id}' is stale")]
    StaleLease { task_id: String },
    #[error("no published workflow version '{workflow_id}:{workflow_version}' exists")]
    PublishedWorkflowVersionNotFound {
        workflow_id: String,
        workflow_version: u32,
    },
    #[error(
        "published workflow '{workflow_id}' version {workflow_version} is missing snapshotted publication execution metadata"
    )]
    MissingPublicationExecutionMetadata {
        workflow_id: String,
        workflow_version: u32,
    },
    #[error(
        "published workflow '{workflow_id}' version {workflow_version} has an execution metadata invariant violation: {message}"
    )]
    ExecutionMetadataInvariant {
        workflow_id: String,
        workflow_version: u32,
        message: String,
    },
    #[error("run '{run_id}' does not exist")]
    RunNotFound { run_id: String },
    #[error("step '{step_id}' with kind '{kind:?}' is not supported by the MVP runtime path")]
    UnsupportedStepKind { step_id: String, kind: StepKind },
    #[error("step instance '{run_id}/{step_id}' does not exist")]
    StepInstanceNotFound { run_id: String, step_id: String },
    #[error("branch expression '{expression}' is not supported by the MVP runtime evaluator")]
    UnsupportedBranchExpression { expression: String },
    #[error("persistence error: {0}")]
    Persistence(#[from] PersistenceError),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
}

#[derive(Clone)]
pub struct TaskRuntime {
    pub(crate) persistence: HiqlitePersistence,
}

impl TaskRuntime {
    pub fn new(persistence: HiqlitePersistence, _registry: WorkerRegistry) -> Self {
        Self { persistence }
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
        self.complete_task_at(request, now_unix_seconds()).await
    }

    pub(crate) async fn complete_task_at(
        &self,
        request: CompleteTask,
        now: i64,
    ) -> Result<(), TaskRuntimeError> {
        match self
            .persistence
            .complete_task_with_continuation(&request.lease, &request.result, now)
            .await?
        {
            TaskOutcomeApplyResult::Applied | TaskOutcomeApplyResult::AlreadyApplied => {
                self.drain_task_continuation(&request.lease.task_id, now)
                    .await
            }
            TaskOutcomeApplyResult::Expired => {
                self.persistence
                    .expire_task_if_due(&request.lease.task_id, now)
                    .await?;
                Err(TaskRuntimeError::StaleLease {
                    task_id: request.lease.task_id,
                })
            }
            TaskOutcomeApplyResult::Stale => Err(TaskRuntimeError::StaleLease {
                task_id: request.lease.task_id,
            }),
        }
    }

    pub async fn fail_task(&self, request: FailTask) -> Result<(), TaskRuntimeError> {
        let now = now_unix_seconds();
        let snapshot = match self
            .persistence
            .get_task_snapshot(&request.lease.task_id)
            .await
        {
            Ok(Some(snapshot)) => snapshot,
            Ok(None) => {
                return Err(TaskRuntimeError::StaleLease {
                    task_id: request.lease.task_id,
                })
            }
            Err(PersistenceError::InvalidStoredWorkflowVersion {
                entity_kind: "task",
                ref entity_id,
                ..
            }) if entity_id == &request.lease.task_id => {
                return self
                    .quarantine_failed_task_provenance(&request, "invalid_workflow_version", now)
                    .await;
            }
            Err(error) => return Err(error.into()),
        };
        let stored_definition = match self
            .load_pinned_execution_definition(&snapshot.workflow_id, snapshot.workflow_version)
            .await
        {
            Ok(stored_definition) => stored_definition,
            Err(TaskRuntimeError::PublishedWorkflowVersionNotFound { .. }) => {
                return self
                    .quarantine_failed_task_provenance(&request, "pinned_definition_missing", now)
                    .await;
            }
            Err(TaskRuntimeError::Persistence(PersistenceError::StoredDefinitionDecode {
                ..
            })) => {
                return self
                    .quarantine_failed_task_provenance(&request, "pinned_definition_invalid", now)
                    .await;
            }
            Err(error) => return Err(error),
        };
        if stored_definition.classification() == StoredDefinitionClassification::PreSnapshot {
            return self
                .quarantine_failed_task_provenance(
                    &request,
                    "pre_snapshot_execution_metadata_missing",
                    now,
                )
                .await;
        }
        // Retry behavior comes only from the task's pinned published definition;
        // mutable registry defaults are never consulted at execution time.
        let retry_policy = active_retry_policy(stored_definition.executable());
        let should_retry = request.failure.retryable
            && retry_policy
                .map(|policy| snapshot.attempt_sequence < policy.max_attempts)
                .unwrap_or(false);

        let failure = serde_json::to_value(&request.failure)?;
        let result = if should_retry {
            let delay_seconds = retry_delay_seconds(
                retry_policy.expect("retry policy should exist"),
                snapshot.attempt_sequence,
            );
            let due_at = now.saturating_add(i64::try_from(delay_seconds).unwrap_or(i64::MAX));
            self.persistence
                .fail_task_and_schedule_retry(&request.lease, &failure, due_at, now)
                .await?
        } else {
            self.persistence
                .fail_task_terminal(&request.lease, &failure, now)
                .await?
        };

        match result {
            TaskOutcomeApplyResult::Applied | TaskOutcomeApplyResult::AlreadyApplied => Ok(()),
            TaskOutcomeApplyResult::Expired => {
                self.persistence
                    .expire_task_if_due(&request.lease.task_id, now)
                    .await?;
                Err(TaskRuntimeError::StaleLease {
                    task_id: request.lease.task_id,
                })
            }
            TaskOutcomeApplyResult::Stale => Err(TaskRuntimeError::StaleLease {
                task_id: request.lease.task_id,
            }),
        }
    }

    async fn quarantine_failed_task_provenance(
        &self,
        request: &FailTask,
        stable_code: &str,
        now: i64,
    ) -> Result<(), TaskRuntimeError> {
        match self
            .persistence
            .quarantine_failed_task_execution(&request.lease, &request.failure, stable_code, now)
            .await?
        {
            TaskExecutionQuarantineResult::Applied
            | TaskExecutionQuarantineResult::AlreadyApplied => Ok(()),
            TaskExecutionQuarantineResult::Expired => {
                self.persistence
                    .expire_task_if_due(&request.lease.task_id, now)
                    .await?;
                Err(TaskRuntimeError::StaleLease {
                    task_id: request.lease.task_id.clone(),
                })
            }
            TaskExecutionQuarantineResult::Stale
            | TaskExecutionQuarantineResult::RunNotRunning { .. } => {
                Err(TaskRuntimeError::StaleLease {
                    task_id: request.lease.task_id.clone(),
                })
            }
            TaskExecutionQuarantineResult::Conflict { detail } => Err(
                TaskRuntimeError::Persistence(PersistenceError::Invariant(format!(
                    "task execution quarantine for '{}' conflicts with durable evidence: {detail}",
                    request.lease.task_id
                ))),
            ),
        }
    }

    pub(crate) async fn drain_task_continuation(
        &self,
        task_id: &str,
        now: i64,
    ) -> Result<(), TaskRuntimeError> {
        let Some(continuation) = self
            .persistence
            .get_pending_completed_task_continuation(task_id)
            .await?
        else {
            return Ok(());
        };
        self.apply_task_continuation(continuation, now).await
    }

    pub(crate) async fn apply_task_continuation(
        &self,
        continuation: CompletedTaskContinuation,
        now: i64,
    ) -> Result<(), TaskRuntimeError> {
        let run = self
            .persistence
            .get_run(&continuation.run_id)
            .await?
            .ok_or_else(|| TaskRuntimeError::RunNotFound {
                run_id: continuation.run_id.clone(),
            })?;
        if run.status != RUN_STATUS_RUNNING {
            self.persistence
                .mark_completed_task_continuation_suppressed(&continuation.task_id, now)
                .await?;
            self.reconcile_run_status(&continuation.run_id, now).await?;
            return Ok(());
        }

        let definition = match self
            .load_pinned_execution_definition(
                &continuation.workflow_id,
                continuation.workflow_version,
            )
            .await
        {
            Ok(definition) => definition,
            Err(_) => {
                self.quarantine_task_continuation(
                    &continuation.task_id,
                    "pinned_definition_invalid_or_missing",
                    now,
                )
                .await?;
                return Ok(());
            }
        };
        match definition.classification() {
            StoredDefinitionClassification::SnapshottedV1 => {
                self.schedule_downstream_steps(
                    &continuation.run_id,
                    &definition,
                    &continuation.step_id,
                    continuation.output,
                    now,
                )
                .await?;
            }
            StoredDefinitionClassification::PreSnapshot => {
                self.quarantine_task_continuation(
                    &continuation.task_id,
                    "pre_snapshot_execution_metadata_missing",
                    now,
                )
                .await?;
                return Ok(());
            }
        }
        let marker = self
            .persistence
            .mark_completed_task_continuation_applied(&continuation.task_id, now)
            .await?;
        if marker == MutationApplyResult::Stale {
            self.persistence
                .mark_completed_task_continuation_suppressed(&continuation.task_id, now)
                .await?;
        }
        self.reconcile_run_status(&continuation.run_id, now).await
    }

    async fn quarantine_task_continuation(
        &self,
        task_id: &str,
        code: &str,
        now: i64,
    ) -> Result<(), TaskRuntimeError> {
        match self
            .persistence
            .quarantine_task_continuation(task_id, code, now)
            .await?
        {
            ContinuationQuarantineResult::Applied
            | ContinuationQuarantineResult::AlreadyApplied
            | ContinuationQuarantineResult::Stale(
                ContinuationQuarantineStaleReason::RunNotRunning { .. },
            ) => Ok(()),
            ContinuationQuarantineResult::Stale(reason) => Err(TaskRuntimeError::Persistence(
                PersistenceError::Invariant(format!(
                    "task continuation '{task_id}' could not be quarantined: {reason:?}"
                )),
            )),
            ContinuationQuarantineResult::Conflict { existing_state } => {
                Err(TaskRuntimeError::Persistence(PersistenceError::Invariant(
                    format!(
                        "task continuation '{task_id}' has conflicting quarantine state {existing_state:?}"
                    ),
                )))
            }
        }
    }

    pub async fn list_step_instances(
        &self,
        run_id: &str,
    ) -> Result<Vec<StepInstanceRecord>, TaskRuntimeError> {
        Ok(self.persistence.list_step_instances(run_id).await?)
    }

    pub(crate) async fn list_step_instances_page(
        &self,
        run_id: &str,
        cursor: Option<&(i64, String)>,
        limit: u32,
    ) -> Result<Vec<StepInstanceRecord>, TaskRuntimeError> {
        Ok(self
            .persistence
            .list_step_instances_page(run_id, cursor, limit)
            .await?)
    }

    pub async fn list_transitions(
        &self,
        run_id: &str,
    ) -> Result<Vec<TransitionRecord>, TaskRuntimeError> {
        Ok(self.persistence.list_transitions(run_id).await?)
    }

    pub(crate) async fn list_transitions_page(
        &self,
        run_id: &str,
        cursor: Option<&(i64, String)>,
        limit: u32,
    ) -> Result<Vec<TransitionRecord>, TaskRuntimeError> {
        Ok(self
            .persistence
            .list_transitions_page(run_id, cursor, limit)
            .await?)
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
            RUN_STATUS_COMPLETED | RUN_STATUS_FAILED | RUN_STATUS_CANCELLED
        ) {
            return Ok(());
        }

        match self
            .persistence
            .cancel_run_atomically(run_id, reason, requested_by, now)
            .await?
        {
            MutationApplyResult::Applied
            | MutationApplyResult::AlreadyApplied
            | MutationApplyResult::Stale => Ok(()),
        }
    }

    pub(crate) async fn load_pinned_execution_definition(
        &self,
        workflow_id: &str,
        workflow_version: u32,
    ) -> Result<DecodedStoredDefinition, TaskRuntimeError> {
        self.persistence
            .load_published_definition_for_execution(workflow_id, workflow_version)
            .await?
            .ok_or_else(|| TaskRuntimeError::PublishedWorkflowVersionNotFound {
                workflow_id: workflow_id.to_owned(),
                workflow_version,
            })
    }

    #[cfg(test)]
    pub(crate) async fn load_current_execution_definition(
        &self,
        workflow_id: &str,
        workflow_version: u32,
    ) -> Result<DecodedStoredDefinition, TaskRuntimeError> {
        let decoded = self
            .load_pinned_execution_definition(workflow_id, workflow_version)
            .await?;
        current_execution_context(&decoded)?;
        Ok(decoded)
    }

    pub(crate) async fn schedule_root_steps(
        &self,
        run_id: &str,
        stored_definition: &DecodedStoredDefinition,
        input: Value,
        now: i64,
    ) -> Result<(), TaskRuntimeError> {
        let execution = current_execution_context(stored_definition)?;
        for step in root_steps(execution.definition()) {
            self.schedule_step(run_id, execution, step, input.clone(), now)
                .await?;
        }
        Ok(())
    }

    pub(crate) async fn schedule_downstream_steps(
        &self,
        run_id: &str,
        stored_definition: &DecodedStoredDefinition,
        from_step_id: &str,
        input: Value,
        now: i64,
    ) -> Result<(), TaskRuntimeError> {
        let execution = current_execution_context(stored_definition)?;
        for step in downstream_steps(execution.definition(), from_step_id) {
            self.schedule_step(run_id, execution, step, input.clone(), now)
                .await?;
        }
        Ok(())
    }

    pub(crate) async fn schedule_step(
        &self,
        run_id: &str,
        execution: CurrentExecutionContext<'_>,
        step: &ExecutableStepDefinition,
        input: Value,
        now: i64,
    ) -> Result<(), TaskRuntimeError> {
        match &step.kind {
            ExecutableStepKind::Task(_) => {
                self.schedule_task_step(run_id, execution, step, input, now)
                    .await
            }
            ExecutableStepKind::Sleep(_) => {
                self.schedule_sleep_step(run_id, step, input, now).await
            }
            ExecutableStepKind::Branch(_) => {
                self.schedule_branch_step(run_id, execution, step, input, now)
                    .await
            }
        }
    }

    async fn schedule_task_step(
        &self,
        run_id: &str,
        execution: CurrentExecutionContext<'_>,
        step: &ExecutableStepDefinition,
        input: Value,
        now: i64,
    ) -> Result<(), TaskRuntimeError> {
        let ExecutableStepKind::Task(task_metadata) = &step.kind else {
            return Err(execution_invariant(
                execution,
                format!("step '{}' is not a task", step.id),
            ));
        };
        let route = execution
            .publication_metadata()
            .task_route(&step.id)
            .ok_or_else(|| {
                execution_invariant(
                    execution,
                    format!("task step '{}' has no complete published route", step.id),
                )
            })?;
        if route.handler() != task_metadata.handler {
            return Err(execution_invariant(
                execution,
                format!(
                    "task step '{}' route handler '{}' does not match definition handler '{}'",
                    step.id,
                    route.handler(),
                    task_metadata.handler
                ),
            ));
        }
        let handler_id = route.handler().to_owned();
        let capability = route.capability().to_owned();
        let queue_name = route.queue().to_owned();
        // Timeouts are step-level deadlines; retries and lease requeues consume the same budget.
        let timeout_at = task_timeout_deadline(now, route.timeout_seconds(), &handler_id)?;
        let definition = execution.definition();

        self.persistence
            .insert_step_instance_if_run_running(&StepInstanceRecord {
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

        let task_inserted = self
            .persistence
            .enqueue_task_if_run_running(&QueuedTaskRecord {
                task_id: next_runtime_id("task"),
                run_id: run_id.to_owned(),
                workflow_id: definition.workflow_id.clone(),
                workflow_version: definition.workflow_version,
                step_id: step.id.clone(),
                step_kind: "task".to_owned(),
                handler_id,
                capability,
                queue_name,
                input,
                attempt_sequence: 0,
                timeout_at,
                created_at: now,
            })
            .await?;

        if task_inserted {
            self.record_transition_with_id(
                stable_schedule_transition_id("task-queued", run_id, &step.id),
                run_id,
                "task_queued",
                Some(json!({ "step_id": step.id })),
                now,
            )
            .await?;
        }
        Ok(())
    }

    pub(crate) async fn schedule_sleep_step(
        &self,
        run_id: &str,
        step: &ExecutableStepDefinition,
        input: Value,
        now: i64,
    ) -> Result<(), TaskRuntimeError> {
        let ExecutableStepKind::Sleep(sleep) = &step.kind else {
            unreachable!("schedule_sleep_step is called only for sleep steps");
        };

        self.persistence
            .insert_step_instance_if_run_running(&StepInstanceRecord {
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

        let timer_inserted = self
            .persistence
            .insert_timer_if_run_running(&TimerRecord {
                timer_id: next_runtime_id("timer"),
                run_id: run_id.to_owned(),
                step_id: step.id.clone(),
                timer_kind: TIMER_KIND_SLEEP.to_owned(),
                due_at: now + sleep.seconds as i64,
                status: TIMER_STATUS_PENDING.to_owned(),
                effect_state: Some(TIMER_EFFECT_STATE_PENDING.to_owned()),
                payload: Some(json!({ "input": input })),
                created_at: now,
                updated_at: now,
            })
            .await?;

        if timer_inserted {
            self.record_transition_with_id(
                stable_schedule_transition_id("timer-scheduled", run_id, &step.id),
                run_id,
                "timer_scheduled",
                Some(json!({
                    "step_id": step.id,
                    "timer_kind": TIMER_KIND_SLEEP,
                })),
                now,
            )
            .await?;
        }
        Ok(())
    }

    async fn schedule_branch_step(
        &self,
        run_id: &str,
        execution: CurrentExecutionContext<'_>,
        step: &ExecutableStepDefinition,
        input: Value,
        now: i64,
    ) -> Result<(), TaskRuntimeError> {
        let ExecutableStepKind::Branch(branch) = &step.kind else {
            return Err(execution_invariant(
                execution,
                format!("step '{}' is not a branch", step.id),
            ));
        };
        let expression = branch.expression.clone();
        let matched = evaluate_branch_expression(&expression, &input)?;

        let step_inserted = self
            .persistence
            .insert_step_instance_if_run_running(&StepInstanceRecord {
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

        if step_inserted {
            self.record_transition_with_id(
                stable_schedule_transition_id("branch-evaluated", run_id, &step.id),
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
        }

        if matched {
            for downstream in downstream_steps(execution.definition(), &step.id) {
                Box::pin(self.schedule_step(run_id, execution, downstream, input.clone(), now))
                    .await?;
            }
        }

        Ok(())
    }

    pub(crate) async fn reconcile_run_status(
        &self,
        run_id: &str,
        now: i64,
    ) -> Result<(), TaskRuntimeError> {
        match self
            .persistence
            .complete_run_if_running_and_idle(run_id, now)
            .await?
        {
            MutationApplyResult::Applied
            | MutationApplyResult::AlreadyApplied
            | MutationApplyResult::Stale => Ok(()),
        }
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

    pub(crate) async fn record_transition_with_id(
        &self,
        transition_id: String,
        run_id: &str,
        transition_type: &str,
        payload: Option<Value>,
        now: i64,
    ) -> Result<(), TaskRuntimeError> {
        self.persistence
            .insert_transition_if_absent_for_running_run(&TransitionRecord {
                transition_id,
                run_id: run_id.to_owned(),
                transition_type: transition_type.to_owned(),
                payload,
                created_at: now,
            })
            .await?;
        Ok(())
    }
}

fn current_execution_context(
    stored_definition: &DecodedStoredDefinition,
) -> Result<CurrentExecutionContext<'_>, TaskRuntimeError> {
    stored_definition
        .current_execution_context()
        .map_err(|error| match error {
            CurrentExecutionCompatibilityError::MissingPublicationMetadata {
                workflow_id,
                workflow_version,
            } => TaskRuntimeError::MissingPublicationExecutionMetadata {
                workflow_id,
                workflow_version,
            },
        })
}

fn execution_invariant(
    execution: CurrentExecutionContext<'_>,
    message: String,
) -> TaskRuntimeError {
    TaskRuntimeError::ExecutionMetadataInvariant {
        workflow_id: execution.definition().workflow_id.clone(),
        workflow_version: execution.definition().workflow_version,
        message,
    }
}

pub(crate) fn root_steps(
    definition: &ExecutableWorkflowDefinition,
) -> Vec<&ExecutableStepDefinition> {
    definition
        .steps
        .iter()
        .filter(|step| !definition.edges.iter().any(|edge| edge.to == step.id))
        .collect()
}

pub(crate) fn downstream_steps<'a>(
    definition: &'a ExecutableWorkflowDefinition,
    from_step_id: &str,
) -> Vec<&'a ExecutableStepDefinition> {
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

pub(crate) fn stable_schedule_transition_id(
    operation: &str,
    run_id: &str,
    step_id: &str,
) -> String {
    use std::fmt::Write as _;

    let mut id = format!("schedule-{operation}");
    for component in [run_id, step_id] {
        write!(&mut id, ":{}:{component}", component.len())
            .expect("writing a stable identifier into a String cannot fail");
    }
    id
}

pub(crate) fn active_retry_policy(
    definition: &ExecutableWorkflowDefinition,
) -> Option<&ExecutableRetryPolicy> {
    definition.defaults.retry_policy.as_ref()
}

fn task_timeout_deadline(
    now: i64,
    timeout_seconds: Option<u64>,
    handler_id: &str,
) -> Result<Option<i64>, TaskRuntimeError> {
    if !(MIN_ENGINE_TIMESTAMP_SECONDS..=MAX_ENGINE_TIMESTAMP_SECONDS).contains(&now) {
        return Err(TaskRuntimeError::UnsupportedEngineTimestamp {
            timestamp: now,
            min_timestamp: MIN_ENGINE_TIMESTAMP_SECONDS,
            max_timestamp: MAX_ENGINE_TIMESTAMP_SECONDS,
        });
    }

    let Some(timeout_seconds) = timeout_seconds else {
        return Ok(None);
    };

    if timeout_seconds > MAX_TASK_TIMEOUT_SECONDS {
        return Err(TaskRuntimeError::InvalidTaskTimeout {
            handler_id: handler_id.to_owned(),
            timeout_seconds,
        });
    }

    let timeout_seconds = timeout_seconds as i64;
    now.checked_add(timeout_seconds)
        .ok_or(TaskRuntimeError::InvalidTaskTimeout {
            handler_id: handler_id.to_owned(),
            timeout_seconds: timeout_seconds as u64,
        })
        .map(Some)
}

fn retry_delay_seconds(policy: &ExecutableRetryPolicy, prior_attempt_count: u32) -> u64 {
    let multiplier = policy
        .backoff_coefficient
        .saturating_pow(prior_attempt_count.saturating_sub(1));
    let delay = policy
        .initial_interval_seconds
        .saturating_mul(multiplier as u64);
    delay.min(policy.max_interval_seconds)
}

pub(crate) fn evaluate_branch_expression(
    expression: &str,
    input: &Value,
) -> Result<bool, TaskRuntimeError> {
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

#[cfg(test)]
mod tests {
    use super::{task_timeout_deadline, TaskRuntimeError};
    use serde_json::json;
    use tempfile::TempDir;
    use workflow_definition::{
        FlowDefinition, MAX_ENGINE_TIMESTAMP_SECONDS, MAX_TASK_TIMEOUT_SECONDS,
        MIN_ENGINE_TIMESTAMP_SECONDS,
    };
    use workflow_protocol::{
        FailTask, FailurePayload, PollTask, PollTaskResponse, TaskMutationContext,
    };
    use workflow_worker::{Capability, WorkerRegistry};

    use crate::{
        definition_store::{StoredWorkflowDefinition, WorkflowRunRecord},
        persistence::HiqlitePersistence,
        runtime::{QueuedTaskRecord, StepInstanceRecord, TaskRuntime},
    };

    #[test]
    fn task_timeout_deadline_accepts_supported_horizon_with_maximum_timeout() {
        let deadline = task_timeout_deadline(
            MAX_ENGINE_TIMESTAMP_SECONDS,
            Some(MAX_TASK_TIMEOUT_SECONDS),
            "task.run",
        )
        .expect("supported horizon plus maximum timeout should be representable");

        assert_eq!(deadline, Some(i64::MAX - 1));
    }

    #[test]
    fn task_timeout_deadline_rejects_timestamp_above_supported_horizon() {
        let timestamp = MAX_ENGINE_TIMESTAMP_SECONDS + 1;
        let error = task_timeout_deadline(timestamp, Some(MAX_TASK_TIMEOUT_SECONDS), "task.run")
            .expect_err("timestamp above the supported horizon should fail before addition");

        assert!(matches!(
            error,
            TaskRuntimeError::UnsupportedEngineTimestamp {
                timestamp: actual_timestamp,
                min_timestamp,
                max_timestamp,
            } if actual_timestamp == timestamp
                && min_timestamp == MIN_ENGINE_TIMESTAMP_SECONDS
                && max_timestamp == MAX_ENGINE_TIMESTAMP_SECONDS
        ));
    }

    #[tokio::test]
    async fn exact_pre_snapshot_current_load_fails_without_live_registry_fallback() {
        let tempdir = TempDir::new().expect("temporary directory should exist");
        let persistence = HiqlitePersistence::for_test(tempdir.path())
            .await
            .expect("persistence should start");
        let definition: FlowDefinition = serde_json::from_value(json!({
            "workflow_id": "orders",
            "workflow_version": 1,
            "defaults": {},
            "trigger": {"type": "event", "event_type": "order.ready"},
            "steps": [{
                "id": "run_task",
                "kind": "task",
                "task": {"handler": "orders.fetch"}
            }],
            "edges": []
        }))
        .expect("definition should deserialize");
        persistence
            .insert_published_definition(&StoredWorkflowDefinition {
                workflow_id: "orders".to_owned(),
                workflow_version: 1,
                definition,
                content_hash: "orders-v1".to_owned(),
                created_at: 1,
                published_at: Some(1),
            })
            .await
            .expect("pre-snapshot definition should insert");
        let registry = WorkerRegistry::new();
        registry
            .define_handler("orders.fetch")
            .capability(Capability::Task)
            .default_queue("live-registry-must-not-be-used");
        let runtime = TaskRuntime::new(persistence, registry);

        let error = runtime
            .load_current_execution_definition("orders", 1)
            .await
            .expect_err("pre-snapshot current execution must require published metadata");
        assert!(matches!(
            error,
            TaskRuntimeError::MissingPublicationExecutionMetadata {
                ref workflow_id,
                workflow_version: 1,
            } if workflow_id == "orders"
        ));
    }

    #[tokio::test]
    async fn missing_pinned_definition_failure_quarantines_once_and_cancellation_suppresses() {
        let fixture = FailureQuarantineFixture::new(None, None).await;
        let failure = failure_payload(false);
        let request = FailTask {
            lease: fixture.lease.clone(),
            failure: failure.clone(),
        };

        fixture
            .runtime
            .fail_task(request.clone())
            .await
            .expect("missing pinned definition should quarantine the exact failed lease");
        fixture
            .runtime
            .fail_task(request)
            .await
            .expect("same worker failure should replay idempotently");

        let restarted = TaskRuntime::new(fixture.persistence.clone(), WorkerRegistry::new());
        restarted
            .recovery_scan_at(i64::MAX - 1)
            .await
            .expect("quarantined failure should not hot-loop during recovery");
        restarted
            .recovery_scan_at(i64::MAX - 1)
            .await
            .expect("replayed recovery should remain stable");
        fixture
            .assert_quarantine("pinned_definition_missing", &failure)
            .await;

        restarted
            .cancel_run(
                &fixture.run_id,
                Some("manual recovery cancelled"),
                Some("test"),
            )
            .await
            .expect("cancellation should suppress the open quarantine");
        assert_eq!(
            fixture
                .persistence
                .get_run(&fixture.run_id)
                .await
                .expect("cancelled run should load")
                .expect("cancelled run should exist")
                .status,
            "cancelled"
        );
    }

    #[tokio::test]
    async fn malformed_pinned_definition_quarantines_retryable_failure_without_retry() {
        let fixture = FailureQuarantineFixture::new(Some("not-json"), None).await;
        let failure = failure_payload(true);

        fixture
            .runtime
            .fail_task(FailTask {
                lease: fixture.lease.clone(),
                failure: failure.clone(),
            })
            .await
            .expect("malformed pinned definition should quarantine without retry lookup");
        fixture
            .assert_quarantine("pinned_definition_invalid", &failure)
            .await;
    }

    #[tokio::test]
    async fn pre_snapshot_pinned_definition_failure_quarantines_without_live_defaults() {
        let definition = json!({
            "workflow_id": "failure-flow",
            "workflow_version": 1,
            "defaults": {},
            "trigger": {"type": "event", "event_type": "failure.started"},
            "steps": [{
                "id": "run_task",
                "kind": "task",
                "task": {"handler": "failure.run"}
            }],
            "edges": []
        })
        .to_string();
        let fixture = FailureQuarantineFixture::new(Some(&definition), None).await;
        let failure = failure_payload(true);

        fixture
            .runtime
            .fail_task(FailTask {
                lease: fixture.lease.clone(),
                failure: failure.clone(),
            })
            .await
            .expect("pre-snapshot provenance should quarantine without live registry defaults");
        fixture
            .assert_quarantine("pre_snapshot_execution_metadata_missing", &failure)
            .await;
    }

    struct FailureQuarantineFixture {
        _tempdir: TempDir,
        persistence: HiqlitePersistence,
        runtime: TaskRuntime,
        run_id: String,
        task_id: String,
        lease: TaskMutationContext,
    }

    impl FailureQuarantineFixture {
        async fn new(definition_json: Option<&str>, execution_metadata_json: Option<&str>) -> Self {
            let tempdir = TempDir::new().expect("temporary directory should exist");
            let persistence = HiqlitePersistence::for_test(tempdir.path())
                .await
                .expect("persistence should start");
            if let Some(definition_json) = definition_json {
                persistence
                    .insert_raw_published_definition_for_test(
                        "failure-flow",
                        1,
                        definition_json,
                        execution_metadata_json,
                    )
                    .await
                    .expect("raw pinned definition should insert");
            }
            let run_id = "run-failure-quarantine".to_owned();
            let task_id = "task-failure-quarantine".to_owned();
            persistence
                .insert_run(&WorkflowRunRecord {
                    run_id: run_id.clone(),
                    workflow_id: "failure-flow".to_owned(),
                    workflow_version: 1,
                    status: "running".to_owned(),
                    cancel_reason: None,
                    cancel_requested_by: None,
                    cancel_requested_at: None,
                    created_at: 1,
                })
                .await
                .expect("run should insert");
            persistence
                .insert_step_instance(&StepInstanceRecord {
                    step_instance_id: "step-instance-failure-quarantine".to_owned(),
                    run_id: run_id.clone(),
                    step_id: "run_task".to_owned(),
                    status: "queued".to_owned(),
                    attempt_count: 0,
                    input: Some(json!({"input": true})),
                    output: None,
                    error: None,
                    created_at: 1,
                    updated_at: 1,
                })
                .await
                .expect("step should insert");
            persistence
                .enqueue_task(&QueuedTaskRecord {
                    task_id: task_id.clone(),
                    run_id: run_id.clone(),
                    workflow_id: "failure-flow".to_owned(),
                    workflow_version: 1,
                    step_id: "run_task".to_owned(),
                    step_kind: "task".to_owned(),
                    handler_id: "failure.run".to_owned(),
                    capability: "task".to_owned(),
                    queue_name: "default".to_owned(),
                    input: json!({"input": true}),
                    attempt_sequence: 0,
                    timeout_at: None,
                    created_at: 1,
                })
                .await
                .expect("task should insert");
            let runtime = TaskRuntime::new(persistence.clone(), WorkerRegistry::new());
            let assigned = runtime
                .poll_task(PollTask {
                    worker_id: "worker-failure".to_owned(),
                    worker_name: "failure-worker".to_owned(),
                    queues: vec!["default".to_owned()],
                    capabilities: vec!["task".to_owned()],
                    max_inflight: 1,
                })
                .await
                .expect("task poll should succeed");
            let PollTaskResponse::Assigned(assigned) = assigned else {
                panic!("failure fixture task should be assigned");
            };
            let lease = TaskMutationContext {
                task_id: assigned.task_id,
                attempt_id: assigned.attempt_id,
                worker_id: "worker-failure".to_owned(),
                fencing_token: assigned.fencing_token,
            };
            Self {
                _tempdir: tempdir,
                persistence,
                runtime,
                run_id,
                task_id,
                lease,
            }
        }

        async fn assert_quarantine(&self, stable_code: &str, failure: &FailurePayload) {
            let task = self
                .persistence
                .get_task_snapshot(&self.task_id)
                .await
                .expect("task should load")
                .expect("task should exist");
            assert_eq!(task.status, "failed");
            assert!(task.active_attempt_id.is_none());
            assert!(task.active_fencing_token.is_none());
            assert!(task.leased_by_worker_id.is_none());

            let step = self
                .persistence
                .get_step_instance(&self.run_id, "run_task")
                .await
                .expect("step should load")
                .expect("step should exist");
            assert_eq!(step.status, "failed");
            assert_eq!(step.error, Some(serde_json::to_value(failure).unwrap()));

            let transitions = self
                .persistence
                .list_transitions(&self.run_id)
                .await
                .expect("transitions should load");
            let quarantines = transitions
                .iter()
                .filter(|transition| transition.transition_type == "task_execution_quarantined")
                .collect::<Vec<_>>();
            assert_eq!(quarantines.len(), 1);
            assert_eq!(
                quarantines[0]
                    .payload
                    .as_ref()
                    .and_then(|payload| payload.get("code"))
                    .and_then(|code| code.as_str()),
                Some(stable_code)
            );
            assert!(!transitions.iter().any(|transition| {
                matches!(
                    transition.transition_type.as_str(),
                    "task_retry_scheduled" | "task_failed" | "run_failed"
                )
            }));
            assert_eq!(
                self.persistence
                    .get_run(&self.run_id)
                    .await
                    .expect("run should load")
                    .expect("run should exist")
                    .status,
                "running",
                "open quarantine must prevent false run completion"
            );
            assert!(matches!(
                self.runtime
                    .poll_task(PollTask {
                        worker_id: "worker-after-quarantine".to_owned(),
                        worker_name: "worker-after-quarantine".to_owned(),
                        queues: vec!["default".to_owned()],
                        capabilities: vec!["task".to_owned()],
                        max_inflight: 1,
                    })
                    .await
                    .expect("post-quarantine poll should succeed"),
                PollTaskResponse::NoTaskAvailable
            ));
        }
    }

    fn failure_payload(retryable: bool) -> FailurePayload {
        FailurePayload {
            classification: "application".to_owned(),
            code: "WORKER_REPORTED_FAILURE".to_owned(),
            message: "worker could not safely continue".to_owned(),
            retryable,
            details: json!({"attempted": true}),
        }
    }
}
