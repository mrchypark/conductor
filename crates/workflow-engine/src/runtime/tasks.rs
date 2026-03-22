use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;
use workflow_definition::{FlowDefinition, StepDefinition, StepKind, Trigger};
use workflow_protocol::{
    AssignedTask, CompleteTask, FailTask, HeartbeatLease, PollTask, PollTaskResponse,
};
use workflow_worker::WorkerRegistry;

use crate::{
    definition_store::WorkflowRunRecord,
    persistence::{HiqlitePersistence, PersistenceError},
};

const LEASE_TTL_SECONDS: i64 = 30;

static NEXT_RUNTIME_ID: AtomicU64 = AtomicU64::new(1);

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
    #[error("task runtime only supports root task steps in this slice; step '{step_id}' has kind '{kind:?}'")]
    UnsupportedRootStep { step_id: String, kind: StepKind },
    #[error(transparent)]
    Persistence(#[from] PersistenceError),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}

#[derive(Clone)]
pub struct TaskRuntime {
    persistence: HiqlitePersistence,
    registry: WorkerRegistry,
}

impl TaskRuntime {
    pub fn new(persistence: HiqlitePersistence, registry: WorkerRegistry) -> Self {
        Self {
            persistence,
            registry,
        }
    }

    pub async fn ingest_event(
        &self,
        event_type: &str,
        payload: Value,
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
            let now = now_unix_seconds();
            let run_id = next_runtime_id("run");
            let run = WorkflowRunRecord {
                run_id: run_id.clone(),
                workflow_id: definition.workflow_id.clone(),
                workflow_version: definition.workflow_version,
                status: "running".to_owned(),
                created_at: now,
            };
            self.persistence.insert_run(&run).await?;

            for step in root_steps(&definition.definition) {
                if step.kind != StepKind::Task {
                    return Err(TaskRuntimeError::UnsupportedRootStep {
                        step_id: step.id.clone(),
                        kind: step.kind,
                    });
                }

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

                let step_instance = StepInstanceRecord {
                    step_instance_id: next_runtime_id("step"),
                    run_id: run_id.clone(),
                    step_id: step.id.clone(),
                    status: "queued".to_owned(),
                    attempt_count: 0,
                    input: Some(payload.clone()),
                    output: None,
                    error: None,
                    created_at: now,
                    updated_at: now,
                };
                self.persistence
                    .insert_step_instance(&step_instance)
                    .await?;

                let task = QueuedTaskRecord {
                    task_id: next_runtime_id("task"),
                    run_id: run_id.clone(),
                    workflow_id: definition.workflow_id.clone(),
                    workflow_version: definition.workflow_version,
                    step_id: step.id.clone(),
                    step_kind: "task".to_owned(),
                    handler_id,
                    capability: registration.capability.as_str().to_owned(),
                    queue_name: registration.default_queue,
                    input: payload.clone(),
                    attempt_sequence: 0,
                    created_at: now,
                };
                self.persistence.enqueue_task(&task).await?;
            }

            started_runs.push(StartedRun {
                run_id,
                workflow_id: definition.workflow_id,
                workflow_version: definition.workflow_version,
            });
        }

        Ok(started_runs)
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

        Ok(match leased {
            Some(lease) => PollTaskResponse::Assigned(Box::new(AssignedTask {
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
            })),
            None => PollTaskResponse::NoTaskAvailable,
        })
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
        match self
            .persistence
            .complete_task(&request.lease, &request.result, now_unix_seconds())
            .await?
        {
            MutationApplyResult::Applied | MutationApplyResult::AlreadyApplied => Ok(()),
            MutationApplyResult::Stale => Err(TaskRuntimeError::StaleLease {
                task_id: request.lease.task_id,
            }),
        }
    }

    pub async fn fail_task(&self, request: FailTask) -> Result<(), TaskRuntimeError> {
        match self
            .persistence
            .fail_task(
                &request.lease,
                &serde_json::to_value(&request.failure)?,
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

    pub async fn list_step_instances(
        &self,
        run_id: &str,
    ) -> Result<Vec<StepInstanceRecord>, TaskRuntimeError> {
        Ok(self.persistence.list_step_instances(run_id).await?)
    }
}

fn root_steps(definition: &FlowDefinition) -> Vec<&StepDefinition> {
    definition
        .steps
        .iter()
        .filter(|step| !definition.edges.iter().any(|edge| edge.to == step.id))
        .collect()
}

fn next_runtime_id(prefix: &str) -> String {
    let id = NEXT_RUNTIME_ID.fetch_add(1, Ordering::Relaxed);
    format!("{prefix}-{id}")
}

fn now_unix_seconds() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};

    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time should be after unix epoch")
        .as_secs() as i64
}
