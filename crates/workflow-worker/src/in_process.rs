use std::{collections::BTreeMap, sync::Arc};

use async_trait::async_trait;
use serde_json::Value;
use thiserror::Error;
use workflow_protocol::{
    CompleteTask, FailTask, FailurePayload, HeartbeatLease, PollTask, PollTaskResponse,
    TaskMutationContext, TaskOutput,
};

type HandlerFn = Arc<dyn Fn(Value) -> Result<Value, FailurePayload> + Send + Sync>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RunOnceOutcome {
    NoTaskAvailable,
    Completed {
        task_id: String,
        run_id: String,
        step_id: String,
    },
    Failed {
        task_id: String,
        run_id: String,
        step_id: String,
        failure_code: String,
    },
}

impl RunOnceOutcome {
    pub fn completed_task(&self) -> Option<&str> {
        match self {
            Self::Completed { task_id, .. } => Some(task_id.as_str()),
            Self::NoTaskAvailable | Self::Failed { .. } => None,
        }
    }
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum TaskClientError {
    #[error("{message}")]
    Message { message: String },
}

#[async_trait]
pub trait TaskExecutionClient: Clone + Send + Sync + 'static {
    async fn poll_task(&self, request: PollTask) -> Result<PollTaskResponse, TaskClientError>;
    async fn heartbeat_lease(&self, request: HeartbeatLease) -> Result<(), TaskClientError>;
    async fn complete_task(&self, request: CompleteTask) -> Result<(), TaskClientError>;
    async fn fail_task(&self, request: FailTask) -> Result<(), TaskClientError>;
}

#[derive(Debug, Error)]
pub enum InProcessWorkerError {
    #[error("task execution client error: {0}")]
    Client(#[from] TaskClientError),
    #[error("assigned task references unregistered handler '{handler_id}'")]
    MissingHandler { handler_id: String },
}

#[derive(Clone)]
pub struct InProcessWorker<C> {
    client: C,
    worker_id: String,
    worker_name: String,
    queues: Vec<String>,
    capabilities: Vec<String>,
    max_inflight: u32,
    handlers: BTreeMap<String, HandlerFn>,
}

impl<C> InProcessWorker<C>
where
    C: TaskExecutionClient,
{
    pub fn new(client: C, worker_id: String, worker_name: String) -> Self {
        Self {
            client,
            worker_id,
            worker_name,
            queues: Vec::new(),
            capabilities: Vec::new(),
            max_inflight: 1,
            handlers: BTreeMap::new(),
        }
    }

    pub fn queue(mut self, queue: impl Into<String>) -> Self {
        self.queues.push(queue.into());
        self
    }

    pub fn capability(mut self, capability: impl Into<String>) -> Self {
        self.capabilities.push(capability.into());
        self
    }

    pub fn max_inflight(mut self, max_inflight: u32) -> Self {
        self.max_inflight = max_inflight;
        self
    }

    pub fn register_handler<F>(mut self, handler_id: impl Into<String>, handler: F) -> Self
    where
        F: Fn(Value) -> Result<Value, FailurePayload> + Send + Sync + 'static,
    {
        self.handlers.insert(handler_id.into(), Arc::new(handler));
        self
    }

    pub async fn run_once(&self) -> Result<RunOnceOutcome, InProcessWorkerError> {
        let response = self
            .client
            .poll_task(PollTask {
                worker_id: self.worker_id.clone(),
                worker_name: self.worker_name.clone(),
                queues: self.queues.clone(),
                capabilities: self.capabilities.clone(),
                max_inflight: self.max_inflight,
            })
            .await?;

        match response {
            PollTaskResponse::NoTaskAvailable => Ok(RunOnceOutcome::NoTaskAvailable),
            PollTaskResponse::Assigned(task) => {
                let task = *task;
                let lease = TaskMutationContext {
                    task_id: task.task_id.clone(),
                    attempt_id: task.attempt_id.clone(),
                    worker_id: self.worker_id.clone(),
                    fencing_token: task.fencing_token,
                };

                self.client
                    .heartbeat_lease(HeartbeatLease {
                        lease: lease.clone(),
                    })
                    .await?;

                let handler = self.handlers.get(&task.handler_id).ok_or_else(|| {
                    InProcessWorkerError::MissingHandler {
                        handler_id: task.handler_id.clone(),
                    }
                })?;

                match handler(task.input.clone()) {
                    Ok(payload) => {
                        self.client
                            .complete_task(CompleteTask {
                                lease,
                                result: TaskOutput { payload },
                            })
                            .await?;
                        Ok(RunOnceOutcome::Completed {
                            task_id: task.task_id,
                            run_id: task.run_id,
                            step_id: task.step_id,
                        })
                    }
                    Err(failure) => {
                        let failure_code = failure.code.clone();
                        self.client.fail_task(FailTask { lease, failure }).await?;
                        Ok(RunOnceOutcome::Failed {
                            task_id: task.task_id,
                            run_id: task.run_id,
                            step_id: task.step_id,
                            failure_code,
                        })
                    }
                }
            }
        }
    }
}
