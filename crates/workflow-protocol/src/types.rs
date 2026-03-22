use serde::{Deserialize, Serialize};
use serde_json::Value;

pub type RunId = String;
pub type WorkflowId = String;
pub type WorkflowVersion = u32;
pub type TaskId = String;
pub type AttemptId = String;
pub type WorkerId = String;
pub type WorkerName = String;
pub type QueueName = String;
pub type Capability = String;
pub type StepId = String;
pub type StepKindName = String;
pub type RunStatus = String;
pub type FencingToken = u64;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskMutationContext {
    pub task_id: TaskId,
    pub attempt_id: AttemptId,
    pub worker_id: WorkerId,
    pub fencing_token: FencingToken,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TaskOutput {
    pub payload: Value,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FailurePayload {
    pub classification: String,
    pub code: String,
    pub message: String,
    pub retryable: bool,
    pub details: Value,
}
