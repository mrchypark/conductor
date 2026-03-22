use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    AttemptId, Capability, FailurePayload, FencingToken, QueueName, RunId, RunStatus, StepId,
    StepKindName, TaskId, TaskMutationContext, TaskOutput, WorkerId, WorkerName, WorkflowId,
    WorkflowVersion,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PollTask {
    pub worker_id: WorkerId,
    pub worker_name: WorkerName,
    pub queues: Vec<QueueName>,
    pub capabilities: Vec<Capability>,
    pub max_inflight: u32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum PollTaskResponse {
    Assigned(AssignedTask),
    NoTaskAvailable,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AssignedTask {
    pub task_id: TaskId,
    pub run_id: RunId,
    pub attempt_id: AttemptId,
    pub fencing_token: FencingToken,
    pub workflow_id: WorkflowId,
    pub workflow_version: WorkflowVersion,
    pub step_id: StepId,
    pub step_kind: StepKindName,
    pub input: Value,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HeartbeatLease {
    #[serde(flatten)]
    pub lease: TaskMutationContext,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CompleteTask {
    #[serde(flatten)]
    pub lease: TaskMutationContext,
    pub result: TaskOutput,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FailTask {
    #[serde(flatten)]
    pub lease: TaskMutationContext,
    pub failure: FailurePayload,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EmitEvent {
    #[serde(flatten)]
    pub lease: TaskMutationContext,
    pub event_type: String,
    pub payload: Value,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GetRun {
    pub run_id: RunId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GetRunResponse {
    pub run_id: RunId,
    pub workflow_id: WorkflowId,
    pub workflow_version: WorkflowVersion,
    pub status: RunStatus,
}
