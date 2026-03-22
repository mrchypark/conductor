use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    Capability, FailurePayload, QueueName, RunId, TaskMutationContext, TaskOutput, WorkerId,
    WorkerName,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PollTask {
    pub worker_id: WorkerId,
    pub worker_name: WorkerName,
    pub queues: Vec<QueueName>,
    pub capabilities: Vec<Capability>,
    pub max_inflight: u32,
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
