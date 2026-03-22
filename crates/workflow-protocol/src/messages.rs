use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{RunId, TaskMutationContext, TaskOutput, WorkerId};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PollTask {
    pub worker_id: WorkerId,
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
    pub error: String,
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
