use serde::{Deserialize, Serialize};
use serde_json::Value;

pub type RunId = String;
pub type TaskId = String;
pub type AttemptId = String;
pub type WorkerId = String;
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
