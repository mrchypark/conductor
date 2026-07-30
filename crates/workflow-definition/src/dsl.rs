use serde::{Deserialize, Serialize};

/// Inclusive lower bound for engine timestamps used in deadline calculations.
pub const MIN_ENGINE_TIMESTAMP_SECONDS: i64 = 0;

/// Inclusive upper bound for engine timestamps used in deadline calculations.
///
/// Limiting the supported clock domain to the lower half of non-negative `i64`
/// leaves enough headroom to add the maximum supported task timeout.
pub const MAX_ENGINE_TIMESTAMP_SECONDS: i64 = i64::MAX / 2;

/// Maximum supported task timeout. Any supported engine timestamp plus this
/// timeout is representable as an `i64`.
pub const MAX_TASK_TIMEOUT_SECONDS: u64 = MAX_ENGINE_TIMESTAMP_SECONDS as u64;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FlowDefinition {
    #[serde(alias = "flow_id")]
    pub workflow_id: String,
    #[serde(alias = "version")]
    pub workflow_version: u32,
    #[serde(default)]
    pub defaults: WorkflowDefaults,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trigger: Option<Trigger>,
    #[serde(default)]
    pub steps: Vec<StepDefinition>,
    #[serde(default)]
    pub edges: Vec<Edge>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Trigger {
    Event(EventTrigger),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventTrigger {
    pub event_type: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StepDefinition {
    #[serde(alias = "step_id")]
    pub id: String,
    pub kind: StepKind,
    #[serde(flatten)]
    pub metadata: StepMetadata,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowDefaults {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retry_policy: Option<RetryPolicy>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetryPolicy {
    pub max_attempts: u32,
    pub initial_interval_seconds: u64,
    pub backoff_coefficient: u32,
    pub max_interval_seconds: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StepKind {
    Task,
    Sleep,
    Branch,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StepMetadata {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub task: Option<TaskMetadata>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sleep: Option<SleepMetadata>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub branch: Option<BranchMetadata>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskMetadata {
    pub handler: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub capability: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub queue: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout_seconds: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SleepMetadata {
    pub seconds: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BranchMetadata {
    pub expression: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Edge {
    pub from: String,
    pub to: String,
}
