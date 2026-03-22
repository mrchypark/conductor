use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FlowDefinition {
    pub flow_id: String,
    pub version: u32,
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
    pub step_id: String,
    pub kind: StepKind,
    #[serde(flatten)]
    pub metadata: StepMetadata,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StepKind {
    Task,
    Sleep,
    WaitForEvent,
    Branch,
    EmitEvent,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StepMetadata {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub task: Option<TaskMetadata>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sleep: Option<SleepMetadata>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub wait_for_event: Option<WaitForEventMetadata>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub branch: Option<BranchMetadata>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub emit_event: Option<EmitEventMetadata>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskMetadata {
    pub handler: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SleepMetadata {
    pub seconds: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WaitForEventMetadata {
    pub event_type: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BranchMetadata {
    pub expression: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EmitEventMetadata {
    pub event_type: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Edge {
    pub from: String,
    pub to: String,
}
