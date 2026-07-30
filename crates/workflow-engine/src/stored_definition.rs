use std::collections::{BTreeMap, BTreeSet};

use serde::{de::DeserializeOwned, Deserialize, Serialize};
use thiserror::Error;
use workflow_definition::{
    validate_definition, FlowDefinition, StepKind, Trigger, MAX_TASK_TIMEOUT_SECONDS,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum PublicationSemanticsVersion {
    #[serde(rename = "v1")]
    V1,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ResolvedTaskRoute {
    handler: String,
    capability: String,
    queue: String,
    timeout_seconds: Option<u64>,
}

impl ResolvedTaskRoute {
    pub(crate) fn new(
        handler: impl Into<String>,
        capability: impl Into<String>,
        queue: impl Into<String>,
        timeout_seconds: Option<u64>,
    ) -> Result<Self, StoredDefinitionDecodeError> {
        let route = Self {
            handler: handler.into(),
            capability: capability.into(),
            queue: queue.into(),
            timeout_seconds,
        };
        validate_route_timeout(None, &route)?;
        Ok(route)
    }

    pub(crate) fn handler(&self) -> &str {
        &self.handler
    }

    pub(crate) fn capability(&self) -> &str {
        &self.capability
    }

    pub(crate) fn queue(&self) -> &str {
        &self.queue
    }

    pub(crate) fn timeout_seconds(&self) -> Option<u64> {
        self.timeout_seconds
    }
}

/// Routing inputs resolved once when an immutable workflow version is published.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PublicationExecutionMetadata {
    semantics_version: PublicationSemanticsVersion,
    task_routes: BTreeMap<String, ResolvedTaskRoute>,
}

impl PublicationExecutionMetadata {
    pub(crate) fn v1(
        task_routes: BTreeMap<String, ResolvedTaskRoute>,
    ) -> Result<Self, StoredDefinitionDecodeError> {
        validate_route_timeouts(&task_routes)?;
        Ok(Self {
            semantics_version: PublicationSemanticsVersion::V1,
            task_routes,
        })
    }

    pub(crate) fn semantics_version(&self) -> PublicationSemanticsVersion {
        self.semantics_version
    }

    pub(crate) fn task_routes(&self) -> &BTreeMap<String, ResolvedTaskRoute> {
        &self.task_routes
    }

    pub(crate) fn task_route(&self, step_id: &str) -> Option<&ResolvedTaskRoute> {
        self.task_routes.get(step_id)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StoredDefinitionClassification {
    SnapshottedV1,
    PreSnapshot,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum NewRunEligibility {
    Eligible,
    IneligiblePreSnapshot,
    IneligibleDecodeError,
}

impl StoredDefinitionClassification {
    pub(crate) fn new_run_eligibility(self) -> NewRunEligibility {
        match self {
            Self::SnapshottedV1 => NewRunEligibility::Eligible,
            Self::PreSnapshot => NewRunEligibility::IneligiblePreSnapshot,
        }
    }
}

/// Engine-only graph built from the current public authoring DSL.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ExecutableWorkflowDefinition {
    pub(crate) workflow_id: String,
    pub(crate) workflow_version: u32,
    pub(crate) defaults: ExecutableWorkflowDefaults,
    pub(crate) trigger_event_type: String,
    pub(crate) steps: Vec<ExecutableStepDefinition>,
    pub(crate) edges: Vec<ExecutableEdge>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct ExecutableWorkflowDefaults {
    pub(crate) retry_policy: Option<ExecutableRetryPolicy>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ExecutableRetryPolicy {
    pub(crate) max_attempts: u32,
    pub(crate) initial_interval_seconds: u64,
    pub(crate) backoff_coefficient: u32,
    pub(crate) max_interval_seconds: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ExecutableEdge {
    pub(crate) from: String,
    pub(crate) to: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ExecutableStepDefinition {
    pub(crate) id: String,
    pub(crate) kind: ExecutableStepKind,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ExecutableStepKind {
    Task(ExecutableTaskMetadata),
    Sleep(ExecutableSleepMetadata),
    Branch(ExecutableBranchMetadata),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ExecutableTaskMetadata {
    pub(crate) handler: String,
    pub(crate) capability: Option<String>,
    pub(crate) queue: Option<String>,
    pub(crate) timeout_seconds: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ExecutableSleepMetadata {
    pub(crate) seconds: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ExecutableBranchMetadata {
    pub(crate) expression: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DecodedStoredDefinition {
    classification: StoredDefinitionClassification,
    raw_definition_json: String,
    raw_execution_metadata_json: Option<String>,
    authoring_definition: Option<FlowDefinition>,
    execution_metadata: Option<PublicationExecutionMetadata>,
    executable: ExecutableWorkflowDefinition,
}

/// Borrowed, fully validated execution envelope for the current runtime.
///
/// Construction is intentionally limited to snapshotted publications so no
/// current execution path can fall back to registry state for an older row.
#[derive(Debug, Clone, Copy)]
pub(crate) struct CurrentExecutionContext<'a> {
    definition: &'a ExecutableWorkflowDefinition,
    publication_metadata: &'a PublicationExecutionMetadata,
}

impl<'a> CurrentExecutionContext<'a> {
    pub(crate) fn definition(self) -> &'a ExecutableWorkflowDefinition {
        self.definition
    }

    pub(crate) fn publication_metadata(self) -> &'a PublicationExecutionMetadata {
        self.publication_metadata
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub(crate) enum CurrentExecutionCompatibilityError {
    #[error(
        "published workflow '{workflow_id}' version {workflow_version} is missing snapshotted publication execution metadata"
    )]
    MissingPublicationMetadata {
        workflow_id: String,
        workflow_version: u32,
    },
}

impl DecodedStoredDefinition {
    pub(crate) fn decode(
        definition_json: &str,
        execution_metadata_json: Option<&str>,
    ) -> Result<Self, StoredDefinitionDecodeError> {
        let current =
            decode_exact_json::<FlowDefinition>(definition_json).map_err(|current_error| {
                StoredDefinitionDecodeError::UnsupportedDefinition { current_error }
            })?;
        Self::from_current(current, definition_json, execution_metadata_json)
    }

    fn from_current(
        current: FlowDefinition,
        definition_json: &str,
        execution_metadata_json: Option<&str>,
    ) -> Result<Self, StoredDefinitionDecodeError> {
        validate_definition(&current).map_err(|error| {
            StoredDefinitionDecodeError::InvalidDefinition {
                message: format!("current definition failed validation: {error:?}"),
            }
        })?;
        let executable = ExecutableWorkflowDefinition::from(&current);
        let (classification, execution_metadata) = match execution_metadata_json {
            Some(raw_metadata) => {
                let metadata = decode_execution_metadata(raw_metadata)?;
                validate_execution_metadata(&executable, &metadata)?;
                (
                    StoredDefinitionClassification::SnapshottedV1,
                    Some(metadata),
                )
            }
            None => (StoredDefinitionClassification::PreSnapshot, None),
        };

        Ok(Self {
            classification,
            raw_definition_json: definition_json.to_owned(),
            raw_execution_metadata_json: execution_metadata_json.map(ToOwned::to_owned),
            authoring_definition: Some(current),
            execution_metadata,
            executable,
        })
    }

    pub(crate) fn classification(&self) -> StoredDefinitionClassification {
        self.classification
    }

    pub(crate) fn new_run_eligibility(&self) -> NewRunEligibility {
        self.classification.new_run_eligibility()
    }

    pub(crate) fn is_new_run_eligible(&self) -> bool {
        self.new_run_eligibility() == NewRunEligibility::Eligible
    }

    pub(crate) fn raw_definition_json(&self) -> &str {
        &self.raw_definition_json
    }

    pub(crate) fn raw_execution_metadata_json(&self) -> Option<&str> {
        self.raw_execution_metadata_json.as_deref()
    }

    pub(crate) fn authoring_definition(&self) -> Option<&FlowDefinition> {
        self.authoring_definition.as_ref()
    }

    pub(crate) fn execution_metadata(&self) -> Option<&PublicationExecutionMetadata> {
        self.execution_metadata.as_ref()
    }

    pub(crate) fn executable(&self) -> &ExecutableWorkflowDefinition {
        &self.executable
    }

    pub(crate) fn current_execution_context(
        &self,
    ) -> Result<CurrentExecutionContext<'_>, CurrentExecutionCompatibilityError> {
        match self.classification {
            StoredDefinitionClassification::SnapshottedV1 => {
                let publication_metadata = self.execution_metadata.as_ref().ok_or_else(|| {
                    CurrentExecutionCompatibilityError::MissingPublicationMetadata {
                        workflow_id: self.executable.workflow_id.clone(),
                        workflow_version: self.executable.workflow_version,
                    }
                })?;
                Ok(CurrentExecutionContext {
                    definition: &self.executable,
                    publication_metadata,
                })
            }
            StoredDefinitionClassification::PreSnapshot => Err(
                CurrentExecutionCompatibilityError::MissingPublicationMetadata {
                    workflow_id: self.executable.workflow_id.clone(),
                    workflow_version: self.executable.workflow_version,
                },
            ),
        }
    }
}

pub(crate) fn decode_stored_definition(
    definition_json: &str,
    execution_metadata_json: Option<&str>,
) -> Result<DecodedStoredDefinition, StoredDefinitionDecodeError> {
    DecodedStoredDefinition::decode(definition_json, execution_metadata_json)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StoredDefinitionDecodeDiagnosticCode {
    UnsupportedDefinition,
    InvalidDefinition,
    InvalidExecutionMetadata,
    UnknownExecutionSemanticsVersion,
    WorkflowIdMismatch,
    WorkflowVersionMismatch,
}

impl StoredDefinitionDecodeDiagnosticCode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::UnsupportedDefinition => "unsupported_definition",
            Self::InvalidDefinition => "invalid_definition",
            Self::InvalidExecutionMetadata => "invalid_execution_metadata",
            Self::UnknownExecutionSemanticsVersion => "unknown_execution_semantics_version",
            Self::WorkflowIdMismatch => "workflow_id_mismatch",
            Self::WorkflowVersionMismatch => "workflow_version_mismatch",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredDefinitionDecodeDiagnostic {
    pub code: StoredDefinitionDecodeDiagnosticCode,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub(crate) enum StoredDefinitionDecodeError {
    #[error("stored definition does not match the current authoring format: {current_error}")]
    UnsupportedDefinition { current_error: String },
    #[error("invalid stored definition: {message}")]
    InvalidDefinition { message: String },
    #[error("invalid publication execution metadata: {message}")]
    InvalidExecutionMetadata { message: String },
    #[error("unknown publication execution semantics version '{version}'")]
    UnknownExecutionSemanticsVersion { version: String },
    #[error(
        "stored definition workflow_id '{definition_workflow_id}' does not match row workflow_id '{row_workflow_id}'"
    )]
    WorkflowIdMismatch {
        row_workflow_id: String,
        definition_workflow_id: String,
    },
    #[error(
        "stored definition workflow_version {definition_workflow_version} does not match row workflow_version {row_workflow_version}"
    )]
    WorkflowVersionMismatch {
        row_workflow_version: i64,
        definition_workflow_version: u32,
    },
}

impl StoredDefinitionDecodeError {
    pub(crate) fn diagnostic(&self) -> StoredDefinitionDecodeDiagnostic {
        let code = match self {
            Self::UnsupportedDefinition { .. } => {
                StoredDefinitionDecodeDiagnosticCode::UnsupportedDefinition
            }
            Self::InvalidDefinition { .. } => {
                StoredDefinitionDecodeDiagnosticCode::InvalidDefinition
            }
            Self::InvalidExecutionMetadata { .. } => {
                StoredDefinitionDecodeDiagnosticCode::InvalidExecutionMetadata
            }
            Self::UnknownExecutionSemanticsVersion { .. } => {
                StoredDefinitionDecodeDiagnosticCode::UnknownExecutionSemanticsVersion
            }
            Self::WorkflowIdMismatch { .. } => {
                StoredDefinitionDecodeDiagnosticCode::WorkflowIdMismatch
            }
            Self::WorkflowVersionMismatch { .. } => {
                StoredDefinitionDecodeDiagnosticCode::WorkflowVersionMismatch
            }
        };
        StoredDefinitionDecodeDiagnostic {
            code,
            message: self.to_string(),
        }
    }
}

fn decode_exact_json<T>(raw_json: &str) -> Result<T, String>
where
    T: DeserializeOwned + Serialize,
{
    let raw_value: serde_json::Value =
        serde_json::from_str(raw_json).map_err(|error| error.to_string())?;
    let decoded: T =
        serde_json::from_value(raw_value.clone()).map_err(|error| error.to_string())?;
    let canonical_value = serde_json::to_value(&decoded).map_err(|error| error.to_string())?;
    if raw_value != canonical_value {
        return Err("JSON shape differs from the canonical serialized format".to_owned());
    }
    Ok(decoded)
}

fn decode_execution_metadata(
    metadata_json: &str,
) -> Result<PublicationExecutionMetadata, StoredDefinitionDecodeError> {
    #[derive(Deserialize)]
    struct SemanticsProbe {
        semantics_version: String,
    }

    let probe: SemanticsProbe = serde_json::from_str(metadata_json).map_err(|error| {
        StoredDefinitionDecodeError::InvalidExecutionMetadata {
            message: error.to_string(),
        }
    })?;
    if probe.semantics_version != "v1" {
        return Err(
            StoredDefinitionDecodeError::UnknownExecutionSemanticsVersion {
                version: probe.semantics_version,
            },
        );
    }

    let metadata: PublicationExecutionMetadata = decode_exact_json(metadata_json)
        .map_err(|message| StoredDefinitionDecodeError::InvalidExecutionMetadata { message })?;
    validate_route_timeouts(&metadata.task_routes)?;
    Ok(metadata)
}

fn validate_route_timeouts(
    task_routes: &BTreeMap<String, ResolvedTaskRoute>,
) -> Result<(), StoredDefinitionDecodeError> {
    for (step_id, route) in task_routes {
        validate_route_timeout(Some(step_id), route)?;
    }
    Ok(())
}

fn validate_route_timeout(
    step_id: Option<&str>,
    route: &ResolvedTaskRoute,
) -> Result<(), StoredDefinitionDecodeError> {
    if route
        .timeout_seconds
        .is_some_and(|timeout| timeout > MAX_TASK_TIMEOUT_SECONDS)
    {
        let subject = step_id
            .map(|id| format!("step '{id}'"))
            .unwrap_or_else(|| format!("handler '{}'", route.handler));
        return Err(StoredDefinitionDecodeError::InvalidExecutionMetadata {
            message: format!(
                "task route for {subject} has timeout_seconds above supported maximum {MAX_TASK_TIMEOUT_SECONDS}"
            ),
        });
    }
    Ok(())
}

fn validate_execution_metadata(
    definition: &ExecutableWorkflowDefinition,
    metadata: &PublicationExecutionMetadata,
) -> Result<(), StoredDefinitionDecodeError> {
    validate_route_timeouts(&metadata.task_routes)?;
    let task_steps = definition
        .steps
        .iter()
        .filter_map(|step| match &step.kind {
            ExecutableStepKind::Task(task) => Some((step.id.as_str(), task)),
            _ => None,
        })
        .collect::<BTreeMap<_, _>>();
    let expected_step_ids = task_steps.keys().copied().collect::<BTreeSet<_>>();
    let actual_step_ids = metadata
        .task_routes
        .keys()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();

    if expected_step_ids != actual_step_ids {
        return Err(StoredDefinitionDecodeError::InvalidExecutionMetadata {
            message: format!(
                "task route step ids do not match definition: expected {expected_step_ids:?}, found {actual_step_ids:?}"
            ),
        });
    }

    for (step_id, task) in task_steps {
        let route = &metadata.task_routes[step_id];
        if route.handler != task.handler {
            return Err(StoredDefinitionDecodeError::InvalidExecutionMetadata {
                message: format!(
                    "task route for step '{step_id}' uses handler '{}', expected '{}'",
                    route.handler, task.handler
                ),
            });
        }
        if task
            .capability
            .as_deref()
            .is_some_and(|value| value != route.capability)
        {
            return Err(StoredDefinitionDecodeError::InvalidExecutionMetadata {
                message: format!(
                    "task route for step '{step_id}' does not preserve explicit capability"
                ),
            });
        }
        if task
            .queue
            .as_deref()
            .is_some_and(|value| value != route.queue)
        {
            return Err(StoredDefinitionDecodeError::InvalidExecutionMetadata {
                message: format!(
                    "task route for step '{step_id}' does not preserve explicit queue"
                ),
            });
        }
        if task
            .timeout_seconds
            .is_some_and(|value| Some(value) != route.timeout_seconds)
        {
            return Err(StoredDefinitionDecodeError::InvalidExecutionMetadata {
                message: format!(
                    "task route for step '{step_id}' does not preserve explicit timeout_seconds"
                ),
            });
        }
    }

    Ok(())
}

impl From<&FlowDefinition> for ExecutableWorkflowDefinition {
    fn from(definition: &FlowDefinition) -> Self {
        let trigger_event_type = match definition
            .trigger
            .as_ref()
            .expect("validated stored definition requires an event trigger")
        {
            Trigger::Event(trigger) => trigger.event_type.clone(),
        };
        let steps = definition
            .steps
            .iter()
            .map(|step| ExecutableStepDefinition {
                id: step.id.clone(),
                kind: match step.kind {
                    StepKind::Task => {
                        let task = step
                            .metadata
                            .task
                            .as_ref()
                            .expect("validated task step requires task metadata");
                        ExecutableStepKind::Task(ExecutableTaskMetadata {
                            handler: task.handler.clone(),
                            capability: task.capability.clone(),
                            queue: task.queue.clone(),
                            timeout_seconds: task.timeout_seconds,
                        })
                    }
                    StepKind::Sleep => {
                        let sleep = step
                            .metadata
                            .sleep
                            .as_ref()
                            .expect("validated sleep step requires sleep metadata");
                        ExecutableStepKind::Sleep(ExecutableSleepMetadata {
                            seconds: sleep.seconds,
                        })
                    }
                    StepKind::Branch => {
                        let branch = step
                            .metadata
                            .branch
                            .as_ref()
                            .expect("validated branch step requires branch metadata");
                        ExecutableStepKind::Branch(ExecutableBranchMetadata {
                            expression: branch.expression.clone(),
                        })
                    }
                },
            })
            .collect();

        Self {
            workflow_id: definition.workflow_id.clone(),
            workflow_version: definition.workflow_version,
            defaults: ExecutableWorkflowDefaults {
                retry_policy: definition.defaults.retry_policy.as_ref().map(|policy| {
                    ExecutableRetryPolicy {
                        max_attempts: policy.max_attempts,
                        initial_interval_seconds: policy.initial_interval_seconds,
                        backoff_coefficient: policy.backoff_coefficient,
                        max_interval_seconds: policy.max_interval_seconds,
                    }
                }),
            },
            trigger_event_type,
            steps,
            edges: definition
                .edges
                .iter()
                .map(|edge| ExecutableEdge {
                    from: edge.from.clone(),
                    to: edge.to.clone(),
                })
                .collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    const CURRENT_DEFINITION_JSON: &str = r#"{
  "workflow_id": "orders",
  "workflow_version": 3,
  "defaults": {},
  "trigger": { "type": "event", "event_type": "order.ready" },
  "steps": [
    {
      "id": "reserve",
      "kind": "task",
      "task": {
        "handler": "inventory.reserve",
        "capability": "task",
        "queue": "orders",
        "timeout_seconds": 30
      }
    },
    { "id": "pause", "kind": "sleep", "sleep": { "seconds": 5 } },
    { "id": "approved", "kind": "branch", "branch": { "expression": "true" } }
  ],
  "edges": [
    { "from": "reserve", "to": "pause" },
    { "from": "pause", "to": "approved" }
  ]
}"#;

    const UNSUPPORTED_WAIT_DEFINITION_JSON: &str = r#"{
  "workflow_id": "unsupported-orders",
  "workflow_version": 1,
  "defaults": {},
  "trigger": { "type": "event", "event_type": "order.created" },
  "steps": [
    {
      "id": "wait_payment",
      "kind": "wait_for_event",
      "wait_for_event": { "event_type": "payment.captured" }
    }
  ],
  "edges": []
}"#;

    const EXECUTION_METADATA_JSON: &str = r#"{
  "semantics_version": "v1",
  "task_routes": {
    "reserve": {
      "handler": "inventory.reserve",
      "capability": "task",
      "queue": "orders",
      "timeout_seconds": 30
    }
  }
}"#;

    #[test]
    fn preserves_exact_raw_json_for_inspection() {
        let decoded =
            DecodedStoredDefinition::decode(CURRENT_DEFINITION_JSON, Some(EXECUTION_METADATA_JSON))
                .expect("snapshotted definition should decode");

        assert_eq!(decoded.raw_definition_json(), CURRENT_DEFINITION_JSON);
        assert_eq!(
            decoded.raw_execution_metadata_json(),
            Some(EXECUTION_METADATA_JSON)
        );
    }

    #[test]
    fn decodes_current_authoring_json_with_and_without_snapshot() {
        let pre_snapshot = DecodedStoredDefinition::decode(CURRENT_DEFINITION_JSON, None)
            .expect("current authoring definition should decode");
        assert_eq!(
            pre_snapshot.classification(),
            StoredDefinitionClassification::PreSnapshot
        );
        assert!(pre_snapshot.authoring_definition().is_some());
        assert_eq!(pre_snapshot.executable().workflow_id, "orders");

        let snapshotted =
            DecodedStoredDefinition::decode(CURRENT_DEFINITION_JSON, Some(EXECUTION_METADATA_JSON))
                .expect("current definition with v1 metadata should decode");
        assert_eq!(
            snapshotted.classification(),
            StoredDefinitionClassification::SnapshottedV1
        );
        assert_eq!(
            snapshotted
                .execution_metadata()
                .expect("metadata should exist")
                .task_route("reserve")
                .expect("route should exist")
                .queue(),
            "orders"
        );
    }

    #[test]
    fn rejects_malformed_or_noncanonical_definitions_and_metadata() {
        assert!(matches!(
            DecodedStoredDefinition::decode("{", None),
            Err(StoredDefinitionDecodeError::UnsupportedDefinition { .. })
        ));
        assert!(matches!(
            DecodedStoredDefinition::decode(CURRENT_DEFINITION_JSON, Some("{")),
            Err(StoredDefinitionDecodeError::InvalidExecutionMetadata { .. })
        ));

        let definition_with_unknown_field = CURRENT_DEFINITION_JSON.replacen(
            "\"workflow_version\": 3,",
            "\"workflow_version\": 3, \"unknown\": true,",
            1,
        );
        assert!(matches!(
            DecodedStoredDefinition::decode(&definition_with_unknown_field, None),
            Err(StoredDefinitionDecodeError::UnsupportedDefinition { .. })
        ));

        assert!(matches!(
            DecodedStoredDefinition::decode(UNSUPPORTED_WAIT_DEFINITION_JSON, None),
            Err(StoredDefinitionDecodeError::UnsupportedDefinition { .. })
        ));
    }

    #[test]
    fn rejects_unknown_semantics_and_mismatched_route_snapshots() {
        let unknown = EXECUTION_METADATA_JSON.replace("\"v1\"", "\"v2\"");
        assert!(matches!(
            DecodedStoredDefinition::decode(CURRENT_DEFINITION_JSON, Some(&unknown)),
            Err(StoredDefinitionDecodeError::UnknownExecutionSemanticsVersion { ref version })
                if version == "v2"
        ));

        let missing_route = r#"{ "semantics_version": "v1", "task_routes": {} }"#;
        assert!(matches!(
            DecodedStoredDefinition::decode(CURRENT_DEFINITION_JSON, Some(missing_route)),
            Err(StoredDefinitionDecodeError::InvalidExecutionMetadata { .. })
        ));
    }

    #[test]
    fn classification_controls_new_run_eligibility() {
        let snapshotted =
            DecodedStoredDefinition::decode(CURRENT_DEFINITION_JSON, Some(EXECUTION_METADATA_JSON))
                .expect("snapshotted definition should decode");
        let pre_snapshot = DecodedStoredDefinition::decode(CURRENT_DEFINITION_JSON, None)
            .expect("pre-snapshot definition should decode");
        assert_eq!(
            snapshotted.new_run_eligibility(),
            NewRunEligibility::Eligible
        );
        assert!(snapshotted.is_new_run_eligible());
        assert_eq!(
            pre_snapshot.new_run_eligibility(),
            NewRunEligibility::IneligiblePreSnapshot
        );
        assert!(!pre_snapshot.is_new_run_eligible());
    }

    #[test]
    fn publication_execution_metadata_roundtrips_with_resolved_route() {
        let route = ResolvedTaskRoute::new("inventory.reserve", "task", "orders", Some(30))
            .expect("bounded route should build");
        let metadata =
            PublicationExecutionMetadata::v1(BTreeMap::from([("reserve".to_owned(), route)]))
                .expect("bounded snapshot should build");

        let json = serde_json::to_value(&metadata).expect("metadata should serialize");
        assert_eq!(
            json,
            json!({
                "semantics_version": "v1",
                "task_routes": {
                    "reserve": {
                        "handler": "inventory.reserve",
                        "capability": "task",
                        "queue": "orders",
                        "timeout_seconds": 30
                    }
                }
            })
        );
        let roundtrip: PublicationExecutionMetadata =
            serde_json::from_value(json).expect("metadata should deserialize");
        assert_eq!(roundtrip, metadata);
        let route = roundtrip.task_route("reserve").expect("route should exist");
        assert_eq!(route.handler(), "inventory.reserve");
        assert_eq!(route.capability(), "task");
        assert_eq!(route.timeout_seconds(), Some(30));
        assert_eq!(
            roundtrip.semantics_version(),
            PublicationSemanticsVersion::V1
        );
        assert_eq!(roundtrip.task_routes().len(), 1);
    }

    #[test]
    fn rejects_oversized_route_timeout_for_decode_and_build_paths() {
        let oversized_timeout = MAX_TASK_TIMEOUT_SECONDS + 1;
        assert!(matches!(
            ResolvedTaskRoute::new(
                "inventory.reserve",
                "task",
                "orders",
                Some(oversized_timeout)
            ),
            Err(StoredDefinitionDecodeError::InvalidExecutionMetadata { .. })
        ));

        let forged_route = ResolvedTaskRoute {
            handler: "inventory.reserve".to_owned(),
            capability: "task".to_owned(),
            queue: "orders".to_owned(),
            timeout_seconds: Some(oversized_timeout),
        };
        assert!(matches!(
            PublicationExecutionMetadata::v1(BTreeMap::from([(
                "reserve".to_owned(),
                forged_route,
            )])),
            Err(StoredDefinitionDecodeError::InvalidExecutionMetadata { .. })
        ));

        let implicit_authoring_timeout =
            CURRENT_DEFINITION_JSON.replace(",\n        \"timeout_seconds\": 30", "");
        let oversized_snapshot = json!({
            "semantics_version": "v1",
            "task_routes": {
                "reserve": {
                    "handler": "inventory.reserve",
                    "capability": "task",
                    "queue": "orders",
                    "timeout_seconds": oversized_timeout
                }
            }
        })
        .to_string();
        assert!(matches!(
            DecodedStoredDefinition::decode(&implicit_authoring_timeout, Some(&oversized_snapshot)),
            Err(StoredDefinitionDecodeError::InvalidExecutionMetadata { .. })
        ));
    }
}
