use std::collections::HashSet;

use crate::{FlowDefinition, StepDefinition, StepKind, MAX_TASK_TIMEOUT_SECONDS};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ValidationError {
    MissingTrigger {
        workflow_id: String,
    },
    DuplicateStepId {
        step_id: String,
    },
    MissingRequiredStepMetadata {
        step_id: String,
        kind: StepKind,
        metadata_field: String,
    },
    UnknownEdgeSource {
        edge_from: String,
        edge_to: String,
    },
    UnknownEdgeTarget {
        edge_from: String,
        edge_to: String,
    },
    UnsupportedStepKindMetadata {
        step_id: String,
        kind: StepKind,
        metadata_field: String,
    },
    TaskTimeoutOutOfRange {
        step_id: String,
        timeout_seconds: u64,
        max_timeout_seconds: u64,
    },
}

pub fn validate_definition(definition: &FlowDefinition) -> Result<(), ValidationError> {
    validate_task_timeouts(definition)?;

    if definition.trigger.is_none() {
        return Err(ValidationError::MissingTrigger {
            workflow_id: definition.workflow_id.clone(),
        });
    }

    let mut step_ids = HashSet::new();
    for step in &definition.steps {
        if !step_ids.insert(step.id.as_str()) {
            return Err(ValidationError::DuplicateStepId {
                step_id: step.id.clone(),
            });
        }

        validate_step_metadata(step)?;
    }

    for edge in &definition.edges {
        if !step_ids.contains(edge.from.as_str()) {
            return Err(ValidationError::UnknownEdgeSource {
                edge_from: edge.from.clone(),
                edge_to: edge.to.clone(),
            });
        }

        if !step_ids.contains(edge.to.as_str()) {
            return Err(ValidationError::UnknownEdgeTarget {
                edge_from: edge.from.clone(),
                edge_to: edge.to.clone(),
            });
        }
    }

    Ok(())
}

fn validate_task_timeouts(definition: &FlowDefinition) -> Result<(), ValidationError> {
    for step in &definition.steps {
        if step.kind != StepKind::Task {
            continue;
        }

        if let Some(timeout_seconds) = step
            .metadata
            .task
            .as_ref()
            .and_then(|task| task.timeout_seconds)
        {
            if timeout_seconds > MAX_TASK_TIMEOUT_SECONDS {
                return Err(ValidationError::TaskTimeoutOutOfRange {
                    step_id: step.id.clone(),
                    timeout_seconds,
                    max_timeout_seconds: MAX_TASK_TIMEOUT_SECONDS,
                });
            }
        }
    }

    Ok(())
}

fn validate_step_metadata(step: &StepDefinition) -> Result<(), ValidationError> {
    let (expected, present) = match step.kind {
        StepKind::Task => ("task", step.metadata.task.is_some()),
        StepKind::Sleep => ("sleep", step.metadata.sleep.is_some()),
        StepKind::Branch => ("branch", step.metadata.branch.is_some()),
    };

    if !present {
        return Err(ValidationError::MissingRequiredStepMetadata {
            step_id: step.id.clone(),
            kind: step.kind,
            metadata_field: expected.to_owned(),
        });
    }

    for metadata_field in [
        ("task", step.metadata.task.is_some()),
        ("sleep", step.metadata.sleep.is_some()),
        ("branch", step.metadata.branch.is_some()),
    ] {
        if metadata_field.1 && metadata_field.0 != expected {
            return Err(ValidationError::UnsupportedStepKindMetadata {
                step_id: step.id.clone(),
                kind: step.kind,
                metadata_field: metadata_field.0.to_owned(),
            });
        }
    }

    Ok(())
}
