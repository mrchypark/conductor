use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;
use workflow_definition::{validate_definition, FlowDefinition, ValidationError};
use workflow_worker::{Capability, WorkerRegistry};

use crate::persistence::{HiqlitePersistence, PersistenceError};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ValidationIssue {
    pub code: String,
    pub message: String,
}

impl From<ValidationError> for ValidationIssue {
    fn from(error: ValidationError) -> Self {
        match error {
            ValidationError::MissingTrigger => Self {
                code: "missing_trigger".to_owned(),
                message: "workflow definitions must include a trigger".to_owned(),
            },
            ValidationError::DuplicateStepId { step_id } => Self {
                code: "duplicate_step_id".to_owned(),
                message: format!("step id '{step_id}' is duplicated"),
            },
            ValidationError::MissingRequiredStepMetadata {
                step_id,
                kind,
                metadata_field,
            } => Self {
                code: "missing_required_step_metadata".to_owned(),
                message: format!(
                    "step '{step_id}' with kind '{kind:?}' is missing '{metadata_field}' metadata"
                ),
            },
            ValidationError::UnknownEdgeSource { edge_from, edge_to } => Self {
                code: "unknown_edge_source".to_owned(),
                message: format!("edge '{edge_from}' -> '{edge_to}' has an unknown source"),
            },
            ValidationError::UnknownEdgeTarget { edge_from, edge_to } => Self {
                code: "unknown_edge_target".to_owned(),
                message: format!("edge '{edge_from}' -> '{edge_to}' has an unknown target"),
            },
            ValidationError::UnsupportedStepKindMetadata {
                step_id,
                kind,
                metadata_field,
            } => Self {
                code: "unsupported_step_kind_metadata".to_owned(),
                message: format!(
                    "step '{step_id}' with kind '{kind:?}' cannot use '{metadata_field}' metadata"
                ),
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DefinitionValidation {
    pub ok: bool,
    pub errors: Vec<ValidationIssue>,
    pub warnings: Vec<ValidationIssue>,
    pub content_hash: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredWorkflowDefinition {
    pub workflow_id: String,
    pub workflow_version: u32,
    pub definition: FlowDefinition,
    pub content_hash: String,
    pub created_at: i64,
    pub published_at: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowSummary {
    pub workflow_id: String,
    pub draft: Option<StoredWorkflowDefinition>,
    pub latest_published: Option<StoredWorkflowDefinition>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowRunRecord {
    pub run_id: String,
    pub workflow_id: String,
    pub workflow_version: u32,
    pub status: String,
    pub cancel_reason: Option<String>,
    pub cancel_requested_by: Option<String>,
    pub cancel_requested_at: Option<i64>,
    pub created_at: i64,
}

#[derive(Debug, Error)]
pub enum WorkflowEngineError {
    #[error("no draft exists for workflow '{workflow_id}'")]
    DraftNotFound { workflow_id: String },
    #[error("no published definition exists for workflow '{workflow_id}'")]
    PublishedDefinitionNotFound { workflow_id: String },
    #[error(transparent)]
    Persistence(#[from] PersistenceError),
}

#[derive(Debug, Error)]
pub enum PublishDraftError {
    #[error("no draft exists for workflow '{workflow_id}'")]
    DraftNotFound { workflow_id: String },
    #[error(
        "draft content for workflow '{workflow_id}' changed after validation: expected hash {expected_content_hash}, found {actual_content_hash}"
    )]
    StaleDraftContent {
        workflow_id: String,
        expected_content_hash: String,
        actual_content_hash: String,
    },
    #[error("draft for workflow '{workflow_id}' has not been successfully validated")]
    DraftNotValidated { workflow_id: String },
    #[error(
        "draft for workflow references handler '{handler_id}' without required capability '{capability}' in the worker manifest"
    )]
    HandlerManifestMismatch {
        handler_id: String,
        capability: String,
    },
    #[error(transparent)]
    Persistence(#[from] PersistenceError),
}

#[derive(Clone)]
pub struct DefinitionStore {
    persistence: HiqlitePersistence,
    registry: WorkerRegistry,
}

impl DefinitionStore {
    pub fn new(persistence: HiqlitePersistence) -> Self {
        Self {
            persistence,
            registry: WorkerRegistry::new(),
        }
    }

    pub fn with_registry(persistence: HiqlitePersistence, registry: WorkerRegistry) -> Self {
        Self {
            persistence,
            registry,
        }
    }

    pub async fn replace_draft(
        &self,
        definition: FlowDefinition,
    ) -> Result<StoredWorkflowDefinition, WorkflowEngineError> {
        let content_hash = content_hash(&definition)?;
        let now = now_unix_seconds();
        let record = StoredWorkflowDefinition {
            workflow_id: definition.workflow_id.clone(),
            workflow_version: 0,
            definition,
            content_hash,
            created_at: now,
            published_at: None,
        };

        self.persistence.replace_draft(&record).await?;
        Ok(record)
    }

    pub async fn validate_draft(
        &self,
        workflow_id: &str,
    ) -> Result<DefinitionValidation, WorkflowEngineError> {
        let draft = self
            .persistence
            .get_draft(workflow_id)
            .await?
            .ok_or_else(|| WorkflowEngineError::DraftNotFound {
                workflow_id: workflow_id.to_owned(),
            })?;

        let validation = match validate_definition(&draft.definition) {
            Ok(()) => DefinitionValidation {
                ok: true,
                errors: Vec::new(),
                warnings: Vec::new(),
                content_hash: draft.content_hash.clone(),
            },
            Err(error) => DefinitionValidation {
                ok: false,
                errors: vec![ValidationIssue::from(error)],
                warnings: Vec::new(),
                content_hash: draft.content_hash.clone(),
            },
        };

        self.persistence
            .record_draft_validation(
                workflow_id,
                validation.ok.then_some(validation.content_hash.as_str()),
            )
            .await?;

        Ok(validation)
    }

    pub async fn publish_draft(
        &self,
        workflow_id: &str,
        validated_content_hash: &str,
    ) -> Result<StoredWorkflowDefinition, PublishDraftError> {
        let draft = self
            .validated_draft_for_publish(workflow_id, validated_content_hash)
            .await?;
        validate_task_handlers(&draft.definition, &self.registry)?;

        self.publish_loaded_draft(workflow_id, draft).await
    }

    async fn validated_draft_for_publish(
        &self,
        workflow_id: &str,
        validated_content_hash: &str,
    ) -> Result<LoadedDraft, PublishDraftError> {
        let draft = self
            .persistence
            .get_draft(workflow_id)
            .await?
            .ok_or_else(|| PublishDraftError::DraftNotFound {
                workflow_id: workflow_id.to_owned(),
            })?;

        if draft.content_hash != validated_content_hash {
            return Err(PublishDraftError::StaleDraftContent {
                workflow_id: workflow_id.to_owned(),
                expected_content_hash: validated_content_hash.to_owned(),
                actual_content_hash: draft.content_hash,
            });
        }

        if draft.last_validated_content_hash.as_deref() != Some(validated_content_hash) {
            return Err(PublishDraftError::DraftNotValidated {
                workflow_id: workflow_id.to_owned(),
            });
        }

        Ok(LoadedDraft {
            definition: draft.definition,
        })
    }

    async fn publish_loaded_draft(
        &self,
        workflow_id: &str,
        draft: LoadedDraft,
    ) -> Result<StoredWorkflowDefinition, PublishDraftError> {
        let workflow_version = self.persistence.next_workflow_version(workflow_id).await?;
        let published_at = now_unix_seconds();
        let mut published_definition = draft.definition;
        published_definition.workflow_version = workflow_version;
        let published_content_hash = content_hash(&published_definition)?;

        let published = StoredWorkflowDefinition {
            workflow_id: workflow_id.to_owned(),
            workflow_version,
            definition: published_definition,
            content_hash: published_content_hash,
            created_at: published_at,
            published_at: Some(published_at),
        };

        self.persistence
            .insert_published_definition(&published)
            .await?;
        Ok(published)
    }

    pub async fn latest_published_definition(
        &self,
        workflow_id: &str,
    ) -> Result<Option<StoredWorkflowDefinition>, WorkflowEngineError> {
        Ok(self
            .persistence
            .latest_published_definition(workflow_id)
            .await?)
    }

    pub async fn draft_definition(
        &self,
        workflow_id: &str,
    ) -> Result<Option<StoredWorkflowDefinition>, WorkflowEngineError> {
        Ok(self
            .persistence
            .get_draft(workflow_id)
            .await?
            .map(|draft| StoredWorkflowDefinition {
                workflow_id: workflow_id.to_owned(),
                workflow_version: 0,
                definition: draft.definition,
                content_hash: draft.content_hash,
                created_at: draft.created_at,
                published_at: None,
            }))
    }

    pub async fn list_published_versions(
        &self,
        workflow_id: &str,
    ) -> Result<Vec<StoredWorkflowDefinition>, WorkflowEngineError> {
        Ok(self
            .persistence
            .list_published_versions(workflow_id)
            .await?)
    }

    pub async fn published_definition(
        &self,
        workflow_id: &str,
        workflow_version: u32,
    ) -> Result<Option<StoredWorkflowDefinition>, WorkflowEngineError> {
        Ok(self
            .persistence
            .get_published_definition(workflow_id, workflow_version)
            .await?)
    }

    pub async fn create_run_for_latest_published(
        &self,
        run_id: &str,
        workflow_id: &str,
    ) -> Result<WorkflowRunRecord, WorkflowEngineError> {
        let latest = self
            .persistence
            .latest_published_definition(workflow_id)
            .await?
            .ok_or_else(|| WorkflowEngineError::PublishedDefinitionNotFound {
                workflow_id: workflow_id.to_owned(),
            })?;

        let run = WorkflowRunRecord {
            run_id: run_id.to_owned(),
            workflow_id: workflow_id.to_owned(),
            workflow_version: latest.workflow_version,
            status: "created".to_owned(),
            cancel_reason: None,
            cancel_requested_by: None,
            cancel_requested_at: None,
            created_at: now_unix_seconds(),
        };

        self.persistence.insert_run(&run).await?;
        Ok(run)
    }

    pub async fn get_run(
        &self,
        run_id: &str,
    ) -> Result<Option<WorkflowRunRecord>, WorkflowEngineError> {
        Ok(self.persistence.get_run(run_id).await?)
    }
}

struct LoadedDraft {
    definition: FlowDefinition,
}

fn validate_task_handlers(
    definition: &FlowDefinition,
    registry: &WorkerRegistry,
) -> Result<(), PublishDraftError> {
    for step in &definition.steps {
        if let Some(task) = step.metadata.task.as_ref() {
            let registration = registry.registration(task.handler.as_str());
            if !matches!(
                registration,
                Some(entry) if entry.capability == Capability::Task
            ) {
                return Err(PublishDraftError::HandlerManifestMismatch {
                    handler_id: task.handler.clone(),
                    capability: Capability::Task.as_str().to_owned(),
                });
            }
        }
    }

    Ok(())
}

fn content_hash(definition: &FlowDefinition) -> Result<String, PersistenceError> {
    let json = serde_json::to_vec(definition)?;
    let mut hasher = Sha256::new();
    hasher.update(json);
    Ok(format!("{:x}", hasher.finalize()))
}

fn now_unix_seconds() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock should be after unix epoch")
        .as_secs() as i64
}
