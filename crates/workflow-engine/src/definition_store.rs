use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;
use workflow_definition::{
    validate_definition, FlowDefinition, ValidationError, MAX_TASK_TIMEOUT_SECONDS,
};
use workflow_worker::{Capability, HandlerRegistration, WorkerRegistry};

use crate::persistence::{
    HiqlitePersistence, LatestEligibleDefinitionSelection, PersistenceError,
    StoredDefinitionInspection,
};
use crate::stored_definition::{
    decode_stored_definition, PublicationExecutionMetadata, ResolvedTaskRoute,
};
use crate::time::now_unix_seconds;

const MAX_CONCURRENT_PUBLISH_RETRIES: usize = 8;
const PUBLICATION_HASH_DOMAIN_V1: &[u8] = b"conductor.workflow-publication.v1\0";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ValidationIssue {
    pub code: String,
    pub message: String,
}

impl From<ValidationError> for ValidationIssue {
    fn from(error: ValidationError) -> Self {
        match error {
            ValidationError::MissingTrigger { workflow_id } => Self {
                code: "missing_trigger".to_owned(),
                message: format!("workflow '{workflow_id}' must include a trigger"),
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
            ValidationError::TaskTimeoutOutOfRange {
                step_id,
                timeout_seconds,
                max_timeout_seconds,
            } => Self {
                code: "task_timeout_out_of_range".to_owned(),
                message: format!(
                    "step '{step_id}' has timeout_seconds '{timeout_seconds}', which exceeds the supported maximum '{max_timeout_seconds}'"
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
    pub latest_published: Option<StoredDefinitionInspection>,
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
    #[error(
        "workflow '{workflow_id}' has published definitions, but none are eligible for a new run"
    )]
    NoEligiblePublishedDefinition { workflow_id: String },
    #[error("invalid workflow definition: {error:?}")]
    InvalidDefinition { error: ValidationError },
    #[error("persistence error: {0}")]
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
    #[error(
        "draft for workflow references handler '{handler_id}' with timeout_seconds '{timeout_seconds}' above the supported maximum '{max_timeout_seconds}'"
    )]
    TaskTimeoutOutOfRange {
        handler_id: String,
        timeout_seconds: u64,
        max_timeout_seconds: u64,
    },
    #[error("concurrent publish contention for workflow '{workflow_id}' exceeded retry budget")]
    ConcurrentPublishContention { workflow_id: String },
    #[error("draft content for workflow '{workflow_id}' changed during publication")]
    DraftChangedDuringPublish { workflow_id: String },
    #[error(
        "workflow '{workflow_id}' has an invalid published version sequence spanning {minimum_version}..={maximum_version}"
    )]
    InvalidPublishedVersionSequence {
        workflow_id: String,
        minimum_version: i64,
        maximum_version: i64,
    },
    #[error(
        "workflow '{workflow_id}' has exhausted the supported published version range at {latest_version}"
    )]
    WorkflowVersionExhausted {
        workflow_id: String,
        latest_version: i64,
    },
    #[error("persistence error: {0}")]
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

    #[cfg(test)]
    pub(crate) async fn insert_raw_published_definition_for_test(
        &self,
        workflow_id: &str,
        workflow_version: i64,
        definition_json: &str,
        execution_metadata_json: Option<&str>,
    ) -> Result<(), WorkflowEngineError> {
        self.persistence
            .insert_raw_published_definition_for_test(
                workflow_id,
                workflow_version,
                definition_json,
                execution_metadata_json,
            )
            .await?;
        Ok(())
    }

    pub async fn replace_draft(
        &self,
        definition: FlowDefinition,
    ) -> Result<StoredWorkflowDefinition, WorkflowEngineError> {
        if let Err(error @ ValidationError::TaskTimeoutOutOfRange { .. }) =
            validate_definition(&definition)
        {
            return Err(WorkflowEngineError::InvalidDefinition { error });
        }

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
        let manifest_snapshot = self.registry.manifest_snapshot();
        let draft = self
            .validated_draft_for_publish(workflow_id, validated_content_hash)
            .await?;
        let execution_metadata =
            resolve_publication_execution_metadata(&draft.definition, &manifest_snapshot)?;

        self.publish_loaded_draft(workflow_id, draft, execution_metadata)
            .await
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
            content_hash: draft.content_hash,
        })
    }

    async fn publish_loaded_draft(
        &self,
        workflow_id: &str,
        draft: LoadedDraft,
        execution_metadata: PublicationExecutionMetadata,
    ) -> Result<StoredWorkflowDefinition, PublishDraftError> {
        let published_at = now_unix_seconds();
        let execution_metadata_json =
            serde_json::to_string(&execution_metadata).map_err(PersistenceError::from)?;
        let expected_draft_definition_json =
            serde_json::to_string(&draft.definition).map_err(PersistenceError::from)?;
        for _ in 0..MAX_CONCURRENT_PUBLISH_RETRIES {
            let workflow_version = match self.persistence.next_workflow_version(workflow_id).await {
                Ok(workflow_version) => workflow_version,
                Err(PersistenceError::InvalidWorkflowVersionSequence {
                    minimum_version,
                    maximum_version,
                    ..
                }) => {
                    return Err(PublishDraftError::InvalidPublishedVersionSequence {
                        workflow_id: workflow_id.to_owned(),
                        minimum_version,
                        maximum_version,
                    });
                }
                Err(PersistenceError::WorkflowVersionExhausted { latest_version, .. }) => {
                    return Err(PublishDraftError::WorkflowVersionExhausted {
                        workflow_id: workflow_id.to_owned(),
                        latest_version,
                    });
                }
                Err(error) => return Err(error.into()),
            };
            let mut published_definition = draft.definition.clone();
            published_definition.workflow_version = workflow_version;
            let definition_json =
                serde_json::to_string(&published_definition).map_err(PersistenceError::from)?;
            decode_stored_definition(&definition_json, Some(&execution_metadata_json)).map_err(
                |error| {
                    PersistenceError::Invariant(format!(
                        "refusing to publish invalid immutable definition envelope: {error}"
                    ))
                },
            )?;
            let published_content_hash =
                publication_content_hash(&definition_json, &execution_metadata_json);

            let published = StoredWorkflowDefinition {
                workflow_id: workflow_id.to_owned(),
                workflow_version,
                definition: published_definition,
                content_hash: published_content_hash,
                created_at: published_at,
                published_at: Some(published_at),
            };

            match self
                .persistence
                .insert_snapshotted_published_definition_if_draft_validated(
                    &published,
                    &execution_metadata_json,
                    &expected_draft_definition_json,
                    &draft.content_hash,
                )
                .await
            {
                Ok(true) => return Ok(published),
                Ok(false) => {
                    return Err(PublishDraftError::DraftChangedDuringPublish {
                        workflow_id: workflow_id.to_owned(),
                    });
                }
                Err(error) if is_publish_version_conflict(&error) => continue,
                Err(error) => return Err(error.into()),
            }
        }

        Err(PublishDraftError::ConcurrentPublishContention {
            workflow_id: workflow_id.to_owned(),
        })
    }

    pub async fn latest_published_definition(
        &self,
        workflow_id: &str,
    ) -> Result<Option<StoredDefinitionInspection>, WorkflowEngineError> {
        Ok(self
            .persistence
            .inspect_latest_published_definition(workflow_id)
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
    ) -> Result<Vec<StoredDefinitionInspection>, WorkflowEngineError> {
        Ok(self
            .persistence
            .inspect_published_definitions(workflow_id)
            .await?)
    }

    pub(crate) async fn list_published_versions_page(
        &self,
        workflow_id: &str,
        after_version: Option<i64>,
        limit: u32,
    ) -> Result<Vec<StoredDefinitionInspection>, WorkflowEngineError> {
        Ok(self
            .persistence
            .inspect_published_definitions_page(workflow_id, after_version, limit)
            .await?)
    }

    pub async fn published_definition(
        &self,
        workflow_id: &str,
        workflow_version: i64,
    ) -> Result<Option<StoredDefinitionInspection>, WorkflowEngineError> {
        Ok(self
            .persistence
            .inspect_published_definition(workflow_id, workflow_version)
            .await?)
    }

    pub async fn create_run_for_latest_published(
        &self,
        run_id: &str,
        workflow_id: &str,
    ) -> Result<WorkflowRunRecord, WorkflowEngineError> {
        let latest = match self
            .persistence
            .latest_eligible_published_definition(workflow_id)
            .await?
        {
            LatestEligibleDefinitionSelection::Selected(definition) => definition,
            LatestEligibleDefinitionSelection::NoPublishedDefinitions => {
                return Err(WorkflowEngineError::PublishedDefinitionNotFound {
                    workflow_id: workflow_id.to_owned(),
                });
            }
            LatestEligibleDefinitionSelection::NoEligiblePublishedDefinition => {
                return Err(WorkflowEngineError::NoEligiblePublishedDefinition {
                    workflow_id: workflow_id.to_owned(),
                });
            }
        };
        let executable = latest.executable();

        let run = WorkflowRunRecord {
            run_id: run_id.to_owned(),
            workflow_id: workflow_id.to_owned(),
            workflow_version: executable.workflow_version,
            status: "created".to_owned(),
            cancel_reason: None,
            cancel_requested_by: None,
            cancel_requested_at: None,
            created_at: now_unix_seconds(),
        };

        self.persistence.insert_run_strict(&run).await?;
        Ok(run)
    }

    pub async fn get_run(
        &self,
        run_id: &str,
    ) -> Result<Option<WorkflowRunRecord>, WorkflowEngineError> {
        Ok(self.persistence.get_run(run_id).await?)
    }
}

fn is_publish_version_conflict(error: &PersistenceError) -> bool {
    matches!(
        error,
        PersistenceError::Hiqlite(hiqlite::Error::ConstraintViolation(_))
    )
}

struct LoadedDraft {
    definition: FlowDefinition,
    content_hash: String,
}

fn resolve_publication_execution_metadata(
    definition: &FlowDefinition,
    manifest_snapshot: &BTreeMap<String, HandlerRegistration>,
) -> Result<PublicationExecutionMetadata, PublishDraftError> {
    let mut task_routes = BTreeMap::new();
    for step in &definition.steps {
        if let Some(task) = step.metadata.task.as_ref() {
            let Some(registration) = manifest_snapshot.get(task.handler.as_str()) else {
                return Err(PublishDraftError::HandlerManifestMismatch {
                    handler_id: task.handler.clone(),
                    capability: task
                        .capability
                        .clone()
                        .unwrap_or_else(|| Capability::Task.as_str().to_owned()),
                });
            };

            if registration.capability != Capability::Task {
                return Err(PublishDraftError::HandlerManifestMismatch {
                    handler_id: task.handler.clone(),
                    capability: Capability::Task.as_str().to_owned(),
                });
            }

            if let Some(explicit_capability) = task.capability.as_ref() {
                if explicit_capability != registration.capability.as_str() {
                    return Err(PublishDraftError::HandlerManifestMismatch {
                        handler_id: task.handler.clone(),
                        capability: explicit_capability.clone(),
                    });
                }
            }

            let timeout_seconds = task.timeout_seconds.or(registration.timeout_seconds);
            if let Some(timeout_seconds) = timeout_seconds {
                if timeout_seconds > MAX_TASK_TIMEOUT_SECONDS {
                    return Err(PublishDraftError::TaskTimeoutOutOfRange {
                        handler_id: task.handler.clone(),
                        timeout_seconds,
                        max_timeout_seconds: MAX_TASK_TIMEOUT_SECONDS,
                    });
                }
            }

            let capability = task
                .capability
                .clone()
                .unwrap_or_else(|| registration.capability.as_str().to_owned());
            let queue = task
                .queue
                .clone()
                .unwrap_or_else(|| registration.default_queue.clone());
            let route =
                ResolvedTaskRoute::new(task.handler.clone(), capability, queue, timeout_seconds)
                    .map_err(|error| {
                        PersistenceError::Invariant(format!(
                            "failed to construct publication route for step '{}': {error}",
                            step.id
                        ))
                    })?;
            task_routes.insert(step.id.clone(), route);
        }
    }

    PublicationExecutionMetadata::v1(task_routes).map_err(|error| {
        PersistenceError::Invariant(format!(
            "failed to construct publication execution metadata: {error}"
        ))
        .into()
    })
}

fn content_hash(definition: &FlowDefinition) -> Result<String, PersistenceError> {
    let json = serde_json::to_vec(definition)?;
    let mut hasher = Sha256::new();
    hasher.update(json);
    Ok(format!("{:x}", hasher.finalize()))
}

fn publication_content_hash(definition_json: &str, execution_metadata_json: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(PUBLICATION_HASH_DOMAIN_V1);
    hash_length_delimited(&mut hasher, definition_json.as_bytes());
    hash_length_delimited(&mut hasher, execution_metadata_json.as_bytes());
    format!("{:x}", hasher.finalize())
}

fn hash_length_delimited(hasher: &mut Sha256, bytes: &[u8]) {
    let length = u64::try_from(bytes.len()).expect("publication JSON length must fit in u64");
    hasher.update(length.to_be_bytes());
    hasher.update(bytes);
}

#[cfg(test)]
mod tests {
    use serde_json::{json, Value};
    use tempfile::TempDir;

    use super::*;

    #[tokio::test]
    async fn publication_snapshot_is_complete_and_immutable_after_registry_mutation() {
        let tempdir = TempDir::new().expect("temporary directory should exist");
        let persistence = HiqlitePersistence::for_test(tempdir.path())
            .await
            .expect("persistence should start");
        let registry = WorkerRegistry::new();
        registry
            .define_handler("orders.reserve")
            .capability(Capability::Task)
            .default_queue("registry-reserve-v1")
            .timeout_seconds(30);
        registry
            .define_handler("orders.notify")
            .capability(Capability::Task)
            .default_queue("orders-v1");
        let store = DefinitionStore::with_registry(persistence.clone(), registry.clone());

        let draft = store
            .replace_draft(publication_test_definition())
            .await
            .expect("draft should replace");
        let validation = store
            .validate_draft("orders")
            .await
            .expect("draft should validate");
        assert!(validation.ok);
        assert_eq!(validation.content_hash, draft.content_hash);

        let published_v1 = store
            .publish_draft("orders", &validation.content_hash)
            .await
            .expect("v1 should publish");
        let inspected_v1 = persistence
            .inspect_published_definition("orders", 1)
            .await
            .expect("v1 inspection should succeed")
            .expect("v1 should exist");
        let v1_metadata = inspected_metadata(&inspected_v1.execution_metadata_json);
        assert_eq!(
            v1_metadata,
            json!({
                "semantics_version": "v1",
                "task_routes": {
                    "notify": {
                        "handler": "orders.notify",
                        "capability": "task",
                        "queue": "orders-v1",
                        "timeout_seconds": null
                    },
                    "reserve": {
                        "handler": "orders.reserve",
                        "capability": "task",
                        "queue": "priority",
                        "timeout_seconds": 45
                    }
                }
            })
        );

        registry
            .define_handler("orders.reserve")
            .capability(Capability::Task)
            .default_queue("registry-reserve-v2")
            .timeout_seconds(90);
        registry
            .define_handler("orders.notify")
            .capability(Capability::Task)
            .default_queue("orders-v2")
            .timeout_seconds(60);

        let reloaded_v1 = persistence
            .inspect_published_definition("orders", 1)
            .await
            .expect("v1 reinspection should succeed")
            .expect("v1 should remain retained");
        assert_eq!(
            inspected_metadata(&reloaded_v1.execution_metadata_json),
            v1_metadata
        );

        let published_v2 = store
            .publish_draft("orders", &validation.content_hash)
            .await
            .expect("v2 should publish");
        assert_eq!(published_v2.workflow_version, 2);
        assert_ne!(published_v2.content_hash, published_v1.content_hash);
        let inspected_v2 = persistence
            .inspect_published_definition("orders", 2)
            .await
            .expect("v2 inspection should succeed")
            .expect("v2 should exist");
        assert_eq!(
            inspected_metadata(&inspected_v2.execution_metadata_json),
            json!({
                "semantics_version": "v1",
                "task_routes": {
                    "notify": {
                        "handler": "orders.notify",
                        "capability": "task",
                        "queue": "orders-v2",
                        "timeout_seconds": 60
                    },
                    "reserve": {
                        "handler": "orders.reserve",
                        "capability": "task",
                        "queue": "priority",
                        "timeout_seconds": 45
                    }
                }
            })
        );
    }

    #[tokio::test]
    async fn publish_rejects_a_draft_replaced_after_validation_snapshot() {
        let tempdir = TempDir::new().expect("temporary directory should exist");
        let persistence = HiqlitePersistence::for_test(tempdir.path())
            .await
            .expect("persistence should start");
        let registry = publication_test_registry();
        let store = DefinitionStore::with_registry(persistence.clone(), registry.clone());

        store
            .replace_draft(publication_test_definition())
            .await
            .expect("draft should replace");
        let validation = store
            .validate_draft("orders")
            .await
            .expect("draft should validate");
        assert!(validation.ok);
        let loaded = store
            .validated_draft_for_publish("orders", &validation.content_hash)
            .await
            .expect("validated snapshot should load");
        let metadata = resolve_publication_execution_metadata(
            &loaded.definition,
            &registry.manifest_snapshot(),
        )
        .expect("snapshot routes should resolve");

        let mut replacement = publication_test_definition();
        replacement.steps[0]
            .metadata
            .task
            .as_mut()
            .expect("reserve should remain a task")
            .queue = Some("replacement-queue".to_owned());
        let replacement = store
            .replace_draft(replacement)
            .await
            .expect("concurrent draft replacement should persist");

        let error = store
            .publish_loaded_draft("orders", loaded, metadata)
            .await
            .expect_err("stale validated snapshot must not publish");
        assert!(matches!(
            error,
            PublishDraftError::DraftChangedDuringPublish { ref workflow_id }
                if workflow_id == "orders"
        ));
        assert!(persistence
            .inspect_published_definitions("orders")
            .await
            .expect("published inspection should succeed")
            .is_empty());
        let live = store
            .draft_definition("orders")
            .await
            .expect("draft lookup should succeed")
            .expect("replacement should remain live");
        assert_eq!(live.content_hash, replacement.content_hash);
    }

    #[tokio::test]
    async fn publish_rejects_invalid_or_exhausted_retained_version_sequences() {
        let tempdir = TempDir::new().expect("temporary directory should exist");
        let persistence = HiqlitePersistence::for_test(tempdir.path())
            .await
            .expect("persistence should start");
        let store =
            DefinitionStore::with_registry(persistence.clone(), publication_test_registry());
        let cases = [
            ("negative-version", -1_i64, "invalid"),
            ("max-version", i64::from(u32::MAX), "exhausted"),
            ("overflow-version", i64::from(u32::MAX) + 1, "invalid"),
        ];

        for (workflow_id, stored_version, expected) in cases {
            let mut definition = publication_test_definition();
            definition.workflow_id = workflow_id.to_owned();
            store
                .replace_draft(definition)
                .await
                .expect("draft should replace");
            let validation = store
                .validate_draft(workflow_id)
                .await
                .expect("draft should validate");
            assert!(validation.ok);
            persistence
                .insert_raw_published_definition_for_test(workflow_id, stored_version, "{}", None)
                .await
                .expect("retained malformed-version fixture should insert");

            let error = store
                .publish_draft(workflow_id, &validation.content_hash)
                .await
                .expect_err("invalid retained version sequence must block publication");
            match expected {
                "exhausted" => assert!(matches!(
                    error,
                    PublishDraftError::WorkflowVersionExhausted {
                        ref workflow_id,
                        latest_version,
                    } if workflow_id == "max-version" && latest_version == i64::from(u32::MAX)
                )),
                "invalid" => assert!(matches!(
                    error,
                    PublishDraftError::InvalidPublishedVersionSequence {
                        workflow_id: ref actual_workflow_id,
                        minimum_version,
                        maximum_version,
                    } if actual_workflow_id == workflow_id
                        && minimum_version == stored_version
                        && maximum_version == stored_version
                )),
                _ => unreachable!("test fixture has a known expected classification"),
            }
            let retained = persistence
                .inspect_published_definitions(workflow_id)
                .await
                .expect("retained rows should remain inspectable");
            assert_eq!(retained.len(), 1);
            assert_eq!(retained[0].workflow_version, stored_version);
        }
    }

    #[tokio::test]
    async fn direct_start_distinguishes_missing_from_only_ineligible_publications() {
        let tempdir = TempDir::new().expect("temporary directory should exist");
        let persistence = HiqlitePersistence::for_test(tempdir.path())
            .await
            .expect("persistence should start");
        let store = DefinitionStore::new(persistence.clone());

        let missing = store
            .create_run_for_latest_published("run-missing", "missing")
            .await
            .expect_err("workflow without publications should be not found");
        assert!(matches!(
            missing,
            WorkflowEngineError::PublishedDefinitionNotFound { ref workflow_id }
                if workflow_id == "missing"
        ));

        let mut pre_snapshot = publication_test_definition();
        pre_snapshot.workflow_version = 1;
        persistence
            .insert_published_definition(&StoredWorkflowDefinition {
                workflow_id: "orders".to_owned(),
                workflow_version: 1,
                definition: pre_snapshot,
                content_hash: "orders-v1".to_owned(),
                created_at: 1,
                published_at: Some(1),
            })
            .await
            .expect("pre-snapshot publication should insert");
        let unsupported = json!({
            "workflow_id": "orders",
            "workflow_version": 2,
            "defaults": {},
            "trigger": {"type": "event", "event_type": "order.created"},
            "steps": [{
                "id": "wait_for_payment",
                "kind": "wait_for_event",
                "wait_for_event": {"event_type": "payment.received"}
            }],
            "edges": []
        })
        .to_string();
        persistence
            .insert_raw_published_definition_for_test("orders", 2, &unsupported, None)
            .await
            .expect("unsupported publication should insert");

        let ineligible = store
            .create_run_for_latest_published("run-ineligible", "orders")
            .await
            .expect_err("ineligible publications must not start a run");
        assert!(matches!(
            ineligible,
            WorkflowEngineError::NoEligiblePublishedDefinition { ref workflow_id }
                if workflow_id == "orders"
        ));
        assert!(store
            .get_run("run-ineligible")
            .await
            .expect("run lookup should succeed")
            .is_none());
    }

    fn inspected_metadata(raw: &Option<String>) -> Value {
        serde_json::from_str(
            raw.as_deref()
                .expect("snapshotted publication should persist execution metadata"),
        )
        .expect("execution metadata should be valid JSON")
    }

    fn publication_test_definition() -> FlowDefinition {
        serde_json::from_value(json!({
            "workflow_id": "orders",
            "workflow_version": 0,
            "defaults": {},
            "trigger": {"type": "event", "event_type": "order.created"},
            "steps": [
                {
                    "id": "reserve",
                    "kind": "task",
                    "task": {
                        "handler": "orders.reserve",
                        "capability": "task",
                        "queue": "priority",
                        "timeout_seconds": 45
                    }
                },
                {
                    "id": "notify",
                    "kind": "task",
                    "task": {"handler": "orders.notify"}
                }
            ],
            "edges": []
        }))
        .expect("publication test definition should deserialize")
    }

    fn publication_test_registry() -> WorkerRegistry {
        let registry = WorkerRegistry::new();
        registry
            .define_handler("orders.reserve")
            .capability(Capability::Task)
            .default_queue("registry-reserve-v1")
            .timeout_seconds(30);
        registry
            .define_handler("orders.notify")
            .capability(Capability::Task)
            .default_queue("orders-v1");
        registry
    }
}
