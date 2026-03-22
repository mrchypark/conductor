use std::path::Path;

use crate::{
    definition_store::{DefinitionStore, PublishDraftError, WorkflowEngineError},
    persistence::HiqlitePersistence,
    DefinitionValidation, StoredWorkflowDefinition, WorkflowRunRecord,
};
use workflow_definition::FlowDefinition;
use workflow_worker::WorkerRegistry;

#[derive(Clone)]
pub struct WorkflowEngineApp {
    definitions: DefinitionStore,
}

impl WorkflowEngineApp {
    pub async fn for_test(base_dir: &Path) -> Result<Self, WorkflowEngineError> {
        let persistence = HiqlitePersistence::for_test(base_dir).await?;
        let definitions = DefinitionStore::new(persistence);
        Ok(Self { definitions })
    }

    pub async fn replace_draft(
        &self,
        definition: FlowDefinition,
    ) -> Result<StoredWorkflowDefinition, WorkflowEngineError> {
        self.definitions.replace_draft(definition).await
    }

    pub async fn validate_draft(
        &self,
        workflow_id: &str,
    ) -> Result<DefinitionValidation, WorkflowEngineError> {
        self.definitions.validate_draft(workflow_id).await
    }

    pub async fn publish_draft(
        &self,
        workflow_id: &str,
        validated_content_hash: &str,
    ) -> Result<StoredWorkflowDefinition, PublishDraftError> {
        self.definitions
            .publish_draft(workflow_id, validated_content_hash)
            .await
    }

    pub async fn publish_draft_with_registry(
        &self,
        workflow_id: &str,
        validated_content_hash: &str,
        registry: &WorkerRegistry,
    ) -> Result<StoredWorkflowDefinition, PublishDraftError> {
        self.definitions
            .publish_draft_with_registry(workflow_id, validated_content_hash, registry)
            .await
    }

    pub async fn latest_published_definition(
        &self,
        workflow_id: &str,
    ) -> Result<Option<StoredWorkflowDefinition>, WorkflowEngineError> {
        self.definitions
            .latest_published_definition(workflow_id)
            .await
    }

    pub async fn list_published_versions(
        &self,
        workflow_id: &str,
    ) -> Result<Vec<StoredWorkflowDefinition>, WorkflowEngineError> {
        self.definitions.list_published_versions(workflow_id).await
    }

    pub async fn create_run_for_latest_published(
        &self,
        run_id: &str,
        workflow_id: &str,
    ) -> Result<WorkflowRunRecord, WorkflowEngineError> {
        self.definitions
            .create_run_for_latest_published(run_id, workflow_id)
            .await
    }

    pub async fn get_run(
        &self,
        run_id: &str,
    ) -> Result<Option<WorkflowRunRecord>, WorkflowEngineError> {
        self.definitions.get_run(run_id).await
    }
}
