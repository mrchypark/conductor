use async_trait::async_trait;
use axum::Router;
use serde_json::Value;
use std::path::Path;
use workflow_protocol::{CompleteTask, FailTask, HeartbeatLease, PollTask, PollTaskResponse};
use workflow_worker::{TaskClientError, TaskExecutionClient};

use crate::{
    api,
    definition_store::{DefinitionStore, PublishDraftError, WorkflowEngineError, WorkflowSummary},
    persistence::HiqlitePersistence,
    runtime::{
        EventEnvelope, EventIngestResult, StartedRun, StepInstanceRecord, TaskRuntime,
        TaskRuntimeError, TransitionRecord, WaiterRecord,
    },
    DefinitionValidation, StoredWorkflowDefinition, WorkflowRunRecord,
};
use workflow_definition::FlowDefinition;
use workflow_worker::WorkerRegistry;

#[derive(Clone)]
pub struct WorkflowEngineApp {
    runtime: TaskRuntime,
    definitions: DefinitionStore,
    registry: WorkerRegistry,
}

impl WorkflowEngineApp {
    pub async fn for_test(base_dir: &Path) -> Result<Self, WorkflowEngineError> {
        let registry = WorkerRegistry::new();
        let persistence = HiqlitePersistence::for_test(base_dir).await?;
        let definitions = DefinitionStore::with_registry(persistence.clone(), registry.clone());
        let runtime = TaskRuntime::new(persistence, registry.clone());
        Ok(Self {
            runtime,
            definitions,
            registry,
        })
    }

    pub async fn for_test_with_registry(
        base_dir: &Path,
        registry: WorkerRegistry,
    ) -> Result<Self, WorkflowEngineError> {
        let persistence = HiqlitePersistence::for_test(base_dir).await?;
        let definitions = DefinitionStore::with_registry(persistence.clone(), registry.clone());
        let runtime = TaskRuntime::new(persistence, registry.clone());
        Ok(Self {
            runtime,
            definitions,
            registry,
        })
    }

    pub fn worker_registry(&self) -> WorkerRegistry {
        self.registry.clone()
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

    pub async fn latest_published_definition(
        &self,
        workflow_id: &str,
    ) -> Result<Option<StoredWorkflowDefinition>, WorkflowEngineError> {
        self.definitions
            .latest_published_definition(workflow_id)
            .await
    }

    pub async fn draft_definition(
        &self,
        workflow_id: &str,
    ) -> Result<Option<StoredWorkflowDefinition>, WorkflowEngineError> {
        self.definitions.draft_definition(workflow_id).await
    }

    pub async fn list_published_versions(
        &self,
        workflow_id: &str,
    ) -> Result<Vec<StoredWorkflowDefinition>, WorkflowEngineError> {
        self.definitions.list_published_versions(workflow_id).await
    }

    pub async fn published_definition(
        &self,
        workflow_id: &str,
        workflow_version: u32,
    ) -> Result<Option<StoredWorkflowDefinition>, WorkflowEngineError> {
        self.definitions
            .published_definition(workflow_id, workflow_version)
            .await
    }

    pub async fn workflow_summary(
        &self,
        workflow_id: &str,
    ) -> Result<WorkflowSummary, WorkflowEngineError> {
        Ok(WorkflowSummary {
            workflow_id: workflow_id.to_owned(),
            draft: self.draft_definition(workflow_id).await?,
            latest_published: self.latest_published_definition(workflow_id).await?,
        })
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

    pub async fn ingest_event(
        &self,
        event_type: &str,
        payload: Value,
    ) -> Result<Vec<StartedRun>, TaskRuntimeError> {
        self.runtime.ingest_event(event_type, payload).await
    }

    pub async fn ingest_events(
        &self,
        events: Vec<EventEnvelope>,
    ) -> Result<Vec<EventIngestResult>, TaskRuntimeError> {
        self.runtime.ingest_events(events).await
    }

    pub async fn poll_task(&self, request: PollTask) -> Result<PollTaskResponse, TaskRuntimeError> {
        self.runtime.poll_task(request).await
    }

    pub async fn heartbeat_lease(&self, request: HeartbeatLease) -> Result<(), TaskRuntimeError> {
        self.runtime.heartbeat_lease(request).await
    }

    pub async fn complete_task(&self, request: CompleteTask) -> Result<(), TaskRuntimeError> {
        self.runtime.complete_task(request).await
    }

    pub async fn fail_task(&self, request: FailTask) -> Result<(), TaskRuntimeError> {
        self.runtime.fail_task(request).await
    }

    pub async fn list_step_instances(
        &self,
        run_id: &str,
    ) -> Result<Vec<StepInstanceRecord>, TaskRuntimeError> {
        self.runtime.list_step_instances(run_id).await
    }

    pub async fn list_waiters(&self, run_id: &str) -> Result<Vec<WaiterRecord>, TaskRuntimeError> {
        self.runtime.list_waiters(run_id).await
    }

    pub async fn list_transitions(
        &self,
        run_id: &str,
    ) -> Result<Vec<TransitionRecord>, TaskRuntimeError> {
        self.runtime.list_transitions(run_id).await
    }

    pub async fn recovery_scan_at(&self, now: i64) -> Result<(), TaskRuntimeError> {
        self.runtime.recovery_scan_at(now).await
    }

    pub async fn cancel_run(
        &self,
        run_id: &str,
        reason: Option<&str>,
        requested_by: Option<&str>,
    ) -> Result<(), TaskRuntimeError> {
        self.runtime.cancel_run(run_id, reason, requested_by).await
    }

    pub fn router(&self) -> Router {
        api::router(self.clone())
    }
}

#[async_trait]
impl TaskExecutionClient for WorkflowEngineApp {
    async fn poll_task(&self, request: PollTask) -> Result<PollTaskResponse, TaskClientError> {
        self.poll_task(request)
            .await
            .map_err(|error| TaskClientError::Message {
                message: error.to_string(),
            })
    }

    async fn heartbeat_lease(&self, request: HeartbeatLease) -> Result<(), TaskClientError> {
        self.heartbeat_lease(request)
            .await
            .map_err(|error| TaskClientError::Message {
                message: error.to_string(),
            })
    }

    async fn complete_task(&self, request: CompleteTask) -> Result<(), TaskClientError> {
        self.complete_task(request)
            .await
            .map_err(|error| TaskClientError::Message {
                message: error.to_string(),
            })
    }

    async fn fail_task(&self, request: FailTask) -> Result<(), TaskClientError> {
        self.fail_task(request)
            .await
            .map_err(|error| TaskClientError::Message {
                message: error.to_string(),
            })
    }
}
