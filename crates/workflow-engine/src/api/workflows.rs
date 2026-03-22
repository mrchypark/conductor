use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use serde::Deserialize;

use crate::{
    DefinitionValidation, PublishDraftError, StoredWorkflowDefinition, WorkflowEngineApp,
    WorkflowEngineError, WorkflowSummary,
};

#[derive(Debug, Deserialize)]
pub struct PublishDraftRequest {
    pub content_hash: String,
}

pub async fn put_draft(
    State(app): State<WorkflowEngineApp>,
    Path(workflow_id): Path<String>,
    Json(definition): Json<workflow_definition::FlowDefinition>,
) -> Result<Json<StoredWorkflowDefinition>, (StatusCode, String)> {
    if definition.workflow_id != workflow_id {
        return Err((
            StatusCode::BAD_REQUEST,
            "workflow_id in path and body must match".to_owned(),
        ));
    }
    app.replace_draft(definition)
        .await
        .map(Json)
        .map_err(internal_error)
}

pub async fn get_workflow(
    State(app): State<WorkflowEngineApp>,
    Path(workflow_id): Path<String>,
) -> Result<Json<WorkflowSummary>, (StatusCode, String)> {
    app.workflow_summary(&workflow_id)
        .await
        .map(Json)
        .map_err(internal_error)
}

pub async fn validate_draft(
    State(app): State<WorkflowEngineApp>,
    Path(workflow_id): Path<String>,
) -> Result<Json<DefinitionValidation>, (StatusCode, String)> {
    app.validate_draft(&workflow_id)
        .await
        .map(Json)
        .map_err(workflow_error)
}

pub async fn publish_draft(
    State(app): State<WorkflowEngineApp>,
    Path(workflow_id): Path<String>,
    Json(request): Json<PublishDraftRequest>,
) -> Result<Json<StoredWorkflowDefinition>, (StatusCode, String)> {
    app.publish_draft(&workflow_id, &request.content_hash)
        .await
        .map(Json)
        .map_err(publish_error)
}

pub async fn list_versions(
    State(app): State<WorkflowEngineApp>,
    Path(workflow_id): Path<String>,
) -> Result<Json<Vec<StoredWorkflowDefinition>>, (StatusCode, String)> {
    app.list_published_versions(&workflow_id)
        .await
        .map(Json)
        .map_err(internal_error)
}

pub async fn get_version(
    State(app): State<WorkflowEngineApp>,
    Path((workflow_id, workflow_version)): Path<(String, u32)>,
) -> Result<Json<StoredWorkflowDefinition>, (StatusCode, String)> {
    let definition = app
        .published_definition(&workflow_id, workflow_version)
        .await
        .map_err(internal_error)?;
    definition.map(Json).ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            "workflow version not found".to_owned(),
        )
    })
}

fn internal_error(error: impl ToString) -> (StatusCode, String) {
    (StatusCode::INTERNAL_SERVER_ERROR, error.to_string())
}

fn workflow_error(error: WorkflowEngineError) -> (StatusCode, String) {
    match error {
        WorkflowEngineError::DraftNotFound { .. }
        | WorkflowEngineError::PublishedDefinitionNotFound { .. } => {
            (StatusCode::NOT_FOUND, error.to_string())
        }
        other => internal_error(other),
    }
}

fn publish_error(error: PublishDraftError) -> (StatusCode, String) {
    match error {
        PublishDraftError::DraftNotFound { .. } => (StatusCode::NOT_FOUND, error.to_string()),
        PublishDraftError::DraftNotValidated { .. } => {
            (StatusCode::PRECONDITION_FAILED, error.to_string())
        }
        PublishDraftError::StaleDraftContent { .. }
        | PublishDraftError::HandlerManifestMismatch { .. } => {
            (StatusCode::CONFLICT, error.to_string())
        }
        other => internal_error(other),
    }
}
