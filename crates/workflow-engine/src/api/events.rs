use axum::{extract::State, http::StatusCode, Json};
use serde::Deserialize;
use serde_json::Value;

use crate::{EventEnvelope, EventIngestResult, TaskRuntimeError, WorkflowEngineApp};

#[derive(Debug, Deserialize)]
pub struct EventsRequest {
    pub events: Vec<EventRequest>,
}

#[derive(Debug, Deserialize)]
pub struct EventRequest {
    pub id: Option<String>,
    pub name: String,
    pub idempotency_key: Option<String>,
    pub data: Value,
}

pub async fn post_events(
    State(app): State<WorkflowEngineApp>,
    Json(request): Json<EventsRequest>,
) -> Result<Json<Vec<EventIngestResult>>, (StatusCode, String)> {
    let envelopes = request
        .events
        .into_iter()
        .map(|event| EventEnvelope {
            id: event.id,
            name: event.name,
            idempotency_key: event.idempotency_key,
            data: event.data,
        })
        .collect();

    app.ingest_events(envelopes)
        .await
        .map(Json)
        .map_err(runtime_error)
}

fn runtime_error(error: TaskRuntimeError) -> (StatusCode, String) {
    match error {
        TaskRuntimeError::RunNotFound { .. } => (StatusCode::NOT_FOUND, "run not found".to_owned()),
        TaskRuntimeError::MissingHandlerMetadata { .. }
        | TaskRuntimeError::PublishedWorkflowVersionNotFound { .. }
        | TaskRuntimeError::UnsupportedStepKind { .. }
        | TaskRuntimeError::UnsupportedBranchExpression { .. } => {
            (StatusCode::CONFLICT, error.to_string())
        }
        other => internal_error(other),
    }
}

fn internal_error(error: impl ToString) -> (StatusCode, String) {
    (StatusCode::INTERNAL_SERVER_ERROR, error.to_string())
}
