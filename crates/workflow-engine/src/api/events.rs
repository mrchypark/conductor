use axum::{extract::State, http::StatusCode, Json};
use serde::Deserialize;
use serde_json::Value;

use crate::{EventEnvelope, EventIngestResult, WorkflowEngineApp};

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
        .map_err(internal_error)
}

fn internal_error(error: impl ToString) -> (StatusCode, String) {
    (StatusCode::INTERNAL_SERVER_ERROR, error.to_string())
}
