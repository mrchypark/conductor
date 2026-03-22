use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use serde::Deserialize;

use crate::{
    StepInstanceRecord, TaskRuntimeError, TransitionRecord, WaiterRecord, WorkflowEngineApp,
    WorkflowRunRecord,
};

#[derive(Debug, Deserialize)]
pub struct CancelRunRequest {
    pub reason: Option<String>,
    pub requested_by: Option<String>,
}

pub async fn get_run(
    State(app): State<WorkflowEngineApp>,
    Path(run_id): Path<String>,
) -> Result<Json<WorkflowRunRecord>, (StatusCode, String)> {
    let run = app.get_run(&run_id).await.map_err(internal_error)?;
    run.map(Json)
        .ok_or_else(|| (StatusCode::NOT_FOUND, "run not found".to_owned()))
}

pub async fn get_steps(
    State(app): State<WorkflowEngineApp>,
    Path(run_id): Path<String>,
) -> Result<Json<Vec<StepInstanceRecord>>, (StatusCode, String)> {
    require_run(&app, &run_id).await?;
    app.list_step_instances(&run_id)
        .await
        .map(Json)
        .map_err(runtime_error)
}

pub async fn get_transitions(
    State(app): State<WorkflowEngineApp>,
    Path(run_id): Path<String>,
) -> Result<Json<Vec<TransitionRecord>>, (StatusCode, String)> {
    require_run(&app, &run_id).await?;
    app.list_transitions(&run_id)
        .await
        .map(Json)
        .map_err(runtime_error)
}

pub async fn get_waits(
    State(app): State<WorkflowEngineApp>,
    Path(run_id): Path<String>,
) -> Result<Json<Vec<WaiterRecord>>, (StatusCode, String)> {
    require_run(&app, &run_id).await?;
    app.list_waiters(&run_id)
        .await
        .map(Json)
        .map_err(runtime_error)
}

pub async fn cancel_run(
    State(app): State<WorkflowEngineApp>,
    Path(run_id): Path<String>,
    Json(request): Json<CancelRunRequest>,
) -> Result<Json<WorkflowRunRecord>, (StatusCode, String)> {
    cancel_run_inner(app, run_id, request).await
}

async fn cancel_run_inner(
    app: WorkflowEngineApp,
    run_id: String,
    request: CancelRunRequest,
) -> Result<Json<WorkflowRunRecord>, (StatusCode, String)> {
    app.cancel_run(
        &run_id,
        request.reason.as_deref(),
        request.requested_by.as_deref(),
    )
    .await
    .map_err(runtime_error)?;

    let run = app
        .get_run(&run_id)
        .await
        .map_err(internal_error)?
        .ok_or_else(|| (StatusCode::NOT_FOUND, "run not found".to_owned()))?;
    Ok(Json(run))
}

async fn require_run(
    app: &WorkflowEngineApp,
    run_id: &str,
) -> Result<WorkflowRunRecord, (StatusCode, String)> {
    let run = app.get_run(run_id).await.map_err(internal_error)?;
    run.ok_or_else(|| (StatusCode::NOT_FOUND, "run not found".to_owned()))
}

fn runtime_error(error: TaskRuntimeError) -> (StatusCode, String) {
    match error {
        TaskRuntimeError::RunNotFound { .. } => (StatusCode::NOT_FOUND, "run not found".to_owned()),
        other => internal_error(other),
    }
}

fn internal_error(error: impl ToString) -> (StatusCode, String) {
    (StatusCode::INTERNAL_SERVER_ERROR, error.to_string())
}
