use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};

use super::validated_list_limit;
use serde::Deserialize;

use crate::{
    StepInstanceRecord, TaskRuntimeError, TransitionRecord, WorkflowEngineApp, WorkflowRunRecord,
};

#[derive(Debug, Deserialize)]
pub struct CancelRunRequest {
    pub reason: Option<String>,
    pub requested_by: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
pub struct ListStepsQuery {
    pub limit: Option<u32>,
    pub after_created_at: Option<i64>,
    pub after_step_instance_id: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
pub struct ListTransitionsQuery {
    pub limit: Option<u32>,
    pub after_created_at: Option<i64>,
    pub after_transition_id: Option<String>,
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
    Query(query): Query<ListStepsQuery>,
) -> Result<Json<Vec<StepInstanceRecord>>, (StatusCode, String)> {
    let limit = validated_list_limit(query.limit)?;
    let cursor = required_cursor_pair(
        query.after_created_at,
        query.after_step_instance_id,
        "after_created_at and after_step_instance_id must be provided together",
    )?;
    require_run(&app, &run_id).await?;
    app.list_step_instances_page(&run_id, cursor.as_ref(), limit)
        .await
        .map(Json)
        .map_err(runtime_error)
}

pub async fn get_transitions(
    State(app): State<WorkflowEngineApp>,
    Path(run_id): Path<String>,
    Query(query): Query<ListTransitionsQuery>,
) -> Result<Json<Vec<TransitionRecord>>, (StatusCode, String)> {
    let limit = validated_list_limit(query.limit)?;
    let cursor = required_cursor_pair(
        query.after_created_at,
        query.after_transition_id,
        "after_created_at and after_transition_id must be provided together",
    )?;
    require_run(&app, &run_id).await?;
    app.list_transitions_page(&run_id, cursor.as_ref(), limit)
        .await
        .map(Json)
        .map_err(runtime_error)
}

fn required_cursor_pair(
    created_at: Option<i64>,
    stable_id: Option<String>,
    message: &'static str,
) -> Result<Option<(i64, String)>, (StatusCode, String)> {
    match (created_at, stable_id) {
        (None, None) => Ok(None),
        (Some(created_at), Some(stable_id)) if !stable_id.is_empty() => {
            Ok(Some((created_at, stable_id)))
        }
        _ => Err((StatusCode::BAD_REQUEST, message.to_owned())),
    }
}

pub async fn cancel_run(
    State(app): State<WorkflowEngineApp>,
    Path(run_id): Path<String>,
    Json(request): Json<CancelRunRequest>,
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

#[cfg(test)]
mod tests {
    use axum::body::Body;
    use http::{Request, StatusCode};
    use tempfile::TempDir;
    use tower::ServiceExt;

    use crate::{
        persistence::HiqlitePersistence, StepInstanceRecord, TransitionRecord, WorkflowEngineApp,
        WorkflowRunRecord,
    };

    #[tokio::test]
    async fn run_history_pages_are_bounded_and_use_stable_ids_to_break_timestamp_ties() {
        let tempdir = TempDir::new().expect("temporary directory should exist");
        let persistence = HiqlitePersistence::for_test(tempdir.path())
            .await
            .expect("persistence should start");
        persistence
            .insert_run(&WorkflowRunRecord {
                run_id: "run-paged-history".to_owned(),
                workflow_id: "orders".to_owned(),
                workflow_version: 1,
                status: "running".to_owned(),
                cancel_reason: None,
                cancel_requested_by: None,
                cancel_requested_at: None,
                created_at: 1,
            })
            .await
            .expect("run fixture should insert");
        for index in (0..=100).rev() {
            persistence
                .insert_step_instance(&StepInstanceRecord {
                    step_instance_id: format!("step-instance-{index:03}"),
                    run_id: "run-paged-history".to_owned(),
                    step_id: format!("step-{index:03}"),
                    status: "completed".to_owned(),
                    attempt_count: 1,
                    input: None,
                    output: None,
                    error: None,
                    created_at: 42,
                    updated_at: 42,
                })
                .await
                .expect("step fixture should insert");
            persistence
                .insert_transition(&TransitionRecord {
                    transition_id: format!("transition-{index:03}"),
                    run_id: "run-paged-history".to_owned(),
                    transition_type: "fixture".to_owned(),
                    payload: None,
                    created_at: 42,
                })
                .await
                .expect("transition fixture should insert");
        }
        let router = WorkflowEngineApp::from_test_persistence(persistence).router();

        let first_steps: Vec<StepInstanceRecord> = json_response(
            router
                .clone()
                .oneshot(get("/v1/runs/run-paged-history/steps"))
                .await
                .expect("first step page request should complete"),
        )
        .await;
        assert_eq!(first_steps.len(), 100);
        assert_eq!(first_steps[0].step_instance_id, "step-instance-000");
        assert_eq!(first_steps[99].step_instance_id, "step-instance-099");
        let second_steps: Vec<StepInstanceRecord> = json_response(
            router
                .clone()
                .oneshot(get(
                    "/v1/runs/run-paged-history/steps?after_created_at=42&after_step_instance_id=step-instance-099",
                ))
                .await
                .expect("second step page request should complete"),
        )
        .await;
        assert_eq!(second_steps.len(), 1);
        assert_eq!(second_steps[0].step_instance_id, "step-instance-100");
        let step_ids = first_steps
            .iter()
            .chain(second_steps.iter())
            .map(|step| step.step_instance_id.as_str())
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(step_ids.len(), 101, "step pages must not duplicate rows");

        let first_transitions: Vec<TransitionRecord> = json_response(
            router
                .clone()
                .oneshot(get("/v1/runs/run-paged-history/transitions"))
                .await
                .expect("first transition page request should complete"),
        )
        .await;
        assert_eq!(first_transitions.len(), 100);
        assert_eq!(first_transitions[0].transition_id, "transition-000");
        assert_eq!(first_transitions[99].transition_id, "transition-099");
        let second_transitions: Vec<TransitionRecord> = json_response(
            router
                .clone()
                .oneshot(get(
                    "/v1/runs/run-paged-history/transitions?after_created_at=42&after_transition_id=transition-099",
                ))
                .await
                .expect("second transition page request should complete"),
        )
        .await;
        assert_eq!(second_transitions.len(), 1);
        assert_eq!(second_transitions[0].transition_id, "transition-100");
        let transition_ids = first_transitions
            .iter()
            .chain(second_transitions.iter())
            .map(|transition| transition.transition_id.as_str())
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(
            transition_ids.len(),
            101,
            "transition pages must not duplicate rows"
        );

        for uri in [
            "/v1/runs/run-paged-history/steps?limit=0",
            "/v1/runs/run-paged-history/steps?limit=101",
            "/v1/runs/run-paged-history/steps?limit=invalid",
            "/v1/runs/run-paged-history/steps?after_created_at=42",
            "/v1/runs/run-paged-history/steps?after_step_instance_id=step-instance-099",
            "/v1/runs/run-paged-history/steps?after_created_at=invalid&after_step_instance_id=step-instance-099",
            "/v1/runs/run-paged-history/steps?after_created_at=42&after_step_instance_id=",
            "/v1/runs/run-paged-history/transitions?limit=0",
            "/v1/runs/run-paged-history/transitions?limit=101",
            "/v1/runs/run-paged-history/transitions?after_created_at=42",
            "/v1/runs/run-paged-history/transitions?after_transition_id=transition-099",
            "/v1/runs/run-paged-history/transitions?after_created_at=42&after_transition_id=",
        ] {
            let response = router
                .clone()
                .oneshot(get(uri))
                .await
                .expect("invalid run-history request should complete");
            assert_eq!(response.status(), StatusCode::BAD_REQUEST, "uri={uri}");
        }
    }

    fn get(uri: &str) -> Request<Body> {
        Request::builder()
            .method("GET")
            .uri(uri)
            .body(Body::empty())
            .expect("request should build")
    }

    async fn json_response<T: serde::de::DeserializeOwned>(
        response: axum::response::Response,
    ) -> T {
        assert_eq!(response.status(), StatusCode::OK);
        serde_json::from_slice(
            &axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .expect("response body should read"),
        )
        .expect("response JSON should decode")
    }
}
