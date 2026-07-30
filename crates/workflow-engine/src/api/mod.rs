mod events;
mod runs;
mod workflows;

use axum::{
    routing::{get, post, put},
    Router,
};

use crate::WorkflowEngineApp;

const LIST_PAGE_DEFAULT: u32 = 100;
const LIST_PAGE_MAX: u32 = 100;

fn validated_list_limit(limit: Option<u32>) -> Result<u32, (axum::http::StatusCode, String)> {
    let limit = limit.unwrap_or(LIST_PAGE_DEFAULT);
    if (1..=LIST_PAGE_MAX).contains(&limit) {
        Ok(limit)
    } else {
        Err((
            axum::http::StatusCode::BAD_REQUEST,
            format!("limit must be between 1 and {LIST_PAGE_MAX}"),
        ))
    }
}

pub fn router(app: WorkflowEngineApp) -> Router {
    Router::new()
        .route(
            "/v1/workflows/{workflow_id}/draft",
            put(workflows::put_draft),
        )
        .route("/v1/workflows/{workflow_id}", get(workflows::get_workflow))
        .route(
            "/v1/workflows/{workflow_id}/versions",
            get(workflows::list_versions),
        )
        .route(
            "/v1/workflows/{workflow_id}/versions/{workflow_version}",
            get(workflows::get_version),
        )
        .route(
            "/v1/workflows/{workflow_id}/draft:validate",
            post(workflows::validate_draft),
        )
        .route(
            "/v1/workflows/{workflow_id}/draft:publish",
            post(workflows::publish_draft),
        )
        .route("/v1/events", post(events::post_events))
        .route("/v1/runs/{run_id}", get(runs::get_run))
        .route("/v1/runs/{run_id}/steps", get(runs::get_steps))
        .route("/v1/runs/{run_id}/transitions", get(runs::get_transitions))
        .route("/v1/runs/{run_id}/cancel", post(runs::cancel_run))
        .with_state(app)
}
