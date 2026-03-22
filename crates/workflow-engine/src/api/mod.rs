mod events;
mod runs;
mod workflows;

use axum::{
    routing::{get, post, put},
    Router,
};

use crate::WorkflowEngineApp;

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
        .route("/v1/runs/{run_id}/waits", get(runs::get_waits))
        .route("/v1/runs/{run_id}/cancel", post(runs::cancel_run))
        .with_state(app)
}
