use axum::body::Body;
use http::{Request, StatusCode};
use serde_json::json;
use tempfile::TempDir;
use tower::ServiceExt;
use workflow_engine::WorkflowEngineApp;
use workflow_sdk::WorkflowBuilder;
use workflow_worker::{Capability, InProcessWorker, RunOnceOutcome, WorkerRegistry};

#[tokio::test]
async fn http_order_sync_flow_runs_end_to_end_and_deduplicates_resume_events() {
    let harness = TestHarness::start().await;
    harness.register_metadata("orders.fetch", "default");
    harness.register_metadata("orders.notify", "default");

    let definition = WorkflowBuilder::new("order-sync")
        .event_trigger("order.created")
        .task_step("fetch_order", "orders.fetch")
        .wait_for_event_step("wait_payment", "payment.captured")
        .task_step("notify_customer", "orders.notify")
        .edge("fetch_order", "wait_payment")
        .edge("wait_payment", "notify_customer")
        .compile();

    let router = harness.app.router();

    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/v1/workflows/order-sync/draft")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::to_vec(&definition).expect("definition should serialize"),
                ))
                .expect("request should build"),
        )
        .await
        .expect("draft request should succeed");
    assert_eq!(response.status(), StatusCode::OK);

    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/workflows/order-sync/draft:validate")
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("validate request should succeed");
    assert_eq!(response.status(), StatusCode::OK);
    let validation: workflow_engine::DefinitionValidation = serde_json::from_slice(
        &axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body should read"),
    )
    .expect("validation should deserialize");
    assert!(validation.ok);

    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/workflows/order-sync/draft:publish")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::to_vec(&json!({
                        "content_hash": validation.content_hash,
                    }))
                    .expect("publish request should serialize"),
                ))
                .expect("request should build"),
        )
        .await
        .expect("publish request should succeed");
    assert_eq!(response.status(), StatusCode::OK);

    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/events")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::to_vec(&json!({
                        "events": [
                            {
                                "id": "evt-order-created-1",
                                "name": "order.created",
                                "data": {
                                    "order_id": "ord-123"
                                }
                            }
                        ]
                    }))
                    .expect("event request should serialize"),
                ))
                .expect("request should build"),
        )
        .await
        .expect("start event request should succeed");
    assert_eq!(response.status(), StatusCode::OK);
    let start_results: Vec<workflow_engine::EventIngestResult> = serde_json::from_slice(
        &axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body should read"),
    )
    .expect("start response should deserialize");
    assert_eq!(start_results.len(), 1);
    assert!(!start_results[0].deduplicated);
    let run_id = start_results[0].started_runs[0].run_id.clone();

    let fetch_worker = InProcessWorker::new(
        harness.app.clone(),
        "fetch-worker".to_owned(),
        "fetch-worker".to_owned(),
    )
    .queue("default")
    .capability(Capability::Task.as_str())
    .register_handler("orders.fetch", |input| {
        Ok(json!({
            "order_id": input["order_id"].clone(),
            "status": "fetched",
        }))
    });
    let fetch_outcome = fetch_worker
        .run_once()
        .await
        .expect("fetch worker should run");
    assert!(matches!(fetch_outcome, RunOnceOutcome::Completed { .. }));

    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/v1/runs/{run_id}/waits"))
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("waits request should succeed");
    assert_eq!(response.status(), StatusCode::OK);
    let waits: Vec<workflow_engine::WaiterRecord> = serde_json::from_slice(
        &axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body should read"),
    )
    .expect("waits should deserialize");
    assert_eq!(waits.len(), 1);
    assert_eq!(waits[0].step_id, "wait_payment");
    assert_eq!(waits[0].status, "waiting");

    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/events")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::to_vec(&json!({
                        "events": [
                            {
                                "id": "evt-payment-captured-1",
                                "name": "payment.captured",
                                "data": {
                                    "order_id": "ord-123",
                                    "payment_id": "pay-1"
                                }
                            }
                        ]
                    }))
                    .expect("resume event should serialize"),
                ))
                .expect("request should build"),
        )
        .await
        .expect("resume event should succeed");
    assert_eq!(response.status(), StatusCode::OK);
    let resume_results: Vec<workflow_engine::EventIngestResult> = serde_json::from_slice(
        &axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body should read"),
    )
    .expect("resume response should deserialize");
    assert!(!resume_results[0].deduplicated);

    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/v1/runs/{run_id}/waits"))
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("waits request should succeed");
    assert_eq!(response.status(), StatusCode::OK);
    let waits: Vec<workflow_engine::WaiterRecord> = serde_json::from_slice(
        &axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body should read"),
    )
    .expect("waits should deserialize");
    assert_eq!(waits.len(), 1);
    assert_eq!(waits[0].status, "resumed");
    assert_eq!(
        waits[0].resume_event_id.as_deref(),
        Some("evt-payment-captured-1")
    );

    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/events")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::to_vec(&json!({
                        "events": [
                            {
                                "id": "evt-payment-captured-1",
                                "name": "payment.captured",
                                "data": {
                                    "order_id": "ord-123",
                                    "payment_id": "pay-1"
                                }
                            }
                        ]
                    }))
                    .expect("duplicate event should serialize"),
                ))
                .expect("request should build"),
        )
        .await
        .expect("duplicate resume event should succeed");
    assert_eq!(response.status(), StatusCode::OK);
    let duplicate_results: Vec<workflow_engine::EventIngestResult> = serde_json::from_slice(
        &axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body should read"),
    )
    .expect("duplicate response should deserialize");
    assert!(duplicate_results[0].deduplicated);

    let notify_worker = InProcessWorker::new(
        harness.app.clone(),
        "notify-worker".to_owned(),
        "notify-worker".to_owned(),
    )
    .queue("default")
    .capability(Capability::Task.as_str())
    .register_handler("orders.notify", |input| {
        Ok(json!({
            "order_id": input["order_id"].clone(),
            "payment_id": input["payment_id"].clone(),
            "notification": "sent",
        }))
    });
    let notify_outcome = notify_worker
        .run_once()
        .await
        .expect("notify worker should run");
    assert!(matches!(notify_outcome, RunOnceOutcome::Completed { .. }));

    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/v1/runs/{run_id}"))
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("run request should succeed");
    assert_eq!(response.status(), StatusCode::OK);
    let run: workflow_engine::WorkflowRunRecord = serde_json::from_slice(
        &axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body should read"),
    )
    .expect("run should deserialize");
    assert_eq!(run.status, "completed");

    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/v1/runs/{run_id}/steps"))
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("steps request should succeed");
    assert_eq!(response.status(), StatusCode::OK);
    let steps: Vec<workflow_engine::StepInstanceRecord> = serde_json::from_slice(
        &axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body should read"),
    )
    .expect("steps should deserialize");
    assert_eq!(steps.len(), 3);
    assert_eq!(steps[0].step_id, "fetch_order");
    assert_eq!(
        steps[0].output,
        Some(json!({ "order_id": "ord-123", "status": "fetched" }))
    );
    assert_eq!(steps[1].step_id, "wait_payment");
    assert_eq!(
        steps[1].output,
        Some(json!({ "order_id": "ord-123", "payment_id": "pay-1" }))
    );
    assert_eq!(steps[2].step_id, "notify_customer");
    assert_eq!(
        steps[2].output,
        Some(json!({
            "order_id": "ord-123",
            "payment_id": "pay-1",
            "notification": "sent",
        }))
    );
    assert!(steps.iter().all(|step| step.status == "completed"));

    let response = router
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/v1/runs/{run_id}/transitions"))
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("transitions request should succeed");
    assert_eq!(response.status(), StatusCode::OK);
    let transitions: Vec<workflow_engine::TransitionRecord> = serde_json::from_slice(
        &axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body should read"),
    )
    .expect("transitions should deserialize");
    let transition_types = transitions
        .iter()
        .map(|record| record.transition_type.as_str())
        .collect::<Vec<_>>();
    assert!(transition_types.contains(&"run_started"));
    assert!(transition_types.contains(&"task_completed"));
    assert!(transition_types.contains(&"waiter_registered"));
    assert!(transition_types.contains(&"waiter_resumed"));
    assert!(transition_types.contains(&"run_completed"));
}

struct TestHarness {
    _tempdir: TempDir,
    app: WorkflowEngineApp,
    registry: WorkerRegistry,
}

impl TestHarness {
    async fn start() -> Self {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let app = WorkflowEngineApp::for_test(tempdir.path())
            .await
            .expect("app should start");
        let registry = app.worker_registry();

        Self {
            _tempdir: tempdir,
            app,
            registry,
        }
    }

    fn register_metadata(&self, handler_id: &str, queue: &str) {
        self.registry
            .register(handler_id, noop_handler)
            .capability(Capability::Task)
            .default_queue(queue);
    }
}

fn noop_handler() {}
