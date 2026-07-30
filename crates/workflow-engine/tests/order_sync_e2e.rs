use axum::body::Body;
use http::{Request, StatusCode};
use serde_json::json;
use tempfile::TempDir;
use tower::ServiceExt;
use workflow_engine::WorkflowEngineApp;
use workflow_sdk::WorkflowBuilder;
use workflow_worker::{Capability, InProcessWorker, RunOnceOutcome, WorkerRegistry};

#[tokio::test]
async fn http_order_sync_flow_runs_end_to_end_with_sleep_between_tasks() {
    let harness = TestHarness::start().await;
    harness.register_metadata("orders.fetch", "default");
    harness.register_metadata("orders.notify", "default");

    let definition = WorkflowBuilder::new("order-sync")
        .event_trigger("order.created")
        .task_step("fetch_order", "orders.fetch")
        .sleep_step("cooldown", 1)
        .task_step("notify_customer", "orders.notify")
        .edge("fetch_order", "cooldown")
        .edge("cooldown", "notify_customer")
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

    harness
        .app
        .recovery_scan_at(now_unix_seconds() + 2)
        .await
        .expect("recovery should fire sleep timer");

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
    let fetch_order = steps
        .iter()
        .find(|step| step.step_id == "fetch_order")
        .expect("fetch_order step should exist");
    assert_eq!(
        fetch_order.output,
        Some(json!({ "order_id": "ord-123", "status": "fetched" }))
    );
    let cooldown = steps
        .iter()
        .find(|step| step.step_id == "cooldown")
        .expect("cooldown step should exist");
    assert_eq!(
        cooldown.output,
        Some(json!({ "order_id": "ord-123", "status": "fetched" }))
    );
    let notify_customer = steps
        .iter()
        .find(|step| step.step_id == "notify_customer")
        .expect("notify_customer step should exist");
    assert_eq!(
        notify_customer.output,
        Some(json!({
            "order_id": "ord-123",
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
    let by_type = |transition_type: &str| {
        transitions
            .iter()
            .filter(|record| record.transition_type == transition_type)
            .collect::<Vec<_>>()
    };
    assert_eq!(by_type("run_started").len(), 1);
    assert_eq!(by_type("run_completed").len(), 1);

    let scheduled = by_type("timer_scheduled");
    assert_eq!(scheduled.len(), 1);
    assert_eq!(
        scheduled[0].payload,
        Some(json!({
            "step_id": "cooldown",
            "timer_kind": "sleep",
        }))
    );

    let fired = by_type("timer_fired");
    assert_eq!(fired.len(), 1);
    let fired_payload = fired[0]
        .payload
        .as_ref()
        .expect("timer_fired should retain its payload");
    assert_eq!(fired_payload["timer_kind"], "sleep");
    assert!(fired_payload["timer_id"]
        .as_str()
        .is_some_and(|id| !id.is_empty()));

    let mut completed_step_ids = by_type("task_completed")
        .iter()
        .map(|record| {
            record
                .payload
                .as_ref()
                .and_then(|payload| payload["step_id"].as_str())
                .expect("task_completed should identify its step")
        })
        .collect::<Vec<_>>();
    completed_step_ids.sort_unstable();
    assert_eq!(completed_step_ids, ["fetch_order", "notify_customer"]);
}

struct TestHarness {
    _tempdir: TempDir,
    app: WorkflowEngineApp,
    registry: WorkerRegistry,
}

fn now_unix_seconds() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};

    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("current time should be after unix epoch")
        .as_secs() as i64
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
            .define_handler(handler_id)
            .capability(Capability::Task)
            .default_queue(queue);
    }
}
