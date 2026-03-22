use axum::body::Body;
use http::{Request, StatusCode};
use serde_json::json;
use tempfile::TempDir;
use tower::ServiceExt;
use workflow_definition::{
    EventTrigger, FlowDefinition, StepDefinition, StepKind, StepMetadata, TaskMetadata, Trigger,
    WaitForEventMetadata, WorkflowDefaults,
};
use workflow_engine::WorkflowEngineApp;
use workflow_worker::{Capability, InProcessWorker, RunOnceOutcome, WorkerRegistry};

#[tokio::test]
async fn waiting_run_resumes_when_matching_event_arrives() {
    let harness = TestHarness::start().await;
    harness.register_metadata("orders.fetch", "default");
    harness.register_metadata("orders.notify", "default");

    harness
        .app
        .replace_draft(waiting_definition("orders"))
        .await
        .expect("draft should replace");
    let validation = harness
        .app
        .validate_draft("orders")
        .await
        .expect("draft should validate");
    assert!(validation.ok);
    harness
        .app
        .publish_draft("orders", &validation.content_hash)
        .await
        .expect("draft should publish");

    let started = harness
        .app
        .ingest_event("order.created", json!({ "order_id": "ord-123" }))
        .await
        .expect("start event should be accepted");
    assert_eq!(started.len(), 1);
    let run_id = started[0].run_id.clone();

    let fetch_worker = InProcessWorker::new(
        harness.app.clone(),
        "worker-fetch".to_owned(),
        "orders-fetch-worker".to_owned(),
    )
    .queue("default")
    .capability(Capability::Task.as_str())
    .register_handler("orders.fetch", |input| {
        Ok(json!({
            "order_id": input["order_id"].clone(),
            "status": "fetched",
        }))
    });

    fetch_worker
        .run_once()
        .await
        .expect("fetch worker should complete");

    let waits = harness
        .app
        .list_waiters(&run_id)
        .await
        .expect("waits should load");
    assert_eq!(waits.len(), 1);
    assert_eq!(waits[0].step_id, "wait_payment");
    assert_eq!(waits[0].status, "waiting");

    harness
        .app
        .ingest_event(
            "payment.captured",
            json!({ "order_id": "ord-123", "payment_id": "pay-1" }),
        )
        .await
        .expect("resume event should be accepted");

    let waits = harness
        .app
        .list_waiters(&run_id)
        .await
        .expect("waits should load");
    assert_eq!(waits[0].status, "resumed");

    let notify_worker = InProcessWorker::new(
        harness.app.clone(),
        "worker-notify".to_owned(),
        "orders-notify-worker".to_owned(),
    )
    .queue("default")
    .capability(Capability::Task.as_str())
    .register_handler("orders.notify", |input| {
        Ok(json!({
            "notified_order_id": input["order_id"].clone(),
            "payment_id": input["payment_id"].clone(),
        }))
    });

    notify_worker
        .run_once()
        .await
        .expect("notify worker should complete");

    let run = harness
        .app
        .get_run(&run_id)
        .await
        .expect("run lookup should succeed")
        .expect("run should exist");
    assert_eq!(run.status, "completed");

    let steps = harness
        .app
        .list_step_instances(&run_id)
        .await
        .expect("steps should load");
    assert_eq!(steps.len(), 3);
    assert_eq!(steps[0].step_id, "fetch_order");
    assert_eq!(steps[0].status, "completed");
    assert_eq!(steps[1].step_id, "wait_payment");
    assert_eq!(steps[1].status, "completed");
    assert_eq!(
        steps[1].output,
        Some(json!({ "order_id": "ord-123", "payment_id": "pay-1" }))
    );
    assert_eq!(steps[2].step_id, "notify_customer");
    assert_eq!(steps[2].status, "completed");

    let transitions = harness
        .app
        .list_transitions(&run_id)
        .await
        .expect("transitions should load");
    let transition_types = transitions
        .iter()
        .map(|record| record.transition_type.as_str())
        .collect::<Vec<_>>();
    assert!(transition_types.contains(&"waiter_registered"));
    assert!(transition_types.contains(&"waiter_resumed"));
    assert!(transition_types.contains(&"run_completed"));
}

#[tokio::test]
async fn http_event_ingest_run_inspection_and_cancellation_barrier_work() {
    let harness = TestHarness::start().await;
    harness.register_metadata("orders.fetch", "default");
    harness.register_metadata("orders.notify", "default");
    harness
        .app
        .replace_draft(waiting_definition("orders"))
        .await
        .expect("draft should replace");
    let validation = harness
        .app
        .validate_draft("orders")
        .await
        .expect("draft should validate");
    assert!(validation.ok);
    harness
        .app
        .publish_draft("orders", &validation.content_hash)
        .await
        .expect("draft should publish");

    let router = harness.app.router();
    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/events")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::to_vec(&serde_json::json!({
                        "events": [
                            {
                                "id": "evt-start-1",
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
        .expect("event request should succeed");
    assert_eq!(response.status(), StatusCode::OK);
    let event_results: Vec<workflow_engine::EventIngestResult> = serde_json::from_slice(
        &axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body should read"),
    )
    .expect("event response should deserialize");
    let run_id = event_results[0].started_runs[0].run_id.clone();

    let fetch_worker = InProcessWorker::new(
        harness.app.clone(),
        "worker-fetch".to_owned(),
        "orders-fetch-worker".to_owned(),
    )
    .queue("default")
    .capability(Capability::Task.as_str())
    .register_handler("orders.fetch", |input| {
        Ok(json!({
            "order_id": input["order_id"].clone(),
        }))
    });
    fetch_worker
        .run_once()
        .await
        .expect("fetch worker should complete");

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
    .expect("waits response should deserialize");
    assert_eq!(waits[0].status, "waiting");

    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/v1/runs/{run_id}/cancel"))
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::to_vec(&serde_json::json!({
                        "reason": "user_requested",
                        "requested_by": "tester@example.com"
                    }))
                    .expect("cancel request should serialize"),
                ))
                .expect("request should build"),
        )
        .await
        .expect("cancel request should succeed");
    assert_eq!(response.status(), StatusCode::OK);

    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/events")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::to_vec(&serde_json::json!({
                        "events": [
                            {
                                "id": "evt-resume-1",
                                "name": "payment.captured",
                                "data": {
                                    "order_id": "ord-123",
                                    "payment_id": "pay-1"
                                }
                            }
                        ]
                    }))
                    .expect("resume request should serialize"),
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
    assert_eq!(resume_results.len(), 1);
    assert!(resume_results[0].started_runs.is_empty());

    let notify_worker = InProcessWorker::new(
        harness.app.clone(),
        "worker-notify".to_owned(),
        "orders-notify-worker".to_owned(),
    )
    .queue("default")
    .capability(Capability::Task.as_str())
    .register_handler("orders.notify", |_input| {
        Ok(json!({
            "unexpected": true
        }))
    });
    let outcome = notify_worker
        .run_once()
        .await
        .expect("notify worker should poll safely");
    assert_eq!(outcome, RunOnceOutcome::NoTaskAvailable);

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
    let run: workflow_engine::WorkflowRunRecord = serde_json::from_slice(
        &axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body should read"),
    )
    .expect("run response should deserialize");
    assert_eq!(run.status, "cancelled");

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
    let waits: Vec<workflow_engine::WaiterRecord> = serde_json::from_slice(
        &axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body should read"),
    )
    .expect("waits response should deserialize");
    assert_eq!(waits.len(), 1);
    assert_eq!(waits[0].status, "cancelled");
    assert_eq!(waits[0].resume_event_id, None);

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
    let steps: Vec<workflow_engine::StepInstanceRecord> = serde_json::from_slice(
        &axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body should read"),
    )
    .expect("steps response should deserialize");
    assert_eq!(steps.len(), 2);

    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/v1/runs/{run_id}/transitions"))
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("transitions request should succeed");
    let transitions: Vec<workflow_engine::TransitionRecord> = serde_json::from_slice(
        &axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body should read"),
    )
    .expect("transitions response should deserialize");
    assert!(transitions
        .iter()
        .any(|record| record.transition_type == "run_cancel_requested"));
    assert!(!transitions
        .iter()
        .any(|record| record.transition_type == "waiter_resumed"));
}

#[tokio::test]
async fn http_run_subresources_return_not_found_for_unknown_run() {
    let harness = TestHarness::start().await;
    let router = harness.app.router();

    for path in [
        "/v1/runs/missing/steps",
        "/v1/runs/missing/transitions",
        "/v1/runs/missing/waits",
    ] {
        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri(path)
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("request should succeed");
        assert_eq!(response.status(), StatusCode::NOT_FOUND, "{path}");
    }

    for path in ["/v1/runs/missing/cancel"] {
        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(path)
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&serde_json::json!({
                            "reason": "missing",
                        }))
                        .expect("cancel request should serialize"),
                    ))
                    .expect("request should build"),
            )
            .await
            .expect("request should succeed");
        assert_eq!(response.status(), StatusCode::NOT_FOUND, "{path}");
    }
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
            .define_handler(handler_id)
            .capability(Capability::Task)
            .default_queue(queue);
    }
}

fn waiting_definition(workflow_id: &str) -> FlowDefinition {
    FlowDefinition {
        workflow_id: workflow_id.to_owned(),
        workflow_version: 0,
        defaults: WorkflowDefaults::default(),
        trigger: Some(Trigger::Event(EventTrigger {
            event_type: "order.created".to_owned(),
        })),
        steps: vec![
            StepDefinition {
                id: "fetch_order".to_owned(),
                kind: StepKind::Task,
                metadata: StepMetadata {
                    task: Some(TaskMetadata {
                        handler: "orders.fetch".to_owned(),
                    }),
                    ..StepMetadata::default()
                },
            },
            StepDefinition {
                id: "wait_payment".to_owned(),
                kind: StepKind::WaitForEvent,
                metadata: StepMetadata {
                    wait_for_event: Some(WaitForEventMetadata {
                        event_type: "payment.captured".to_owned(),
                        timeout_seconds: None,
                    }),
                    ..StepMetadata::default()
                },
            },
            StepDefinition {
                id: "notify_customer".to_owned(),
                kind: StepKind::Task,
                metadata: StepMetadata {
                    task: Some(TaskMetadata {
                        handler: "orders.notify".to_owned(),
                    }),
                    ..StepMetadata::default()
                },
            },
        ],
        edges: vec![
            workflow_definition::Edge {
                from: "fetch_order".to_owned(),
                to: "wait_payment".to_owned(),
            },
            workflow_definition::Edge {
                from: "wait_payment".to_owned(),
                to: "notify_customer".to_owned(),
            },
        ],
    }
}
