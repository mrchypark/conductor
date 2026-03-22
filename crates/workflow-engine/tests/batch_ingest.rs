use axum::body::Body;
use http::{Request, StatusCode};
use tempfile::TempDir;
use tower::ServiceExt;
use workflow_definition::{
    EventTrigger, FlowDefinition, StepDefinition, StepKind, StepMetadata, TaskMetadata, Trigger,
    WorkflowDefaults,
};
use workflow_engine::WorkflowEngineApp;
use workflow_worker::{Capability, WorkerRegistry};

#[tokio::test]
async fn batch_event_ingest_returns_per_item_dedupe_results() {
    let harness = TestHarness::start().await;
    harness.register_metadata("inventory.reserve", "default");

    harness
        .app
        .replace_draft(batch_definition())
        .await
        .expect("batch workflow draft should replace");
    let validation = harness
        .app
        .validate_draft("batch-flow")
        .await
        .expect("batch workflow should validate");
    assert!(validation.ok);
    harness
        .app
        .publish_draft("batch-flow", &validation.content_hash)
        .await
        .expect("batch workflow should publish");

    let response = harness
        .app
        .router()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/events")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::to_vec(&serde_json::json!({
                        "events": [
                            {
                                "id": "evt-batch-1",
                                "name": "batch.created",
                                "data": { "batch_id": "b-1" }
                            },
                            {
                                "id": "evt-batch-1",
                                "name": "batch.created",
                                "data": { "batch_id": "b-1" }
                            },
                            {
                                "id": "evt-batch-2",
                                "name": "batch.created",
                                "data": { "batch_id": "b-2" }
                            }
                        ]
                    }))
                    .expect("request body should serialize"),
                ))
                .expect("request should build"),
        )
        .await
        .expect("batch request should succeed");
    assert_eq!(response.status(), StatusCode::OK);

    let results: Vec<workflow_engine::EventIngestResult> = serde_json::from_slice(
        &axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("response body should read"),
    )
    .expect("results should deserialize");

    assert_eq!(results.len(), 3);
    assert!(!results[0].deduplicated);
    assert_eq!(results[0].started_runs.len(), 1);
    assert!(results[1].deduplicated);
    assert!(results[1].started_runs.is_empty());
    assert!(!results[2].deduplicated);
    assert_eq!(results[2].started_runs.len(), 1);
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

fn batch_definition() -> FlowDefinition {
    FlowDefinition {
        workflow_id: "batch-flow".to_owned(),
        workflow_version: 0,
        defaults: WorkflowDefaults::default(),
        trigger: Some(Trigger::Event(EventTrigger {
            event_type: "batch.created".to_owned(),
        })),
        steps: vec![StepDefinition {
            id: "reserve_inventory".to_owned(),
            kind: StepKind::Task,
            metadata: StepMetadata {
                task: Some(TaskMetadata {
                    handler: "inventory.reserve".to_owned(),
                }),
                ..StepMetadata::default()
            },
        }],
        edges: Vec::new(),
    }
}
