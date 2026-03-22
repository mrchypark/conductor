use serde_json::json;
use tempfile::TempDir;
use workflow_definition::{
    EventTrigger, FlowDefinition, StepDefinition, StepKind, StepMetadata, TaskMetadata, Trigger,
    WorkflowDefaults,
};
use workflow_engine::WorkflowEngineApp;
use workflow_worker::{Capability, InProcessWorker, WorkerRegistry};

#[tokio::test]
async fn event_creates_runnable_task_that_worker_can_lease_and_complete() {
    let harness = TestHarness::start().await;
    harness.register_metadata("orders.fetch");

    harness
        .app
        .replace_draft(single_task_definition("orders", "orders.fetch"))
        .await
        .expect("draft should replace");
    let validation = harness
        .app
        .validate_draft("orders")
        .await
        .expect("validation should complete");
    assert!(validation.ok);
    harness
        .app
        .publish_draft("orders", &validation.content_hash)
        .await
        .expect("publish should succeed");

    let started_runs = harness
        .app
        .ingest_event("order.created", json!({ "order_id": "ord-123" }))
        .await
        .expect("event ingest should create a run");
    assert_eq!(started_runs.len(), 1);
    let run_id = started_runs[0].run_id.clone();

    let worker = InProcessWorker::new(
        harness.app.clone(),
        "worker-1".to_owned(),
        "orders-worker".to_owned(),
    )
    .queue("default")
    .capability(Capability::Task.as_str())
    .register_handler("orders.fetch", |input| {
        Ok(json!({
            "reserved_order_id": input["order_id"].clone(),
        }))
    });

    let result = worker.run_once().await.expect("worker should execute");
    assert!(
        result.completed_task().is_some(),
        "worker should complete one task"
    );

    let run = harness
        .app
        .get_run(&run_id)
        .await
        .expect("run should load")
        .expect("run should exist");
    assert_eq!(run.status, "completed");

    let steps = harness
        .app
        .list_step_instances(&run_id)
        .await
        .expect("step instances should load");
    assert_eq!(steps.len(), 1);
    assert_eq!(steps[0].step_id, "reserve_inventory");
    assert_eq!(steps[0].status, "completed");
    assert_eq!(
        steps[0].output,
        Some(json!({
            "reserved_order_id": "ord-123"
        }))
    );

    harness
        .app
        .cancel_run(&run_id, Some("late_cancel"), Some("tester@example.com"))
        .await
        .expect("late cancel should be a no-op");

    let run = harness
        .app
        .get_run(&run_id)
        .await
        .expect("run should still load")
        .expect("run should still exist");
    assert_eq!(run.status, "completed");

    let transitions = harness
        .app
        .list_transitions(&run_id)
        .await
        .expect("transitions should load");
    assert!(!transitions
        .iter()
        .any(|record| record.transition_type == "run_cancel_requested"));
}

struct TestHarness {
    _tempdir: TempDir,
    app: WorkflowEngineApp,
    registry: WorkerRegistry,
}

impl TestHarness {
    async fn start() -> Self {
        let tempdir = TempDir::new().expect("temporary directory should exist");
        let app = WorkflowEngineApp::for_test(tempdir.path())
            .await
            .expect("test app should start");
        let registry = app.worker_registry();

        Self {
            _tempdir: tempdir,
            app,
            registry,
        }
    }

    fn register_metadata(&self, handler_id: &str) {
        self.registry.register(handler_id, noop_handler);
    }
}

fn noop_handler() {}

fn single_task_definition(workflow_id: &str, handler_id: &str) -> FlowDefinition {
    FlowDefinition {
        workflow_id: workflow_id.to_owned(),
        workflow_version: 0,
        defaults: WorkflowDefaults::default(),
        trigger: Some(Trigger::Event(EventTrigger {
            event_type: "order.created".to_owned(),
        })),
        steps: vec![StepDefinition {
            id: "reserve_inventory".to_owned(),
            kind: StepKind::Task,
            metadata: StepMetadata {
                task: Some(TaskMetadata {
                    handler: handler_id.to_owned(),
                }),
                ..StepMetadata::default()
            },
        }],
        edges: Vec::new(),
    }
}
