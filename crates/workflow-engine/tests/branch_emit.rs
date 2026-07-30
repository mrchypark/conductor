use serde_json::json;
use tempfile::TempDir;
use workflow_definition::{
    BranchMetadata, Edge, EventTrigger, FlowDefinition, StepDefinition, StepKind, StepMetadata,
    TaskMetadata, Trigger, WorkflowDefaults,
};
use workflow_engine::WorkflowEngineApp;
use workflow_worker::{Capability, InProcessWorker, RunOnceOutcome, WorkerRegistry};

#[tokio::test]
async fn branch_true_schedules_followup_task_in_same_workflow() {
    let harness = TestHarness::start().await;
    harness.register_metadata("orders.fetch", "default");
    harness.register_metadata("receipts.send", "default");

    harness
        .app
        .replace_draft(order_workflow_definition())
        .await
        .expect("order workflow draft should replace");
    let validation = harness
        .app
        .validate_draft("order-flow")
        .await
        .expect("order workflow should validate");
    assert!(validation.ok);
    harness
        .app
        .publish_draft("order-flow", &validation.content_hash)
        .await
        .expect("order workflow should publish");

    let started = harness
        .app
        .ingest_event(
            "order.created",
            json!({
                "order_id": "ord-123",
                "emit_receipt": true
            }),
        )
        .await
        .expect("start event should ingest");
    let run_id = started[0].run_id.clone();

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
            "emit_receipt": input["emit_receipt"].clone(),
        }))
    });
    let fetch_outcome = fetch_worker
        .run_once()
        .await
        .expect("fetch worker should run");
    assert!(matches!(fetch_outcome, RunOnceOutcome::Completed { .. }));

    let receipt_worker = InProcessWorker::new(
        harness.app.clone(),
        "receipt-worker".to_owned(),
        "receipt-worker".to_owned(),
    )
    .queue("default")
    .capability(Capability::Task.as_str())
    .register_handler("receipts.send", |input| {
        Ok(json!({
            "receipt_for": input["order_id"].clone(),
        }))
    });
    let receipt_outcome = receipt_worker
        .run_once()
        .await
        .expect("receipt worker should run after branch");
    assert!(matches!(receipt_outcome, RunOnceOutcome::Completed { .. }));

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
        .expect("steps should load");
    assert_eq!(steps.len(), 3);
    assert_eq!(steps[0].step_id, "fetch_order");
    assert_eq!(steps[1].step_id, "should_emit_receipt");
    assert_eq!(steps[2].step_id, "send_receipt");
    assert!(steps.iter().all(|step| step.status == "completed"));

    let transitions = harness
        .app
        .list_transitions(&run_id)
        .await
        .expect("transitions should load");
    assert!(transitions
        .iter()
        .any(|record| record.transition_type == "branch_evaluated"));
}

#[tokio::test]
async fn branch_false_finishes_without_followup_task_creation() {
    let harness = TestHarness::start().await;
    harness.register_metadata("orders.fetch", "default");
    harness.register_metadata("receipts.send", "default");

    harness
        .app
        .replace_draft(order_workflow_definition())
        .await
        .expect("order workflow draft should replace");
    let validation = harness
        .app
        .validate_draft("order-flow")
        .await
        .expect("order workflow should validate");
    assert!(validation.ok);
    harness
        .app
        .publish_draft("order-flow", &validation.content_hash)
        .await
        .expect("order workflow should publish");

    let started = harness
        .app
        .ingest_event(
            "order.created",
            json!({
                "order_id": "ord-456",
                "emit_receipt": false
            }),
        )
        .await
        .expect("start event should ingest");
    let run_id = started[0].run_id.clone();

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
            "emit_receipt": input["emit_receipt"].clone(),
        }))
    });
    let fetch_outcome = fetch_worker
        .run_once()
        .await
        .expect("fetch worker should run");
    assert!(matches!(fetch_outcome, RunOnceOutcome::Completed { .. }));

    let receipt_worker = InProcessWorker::new(
        harness.app.clone(),
        "receipt-worker".to_owned(),
        "receipt-worker".to_owned(),
    )
    .queue("default")
    .capability(Capability::Task.as_str())
    .register_handler("receipts.send", |_input| {
        Ok(json!({
            "unexpected": true,
        }))
    });
    let receipt_outcome = receipt_worker
        .run_once()
        .await
        .expect("receipt worker should poll safely");
    assert_eq!(receipt_outcome, RunOnceOutcome::NoTaskAvailable);

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
        .expect("steps should load");
    assert_eq!(steps.len(), 2);
    assert_eq!(steps[0].step_id, "fetch_order");
    assert_eq!(steps[1].step_id, "should_emit_receipt");

    let transitions = harness
        .app
        .list_transitions(&run_id)
        .await
        .expect("transitions should load");
    assert!(transitions
        .iter()
        .any(|record| record.transition_type == "branch_evaluated"));
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

    fn register_metadata(&self, handler_id: &str, queue: &str) {
        self.registry
            .define_handler(handler_id)
            .default_queue(queue);
    }
}

fn order_workflow_definition() -> FlowDefinition {
    FlowDefinition {
        workflow_id: "order-flow".to_owned(),
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
                        capability: None,
                        queue: None,
                        timeout_seconds: None,
                    }),
                    ..StepMetadata::default()
                },
            },
            StepDefinition {
                id: "should_emit_receipt".to_owned(),
                kind: StepKind::Branch,
                metadata: StepMetadata {
                    branch: Some(BranchMetadata {
                        expression: "event.data.emit_receipt == true".to_owned(),
                    }),
                    ..StepMetadata::default()
                },
            },
            StepDefinition {
                id: "send_receipt".to_owned(),
                kind: StepKind::Task,
                metadata: StepMetadata {
                    task: Some(TaskMetadata {
                        handler: "receipts.send".to_owned(),
                        capability: None,
                        queue: None,
                        timeout_seconds: None,
                    }),
                    ..StepMetadata::default()
                },
            },
        ],
        edges: vec![
            Edge {
                from: "fetch_order".to_owned(),
                to: "should_emit_receipt".to_owned(),
            },
            Edge {
                from: "should_emit_receipt".to_owned(),
                to: "send_receipt".to_owned(),
            },
        ],
    }
}
