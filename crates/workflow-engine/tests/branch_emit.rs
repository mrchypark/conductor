use serde_json::json;
use tempfile::TempDir;
use workflow_definition::{
    BranchMetadata, Edge, EmitEventMetadata, EventTrigger, FlowDefinition, StepDefinition,
    StepKind, StepMetadata, TaskMetadata, Trigger, WorkflowDefaults,
};
use workflow_engine::WorkflowEngineApp;
use workflow_worker::{Capability, InProcessWorker, RunOnceOutcome, WorkerRegistry};

#[tokio::test]
async fn branch_true_emits_event_that_starts_followup_workflow() {
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

    harness
        .app
        .replace_draft(receipt_workflow_definition())
        .await
        .expect("receipt workflow draft should replace");
    let validation = harness
        .app
        .validate_draft("receipt-flow")
        .await
        .expect("receipt workflow should validate");
    assert!(validation.ok);
    harness
        .app
        .publish_draft("receipt-flow", &validation.content_hash)
        .await
        .expect("receipt workflow should publish");

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
    let order_run_id = started[0].run_id.clone();

    let order_worker = InProcessWorker::new(
        harness.app.clone(),
        "order-worker".to_owned(),
        "order-worker".to_owned(),
    )
    .queue("default")
    .capability(Capability::Task.as_str())
    .register_handler("orders.fetch", |input| {
        Ok(json!({
            "order_id": input["order_id"].clone(),
            "emit_receipt": true,
        }))
    });
    let order_outcome = order_worker
        .run_once()
        .await
        .expect("order worker should run");
    assert!(matches!(order_outcome, RunOnceOutcome::Completed { .. }));

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
        .expect("receipt worker should run after emit_event");
    let receipt_run_id = match receipt_outcome {
        RunOnceOutcome::Completed { run_id, .. } => run_id,
        other => panic!("expected completed receipt task, got {other:?}"),
    };

    let order_run = harness
        .app
        .get_run(&order_run_id)
        .await
        .expect("order run should load")
        .expect("order run should exist");
    assert_eq!(order_run.status, "completed");

    let order_steps = harness
        .app
        .list_step_instances(&order_run_id)
        .await
        .expect("order steps should load");
    assert_eq!(order_steps.len(), 3);
    assert_eq!(order_steps[1].step_id, "should_emit_receipt");
    assert_eq!(order_steps[2].step_id, "emit_receipt_requested");
    assert!(order_steps.iter().all(|step| step.status == "completed"));

    let order_transitions = harness
        .app
        .list_transitions(&order_run_id)
        .await
        .expect("order transitions should load");
    assert!(order_transitions
        .iter()
        .any(|record| record.transition_type == "branch_evaluated"));
    assert!(order_transitions
        .iter()
        .any(|record| record.transition_type == "event_emitted"));

    let receipt_run = harness
        .app
        .get_run(&receipt_run_id)
        .await
        .expect("receipt run should load")
        .expect("receipt run should exist");
    assert_eq!(receipt_run.workflow_id, "receipt-flow");
    assert_eq!(receipt_run.status, "completed");
}

#[tokio::test]
async fn branch_false_skips_emit_event_and_finishes_parent_run() {
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

    harness
        .app
        .replace_draft(receipt_workflow_definition())
        .await
        .expect("receipt workflow draft should replace");
    let validation = harness
        .app
        .validate_draft("receipt-flow")
        .await
        .expect("receipt workflow should validate");
    assert!(validation.ok);
    harness
        .app
        .publish_draft("receipt-flow", &validation.content_hash)
        .await
        .expect("receipt workflow should publish");

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
    let order_run_id = started[0].run_id.clone();

    let order_worker = InProcessWorker::new(
        harness.app.clone(),
        "order-worker".to_owned(),
        "order-worker".to_owned(),
    )
    .queue("default")
    .capability(Capability::Task.as_str())
    .register_handler("orders.fetch", |input| {
        Ok(json!({
            "order_id": input["order_id"].clone(),
            "emit_receipt": input["emit_receipt"].clone(),
        }))
    });
    let order_outcome = order_worker
        .run_once()
        .await
        .expect("order worker should run");
    assert!(matches!(order_outcome, RunOnceOutcome::Completed { .. }));

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

    let order_run = harness
        .app
        .get_run(&order_run_id)
        .await
        .expect("order run should load")
        .expect("order run should exist");
    assert_eq!(order_run.status, "completed");

    let order_steps = harness
        .app
        .list_step_instances(&order_run_id)
        .await
        .expect("order steps should load");
    assert_eq!(order_steps.len(), 2);
    assert_eq!(order_steps[0].step_id, "fetch_order");
    assert_eq!(order_steps[1].step_id, "should_emit_receipt");

    let order_transitions = harness
        .app
        .list_transitions(&order_run_id)
        .await
        .expect("order transitions should load");
    assert!(order_transitions
        .iter()
        .any(|record| record.transition_type == "branch_evaluated"));
    assert!(!order_transitions
        .iter()
        .any(|record| record.transition_type == "event_emitted"));
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
                id: "emit_receipt_requested".to_owned(),
                kind: StepKind::EmitEvent,
                metadata: StepMetadata {
                    emit_event: Some(EmitEventMetadata {
                        event_type: "receipt.requested".to_owned(),
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
                to: "emit_receipt_requested".to_owned(),
            },
        ],
    }
}

fn receipt_workflow_definition() -> FlowDefinition {
    FlowDefinition {
        workflow_id: "receipt-flow".to_owned(),
        workflow_version: 0,
        defaults: WorkflowDefaults::default(),
        trigger: Some(Trigger::Event(EventTrigger {
            event_type: "receipt.requested".to_owned(),
        })),
        steps: vec![StepDefinition {
            id: "send_receipt".to_owned(),
            kind: StepKind::Task,
            metadata: StepMetadata {
                task: Some(TaskMetadata {
                    handler: "receipts.send".to_owned(),
                }),
                ..StepMetadata::default()
            },
        }],
        edges: Vec::new(),
    }
}
