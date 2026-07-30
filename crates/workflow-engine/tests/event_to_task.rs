use std::time::{Duration, SystemTime, UNIX_EPOCH};

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

#[tokio::test]
async fn event_queueing_uses_explicit_task_metadata_over_registry_defaults() {
    let harness = TestHarness::start().await;
    harness.register_metadata_with_options("orders.fetch", "default", Some(300));

    harness
        .app
        .replace_draft(single_task_definition_with_metadata(
            "orders",
            TaskMetadata {
                handler: "orders.fetch".to_owned(),
                capability: Some(Capability::Task.as_str().to_owned()),
                queue: Some("priority".to_owned()),
                timeout_seconds: Some(90),
            },
        ))
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

    let default_queue_poll = harness
        .app
        .poll_task(workflow_protocol::PollTask {
            worker_id: "worker-default".to_owned(),
            worker_name: "worker-default".to_owned(),
            queues: vec!["default".to_owned()],
            capabilities: vec![Capability::Task.as_str().to_owned()],
            max_inflight: 1,
        })
        .await
        .expect("poll should succeed");
    assert!(matches!(
        default_queue_poll,
        workflow_protocol::PollTaskResponse::NoTaskAvailable
    ));

    let priority_queue_poll = harness
        .app
        .poll_task(workflow_protocol::PollTask {
            worker_id: "worker-priority".to_owned(),
            worker_name: "worker-priority".to_owned(),
            queues: vec!["priority".to_owned()],
            capabilities: vec![Capability::Task.as_str().to_owned()],
            max_inflight: 1,
        })
        .await
        .expect("poll should succeed");
    assert!(matches!(
        priority_queue_poll,
        workflow_protocol::PollTaskResponse::Assigned(_)
    ));
}

#[tokio::test]
async fn event_queueing_falls_back_to_registry_defaults_when_metadata_is_omitted() {
    let harness = TestHarness::start().await;
    harness.register_metadata_with_options("orders.fetch", "orders", Some(30));

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

    let default_queue_poll = harness
        .app
        .poll_task(workflow_protocol::PollTask {
            worker_id: "worker-default".to_owned(),
            worker_name: "worker-default".to_owned(),
            queues: vec!["default".to_owned()],
            capabilities: vec![Capability::Task.as_str().to_owned()],
            max_inflight: 1,
        })
        .await
        .expect("poll should succeed");
    assert!(matches!(
        default_queue_poll,
        workflow_protocol::PollTaskResponse::NoTaskAvailable
    ));

    let orders_queue_poll = harness
        .app
        .poll_task(workflow_protocol::PollTask {
            worker_id: "worker-orders".to_owned(),
            worker_name: "worker-orders".to_owned(),
            queues: vec!["orders".to_owned()],
            capabilities: vec![Capability::Task.as_str().to_owned()],
            max_inflight: 1,
        })
        .await
        .expect("poll should succeed");
    assert!(matches!(
        orders_queue_poll,
        workflow_protocol::PollTaskResponse::Assigned(_)
    ));
}

#[tokio::test]
async fn cancel_queued_run_prevents_poll_and_reaches_terminal_cancelled() {
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

    harness
        .app
        .cancel_run(&run_id, Some("user_cancelled"), Some("tester@example.com"))
        .await
        .expect("cancel should succeed");

    let run = harness
        .app
        .get_run(&run_id)
        .await
        .expect("run should load")
        .expect("run should exist");
    assert_eq!(run.status, "cancelled");

    let poll = harness
        .app
        .poll_task(workflow_protocol::PollTask {
            worker_id: "worker-default".to_owned(),
            worker_name: "worker-default".to_owned(),
            queues: vec!["default".to_owned()],
            capabilities: vec![Capability::Task.as_str().to_owned()],
            max_inflight: 1,
        })
        .await
        .expect("poll should succeed");
    assert!(matches!(
        poll,
        workflow_protocol::PollTaskResponse::NoTaskAvailable
    ));
}

#[tokio::test]
async fn event_task_timeout_uses_explicit_metadata_before_registry_default() {
    let harness = TestHarness::start().await;
    harness.register_metadata_with_options("orders.fetch", "default", Some(90));

    harness
        .app
        .replace_draft(single_task_definition_with_metadata(
            "orders",
            TaskMetadata {
                handler: "orders.fetch".to_owned(),
                capability: Some(Capability::Task.as_str().to_owned()),
                queue: None,
                timeout_seconds: Some(1),
            },
        ))
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

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("current time should be after unix epoch")
        .as_secs() as i64;
    harness
        .app
        .recovery_scan_at(now + 5)
        .await
        .expect("recovery scan should succeed");

    let run = harness
        .app
        .get_run(&run_id)
        .await
        .expect("run should load")
        .expect("run should exist");
    assert_eq!(run.status, "failed");

    let steps = harness
        .app
        .list_step_instances(&run_id)
        .await
        .expect("step instances should load");
    assert_eq!(steps.len(), 1);
    assert_eq!(steps[0].status, "timed_out");
    assert_eq!(
        steps[0].error,
        Some(json!({
            "reason": "task_timeout"
        }))
    );

    let transitions = harness
        .app
        .list_transitions(&run_id)
        .await
        .expect("transitions should load");
    assert!(transitions
        .iter()
        .any(|record| record.transition_type == "task_timed_out"));
    assert!(transitions
        .iter()
        .any(|record| record.transition_type == "run_failed"));
}

#[tokio::test]
async fn event_poll_skips_task_past_timeout_before_recovery_scan() {
    let harness = TestHarness::start().await;
    harness.register_metadata("orders.fetch");

    harness
        .app
        .replace_draft(single_task_definition_with_metadata(
            "orders",
            TaskMetadata {
                handler: "orders.fetch".to_owned(),
                capability: Some(Capability::Task.as_str().to_owned()),
                queue: None,
                timeout_seconds: Some(1),
            },
        ))
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

    tokio::time::sleep(Duration::from_secs(2)).await;

    let poll = harness
        .app
        .poll_task(workflow_protocol::PollTask {
            worker_id: "worker-default".to_owned(),
            worker_name: "worker-default".to_owned(),
            queues: vec!["default".to_owned()],
            capabilities: vec![Capability::Task.as_str().to_owned()],
            max_inflight: 1,
        })
        .await
        .expect("poll should succeed");
    assert!(matches!(
        poll,
        workflow_protocol::PollTaskResponse::NoTaskAvailable
    ));

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("current time should be after unix epoch")
        .as_secs() as i64;
    harness
        .app
        .recovery_scan_at(now + 1)
        .await
        .expect("recovery scan should succeed");

    let run = harness
        .app
        .get_run(&run_id)
        .await
        .expect("run should load")
        .expect("run should exist");
    assert_eq!(run.status, "failed");
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
        self.register_metadata_with_options(handler_id, "default", None);
    }

    fn register_metadata_with_options(
        &self,
        handler_id: &str,
        queue: &str,
        timeout_seconds: Option<u64>,
    ) {
        let registration = self
            .registry
            .define_handler(handler_id)
            .default_queue(queue);
        if let Some(timeout_seconds) = timeout_seconds {
            registration.timeout_seconds(timeout_seconds);
        }
    }
}

fn single_task_definition(workflow_id: &str, handler_id: &str) -> FlowDefinition {
    single_task_definition_with_metadata(
        workflow_id,
        TaskMetadata {
            handler: handler_id.to_owned(),
            capability: None,
            queue: None,
            timeout_seconds: None,
        },
    )
}

fn single_task_definition_with_metadata(workflow_id: &str, task: TaskMetadata) -> FlowDefinition {
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
                task: Some(task),
                ..StepMetadata::default()
            },
        }],
        edges: Vec::new(),
    }
}
