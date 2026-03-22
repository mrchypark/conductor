use serde_json::json;
use tempfile::TempDir;
use workflow_definition::{
    Edge, EventTrigger, FlowDefinition, SleepMetadata, StepDefinition, StepKind, StepMetadata,
    TaskMetadata, Trigger, WorkflowDefaults,
};
use workflow_engine::WorkflowEngineApp;
use workflow_worker::{Capability, InProcessWorker, RunOnceOutcome, WorkerRegistry};

#[tokio::test]
async fn sleep_timer_fires_and_schedules_downstream_task() {
    let harness = TestHarness::start().await;
    harness.register_metadata("notifications.send", "default");

    harness
        .app
        .replace_draft(sleep_definition())
        .await
        .expect("sleep workflow draft should replace");
    let validation = harness
        .app
        .validate_draft("sleep-flow")
        .await
        .expect("sleep workflow should validate");
    assert!(validation.ok);
    harness
        .app
        .publish_draft("sleep-flow", &validation.content_hash)
        .await
        .expect("sleep workflow should publish");

    let started = harness
        .app
        .ingest_event("reminder.requested", json!({ "order_id": "ord-123" }))
        .await
        .expect("start event should ingest");
    let run_id = started[0].run_id.clone();

    let steps = harness
        .app
        .list_step_instances(&run_id)
        .await
        .expect("steps should load");
    assert_eq!(steps.len(), 1);
    assert_eq!(steps[0].step_id, "sleep_before_notify");
    assert_eq!(steps[0].status, "sleeping");

    harness
        .app
        .recovery_scan_at(now_unix_seconds() + 31)
        .await
        .expect("recovery should fire sleep timer");

    let worker = InProcessWorker::new(
        harness.app.clone(),
        "sleep-worker".to_owned(),
        "sleep-worker".to_owned(),
    )
    .queue("default")
    .capability(Capability::Task.as_str())
    .register_handler("notifications.send", |input| {
        Ok(json!({
            "notified_order_id": input["order_id"].clone(),
        }))
    });
    let outcome = worker.run_once().await.expect("worker should run");
    assert!(matches!(outcome, RunOnceOutcome::Completed { .. }));

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
        .expect("steps should reload");
    assert_eq!(steps.len(), 2);
    assert_eq!(steps[0].status, "completed");
    assert_eq!(steps[1].step_id, "send_notification");
    assert_eq!(steps[1].status, "completed");

    let transitions = harness
        .app
        .list_transitions(&run_id)
        .await
        .expect("transitions should load");
    assert!(transitions
        .iter()
        .any(|record| record.transition_type == "timer_scheduled"));
    assert!(transitions
        .iter()
        .any(|record| record.transition_type == "timer_fired"));
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

fn sleep_definition() -> FlowDefinition {
    FlowDefinition {
        workflow_id: "sleep-flow".to_owned(),
        workflow_version: 0,
        defaults: WorkflowDefaults::default(),
        trigger: Some(Trigger::Event(EventTrigger {
            event_type: "reminder.requested".to_owned(),
        })),
        steps: vec![
            StepDefinition {
                id: "sleep_before_notify".to_owned(),
                kind: StepKind::Sleep,
                metadata: StepMetadata {
                    sleep: Some(SleepMetadata { seconds: 30 }),
                    ..StepMetadata::default()
                },
            },
            StepDefinition {
                id: "send_notification".to_owned(),
                kind: StepKind::Task,
                metadata: StepMetadata {
                    task: Some(TaskMetadata {
                        handler: "notifications.send".to_owned(),
                    }),
                    ..StepMetadata::default()
                },
            },
        ],
        edges: vec![Edge {
            from: "sleep_before_notify".to_owned(),
            to: "send_notification".to_owned(),
        }],
    }
}

fn now_unix_seconds() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};

    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("current time should be after unix epoch")
        .as_secs() as i64
}
