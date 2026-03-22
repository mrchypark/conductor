use serde_json::json;
use tempfile::TempDir;
use workflow_definition::{
    EventTrigger, FlowDefinition, StepDefinition, StepKind, StepMetadata, Trigger,
    WaitForEventMetadata, WorkflowDefaults,
};
use workflow_engine::WorkflowEngineApp;

#[tokio::test]
async fn wait_timeout_marks_step_timed_out_and_fails_run() {
    let harness = TestHarness::start().await;

    harness
        .app
        .replace_draft(timeout_definition())
        .await
        .expect("timeout workflow draft should replace");
    let validation = harness
        .app
        .validate_draft("timeout-flow")
        .await
        .expect("timeout workflow should validate");
    assert!(validation.ok);
    harness
        .app
        .publish_draft("timeout-flow", &validation.content_hash)
        .await
        .expect("timeout workflow should publish");

    let started = harness
        .app
        .ingest_event("approval.requested", json!({ "request_id": "req-1" }))
        .await
        .expect("start event should ingest");
    let run_id = started[0].run_id.clone();

    let waits = harness
        .app
        .list_waiters(&run_id)
        .await
        .expect("waiters should load");
    assert_eq!(waits.len(), 1);
    assert_eq!(waits[0].status, "waiting");

    harness
        .app
        .recovery_scan_at(now_unix_seconds() + 31)
        .await
        .expect("recovery should process wait timeout");

    let run = harness
        .app
        .get_run(&run_id)
        .await
        .expect("run should load")
        .expect("run should exist");
    assert_eq!(run.status, "failed");

    let waits = harness
        .app
        .list_waiters(&run_id)
        .await
        .expect("waiters should reload");
    assert_eq!(waits[0].status, "timed_out");

    let steps = harness
        .app
        .list_step_instances(&run_id)
        .await
        .expect("steps should load");
    assert_eq!(steps.len(), 1);
    assert_eq!(steps[0].step_id, "wait_for_approval");
    assert_eq!(steps[0].status, "timed_out");

    let transitions = harness
        .app
        .list_transitions(&run_id)
        .await
        .expect("transitions should load");
    assert!(transitions
        .iter()
        .any(|record| record.transition_type == "waiter_timed_out"));
    assert!(transitions
        .iter()
        .any(|record| record.transition_type == "run_failed"));
}

struct TestHarness {
    _tempdir: TempDir,
    app: WorkflowEngineApp,
}

impl TestHarness {
    async fn start() -> Self {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let app = WorkflowEngineApp::for_test(tempdir.path())
            .await
            .expect("app should start");

        Self {
            _tempdir: tempdir,
            app,
        }
    }
}

fn timeout_definition() -> FlowDefinition {
    FlowDefinition {
        workflow_id: "timeout-flow".to_owned(),
        workflow_version: 0,
        defaults: WorkflowDefaults::default(),
        trigger: Some(Trigger::Event(EventTrigger {
            event_type: "approval.requested".to_owned(),
        })),
        steps: vec![StepDefinition {
            id: "wait_for_approval".to_owned(),
            kind: StepKind::WaitForEvent,
            metadata: StepMetadata {
                wait_for_event: Some(WaitForEventMetadata {
                    event_type: "approval.granted".to_owned(),
                    timeout_seconds: Some(30),
                }),
                ..StepMetadata::default()
            },
        }],
        edges: Vec::new(),
    }
}

fn now_unix_seconds() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};

    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("current time should be after unix epoch")
        .as_secs() as i64
}
