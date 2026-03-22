use serde_json::json;
use tempfile::TempDir;
use workflow_definition::{
    EventTrigger, FlowDefinition, RetryPolicy, StepDefinition, StepKind, StepMetadata,
    TaskMetadata, Trigger, WorkflowDefaults,
};
use workflow_engine::{TaskRuntimeError, WorkflowEngineApp};
use workflow_protocol::{FailurePayload, PollTask};
use workflow_worker::{Capability, InProcessWorker, WorkerRegistry};

#[tokio::test]
async fn recovery_requeues_expired_leases_and_fires_due_timers_once() {
    let harness = TestHarness::start().await;
    harness.register_metadata("lease.handler", "lease");
    harness.register_metadata("retry.handler", "retry");

    harness
        .app
        .replace_draft(lease_definition())
        .await
        .expect("lease definition should replace");
    let validation = harness
        .app
        .validate_draft("lease-flow")
        .await
        .expect("lease definition should validate");
    assert!(validation.ok);
    harness
        .app
        .publish_draft("lease-flow", &validation.content_hash)
        .await
        .expect("lease definition should publish");

    harness
        .app
        .replace_draft(retry_definition())
        .await
        .expect("retry definition should replace");
    let validation = harness
        .app
        .validate_draft("retry-flow")
        .await
        .expect("retry definition should validate");
    assert!(validation.ok);
    harness
        .app
        .publish_draft("retry-flow", &validation.content_hash)
        .await
        .expect("retry definition should publish");

    let lease_runs = harness
        .app
        .ingest_event("lease.created", json!({ "job_id": "lease-1" }))
        .await
        .expect("lease event should start");
    let retry_runs = harness
        .app
        .ingest_event("retry.created", json!({ "job_id": "retry-1" }))
        .await
        .expect("retry event should start");

    let leased = harness
        .app
        .poll_task(PollTask {
            worker_id: "lease-worker".to_owned(),
            worker_name: "lease-worker".to_owned(),
            queues: vec!["lease".to_owned()],
            capabilities: vec![Capability::Task.as_str().to_owned()],
            max_inflight: 1,
        })
        .await
        .expect("lease task should poll");
    let assigned = match leased {
        workflow_protocol::PollTaskResponse::Assigned(task) => *task,
        workflow_protocol::PollTaskResponse::NoTaskAvailable => panic!("expected lease task"),
    };
    assert_eq!(assigned.attempt_id, "attempt-1");
    assert_eq!(assigned.fencing_token, 1);

    let retry_worker = InProcessWorker::new(
        harness.app.clone(),
        "retry-worker".to_owned(),
        "retry-worker".to_owned(),
    )
    .queue("retry")
    .capability(Capability::Task.as_str())
    .register_handler("retry.handler", |_input| {
        Err(FailurePayload {
            classification: "application_error".to_owned(),
            code: "RETRY_ME".to_owned(),
            message: "please retry".to_owned(),
            retryable: true,
            details: json!({}),
        })
    });
    retry_worker
        .run_once()
        .await
        .expect("retry handler should fail and schedule retry");

    let advanced_now = now_unix_seconds() + 60;
    harness
        .app
        .recovery_scan_at(advanced_now)
        .await
        .expect("recovery should run");
    harness
        .app
        .recovery_scan_at(advanced_now)
        .await
        .expect("recovery should be idempotent");

    let requeued_lease = harness
        .app
        .poll_task(PollTask {
            worker_id: "lease-worker-2".to_owned(),
            worker_name: "lease-worker-2".to_owned(),
            queues: vec!["lease".to_owned()],
            capabilities: vec![Capability::Task.as_str().to_owned()],
            max_inflight: 1,
        })
        .await
        .expect("lease task should repoll");
    let requeued_lease = match requeued_lease {
        workflow_protocol::PollTaskResponse::Assigned(task) => *task,
        workflow_protocol::PollTaskResponse::NoTaskAvailable => panic!("expected requeued lease"),
    };
    assert_eq!(requeued_lease.task_id, assigned.task_id);
    assert_eq!(requeued_lease.attempt_id, "attempt-2");
    assert_eq!(requeued_lease.fencing_token, 2);

    let stale_completion = harness
        .app
        .complete_task(workflow_protocol::CompleteTask {
            lease: workflow_protocol::TaskMutationContext {
                task_id: assigned.task_id.clone(),
                attempt_id: assigned.attempt_id.clone(),
                worker_id: "lease-worker".to_owned(),
                fencing_token: assigned.fencing_token,
            },
            result: workflow_protocol::TaskOutput {
                payload: json!({
                    "leased_job_id": "lease-1",
                    "stale": true,
                }),
            },
        })
        .await
        .expect_err("stale lease completion should be rejected after reassignment");
    assert!(matches!(
        stale_completion,
        TaskRuntimeError::StaleLease { ref task_id } if task_id == &assigned.task_id
    ));

    let success_worker = InProcessWorker::new(
        harness.app.clone(),
        "retry-worker-2".to_owned(),
        "retry-worker-2".to_owned(),
    )
    .queue("retry")
    .capability(Capability::Task.as_str())
    .register_handler("retry.handler", |input| {
        Ok(json!({
            "retried_job_id": input["job_id"].clone(),
        }))
    });
    success_worker
        .run_once()
        .await
        .expect("retry task should succeed after recovery");

    harness
        .app
        .complete_task(workflow_protocol::CompleteTask {
            lease: workflow_protocol::TaskMutationContext {
                task_id: requeued_lease.task_id.clone(),
                attempt_id: requeued_lease.attempt_id.clone(),
                worker_id: "lease-worker-2".to_owned(),
                fencing_token: requeued_lease.fencing_token,
            },
            result: workflow_protocol::TaskOutput {
                payload: json!({
                    "leased_job_id": "lease-1",
                }),
            },
        })
        .await
        .expect("requeued lease task should complete");

    let lease_run = harness
        .app
        .get_run(&lease_runs[0].run_id)
        .await
        .expect("lease run should load")
        .expect("lease run should exist");
    assert_eq!(lease_run.status, "completed");

    let retry_run = harness
        .app
        .get_run(&retry_runs[0].run_id)
        .await
        .expect("retry run should load")
        .expect("retry run should exist");
    assert_eq!(retry_run.status, "completed");

    let retry_steps = harness
        .app
        .list_step_instances(&retry_runs[0].run_id)
        .await
        .expect("retry steps should load");
    assert_eq!(retry_steps.len(), 1);
    assert_eq!(retry_steps[0].attempt_count, 2);
    assert_eq!(retry_steps[0].status, "completed");

    let retry_transitions = harness
        .app
        .list_transitions(&retry_runs[0].run_id)
        .await
        .expect("retry transitions should load");
    let retry_timer_fired_count = retry_transitions
        .iter()
        .filter(|record| record.transition_type == "timer_fired")
        .count();
    assert_eq!(retry_timer_fired_count, 1);
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

fn lease_definition() -> FlowDefinition {
    FlowDefinition {
        workflow_id: "lease-flow".to_owned(),
        workflow_version: 0,
        defaults: WorkflowDefaults::default(),
        trigger: Some(Trigger::Event(EventTrigger {
            event_type: "lease.created".to_owned(),
        })),
        steps: vec![StepDefinition {
            id: "lease_task".to_owned(),
            kind: StepKind::Task,
            metadata: StepMetadata {
                task: Some(TaskMetadata {
                    handler: "lease.handler".to_owned(),
                }),
                ..StepMetadata::default()
            },
        }],
        edges: vec![],
    }
}

fn retry_definition() -> FlowDefinition {
    FlowDefinition {
        workflow_id: "retry-flow".to_owned(),
        workflow_version: 0,
        defaults: WorkflowDefaults {
            retry_policy: Some(RetryPolicy {
                max_attempts: 2,
                initial_interval_seconds: 0,
                backoff_coefficient: 2,
                max_interval_seconds: 0,
            }),
        },
        trigger: Some(Trigger::Event(EventTrigger {
            event_type: "retry.created".to_owned(),
        })),
        steps: vec![StepDefinition {
            id: "retry_task".to_owned(),
            kind: StepKind::Task,
            metadata: StepMetadata {
                task: Some(TaskMetadata {
                    handler: "retry.handler".to_owned(),
                }),
                ..StepMetadata::default()
            },
        }],
        edges: vec![],
    }
}

fn now_unix_seconds() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};

    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time should be after epoch")
        .as_secs() as i64
}
