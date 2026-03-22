use tempfile::TempDir;
use workflow_definition::{
    validate::ValidationError, Edge, EventTrigger, FlowDefinition, StepDefinition, StepKind,
    StepMetadata, TaskMetadata, Trigger, WorkflowDefaults,
};
use workflow_engine::{PublishDraftError, ValidationIssue, WorkflowEngineApp};

#[tokio::test]
async fn definition_publish_creates_new_immutable_workflow_version() {
    let harness = TestHarness::start().await;

    harness
        .app
        .replace_draft(valid_definition("orders", "inventory.reserve"))
        .await
        .expect("draft should replace");

    let validation = harness
        .app
        .validate_draft("orders")
        .await
        .expect("validation should complete");
    assert!(validation.ok, "valid draft should validate successfully");
    assert!(validation.errors.is_empty());
    assert_eq!(validation.warnings, Vec::<ValidationIssue>::new());
    assert!(!validation.content_hash.is_empty());

    let published_v1 = harness
        .app
        .publish_draft("orders", &validation.content_hash)
        .await
        .expect("publish should succeed");
    assert_eq!(published_v1.workflow_version, 1);
    assert_eq!(
        published_v1.definition.steps[0]
            .metadata
            .task
            .as_ref()
            .expect("task metadata")
            .handler,
        "inventory.reserve"
    );

    harness
        .app
        .replace_draft(invalid_definition_missing_trigger("orders"))
        .await
        .expect("invalid draft still replaces");

    let invalid_validation = harness
        .app
        .validate_draft("orders")
        .await
        .expect("validation should complete for invalid draft");
    assert!(!invalid_validation.ok);
    assert_eq!(
        invalid_validation.errors,
        vec![ValidationIssue::from(ValidationError::MissingTrigger)]
    );

    harness
        .app
        .replace_draft(valid_definition("orders", "inventory.reserve.v2"))
        .await
        .expect("second draft should replace");

    let stale_publish = harness
        .app
        .publish_draft("orders", &validation.content_hash)
        .await
        .expect_err("stale validation hash must be rejected");
    assert!(matches!(
        stale_publish,
        PublishDraftError::StaleDraftContent { workflow_id, .. } if workflow_id == "orders"
    ));

    let validation_v2 = harness
        .app
        .validate_draft("orders")
        .await
        .expect("second validation should complete");
    let published_v2 = harness
        .app
        .publish_draft("orders", &validation_v2.content_hash)
        .await
        .expect("second publish should succeed");
    assert_eq!(published_v2.workflow_version, 2);
    assert_eq!(
        published_v2.definition.steps[0]
            .metadata
            .task
            .as_ref()
            .expect("task metadata")
            .handler,
        "inventory.reserve.v2"
    );

    let published_versions = harness
        .app
        .list_published_versions("orders")
        .await
        .expect("published versions should load");
    let published_handlers = published_versions
        .iter()
        .map(|record| {
            (
                record.workflow_version,
                record
                    .definition
                    .steps[0]
                    .metadata
                    .task
                    .as_ref()
                    .expect("task metadata")
                    .handler
                    .clone(),
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        published_handlers,
        vec![
            (1, "inventory.reserve".to_owned()),
            (2, "inventory.reserve.v2".to_owned())
        ]
    );
}

#[tokio::test]
async fn definition_publish_uses_latest_published_version_for_default_lookup() {
    let harness = TestHarness::start().await;

    harness.publish("orders", "inventory.reserve").await;
    harness.publish("orders", "inventory.reserve.v2").await;

    let latest = harness
        .app
        .latest_published_definition("orders")
        .await
        .expect("latest published definition should load")
        .expect("published definition should exist");

    assert_eq!(latest.workflow_version, 2);
    assert_eq!(
        latest.definition.steps[0]
            .metadata
            .task
            .as_ref()
            .expect("task metadata")
            .handler,
        "inventory.reserve.v2"
    );
}

#[tokio::test]
async fn definition_publish_persists_run_pinning_metadata() {
    let harness = TestHarness::start().await;

    harness.publish("orders", "inventory.reserve").await;
    harness.publish("orders", "inventory.reserve.v2").await;

    let created_run = harness
        .app
        .create_run_for_latest_published("run-001", "orders")
        .await
        .expect("run metadata should persist");
    assert_eq!(created_run.run_id, "run-001");
    assert_eq!(created_run.workflow_id, "orders");
    assert_eq!(created_run.workflow_version, 2);

    let persisted_run = harness
        .app
        .get_run("run-001")
        .await
        .expect("run lookup should succeed")
        .expect("run metadata should exist");
    assert_eq!(persisted_run, created_run);
}

struct TestHarness {
    _tempdir: TempDir,
    app: WorkflowEngineApp,
}

impl TestHarness {
    async fn start() -> Self {
        let tempdir = TempDir::new().expect("temporary directory should exist");
        let app = WorkflowEngineApp::for_test(tempdir.path())
            .await
            .expect("test app should start");

        Self {
            _tempdir: tempdir,
            app,
        }
    }

    async fn publish(&self, workflow_id: &str, handler: &str) {
        self.app
            .replace_draft(valid_definition(workflow_id, handler))
            .await
            .expect("draft should replace");
        let validation = self
            .app
            .validate_draft(workflow_id)
            .await
            .expect("draft should validate");
        assert!(validation.ok, "draft should validate successfully");
        self.app
            .publish_draft(workflow_id, &validation.content_hash)
            .await
            .expect("draft should publish");
    }
}

fn valid_definition(workflow_id: &str, handler: &str) -> FlowDefinition {
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
                    handler: handler.to_owned(),
                }),
                ..StepMetadata::default()
            },
        }],
        edges: vec![Edge {
            from: "reserve_inventory".to_owned(),
            to: "reserve_inventory".to_owned(),
        }],
    }
}

fn invalid_definition_missing_trigger(workflow_id: &str) -> FlowDefinition {
    FlowDefinition {
        trigger: None,
        ..valid_definition(workflow_id, "inventory.reserve.invalid")
    }
}
