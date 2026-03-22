use sha2::{Digest, Sha256};
use tempfile::TempDir;
use workflow_definition::{
    validate::ValidationError, BranchMetadata, Edge, EmitEventMetadata, EventTrigger,
    FlowDefinition, SleepMetadata, StepDefinition, StepKind, StepMetadata, TaskMetadata, Trigger,
    WaitForEventMetadata, WorkflowDefaults,
};
use workflow_engine::{PublishDraftError, ValidationIssue, WorkflowEngineApp};
use workflow_sdk::WorkflowBuilder;
use workflow_worker::{Capability, HandlerRegistration, WorkerRegistry};

#[tokio::test]
async fn definition_publish_creates_new_immutable_workflow_version() {
    let harness = TestHarness::start().await;
    harness.register_task_handler("inventory.reserve");

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
    assert_eq!(
        published_v1.content_hash,
        definition_hash(&published_v1.definition),
        "published content hash should match the immutable published payload"
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
        .register_task_handler("inventory.reserve.v2");
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
    assert_eq!(
        published_v2.content_hash,
        definition_hash(&published_v2.definition),
        "published content hash should match the immutable published payload"
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
                record.definition.steps[0]
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

    let pinned_run_v1 = harness
        .app
        .create_run_for_latest_published("run-001", "orders")
        .await
        .expect("run metadata should persist");
    assert_eq!(pinned_run_v1.run_id, "run-001");
    assert_eq!(pinned_run_v1.workflow_id, "orders");
    assert_eq!(pinned_run_v1.workflow_version, 1);

    harness.publish("orders", "inventory.reserve.v2").await;

    let persisted_pinned_run_v1 = harness
        .app
        .get_run("run-001")
        .await
        .expect("run lookup should succeed")
        .expect("run metadata should exist");
    assert_eq!(persisted_pinned_run_v1, pinned_run_v1);
    assert_eq!(persisted_pinned_run_v1.workflow_version, 1);

    let new_run_v2 = harness
        .app
        .create_run_for_latest_published("run-002", "orders")
        .await
        .expect("new run metadata should persist");
    assert_eq!(new_run_v2.workflow_version, 2);

    let persisted_new_run_v2 = harness
        .app
        .get_run("run-002")
        .await
        .expect("new run lookup should succeed")
        .expect("new run metadata should exist");
    assert_eq!(persisted_new_run_v2, new_run_v2);
    assert_eq!(persisted_new_run_v2.workflow_version, 2);
}

#[tokio::test]
async fn definition_publish_requires_handler_metadata_in_default_app_construction() {
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

    let publish = harness
        .app
        .publish_draft("orders", &validation.content_hash)
        .await
        .expect_err("default app path should not bypass handler validation");
    assert!(matches!(
        publish,
        PublishDraftError::HandlerManifestMismatch { ref handler_id, ref capability }
            if handler_id == "inventory.reserve" && capability == Capability::Task.as_str()
    ));
}

#[tokio::test]
async fn rust_builder_compiles_and_validates_before_publish() {
    fn fetch_order() {}

    let harness = TestHarness::start().await;
    let compiled = WorkflowBuilder::new("orders")
        .workflow_version(7)
        .event_trigger("order.created")
        .task_step("fetch_order", "orders.fetch")
        .sleep_step("wait_retry", 30)
        .wait_for_event_step("wait_payment", "payment.captured")
        .branch_step("check_priority", "payload.priority == true")
        .emit_event_step("emit_ready", "order.ready")
        .edge("fetch_order", "wait_retry")
        .edge("wait_retry", "wait_payment")
        .edge("wait_payment", "check_priority")
        .edge("check_priority", "emit_ready")
        .compile();

    assert_eq!(
        compiled,
        FlowDefinition {
            workflow_id: "orders".to_owned(),
            workflow_version: 7,
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
                    id: "wait_retry".to_owned(),
                    kind: StepKind::Sleep,
                    metadata: StepMetadata {
                        sleep: Some(SleepMetadata { seconds: 30 }),
                        ..StepMetadata::default()
                    },
                },
                StepDefinition {
                    id: "wait_payment".to_owned(),
                    kind: StepKind::WaitForEvent,
                    metadata: StepMetadata {
                        wait_for_event: Some(WaitForEventMetadata {
                            event_type: "payment.captured".to_owned(),
                        }),
                        ..StepMetadata::default()
                    },
                },
                StepDefinition {
                    id: "check_priority".to_owned(),
                    kind: StepKind::Branch,
                    metadata: StepMetadata {
                        branch: Some(BranchMetadata {
                            expression: "payload.priority == true".to_owned(),
                        }),
                        ..StepMetadata::default()
                    },
                },
                StepDefinition {
                    id: "emit_ready".to_owned(),
                    kind: StepKind::EmitEvent,
                    metadata: StepMetadata {
                        emit_event: Some(EmitEventMetadata {
                            event_type: "order.ready".to_owned(),
                        }),
                        ..StepMetadata::default()
                    },
                },
            ],
            edges: vec![
                Edge {
                    from: "fetch_order".to_owned(),
                    to: "wait_retry".to_owned(),
                },
                Edge {
                    from: "wait_retry".to_owned(),
                    to: "wait_payment".to_owned(),
                },
                Edge {
                    from: "wait_payment".to_owned(),
                    to: "check_priority".to_owned(),
                },
                Edge {
                    from: "check_priority".to_owned(),
                    to: "emit_ready".to_owned(),
                },
            ],
        }
    );

    harness
        .app
        .replace_draft(compiled)
        .await
        .expect("compiled draft should replace");

    let validation = harness
        .app
        .validate_draft("orders")
        .await
        .expect("compiled draft should validate");
    assert!(validation.ok);

    let missing_handler = harness
        .app
        .publish_draft("orders", &validation.content_hash)
        .await
        .expect_err("publish should reject missing handler metadata");
    assert!(matches!(
        missing_handler,
        PublishDraftError::HandlerManifestMismatch { ref handler_id, ref capability }
            if handler_id == "orders.fetch" && capability == Capability::Task.as_str()
    ));

    harness
        .registry
        .register("orders.fetch", fetch_order)
        .capability(Capability::Task)
        .default_queue("orders")
        .timeout_seconds(45);

    let manifest = harness.registry.manifest();
    assert_eq!(manifest.len(), 1);
    assert_eq!(
        manifest[0],
        HandlerRegistration {
            handler_id: "orders.fetch".to_owned(),
            capability: Capability::Task,
            default_queue: "orders".to_owned(),
            timeout_seconds: Some(45),
        }
    );

    let published = harness
        .app
        .publish_draft("orders", &validation.content_hash)
        .await
        .expect("publish should succeed once manifest metadata exists");

    assert_eq!(published.workflow_id, "orders");
    assert_eq!(published.workflow_version, 1);
    assert_eq!(
        published.definition.steps[0]
            .metadata
            .task
            .as_ref()
            .expect("task metadata")
            .handler,
        "orders.fetch"
    );
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

    async fn publish(&self, workflow_id: &str, handler: &str) {
        self.register_task_handler(handler);
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

    fn register_task_handler(&self, handler: &str) {
        self.registry
            .register(handler, noop_handler)
            .capability(Capability::Task);
    }
}

fn noop_handler() {}

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

fn definition_hash(definition: &FlowDefinition) -> String {
    let json = serde_json::to_vec(definition).expect("definition should serialize");
    let mut hasher = Sha256::new();
    hasher.update(json);
    format!("{:x}", hasher.finalize())
}
