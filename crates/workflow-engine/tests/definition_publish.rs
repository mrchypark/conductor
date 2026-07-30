use std::collections::BTreeMap;

use axum::body::Body;
use http::{Request, StatusCode};
use serde::Serialize;
use sha2::{Digest, Sha256};
use tempfile::TempDir;
use tower::ServiceExt;
use workflow_definition::{
    validate::ValidationError, BranchMetadata, Edge, EventTrigger, FlowDefinition, SleepMetadata,
    StepDefinition, StepKind, StepMetadata, TaskMetadata, Trigger, WorkflowDefaults,
    MAX_TASK_TIMEOUT_SECONDS,
};
use workflow_engine::{PublishDraftError, ValidationIssue, WorkflowEngineApp, WorkflowEngineError};
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
        publication_hash(
            &published_v1.definition,
            [(
                "reserve_inventory",
                expected_route("inventory.reserve", "task", "default", None),
            )],
        ),
        "published content hash should cover the immutable definition and resolved route"
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
        vec![ValidationIssue::from(ValidationError::MissingTrigger {
            workflow_id: "orders".to_owned(),
        })]
    );

    harness.register_task_handler("inventory.reserve.v2");
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
        publication_hash(
            &published_v2.definition,
            [(
                "reserve_inventory",
                expected_route("inventory.reserve.v2", "task", "default", None),
            )],
        ),
        "published content hash should cover the immutable definition and resolved route"
    );

    let published_versions = harness
        .app
        .list_published_versions("orders")
        .await
        .expect("published versions should load");
    let published_handlers = published_versions
        .iter()
        .map(|record| {
            let definition: FlowDefinition = serde_json::from_str(&record.definition_json)
                .expect("published definition JSON should decode");
            (
                record.workflow_version,
                definition.steps[0]
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
async fn definition_publish_preserves_explicit_task_routing_metadata() {
    let harness = TestHarness::start().await;
    harness.register_task_handler("inventory.reserve");

    harness
        .app
        .replace_draft(valid_definition_with_task_metadata(
            "orders",
            TaskMetadata {
                handler: "inventory.reserve".to_owned(),
                capability: Some(Capability::Task.as_str().to_owned()),
                queue: Some("priority".to_owned()),
                timeout_seconds: Some(45),
            },
        ))
        .await
        .expect("draft should replace");

    let validation = harness
        .app
        .validate_draft("orders")
        .await
        .expect("validation should complete");
    assert!(validation.ok, "valid draft should validate successfully");

    let published = harness
        .app
        .publish_draft("orders", &validation.content_hash)
        .await
        .expect("publish should succeed");

    assert_eq!(
        published.definition.steps[0].metadata.task,
        Some(TaskMetadata {
            handler: "inventory.reserve".to_owned(),
            capability: Some(Capability::Task.as_str().to_owned()),
            queue: Some("priority".to_owned()),
            timeout_seconds: Some(45),
        })
    );
    assert_eq!(
        published.content_hash,
        publication_hash(
            &published.definition,
            [(
                "reserve_inventory",
                expected_route("inventory.reserve", "task", "priority", Some(45)),
            )],
        )
    );
}

#[tokio::test]
async fn definition_publish_snapshots_registry_defaults_per_version() {
    let harness = TestHarness::start().await;
    harness
        .registry
        .define_handler("inventory.reserve")
        .capability(Capability::Task)
        .default_queue("orders-v1")
        .timeout_seconds(30);

    harness
        .app
        .replace_draft(valid_definition("orders", "inventory.reserve"))
        .await
        .expect("draft should replace");
    let validation = harness
        .app
        .validate_draft("orders")
        .await
        .expect("draft should validate");
    assert!(validation.ok);

    let published_v1 = harness
        .app
        .publish_draft("orders", &validation.content_hash)
        .await
        .expect("v1 should publish");
    let expected_v1_hash = publication_hash(
        &published_v1.definition,
        [(
            "reserve_inventory",
            expected_route("inventory.reserve", "task", "orders-v1", Some(30)),
        )],
    );
    assert_eq!(published_v1.content_hash, expected_v1_hash);

    harness
        .registry
        .define_handler("inventory.reserve")
        .capability(Capability::Task)
        .default_queue("orders-v2")
        .timeout_seconds(60);

    let reloaded_v1 = harness
        .app
        .published_definition("orders", 1)
        .await
        .expect("v1 lookup should succeed")
        .expect("v1 should remain retained");
    assert_eq!(reloaded_v1.content_hash, expected_v1_hash);

    let published_v2 = harness
        .app
        .publish_draft("orders", &validation.content_hash)
        .await
        .expect("v2 should publish against the new snapshot");
    assert_eq!(published_v2.workflow_version, 2);
    assert_eq!(
        published_v2.content_hash,
        publication_hash(
            &published_v2.definition,
            [(
                "reserve_inventory",
                expected_route("inventory.reserve", "task", "orders-v2", Some(60)),
            )],
        )
    );
    assert_ne!(published_v2.content_hash, published_v1.content_hash);
}

#[tokio::test]
async fn definition_publish_rejects_task_capability_mismatch() {
    let harness = TestHarness::start().await;
    harness.register_task_handler("inventory.reserve");

    harness
        .app
        .replace_draft(valid_definition_with_task_metadata(
            "orders",
            TaskMetadata {
                handler: "inventory.reserve".to_owned(),
                capability: Some("sync-http".to_owned()),
                queue: None,
                timeout_seconds: None,
            },
        ))
        .await
        .expect("draft should replace");

    let validation = harness
        .app
        .validate_draft("orders")
        .await
        .expect("validation should complete");
    assert!(validation.ok, "shape validation should still pass");

    let error = harness
        .app
        .publish_draft("orders", &validation.content_hash)
        .await
        .expect_err("publish should reject conflicting capability metadata");

    assert!(matches!(
        error,
        PublishDraftError::HandlerManifestMismatch { ref handler_id, ref capability }
            if handler_id == "inventory.reserve" && capability == "sync-http"
    ));
}

#[tokio::test]
async fn definition_draft_replacement_rejects_task_timeout_outside_supported_range() {
    let harness = TestHarness::start().await;
    let timeout_seconds = MAX_TASK_TIMEOUT_SECONDS + 1;

    let error = harness
        .app
        .replace_draft(valid_definition_with_task_metadata(
            "orders",
            TaskMetadata {
                handler: "inventory.reserve".to_owned(),
                capability: None,
                queue: None,
                timeout_seconds: Some(timeout_seconds),
            },
        ))
        .await
        .expect_err("draft replacement should reject an oversized task timeout");

    assert!(matches!(
        error,
        WorkflowEngineError::InvalidDefinition {
            error: ValidationError::TaskTimeoutOutOfRange {
                ref step_id,
                timeout_seconds: actual_timeout,
                max_timeout_seconds,
            }
        } if step_id == "reserve_inventory"
            && actual_timeout == timeout_seconds
            && max_timeout_seconds == MAX_TASK_TIMEOUT_SECONDS
    ));
}

#[tokio::test]
async fn definition_publish_rejects_registry_default_task_timeout_outside_supported_range() {
    let harness = TestHarness::start().await;
    let timeout_seconds = MAX_TASK_TIMEOUT_SECONDS + 1;
    harness
        .registry
        .define_handler("inventory.reserve")
        .capability(Capability::Task)
        .timeout_seconds(timeout_seconds);

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
    assert!(validation.ok, "definition should be semantically valid");

    let error = harness
        .app
        .publish_draft("orders", &validation.content_hash)
        .await
        .expect_err("publish should reject an oversized registry default timeout");

    assert!(matches!(
        error,
        PublishDraftError::TaskTimeoutOutOfRange {
            ref handler_id,
            timeout_seconds: actual_timeout,
            max_timeout_seconds,
        } if handler_id == "inventory.reserve"
            && actual_timeout == timeout_seconds
            && max_timeout_seconds == MAX_TASK_TIMEOUT_SECONDS
    ));
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
    let latest_definition: FlowDefinition = serde_json::from_str(&latest.definition_json)
        .expect("latest published definition JSON should decode");
    assert_eq!(
        latest_definition.steps[0]
            .metadata
            .task
            .as_ref()
            .expect("task metadata")
            .handler,
        "inventory.reserve.v2"
    );
}

#[tokio::test]
async fn concurrent_definition_publish_allocates_distinct_versions() {
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
    assert!(validation.ok);

    let left = harness.app.clone();
    let right = harness.app.clone();
    let content_hash = validation.content_hash.clone();

    let (left_result, right_result) = tokio::join!(
        left.publish_draft("orders", &content_hash),
        right.publish_draft("orders", &content_hash),
    );

    let mut versions = vec![
        left_result
            .expect("left publish should succeed")
            .workflow_version,
        right_result
            .expect("right publish should succeed")
            .workflow_version,
    ];
    versions.sort_unstable();
    assert_eq!(versions, vec![1, 2]);

    let published_versions = harness
        .app
        .list_published_versions("orders")
        .await
        .expect("published versions should load");
    assert_eq!(published_versions.len(), 2);
    assert_eq!(
        published_versions
            .iter()
            .map(|record| record.workflow_version)
            .collect::<Vec<_>>(),
        vec![1, 2]
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
    let harness = TestHarness::start().await;
    let compiled = WorkflowBuilder::new("orders")
        .workflow_version(7)
        .event_trigger("order.created")
        .task_step("fetch_order", "orders.fetch")
        .sleep_step("wait_retry", 30)
        .branch_step("check_priority", "event.data.priority == true")
        .task_step("notify_ready", "orders.notify")
        .edge("fetch_order", "wait_retry")
        .edge("wait_retry", "check_priority")
        .edge("check_priority", "notify_ready")
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
                            capability: None,
                            queue: None,
                            timeout_seconds: None,
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
                    id: "check_priority".to_owned(),
                    kind: StepKind::Branch,
                    metadata: StepMetadata {
                        branch: Some(BranchMetadata {
                            expression: "event.data.priority == true".to_owned(),
                        }),
                        ..StepMetadata::default()
                    },
                },
                StepDefinition {
                    id: "notify_ready".to_owned(),
                    kind: StepKind::Task,
                    metadata: StepMetadata {
                        task: Some(TaskMetadata {
                            handler: "orders.notify".to_owned(),
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
                    to: "wait_retry".to_owned(),
                },
                Edge {
                    from: "wait_retry".to_owned(),
                    to: "check_priority".to_owned(),
                },
                Edge {
                    from: "check_priority".to_owned(),
                    to: "notify_ready".to_owned(),
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
        .define_handler("orders.fetch")
        .capability(Capability::Task)
        .default_queue("orders")
        .timeout_seconds(45);
    harness
        .registry
        .define_handler("orders.notify")
        .capability(Capability::Task)
        .default_queue("orders");

    let manifest = harness.registry.manifest();
    assert_eq!(manifest.len(), 2);
    assert_eq!(
        manifest[0],
        HandlerRegistration {
            handler_id: "orders.fetch".to_owned(),
            capability: Capability::Task,
            default_queue: "orders".to_owned(),
            timeout_seconds: Some(45),
        }
    );
    assert_eq!(
        manifest[1],
        HandlerRegistration {
            handler_id: "orders.notify".to_owned(),
            capability: Capability::Task,
            default_queue: "orders".to_owned(),
            timeout_seconds: None,
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
    assert_eq!(
        published.content_hash,
        publication_hash(
            &published.definition,
            [
                (
                    "fetch_order",
                    expected_route("orders.fetch", "task", "orders", Some(45)),
                ),
                (
                    "notify_ready",
                    expected_route("orders.notify", "task", "orders", None),
                ),
            ],
        ),
        "every task step should contribute one resolved route to the publication envelope"
    );
}

#[tokio::test]
async fn http_definition_endpoints_manage_draft_validate_publish() {
    let harness = TestHarness::start().await;
    harness.register_task_handler("inventory.reserve");

    let router = harness.app.router();

    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/v1/workflows/orders/draft")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::to_vec(&valid_definition("orders", "inventory.reserve"))
                        .expect("draft should serialize"),
                ))
                .expect("request should build"),
        )
        .await
        .expect("draft request should succeed");
    assert_eq!(response.status(), StatusCode::OK);

    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/workflows/orders/draft:validate")
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("validate request should succeed");
    assert_eq!(response.status(), StatusCode::OK);
    let validation: workflow_engine::DefinitionValidation = serde_json::from_slice(
        &axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body should read"),
    )
    .expect("validation should deserialize");
    assert!(validation.ok);

    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/workflows/orders/draft:publish")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::to_vec(&serde_json::json!({
                        "content_hash": validation.content_hash,
                    }))
                    .expect("publish request should serialize"),
                ))
                .expect("request should build"),
        )
        .await
        .expect("publish request should succeed");
    assert_eq!(response.status(), StatusCode::OK);
    let published: workflow_engine::StoredWorkflowDefinition = serde_json::from_slice(
        &axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body should read"),
    )
    .expect("publish response should deserialize");
    assert_eq!(published.workflow_id, "orders");
    assert_eq!(published.workflow_version, 1);

    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/v1/workflows/orders")
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("summary request should succeed");
    assert_eq!(response.status(), StatusCode::OK);
    let summary: workflow_engine::WorkflowSummary = serde_json::from_slice(
        &axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body should read"),
    )
    .expect("summary should deserialize");
    assert_eq!(summary.workflow_id, "orders");
    assert!(summary.draft.is_some());
    assert_eq!(
        summary
            .latest_published
            .as_ref()
            .expect("latest published should exist")
            .workflow_version,
        1
    );

    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/v1/workflows/orders/versions")
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("versions request should succeed");
    assert_eq!(response.status(), StatusCode::OK);
    let versions: Vec<workflow_engine::StoredDefinitionInspection> = serde_json::from_slice(
        &axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body should read"),
    )
    .expect("versions should deserialize");
    assert_eq!(versions.len(), 1);
    assert_eq!(versions[0].workflow_version, 1);

    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/v1/workflows/orders/versions/1")
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("version request should succeed");
    assert_eq!(response.status(), StatusCode::OK);
    let version: workflow_engine::StoredDefinitionInspection = serde_json::from_slice(
        &axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body should read"),
    )
    .expect("version should deserialize");
    assert_eq!(version.workflow_version, 1);
}

#[tokio::test]
async fn http_definition_endpoints_return_expected_error_statuses() {
    let harness = TestHarness::start().await;
    let router = harness.app.router();

    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/workflows/missing/draft:validate")
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("validate request should succeed");
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/v1/workflows/orders/draft")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::to_vec(&valid_definition("orders", "inventory.reserve"))
                        .expect("draft should serialize"),
                ))
                .expect("request should build"),
        )
        .await
        .expect("draft request should succeed");
    assert_eq!(response.status(), StatusCode::OK);
    let draft: workflow_engine::StoredWorkflowDefinition = serde_json::from_slice(
        &axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body should read"),
    )
    .expect("draft should deserialize");

    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/workflows/orders/draft:publish")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::to_vec(&serde_json::json!({
                        "content_hash": draft.content_hash,
                    }))
                    .expect("publish request should serialize"),
                ))
                .expect("request should build"),
        )
        .await
        .expect("publish request should succeed");
    assert_eq!(response.status(), StatusCode::PRECONDITION_FAILED);

    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/workflows/orders/draft:validate")
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("validate request should succeed");
    assert_eq!(response.status(), StatusCode::OK);
    let validation: workflow_engine::DefinitionValidation = serde_json::from_slice(
        &axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body should read"),
    )
    .expect("validation should deserialize");
    assert!(validation.ok);

    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/v1/workflows/orders/draft")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::to_vec(&valid_definition("orders", "inventory.reserve.v2"))
                        .expect("draft should serialize"),
                ))
                .expect("request should build"),
        )
        .await
        .expect("draft request should succeed");
    assert_eq!(response.status(), StatusCode::OK);

    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/workflows/orders/draft:publish")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::to_vec(&serde_json::json!({
                        "content_hash": validation.content_hash,
                    }))
                    .expect("publish request should serialize"),
                ))
                .expect("request should build"),
        )
        .await
        .expect("publish request should succeed");
    assert_eq!(response.status(), StatusCode::CONFLICT);

    harness.register_task_handler("inventory.reserve");
    let timeout_seconds = MAX_TASK_TIMEOUT_SECONDS + 1;
    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/v1/workflows/orders/draft")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::to_vec(&valid_definition_with_task_metadata(
                        "orders",
                        TaskMetadata {
                            handler: "inventory.reserve".to_owned(),
                            capability: None,
                            queue: None,
                            timeout_seconds: Some(timeout_seconds),
                        },
                    ))
                    .expect("draft should serialize"),
                ))
                .expect("request should build"),
        )
        .await
        .expect("draft request should succeed");
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    harness
        .registry
        .define_handler("inventory.slow")
        .capability(Capability::Task)
        .timeout_seconds(timeout_seconds);
    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/v1/workflows/slow-orders/draft")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::to_vec(&valid_definition("slow-orders", "inventory.slow"))
                        .expect("draft should serialize"),
                ))
                .expect("request should build"),
        )
        .await
        .expect("draft request should succeed");
    assert_eq!(response.status(), StatusCode::OK);
    let draft: workflow_engine::StoredWorkflowDefinition = serde_json::from_slice(
        &axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body should read"),
    )
    .expect("draft should deserialize");

    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/workflows/slow-orders/draft:validate")
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("validate request should succeed");
    assert_eq!(response.status(), StatusCode::OK);

    let response = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/workflows/slow-orders/draft:publish")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::to_vec(&serde_json::json!({
                        "content_hash": draft.content_hash,
                    }))
                    .expect("publish request should serialize"),
                ))
                .expect("request should build"),
        )
        .await
        .expect("publish request should succeed");
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
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
            .define_handler(handler)
            .capability(Capability::Task);
    }
}

fn valid_definition(workflow_id: &str, handler: &str) -> FlowDefinition {
    valid_definition_with_task_metadata(
        workflow_id,
        TaskMetadata {
            handler: handler.to_owned(),
            capability: None,
            queue: None,
            timeout_seconds: None,
        },
    )
}

fn valid_definition_with_task_metadata(workflow_id: &str, task: TaskMetadata) -> FlowDefinition {
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

#[derive(Serialize)]
struct ExpectedPublicationExecutionMetadata {
    semantics_version: &'static str,
    task_routes: BTreeMap<String, ExpectedResolvedTaskRoute>,
}

#[derive(Serialize)]
struct ExpectedResolvedTaskRoute {
    handler: String,
    capability: String,
    queue: String,
    timeout_seconds: Option<u64>,
}

fn expected_route(
    handler: &str,
    capability: &str,
    queue: &str,
    timeout_seconds: Option<u64>,
) -> ExpectedResolvedTaskRoute {
    ExpectedResolvedTaskRoute {
        handler: handler.to_owned(),
        capability: capability.to_owned(),
        queue: queue.to_owned(),
        timeout_seconds,
    }
}

fn publication_hash<const N: usize>(
    definition: &FlowDefinition,
    routes: [(&str, ExpectedResolvedTaskRoute); N],
) -> String {
    let definition_json = serde_json::to_vec(definition).expect("definition should serialize");
    let execution_metadata_json = serde_json::to_vec(&ExpectedPublicationExecutionMetadata {
        semantics_version: "v1",
        task_routes: routes
            .into_iter()
            .map(|(step_id, route)| (step_id.to_owned(), route))
            .collect(),
    })
    .expect("execution metadata should serialize");
    let mut hasher = Sha256::new();
    hasher.update(b"conductor.workflow-publication.v1\0");
    hasher.update((definition_json.len() as u64).to_be_bytes());
    hasher.update(definition_json);
    hasher.update((execution_metadata_json.len() as u64).to_be_bytes());
    hasher.update(execution_metadata_json);
    format!("{:x}", hasher.finalize())
}
