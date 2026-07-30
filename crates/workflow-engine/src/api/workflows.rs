use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use serde::Deserialize;

use crate::{
    DefinitionValidation, PublishDraftError, StoredDefinitionInspection, StoredWorkflowDefinition,
    WorkflowEngineApp, WorkflowEngineError, WorkflowSummary,
};

use super::validated_list_limit;

#[derive(Debug, Deserialize)]
pub struct PublishDraftRequest {
    pub content_hash: String,
}

#[derive(Debug, Default, Deserialize)]
pub struct ListVersionsQuery {
    pub limit: Option<u32>,
    pub after_version: Option<i64>,
}

pub async fn put_draft(
    State(app): State<WorkflowEngineApp>,
    Path(workflow_id): Path<String>,
    Json(definition): Json<workflow_definition::FlowDefinition>,
) -> Result<Json<StoredWorkflowDefinition>, (StatusCode, String)> {
    if definition.workflow_id != workflow_id {
        return Err((
            StatusCode::BAD_REQUEST,
            "workflow_id in path and body must match".to_owned(),
        ));
    }
    app.replace_draft(definition)
        .await
        .map(Json)
        .map_err(workflow_error)
}

pub async fn get_workflow(
    State(app): State<WorkflowEngineApp>,
    Path(workflow_id): Path<String>,
) -> Result<Json<WorkflowSummary>, (StatusCode, String)> {
    app.workflow_summary(&workflow_id)
        .await
        .map(Json)
        .map_err(internal_error)
}

pub async fn validate_draft(
    State(app): State<WorkflowEngineApp>,
    Path(workflow_id): Path<String>,
) -> Result<Json<DefinitionValidation>, (StatusCode, String)> {
    app.validate_draft(&workflow_id)
        .await
        .map(Json)
        .map_err(workflow_error)
}

pub async fn publish_draft(
    State(app): State<WorkflowEngineApp>,
    Path(workflow_id): Path<String>,
    Json(request): Json<PublishDraftRequest>,
) -> Result<Json<StoredWorkflowDefinition>, (StatusCode, String)> {
    app.publish_draft(&workflow_id, &request.content_hash)
        .await
        .map(Json)
        .map_err(publish_error)
}

pub async fn list_versions(
    State(app): State<WorkflowEngineApp>,
    Path(workflow_id): Path<String>,
    Query(query): Query<ListVersionsQuery>,
) -> Result<Json<Vec<StoredDefinitionInspection>>, (StatusCode, String)> {
    let limit = validated_list_limit(query.limit)?;
    app.list_published_versions_page(&workflow_id, query.after_version, limit)
        .await
        .map(Json)
        .map_err(internal_error)
}

pub async fn get_version(
    State(app): State<WorkflowEngineApp>,
    Path((workflow_id, workflow_version)): Path<(String, i64)>,
) -> Result<Json<StoredDefinitionInspection>, (StatusCode, String)> {
    let definition = app
        .published_definition(&workflow_id, workflow_version)
        .await
        .map_err(internal_error)?;
    definition.map(Json).ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            "workflow version not found".to_owned(),
        )
    })
}

fn internal_error(error: impl ToString) -> (StatusCode, String) {
    (StatusCode::INTERNAL_SERVER_ERROR, error.to_string())
}

fn workflow_error(error: WorkflowEngineError) -> (StatusCode, String) {
    match error {
        WorkflowEngineError::DraftNotFound { .. }
        | WorkflowEngineError::PublishedDefinitionNotFound { .. } => {
            (StatusCode::NOT_FOUND, error.to_string())
        }
        WorkflowEngineError::InvalidDefinition { .. } => {
            (StatusCode::BAD_REQUEST, error.to_string())
        }
        other => internal_error(other),
    }
}

fn publish_error(error: PublishDraftError) -> (StatusCode, String) {
    match error {
        PublishDraftError::DraftNotFound { .. } => (StatusCode::NOT_FOUND, error.to_string()),
        PublishDraftError::DraftNotValidated { .. } => {
            (StatusCode::PRECONDITION_FAILED, error.to_string())
        }
        PublishDraftError::TaskTimeoutOutOfRange { .. } => {
            (StatusCode::BAD_REQUEST, error.to_string())
        }
        PublishDraftError::StaleDraftContent { .. }
        | PublishDraftError::ConcurrentPublishContention { .. }
        | PublishDraftError::DraftChangedDuringPublish { .. }
        | PublishDraftError::InvalidPublishedVersionSequence { .. }
        | PublishDraftError::WorkflowVersionExhausted { .. }
        | PublishDraftError::HandlerManifestMismatch { .. } => {
            (StatusCode::CONFLICT, error.to_string())
        }
        other => internal_error(other),
    }
}

#[cfg(test)]
mod tests {
    use axum::body::Body;
    use http::{Request, StatusCode};
    use serde_json::json;
    use tempfile::TempDir;
    use tower::ServiceExt;
    use workflow_definition::FlowDefinition;
    use workflow_worker::Capability;

    use crate::{
        StoredDefinitionClassification, StoredDefinitionDecodeDiagnosticCode,
        StoredDefinitionExecutionCompatibility, StoredDefinitionInspection, WorkflowEngineApp,
    };

    #[tokio::test]
    async fn version_inspection_is_lossless_for_every_retained_row() {
        let tempdir = TempDir::new().expect("temporary directory should exist");
        let app = WorkflowEngineApp::for_test(tempdir.path())
            .await
            .expect("test app should start");

        let unsupported_v1 = unsupported_definition_json("orders", 1, "wait_for_event");
        app.insert_raw_published_definition_for_test("orders", 1, &unsupported_v1, None)
            .await
            .expect("unsupported row should insert");
        app.insert_raw_published_definition_for_test("orders", 2, "{", None)
            .await
            .expect("malformed row should insert");
        let mut invalid_metadata_v3 = current_definition("orders", "orders.reserve");
        invalid_metadata_v3.workflow_version = 3;
        let invalid_metadata_definition_json =
            serde_json::to_string(&invalid_metadata_v3).expect("definition should serialize");
        app.insert_raw_published_definition_for_test(
            "orders",
            3,
            &invalid_metadata_definition_json,
            Some("{"),
        )
        .await
        .expect("row with malformed execution metadata should insert");

        app.worker_registry()
            .define_handler("orders.reserve")
            .capability(Capability::Task);
        app.replace_draft(current_definition("orders", "orders.reserve"))
            .await
            .expect("draft should replace");
        let validation = app
            .validate_draft("orders")
            .await
            .expect("draft should validate");
        assert!(validation.ok);
        let snapshot = app
            .publish_draft("orders", &validation.content_hash)
            .await
            .expect("snapshot should publish after retained rows");
        assert_eq!(snapshot.workflow_version, 4);

        let router = app.router();
        let summary_response = router
            .clone()
            .oneshot(get("/v1/workflows/orders"))
            .await
            .expect("summary request should complete");
        assert_eq!(summary_response.status(), StatusCode::OK);
        let summary: serde_json::Value = serde_json::from_slice(
            &axum::body::to_bytes(summary_response.into_body(), usize::MAX)
                .await
                .expect("summary body should read"),
        )
        .expect("summary wire JSON should decode");
        let latest_wire = summary
            .get("latest_published")
            .and_then(serde_json::Value::as_object)
            .expect("summary should contain a latest inspection object");
        assert!(latest_wire.get("definition").is_none());
        assert!(latest_wire
            .get("definition_json")
            .is_some_and(serde_json::Value::is_string));
        assert!(latest_wire
            .get("execution_metadata_json")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|raw| raw.contains("semantics_version")));
        assert_eq!(
            latest_wire.get("classification"),
            Some(&json!("snapshotted_v1"))
        );
        assert_eq!(latest_wire.get("new_run_eligible"), Some(&json!(true)));
        assert_eq!(latest_wire.get("decode_diagnostic"), Some(&json!(null)));

        let response = router
            .clone()
            .oneshot(get("/v1/workflows/orders/versions"))
            .await
            .expect("list request should complete");
        assert_eq!(response.status(), StatusCode::OK);
        let versions: Vec<StoredDefinitionInspection> = serde_json::from_slice(
            &axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .expect("list body should read"),
        )
        .expect("inspection list should deserialize");

        assert_eq!(
            versions
                .iter()
                .map(|version| version.workflow_version)
                .collect::<Vec<_>>(),
            vec![1, 2, 3, 4]
        );
        assert_eq!(versions[0].definition_json, unsupported_v1);
        assert_eq!(versions[0].execution_metadata_json, None);
        assert_eq!(versions[0].content_hash, "orders-v1");
        assert_eq!(versions[0].created_at, 1);
        assert_eq!(versions[0].published_at, Some(1));
        assert_eq!(versions[0].classification, None);
        assert_eq!(
            versions[0].execution_compatibility,
            StoredDefinitionExecutionCompatibility::Incompatible
        );
        assert!(!versions[0].new_run_eligible);
        assert_eq!(
            versions[0]
                .decode_diagnostic
                .as_ref()
                .expect("unsupported row should have a typed diagnostic")
                .code,
            StoredDefinitionDecodeDiagnosticCode::UnsupportedDefinition
        );

        assert_eq!(versions[1].definition_json, "{");
        assert_eq!(versions[1].classification, None);
        assert_eq!(
            versions[1].execution_compatibility,
            StoredDefinitionExecutionCompatibility::Incompatible
        );
        assert!(!versions[1].new_run_eligible);
        let malformed_diagnostic = versions[1]
            .decode_diagnostic
            .as_ref()
            .expect("malformed row should have a typed diagnostic");
        assert_eq!(
            malformed_diagnostic.code,
            StoredDefinitionDecodeDiagnosticCode::UnsupportedDefinition
        );
        assert!(!malformed_diagnostic.message.is_empty());

        assert_eq!(versions[2].execution_metadata_json.as_deref(), Some("{"));
        assert_eq!(versions[2].classification, None);
        assert_eq!(
            versions[2].execution_compatibility,
            StoredDefinitionExecutionCompatibility::Incompatible
        );
        assert!(!versions[2].new_run_eligible);
        assert_eq!(
            versions[2]
                .decode_diagnostic
                .as_ref()
                .map(|diagnostic| diagnostic.code),
            Some(StoredDefinitionDecodeDiagnosticCode::InvalidExecutionMetadata)
        );

        assert_eq!(
            versions[3].classification,
            Some(StoredDefinitionClassification::SnapshottedV1)
        );
        assert_eq!(
            versions[3].execution_compatibility,
            StoredDefinitionExecutionCompatibility::Compatible
        );
        assert!(versions[3].new_run_eligible);
        assert!(versions[3].execution_metadata_json.is_some());
        assert_eq!(versions[3].decode_diagnostic, None);

        for (workflow_version, expected_definition) in [
            (1, unsupported_v1.as_str()),
            (2, "{"),
            (3, invalid_metadata_definition_json.as_str()),
        ] {
            let response = router
                .clone()
                .oneshot(get(&format!(
                    "/v1/workflows/orders/versions/{workflow_version}"
                )))
                .await
                .expect("exact inspection request should complete");
            assert_eq!(response.status(), StatusCode::OK);
            let inspected: StoredDefinitionInspection = serde_json::from_slice(
                &axum::body::to_bytes(response.into_body(), usize::MAX)
                    .await
                    .expect("exact body should read"),
            )
            .expect("exact inspection should deserialize");
            assert_eq!(inspected.workflow_version, workflow_version);
            assert_eq!(inspected.definition_json, expected_definition);
            if workflow_version == 3 {
                assert_eq!(inspected.execution_metadata_json.as_deref(), Some("{"));
                assert_eq!(
                    inspected
                        .decode_diagnostic
                        .as_ref()
                        .map(|diagnostic| diagnostic.code),
                    Some(StoredDefinitionDecodeDiagnosticCode::InvalidExecutionMetadata)
                );
            }
        }

        let missing = router
            .oneshot(get("/v1/workflows/orders/versions/99"))
            .await
            .expect("missing request should complete");
        assert_eq!(missing.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn exact_inspection_accepts_retained_i64_version_identifiers() {
        const ABOVE_U32_MAX: i64 = u32::MAX as i64 + 1;

        let tempdir = TempDir::new().expect("temporary directory should exist");
        let app = WorkflowEngineApp::for_test(tempdir.path())
            .await
            .expect("test app should start");
        app.insert_raw_published_definition_for_test("wide-versions", -1, "negative", None)
            .await
            .expect("negative retained version should insert");
        app.insert_raw_published_definition_for_test(
            "wide-versions",
            ABOVE_U32_MAX,
            "above-u32",
            None,
        )
        .await
        .expect("wide retained version should insert");

        let router = app.router();
        for (workflow_version, raw_definition) in [(-1, "negative"), (ABOVE_U32_MAX, "above-u32")] {
            let response = router
                .clone()
                .oneshot(get(&format!(
                    "/v1/workflows/wide-versions/versions/{workflow_version}"
                )))
                .await
                .expect("exact inspection request should complete");
            assert_eq!(response.status(), StatusCode::OK);
            let inspected: StoredDefinitionInspection = serde_json::from_slice(
                &axum::body::to_bytes(response.into_body(), usize::MAX)
                    .await
                    .expect("inspection body should read"),
            )
            .expect("inspection response should decode");
            assert_eq!(inspected.workflow_version, workflow_version);
            assert_eq!(inspected.definition_json, raw_definition);
            assert!(!inspected.new_run_eligible);
        }

        let missing = router
            .oneshot(get(&format!(
                "/v1/workflows/wide-versions/versions/{}",
                ABOVE_U32_MAX + 1
            )))
            .await
            .expect("missing exact inspection request should complete");
        assert_eq!(missing.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn version_history_is_bounded_lossless_and_keyset_stable_across_i64_rows() {
        const ABOVE_U32_MAX: i64 = u32::MAX as i64 + 1;

        let tempdir = TempDir::new().expect("temporary directory should exist");
        let app = WorkflowEngineApp::for_test(tempdir.path())
            .await
            .expect("test app should start");
        app.insert_raw_published_definition_for_test("paged-versions", -1, "raw-negative", None)
            .await
            .expect("negative retained row should insert");
        for version in 1_i64..=100 {
            app.insert_raw_published_definition_for_test(
                "paged-versions",
                version,
                &format!("raw-{version}"),
                None,
            )
            .await
            .expect("retained row should insert");
        }
        app.insert_raw_published_definition_for_test(
            "paged-versions",
            ABOVE_U32_MAX,
            "raw-above-u32",
            None,
        )
        .await
        .expect("above-u32 retained row should insert");

        let router = app.router();
        let first = router
            .clone()
            .oneshot(get("/v1/workflows/paged-versions/versions"))
            .await
            .expect("default page request should complete");
        assert_eq!(first.status(), StatusCode::OK);
        let first: Vec<StoredDefinitionInspection> = serde_json::from_slice(
            &axum::body::to_bytes(first.into_body(), usize::MAX)
                .await
                .expect("first page body should read"),
        )
        .expect("first page should decode");
        assert_eq!(first.len(), 100);
        assert_eq!(first.first().unwrap().workflow_version, -1);
        assert_eq!(first.first().unwrap().definition_json, "raw-negative");
        assert!(first.first().unwrap().decode_diagnostic.is_some());
        assert_eq!(first.last().unwrap().workflow_version, 99);

        let second = router
            .clone()
            .oneshot(get(
                "/v1/workflows/paged-versions/versions?limit=100&after_version=99",
            ))
            .await
            .expect("second page request should complete");
        assert_eq!(second.status(), StatusCode::OK);
        let second: Vec<StoredDefinitionInspection> = serde_json::from_slice(
            &axum::body::to_bytes(second.into_body(), usize::MAX)
                .await
                .expect("second page body should read"),
        )
        .expect("second page should decode");
        assert_eq!(
            second
                .iter()
                .map(|row| row.workflow_version)
                .collect::<Vec<_>>(),
            vec![100, ABOVE_U32_MAX]
        );
        assert_eq!(second[1].definition_json, "raw-above-u32");
        assert!(second[1].decode_diagnostic.is_some());

        let all_versions = first
            .iter()
            .chain(second.iter())
            .map(|row| row.workflow_version)
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(all_versions.len(), 102, "pages must not duplicate rows");

        for uri in [
            "/v1/workflows/paged-versions/versions?limit=0",
            "/v1/workflows/paged-versions/versions?limit=101",
            "/v1/workflows/paged-versions/versions?limit=not-a-number",
            "/v1/workflows/paged-versions/versions?after_version=not-a-number",
        ] {
            let response = router
                .clone()
                .oneshot(get(uri))
                .await
                .expect("invalid page request should complete");
            assert_eq!(response.status(), StatusCode::BAD_REQUEST, "uri={uri}");
        }
    }

    #[tokio::test]
    async fn latest_inspection_and_new_run_selection_have_distinct_semantics() {
        let tempdir = TempDir::new().expect("temporary directory should exist");
        let app = WorkflowEngineApp::for_test(tempdir.path())
            .await
            .expect("test app should start");
        app.worker_registry()
            .define_handler("orders.reserve")
            .capability(Capability::Task);
        app.replace_draft(current_definition("orders", "orders.reserve"))
            .await
            .expect("draft should replace");
        let validation = app
            .validate_draft("orders")
            .await
            .expect("draft should validate");
        app.publish_draft("orders", &validation.content_hash)
            .await
            .expect("eligible snapshot should publish");

        let unsupported_v2 = unsupported_definition_json("orders", 2, "emit_event");
        app.insert_raw_published_definition_for_test("orders", 2, &unsupported_v2, None)
            .await
            .expect("newer unsupported row should insert");

        let latest = app
            .latest_published_definition("orders")
            .await
            .expect("latest inspection should load")
            .expect("latest retained row should exist");
        assert_eq!(latest.workflow_version, 2);
        assert_eq!(latest.definition_json, unsupported_v2);
        assert_eq!(latest.classification, None);
        assert_eq!(
            latest.execution_compatibility,
            StoredDefinitionExecutionCompatibility::Incompatible
        );
        assert_eq!(
            latest
                .decode_diagnostic
                .as_ref()
                .expect("unsupported row should have a typed diagnostic")
                .code,
            StoredDefinitionDecodeDiagnosticCode::UnsupportedDefinition
        );
        assert!(!latest.new_run_eligible);

        let run = app
            .create_run_for_latest_published("run-from-snapshot", "orders")
            .await
            .expect("new run should select the older eligible snapshot");
        assert_eq!(run.workflow_version, 1);
    }

    fn get(uri: &str) -> Request<Body> {
        Request::builder()
            .method("GET")
            .uri(uri)
            .body(Body::empty())
            .expect("request should build")
    }

    fn current_definition(workflow_id: &str, handler: &str) -> FlowDefinition {
        serde_json::from_value(json!({
            "workflow_id": workflow_id,
            "workflow_version": 0,
            "defaults": {},
            "trigger": {"type": "event", "event_type": "order.created"},
            "steps": [{
                "id": "reserve",
                "kind": "task",
                "task": {"handler": handler}
            }],
            "edges": []
        }))
        .expect("current definition should decode")
    }

    fn unsupported_definition_json(workflow_id: &str, version: u32, kind: &str) -> String {
        let (metadata_name, metadata) = match kind {
            "wait_for_event" => (
                "wait_for_event",
                json!({"event_type": "payment.confirmed", "timeout_seconds": 60}),
            ),
            "emit_event" => ("emit_event", json!({"event_type": "payment.confirmed"})),
            other => panic!("unexpected unsupported kind: {other}"),
        };
        let mut step = serde_json::Map::new();
        step.insert("id".to_owned(), json!("unsupported_step"));
        step.insert("kind".to_owned(), json!(kind));
        step.insert(metadata_name.to_owned(), metadata);
        json!({
            "workflow_id": workflow_id,
            "workflow_version": version,
            "defaults": {},
            "trigger": {"type": "event", "event_type": "order.created"},
            "steps": [step],
            "edges": []
        })
        .to_string()
    }
}
