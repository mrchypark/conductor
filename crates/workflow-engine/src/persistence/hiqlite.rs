use std::{collections::HashSet, net::TcpListener, path::Path};

use hiqlite::{start_node, Client, Node, NodeConfig, Param};
use serde::Deserialize;
use serde_json::Value;
use thiserror::Error;
use workflow_definition::FlowDefinition;
use workflow_protocol::{FailurePayload, PollTask, TaskMutationContext, TaskOutput};

use crate::{
    definition_store::{StoredWorkflowDefinition, WorkflowRunRecord},
    persistence::{
        schema::WORKFLOW_ENGINE_SCHEMA, CompletedTaskContinuation, ContinuationQuarantineResult,
        ContinuationQuarantineStaleReason, LatestEligibleDefinitionSelection,
        StoredDefinitionExecutionCompatibility, StoredDefinitionInspection,
        TaskExecutionQuarantineResult, TaskOutcomeApplyResult, TimerClaimResult,
    },
    runtime::tasks::{TaskSnapshot, TimerRecord},
    runtime::{
        MutationApplyResult, QueuedTaskRecord, StepInstanceRecord, TaskLeaseRecord,
        TransitionRecord,
    },
    stored_definition::{
        decode_stored_definition, DecodedStoredDefinition, StoredDefinitionDecodeError,
    },
};

const STATUS_DRAFT: &str = "draft";
const STATUS_PUBLISHED: &str = "published";
const TASK_STATUS_QUEUED: &str = "queued";
const TASK_STATUS_LEASED: &str = "leased";
const TASK_STATUS_COMPLETED: &str = "completed";
const TASK_STATUS_FAILED: &str = "failed";
const TASK_STATUS_TIMED_OUT: &str = "timed_out";
const TASK_STATUS_RETRY_PENDING: &str = "retry_pending";
const TASK_STATUS_CANCELLED: &str = "cancelled";
const RUN_STATUS_RUNNING: &str = "running";
const RUN_STATUS_COMPLETED: &str = "completed";
const RUN_STATUS_FAILED: &str = "failed";
const RUN_STATUS_CANCELLED: &str = "cancelled";
const CONTINUATION_STATE_PENDING: &str = "pending";
const CONTINUATION_STATE_APPLIED: &str = "applied";
const CONTINUATION_STATE_SUPPRESSED: &str = "suppressed";
const CONTINUATION_QUARANTINE_PREFIX: &str = "quarantined:";
const CONTINUATION_QUARANTINE_PATTERN: &str = "quarantined:%";
const TIMER_STATUS_PENDING: &str = "pending";
const TIMER_STATUS_FIRED: &str = "fired";
const TIMER_STATUS_CANCELLED: &str = "cancelled";
const TIMER_EFFECT_STATE_PENDING: &str = "pending";
const TIMER_EFFECT_STATE_APPLIED: &str = "applied";
const TIMER_EFFECT_STATE_SUPPRESSED: &str = "suppressed";
/// Maximum number of records from any one durable work class processed by a
/// single recovery cycle.
pub(crate) const RECOVERY_SCAN_BATCH_SIZE: i64 = 100;
/// Recovery deliberately bounds each timer scan to avoid monopolizing the runtime loop.
pub(crate) const DUE_TIMER_SCAN_BATCH_SIZE: i64 = RECOVERY_SCAN_BATCH_SIZE;

macro_rules! params {
    ( $( $param:expr ),* $(,)? ) => {
        vec![$(Param::from($param)),*]
    };
}

#[derive(Debug, Error)]
pub enum PersistenceError {
    #[error("hiqlite error: {0}")]
    Hiqlite(#[from] hiqlite::Error),
    #[error("json serialization error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("persistence invariant violation: {0}")]
    Invariant(String),
    #[error("stored definition '{workflow_id}' version {workflow_version} is not execution-compatible ({diagnostic_code}): {message}")]
    StoredDefinitionDecode {
        workflow_id: String,
        workflow_version: i64,
        diagnostic_code: String,
        message: String,
    },
    #[error(
        "workflow '{workflow_id}' has an invalid published version sequence spanning {minimum_version}..={maximum_version}"
    )]
    InvalidWorkflowVersionSequence {
        workflow_id: String,
        minimum_version: i64,
        maximum_version: i64,
    },
    #[error(
        "workflow '{workflow_id}' has exhausted the supported published version range at {latest_version}"
    )]
    WorkflowVersionExhausted {
        workflow_id: String,
        latest_version: i64,
    },
    #[error(
        "stored {entity_kind} '{entity_id}' references invalid workflow version {workflow_version}"
    )]
    InvalidStoredWorkflowVersion {
        entity_kind: &'static str,
        entity_id: String,
        workflow_version: i64,
    },
    #[error(
        "event inbox replay conflict for event_id={event_id:?} dedupe_key={dedupe_key:?}; existing event_id={existing_event_id:?}"
    )]
    EventInboxConflict {
        event_id: String,
        dedupe_key: Option<String>,
        existing_event_id: String,
    },
}

#[derive(Clone)]
pub struct HiqlitePersistence {
    client: Client,
}

impl HiqlitePersistence {
    pub async fn for_test(base_dir: &Path) -> Result<Self, PersistenceError> {
        std::fs::create_dir_all(base_dir).expect("test base directory should be creatable");

        let config = local_node_config(base_dir);
        let client = start_node(config).await?;
        for statement_result in client.batch(WORKFLOW_ENGINE_SCHEMA).await? {
            statement_result?;
        }
        apply_compat_migrations(&client).await?;

        Ok(Self { client })
    }

    pub async fn replace_draft(
        &self,
        record: &StoredWorkflowDefinition,
    ) -> Result<(), PersistenceError> {
        let definition_json = serde_json::to_string(&record.definition)?;
        self.client
            .execute(
                r#"
                INSERT INTO workflow_definitions (
                    workflow_id,
                    workflow_version,
                    status,
                    definition_json,
                    content_hash,
                    last_validated_content_hash,
                    created_at,
                    published_at
                )
                VALUES ($1, 0, $2, $3, $4, NULL, $5, NULL)
                ON CONFLICT(workflow_id, status, workflow_version)
                DO UPDATE SET
                    definition_json = excluded.definition_json,
                    content_hash = excluded.content_hash,
                    last_validated_content_hash = NULL,
                    created_at = excluded.created_at,
                    published_at = NULL
                "#,
                params!(
                    record.workflow_id.clone(),
                    STATUS_DRAFT,
                    definition_json,
                    record.content_hash.clone(),
                    record.created_at
                ),
            )
            .await?;
        Ok(())
    }

    pub async fn record_draft_validation(
        &self,
        workflow_id: &str,
        content_hash: Option<&str>,
    ) -> Result<(), PersistenceError> {
        self.client
            .execute(
                r#"
                UPDATE workflow_definitions
                SET last_validated_content_hash = $1
                WHERE workflow_id = $2 AND status = $3 AND workflow_version = 0
                "#,
                params!(content_hash, workflow_id.to_owned(), STATUS_DRAFT),
            )
            .await?;
        Ok(())
    }

    pub async fn get_draft(
        &self,
        workflow_id: &str,
    ) -> Result<Option<DraftDefinitionRecord>, PersistenceError> {
        let row = self
            .client
            .query_as_optional::<WorkflowDefinitionRow, _>(
                r#"
                SELECT
                    workflow_id,
                    workflow_version,
                    definition_json,
                    content_hash,
                    last_validated_content_hash,
                    execution_metadata_json,
                    created_at,
                    published_at
                FROM workflow_definitions
                WHERE workflow_id = $1 AND status = $2 AND workflow_version = 0
                "#,
                params!(workflow_id.to_owned(), STATUS_DRAFT),
            )
            .await?;

        row.map(TryInto::try_into).transpose()
    }

    pub async fn next_workflow_version(&self, workflow_id: &str) -> Result<u32, PersistenceError> {
        let bounds = self
            .client
            .query_as_optional::<VersionBoundsRow, _>(
                r#"
                SELECT MIN(workflow_version) AS minimum_workflow_version,
                    MAX(workflow_version) AS maximum_workflow_version
                FROM workflow_definitions
                WHERE workflow_id = $1 AND status = $2
                "#,
                params!(workflow_id.to_owned(), STATUS_PUBLISHED),
            )
            .await?;
        let Some((minimum_version, maximum_version)) = bounds.and_then(|row| {
            row.minimum_workflow_version
                .zip(row.maximum_workflow_version)
        }) else {
            return Ok(1);
        };
        if minimum_version < 1 || maximum_version > i64::from(u32::MAX) {
            return Err(PersistenceError::InvalidWorkflowVersionSequence {
                workflow_id: workflow_id.to_owned(),
                minimum_version,
                maximum_version,
            });
        }
        if maximum_version == i64::from(u32::MAX) {
            return Err(PersistenceError::WorkflowVersionExhausted {
                workflow_id: workflow_id.to_owned(),
                latest_version: maximum_version,
            });
        }
        u32::try_from(maximum_version + 1).map_err(|_| PersistenceError::WorkflowVersionExhausted {
            workflow_id: workflow_id.to_owned(),
            latest_version: maximum_version,
        })
    }

    pub async fn insert_published_definition(
        &self,
        record: &StoredWorkflowDefinition,
    ) -> Result<(), PersistenceError> {
        let definition_json = serde_json::to_string(&record.definition)?;
        self.client
            .execute(
                r#"
                INSERT INTO workflow_definitions (
                    workflow_id,
                    workflow_version,
                    status,
                    definition_json,
                    content_hash,
                    last_validated_content_hash,
                    execution_metadata_json,
                    created_at,
                    published_at
                )
                VALUES ($1, $2, $3, $4, $5, NULL, NULL, $6, $7)
                "#,
                params!(
                    record.workflow_id.clone(),
                    record.workflow_version,
                    STATUS_PUBLISHED,
                    definition_json,
                    record.content_hash.clone(),
                    record.created_at,
                    record.published_at
                ),
            )
            .await?;
        Ok(())
    }

    #[cfg(test)]
    pub(crate) async fn insert_snapshotted_published_definition(
        &self,
        record: &StoredWorkflowDefinition,
        execution_metadata_json: &str,
    ) -> Result<(), PersistenceError> {
        let definition_json = serde_json::to_string(&record.definition)?;
        self.client
            .execute(
                r#"
                INSERT INTO workflow_definitions (
                    workflow_id,
                    workflow_version,
                    status,
                    definition_json,
                    content_hash,
                    last_validated_content_hash,
                    execution_metadata_json,
                    created_at,
                    published_at
                )
                VALUES ($1, $2, $3, $4, $5, NULL, $6, $7, $8)
                "#,
                params!(
                    record.workflow_id.clone(),
                    record.workflow_version,
                    STATUS_PUBLISHED,
                    definition_json,
                    record.content_hash.clone(),
                    execution_metadata_json.to_owned(),
                    record.created_at,
                    record.published_at
                ),
            )
            .await?;
        Ok(())
    }

    /// Inserts an immutable publication only while the live draft is still
    /// the exact definition/hash that was validated for this attempt.
    pub(crate) async fn insert_snapshotted_published_definition_if_draft_validated(
        &self,
        record: &StoredWorkflowDefinition,
        execution_metadata_json: &str,
        expected_draft_definition_json: &str,
        expected_validated_content_hash: &str,
    ) -> Result<bool, PersistenceError> {
        let published_definition_json = serde_json::to_string(&record.definition)?;
        let inserted = self
            .client
            .execute(
                r#"
                INSERT INTO workflow_definitions (
                    workflow_id,
                    workflow_version,
                    status,
                    definition_json,
                    content_hash,
                    last_validated_content_hash,
                    execution_metadata_json,
                    created_at,
                    published_at
                )
                SELECT $1, $2, $3, $4, $5, NULL, $6, $7, $8
                FROM workflow_definitions draft
                WHERE draft.workflow_id = $1
                    AND draft.status = $9
                    AND draft.workflow_version = 0
                    AND draft.definition_json = $10
                    AND draft.content_hash = $11
                    AND draft.last_validated_content_hash = $11
                "#,
                params!(
                    record.workflow_id.clone(),
                    record.workflow_version,
                    STATUS_PUBLISHED,
                    published_definition_json,
                    record.content_hash.clone(),
                    execution_metadata_json.to_owned(),
                    record.created_at,
                    record.published_at,
                    STATUS_DRAFT,
                    expected_draft_definition_json.to_owned(),
                    expected_validated_content_hash.to_owned(),
                ),
            )
            .await?;
        Ok(inserted == 1)
    }

    #[cfg(test)]
    pub(crate) async fn insert_raw_published_definition_for_test(
        &self,
        workflow_id: &str,
        workflow_version: i64,
        definition_json: &str,
        execution_metadata_json: Option<&str>,
    ) -> Result<(), PersistenceError> {
        self.client
            .execute(
                r#"
                INSERT INTO workflow_definitions (
                    workflow_id, workflow_version, status, definition_json, content_hash,
                    last_validated_content_hash, execution_metadata_json, created_at, published_at
                )
                VALUES ($1, $2, $3, $4, $5, NULL, $6, $7, $7)
                "#,
                params!(
                    workflow_id.to_owned(),
                    workflow_version,
                    STATUS_PUBLISHED,
                    definition_json.to_owned(),
                    format!("{workflow_id}-v{workflow_version}"),
                    execution_metadata_json.map(ToOwned::to_owned),
                    workflow_version
                ),
            )
            .await?;
        Ok(())
    }

    /// Gets the highest retained published version, regardless of execution eligibility.
    pub(crate) async fn inspect_latest_published_definition(
        &self,
        workflow_id: &str,
    ) -> Result<Option<StoredDefinitionInspection>, PersistenceError> {
        let row = self
            .client
            .query_as_optional::<WorkflowDefinitionRow, _>(
                r#"
                SELECT
                    workflow_id,
                    workflow_version,
                    definition_json,
                    content_hash,
                    last_validated_content_hash,
                    execution_metadata_json,
                    created_at,
                    published_at
                FROM workflow_definitions
                WHERE workflow_id = $1 AND status = $2
                ORDER BY workflow_version DESC
                LIMIT 1
                "#,
                params!(workflow_id.to_owned(), STATUS_PUBLISHED),
            )
            .await?;

        Ok(row.map(inspect_stored_definition_row))
    }

    /// Lists every retained published row without treating decode failures as absence.
    pub(crate) async fn inspect_published_definitions(
        &self,
        workflow_id: &str,
    ) -> Result<Vec<StoredDefinitionInspection>, PersistenceError> {
        let rows = self
            .client
            .query_as::<WorkflowDefinitionRow, _>(
                r#"
                SELECT
                    workflow_id,
                    workflow_version,
                    definition_json,
                    content_hash,
                    last_validated_content_hash,
                    execution_metadata_json,
                    created_at,
                    published_at
                FROM workflow_definitions
                WHERE workflow_id = $1 AND status = $2
                ORDER BY workflow_version ASC
                "#,
                params!(workflow_id.to_owned(), STATUS_PUBLISHED),
            )
            .await?;

        Ok(rows
            .into_iter()
            .map(inspect_stored_definition_row)
            .collect())
    }

    /// Lists one stable ascending page of retained published rows without
    /// narrowing their signed 64-bit version identifiers.
    pub(crate) async fn inspect_published_definitions_page(
        &self,
        workflow_id: &str,
        after_version: Option<i64>,
        limit: u32,
    ) -> Result<Vec<StoredDefinitionInspection>, PersistenceError> {
        let rows = self
            .client
            .query_as::<WorkflowDefinitionRow, _>(
                r#"
                SELECT
                    workflow_id,
                    workflow_version,
                    definition_json,
                    content_hash,
                    last_validated_content_hash,
                    execution_metadata_json,
                    created_at,
                    published_at
                FROM workflow_definitions
                WHERE workflow_id = $1 AND status = $2
                    AND ($3 IS NULL OR workflow_version > $3)
                ORDER BY workflow_version ASC
                LIMIT $4
                "#,
                params!(
                    workflow_id.to_owned(),
                    STATUS_PUBLISHED,
                    after_version,
                    i64::from(limit),
                ),
            )
            .await?;

        Ok(rows
            .into_iter()
            .map(inspect_stored_definition_row)
            .collect())
    }

    /// Gets one retained published row for administrative inspection.
    pub(crate) async fn inspect_published_definition(
        &self,
        workflow_id: &str,
        workflow_version: i64,
    ) -> Result<Option<StoredDefinitionInspection>, PersistenceError> {
        let row = self
            .client
            .query_as_optional::<WorkflowDefinitionRow, _>(
                r#"
                SELECT
                    workflow_id,
                    workflow_version,
                    definition_json,
                    content_hash,
                    last_validated_content_hash,
                    execution_metadata_json,
                    created_at,
                    published_at
                FROM workflow_definitions
                WHERE workflow_id = $1 AND workflow_version = $2 AND status = $3
                LIMIT 1
                "#,
                params!(workflow_id.to_owned(), workflow_version, STATUS_PUBLISHED),
            )
            .await?;

        Ok(row.map(inspect_stored_definition_row))
    }

    /// Loads the exact published version pinned by an existing run.
    pub(crate) async fn load_published_definition_for_execution(
        &self,
        workflow_id: &str,
        workflow_version: u32,
    ) -> Result<Option<DecodedStoredDefinition>, PersistenceError> {
        let row = self
            .client
            .query_as_optional::<WorkflowDefinitionRow, _>(
                r#"
                SELECT
                    workflow_id,
                    workflow_version,
                    definition_json,
                    content_hash,
                    last_validated_content_hash,
                    execution_metadata_json,
                    created_at,
                    published_at
                FROM workflow_definitions
                WHERE workflow_id = $1 AND workflow_version = $2 AND status = $3
                LIMIT 1
                "#,
                params!(
                    workflow_id.to_owned(),
                    workflow_version as i64,
                    STATUS_PUBLISHED
                ),
            )
            .await?;

        row.map(decode_stored_definition_row).transpose()
    }

    /// Selects only the highest snapshotted version for a new run.
    pub(crate) async fn latest_eligible_published_definition(
        &self,
        workflow_id: &str,
    ) -> Result<LatestEligibleDefinitionSelection, PersistenceError> {
        let rows = self
            .client
            .query_as::<WorkflowDefinitionRow, _>(
                r#"
                SELECT
                    workflow_id,
                    workflow_version,
                    definition_json,
                    content_hash,
                    last_validated_content_hash,
                    execution_metadata_json,
                    created_at,
                    published_at
                FROM workflow_definitions
                WHERE workflow_id = $1 AND status = $2
                ORDER BY workflow_version DESC
                "#,
                params!(workflow_id.to_owned(), STATUS_PUBLISHED),
            )
            .await?;

        if rows.is_empty() {
            return Ok(LatestEligibleDefinitionSelection::NoPublishedDefinitions);
        }
        for row in rows {
            if let Ok(decoded) = decode_and_validate_stored_definition_row(&row) {
                if decoded.is_new_run_eligible() {
                    return Ok(LatestEligibleDefinitionSelection::Selected(Box::new(
                        decoded,
                    )));
                }
            }
        }

        Ok(LatestEligibleDefinitionSelection::NoEligiblePublishedDefinition)
    }

    /// Selects the highest snapshotted version for every workflow, ordered by workflow id.
    pub(crate) async fn list_latest_eligible_published_definitions(
        &self,
    ) -> Result<Vec<DecodedStoredDefinition>, PersistenceError> {
        let rows = self
            .client
            .query_as::<WorkflowDefinitionRow, _>(
                r#"
                SELECT
                    workflow_id,
                    workflow_version,
                    definition_json,
                    content_hash,
                    last_validated_content_hash,
                    execution_metadata_json,
                    created_at,
                    published_at
                FROM workflow_definitions
                WHERE status = $1
                ORDER BY workflow_id ASC, workflow_version DESC
                "#,
                params!(STATUS_PUBLISHED),
            )
            .await?;

        let mut selected = Vec::new();
        let mut selected_workflows = HashSet::new();
        for row in rows {
            if selected_workflows.contains(&row.workflow_id) {
                continue;
            }
            let workflow_id = row.workflow_id.clone();
            if let Ok(decoded) = decode_and_validate_stored_definition_row(&row) {
                if decoded.is_new_run_eligible() {
                    selected_workflows.insert(workflow_id);
                    selected.push(decoded);
                }
            }
        }

        Ok(selected)
    }

    pub async fn insert_run(&self, run: &WorkflowRunRecord) -> Result<(), PersistenceError> {
        self.insert_run_if_absent(run).await?;
        Ok(())
    }

    pub async fn insert_run_if_absent(
        &self,
        run: &WorkflowRunRecord,
    ) -> Result<bool, PersistenceError> {
        let inserted = self
            .client
            .execute(
                r#"
                INSERT OR IGNORE INTO workflow_runs (
                    run_id,
                    workflow_id,
                    workflow_version,
                    status,
                    cancel_reason,
                    cancel_requested_by,
                    cancel_requested_at,
                    created_at,
                    updated_at
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $8)
                "#,
                params!(
                    run.run_id.clone(),
                    run.workflow_id.clone(),
                    run.workflow_version,
                    run.status.clone(),
                    run.cancel_reason.clone(),
                    run.cancel_requested_by.clone(),
                    run.cancel_requested_at,
                    run.created_at
                ),
            )
            .await?;
        Ok(inserted == 1)
    }

    pub async fn insert_run_strict(&self, run: &WorkflowRunRecord) -> Result<(), PersistenceError> {
        self.client
            .execute(
                r#"
                INSERT INTO workflow_runs (
                    run_id,
                    workflow_id,
                    workflow_version,
                    status,
                    cancel_reason,
                    cancel_requested_by,
                    cancel_requested_at,
                    created_at,
                    updated_at
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $8)
                "#,
                params!(
                    run.run_id.clone(),
                    run.workflow_id.clone(),
                    run.workflow_version,
                    run.status.clone(),
                    run.cancel_reason.clone(),
                    run.cancel_requested_by.clone(),
                    run.cancel_requested_at,
                    run.created_at
                ),
            )
            .await?;
        Ok(())
    }

    pub async fn get_run(
        &self,
        run_id: &str,
    ) -> Result<Option<WorkflowRunRecord>, PersistenceError> {
        let row = self
            .client
            .query_as_optional::<WorkflowRunRow, _>(
                r#"
                SELECT
                    run_id,
                    workflow_id,
                    workflow_version,
                    status,
                    cancel_reason,
                    cancel_requested_by,
                    cancel_requested_at,
                    created_at
                FROM workflow_runs
                WHERE run_id = $1
                "#,
                params!(run_id.to_owned()),
            )
            .await?;

        row.map(TryInto::try_into).transpose()
    }

    pub(crate) async fn cancel_run_atomically(
        &self,
        run_id: &str,
        reason: Option<&str>,
        requested_by: Option<&str>,
        now: i64,
    ) -> Result<MutationApplyResult, PersistenceError> {
        let requested_transition_id = stable_cancel_transition_id("requested", run_id);
        let cancelled_transition_id = stable_cancel_transition_id("cancelled", run_id);
        let transition_payload = serde_json::to_string(&serde_json::json!({
            "reason": reason,
            "requested_by": requested_by,
        }))?;
        let cancellation_error = serde_json::to_string(&serde_json::json!({
            "reason": "run_cancelled",
        }))?;
        let results = self
            .client
            .txn([
                (
                    r#"
                    UPDATE workflow_runs
                    SET status = $1,
                        cancel_reason = $2,
                        cancel_requested_by = $3,
                        cancel_requested_at = $4,
                        updated_at = $4
                    WHERE run_id = $5
                        AND status NOT IN ($6, $7, $8)
                    "#,
                    params!(
                        RUN_STATUS_CANCELLED,
                        reason.map(ToOwned::to_owned),
                        requested_by.map(ToOwned::to_owned),
                        now,
                        run_id.to_owned(),
                        "completed".to_owned(),
                        RUN_STATUS_FAILED,
                        RUN_STATUS_CANCELLED,
                    ),
                ),
                (
                    r#"
                    UPDATE task_queue
                    SET status = CASE
                            WHEN status IN ($1, $2, $3) THEN $4
                            ELSE status
                        END,
                        continuation_state = CASE
                            WHEN continuation_state = $5 OR continuation_state LIKE $6 THEN $7
                            ELSE continuation_state
                        END,
                        active_attempt_id = CASE
                            WHEN status IN ($1, $2, $3) THEN NULL
                            ELSE active_attempt_id
                        END,
                        active_fencing_token = CASE
                            WHEN status IN ($1, $2, $3) THEN NULL
                            ELSE active_fencing_token
                        END,
                        leased_by_worker_id = CASE
                            WHEN status IN ($1, $2, $3) THEN NULL
                            ELSE leased_by_worker_id
                        END,
                        updated_at = $8
                    WHERE run_id = $9
                        AND (
                            status IN ($1, $2, $3)
                            OR continuation_state = $5
                            OR continuation_state LIKE $6
                        )
                        AND EXISTS (
                            SELECT 1
                            FROM workflow_runs
                            WHERE run_id = $9 AND status = $10
                        )
                    "#,
                    params!(
                        TASK_STATUS_QUEUED,
                        TASK_STATUS_RETRY_PENDING,
                        TASK_STATUS_LEASED,
                        TASK_STATUS_CANCELLED,
                        CONTINUATION_STATE_PENDING,
                        CONTINUATION_QUARANTINE_PATTERN,
                        CONTINUATION_STATE_SUPPRESSED,
                        now,
                        run_id.to_owned(),
                        RUN_STATUS_CANCELLED,
                    ),
                ),
                (
                    r#"
                    UPDATE step_instances
                    SET status = $1,
                        output_json = NULL,
                        error_json = $2,
                        updated_at = $3
                    WHERE run_id = $4
                        AND status NOT IN ($5, $6, $7, $1)
                        AND EXISTS (
                            SELECT 1 FROM workflow_runs
                            WHERE run_id = $4 AND status = $8
                        )
                    "#,
                    params!(
                        TASK_STATUS_CANCELLED,
                        cancellation_error,
                        now,
                        run_id.to_owned(),
                        TASK_STATUS_COMPLETED,
                        TASK_STATUS_FAILED,
                        TASK_STATUS_TIMED_OUT,
                        RUN_STATUS_CANCELLED,
                    ),
                ),
                (
                    r#"
                    UPDATE timers
                    SET status = CASE
                            WHEN status = $1 THEN $2
                            ELSE status
                        END,
                        effect_state = CASE
                            WHEN effect_state IS NULL OR effect_state = $3 OR effect_state LIKE $4
                                THEN $5
                            ELSE effect_state
                        END,
                        updated_at = $6
                    WHERE run_id = $7
                        AND (
                            status = $1
                            OR (
                                status = $8
                                AND (
                                    effect_state IS NULL
                                    OR effect_state = $3
                                    OR effect_state LIKE $4
                                )
                            )
                        )
                        AND EXISTS (
                            SELECT 1 FROM workflow_runs
                            WHERE run_id = $7 AND status = $9
                        )
                    "#,
                    params!(
                        TIMER_STATUS_PENDING,
                        TIMER_STATUS_CANCELLED,
                        TIMER_EFFECT_STATE_PENDING,
                        CONTINUATION_QUARANTINE_PATTERN,
                        TIMER_EFFECT_STATE_SUPPRESSED,
                        now,
                        run_id.to_owned(),
                        TIMER_STATUS_FIRED,
                        RUN_STATUS_CANCELLED,
                    ),
                ),
                (
                    r#"
                    DELETE FROM leases
                    WHERE task_id IN (
                        SELECT task_id
                        FROM task_queue
                        WHERE run_id = $1
                    )
                        AND EXISTS (
                            SELECT 1
                            FROM workflow_runs
                            WHERE run_id = $1 AND status = $2
                        )
                    "#,
                    params!(run_id.to_owned(), RUN_STATUS_CANCELLED),
                ),
                (
                    r#"
                    INSERT OR IGNORE INTO transition_log (
                        transition_id, run_id, transition_type, payload_json, created_at
                    )
                    SELECT $1, $2, $3, $4, $5
                    WHERE EXISTS (
                        SELECT 1 FROM workflow_runs
                        WHERE run_id = $2 AND status = $6
                    )
                    "#,
                    params!(
                        requested_transition_id,
                        run_id.to_owned(),
                        "run_cancel_requested".to_owned(),
                        transition_payload.clone(),
                        now,
                        RUN_STATUS_CANCELLED,
                    ),
                ),
                (
                    r#"
                    INSERT OR IGNORE INTO transition_log (
                        transition_id, run_id, transition_type, payload_json, created_at
                    )
                    SELECT $1, $2, $3, $4, $5
                    WHERE EXISTS (
                        SELECT 1 FROM workflow_runs
                        WHERE run_id = $2 AND status = $6
                    )
                    "#,
                    params!(
                        cancelled_transition_id,
                        run_id.to_owned(),
                        "run_cancelled".to_owned(),
                        transition_payload,
                        now,
                        RUN_STATUS_CANCELLED,
                    ),
                ),
            ])
            .await?;

        let counts = txn_counts(results)?;
        if counts.len() == 7 && counts[0] == 1 && counts[5] == 1 && counts[6] == 1 {
            return Ok(MutationApplyResult::Applied);
        }
        let run = self.get_run(run_id).await?;
        if run.as_ref().map(|run| run.status.as_str()) == Some(RUN_STATUS_CANCELLED) {
            return Ok(MutationApplyResult::AlreadyApplied);
        }
        if counts.iter().all(|count| *count == 0) {
            return Ok(MutationApplyResult::Stale);
        }
        Err(PersistenceError::Invariant(format!(
            "cancel_run_atomically produced unexpected row counts: {counts:?}"
        )))
    }

    pub(crate) async fn complete_run_if_running_and_idle(
        &self,
        run_id: &str,
        now: i64,
    ) -> Result<MutationApplyResult, PersistenceError> {
        let transition_id = stable_run_completion_transition_id(run_id);
        let intent_payload = serde_json::to_string(&serde_json::json!({
            "atomic_state": "intent",
            "run_id": run_id,
        }))?;
        let results = self
            .client
            .txn([
                (
                    r#"
                    INSERT OR IGNORE INTO transition_log (
                        transition_id, run_id, transition_type, payload_json, created_at
                    )
                    SELECT $1, run.run_id, $2, $3, $4
                    FROM workflow_runs run
                    WHERE run.run_id = $5
                        AND run.status = $6
                        AND NOT EXISTS (
                            SELECT 1
                            FROM task_queue task
                            WHERE task.run_id = run.run_id
                                AND task.status IN ($7, $8, $9)
                        )
                        AND NOT EXISTS (
                            SELECT 1
                            FROM leases lease
                            INNER JOIN task_queue task ON task.task_id = lease.task_id
                            WHERE task.run_id = run.run_id
                        )
                        AND NOT EXISTS (
                            SELECT 1
                            FROM task_queue task
                            WHERE task.run_id = run.run_id
                                AND (
                                    task.continuation_state = $10
                                    OR task.continuation_state LIKE $11
                                )
                        )
                        AND NOT EXISTS (
                            SELECT 1
                            FROM timers timer
                            WHERE timer.run_id = run.run_id
                                AND (
                                    timer.status = $12
                                    OR (
                                        timer.status = $13
                                        AND (
                                            timer.effect_state IS NULL
                                            OR timer.effect_state = $14
                                            OR timer.effect_state LIKE $15
                                        )
                                    )
                                )
                        )
                    "#,
                    params!(
                        transition_id.clone(),
                        "run_completed".to_owned(),
                        intent_payload.clone(),
                        now,
                        run_id.to_owned(),
                        RUN_STATUS_RUNNING,
                        TASK_STATUS_QUEUED,
                        TASK_STATUS_LEASED,
                        TASK_STATUS_RETRY_PENDING,
                        CONTINUATION_STATE_PENDING,
                        CONTINUATION_QUARANTINE_PATTERN,
                        TIMER_STATUS_PENDING,
                        TIMER_STATUS_FIRED,
                        TIMER_EFFECT_STATE_PENDING,
                        CONTINUATION_QUARANTINE_PATTERN,
                    ),
                ),
                (
                    r#"
                    UPDATE workflow_runs
                    SET status = $1, updated_at = $2
                    WHERE run_id = $3
                        AND status = $4
                        AND EXISTS (
                            SELECT 1
                            FROM transition_log
                            WHERE transition_id = $5 AND payload_json = $6
                        )
                    "#,
                    params!(
                        RUN_STATUS_COMPLETED,
                        now,
                        run_id.to_owned(),
                        RUN_STATUS_RUNNING,
                        transition_id.clone(),
                        intent_payload.clone(),
                    ),
                ),
                (
                    r#"
                    UPDATE transition_log
                    SET payload_json = NULL
                    WHERE transition_id = $1
                        AND payload_json = $2
                        AND EXISTS (
                            SELECT 1
                            FROM workflow_runs
                            WHERE run_id = $3 AND status = $4
                        )
                    "#,
                    params!(
                        transition_id.clone(),
                        intent_payload,
                        run_id.to_owned(),
                        RUN_STATUS_COMPLETED,
                    ),
                ),
            ])
            .await?;

        let counts = txn_counts(results)?;
        match counts.as_slice() {
            [1, 1, 1] => Ok(MutationApplyResult::Applied),
            [0, 0, 0] => {
                let run = self.get_run(run_id).await?;
                if run.as_ref().map(|run| run.status.as_str()) == Some(RUN_STATUS_COMPLETED)
                    && self.task_outcome_transition_exists(&transition_id).await?
                {
                    Ok(MutationApplyResult::AlreadyApplied)
                } else {
                    Ok(MutationApplyResult::Stale)
                }
            }
            _ => Err(PersistenceError::Invariant(format!(
                "complete_run_if_running_and_idle produced unexpected row counts: {counts:?}"
            ))),
        }
    }

    #[cfg(test)]
    pub(crate) async fn insert_step_instance(
        &self,
        step: &StepInstanceRecord,
    ) -> Result<bool, PersistenceError> {
        let inserted = self
            .client
            .execute(
                r#"
                INSERT OR IGNORE INTO step_instances (
                    step_instance_id,
                    run_id,
                    step_id,
                    status,
                    attempt_count,
                    input_json,
                    output_json,
                    error_json,
                    created_at,
                    updated_at
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                "#,
                params!(
                    step.step_instance_id.clone(),
                    step.run_id.clone(),
                    step.step_id.clone(),
                    step.status.clone(),
                    step.attempt_count as i64,
                    json_text(step.input.as_ref())?,
                    json_text(step.output.as_ref())?,
                    json_text(step.error.as_ref())?,
                    step.created_at,
                    step.updated_at
                ),
            )
            .await?;
        Ok(inserted == 1)
    }

    pub(crate) async fn insert_step_instance_if_run_running(
        &self,
        step: &StepInstanceRecord,
    ) -> Result<bool, PersistenceError> {
        let inserted = self
            .client
            .execute(
                r#"
                INSERT OR IGNORE INTO step_instances (
                    step_instance_id, run_id, step_id, status, attempt_count,
                    input_json, output_json, error_json, created_at, updated_at
                )
                SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10
                WHERE EXISTS (
                    SELECT 1 FROM workflow_runs
                    WHERE run_id = $2 AND status = $11
                )
                "#,
                params!(
                    step.step_instance_id.clone(),
                    step.run_id.clone(),
                    step.step_id.clone(),
                    step.status.clone(),
                    step.attempt_count as i64,
                    json_text(step.input.as_ref())?,
                    json_text(step.output.as_ref())?,
                    json_text(step.error.as_ref())?,
                    step.created_at,
                    step.updated_at,
                    RUN_STATUS_RUNNING,
                ),
            )
            .await?;
        Ok(inserted == 1)
    }

    pub async fn list_step_instances(
        &self,
        run_id: &str,
    ) -> Result<Vec<StepInstanceRecord>, PersistenceError> {
        let rows = self
            .client
            .query_as::<StepInstanceRow, _>(
                r#"
                SELECT
                    step_instance_id,
                    run_id,
                    step_id,
                    status,
                    attempt_count,
                    input_json,
                    output_json,
                    error_json,
                    created_at,
                    updated_at
                FROM step_instances
                WHERE run_id = $1
                ORDER BY step_instances.created_at ASC, step_instances.rowid ASC
                "#,
                params!(run_id.to_owned()),
            )
            .await?;

        rows.into_iter().map(TryInto::try_into).collect()
    }

    pub(crate) async fn list_step_instances_page(
        &self,
        run_id: &str,
        cursor: Option<&(i64, String)>,
        limit: u32,
    ) -> Result<Vec<StepInstanceRecord>, PersistenceError> {
        let after_created_at = cursor.map(|(created_at, _)| *created_at);
        let after_step_instance_id = cursor.map(|(_, stable_id)| stable_id.clone());
        let rows = self
            .client
            .query_as::<StepInstanceRow, _>(
                r#"
                SELECT
                    step_instance_id,
                    run_id,
                    step_id,
                    status,
                    attempt_count,
                    input_json,
                    output_json,
                    error_json,
                    created_at,
                    updated_at
                FROM step_instances INDEXED BY idx_step_instances_run_created
                WHERE run_id = $1
                    AND (
                        $2 IS NULL
                        OR created_at > $2
                        OR (created_at = $2 AND step_instance_id > $3)
                    )
                ORDER BY created_at ASC, step_instance_id ASC
                LIMIT $4
                "#,
                params!(
                    run_id.to_owned(),
                    after_created_at,
                    after_step_instance_id,
                    i64::from(limit),
                ),
            )
            .await?;

        rows.into_iter().map(TryInto::try_into).collect()
    }

    pub(crate) async fn get_step_instance(
        &self,
        run_id: &str,
        step_id: &str,
    ) -> Result<Option<StepInstanceRecord>, PersistenceError> {
        let row = self
            .client
            .query_as_optional::<StepInstanceRow, _>(
                r#"
                SELECT
                    step_instance_id,
                    run_id,
                    step_id,
                    status,
                    attempt_count,
                    input_json,
                    output_json,
                    error_json,
                    created_at,
                    updated_at
                FROM step_instances
                WHERE run_id = $1 AND step_id = $2
                LIMIT 1
                "#,
                params!(run_id.to_owned(), step_id.to_owned()),
            )
            .await?;

        row.map(TryInto::try_into).transpose()
    }

    #[cfg(test)]
    pub(crate) async fn insert_timer(&self, timer: &TimerRecord) -> Result<bool, PersistenceError> {
        let inserted = self
            .client
            .execute(
                r#"
                INSERT OR IGNORE INTO timers (
                    timer_id,
                    run_id,
                    step_id,
                    timer_kind,
                    due_at,
                    status,
                    payload_json,
                    effect_state,
                    created_at,
                    updated_at
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                "#,
                params!(
                    timer.timer_id.clone(),
                    timer.run_id.clone(),
                    timer.step_id.clone(),
                    timer.timer_kind.clone(),
                    timer.due_at,
                    timer.status.clone(),
                    json_text(timer.payload.as_ref())?,
                    timer.effect_state.clone(),
                    timer.created_at,
                    timer.updated_at
                ),
            )
            .await?;
        Ok(inserted == 1)
    }

    pub(crate) async fn insert_timer_if_run_running(
        &self,
        timer: &TimerRecord,
    ) -> Result<bool, PersistenceError> {
        let inserted = self
            .client
            .execute(
                r#"
                INSERT OR IGNORE INTO timers (
                    timer_id, run_id, step_id, timer_kind, due_at, status,
                    payload_json, effect_state, created_at, updated_at
                )
                SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10
                WHERE EXISTS (
                    SELECT 1 FROM workflow_runs
                    WHERE run_id = $2 AND status = $11
                )
                "#,
                params!(
                    timer.timer_id.clone(),
                    timer.run_id.clone(),
                    timer.step_id.clone(),
                    timer.timer_kind.clone(),
                    timer.due_at,
                    timer.status.clone(),
                    json_text(timer.payload.as_ref())?,
                    timer.effect_state.clone(),
                    timer.created_at,
                    timer.updated_at,
                    RUN_STATUS_RUNNING,
                ),
            )
            .await?;
        Ok(inserted == 1)
    }

    pub(crate) async fn list_due_timers(
        &self,
        now: i64,
    ) -> Result<Vec<TimerRecord>, PersistenceError> {
        let rows = self
            .client
            .query_as::<TimerRow, _>(
                r#"
                SELECT
                    timer_id,
                    run_id,
                    step_id,
                    timer_kind,
                    due_at,
                    status,
                    payload_json,
                    effect_state,
                    created_at,
                    updated_at
                FROM timers
                WHERE due_at <= $1
                    AND (
                        status = $2
                        OR (
                            status = $3
                            AND (effect_state IS NULL OR effect_state = $4)
                        )
                    )
                ORDER BY due_at ASC, timer_id ASC
                LIMIT $5
                "#,
                params!(
                    now,
                    TIMER_STATUS_PENDING,
                    TIMER_STATUS_FIRED,
                    TIMER_EFFECT_STATE_PENDING,
                    DUE_TIMER_SCAN_BATCH_SIZE,
                ),
            )
            .await?;

        rows.into_iter().map(TryInto::try_into).collect()
    }

    /// Durably quarantines a fired timer effect whose downstream execution
    /// provenance cannot be reconstructed without live defaults.
    #[allow(dead_code)] // Private recovery seam.
    pub(crate) async fn quarantine_timer_effect(
        &self,
        timer_id: &str,
        stable_code: &str,
        now: i64,
    ) -> Result<ContinuationQuarantineResult, PersistenceError> {
        validate_quarantine_code(stable_code)?;
        let marker = format!("{CONTINUATION_QUARANTINE_PREFIX}{stable_code}");
        let transition_id = stable_outcome_id("timer-effect-quarantined", &[timer_id]);
        let payload = serde_json::to_string(&serde_json::json!({
            "timer_id": timer_id,
            "code": stable_code,
        }))?;
        let results = self
            .client
            .txn([
                (
                    r#"
                    INSERT OR IGNORE INTO transition_log (
                        transition_id, run_id, transition_type, payload_json, created_at
                    )
                    SELECT $1, timer.run_id, $2, $3, $4
                    FROM timers timer
                    INNER JOIN workflow_runs run ON run.run_id = timer.run_id
                    WHERE timer.timer_id = $5
                        AND timer.status = $6
                        AND timer.effect_state = $7
                        AND run.status = $8
                    "#,
                    params!(
                        transition_id.clone(),
                        "timer_effect_quarantined".to_owned(),
                        payload.clone(),
                        now,
                        timer_id.to_owned(),
                        TIMER_STATUS_FIRED,
                        TIMER_EFFECT_STATE_PENDING,
                        RUN_STATUS_RUNNING,
                    ),
                ),
                (
                    r#"
                    UPDATE timers
                    SET effect_state = $1, updated_at = $2
                    WHERE timer_id = $3
                        AND status = $4
                        AND effect_state = $5
                        AND EXISTS (
                            SELECT 1 FROM workflow_runs
                            WHERE run_id = timers.run_id AND status = $6
                        )
                        AND EXISTS (
                            SELECT 1 FROM transition_log
                            WHERE transition_id = $7 AND transition_type = $8
                                AND payload_json = $9
                        )
                    "#,
                    params!(
                        marker.clone(),
                        now,
                        timer_id.to_owned(),
                        TIMER_STATUS_FIRED,
                        TIMER_EFFECT_STATE_PENDING,
                        RUN_STATUS_RUNNING,
                        transition_id.clone(),
                        "timer_effect_quarantined".to_owned(),
                        payload.clone(),
                    ),
                ),
            ])
            .await?;
        let counts = txn_counts(results)?;
        if matches!(counts.as_slice(), [1, 1] | [0, 1]) {
            return Ok(ContinuationQuarantineResult::Applied);
        }
        self.classify_continuation_quarantine(
            ContinuationQuarantineSource::Timer,
            timer_id,
            &marker,
            &transition_id,
            "timer_effect_quarantined",
            &payload,
            &counts,
        )
        .await
    }

    #[allow(dead_code)] // Shared by the private timeout compatibility seam.
    async fn get_timer_record(
        &self,
        timer_id: &str,
    ) -> Result<Option<TimerRecord>, PersistenceError> {
        let row = self
            .client
            .query_as_optional::<TimerRow, _>(
                r#"
                SELECT timer_id, run_id, step_id, timer_kind, due_at, status,
                    payload_json, effect_state, created_at, updated_at
                FROM timers
                WHERE timer_id = $1
                LIMIT 1
                "#,
                params!(timer_id.to_owned()),
            )
            .await?;
        row.map(TryInto::try_into).transpose()
    }

    pub(crate) async fn claim_sleep_timer_if_due(
        &self,
        timer_id: &str,
        output: &Value,
        now: i64,
    ) -> Result<TimerClaimResult, PersistenceError> {
        let transition_id = stable_timer_transition_id("fired", timer_id);
        let intent_payload = serde_json::to_string(&serde_json::json!({
            "atomic_state": "intent",
            "timer_id": timer_id,
        }))?;
        let applied_payload = serde_json::to_string(&serde_json::json!({
            "timer_id": timer_id,
            "timer_kind": "sleep",
        }))?;
        let output_json = serde_json::to_string(output)?;
        let results = self
            .client
            .txn([
                (
                    r#"
                    INSERT OR IGNORE INTO transition_log (
                        transition_id, run_id, transition_type, payload_json, created_at
                    )
                    SELECT $1, timer.run_id, $2, $3, $4
                    FROM timers timer
                    INNER JOIN workflow_runs run ON run.run_id = timer.run_id
                    INNER JOIN step_instances step
                        ON step.run_id = timer.run_id AND step.step_id = timer.step_id
                    WHERE timer.timer_id = $5
                        AND timer.timer_kind = $6
                        AND timer.due_at <= $4
                        AND run.status = $7
                        AND (
                            (timer.status = $8 AND step.status = $9)
                            OR (
                                timer.status = $10
                                AND (timer.effect_state IS NULL OR timer.effect_state = $11)
                                AND step.status IN ($9, $12)
                            )
                        )
                    "#,
                    params!(
                        transition_id.clone(),
                        "timer_fired".to_owned(),
                        intent_payload.clone(),
                        now,
                        timer_id.to_owned(),
                        "sleep".to_owned(),
                        RUN_STATUS_RUNNING,
                        TIMER_STATUS_PENDING,
                        "sleeping".to_owned(),
                        TIMER_STATUS_FIRED,
                        TIMER_EFFECT_STATE_PENDING,
                        TASK_STATUS_COMPLETED,
                    ),
                ),
                (
                    r#"
                    UPDATE timers
                    SET status = $1, effect_state = $2, updated_at = $3
                    WHERE timer_id = $4
                        AND EXISTS (
                            SELECT 1 FROM transition_log
                            WHERE transition_id = $5 AND payload_json = $6
                        )
                    "#,
                    params!(
                        TIMER_STATUS_FIRED,
                        TIMER_EFFECT_STATE_PENDING,
                        now,
                        timer_id.to_owned(),
                        transition_id.clone(),
                        intent_payload.clone(),
                    ),
                ),
                (
                    r#"
                    UPDATE step_instances
                    SET status = $1, output_json = $2, error_json = NULL, updated_at = $3
                    WHERE (run_id, step_id) = (
                            SELECT run_id, step_id FROM timers WHERE timer_id = $4
                        )
                        AND status IN ($5, $1)
                        AND EXISTS (
                            SELECT 1 FROM transition_log
                            WHERE transition_id = $6 AND payload_json = $7
                        )
                    "#,
                    params!(
                        TASK_STATUS_COMPLETED,
                        output_json,
                        now,
                        timer_id.to_owned(),
                        "sleeping".to_owned(),
                        transition_id.clone(),
                        intent_payload.clone(),
                    ),
                ),
                (
                    r#"
                    UPDATE transition_log
                    SET payload_json = $1
                    WHERE transition_id = $2 AND payload_json = $3
                    "#,
                    params!(
                        applied_payload,
                        transition_id.clone(),
                        intent_payload.clone()
                    ),
                ),
                (
                    r#"
                    UPDATE transition_log
                    SET run_id = NULL
                    WHERE transition_id = $1 AND payload_json = $2
                    "#,
                    params!(transition_id, intent_payload),
                ),
            ])
            .await?;

        let counts = txn_counts(results)?;
        if counts.as_slice() == [1, 1, 1, 1, 0] {
            return Ok(TimerClaimResult::Claimed);
        }
        if counts.iter().all(|count| *count == 0) {
            let state = self.timer_effect_state(timer_id).await?;
            return if matches!(
                state.as_ref(),
                Some(state)
                    if state.status == TIMER_STATUS_FIRED
                        && state.effect_state.as_deref() == Some(TIMER_EFFECT_STATE_PENDING)
            ) {
                Ok(TimerClaimResult::ReplayPendingEffect)
            } else {
                Ok(TimerClaimResult::NotClaimable)
            };
        }
        Err(PersistenceError::Invariant(format!(
            "claim_sleep_timer_if_due produced unexpected row counts: {counts:?}"
        )))
    }

    pub(crate) async fn mark_timer_effect_applied(
        &self,
        timer_id: &str,
        now: i64,
    ) -> Result<MutationApplyResult, PersistenceError> {
        let updated = self
            .client
            .execute(
                r#"
                UPDATE timers
                SET effect_state = $1, updated_at = $2
                WHERE timer_id = $3
                    AND status = $4
                    AND effect_state = $5
                    AND EXISTS (
                        SELECT 1 FROM workflow_runs
                        WHERE run_id = timers.run_id AND status = $6
                    )
                "#,
                params!(
                    TIMER_EFFECT_STATE_APPLIED,
                    now,
                    timer_id.to_owned(),
                    TIMER_STATUS_FIRED,
                    TIMER_EFFECT_STATE_PENDING,
                    RUN_STATUS_RUNNING,
                ),
            )
            .await?;
        if updated == 1 {
            return Ok(MutationApplyResult::Applied);
        }
        let state = self.timer_effect_state(timer_id).await?;
        if state
            .as_ref()
            .and_then(|state| state.effect_state.as_deref())
            == Some(TIMER_EFFECT_STATE_APPLIED)
        {
            Ok(MutationApplyResult::AlreadyApplied)
        } else {
            Ok(MutationApplyResult::Stale)
        }
    }

    pub(crate) async fn apply_retry_timer_if_due(
        &self,
        timer_id: &str,
        task_id: &str,
        now: i64,
    ) -> Result<MutationApplyResult, PersistenceError> {
        let fired_transition_id = stable_timer_transition_id("fired", timer_id);
        let requeued_transition_id = stable_outcome_id("task-requeued", &[timer_id, task_id]);
        let intent_payload = serde_json::to_string(&serde_json::json!({
            "atomic_state": "intent",
            "timer_id": timer_id,
            "task_id": task_id,
        }))?;
        let fired_payload = serde_json::to_string(&serde_json::json!({
            "timer_id": timer_id,
            "timer_kind": "retry",
        }))?;
        let requeued_payload = serde_json::to_string(&serde_json::json!({
            "timer_id": timer_id,
            "task_id": task_id,
        }))?;
        let results = self
            .client
            .txn([
                (
                    r#"
                    INSERT OR IGNORE INTO transition_log (
                        transition_id, run_id, transition_type, payload_json, created_at
                    )
                    SELECT $1, timer.run_id, $2, $3, $4
                    FROM timers timer
                    INNER JOIN workflow_runs run ON run.run_id = timer.run_id
                    INNER JOIN task_queue task
                        ON task.task_id = $5
                        AND task.run_id = timer.run_id
                        AND task.step_id = timer.step_id
                    WHERE timer.timer_id = $6
                        AND timer.timer_kind = $7
                        AND timer.due_at <= $4
                        AND run.status = $8
                        AND task.status IN ($9, $10)
                        AND (
                            timer.status = $11
                            OR (
                                timer.status = $12
                                AND (timer.effect_state IS NULL OR timer.effect_state = $13)
                            )
                        )
                    "#,
                    params!(
                        fired_transition_id.clone(),
                        "timer_fired".to_owned(),
                        intent_payload.clone(),
                        now,
                        task_id.to_owned(),
                        timer_id.to_owned(),
                        "retry".to_owned(),
                        RUN_STATUS_RUNNING,
                        TASK_STATUS_RETRY_PENDING,
                        TASK_STATUS_QUEUED,
                        TIMER_STATUS_PENDING,
                        TIMER_STATUS_FIRED,
                        TIMER_EFFECT_STATE_PENDING,
                    ),
                ),
                (
                    r#"
                    UPDATE timers
                    SET status = $1, effect_state = $2, updated_at = $3
                    WHERE timer_id = $4
                        AND EXISTS (
                            SELECT 1 FROM transition_log
                            WHERE transition_id = $5 AND payload_json = $6
                        )
                    "#,
                    params!(
                        TIMER_STATUS_FIRED,
                        TIMER_EFFECT_STATE_APPLIED,
                        now,
                        timer_id.to_owned(),
                        fired_transition_id.clone(),
                        intent_payload.clone(),
                    ),
                ),
                (
                    r#"
                    UPDATE task_queue
                    SET status = $1, leased_by_worker_id = NULL, updated_at = $2
                    WHERE task_id = $3
                        AND status IN ($4, $1)
                        AND EXISTS (
                            SELECT 1 FROM transition_log
                            WHERE transition_id = $5 AND payload_json = $6
                        )
                    "#,
                    params!(
                        TASK_STATUS_QUEUED,
                        now,
                        task_id.to_owned(),
                        TASK_STATUS_RETRY_PENDING,
                        fired_transition_id.clone(),
                        intent_payload.clone(),
                    ),
                ),
                (
                    r#"
                    UPDATE step_instances
                    SET status = $1, output_json = NULL, error_json = NULL, updated_at = $2
                    WHERE (run_id, step_id) = (
                            SELECT run_id, step_id FROM timers WHERE timer_id = $3
                        )
                        AND status IN ($4, $1)
                        AND EXISTS (
                            SELECT 1 FROM transition_log
                            WHERE transition_id = $5 AND payload_json = $6
                        )
                    "#,
                    params!(
                        TASK_STATUS_QUEUED,
                        now,
                        timer_id.to_owned(),
                        TASK_STATUS_RETRY_PENDING,
                        fired_transition_id.clone(),
                        intent_payload.clone(),
                    ),
                ),
                (
                    r#"
                    INSERT OR IGNORE INTO transition_log (
                        transition_id, run_id, transition_type, payload_json, created_at
                    )
                    SELECT $1, timer.run_id, $2, $3, $4
                    FROM timers timer
                    INNER JOIN task_queue task ON task.task_id = $5
                    WHERE timer.timer_id = $6
                        AND timer.effect_state = $7
                        AND task.status = $8
                        AND EXISTS (
                            SELECT 1 FROM transition_log
                            WHERE transition_id = $9 AND payload_json = $10
                        )
                    "#,
                    params!(
                        requeued_transition_id,
                        "task_requeued".to_owned(),
                        requeued_payload,
                        now,
                        task_id.to_owned(),
                        timer_id.to_owned(),
                        TIMER_EFFECT_STATE_APPLIED,
                        TASK_STATUS_QUEUED,
                        fired_transition_id.clone(),
                        intent_payload.clone(),
                    ),
                ),
                (
                    "UPDATE transition_log SET payload_json = $1 WHERE transition_id = $2 AND payload_json = $3",
                    params!(
                        fired_payload,
                        fired_transition_id.clone(),
                        intent_payload.clone(),
                    ),
                ),
                (
                    "UPDATE transition_log SET run_id = NULL WHERE transition_id = $1 AND payload_json = $2",
                    params!(fired_transition_id, intent_payload),
                ),
            ])
            .await?;

        let counts = txn_counts(results)?;
        if counts.as_slice() == [1, 1, 1, 1, 1, 1, 0] {
            return Ok(MutationApplyResult::Applied);
        }
        if counts.iter().all(|count| *count == 0) {
            let state = self.timer_effect_state(timer_id).await?;
            return if state
                .as_ref()
                .and_then(|state| state.effect_state.as_deref())
                == Some(TIMER_EFFECT_STATE_APPLIED)
            {
                Ok(MutationApplyResult::AlreadyApplied)
            } else {
                Ok(MutationApplyResult::Stale)
            };
        }
        Err(PersistenceError::Invariant(format!(
            "apply_retry_timer_if_due produced unexpected row counts: {counts:?}"
        )))
    }

    async fn timer_effect_state(
        &self,
        timer_id: &str,
    ) -> Result<Option<TimerEffectStateRow>, PersistenceError> {
        self.client
            .query_as_optional::<TimerEffectStateRow, _>(
                "SELECT status, effect_state FROM timers WHERE timer_id = $1",
                params!(timer_id.to_owned()),
            )
            .await
            .map_err(Into::into)
    }

    #[cfg(test)]
    pub(crate) async fn enqueue_task(
        &self,
        task: &QueuedTaskRecord,
    ) -> Result<bool, PersistenceError> {
        let inserted = self
            .client
            .execute(
                r#"
                INSERT OR IGNORE INTO task_queue (
                    task_id,
                    run_id,
                    workflow_id,
                    workflow_version,
                    step_id,
                    step_kind,
                    handler_id,
                    capability,
                    queue_name,
                    input_json,
                    timeout_at,
                    status,
                    attempt_sequence,
                    active_attempt_id,
                    active_fencing_token,
                    leased_by_worker_id,
                    created_at,
                    updated_at
                )
                VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, NULL, NULL, NULL, $14, $14
                )
                "#,
                params!(
                    task.task_id.clone(),
                    task.run_id.clone(),
                    task.workflow_id.clone(),
                    task.workflow_version as i64,
                    task.step_id.clone(),
                    task.step_kind.clone(),
                    task.handler_id.clone(),
                    task.capability.clone(),
                    task.queue_name.clone(),
                    serde_json::to_string(&task.input)?,
                    task.timeout_at,
                    TASK_STATUS_QUEUED,
                    task.attempt_sequence as i64,
                    task.created_at
                ),
            )
            .await?;
        Ok(inserted == 1)
    }

    pub(crate) async fn enqueue_task_if_run_running(
        &self,
        task: &QueuedTaskRecord,
    ) -> Result<bool, PersistenceError> {
        let inserted = self
            .client
            .execute(
                r#"
                INSERT OR IGNORE INTO task_queue (
                    task_id, run_id, workflow_id, workflow_version, step_id,
                    step_kind, handler_id, capability, queue_name, input_json,
                    timeout_at, status, attempt_sequence, active_attempt_id,
                    active_fencing_token, leased_by_worker_id, created_at, updated_at
                )
                SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                    $11, $12, $13, NULL, NULL, NULL, $14, $14
                WHERE EXISTS (
                    SELECT 1 FROM workflow_runs
                    WHERE run_id = $2 AND status = $15
                )
                "#,
                params!(
                    task.task_id.clone(),
                    task.run_id.clone(),
                    task.workflow_id.clone(),
                    task.workflow_version as i64,
                    task.step_id.clone(),
                    task.step_kind.clone(),
                    task.handler_id.clone(),
                    task.capability.clone(),
                    task.queue_name.clone(),
                    serde_json::to_string(&task.input)?,
                    task.timeout_at,
                    TASK_STATUS_QUEUED,
                    task.attempt_sequence as i64,
                    task.created_at,
                    RUN_STATUS_RUNNING,
                ),
            )
            .await?;
        Ok(inserted == 1)
    }

    pub async fn lease_next_task(
        &self,
        poll: &PollTask,
        lease_expiry: i64,
        leased_at: i64,
    ) -> Result<Option<TaskLeaseRecord>, PersistenceError> {
        if poll.queues.is_empty() || poll.capabilities.is_empty() || poll.max_inflight == 0 {
            return Ok(None);
        }

        let queue_placeholders = sql_placeholders(4, poll.queues.len());
        let capability_placeholders =
            sql_placeholders(4 + poll.queues.len(), poll.capabilities.len());
        let query = format!(
            r#"
                SELECT
                    t.task_id,
                    t.run_id,
                    t.workflow_id,
                    t.workflow_version,
                    t.step_id,
                    t.step_kind,
                    t.handler_id,
                    t.capability,
                    t.queue_name,
                    t.input_json,
                    t.timeout_at,
                    t.status,
                    t.attempt_sequence,
                    t.active_attempt_id,
                    t.active_fencing_token,
                    t.leased_by_worker_id
                FROM task_queue t
                INNER JOIN workflow_runs r ON r.run_id = t.run_id
                WHERE t.status = $1
                    AND r.status = $2
                    AND t.continuation_state IS NULL
                    AND (t.timeout_at IS NULL OR t.timeout_at > $3)
                    AND NOT EXISTS (SELECT 1 FROM leases WHERE task_id = t.task_id)
                    AND t.queue_name IN ({queue_placeholders})
                    AND t.capability IN ({capability_placeholders})
                ORDER BY t.created_at ASC
                "#
        );
        let mut query_params = params!(TASK_STATUS_QUEUED, "running".to_owned(), leased_at);
        query_params.extend(poll.queues.iter().cloned().map(Param::from));
        query_params.extend(poll.capabilities.iter().cloned().map(Param::from));
        let queued_rows = self
            .client
            .query_as::<TaskQueueRow, _>(query, query_params)
            .await?;

        for row in queued_rows {
            if let Some(lease) = self
                .claim_selected_task(poll, row, lease_expiry, leased_at)
                .await?
            {
                return Ok(Some(lease));
            }
        }

        Ok(None)
    }

    async fn claim_selected_task(
        &self,
        poll: &PollTask,
        row: TaskQueueRow,
        lease_expiry: i64,
        leased_at: i64,
    ) -> Result<Option<TaskLeaseRecord>, PersistenceError> {
        let workflow_version =
            match checked_stored_workflow_version("task", &row.task_id, row.workflow_version) {
                Ok(workflow_version) => workflow_version,
                Err(PersistenceError::InvalidStoredWorkflowVersion { .. }) => {
                    self.quarantine_task_execution(
                        &row.task_id,
                        "invalid_workflow_version",
                        leased_at,
                    )
                    .await?;
                    return Ok(None);
                }
                Err(error) => return Err(error),
            };
        let Some(attempt_sequence) = row
            .attempt_sequence
            .checked_add(1)
            .filter(|attempt_sequence| *attempt_sequence > 0)
        else {
            self.quarantine_task_execution(&row.task_id, "invalid_attempt_sequence", leased_at)
                .await?;
            return Ok(None);
        };
        let attempt_id = format!("attempt-{attempt_sequence}");
        let Some(fencing_token) = row
            .active_fencing_token
            .unwrap_or(0)
            .checked_add(1)
            .filter(|fencing_token| *fencing_token > 0)
        else {
            self.quarantine_task_execution(&row.task_id, "invalid_fencing_token", leased_at)
                .await?;
            return Ok(None);
        };

        let results = self
            .client
            .txn([
                (
                    r#"
                    UPDATE task_queue
                    SET status = $1,
                        attempt_sequence = $2,
                        active_attempt_id = $3,
                        active_fencing_token = $4,
                        leased_by_worker_id = $5,
                        updated_at = $6
                    WHERE task_id = $7
                        AND status = $8
                        AND EXISTS (
                            SELECT 1
                            FROM workflow_runs
                            WHERE run_id = $9 AND status = $10
                        )
                        AND continuation_state IS NULL
                        AND (timeout_at IS NULL OR timeout_at > $6)
                        AND NOT EXISTS (
                            SELECT 1 FROM leases WHERE task_id = task_queue.task_id
                        )
                        AND (
                            SELECT COUNT(*)
                            FROM leases active_lease
                            INNER JOIN task_queue active_task
                                ON active_task.task_id = active_lease.task_id
                                AND active_task.active_attempt_id = active_lease.attempt_id
                                AND active_task.active_fencing_token = active_lease.fencing_token
                                AND active_task.leased_by_worker_id = active_lease.worker_id
                            INNER JOIN workflow_runs active_run
                                ON active_run.run_id = active_task.run_id
                            WHERE active_lease.worker_id = $5
                                AND active_lease.expires_at > $6
                                AND active_task.status = $1
                                AND active_task.continuation_state IS NULL
                                AND active_run.status = $10
                                AND (
                                    active_task.timeout_at IS NULL
                                    OR active_task.timeout_at > $6
                                )
                        ) < $11
                        AND attempt_sequence = $12
                        AND active_attempt_id IS $13
                        AND active_fencing_token IS $14
                        AND leased_by_worker_id IS $15
                    "#,
                    params!(
                        TASK_STATUS_LEASED,
                        attempt_sequence,
                        attempt_id.clone(),
                        fencing_token,
                        poll.worker_id.clone(),
                        leased_at,
                        row.task_id.clone(),
                        TASK_STATUS_QUEUED,
                        row.run_id.clone(),
                        RUN_STATUS_RUNNING,
                        poll.max_inflight as i64,
                        row.attempt_sequence,
                        row.active_attempt_id.clone(),
                        row.active_fencing_token,
                        row.leased_by_worker_id.clone(),
                    ),
                ),
                (
                    r#"
                    INSERT INTO leases (
                        lease_id, task_id, attempt_id, worker_id, fencing_token,
                        expires_at, heartbeat_at
                    )
                    SELECT $1, task_id, active_attempt_id, leased_by_worker_id,
                        active_fencing_token, $2, $3
                    FROM task_queue
                    WHERE task_id = $4 AND status = $5
                        AND active_attempt_id = $6
                        AND active_fencing_token = $7
                        AND leased_by_worker_id = $8
                        AND continuation_state IS NULL
                        AND changes() = 1
                    "#,
                    params!(
                        format!("{}:{attempt_id}", row.task_id),
                        lease_expiry,
                        leased_at,
                        row.task_id.clone(),
                        TASK_STATUS_LEASED,
                        attempt_id.clone(),
                        fencing_token,
                        poll.worker_id.clone(),
                    ),
                ),
                (
                    r#"
                    UPDATE task_queue
                    SET run_id = NULL
                    WHERE task_id = $1 AND status = $2
                        AND active_attempt_id = $3
                        AND active_fencing_token = $4
                        AND leased_by_worker_id = $5
                        AND NOT EXISTS (
                            SELECT 1 FROM leases
                            WHERE task_id = $1 AND attempt_id = $3
                                AND fencing_token = $4 AND worker_id = $5
                        )
                    "#,
                    params!(
                        row.task_id.clone(),
                        TASK_STATUS_LEASED,
                        attempt_id.clone(),
                        fencing_token,
                        poll.worker_id.clone(),
                    ),
                ),
            ])
            .await?;
        let counts = txn_counts(results)?;
        match counts.as_slice() {
            [1, 1, 0] => {}
            [0, 0, 0] => return Ok(None),
            _ => {
                return Err(PersistenceError::Invariant(format!(
                    "lease_next_task produced unexpected row counts: {counts:?}"
                )));
            }
        }
        let fencing_token = u64::try_from(fencing_token).map_err(|_| {
            PersistenceError::Invariant(format!(
                "leased task '{}' produced invalid fencing token {fencing_token}",
                row.task_id
            ))
        })?;

        Ok(Some(TaskLeaseRecord {
            task_id: row.task_id,
            run_id: row.run_id,
            workflow_id: row.workflow_id,
            workflow_version,
            step_id: row.step_id,
            step_kind: row.step_kind,
            handler_id: row.handler_id,
            input: serde_json::from_str::<Value>(&row.input_json)?,
            attempt_id,
            fencing_token,
        }))
    }

    pub async fn heartbeat_lease(
        &self,
        lease: &TaskMutationContext,
        lease_expiry: i64,
        heartbeat_at: i64,
    ) -> Result<MutationApplyResult, PersistenceError> {
        let Some(row) = self.get_task_row(&lease.task_id).await? else {
            return Ok(MutationApplyResult::Stale);
        };

        if !lease_matches(&row, lease) {
            return Ok(MutationApplyResult::Stale);
        }

        let updated = self
            .client
            .execute(
                r#"
                UPDATE leases
                SET expires_at = $1, heartbeat_at = $2
                WHERE task_id = $3 AND attempt_id = $4 AND worker_id = $5 AND fencing_token = $6
                    AND expires_at > $2
                "#,
                params!(
                    lease_expiry,
                    heartbeat_at,
                    lease.task_id.clone(),
                    lease.attempt_id.clone(),
                    lease.worker_id.clone(),
                    lease.fencing_token as i64
                ),
            )
            .await?;

        if updated == 1 {
            Ok(MutationApplyResult::Applied)
        } else {
            Ok(MutationApplyResult::Stale)
        }
    }

    /// Atomically records a worker completion and the durable intent to process
    /// its continuation. Runtime continuation processing may reload the output
    /// from the completed step instance.
    pub async fn complete_task_with_continuation(
        &self,
        lease: &TaskMutationContext,
        result: &TaskOutput,
        now: i64,
    ) -> Result<TaskOutcomeApplyResult, PersistenceError> {
        let Some(task) = self.get_task_row(&lease.task_id).await? else {
            return Ok(TaskOutcomeApplyResult::Stale);
        };
        let transition_id = stable_task_transition_id("completed", lease);
        let intent_payload = serde_json::to_string(&serde_json::json!({
            "atomic_state": "intent",
            "task_id": task.task_id,
            "step_id": task.step_id,
            "attempt_id": lease.attempt_id,
        }))?;
        let applied_payload = serde_json::to_string(&serde_json::json!({
            "task_id": task.task_id,
            "step_id": task.step_id,
            "attempt_id": lease.attempt_id,
        }))?;
        let output_json = serde_json::to_string(&result.payload)?;

        let results = self
            .client
            .txn([
                (
                    r#"
                    INSERT OR IGNORE INTO transition_log (
                        transition_id, run_id, transition_type, payload_json, created_at
                    )
                    SELECT $1, t.run_id, $2, $3, $4
                    FROM task_queue t
                    INNER JOIN workflow_runs r ON r.run_id = t.run_id
                    INNER JOIN leases l
                        ON l.task_id = t.task_id
                        AND l.attempt_id = t.active_attempt_id
                        AND l.worker_id = t.leased_by_worker_id
                        AND l.fencing_token = t.active_fencing_token
                    WHERE t.task_id = $5
                        AND t.status = $6
                        AND t.active_attempt_id = $7
                        AND t.active_fencing_token = $8
                        AND t.leased_by_worker_id = $9
                        AND l.attempt_id = $7
                        AND l.fencing_token = $8
                        AND l.worker_id = $9
                        AND l.expires_at > $4
                        AND r.status = $10
                        AND (t.timeout_at IS NULL OR t.timeout_at > $4)
                    "#,
                    params!(
                        transition_id.clone(),
                        "task_completed".to_owned(),
                        intent_payload.clone(),
                        now,
                        lease.task_id.clone(),
                        TASK_STATUS_LEASED,
                        lease.attempt_id.clone(),
                        lease.fencing_token as i64,
                        lease.worker_id.clone(),
                        RUN_STATUS_RUNNING,
                    ),
                ),
                (
                    r#"
                    UPDATE task_queue
                    SET status = $1,
                        continuation_state = $2,
                        updated_at = $3
                    WHERE task_id = $4
                        AND status = $5
                        AND active_attempt_id = $6
                        AND active_fencing_token = $7
                        AND leased_by_worker_id = $8
                        AND (timeout_at IS NULL OR timeout_at > $3)
                        AND EXISTS (
                            SELECT 1 FROM workflow_runs
                            WHERE run_id = task_queue.run_id AND status = $9
                        )
                        AND EXISTS (
                            SELECT 1 FROM transition_log
                            WHERE transition_id = $10 AND payload_json = $11
                        )
                    "#,
                    params!(
                        TASK_STATUS_COMPLETED,
                        CONTINUATION_STATE_PENDING,
                        now,
                        lease.task_id.clone(),
                        TASK_STATUS_LEASED,
                        lease.attempt_id.clone(),
                        lease.fencing_token as i64,
                        lease.worker_id.clone(),
                        RUN_STATUS_RUNNING,
                        transition_id.clone(),
                        intent_payload.clone(),
                    ),
                ),
                (
                    r#"
                    UPDATE step_instances
                    SET status = $1,
                        attempt_count = $2,
                        output_json = $3,
                        error_json = NULL,
                        updated_at = $4
                    WHERE run_id = $5
                        AND step_id = $6
                        AND EXISTS (
                            SELECT 1
                            FROM task_queue
                            WHERE task_id = $7
                                AND run_id = $5
                                AND step_id = $6
                                AND status = $1
                                AND continuation_state = $8
                                AND active_attempt_id = $9
                                AND active_fencing_token = $10
                        )
                        AND EXISTS (
                            SELECT 1 FROM transition_log
                            WHERE transition_id = $11 AND payload_json = $12
                        )
                    "#,
                    params!(
                        TASK_STATUS_COMPLETED,
                        task.attempt_sequence,
                        output_json.clone(),
                        now,
                        task.run_id.clone(),
                        task.step_id.clone(),
                        lease.task_id.clone(),
                        CONTINUATION_STATE_PENDING,
                        lease.attempt_id.clone(),
                        lease.fencing_token as i64,
                        transition_id.clone(),
                        intent_payload.clone(),
                    ),
                ),
                (
                    r#"
                    DELETE FROM leases
                    WHERE task_id = $1
                        AND attempt_id = $2
                        AND worker_id = $3
                        AND fencing_token = $4
                        AND EXISTS (
                            SELECT 1 FROM task_queue
                            WHERE task_id = $1
                                AND status = $5
                                AND continuation_state = $6
                        )
                        AND EXISTS (
                            SELECT 1 FROM transition_log
                            WHERE transition_id = $7 AND payload_json = $8
                        )
                    "#,
                    params!(
                        lease.task_id.clone(),
                        lease.attempt_id.clone(),
                        lease.worker_id.clone(),
                        lease.fencing_token as i64,
                        TASK_STATUS_COMPLETED,
                        CONTINUATION_STATE_PENDING,
                        transition_id.clone(),
                        intent_payload.clone(),
                    ),
                ),
                (
                    r#"
                    UPDATE transition_log
                    SET payload_json = $1
                    WHERE transition_id = $2
                        AND payload_json = $3
                        AND EXISTS (
                            SELECT 1 FROM task_queue t
                            INNER JOIN workflow_runs r ON r.run_id = t.run_id
                            WHERE t.task_id = $4
                                AND t.status = $5
                                AND t.continuation_state = $6
                                AND t.active_attempt_id = $7
                                AND t.active_fencing_token = $8
                                AND r.status = $9
                        )
                        AND EXISTS (
                            SELECT 1 FROM step_instances
                            WHERE run_id = $10
                                AND step_id = $11
                                AND status = $5
                                AND output_json = $12
                                AND error_json IS NULL
                        )
                        AND NOT EXISTS (
                            SELECT 1 FROM leases WHERE task_id = $4
                        )
                    "#,
                    params!(
                        applied_payload,
                        transition_id.clone(),
                        intent_payload.clone(),
                        lease.task_id.clone(),
                        TASK_STATUS_COMPLETED,
                        CONTINUATION_STATE_PENDING,
                        lease.attempt_id.clone(),
                        lease.fencing_token as i64,
                        RUN_STATUS_RUNNING,
                        task.run_id.clone(),
                        task.step_id.clone(),
                        output_json,
                    ),
                ),
                (
                    // An intent left behind means one of the mandatory writes did not
                    // happen. Violating the NOT NULL constraint aborts the whole txn.
                    r#"
                    UPDATE transition_log
                    SET run_id = NULL
                    WHERE transition_id = $1 AND payload_json = $2
                    "#,
                    params!(transition_id.clone(), intent_payload),
                ),
            ])
            .await?;

        let counts = txn_counts(results)?;
        if counts.as_slice() == [1, 1, 1, 1, 1, 0] {
            return Ok(TaskOutcomeApplyResult::Applied);
        }
        if counts.iter().all(|count| *count == 0) {
            return self
                .classify_unapplied_task_outcome(&lease.task_id, &transition_id, now)
                .await;
        }
        Err(PersistenceError::Invariant(format!(
            "complete_task_with_continuation produced unexpected row counts: {counts:?}"
        )))
    }

    /// Atomically releases a failed attempt into retry-pending state and creates
    /// its deterministic retry timer. The stored due time is clamped to the
    /// task's absolute timeout, so a retry can never extend the task budget.
    pub async fn fail_task_and_schedule_retry(
        &self,
        lease: &TaskMutationContext,
        failure: &Value,
        requested_due_at: i64,
        now: i64,
    ) -> Result<TaskOutcomeApplyResult, PersistenceError> {
        let Some(task) = self.get_task_row(&lease.task_id).await? else {
            return Ok(TaskOutcomeApplyResult::Stale);
        };
        let transition_id = stable_task_transition_id("retry-scheduled", lease);
        let timer_id = stable_task_timer_id("retry", lease);
        let intent_payload = serde_json::to_string(&serde_json::json!({
            "atomic_state": "intent",
            "task_id": task.task_id,
            "step_id": task.step_id,
            "attempt_id": lease.attempt_id,
        }))?;
        let applied_payload = serde_json::to_string(&serde_json::json!({
            "task_id": task.task_id,
            "step_id": task.step_id,
            "attempt_id": lease.attempt_id,
            "timer_id": timer_id,
        }))?;
        let timer_payload = serde_json::to_string(&serde_json::json!({
            "task_id": task.task_id,
            "attempt_id": lease.attempt_id,
        }))?;
        let failure_json = serde_json::to_string(failure)?;

        let results = self
            .client
            .txn([
                (
                    r#"
                    INSERT OR IGNORE INTO transition_log (
                        transition_id, run_id, transition_type, payload_json, created_at
                    )
                    SELECT $1, t.run_id, $2, $3, $4
                    FROM task_queue t
                    INNER JOIN workflow_runs r ON r.run_id = t.run_id
                    INNER JOIN leases l
                        ON l.task_id = t.task_id
                        AND l.attempt_id = t.active_attempt_id
                        AND l.worker_id = t.leased_by_worker_id
                        AND l.fencing_token = t.active_fencing_token
                    WHERE t.task_id = $5
                        AND t.status = $6
                        AND t.active_attempt_id = $7
                        AND t.active_fencing_token = $8
                        AND t.leased_by_worker_id = $9
                        AND l.attempt_id = $7
                        AND l.fencing_token = $8
                        AND l.worker_id = $9
                        AND l.expires_at > $4
                        AND r.status = $10
                        AND (t.timeout_at IS NULL OR t.timeout_at > $4)
                    "#,
                    params!(
                        transition_id.clone(),
                        "retry_timer_scheduled".to_owned(),
                        intent_payload.clone(),
                        now,
                        lease.task_id.clone(),
                        TASK_STATUS_LEASED,
                        lease.attempt_id.clone(),
                        lease.fencing_token as i64,
                        lease.worker_id.clone(),
                        RUN_STATUS_RUNNING,
                    ),
                ),
                (
                    r#"
                    UPDATE task_queue
                    SET status = $1,
                        leased_by_worker_id = NULL,
                        updated_at = $2
                    WHERE task_id = $3
                        AND status = $4
                        AND active_attempt_id = $5
                        AND active_fencing_token = $6
                        AND leased_by_worker_id = $7
                        AND (timeout_at IS NULL OR timeout_at > $2)
                        AND EXISTS (
                            SELECT 1 FROM workflow_runs
                            WHERE run_id = task_queue.run_id AND status = $8
                        )
                        AND EXISTS (
                            SELECT 1 FROM transition_log
                            WHERE transition_id = $9 AND payload_json = $10
                        )
                    "#,
                    params!(
                        TASK_STATUS_RETRY_PENDING,
                        now,
                        lease.task_id.clone(),
                        TASK_STATUS_LEASED,
                        lease.attempt_id.clone(),
                        lease.fencing_token as i64,
                        lease.worker_id.clone(),
                        RUN_STATUS_RUNNING,
                        transition_id.clone(),
                        intent_payload.clone(),
                    ),
                ),
                (
                    r#"
                    UPDATE step_instances
                    SET status = $1,
                        attempt_count = $2,
                        output_json = NULL,
                        error_json = $3,
                        updated_at = $4
                    WHERE run_id = $5
                        AND step_id = $6
                        AND EXISTS (
                            SELECT 1 FROM task_queue
                            WHERE task_id = $7
                                AND status = $1
                                AND active_attempt_id = $8
                                AND active_fencing_token = $9
                        )
                        AND EXISTS (
                            SELECT 1 FROM transition_log
                            WHERE transition_id = $10 AND payload_json = $11
                        )
                    "#,
                    params!(
                        TASK_STATUS_RETRY_PENDING,
                        task.attempt_sequence,
                        failure_json.clone(),
                        now,
                        task.run_id.clone(),
                        task.step_id.clone(),
                        lease.task_id.clone(),
                        lease.attempt_id.clone(),
                        lease.fencing_token as i64,
                        transition_id.clone(),
                        intent_payload.clone(),
                    ),
                ),
                (
                    r#"
                    INSERT OR IGNORE INTO timers (
                        timer_id, run_id, step_id, timer_kind, due_at, status,
                        payload_json, effect_state, created_at, updated_at
                    )
                    SELECT $1, t.run_id, t.step_id, $2,
                        CASE
                            WHEN t.timeout_at IS NULL OR $3 <= t.timeout_at THEN $3
                            ELSE t.timeout_at
                        END,
                        $4, $5, $6, $7, $7
                    FROM task_queue t
                    WHERE t.task_id = $8
                        AND t.status = $9
                        AND t.active_attempt_id = $10
                        AND t.active_fencing_token = $11
                        AND EXISTS (
                            SELECT 1 FROM transition_log
                            WHERE transition_id = $12 AND payload_json = $13
                        )
                    "#,
                    params!(
                        timer_id.clone(),
                        "retry".to_owned(),
                        requested_due_at,
                        TIMER_STATUS_PENDING,
                        timer_payload.clone(),
                        TIMER_EFFECT_STATE_PENDING,
                        now,
                        lease.task_id.clone(),
                        TASK_STATUS_RETRY_PENDING,
                        lease.attempt_id.clone(),
                        lease.fencing_token as i64,
                        transition_id.clone(),
                        intent_payload.clone(),
                    ),
                ),
                (
                    r#"
                    DELETE FROM leases
                    WHERE task_id = $1
                        AND attempt_id = $2
                        AND worker_id = $3
                        AND fencing_token = $4
                        AND EXISTS (
                            SELECT 1 FROM transition_log
                            WHERE transition_id = $5 AND payload_json = $6
                        )
                    "#,
                    params!(
                        lease.task_id.clone(),
                        lease.attempt_id.clone(),
                        lease.worker_id.clone(),
                        lease.fencing_token as i64,
                        transition_id.clone(),
                        intent_payload.clone(),
                    ),
                ),
                (
                    r#"
                    UPDATE transition_log
                    SET payload_json = $1
                    WHERE transition_id = $2
                        AND payload_json = $3
                        AND EXISTS (
                            SELECT 1 FROM task_queue t
                            INNER JOIN workflow_runs r ON r.run_id = t.run_id
                            WHERE t.task_id = $4
                                AND t.status = $5
                                AND t.leased_by_worker_id IS NULL
                                AND t.active_attempt_id = $6
                                AND t.active_fencing_token = $7
                                AND r.status = $8
                        )
                        AND EXISTS (
                            SELECT 1 FROM step_instances
                            WHERE run_id = $9
                                AND step_id = $10
                                AND status = $5
                                AND attempt_count = $11
                                AND error_json = $12
                        )
                        AND EXISTS (
                            SELECT 1 FROM timers timer
                            INNER JOIN task_queue t ON t.run_id = timer.run_id
                                AND t.step_id = timer.step_id
                            WHERE timer.timer_id = $13
                                AND t.task_id = $4
                                AND timer.timer_kind = $14
                                AND timer.status = $15
                                AND timer.effect_state = $16
                                AND timer.payload_json = $17
                                AND (t.timeout_at IS NULL OR timer.due_at <= t.timeout_at)
                        )
                        AND NOT EXISTS (
                            SELECT 1 FROM leases WHERE task_id = $4
                        )
                    "#,
                    params!(
                        applied_payload,
                        transition_id.clone(),
                        intent_payload.clone(),
                        lease.task_id.clone(),
                        TASK_STATUS_RETRY_PENDING,
                        lease.attempt_id.clone(),
                        lease.fencing_token as i64,
                        RUN_STATUS_RUNNING,
                        task.run_id.clone(),
                        task.step_id.clone(),
                        task.attempt_sequence,
                        failure_json,
                        timer_id,
                        "retry".to_owned(),
                        TIMER_STATUS_PENDING,
                        TIMER_EFFECT_STATE_PENDING,
                        timer_payload,
                    ),
                ),
                (
                    r#"
                    UPDATE transition_log
                    SET run_id = NULL
                    WHERE transition_id = $1 AND payload_json = $2
                    "#,
                    params!(transition_id.clone(), intent_payload),
                ),
            ])
            .await?;

        let counts = txn_counts(results)?;
        if counts.as_slice() == [1, 1, 1, 1, 1, 1, 0] {
            return Ok(TaskOutcomeApplyResult::Applied);
        }
        if counts.iter().all(|count| *count == 0) {
            return self
                .classify_unapplied_task_outcome(&lease.task_id, &transition_id, now)
                .await;
        }
        Err(PersistenceError::Invariant(format!(
            "fail_task_and_schedule_retry produced unexpected row counts: {counts:?}"
        )))
    }

    /// Atomically fails a worker attempt and terminalizes every open durable
    /// effect owned by its run.
    pub async fn fail_task_terminal(
        &self,
        lease: &TaskMutationContext,
        failure: &Value,
        now: i64,
    ) -> Result<TaskOutcomeApplyResult, PersistenceError> {
        let Some(task) = self.get_task_row(&lease.task_id).await? else {
            return Ok(TaskOutcomeApplyResult::Stale);
        };
        let transition_id = stable_task_transition_id("failed", lease);
        let run_transition_id = stable_run_failure_transition_id(&task.run_id, &transition_id);
        let intent_payload = serde_json::to_string(&serde_json::json!({
            "atomic_state": "intent",
            "task_id": task.task_id,
            "step_id": task.step_id,
            "attempt_id": lease.attempt_id,
        }))?;
        let applied_payload = serde_json::to_string(&serde_json::json!({
            "task_id": task.task_id,
            "step_id": task.step_id,
            "attempt_id": lease.attempt_id,
        }))?;
        let run_failure_payload = serde_json::to_string(&serde_json::json!({
            "failed_task_id": task.task_id,
            "attempt_id": lease.attempt_id,
        }))?;
        let failure_json = serde_json::to_string(failure)?;
        let sibling_failure_json = serde_json::to_string(&serde_json::json!({
            "reason": "run_failed",
            "failed_task_id": task.task_id,
        }))?;

        let results = self
            .client
            .txn([
                (
                    r#"
                    INSERT OR IGNORE INTO transition_log (
                        transition_id, run_id, transition_type, payload_json, created_at
                    )
                    SELECT $1, t.run_id, $2, $3, $4
                    FROM task_queue t
                    INNER JOIN workflow_runs r ON r.run_id = t.run_id
                    INNER JOIN leases l
                        ON l.task_id = t.task_id
                        AND l.attempt_id = t.active_attempt_id
                        AND l.worker_id = t.leased_by_worker_id
                        AND l.fencing_token = t.active_fencing_token
                    WHERE t.task_id = $5
                        AND t.status = $6
                        AND t.active_attempt_id = $7
                        AND t.active_fencing_token = $8
                        AND t.leased_by_worker_id = $9
                        AND l.attempt_id = $7
                        AND l.fencing_token = $8
                        AND l.worker_id = $9
                        AND l.expires_at > $4
                        AND r.status = $10
                        AND (t.timeout_at IS NULL OR t.timeout_at > $4)
                    "#,
                    params!(
                        transition_id.clone(),
                        "task_failed".to_owned(),
                        intent_payload.clone(),
                        now,
                        lease.task_id.clone(),
                        TASK_STATUS_LEASED,
                        lease.attempt_id.clone(),
                        lease.fencing_token as i64,
                        lease.worker_id.clone(),
                        RUN_STATUS_RUNNING,
                    ),
                ),
                (
                    r#"
                    UPDATE workflow_runs
                    SET status = $1, updated_at = $2
                    WHERE run_id = $3
                        AND status = $4
                        AND EXISTS (
                            SELECT 1 FROM transition_log
                            WHERE transition_id = $5 AND payload_json = $6
                        )
                    "#,
                    params!(
                        RUN_STATUS_FAILED,
                        now,
                        task.run_id.clone(),
                        RUN_STATUS_RUNNING,
                        transition_id.clone(),
                        intent_payload.clone(),
                    ),
                ),
                (
                    r#"
                    UPDATE task_queue
                    SET status = CASE
                            WHEN status IN ($1, $2, $3) THEN $4
                            ELSE status
                        END,
                        continuation_state = CASE
                            WHEN continuation_state = $5 THEN $6
                            ELSE continuation_state
                        END,
                        active_attempt_id = CASE
                            WHEN status IN ($1, $2, $3) THEN NULL
                            ELSE active_attempt_id
                        END,
                        active_fencing_token = CASE
                            WHEN status IN ($1, $2, $3) THEN NULL
                            ELSE active_fencing_token
                        END,
                        leased_by_worker_id = CASE
                            WHEN status IN ($1, $2, $3) THEN NULL
                            ELSE leased_by_worker_id
                        END,
                        updated_at = $7
                    WHERE run_id = $8
                        AND (
                            status IN ($1, $2, $3)
                            OR continuation_state = $5
                        )
                        AND EXISTS (
                            SELECT 1 FROM workflow_runs
                            WHERE run_id = $8 AND status = $9
                        )
                        AND EXISTS (
                            SELECT 1 FROM transition_log
                            WHERE transition_id = $10 AND payload_json = $11
                        )
                    "#,
                    params!(
                        TASK_STATUS_QUEUED,
                        TASK_STATUS_LEASED,
                        TASK_STATUS_RETRY_PENDING,
                        TASK_STATUS_FAILED,
                        CONTINUATION_STATE_PENDING,
                        CONTINUATION_STATE_SUPPRESSED,
                        now,
                        task.run_id.clone(),
                        RUN_STATUS_FAILED,
                        transition_id.clone(),
                        intent_payload.clone(),
                    ),
                ),
                (
                    r#"
                    UPDATE step_instances
                    SET status = $1,
                        attempt_count = CASE
                            WHEN step_id = $2 THEN $3
                            ELSE attempt_count
                        END,
                        output_json = NULL,
                        error_json = CASE
                            WHEN step_id = $2 THEN $4
                            ELSE $5
                        END,
                        updated_at = $6
                    WHERE run_id = $7
                        AND status NOT IN ($8, $1, $9, $10)
                        AND EXISTS (
                            SELECT 1 FROM workflow_runs
                            WHERE run_id = $7 AND status = $11
                        )
                        AND EXISTS (
                            SELECT 1 FROM transition_log
                            WHERE transition_id = $12 AND payload_json = $13
                        )
                    "#,
                    params!(
                        TASK_STATUS_FAILED,
                        task.step_id.clone(),
                        task.attempt_sequence,
                        failure_json.clone(),
                        sibling_failure_json,
                        now,
                        task.run_id.clone(),
                        TASK_STATUS_COMPLETED,
                        TASK_STATUS_TIMED_OUT,
                        "cancelled".to_owned(),
                        RUN_STATUS_FAILED,
                        transition_id.clone(),
                        intent_payload.clone(),
                    ),
                ),
                (
                    r#"
                    UPDATE timers
                    SET status = CASE
                            WHEN status = $1 THEN $2
                            ELSE status
                        END,
                        effect_state = CASE
                            WHEN effect_state IS NULL OR effect_state = $3 THEN $4
                            ELSE effect_state
                        END,
                        updated_at = $5
                    WHERE run_id = $6
                        AND (status = $1 OR effect_state = $3)
                        AND EXISTS (
                            SELECT 1 FROM transition_log
                            WHERE transition_id = $7 AND payload_json = $8
                        )
                    "#,
                    params!(
                        TIMER_STATUS_PENDING,
                        TIMER_STATUS_CANCELLED,
                        TIMER_EFFECT_STATE_PENDING,
                        TIMER_EFFECT_STATE_SUPPRESSED,
                        now,
                        task.run_id.clone(),
                        transition_id.clone(),
                        intent_payload.clone(),
                    ),
                ),
                (
                    r#"
                    DELETE FROM leases
                    WHERE task_id IN (
                        SELECT task_id FROM task_queue WHERE run_id = $1
                    )
                        AND EXISTS (
                            SELECT 1 FROM transition_log
                            WHERE transition_id = $2 AND payload_json = $3
                        )
                    "#,
                    params!(
                        task.run_id.clone(),
                        transition_id.clone(),
                        intent_payload.clone(),
                    ),
                ),
                (
                    r#"
                    INSERT OR IGNORE INTO transition_log (
                        transition_id, run_id, transition_type, payload_json, created_at
                    )
                    SELECT $1, $2, $3, $4, $5
                    WHERE EXISTS (
                            SELECT 1 FROM transition_log
                            WHERE transition_id = $6 AND payload_json = $7
                        )
                        AND EXISTS (
                            SELECT 1 FROM workflow_runs
                            WHERE run_id = $2 AND status = $8
                        )
                        AND NOT EXISTS (
                            SELECT 1 FROM task_queue
                            WHERE run_id = $2
                                AND (
                                    status IN ($9, $10, $11)
                                    OR continuation_state = $12
                                )
                        )
                        AND NOT EXISTS (
                            SELECT 1 FROM timers
                            WHERE run_id = $2
                                AND (status = $13 OR effect_state = $14)
                        )
                        AND NOT EXISTS (
                            SELECT 1 FROM leases
                            WHERE task_id IN (
                                SELECT task_id FROM task_queue WHERE run_id = $2
                            )
                        )
                        AND EXISTS (
                            SELECT 1 FROM step_instances
                            WHERE run_id = $2
                                AND step_id = $15
                                AND status = $16
                                AND error_json = $17
                        )
                    "#,
                    params!(
                        run_transition_id.clone(),
                        task.run_id.clone(),
                        "run_failed".to_owned(),
                        run_failure_payload,
                        now,
                        transition_id.clone(),
                        intent_payload.clone(),
                        RUN_STATUS_FAILED,
                        TASK_STATUS_QUEUED,
                        TASK_STATUS_LEASED,
                        TASK_STATUS_RETRY_PENDING,
                        CONTINUATION_STATE_PENDING,
                        TIMER_STATUS_PENDING,
                        TIMER_EFFECT_STATE_PENDING,
                        task.step_id.clone(),
                        TASK_STATUS_FAILED,
                        failure_json,
                    ),
                ),
                (
                    r#"
                    UPDATE transition_log
                    SET payload_json = $1
                    WHERE transition_id = $2
                        AND payload_json = $3
                        AND EXISTS (
                            SELECT 1 FROM transition_log
                            WHERE transition_id = $4 AND transition_type = $5
                        )
                    "#,
                    params!(
                        applied_payload,
                        transition_id.clone(),
                        intent_payload.clone(),
                        run_transition_id,
                        "run_failed".to_owned(),
                    ),
                ),
                (
                    r#"
                    UPDATE transition_log
                    SET run_id = NULL
                    WHERE transition_id = $1 AND payload_json = $2
                    "#,
                    params!(transition_id.clone(), intent_payload),
                ),
            ])
            .await?;

        let counts = txn_counts(results)?;
        if counts.len() == 9
            && counts[0] == 1
            && counts[1] == 1
            && counts[2] >= 1
            && counts[3] >= 1
            && counts[5] >= 1
            && counts[6] == 1
            && counts[7] == 1
            && counts[8] == 0
        {
            return Ok(TaskOutcomeApplyResult::Applied);
        }
        if counts.iter().all(|count| *count == 0) {
            return self
                .classify_unapplied_task_outcome(&lease.task_id, &transition_id, now)
                .await;
        }
        Err(PersistenceError::Invariant(format!(
            "fail_task_terminal produced unexpected row counts: {counts:?}"
        )))
    }

    /// Claims a due timeout and atomically terminalizes all durable work owned by
    /// the run. Equality belongs to timeout recovery (`timeout_at <= now`).
    pub async fn expire_task_if_due(
        &self,
        task_id: &str,
        now: i64,
    ) -> Result<TaskOutcomeApplyResult, PersistenceError> {
        let Some(task) = self.get_task_row(task_id).await? else {
            return Ok(TaskOutcomeApplyResult::Stale);
        };
        let transition_id = stable_task_timeout_transition_id(&task.task_id);
        let run_transition_id = stable_run_failure_transition_id(&task.run_id, &transition_id);
        let intent_payload = serde_json::to_string(&serde_json::json!({
            "atomic_state": "intent",
            "task_id": task.task_id,
            "step_id": task.step_id,
        }))?;
        let applied_payload = serde_json::to_string(&serde_json::json!({
            "task_id": task.task_id,
            "step_id": task.step_id,
        }))?;
        let timeout_error_json = serde_json::to_string(&serde_json::json!({
            "reason": "task_timeout"
        }))?;
        let sibling_failure_json = serde_json::to_string(&serde_json::json!({
            "reason": "run_failed",
            "timed_out_task_id": task.task_id,
        }))?;
        let run_failure_payload = serde_json::to_string(&serde_json::json!({
            "timed_out_task_id": task.task_id,
        }))?;

        let results = self
            .client
            .txn([
                (
                    r#"
                    INSERT OR IGNORE INTO transition_log (
                        transition_id, run_id, transition_type, payload_json, created_at
                    )
                    SELECT $1, t.run_id, $2, $3, $4
                    FROM task_queue t
                    INNER JOIN workflow_runs r ON r.run_id = t.run_id
                    WHERE t.task_id = $5
                        AND t.status IN ($6, $7, $8)
                        AND t.timeout_at IS NOT NULL
                        AND t.timeout_at <= $4
                        AND r.status = $9
                    "#,
                    params!(
                        transition_id.clone(),
                        "task_timed_out".to_owned(),
                        intent_payload.clone(),
                        now,
                        task.task_id.clone(),
                        TASK_STATUS_QUEUED,
                        TASK_STATUS_LEASED,
                        TASK_STATUS_RETRY_PENDING,
                        RUN_STATUS_RUNNING,
                    ),
                ),
                (
                    r#"
                    UPDATE workflow_runs
                    SET status = $1, updated_at = $2
                    WHERE run_id = $3
                        AND status = $4
                        AND EXISTS (
                            SELECT 1 FROM transition_log
                            WHERE transition_id = $5 AND payload_json = $6
                        )
                    "#,
                    params!(
                        RUN_STATUS_FAILED,
                        now,
                        task.run_id.clone(),
                        RUN_STATUS_RUNNING,
                        transition_id.clone(),
                        intent_payload.clone(),
                    ),
                ),
                (
                    r#"
                    UPDATE task_queue
                    SET status = CASE
                            WHEN task_id = $1 THEN $2
                            WHEN status IN ($3, $4, $5) THEN $6
                            ELSE status
                        END,
                        continuation_state = CASE
                            WHEN continuation_state = $7 THEN $8
                            ELSE continuation_state
                        END,
                        active_attempt_id = CASE
                            WHEN status IN ($3, $4, $5) THEN NULL
                            ELSE active_attempt_id
                        END,
                        active_fencing_token = CASE
                            WHEN status IN ($3, $4, $5) THEN NULL
                            ELSE active_fencing_token
                        END,
                        leased_by_worker_id = CASE
                            WHEN status IN ($3, $4, $5) THEN NULL
                            ELSE leased_by_worker_id
                        END,
                        updated_at = $9
                    WHERE run_id = $10
                        AND (
                            status IN ($3, $4, $5)
                            OR continuation_state = $7
                        )
                        AND EXISTS (
                            SELECT 1 FROM transition_log
                            WHERE transition_id = $11 AND payload_json = $12
                        )
                    "#,
                    params!(
                        task.task_id.clone(),
                        TASK_STATUS_TIMED_OUT,
                        TASK_STATUS_QUEUED,
                        TASK_STATUS_LEASED,
                        TASK_STATUS_RETRY_PENDING,
                        TASK_STATUS_FAILED,
                        CONTINUATION_STATE_PENDING,
                        CONTINUATION_STATE_SUPPRESSED,
                        now,
                        task.run_id.clone(),
                        transition_id.clone(),
                        intent_payload.clone(),
                    ),
                ),
                (
                    r#"
                    UPDATE step_instances
                    SET status = CASE
                            WHEN step_id = $1 THEN $2
                            ELSE $3
                        END,
                        attempt_count = CASE
                            WHEN step_id = $1 THEN $4
                            ELSE attempt_count
                        END,
                        output_json = NULL,
                        error_json = CASE
                            WHEN step_id = $1 THEN $5
                            ELSE $6
                        END,
                        updated_at = $7
                    WHERE run_id = $8
                        AND status NOT IN ($9, $3, $2, $10)
                        AND EXISTS (
                            SELECT 1 FROM transition_log
                            WHERE transition_id = $11 AND payload_json = $12
                        )
                    "#,
                    params!(
                        task.step_id.clone(),
                        TASK_STATUS_TIMED_OUT,
                        TASK_STATUS_FAILED,
                        task.attempt_sequence,
                        timeout_error_json.clone(),
                        sibling_failure_json,
                        now,
                        task.run_id.clone(),
                        TASK_STATUS_COMPLETED,
                        "cancelled".to_owned(),
                        transition_id.clone(),
                        intent_payload.clone(),
                    ),
                ),
                (
                    r#"
                    UPDATE timers
                    SET status = CASE
                            WHEN status = $1 THEN $2
                            ELSE status
                        END,
                        effect_state = CASE
                            WHEN effect_state IS NULL OR effect_state = $3 THEN $4
                            ELSE effect_state
                        END,
                        updated_at = $5
                    WHERE run_id = $6
                        AND (status = $1 OR effect_state = $3)
                        AND EXISTS (
                            SELECT 1 FROM transition_log
                            WHERE transition_id = $7 AND payload_json = $8
                        )
                    "#,
                    params!(
                        TIMER_STATUS_PENDING,
                        TIMER_STATUS_CANCELLED,
                        TIMER_EFFECT_STATE_PENDING,
                        TIMER_EFFECT_STATE_SUPPRESSED,
                        now,
                        task.run_id.clone(),
                        transition_id.clone(),
                        intent_payload.clone(),
                    ),
                ),
                (
                    r#"
                    DELETE FROM leases
                    WHERE task_id IN (
                        SELECT task_id FROM task_queue WHERE run_id = $1
                    )
                        AND EXISTS (
                            SELECT 1 FROM transition_log
                            WHERE transition_id = $2 AND payload_json = $3
                        )
                    "#,
                    params!(
                        task.run_id.clone(),
                        transition_id.clone(),
                        intent_payload.clone(),
                    ),
                ),
                (
                    r#"
                    INSERT OR IGNORE INTO transition_log (
                        transition_id, run_id, transition_type, payload_json, created_at
                    )
                    SELECT $1, $2, $3, $4, $5
                    WHERE EXISTS (
                            SELECT 1 FROM transition_log
                            WHERE transition_id = $6 AND payload_json = $7
                        )
                        AND EXISTS (
                            SELECT 1 FROM workflow_runs
                            WHERE run_id = $2 AND status = $8
                        )
                        AND EXISTS (
                            SELECT 1 FROM task_queue
                            WHERE task_id = $9 AND status = $10
                        )
                        AND NOT EXISTS (
                            SELECT 1 FROM task_queue
                            WHERE run_id = $2
                                AND (
                                    status IN ($11, $12, $13)
                                    OR continuation_state = $14
                                )
                        )
                        AND NOT EXISTS (
                            SELECT 1 FROM timers
                            WHERE run_id = $2
                                AND (status = $15 OR effect_state = $16)
                        )
                        AND NOT EXISTS (
                            SELECT 1 FROM leases
                            WHERE task_id IN (
                                SELECT task_id FROM task_queue WHERE run_id = $2
                            )
                        )
                        AND EXISTS (
                            SELECT 1 FROM step_instances
                            WHERE run_id = $2
                                AND step_id = $17
                                AND status = $10
                                AND error_json = $18
                        )
                    "#,
                    params!(
                        run_transition_id.clone(),
                        task.run_id.clone(),
                        "run_failed".to_owned(),
                        run_failure_payload,
                        now,
                        transition_id.clone(),
                        intent_payload.clone(),
                        RUN_STATUS_FAILED,
                        task.task_id.clone(),
                        TASK_STATUS_TIMED_OUT,
                        TASK_STATUS_QUEUED,
                        TASK_STATUS_LEASED,
                        TASK_STATUS_RETRY_PENDING,
                        CONTINUATION_STATE_PENDING,
                        TIMER_STATUS_PENDING,
                        TIMER_EFFECT_STATE_PENDING,
                        task.step_id.clone(),
                        timeout_error_json,
                    ),
                ),
                (
                    r#"
                    UPDATE transition_log
                    SET payload_json = $1
                    WHERE transition_id = $2
                        AND payload_json = $3
                        AND EXISTS (
                            SELECT 1 FROM transition_log
                            WHERE transition_id = $4 AND transition_type = $5
                        )
                    "#,
                    params!(
                        applied_payload,
                        transition_id.clone(),
                        intent_payload.clone(),
                        run_transition_id,
                        "run_failed".to_owned(),
                    ),
                ),
                (
                    r#"
                    UPDATE transition_log
                    SET run_id = NULL
                    WHERE transition_id = $1 AND payload_json = $2
                    "#,
                    params!(transition_id.clone(), intent_payload),
                ),
            ])
            .await?;

        let counts = txn_counts(results)?;
        if counts.len() == 9
            && counts[0] == 1
            && counts[1] == 1
            && counts[2] >= 1
            && counts[3] >= 1
            && counts[6] == 1
            && counts[7] == 1
            && counts[8] == 0
        {
            return Ok(TaskOutcomeApplyResult::Applied);
        }
        if counts.iter().all(|count| *count == 0) {
            return if self.task_outcome_transition_exists(&transition_id).await? {
                Ok(TaskOutcomeApplyResult::AlreadyApplied)
            } else {
                Ok(TaskOutcomeApplyResult::Stale)
            };
        }
        Err(PersistenceError::Invariant(format!(
            "expire_task_if_due produced unexpected row counts: {counts:?}"
        )))
    }

    pub(crate) async fn get_pending_completed_task_continuation(
        &self,
        task_id: &str,
    ) -> Result<Option<CompletedTaskContinuation>, PersistenceError> {
        let row = self
            .client
            .query_as_optional::<CompletedTaskContinuationRow, _>(
                r#"
                SELECT
                    t.task_id,
                    t.run_id,
                    t.workflow_id,
                    t.workflow_version,
                    t.step_id,
                    s.output_json
                FROM task_queue t INDEXED BY idx_task_queue_continuation_recovery
                INNER JOIN step_instances s
                    ON s.run_id = t.run_id AND s.step_id = t.step_id
                WHERE t.task_id = $1
                    AND t.status = $2
                    AND t.continuation_state = $3
                LIMIT 1
                "#,
                params!(
                    task_id.to_owned(),
                    TASK_STATUS_COMPLETED,
                    CONTINUATION_STATE_PENDING,
                ),
            )
            .await?;

        row.map(TryInto::try_into).transpose()
    }

    pub(crate) async fn list_pending_completed_task_continuations(
        &self,
        now: i64,
    ) -> Result<Vec<CompletedTaskContinuation>, PersistenceError> {
        let rows = self
            .client
            .query_as::<CompletedTaskContinuationRow, _>(
                r#"
                SELECT
                    t.task_id,
                    t.run_id,
                    t.workflow_id,
                    t.workflow_version,
                    t.step_id,
                    s.output_json
                FROM task_queue t INDEXED BY idx_task_queue_continuation_recovery
                INNER JOIN step_instances s
                    ON s.run_id = t.run_id AND s.step_id = t.step_id
                WHERE t.status = $1
                    AND t.continuation_state = $2
                ORDER BY t.updated_at ASC, t.task_id ASC
                LIMIT $3
                "#,
                params!(
                    TASK_STATUS_COMPLETED,
                    CONTINUATION_STATE_PENDING,
                    RECOVERY_SCAN_BATCH_SIZE,
                ),
            )
            .await?;

        let mut continuations = Vec::with_capacity(rows.len());
        for row in rows {
            let task_id = row.task_id.clone();
            match row.try_into() {
                Ok(continuation) => continuations.push(continuation),
                Err(PersistenceError::InvalidStoredWorkflowVersion { .. }) => {
                    self.quarantine_task_continuation(&task_id, "invalid_workflow_version", now)
                        .await?;
                }
                Err(PersistenceError::Invariant(_)) | Err(PersistenceError::Json(_)) => {
                    self.quarantine_task_continuation(&task_id, "invalid_continuation_output", now)
                        .await?;
                }
                Err(error) => return Err(error),
            }
        }
        Ok(continuations)
    }

    /// Durably removes a queued, leased, or retry-pending task with invalid
    /// pinned execution provenance from worker and recovery scans. Existing
    /// lease and fencing evidence is intentionally preserved for inspection.
    #[allow(dead_code)] // Private runtime seam for pinned-provenance validation.
    pub(crate) async fn quarantine_task_execution(
        &self,
        task_id: &str,
        stable_code: &str,
        now: i64,
    ) -> Result<ContinuationQuarantineResult, PersistenceError> {
        validate_quarantine_code(stable_code)?;
        let marker = format!("{CONTINUATION_QUARANTINE_PREFIX}{stable_code}");
        let transition_id = stable_outcome_id("task-execution-quarantined", &[task_id]);
        let payload = serde_json::to_string(&serde_json::json!({
            "task_id": task_id,
            "code": stable_code,
        }))?;
        let results = self
            .client
            .txn([
                (
                    r#"
                    INSERT OR IGNORE INTO transition_log (
                        transition_id, run_id, transition_type, payload_json, created_at
                    )
                    SELECT $1, task.run_id, $2, $3, $4
                    FROM task_queue task
                    INNER JOIN workflow_runs run ON run.run_id = task.run_id
                    WHERE task.task_id = $5
                        AND task.status IN ($6, $7, $8)
                        AND task.continuation_state IS NULL
                        AND run.status = $9
                    "#,
                    params!(
                        transition_id.clone(),
                        "task_execution_quarantined".to_owned(),
                        payload.clone(),
                        now,
                        task_id.to_owned(),
                        TASK_STATUS_QUEUED,
                        TASK_STATUS_LEASED,
                        TASK_STATUS_RETRY_PENDING,
                        RUN_STATUS_RUNNING,
                    ),
                ),
                (
                    r#"
                    UPDATE task_queue
                    SET continuation_state = $1, updated_at = $2
                    WHERE task_id = $3
                        AND status IN ($4, $5, $6)
                        AND continuation_state IS NULL
                        AND EXISTS (
                            SELECT 1 FROM workflow_runs
                            WHERE run_id = task_queue.run_id AND status = $7
                        )
                        AND EXISTS (
                            SELECT 1 FROM transition_log
                            WHERE transition_id = $8 AND transition_type = $9
                                AND payload_json = $10
                        )
                    "#,
                    params!(
                        marker.clone(),
                        now,
                        task_id.to_owned(),
                        TASK_STATUS_QUEUED,
                        TASK_STATUS_LEASED,
                        TASK_STATUS_RETRY_PENDING,
                        RUN_STATUS_RUNNING,
                        transition_id.clone(),
                        "task_execution_quarantined".to_owned(),
                        payload.clone(),
                    ),
                ),
            ])
            .await?;
        let counts = txn_counts(results)?;
        if matches!(counts.as_slice(), [1, 1] | [0, 1]) {
            return Ok(ContinuationQuarantineResult::Applied);
        }
        self.classify_continuation_quarantine(
            ContinuationQuarantineSource::Task,
            task_id,
            &marker,
            &transition_id,
            "task_execution_quarantined",
            &payload,
            &counts,
        )
        .await
    }

    /// Atomically consumes the exact worker lease into a durable manual-recovery
    /// quarantine while retaining the worker's structured failure as step and
    /// transition evidence. No registry defaults or retry policy are consulted.
    pub(crate) async fn quarantine_failed_task_execution(
        &self,
        lease: &TaskMutationContext,
        failure: &FailurePayload,
        stable_code: &str,
        now: i64,
    ) -> Result<TaskExecutionQuarantineResult, PersistenceError> {
        validate_quarantine_code(stable_code)?;
        let Some(task) = self.get_task_row(&lease.task_id).await? else {
            return Ok(TaskExecutionQuarantineResult::Stale);
        };
        let marker = format!("{CONTINUATION_QUARANTINE_PREFIX}{stable_code}");
        let fencing_token = lease.fencing_token.to_string();
        let transition_id = stable_outcome_id(
            "task-execution-failure-quarantined",
            &[
                &lease.task_id,
                &lease.attempt_id,
                &lease.worker_id,
                &fencing_token,
            ],
        );
        let failure_json = serde_json::to_string(failure)?;
        let transition_payload = serde_json::to_string(&serde_json::json!({
            "task_id": lease.task_id,
            "step_id": task.step_id,
            "attempt_id": lease.attempt_id,
            "worker_id": lease.worker_id,
            "fencing_token": lease.fencing_token,
            "code": stable_code,
            "failure": failure,
        }))?;
        let results = self
            .client
            .txn([
                (
                    r#"
                    INSERT OR IGNORE INTO transition_log (
                        transition_id, run_id, transition_type, payload_json, created_at
                    )
                    SELECT $1, task.run_id, $2, $3, $4
                    FROM task_queue task
                    INNER JOIN workflow_runs run ON run.run_id = task.run_id
                    INNER JOIN leases lease
                        ON lease.task_id = task.task_id
                        AND lease.attempt_id = task.active_attempt_id
                        AND lease.worker_id = task.leased_by_worker_id
                        AND lease.fencing_token = task.active_fencing_token
                    WHERE task.task_id = $5 AND task.status = $6
                        AND task.continuation_state IS NULL
                        AND task.active_attempt_id = $7
                        AND task.active_fencing_token = $8
                        AND task.leased_by_worker_id = $9
                        AND lease.attempt_id = $7 AND lease.fencing_token = $8
                        AND lease.worker_id = $9 AND lease.expires_at > $4
                        AND run.status = $10
                        AND (task.timeout_at IS NULL OR task.timeout_at > $4)
                    "#,
                    params!(
                        transition_id.clone(),
                        "task_execution_quarantined".to_owned(),
                        transition_payload.clone(),
                        now,
                        lease.task_id.clone(),
                        TASK_STATUS_LEASED,
                        lease.attempt_id.clone(),
                        lease.fencing_token as i64,
                        lease.worker_id.clone(),
                        RUN_STATUS_RUNNING,
                    ),
                ),
                (
                    r#"
                    UPDATE task_queue
                    SET status = $1, continuation_state = $2,
                        active_attempt_id = NULL, active_fencing_token = NULL,
                        leased_by_worker_id = NULL, updated_at = $3
                    WHERE task_id = $4 AND status = $5
                        AND continuation_state IS NULL
                        AND active_attempt_id = $6 AND active_fencing_token = $7
                        AND leased_by_worker_id = $8
                        AND EXISTS (
                            SELECT 1 FROM leases
                            WHERE task_id = $4 AND attempt_id = $6
                                AND fencing_token = $7 AND worker_id = $8
                        )
                        AND EXISTS (
                            SELECT 1 FROM transition_log
                            WHERE transition_id = $9 AND transition_type = $10
                                AND payload_json = $11
                        )
                    "#,
                    params!(
                        TASK_STATUS_FAILED,
                        marker.clone(),
                        now,
                        lease.task_id.clone(),
                        TASK_STATUS_LEASED,
                        lease.attempt_id.clone(),
                        lease.fencing_token as i64,
                        lease.worker_id.clone(),
                        transition_id.clone(),
                        "task_execution_quarantined".to_owned(),
                        transition_payload.clone(),
                    ),
                ),
                (
                    r#"
                    UPDATE step_instances
                    SET status = $1, attempt_count = $2, output_json = NULL,
                        error_json = $3, updated_at = $4
                    WHERE run_id = $5 AND step_id = $6
                        AND status NOT IN ($7, $8, $9)
                        AND EXISTS (
                            SELECT 1 FROM task_queue
                            WHERE task_id = $10 AND status = $1
                                AND continuation_state = $11
                        )
                        AND EXISTS (
                            SELECT 1 FROM leases
                            WHERE task_id = $10 AND attempt_id = $12
                                AND fencing_token = $13 AND worker_id = $14
                        )
                        AND EXISTS (
                            SELECT 1 FROM transition_log
                            WHERE transition_id = $15 AND transition_type = $16
                                AND payload_json = $17
                        )
                    "#,
                    params!(
                        TASK_STATUS_FAILED,
                        task.attempt_sequence,
                        failure_json.clone(),
                        now,
                        task.run_id.clone(),
                        task.step_id.clone(),
                        TASK_STATUS_COMPLETED,
                        TASK_STATUS_CANCELLED,
                        TASK_STATUS_TIMED_OUT,
                        lease.task_id.clone(),
                        marker.clone(),
                        lease.attempt_id.clone(),
                        lease.fencing_token as i64,
                        lease.worker_id.clone(),
                        transition_id.clone(),
                        "task_execution_quarantined".to_owned(),
                        transition_payload.clone(),
                    ),
                ),
                (
                    r#"
                    DELETE FROM leases
                    WHERE task_id = $1 AND attempt_id = $2
                        AND fencing_token = $3 AND worker_id = $4
                        AND EXISTS (
                            SELECT 1 FROM task_queue
                            WHERE task_id = $1 AND status = $5
                                AND continuation_state = $6
                        )
                        AND EXISTS (
                            SELECT 1 FROM step_instances step
                            INNER JOIN task_queue task
                                ON task.run_id = step.run_id AND task.step_id = step.step_id
                            WHERE task.task_id = $1 AND step.status = $5
                                AND step.error_json = $7
                        )
                        AND EXISTS (
                            SELECT 1 FROM transition_log
                            WHERE transition_id = $8 AND transition_type = $9
                                AND payload_json = $10
                        )
                    "#,
                    params!(
                        lease.task_id.clone(),
                        lease.attempt_id.clone(),
                        lease.fencing_token as i64,
                        lease.worker_id.clone(),
                        TASK_STATUS_FAILED,
                        marker.clone(),
                        failure_json.clone(),
                        transition_id.clone(),
                        "task_execution_quarantined".to_owned(),
                        transition_payload.clone(),
                    ),
                ),
                (
                    r#"
                    UPDATE transition_log
                    SET run_id = NULL
                    WHERE transition_id = $1 AND transition_type = $2
                        AND payload_json = $3
                        AND EXISTS (
                            SELECT 1 FROM leases
                            WHERE task_id = $4 AND attempt_id = $5
                                AND fencing_token = $6 AND worker_id = $7
                        )
                    "#,
                    params!(
                        transition_id.clone(),
                        "task_execution_quarantined".to_owned(),
                        transition_payload.clone(),
                        lease.task_id.clone(),
                        lease.attempt_id.clone(),
                        lease.fencing_token as i64,
                        lease.worker_id.clone(),
                    ),
                ),
            ])
            .await?;
        let counts = txn_counts(results)?;
        if counts.as_slice() == [1, 1, 1, 1, 0] {
            return Ok(TaskExecutionQuarantineResult::Applied);
        }
        if counts.iter().any(|count| *count != 0) {
            return Err(PersistenceError::Invariant(format!(
                "quarantine_failed_task_execution produced unexpected row counts: {counts:?}"
            )));
        }

        let transition = self.get_transition_evidence(&transition_id).await?;
        let state = self
            .client
            .query_as_optional::<FailedTaskQuarantineStateRow, _>(
                r#"
                SELECT task.status AS task_status, task.continuation_state,
                    task.timeout_at, run.status AS run_status,
                    step.status AS step_status, step.error_json,
                    EXISTS (
                        SELECT 1 FROM leases
                        WHERE task_id = $1 AND attempt_id = $2
                            AND fencing_token = $3 AND worker_id = $4
                    ) AS exact_lease_present
                FROM task_queue task
                INNER JOIN workflow_runs run ON run.run_id = task.run_id
                LEFT JOIN step_instances step
                    ON step.run_id = task.run_id AND step.step_id = task.step_id
                WHERE task.task_id = $1
                LIMIT 1
                "#,
                params!(
                    lease.task_id.clone(),
                    lease.attempt_id.clone(),
                    lease.fencing_token as i64,
                    lease.worker_id.clone(),
                ),
            )
            .await?;
        let transition_matches = matches!(
            transition.as_ref(),
            Some(transition)
                if transition.transition_type == "task_execution_quarantined"
                    && transition.payload_json.as_deref() == Some(transition_payload.as_str())
        );
        let Some(state) = state else {
            return Ok(TaskExecutionQuarantineResult::Stale);
        };
        if transition_matches
            && state.task_status == TASK_STATUS_FAILED
            && state.continuation_state.as_deref() == Some(marker.as_str())
            && state.step_status.as_deref() == Some(TASK_STATUS_FAILED)
            && state.error_json.as_deref() == Some(failure_json.as_str())
            && state.exact_lease_present == 0
        {
            return Ok(TaskExecutionQuarantineResult::AlreadyApplied);
        }
        if state.run_status != RUN_STATUS_RUNNING {
            return Ok(TaskExecutionQuarantineResult::RunNotRunning {
                status: state.run_status,
            });
        }
        if transition.is_some() {
            return Ok(TaskExecutionQuarantineResult::Conflict {
                detail: "stable quarantine transition has incompatible evidence".to_owned(),
            });
        }
        if state.timeout_at.is_some_and(|timeout_at| timeout_at <= now) {
            return Ok(TaskExecutionQuarantineResult::Expired);
        }
        Ok(TaskExecutionQuarantineResult::Stale)
    }

    /// Durably quarantines a completed task continuation whose downstream
    /// execution provenance cannot be reconstructed without live defaults.
    #[allow(dead_code)] // Private recovery seam.
    pub(crate) async fn quarantine_task_continuation(
        &self,
        task_id: &str,
        stable_code: &str,
        now: i64,
    ) -> Result<ContinuationQuarantineResult, PersistenceError> {
        validate_quarantine_code(stable_code)?;
        let marker = format!("{CONTINUATION_QUARANTINE_PREFIX}{stable_code}");
        let transition_id = stable_outcome_id("task-continuation-quarantined", &[task_id]);
        let payload = serde_json::to_string(&serde_json::json!({
            "task_id": task_id,
            "code": stable_code,
        }))?;
        let results = self
            .client
            .txn([
                (
                    r#"
                    INSERT OR IGNORE INTO transition_log (
                        transition_id, run_id, transition_type, payload_json, created_at
                    )
                    SELECT $1, task.run_id, $2, $3, $4
                    FROM task_queue task
                    INNER JOIN workflow_runs run ON run.run_id = task.run_id
                    WHERE task.task_id = $5
                        AND task.status = $6
                        AND task.continuation_state = $7
                        AND run.status = $8
                    "#,
                    params!(
                        transition_id.clone(),
                        "task_continuation_quarantined".to_owned(),
                        payload.clone(),
                        now,
                        task_id.to_owned(),
                        TASK_STATUS_COMPLETED,
                        CONTINUATION_STATE_PENDING,
                        RUN_STATUS_RUNNING,
                    ),
                ),
                (
                    r#"
                    UPDATE task_queue
                    SET continuation_state = $1, updated_at = $2
                    WHERE task_id = $3
                        AND status = $4
                        AND continuation_state = $5
                        AND EXISTS (
                            SELECT 1 FROM workflow_runs
                            WHERE run_id = task_queue.run_id AND status = $6
                        )
                        AND EXISTS (
                            SELECT 1 FROM transition_log
                            WHERE transition_id = $7 AND transition_type = $8
                                AND payload_json = $9
                        )
                    "#,
                    params!(
                        marker.clone(),
                        now,
                        task_id.to_owned(),
                        TASK_STATUS_COMPLETED,
                        CONTINUATION_STATE_PENDING,
                        RUN_STATUS_RUNNING,
                        transition_id.clone(),
                        "task_continuation_quarantined".to_owned(),
                        payload.clone(),
                    ),
                ),
            ])
            .await?;
        let counts = txn_counts(results)?;
        if matches!(counts.as_slice(), [1, 1] | [0, 1]) {
            return Ok(ContinuationQuarantineResult::Applied);
        }
        self.classify_continuation_quarantine(
            ContinuationQuarantineSource::Task,
            task_id,
            &marker,
            &transition_id,
            "task_continuation_quarantined",
            &payload,
            &counts,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)] // Shared typed classifier for durable continuation sources.
    async fn classify_continuation_quarantine(
        &self,
        source: ContinuationQuarantineSource,
        source_id: &str,
        marker: &str,
        transition_id: &str,
        transition_type: &str,
        payload: &str,
        counts: &[usize],
    ) -> Result<ContinuationQuarantineResult, PersistenceError> {
        if counts != [0, 0] {
            return Err(PersistenceError::Invariant(format!(
                "continuation quarantine produced unexpected row counts: {counts:?}"
            )));
        }
        let state = match source {
            ContinuationQuarantineSource::Task => {
                self.client
                    .query_as_optional::<ContinuationQuarantineStateRow, _>(
                        r#"
                        SELECT task.status, task.continuation_state, run.status AS run_status
                        FROM task_queue task
                        INNER JOIN workflow_runs run ON run.run_id = task.run_id
                        WHERE task.task_id = $1
                        "#,
                        params!(source_id.to_owned()),
                    )
                    .await?
            }
            ContinuationQuarantineSource::Timer => {
                self.client
                    .query_as_optional::<ContinuationQuarantineStateRow, _>(
                        r#"
                        SELECT timer.status, timer.effect_state AS continuation_state,
                            run.status AS run_status
                        FROM timers timer
                        INNER JOIN workflow_runs run ON run.run_id = timer.run_id
                        WHERE timer.timer_id = $1
                        "#,
                        params!(source_id.to_owned()),
                    )
                    .await?
            }
        };
        let transition = self.get_transition_evidence(transition_id).await?;
        let transition_matches = matches!(
            transition.as_ref(),
            Some(transition)
                if transition.transition_type == transition_type
                    && transition.payload_json.as_deref() == Some(payload)
        );
        let Some(state) = state else {
            return Ok(ContinuationQuarantineResult::Stale(
                ContinuationQuarantineStaleReason::SourceMissing,
            ));
        };
        if state.continuation_state.as_deref() == Some(marker) && transition_matches {
            return Ok(ContinuationQuarantineResult::AlreadyApplied);
        }
        if state
            .continuation_state
            .as_deref()
            .is_some_and(|value| value.starts_with(CONTINUATION_QUARANTINE_PREFIX))
            || transition.is_some()
        {
            return Ok(ContinuationQuarantineResult::Conflict {
                existing_state: state.continuation_state,
            });
        }
        if state.run_status != RUN_STATUS_RUNNING {
            return Ok(ContinuationQuarantineResult::Stale(
                ContinuationQuarantineStaleReason::RunNotRunning {
                    status: state.run_status,
                },
            ));
        }
        Ok(ContinuationQuarantineResult::Stale(
            ContinuationQuarantineStaleReason::SourceNotRunnable {
                status: state.status,
                continuation_state: state.continuation_state,
            },
        ))
    }

    pub(crate) async fn mark_completed_task_continuation_applied(
        &self,
        task_id: &str,
        now: i64,
    ) -> Result<MutationApplyResult, PersistenceError> {
        self.mark_completed_task_continuation(task_id, CONTINUATION_STATE_APPLIED, true, now)
            .await
    }

    pub(crate) async fn mark_completed_task_continuation_suppressed(
        &self,
        task_id: &str,
        now: i64,
    ) -> Result<MutationApplyResult, PersistenceError> {
        self.mark_completed_task_continuation(task_id, CONTINUATION_STATE_SUPPRESSED, false, now)
            .await
    }

    async fn mark_completed_task_continuation(
        &self,
        task_id: &str,
        target_state: &str,
        require_running_run: bool,
        now: i64,
    ) -> Result<MutationApplyResult, PersistenceError> {
        let updated = self
            .client
            .execute(
                r#"
                UPDATE task_queue
                SET continuation_state = $1, updated_at = $2
                WHERE task_id = $3
                    AND status = $4
                    AND continuation_state = $5
                    AND (
                        $6 = 0
                        OR EXISTS (
                            SELECT 1 FROM workflow_runs
                            WHERE run_id = task_queue.run_id AND status = $7
                        )
                    )
                "#,
                params!(
                    target_state.to_owned(),
                    now,
                    task_id.to_owned(),
                    TASK_STATUS_COMPLETED,
                    CONTINUATION_STATE_PENDING,
                    i64::from(require_running_run),
                    RUN_STATUS_RUNNING,
                ),
            )
            .await?;

        match updated {
            1 => Ok(MutationApplyResult::Applied),
            0 => {
                let state = self
                    .client
                    .query_as_optional::<ContinuationStateRow, _>(
                        "SELECT continuation_state FROM task_queue WHERE task_id = $1",
                        params!(task_id.to_owned()),
                    )
                    .await?;
                if state.and_then(|row| row.continuation_state).as_deref() == Some(target_state) {
                    Ok(MutationApplyResult::AlreadyApplied)
                } else {
                    Ok(MutationApplyResult::Stale)
                }
            }
            count => Err(PersistenceError::Invariant(format!(
                "mark_completed_task_continuation updated {count} rows for task '{task_id}'"
            ))),
        }
    }

    #[cfg(test)]
    #[allow(dead_code)]
    pub(crate) async fn complete_task(
        &self,
        lease: &TaskMutationContext,
        result: &TaskOutput,
        updated_at: i64,
    ) -> Result<MutationApplyResult, PersistenceError> {
        let Some(row) = self.get_task_row(&lease.task_id).await? else {
            return Ok(MutationApplyResult::Stale);
        };

        if !lease_matches(&row, lease) {
            return Ok(MutationApplyResult::Stale);
        }

        if row.status == TASK_STATUS_COMPLETED {
            return Ok(MutationApplyResult::AlreadyApplied);
        }

        let results = self
            .client
            .txn([
                (
                    r#"
                    UPDATE task_queue
                    SET status = $1, updated_at = $2
                    WHERE task_id = $3
                        AND status = $4
                        AND active_attempt_id = $5
                        AND active_fencing_token = $6
                        AND leased_by_worker_id = $7
                    "#,
                    params!(
                        TASK_STATUS_COMPLETED,
                        updated_at,
                        lease.task_id.clone(),
                        TASK_STATUS_LEASED,
                        lease.attempt_id.clone(),
                        lease.fencing_token as i64,
                        lease.worker_id.clone()
                    ),
                ),
                (
                    r#"
                    DELETE FROM leases
                    WHERE task_id = $1 AND attempt_id = $2 AND worker_id = $3 AND fencing_token = $4
                    "#,
                    params!(
                        lease.task_id.clone(),
                        lease.attempt_id.clone(),
                        lease.worker_id.clone(),
                        lease.fencing_token as i64
                    ),
                ),
                (
                    r#"
                    UPDATE step_instances
                    SET status = $1,
                        attempt_count = $2,
                        output_json = $3,
                        error_json = NULL,
                        updated_at = $4
                    WHERE run_id = $5
                        AND step_id = $6
                        AND EXISTS (
                            SELECT 1
                            FROM task_queue
                            WHERE task_id = $7
                                AND status = $1
                                AND active_attempt_id = $8
                                AND active_fencing_token = $9
                        )
                    "#,
                    params!(
                        TASK_STATUS_COMPLETED,
                        row.attempt_sequence,
                        serde_json::to_string(&result.payload)?,
                        updated_at,
                        row.run_id.clone(),
                        row.step_id.clone(),
                        lease.task_id.clone(),
                        lease.attempt_id.clone(),
                        lease.fencing_token as i64
                    ),
                ),
            ])
            .await?;

        let counts = txn_counts(results)?;
        match counts.as_slice() {
            [1, 1, 1] => Ok(MutationApplyResult::Applied),
            [0, 0, 0] => Ok(MutationApplyResult::Stale),
            _ => Err(PersistenceError::Invariant(format!(
                "complete_task produced unexpected row counts: {counts:?}"
            ))),
        }
    }

    #[cfg(test)]
    #[allow(dead_code)]
    pub(crate) async fn fail_task(
        &self,
        lease: &TaskMutationContext,
        failure: &Value,
        updated_at: i64,
    ) -> Result<MutationApplyResult, PersistenceError> {
        let Some(row) = self.get_task_row(&lease.task_id).await? else {
            return Ok(MutationApplyResult::Stale);
        };

        if !lease_matches(&row, lease) {
            return Ok(MutationApplyResult::Stale);
        }

        if row.status == TASK_STATUS_FAILED {
            return Ok(MutationApplyResult::AlreadyApplied);
        }

        let results = self
            .client
            .txn([
                (
                    r#"
                    UPDATE task_queue
                    SET status = $1, updated_at = $2
                    WHERE task_id = $3
                        AND status = $4
                        AND active_attempt_id = $5
                        AND active_fencing_token = $6
                        AND leased_by_worker_id = $7
                    "#,
                    params!(
                        TASK_STATUS_FAILED,
                        updated_at,
                        lease.task_id.clone(),
                        TASK_STATUS_LEASED,
                        lease.attempt_id.clone(),
                        lease.fencing_token as i64,
                        lease.worker_id.clone()
                    ),
                ),
                (
                    r#"
                    DELETE FROM leases
                    WHERE task_id = $1 AND attempt_id = $2 AND worker_id = $3 AND fencing_token = $4
                    "#,
                    params!(
                        lease.task_id.clone(),
                        lease.attempt_id.clone(),
                        lease.worker_id.clone(),
                        lease.fencing_token as i64
                    ),
                ),
                (
                    r#"
                    UPDATE step_instances
                    SET status = $1,
                        attempt_count = $2,
                        output_json = NULL,
                        error_json = $3,
                        updated_at = $4
                    WHERE run_id = $5
                        AND step_id = $6
                        AND EXISTS (
                            SELECT 1
                            FROM task_queue
                            WHERE task_id = $7
                                AND status = $1
                                AND active_attempt_id = $8
                                AND active_fencing_token = $9
                        )
                    "#,
                    params!(
                        TASK_STATUS_FAILED,
                        row.attempt_sequence,
                        serde_json::to_string(failure)?,
                        updated_at,
                        row.run_id.clone(),
                        row.step_id.clone(),
                        lease.task_id.clone(),
                        lease.attempt_id.clone(),
                        lease.fencing_token as i64
                    ),
                ),
            ])
            .await?;

        let counts = txn_counts(results)?;
        match counts.as_slice() {
            [1, 1, 1] => Ok(MutationApplyResult::Applied),
            [0, 0, 0] => Ok(MutationApplyResult::Stale),
            _ => Err(PersistenceError::Invariant(format!(
                "fail_task produced unexpected row counts: {counts:?}"
            ))),
        }
    }

    pub(crate) async fn get_task_snapshot(
        &self,
        task_id: &str,
    ) -> Result<Option<TaskSnapshot>, PersistenceError> {
        self.get_task_row(task_id)
            .await?
            .map(TryInto::try_into)
            .transpose()
    }

    pub(crate) async fn requeue_expired_leases(
        &self,
        now: i64,
    ) -> Result<Vec<TaskSnapshot>, PersistenceError> {
        let rows = self
            .client
            .query_as::<TaskQueueRow, _>(
                r#"
                SELECT
                    t.task_id,
                    t.run_id,
                    t.workflow_id,
                    t.workflow_version,
                    t.step_id,
                    t.step_kind,
                    t.handler_id,
                    t.capability,
                    t.queue_name,
                    t.input_json,
                    t.timeout_at,
                    t.status,
                    t.attempt_sequence,
                    t.active_attempt_id,
                    t.active_fencing_token,
                    t.leased_by_worker_id
                FROM task_queue t INDEXED BY idx_task_queue_lease_recovery
                LEFT JOIN leases l
                    ON l.task_id = t.task_id
                    AND l.attempt_id = t.active_attempt_id
                    AND l.fencing_token = t.active_fencing_token
                WHERE t.status = $1
                    AND t.continuation_state IS NULL
                    AND (l.task_id IS NULL OR l.expires_at <= $2)
                ORDER BY t.updated_at ASC, t.task_id ASC
                LIMIT $3
                "#,
                params!(TASK_STATUS_LEASED, now, RECOVERY_SCAN_BATCH_SIZE),
            )
            .await?;

        let mut requeued = Vec::new();
        for row in rows {
            if checked_stored_workflow_version("task", &row.task_id, row.workflow_version).is_err()
            {
                self.quarantine_task_execution(&row.task_id, "invalid_workflow_version", now)
                    .await?;
                continue;
            }
            let results = self
                .client
                .txn([
                    (
                        r#"
                        UPDATE task_queue
                        SET status = $1,
                            leased_by_worker_id = NULL,
                            updated_at = $2
                        WHERE task_id = $3
                            AND status = $4
                            AND active_attempt_id = $5
                            AND active_fencing_token = $6
                            AND leased_by_worker_id = $7
                            AND continuation_state IS NULL
                            AND (
                                NOT EXISTS (
                                    SELECT 1 FROM leases WHERE task_id = $3
                                )
                                OR EXISTS (
                                    SELECT 1 FROM leases
                                    WHERE task_id = $3 AND attempt_id = $5
                                        AND fencing_token = $6 AND worker_id = $7
                                        AND expires_at <= $2
                                )
                            )
                        "#,
                        params!(
                            TASK_STATUS_QUEUED,
                            now,
                            row.task_id.clone(),
                            TASK_STATUS_LEASED,
                            row.active_attempt_id.clone(),
                            row.active_fencing_token,
                            row.leased_by_worker_id.clone(),
                        ),
                    ),
                    (
                        r#"
                    DELETE FROM leases
                    WHERE task_id = $1
                        AND attempt_id = $2
                        AND fencing_token = $3
                        AND changes() = 1
                    "#,
                        params!(
                            row.task_id.clone(),
                            row.active_attempt_id.clone(),
                            row.active_fencing_token
                        ),
                    ),
                ])
                .await?;

            let counts = txn_counts(results)?;
            match counts.as_slice() {
                [1, deleted] if *deleted <= 1 => {
                    let mut row = row;
                    row.status = TASK_STATUS_QUEUED.to_owned();
                    row.leased_by_worker_id = None;
                    requeued.push(row.try_into()?);
                }
                [0, _] => {}
                _ => {
                    return Err(PersistenceError::Invariant(format!(
                        "requeue_expired_leases produced unexpected row counts: {counts:?}"
                    )))
                }
            }
        }

        Ok(requeued)
    }

    pub(crate) async fn list_timed_out_tasks(
        &self,
        now: i64,
    ) -> Result<Vec<TaskSnapshot>, PersistenceError> {
        let rows = self
            .client
            .query_as::<TaskQueueRow, _>(
                r#"
                SELECT
                    t.task_id,
                    t.run_id,
                    t.workflow_id,
                    t.workflow_version,
                    t.step_id,
                    t.step_kind,
                    t.handler_id,
                    t.capability,
                    t.queue_name,
                    t.input_json,
                    t.timeout_at,
                    t.status,
                    t.attempt_sequence,
                    t.active_attempt_id,
                    t.active_fencing_token,
                    t.leased_by_worker_id
                FROM task_queue t INDEXED BY idx_task_queue_timeout_recovery
                INNER JOIN workflow_runs r ON r.run_id = t.run_id
                WHERE r.status = 'running'
                    AND t.continuation_state IS NULL
                    AND t.timeout_at IS NOT NULL
                    AND t.timeout_at <= $1
                    AND t.status IN ($2, $3, $4)
                ORDER BY t.timeout_at ASC, t.created_at ASC, t.task_id ASC
                LIMIT $5
                "#,
                params!(
                    now,
                    TASK_STATUS_QUEUED,
                    TASK_STATUS_LEASED,
                    TASK_STATUS_RETRY_PENDING,
                    RECOVERY_SCAN_BATCH_SIZE,
                ),
            )
            .await?;

        let mut tasks = Vec::with_capacity(rows.len());
        for row in rows {
            if checked_stored_workflow_version("task", &row.task_id, row.workflow_version).is_err()
            {
                self.quarantine_task_execution(&row.task_id, "invalid_workflow_version", now)
                    .await?;
                continue;
            }
            tasks.push(row.try_into()?);
        }
        Ok(tasks)
    }

    async fn get_task_row(&self, task_id: &str) -> Result<Option<TaskQueueRow>, PersistenceError> {
        self.client
            .query_as_optional::<TaskQueueRow, _>(
                r#"
                SELECT
                    task_id,
                    run_id,
                    workflow_id,
                    workflow_version,
                    step_id,
                    step_kind,
                    handler_id,
                    capability,
                    queue_name,
                    input_json,
                    timeout_at,
                    status,
                    attempt_sequence,
                    active_attempt_id,
                    active_fencing_token,
                    leased_by_worker_id
                FROM task_queue
                WHERE task_id = $1
                "#,
                params!(task_id.to_owned()),
            )
            .await
            .map_err(Into::into)
    }

    async fn classify_unapplied_task_outcome(
        &self,
        task_id: &str,
        transition_id: &str,
        now: i64,
    ) -> Result<TaskOutcomeApplyResult, PersistenceError> {
        if self.task_outcome_transition_exists(transition_id).await? {
            return Ok(TaskOutcomeApplyResult::AlreadyApplied);
        }
        let claimable = self
            .client
            .query_as::<TaskStatusRow, _>(
                r#"
                SELECT t.status
                FROM task_queue t
                INNER JOIN workflow_runs r ON r.run_id = t.run_id
                WHERE t.task_id = $1
                    AND r.status = $2
                    AND t.status IN ($3, $4, $5)
                    AND t.timeout_at IS NOT NULL
                    AND t.timeout_at <= $6
                "#,
                params!(
                    task_id.to_owned(),
                    RUN_STATUS_RUNNING,
                    TASK_STATUS_QUEUED,
                    TASK_STATUS_LEASED,
                    TASK_STATUS_RETRY_PENDING,
                    now,
                ),
            )
            .await?;
        if claimable.iter().any(|row| !row.status.is_empty()) {
            Ok(TaskOutcomeApplyResult::Expired)
        } else {
            Ok(TaskOutcomeApplyResult::Stale)
        }
    }

    async fn task_outcome_transition_exists(
        &self,
        transition_id: &str,
    ) -> Result<bool, PersistenceError> {
        let row = self
            .client
            .query_as::<TaskStatusRow, _>(
                r#"
                SELECT transition_type AS status
                FROM transition_log
                WHERE transition_id = $1
                "#,
                params!(transition_id.to_owned()),
            )
            .await?;
        Ok(row.iter().any(|row| !row.status.is_empty()))
    }

    async fn get_transition_evidence(
        &self,
        transition_id: &str,
    ) -> Result<Option<TransitionEvidenceRow>, PersistenceError> {
        self.client
            .query_as_optional::<TransitionEvidenceRow, _>(
                r#"
                SELECT transition_type, payload_json
                FROM transition_log
                WHERE transition_id = $1
                LIMIT 1
                "#,
                params!(transition_id.to_owned()),
            )
            .await
            .map_err(Into::into)
    }

    #[cfg(test)]
    pub(crate) async fn run_has_open_work(&self, run_id: &str) -> Result<bool, PersistenceError> {
        let open_work = self
            .client
            .query_as_optional::<TaskStatusRow, _>(
                r#"
                SELECT status
                FROM task_queue
                WHERE run_id = $1
                    AND (
                        status IN ($2, $3, $4)
                        OR continuation_state = $5
                        OR continuation_state LIKE $6
                    )
                UNION ALL
                SELECT timer.status
                FROM timers timer
                WHERE timer.run_id = $1
                    AND (
                        timer.status = $7
                        OR (
                            timer.status = $8
                            AND (
                                timer.effect_state IS NULL
                                OR timer.effect_state = $9
                                OR timer.effect_state LIKE $10
                            )
                        )
                    )
                UNION ALL
                SELECT lease.lease_id AS status
                FROM leases lease
                INNER JOIN task_queue task ON task.task_id = lease.task_id
                WHERE task.run_id = $1
                LIMIT 1
                "#,
                params!(
                    run_id.to_owned(),
                    TASK_STATUS_QUEUED,
                    TASK_STATUS_LEASED,
                    TASK_STATUS_RETRY_PENDING,
                    CONTINUATION_STATE_PENDING,
                    CONTINUATION_QUARANTINE_PATTERN,
                    TIMER_STATUS_PENDING,
                    TIMER_STATUS_FIRED,
                    TIMER_EFFECT_STATE_PENDING,
                    CONTINUATION_QUARANTINE_PATTERN,
                ),
            )
            .await?;

        Ok(open_work.is_some_and(|row| !row.status.is_empty()))
    }

    /// Inserts a new inbox record or atomically converges on the durable record
    /// that already owns the supplied event id or dedupe key.
    ///
    /// An ignored insert is accepted only when every colliding durable identity
    /// has the same event type and payload. Conflicting evidence is never
    /// overwritten and is returned as a named persistence error.
    pub(crate) async fn insert_or_load_event_received(
        &self,
        event_id: &str,
        event_type: &str,
        payload: &Value,
        dedupe_key: Option<&str>,
        now: i64,
    ) -> Result<(EventInboxRecord, bool), PersistenceError> {
        let payload_json = serde_json::to_string(payload)?;
        let transition_id = stable_outcome_id("event-received", &[event_id]);
        let transition_payload = serde_json::to_string(&serde_json::json!({
            "event_id": event_id,
            "event_type": event_type,
        }))?;
        let counts = txn_counts(
            self.client
                .txn([
                    (
                        r#"
                        INSERT OR IGNORE INTO event_inbox (
                            event_id, event_type, processing_state, payload_json,
                            dedupe_key, created_at, updated_at, applied_at
                        )
                        VALUES ($1, $2, $3, $4, $5, $6, $6, NULL)
                        "#,
                        params!(
                            event_id.to_owned(),
                            event_type.to_owned(),
                            "received".to_owned(),
                            payload_json.clone(),
                            dedupe_key.map(ToOwned::to_owned),
                            now,
                        ),
                    ),
                    (
                        r#"
                        INSERT OR IGNORE INTO transition_log (
                            transition_id, run_id, transition_type, payload_json, created_at
                        )
                        SELECT $1, $2, $3, $4, $5
                        FROM event_inbox event
                        WHERE event.event_id = $6
                            AND event.event_type = $7
                            AND event.payload_json = $8
                            AND (
                                ($9 IS NULL AND event.dedupe_key IS NULL)
                                OR event.dedupe_key = $9
                            )
                        "#,
                        params!(
                            transition_id,
                            "event-inbox".to_owned(),
                            "event_received".to_owned(),
                            transition_payload,
                            now,
                            event_id.to_owned(),
                            event_type.to_owned(),
                            payload_json,
                            dedupe_key.map(ToOwned::to_owned),
                        ),
                    ),
                ])
                .await?,
        )?;
        let inserted = counts.first().copied().ok_or_else(|| {
            PersistenceError::Invariant(format!(
                "event inbox transaction returned no row counts for event '{event_id}'"
            ))
        })?;
        if inserted == 1 {
            return Ok((
                EventInboxRecord {
                    event_id: event_id.to_owned(),
                    event_type: event_type.to_owned(),
                    processing_state: "received".to_owned(),
                    payload: payload.clone(),
                    dedupe_key: dedupe_key.map(ToOwned::to_owned),
                },
                true,
            ));
        }
        if inserted != 0 {
            return Err(PersistenceError::Invariant(format!(
                "event inbox insert affected {inserted} rows for event '{event_id}'"
            )));
        }

        let by_event_id = self.get_event_by_id(event_id).await?;
        let by_dedupe_key = match dedupe_key {
            Some(dedupe_key) => self.find_event_by_dedupe_key(dedupe_key).await?,
            None => None,
        };
        let existing = match (by_event_id, by_dedupe_key) {
            (Some(by_id), Some(by_key)) if by_id.event_id != by_key.event_id => {
                return Err(event_inbox_conflict(event_id, dedupe_key, &by_key.event_id));
            }
            (Some(by_id), _) => by_id,
            (None, Some(by_key)) => by_key,
            (None, None) => {
                return Err(PersistenceError::Invariant(format!(
                    "event inbox ignored insert for event '{event_id}' without durable collision evidence"
                )));
            }
        };

        let identity_matches = existing.event_type == event_type
            && existing.payload == *payload
            && (existing.event_id != event_id || existing.dedupe_key.as_deref() == dedupe_key);
        if !identity_matches {
            return Err(event_inbox_conflict(
                event_id,
                dedupe_key,
                &existing.event_id,
            ));
        }

        Ok((existing, false))
    }

    async fn find_event_by_dedupe_key(
        &self,
        dedupe_key: &str,
    ) -> Result<Option<EventInboxRecord>, PersistenceError> {
        let row = self
            .client
            .query_as_optional::<EventInboxRow, _>(
                r#"
                SELECT event_id, event_type, processing_state, payload_json, dedupe_key
                FROM event_inbox
                WHERE dedupe_key = $1
                LIMIT 1
                "#,
                params!(dedupe_key.to_owned()),
            )
            .await?;

        row.map(TryInto::try_into).transpose()
    }

    #[allow(dead_code)] // Shared by the private interrupted-resume repair seam.
    async fn get_event_by_id(
        &self,
        event_id: &str,
    ) -> Result<Option<EventInboxRecord>, PersistenceError> {
        let row = self
            .client
            .query_as_optional::<EventInboxRow, _>(
                r#"
                SELECT event_id, event_type, processing_state, payload_json, dedupe_key
                FROM event_inbox
                WHERE event_id = $1
                LIMIT 1
                "#,
                params!(event_id.to_owned()),
            )
            .await?;
        row.map(TryInto::try_into).transpose()
    }

    pub(crate) async fn list_received_events(
        &self,
    ) -> Result<Vec<EventInboxRecord>, PersistenceError> {
        let rows = self
            .client
            .query_as::<EventInboxRow, _>(
                r#"
                SELECT
                    event_id,
                    event_type,
                    processing_state,
                    payload_json,
                    dedupe_key
                FROM event_inbox INDEXED BY idx_event_inbox_recovery
                WHERE processing_state = $1
                ORDER BY event_inbox.created_at ASC, event_inbox.event_id ASC
                LIMIT $2
                "#,
                params!("received".to_owned(), RECOVERY_SCAN_BATCH_SIZE),
            )
            .await?;

        rows.into_iter().map(TryInto::try_into).collect()
    }

    /// Repairs the audit side of receipt-before-apply crashes with a stable,
    /// collision-checked transition id.
    pub(crate) async fn ensure_event_received_transition(
        &self,
        event_id: &str,
        event_type: &str,
        now: i64,
    ) -> Result<(), PersistenceError> {
        let transition_id = stable_outcome_id("event-received", &[event_id]);
        let payload = serde_json::to_string(&serde_json::json!({
            "event_id": event_id,
            "event_type": event_type,
        }))?;
        self.client
            .execute(
                r#"
                INSERT OR IGNORE INTO transition_log (
                    transition_id, run_id, transition_type, payload_json, created_at
                ) VALUES ($1, $2, $3, $4, $5)
                "#,
                params!(
                    transition_id.clone(),
                    "event-inbox".to_owned(),
                    "event_received".to_owned(),
                    payload.clone(),
                    now,
                ),
            )
            .await?;
        let evidence = self
            .client
            .query_as_optional::<TransitionEvidenceRow, _>(
                r#"
                SELECT transition_type, payload_json
                FROM transition_log
                WHERE transition_id = $1
                LIMIT 1
                "#,
                params!(transition_id),
            )
            .await?
            .ok_or_else(|| {
                PersistenceError::Invariant(format!(
                    "event receipt transition for '{event_id}' disappeared after insert"
                ))
            })?;
        if evidence.transition_type != "event_received"
            || evidence.payload_json.as_deref() != Some(payload.as_str())
        {
            return Err(PersistenceError::Invariant(format!(
                "event receipt transition collision for event '{event_id}'"
            )));
        }
        Ok(())
    }

    pub(crate) async fn mark_event_applied(
        &self,
        event_id: &str,
        now: i64,
    ) -> Result<(), PersistenceError> {
        self.client
            .execute(
                r#"
                UPDATE event_inbox
                SET processing_state = $1, updated_at = $2, applied_at = $2
                WHERE event_id = $3 AND processing_state = $4
                "#,
                params!(
                    "applied".to_owned(),
                    now,
                    event_id.to_owned(),
                    "received".to_owned(),
                ),
            )
            .await?;
        Ok(())
    }

    pub(crate) async fn insert_transition(
        &self,
        record: &TransitionRecord,
    ) -> Result<(), PersistenceError> {
        self.client
            .execute(
                r#"
                INSERT INTO transition_log (
                    transition_id,
                    run_id,
                    transition_type,
                    payload_json,
                    created_at
                )
                VALUES ($1, $2, $3, $4, $5)
                "#,
                params!(
                    record.transition_id.clone(),
                    record.run_id.clone(),
                    record.transition_type.clone(),
                    json_text(record.payload.as_ref())?,
                    record.created_at
                ),
            )
            .await?;
        Ok(())
    }

    pub(crate) async fn insert_transition_if_absent_for_running_run(
        &self,
        record: &TransitionRecord,
    ) -> Result<bool, PersistenceError> {
        let inserted = self
            .client
            .execute(
                r#"
                INSERT OR IGNORE INTO transition_log (
                    transition_id,
                    run_id,
                    transition_type,
                    payload_json,
                    created_at
                )
                SELECT $1, $2, $3, $4, $5
                WHERE EXISTS (
                    SELECT 1 FROM workflow_runs
                    WHERE run_id = $2 AND status = $6
                )
                "#,
                params!(
                    record.transition_id.clone(),
                    record.run_id.clone(),
                    record.transition_type.clone(),
                    json_text(record.payload.as_ref())?,
                    record.created_at,
                    RUN_STATUS_RUNNING,
                ),
            )
            .await?;
        Ok(inserted == 1)
    }

    pub async fn list_transitions(
        &self,
        run_id: &str,
    ) -> Result<Vec<TransitionRecord>, PersistenceError> {
        let rows = self
            .client
            .query_as::<TransitionRow, _>(
                r#"
                SELECT
                    transition_id,
                    run_id,
                    transition_type,
                    payload_json,
                    created_at
                FROM transition_log
                WHERE run_id = $1
                ORDER BY transition_log.created_at ASC
                "#,
                params!(run_id.to_owned()),
            )
            .await?;

        rows.into_iter().map(TryInto::try_into).collect()
    }

    pub(crate) async fn list_transitions_page(
        &self,
        run_id: &str,
        cursor: Option<&(i64, String)>,
        limit: u32,
    ) -> Result<Vec<TransitionRecord>, PersistenceError> {
        let after_created_at = cursor.map(|(created_at, _)| *created_at);
        let after_transition_id = cursor.map(|(_, stable_id)| stable_id.clone());
        let rows = self
            .client
            .query_as::<TransitionRow, _>(
                r#"
                SELECT
                    transition_id,
                    run_id,
                    transition_type,
                    payload_json,
                    created_at
                FROM transition_log INDEXED BY idx_transition_log_run_created
                WHERE run_id = $1
                    AND (
                        $2 IS NULL
                        OR created_at > $2
                        OR (created_at = $2 AND transition_id > $3)
                    )
                ORDER BY created_at ASC, transition_id ASC
                LIMIT $4
                "#,
                params!(
                    run_id.to_owned(),
                    after_created_at,
                    after_transition_id,
                    i64::from(limit),
                ),
            )
            .await?;

        rows.into_iter().map(TryInto::try_into).collect()
    }
}

#[derive(Debug, Deserialize)]
struct WorkflowDefinitionRow {
    workflow_id: String,
    workflow_version: i64,
    definition_json: String,
    content_hash: String,
    last_validated_content_hash: Option<String>,
    #[allow(dead_code)] // Read by the explicit stored-definition seams before call-site switch.
    execution_metadata_json: Option<String>,
    created_at: i64,
    published_at: Option<i64>,
}

fn inspect_stored_definition_row(row: WorkflowDefinitionRow) -> StoredDefinitionInspection {
    let decoded = decode_and_validate_stored_definition_row(&row);
    let classification = decoded.as_ref().ok().map(|value| value.classification());
    let new_run_eligible = decoded
        .as_ref()
        .is_ok_and(DecodedStoredDefinition::is_new_run_eligible);
    let (execution_compatibility, decode_diagnostic) = match decoded {
        Ok(_) => (StoredDefinitionExecutionCompatibility::Compatible, None),
        Err(error) => (
            StoredDefinitionExecutionCompatibility::Incompatible,
            Some(error.diagnostic()),
        ),
    };
    StoredDefinitionInspection {
        workflow_id: row.workflow_id,
        workflow_version: row.workflow_version,
        definition_json: row.definition_json,
        execution_metadata_json: row.execution_metadata_json,
        content_hash: row.content_hash,
        last_validated_content_hash: row.last_validated_content_hash,
        created_at: row.created_at,
        published_at: row.published_at,
        classification,
        execution_compatibility,
        new_run_eligible,
        decode_diagnostic,
    }
}

#[allow(dead_code)]
fn decode_stored_definition_row(
    row: WorkflowDefinitionRow,
) -> Result<DecodedStoredDefinition, PersistenceError> {
    decode_and_validate_stored_definition_row(&row).map_err(|error| {
        let diagnostic = error.diagnostic();
        PersistenceError::StoredDefinitionDecode {
            workflow_id: row.workflow_id.clone(),
            workflow_version: row.workflow_version,
            diagnostic_code: diagnostic.code.as_str().to_owned(),
            message: diagnostic.message,
        }
    })
}

fn decode_and_validate_stored_definition_row(
    row: &WorkflowDefinitionRow,
) -> Result<DecodedStoredDefinition, StoredDefinitionDecodeError> {
    let decoded =
        decode_stored_definition(&row.definition_json, row.execution_metadata_json.as_deref())?;
    if decoded.executable().workflow_id != row.workflow_id {
        return Err(StoredDefinitionDecodeError::WorkflowIdMismatch {
            row_workflow_id: row.workflow_id.clone(),
            definition_workflow_id: decoded.executable().workflow_id.clone(),
        });
    }
    if i64::from(decoded.executable().workflow_version) != row.workflow_version {
        return Err(StoredDefinitionDecodeError::WorkflowVersionMismatch {
            row_workflow_version: row.workflow_version,
            definition_workflow_version: decoded.executable().workflow_version,
        });
    }
    Ok(decoded)
}

#[derive(Debug)]
pub struct DraftDefinitionRecord {
    pub definition: FlowDefinition,
    pub content_hash: String,
    pub last_validated_content_hash: Option<String>,
    pub created_at: i64,
}

impl TryFrom<WorkflowDefinitionRow> for DraftDefinitionRecord {
    type Error = PersistenceError;

    fn try_from(row: WorkflowDefinitionRow) -> Result<Self, Self::Error> {
        Ok(Self {
            definition: serde_json::from_str(&row.definition_json)?,
            content_hash: row.content_hash,
            last_validated_content_hash: row.last_validated_content_hash,
            created_at: row.created_at,
        })
    }
}

#[derive(Debug, Deserialize)]
struct WorkflowRunRow {
    run_id: String,
    workflow_id: String,
    workflow_version: i64,
    status: String,
    cancel_reason: Option<String>,
    cancel_requested_by: Option<String>,
    cancel_requested_at: Option<i64>,
    created_at: i64,
}

impl TryFrom<WorkflowRunRow> for WorkflowRunRecord {
    type Error = PersistenceError;

    fn try_from(row: WorkflowRunRow) -> Result<Self, Self::Error> {
        let workflow_version =
            checked_stored_workflow_version("workflow run", &row.run_id, row.workflow_version)?;
        Ok(Self {
            run_id: row.run_id,
            workflow_id: row.workflow_id,
            workflow_version,
            status: row.status,
            cancel_reason: row.cancel_reason,
            cancel_requested_by: row.cancel_requested_by,
            cancel_requested_at: row.cancel_requested_at,
            created_at: row.created_at,
        })
    }
}

#[derive(Debug, Deserialize)]
struct StepInstanceRow {
    step_instance_id: String,
    run_id: String,
    step_id: String,
    status: String,
    attempt_count: i64,
    input_json: Option<String>,
    output_json: Option<String>,
    error_json: Option<String>,
    created_at: i64,
    updated_at: i64,
}

impl TryFrom<StepInstanceRow> for StepInstanceRecord {
    type Error = PersistenceError;

    fn try_from(row: StepInstanceRow) -> Result<Self, Self::Error> {
        Ok(Self {
            step_instance_id: row.step_instance_id,
            run_id: row.run_id,
            step_id: row.step_id,
            status: row.status,
            attempt_count: row.attempt_count as u32,
            input: parse_optional_json(row.input_json)?,
            output: parse_optional_json(row.output_json)?,
            error: parse_optional_json(row.error_json)?,
            created_at: row.created_at,
            updated_at: row.updated_at,
        })
    }
}

#[derive(Debug, Deserialize)]
struct TimerRow {
    timer_id: String,
    run_id: String,
    step_id: String,
    timer_kind: String,
    due_at: i64,
    status: String,
    payload_json: Option<String>,
    effect_state: Option<String>,
    created_at: i64,
    updated_at: i64,
}

#[derive(Debug, Deserialize)]
struct TimerEffectStateRow {
    status: String,
    effect_state: Option<String>,
}

impl TryFrom<TimerRow> for TimerRecord {
    type Error = PersistenceError;

    fn try_from(row: TimerRow) -> Result<Self, Self::Error> {
        Ok(Self {
            timer_id: row.timer_id,
            run_id: row.run_id,
            step_id: row.step_id,
            timer_kind: row.timer_kind,
            due_at: row.due_at,
            status: row.status,
            effect_state: row.effect_state,
            payload: parse_optional_json(row.payload_json)?,
            created_at: row.created_at,
            updated_at: row.updated_at,
        })
    }
}

#[derive(Debug, Clone)]
pub(crate) struct EventInboxRecord {
    pub event_id: String,
    pub event_type: String,
    pub processing_state: String,
    pub payload: Value,
    pub dedupe_key: Option<String>,
}

#[derive(Debug, Deserialize)]
struct EventInboxRow {
    event_id: String,
    event_type: String,
    processing_state: String,
    payload_json: String,
    dedupe_key: Option<String>,
}

impl TryFrom<EventInboxRow> for EventInboxRecord {
    type Error = PersistenceError;

    fn try_from(row: EventInboxRow) -> Result<Self, Self::Error> {
        Ok(Self {
            event_id: row.event_id,
            event_type: row.event_type,
            processing_state: row.processing_state,
            payload: serde_json::from_str(&row.payload_json)?,
            dedupe_key: row.dedupe_key,
        })
    }
}

#[derive(Debug, Deserialize)]
struct TransitionRow {
    transition_id: String,
    run_id: String,
    transition_type: String,
    payload_json: Option<String>,
    created_at: i64,
}

impl TryFrom<TransitionRow> for TransitionRecord {
    type Error = PersistenceError;

    fn try_from(row: TransitionRow) -> Result<Self, Self::Error> {
        Ok(Self {
            transition_id: row.transition_id,
            run_id: row.run_id,
            transition_type: row.transition_type,
            payload: parse_optional_json(row.payload_json)?,
            created_at: row.created_at,
        })
    }
}

#[derive(Debug, Deserialize)]
struct TaskQueueRow {
    task_id: String,
    run_id: String,
    workflow_id: String,
    workflow_version: i64,
    step_id: String,
    step_kind: String,
    handler_id: String,
    capability: String,
    queue_name: String,
    input_json: String,
    timeout_at: Option<i64>,
    status: String,
    attempt_sequence: i64,
    active_attempt_id: Option<String>,
    active_fencing_token: Option<i64>,
    leased_by_worker_id: Option<String>,
}

impl TryFrom<TaskQueueRow> for TaskSnapshot {
    type Error = PersistenceError;

    fn try_from(row: TaskQueueRow) -> Result<Self, Self::Error> {
        let workflow_version =
            checked_stored_workflow_version("task", &row.task_id, row.workflow_version)?;
        Ok(Self {
            task_id: row.task_id,
            run_id: row.run_id,
            workflow_id: row.workflow_id,
            workflow_version,
            step_id: row.step_id,
            step_kind: row.step_kind,
            handler_id: row.handler_id,
            capability: row.capability,
            queue_name: row.queue_name,
            input: serde_json::from_str::<Value>(&row.input_json)?,
            status: row.status,
            attempt_sequence: row.attempt_sequence as u32,
            timeout_at: row.timeout_at,
            active_attempt_id: row.active_attempt_id,
            active_fencing_token: row.active_fencing_token.map(|value| value as u64),
            leased_by_worker_id: row.leased_by_worker_id,
        })
    }
}

#[derive(Debug, Deserialize)]
struct TaskStatusRow {
    status: String,
}

#[derive(Debug, Clone, Copy)]
enum ContinuationQuarantineSource {
    Task,
    Timer,
}

#[derive(Debug, Deserialize)]
struct ContinuationQuarantineStateRow {
    status: String,
    continuation_state: Option<String>,
    run_status: String,
}

#[derive(Debug, Deserialize)]
struct FailedTaskQuarantineStateRow {
    task_status: String,
    continuation_state: Option<String>,
    timeout_at: Option<i64>,
    run_status: String,
    step_status: Option<String>,
    error_json: Option<String>,
    exact_lease_present: i64,
}

#[derive(Debug, Deserialize)]
struct TransitionEvidenceRow {
    transition_type: String,
    payload_json: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ContinuationStateRow {
    continuation_state: Option<String>,
}

#[derive(Debug, Deserialize)]
struct CompletedTaskContinuationRow {
    task_id: String,
    run_id: String,
    workflow_id: String,
    workflow_version: i64,
    step_id: String,
    output_json: Option<String>,
}

impl TryFrom<CompletedTaskContinuationRow> for CompletedTaskContinuation {
    type Error = PersistenceError;

    fn try_from(row: CompletedTaskContinuationRow) -> Result<Self, Self::Error> {
        let output_json = row.output_json.ok_or_else(|| {
            PersistenceError::Invariant(format!(
                "pending completed-task continuation '{}' is missing persisted output",
                row.task_id
            ))
        })?;
        let workflow_version =
            checked_stored_workflow_version("task", &row.task_id, row.workflow_version)?;
        Ok(Self {
            task_id: row.task_id,
            run_id: row.run_id,
            workflow_id: row.workflow_id,
            workflow_version,
            step_id: row.step_id,
            output: serde_json::from_str(&output_json)?,
        })
    }
}

#[derive(Debug, Deserialize)]
struct VersionBoundsRow {
    minimum_workflow_version: Option<i64>,
    maximum_workflow_version: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct ColumnInfoRow {
    name: String,
}

#[derive(Debug, Deserialize)]
struct TableNameRow {
    name: String,
}

#[derive(Debug, Deserialize)]
struct IndexMasterRow {
    table_name: String,
    sql: Option<String>,
}

#[derive(Debug, Deserialize)]
struct IndexListRow {
    name: String,
    #[serde(rename = "unique")]
    is_unique: i64,
    partial: i64,
}

#[derive(Debug, Deserialize)]
struct IndexColumnRow {
    seqno: i64,
    name: Option<String>,
}

#[derive(Debug, Deserialize)]
struct DuplicateRow {
    run_id: String,
    step_id: String,
    stable_row_id: String,
}

#[derive(Debug, PartialEq, Eq)]
struct DuplicateGroup {
    table: &'static str,
    run_id: String,
    step_id: String,
    timer_kind: Option<&'static str>,
    stable_row_ids: Vec<String>,
}

impl DuplicateGroup {
    fn diagnostic(&self) -> String {
        let timer_kind = self
            .timer_kind
            .map(|kind| format!(", timer_kind={kind:?}"))
            .unwrap_or_default();
        format!(
            "table={} logical_key=(run_id={:?}, step_id={:?}{timer_kind}) stable_row_ids={:?}",
            self.table, self.run_id, self.step_id, self.stable_row_ids
        )
    }
}

type CompatMigrationStatement = (&'static str, Vec<Param>);

#[derive(Clone, Copy)]
struct CompatIndexDefinition {
    name: &'static str,
    table: &'static str,
    unique: bool,
    columns: &'static [&'static str],
    predicate: Option<&'static str>,
    create_sql: &'static str,
}

const COMPAT_COLUMNS: [(&str, &str, &str); 4] = [
    (
        "workflow_definitions",
        "execution_metadata_json",
        "ALTER TABLE workflow_definitions ADD COLUMN execution_metadata_json TEXT",
    ),
    (
        "task_queue",
        "timeout_at",
        "ALTER TABLE task_queue ADD COLUMN timeout_at INTEGER",
    ),
    (
        "task_queue",
        "continuation_state",
        "ALTER TABLE task_queue ADD COLUMN continuation_state TEXT",
    ),
    (
        "timers",
        "effect_state",
        "ALTER TABLE timers ADD COLUMN effect_state TEXT",
    ),
];

const COMPAT_INDEXES: [CompatIndexDefinition; 9] = [
    CompatIndexDefinition {
        name: "idx_step_instances_run_step",
        table: "step_instances",
        unique: true,
        columns: &["run_id", "step_id"],
        predicate: None,
        create_sql: "CREATE UNIQUE INDEX IF NOT EXISTS idx_step_instances_run_step ON step_instances(run_id, step_id)",
    },
    CompatIndexDefinition {
        name: "idx_step_instances_run_created",
        table: "step_instances",
        unique: false,
        columns: &["run_id", "created_at", "step_instance_id"],
        predicate: None,
        create_sql: "CREATE INDEX IF NOT EXISTS idx_step_instances_run_created ON step_instances(run_id, created_at, step_instance_id)",
    },
    CompatIndexDefinition {
        name: "idx_task_queue_run_step",
        table: "task_queue",
        unique: true,
        columns: &["run_id", "step_id"],
        predicate: None,
        create_sql: "CREATE UNIQUE INDEX IF NOT EXISTS idx_task_queue_run_step ON task_queue(run_id, step_id)",
    },
    CompatIndexDefinition {
        name: "idx_task_queue_timeout_recovery",
        table: "task_queue",
        unique: false,
        columns: &[
            "status",
            "continuation_state",
            "timeout_at",
            "created_at",
            "task_id",
        ],
        predicate: None,
        create_sql: "CREATE INDEX IF NOT EXISTS idx_task_queue_timeout_recovery ON task_queue(status, continuation_state, timeout_at, created_at, task_id)",
    },
    CompatIndexDefinition {
        name: "idx_task_queue_lease_recovery",
        table: "task_queue",
        unique: false,
        columns: &["status", "continuation_state", "updated_at", "task_id"],
        predicate: None,
        create_sql: "CREATE INDEX IF NOT EXISTS idx_task_queue_lease_recovery ON task_queue(status, continuation_state, updated_at, task_id)",
    },
    CompatIndexDefinition {
        name: "idx_task_queue_continuation_recovery",
        table: "task_queue",
        unique: false,
        columns: &["status", "continuation_state", "updated_at", "task_id"],
        predicate: None,
        create_sql: "CREATE INDEX IF NOT EXISTS idx_task_queue_continuation_recovery ON task_queue(status, continuation_state, updated_at, task_id)",
    },
    CompatIndexDefinition {
        name: "idx_timers_sleep_run_step",
        table: "timers",
        unique: true,
        columns: &["run_id", "step_id", "timer_kind"],
        predicate: Some("timer_kind = 'sleep'"),
        create_sql: "CREATE UNIQUE INDEX IF NOT EXISTS idx_timers_sleep_run_step ON timers(run_id, step_id, timer_kind) WHERE timer_kind = 'sleep'",
    },
    CompatIndexDefinition {
        name: "idx_transition_log_run_created",
        table: "transition_log",
        unique: false,
        columns: &["run_id", "created_at", "transition_id"],
        predicate: None,
        create_sql: "CREATE INDEX IF NOT EXISTS idx_transition_log_run_created ON transition_log(run_id, created_at, transition_id)",
    },
    CompatIndexDefinition {
        name: "idx_event_inbox_recovery",
        table: "event_inbox",
        unique: false,
        columns: &["processing_state", "created_at", "event_id"],
        predicate: None,
        create_sql: "CREATE INDEX IF NOT EXISTS idx_event_inbox_recovery ON event_inbox(processing_state, created_at, event_id)",
    },
];

async fn apply_compat_migrations(client: &Client) -> Result<(), PersistenceError> {
    ensure_no_compatibility_duplicates(client).await?;
    let statements = compatibility_migration_statements(client).await?;
    apply_compat_migration_plan(client, statements).await
}

async fn ensure_no_compatibility_duplicates(client: &Client) -> Result<(), PersistenceError> {
    let duplicates = compatibility_duplicates(client).await?;
    if duplicates.is_empty() {
        return Ok(());
    }

    let diagnostics = duplicates
        .iter()
        .map(DuplicateGroup::diagnostic)
        .collect::<Vec<_>>()
        .join("; ");
    Err(PersistenceError::Invariant(format!(
        "compatibility migration blocked by duplicate durable rows: {diagnostics}"
    )))
}

async fn compatibility_duplicates(
    client: &Client,
) -> Result<Vec<DuplicateGroup>, PersistenceError> {
    let mut duplicates = Vec::new();
    duplicates.extend(duplicate_groups(
        "step_instances",
        None,
        client
            .query_as::<DuplicateRow, _>(
                r#"
                    WITH duplicate_keys AS (
                        SELECT run_id, step_id
                        FROM step_instances
                        GROUP BY run_id, step_id
                        HAVING COUNT(*) > 1
                    )
                    SELECT
                        source_rows.run_id,
                        source_rows.step_id,
                        source_rows.step_instance_id AS stable_row_id
                    FROM step_instances source_rows
                    INNER JOIN duplicate_keys keys
                        ON keys.run_id = source_rows.run_id
                        AND keys.step_id = source_rows.step_id
                    ORDER BY source_rows.run_id, source_rows.step_id, source_rows.step_instance_id
                    "#,
                Vec::<Param>::new(),
            )
            .await?,
    ));
    duplicates.extend(duplicate_groups(
        "task_queue",
        None,
        client
            .query_as::<DuplicateRow, _>(
                r#"
                    WITH duplicate_keys AS (
                        SELECT run_id, step_id
                        FROM task_queue
                        GROUP BY run_id, step_id
                        HAVING COUNT(*) > 1
                    )
                    SELECT
                        source_rows.run_id,
                        source_rows.step_id,
                        source_rows.task_id AS stable_row_id
                    FROM task_queue source_rows
                    INNER JOIN duplicate_keys keys
                        ON keys.run_id = source_rows.run_id
                        AND keys.step_id = source_rows.step_id
                    ORDER BY source_rows.run_id, source_rows.step_id, source_rows.task_id
                    "#,
                Vec::<Param>::new(),
            )
            .await?,
    ));
    duplicates.extend(duplicate_groups(
        "timers",
        Some("sleep"),
        client
            .query_as::<DuplicateRow, _>(
                r#"
                    WITH duplicate_keys AS (
                        SELECT run_id, step_id
                        FROM timers
                        WHERE timer_kind = 'sleep'
                        GROUP BY run_id, step_id
                        HAVING COUNT(*) > 1
                    )
                    SELECT
                        source_rows.run_id,
                        source_rows.step_id,
                        source_rows.timer_id AS stable_row_id
                    FROM timers source_rows
                    INNER JOIN duplicate_keys keys
                        ON keys.run_id = source_rows.run_id
                        AND keys.step_id = source_rows.step_id
                    WHERE source_rows.timer_kind = 'sleep'
                    ORDER BY source_rows.run_id, source_rows.step_id, source_rows.timer_id
                    "#,
                Vec::<Param>::new(),
            )
            .await?,
    ));
    Ok(duplicates)
}

fn duplicate_groups(
    table: &'static str,
    timer_kind: Option<&'static str>,
    rows: Vec<DuplicateRow>,
) -> Vec<DuplicateGroup> {
    let mut groups = Vec::<DuplicateGroup>::new();
    for row in rows {
        match groups.last_mut() {
            Some(group) if group.run_id == row.run_id && group.step_id == row.step_id => {
                group.stable_row_ids.push(row.stable_row_id);
            }
            _ => groups.push(DuplicateGroup {
                table,
                run_id: row.run_id,
                step_id: row.step_id,
                timer_kind,
                stable_row_ids: vec![row.stable_row_id],
            }),
        }
    }
    groups
}

async fn compatibility_migration_statements(
    client: &Client,
) -> Result<Vec<CompatMigrationStatement>, PersistenceError> {
    validated_compat_index_presence(client).await?;
    let mut statements = Vec::new();
    for &(table, column, statement) in &COMPAT_COLUMNS {
        if !table_has_column(client, table, column).await? {
            statements.push((statement, Vec::<Param>::new()));
        }
    }

    for definition in &COMPAT_INDEXES {
        if table_exists(client, definition.table).await? {
            statements.push((definition.create_sql, Vec::<Param>::new()));
        }
    }
    Ok(statements)
}

async fn table_exists(client: &Client, table: &str) -> Result<bool, PersistenceError> {
    Ok(client
        .query_as_optional::<TableNameRow, _>(
            r#"
            SELECT name
            FROM sqlite_master
            WHERE type = 'table' AND name = $1
            "#,
            params!(table.to_owned()),
        )
        .await?
        .is_some_and(|row| row.name == table))
}

async fn table_has_column(
    client: &Client,
    table: &str,
    column: &str,
) -> Result<bool, PersistenceError> {
    let rows = client
        .query_as::<ColumnInfoRow, _>(format!("PRAGMA table_info({table})"), Vec::<Param>::new())
        .await?;
    Ok(rows.iter().any(|row| row.name == column))
}

async fn validated_compat_index_presence(client: &Client) -> Result<Vec<bool>, PersistenceError> {
    let mut presence = Vec::with_capacity(COMPAT_INDEXES.len());
    for definition in &COMPAT_INDEXES {
        presence.push(if table_exists(client, definition.table).await? {
            validate_compat_index(client, *definition).await?
        } else {
            true
        });
    }
    Ok(presence)
}

async fn validate_compat_index(
    client: &Client,
    expected: CompatIndexDefinition,
) -> Result<bool, PersistenceError> {
    let Some(master) = client
        .query_as_optional::<IndexMasterRow, _>(
            r#"
            SELECT tbl_name AS table_name, sql
            FROM sqlite_master
            WHERE type = 'index' AND name = $1
            "#,
            params!(expected.name.to_owned()),
        )
        .await?
    else {
        return Ok(false);
    };

    if master.table_name != expected.table {
        return Err(PersistenceError::Invariant(format!(
            "compatibility migration blocked by malformed index {}: expected table={}; actual table={}",
            expected.name, expected.table, master.table_name
        )));
    }

    let index_list = client
        .query_as::<IndexListRow, _>(
            format!("PRAGMA index_list({})", expected.table),
            Vec::<Param>::new(),
        )
        .await?;
    let Some(index_entry) = index_list
        .into_iter()
        .find(|entry| entry.name == expected.name)
    else {
        return Err(PersistenceError::Invariant(format!(
            "compatibility migration blocked by inconsistent index catalog for {}",
            expected.name
        )));
    };

    let mut column_rows = client
        .query_as::<IndexColumnRow, _>(
            format!("PRAGMA index_info({})", expected.name),
            Vec::<Param>::new(),
        )
        .await?;
    column_rows.sort_by_key(|row| row.seqno);
    let actual_columns = column_rows
        .into_iter()
        .map(|row| row.name.unwrap_or_else(|| "<expression>".to_owned()))
        .collect::<Vec<_>>();
    let actual_predicate = normalized_index_predicate(master.sql.as_deref());
    let is_unique = index_entry.is_unique == 1;
    let is_partial = index_entry.partial == 1;
    let expected_partial = expected.predicate.is_some();
    let columns_match = actual_columns
        .iter()
        .map(String::as_str)
        .eq(expected.columns.iter().copied());
    let predicates_match = actual_predicate.as_deref().map(predicate_comparison_key)
        == expected.predicate.map(predicate_comparison_key);

    if is_unique != expected.unique
        || is_partial != expected_partial
        || !columns_match
        || !predicates_match
    {
        return Err(PersistenceError::Invariant(format!(
            "compatibility migration blocked by malformed index {}: expected table={} unique={} ordered_columns={:?} predicate={:?}; actual table={} unique={} ordered_columns={:?} predicate={:?}",
            expected.name,
            expected.table,
            expected.unique,
            expected.columns,
            expected.predicate,
            master.table_name,
            is_unique,
            actual_columns,
            actual_predicate
        )));
    }

    Ok(true)
}

fn normalized_index_predicate(sql: Option<&str>) -> Option<String> {
    let normalized = normalize_sql_outside_literals(sql?, true);
    normalized
        .split_once(" where ")
        .map(|(_, predicate)| predicate.to_owned())
}

fn predicate_comparison_key(predicate: &str) -> String {
    normalize_sql_outside_literals(predicate, false)
}

fn normalize_sql_outside_literals(sql: &str, retain_spaces: bool) -> String {
    let mut normalized = String::with_capacity(sql.len());
    let mut chars = sql.chars().peekable();
    let mut in_literal = false;
    let mut pending_space = false;

    while let Some(character) = chars.next() {
        if in_literal {
            normalized.push(character);
            if character == '\'' {
                if chars.peek() == Some(&'\'') {
                    normalized.push(chars.next().expect("peeked quote should exist"));
                } else {
                    in_literal = false;
                }
            }
            continue;
        }

        if character == '\'' {
            if retain_spaces && pending_space && !normalized.is_empty() {
                normalized.push(' ');
            }
            pending_space = false;
            normalized.push(character);
            in_literal = true;
        } else if character.is_whitespace() {
            pending_space = true;
        } else {
            if retain_spaces && pending_space && !normalized.is_empty() {
                normalized.push(' ');
            }
            pending_space = false;
            normalized.push(character.to_ascii_lowercase());
        }
    }

    normalized
}

async fn compatibility_migration_is_complete(client: &Client) -> Result<bool, PersistenceError> {
    for &(table, column, _) in &COMPAT_COLUMNS {
        if !table_has_column(client, table, column).await? {
            return Ok(false);
        }
    }
    Ok(validated_compat_index_presence(client)
        .await?
        .into_iter()
        .all(|present| present))
}

async fn execute_compat_migration_transaction(
    client: &Client,
    statements: Vec<CompatMigrationStatement>,
) -> Result<(), PersistenceError> {
    // Hiqlite executes this entire list through one rusqlite transaction, including SQLite DDL.
    // Duplicate inspection above is read-only; this transaction is the sole mutation boundary.
    // Any ALTER or CREATE failure rolls back every column and index in this list.
    for statement_result in client.txn(statements).await? {
        statement_result?;
    }
    Ok(())
}

async fn apply_compat_migration_plan(
    client: &Client,
    mut statements: Vec<CompatMigrationStatement>,
) -> Result<(), PersistenceError> {
    // Two startup nodes can build the same plan before either commits. The second node may then
    // see a duplicate-column error from its stale plan. Reinspect once: if the first node completed
    // the exact schema, accept it; otherwise rebuild and retry once. Every failed DDL attempt is
    // still rolled back atomically by execute_compat_migration_transaction.
    for attempt in 0..2 {
        match execute_compat_migration_transaction(client, statements).await {
            Ok(()) => {
                if compatibility_migration_is_complete(client).await? {
                    return Ok(());
                }
                return Err(PersistenceError::Invariant(
                    "compatibility migration committed without the complete expected schema"
                        .to_owned(),
                ));
            }
            Err(error) if attempt == 1 => return Err(error),
            Err(_) => {
                ensure_no_compatibility_duplicates(client).await?;
                if compatibility_migration_is_complete(client).await? {
                    return Ok(());
                }
                statements = compatibility_migration_statements(client).await?;
            }
        }
    }
    unreachable!("compatibility migration retry loop has a fixed non-zero bound")
}

#[cfg(test)]
#[allow(clippy::items_after_test_module)]
mod tests {
    use serde::Deserialize;
    use serde_json::json;
    use tempfile::TempDir;
    use workflow_definition::FlowDefinition;
    use workflow_protocol::{FailurePayload, PollTask, TaskMutationContext, TaskOutput};

    use crate::definition_store::{StoredWorkflowDefinition, WorkflowRunRecord};
    use crate::persistence::{
        ContinuationQuarantineResult, LatestEligibleDefinitionSelection,
        TaskExecutionQuarantineResult, TaskOutcomeApplyResult,
    };
    use crate::runtime::{MutationApplyResult, QueuedTaskRecord, StepInstanceRecord};
    use crate::stored_definition::{
        StoredDefinitionClassification, StoredDefinitionDecodeDiagnosticCode,
    };

    use super::{
        apply_compat_migration_plan, apply_compat_migrations, compatibility_migration_statements,
        execute_compat_migration_transaction, local_node_config, stable_run_failure_transition_id,
        stable_task_timeout_transition_id, stable_task_timer_id, stable_task_transition_id,
        table_exists, table_has_column, HiqlitePersistence, PersistenceError,
        CONTINUATION_STATE_PENDING, CONTINUATION_STATE_SUPPRESSED, RECOVERY_SCAN_BATCH_SIZE,
        RUN_STATUS_CANCELLED, RUN_STATUS_FAILED, RUN_STATUS_RUNNING, STATUS_PUBLISHED,
        TASK_STATUS_CANCELLED, TASK_STATUS_COMPLETED, TASK_STATUS_FAILED, TASK_STATUS_LEASED,
        TASK_STATUS_QUEUED, TASK_STATUS_RETRY_PENDING, TASK_STATUS_TIMED_OUT,
        TIMER_EFFECT_STATE_PENDING, TIMER_EFFECT_STATE_SUPPRESSED, TIMER_STATUS_CANCELLED,
        TIMER_STATUS_PENDING,
    };
    use hiqlite::{start_node, Client, Param};

    const PRE_FEATURE_SCHEMA: &str = r#"
        CREATE TABLE workflow_definitions (
            workflow_id TEXT NOT NULL,
            workflow_version INTEGER NOT NULL,
            status TEXT NOT NULL,
            definition_json TEXT NOT NULL,
            content_hash TEXT NOT NULL,
            last_validated_content_hash TEXT,
            created_at INTEGER NOT NULL,
            published_at INTEGER,
            PRIMARY KEY (workflow_id, status, workflow_version)
        );

        CREATE TABLE step_instances (
            step_instance_id TEXT PRIMARY KEY,
            run_id TEXT NOT NULL,
            step_id TEXT NOT NULL,
            status TEXT NOT NULL,
            attempt_count INTEGER NOT NULL,
            input_json TEXT,
            output_json TEXT,
            error_json TEXT,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL
        );

        CREATE TABLE task_queue (
            task_id TEXT PRIMARY KEY,
            run_id TEXT NOT NULL,
            workflow_id TEXT NOT NULL,
            workflow_version INTEGER NOT NULL,
            step_id TEXT NOT NULL,
            step_kind TEXT NOT NULL,
            handler_id TEXT NOT NULL,
            capability TEXT NOT NULL,
            queue_name TEXT NOT NULL,
            input_json TEXT NOT NULL,
            status TEXT NOT NULL,
            attempt_sequence INTEGER NOT NULL,
            active_attempt_id TEXT,
            active_fencing_token INTEGER,
            leased_by_worker_id TEXT,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL
        );

        CREATE TABLE timers (
            timer_id TEXT PRIMARY KEY,
            run_id TEXT NOT NULL,
            step_id TEXT NOT NULL,
            timer_kind TEXT NOT NULL,
            due_at INTEGER NOT NULL,
            status TEXT NOT NULL,
            payload_json TEXT,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL
        );

        CREATE TABLE leases (
            lease_id TEXT PRIMARY KEY,
            task_id TEXT NOT NULL,
            attempt_id TEXT NOT NULL,
            worker_id TEXT NOT NULL,
            fencing_token INTEGER NOT NULL,
            expires_at INTEGER NOT NULL,
            heartbeat_at INTEGER NOT NULL
        );
    "#;

    #[derive(Debug, Deserialize, PartialEq, Eq)]
    struct StableIdRow {
        stable_row_id: String,
    }

    #[derive(Debug, Deserialize, PartialEq, Eq)]
    struct LeaseReferenceRow {
        lease_id: String,
        task_id: String,
    }

    #[derive(Debug, Deserialize, PartialEq, Eq)]
    struct NameRow {
        name: String,
    }

    #[tokio::test]
    async fn duplicate_pre_feature_rows_fail_safely_and_preserve_rows_and_references() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let client = pre_feature_client(&tempdir).await;
        apply_batch(
            &client,
            r#"
            INSERT INTO step_instances VALUES
                ('instance-b', 'run-legacy', 'step-instance', 'running', 1, NULL, NULL, NULL, 1, 1),
                ('instance-a', 'run-legacy', 'step-instance', 'running', 1, NULL, NULL, NULL, 1, 1);

            INSERT INTO task_queue VALUES
                ('task-b', 'run-legacy', 'orders', 1, 'step-task', 'task', 'orders.sync', 'network', 'default', '{}', 'queued', 1, NULL, NULL, NULL, 1, 1),
                ('task-a', 'run-legacy', 'orders', 1, 'step-task', 'task', 'orders.sync', 'network', 'default', '{}', 'queued', 1, NULL, NULL, NULL, 1, 1);

            INSERT INTO timers VALUES
                ('timer-b', 'run-legacy', 'step-timer', 'sleep', 10, 'pending', NULL, 1, 1),
                ('timer-a', 'run-legacy', 'step-timer', 'sleep', 10, 'pending', NULL, 1, 1);

            INSERT INTO leases VALUES
                ('lease-a', 'task-a', 'attempt-a', 'worker-a', 1, 10, 1),
                ('lease-b', 'task-b', 'attempt-b', 'worker-b', 1, 10, 1);
            "#,
        )
        .await;

        let error = apply_compat_migrations(&client)
            .await
            .expect_err("duplicates must block migration");
        assert_eq!(
            error.to_string(),
            concat!(
                "persistence invariant violation: compatibility migration blocked by duplicate durable rows: ",
                "table=step_instances logical_key=(run_id=\"run-legacy\", step_id=\"step-instance\") stable_row_ids=[\"instance-a\", \"instance-b\"]; ",
                "table=task_queue logical_key=(run_id=\"run-legacy\", step_id=\"step-task\") stable_row_ids=[\"task-a\", \"task-b\"]; ",
                "table=timers logical_key=(run_id=\"run-legacy\", step_id=\"step-timer\", timer_kind=\"sleep\") stable_row_ids=[\"timer-a\", \"timer-b\"]"
            )
        );

        assert_eq!(
            stable_ids(
                &client,
                "SELECT step_instance_id AS stable_row_id FROM step_instances ORDER BY step_instance_id",
            )
            .await,
            vec!["instance-a", "instance-b"]
        );
        assert_eq!(
            stable_ids(
                &client,
                "SELECT task_id AS stable_row_id FROM task_queue ORDER BY task_id",
            )
            .await,
            vec!["task-a", "task-b"]
        );
        assert_eq!(
            stable_ids(
                &client,
                "SELECT timer_id AS stable_row_id FROM timers ORDER BY timer_id",
            )
            .await,
            vec!["timer-a", "timer-b"]
        );
        let lease_references = client
            .query_as::<LeaseReferenceRow, _>(
                "SELECT lease_id, task_id FROM leases ORDER BY lease_id",
                Vec::<Param>::new(),
            )
            .await
            .expect("lease references should remain readable");
        assert_eq!(
            lease_references,
            vec![
                LeaseReferenceRow {
                    lease_id: "lease-a".to_owned(),
                    task_id: "task-a".to_owned(),
                },
                LeaseReferenceRow {
                    lease_id: "lease-b".to_owned(),
                    task_id: "task-b".to_owned(),
                },
            ]
        );
        assert_compat_columns_absent(&client).await;
        assert!(compatibility_index_names(&client).await.is_empty());
    }

    #[tokio::test]
    async fn compatibility_migration_rolls_back_ddl_as_a_unit() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let client = pre_feature_client(&tempdir).await;
        let mut statements = compatibility_migration_statements(&client)
            .await
            .expect("migration statements should be built");
        statements.push((
            "CREATE UNIQUE INDEX forced_migration_failure ON timers(column_does_not_exist)",
            Vec::<Param>::new(),
        ));

        let error = execute_compat_migration_transaction(&client, statements)
            .await
            .expect_err("the injected final DDL statement should fail");
        assert!(matches!(error, PersistenceError::Hiqlite(_)));
        assert_compat_columns_absent(&client).await;
        assert!(compatibility_index_names(&client).await.is_empty());
    }

    #[tokio::test]
    async fn clean_pre_feature_schema_migrates_idempotently() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let client = pre_feature_client(&tempdir).await;
        client
            .execute(
                r#"
                INSERT INTO workflow_definitions (
                    workflow_id, workflow_version, status, definition_json, content_hash,
                    last_validated_content_hash, created_at, published_at
                ) VALUES ($1, $2, $3, $4, $5, NULL, $6, $7)
                "#,
                vec![
                    Param::from("orders".to_owned()),
                    Param::from(1_i64),
                    Param::from("published".to_owned()),
                    Param::from("{}".to_owned()),
                    Param::from("hash".to_owned()),
                    Param::from(1_i64),
                    Param::from(1_i64),
                ],
            )
            .await
            .expect("pre-feature definition should insert");

        apply_compat_migrations(&client)
            .await
            .expect("clean pre-feature schema should migrate");
        apply_compat_migrations(&client)
            .await
            .expect("migration should be idempotent");
        assert!(!table_exists(&client, "event_inbox")
            .await
            .expect("absent optional table inspection should succeed"));
        assert!(!table_exists(&client, "transition_log")
            .await
            .expect("absent optional transition table inspection should succeed"));

        for (table, column) in [
            ("workflow_definitions", "execution_metadata_json"),
            ("task_queue", "timeout_at"),
            ("task_queue", "continuation_state"),
            ("timers", "effect_state"),
        ] {
            assert!(
                table_has_column(&client, table, column)
                    .await
                    .expect("column inspection should succeed"),
                "expected {table}.{column}"
            );
        }
        assert_eq!(
            compatibility_index_names(&client).await,
            vec![
                "idx_step_instances_run_created",
                "idx_step_instances_run_step",
                "idx_task_queue_continuation_recovery",
                "idx_task_queue_lease_recovery",
                "idx_task_queue_run_step",
                "idx_task_queue_timeout_recovery",
                "idx_timers_sleep_run_step",
            ]
        );
        let metadata_is_null = client
            .query_as::<NameRow, _>(
                r#"
                SELECT CASE
                    WHEN execution_metadata_json IS NULL THEN 'null'
                    ELSE 'not-null'
                END AS name
                FROM workflow_definitions
                WHERE workflow_id = 'orders'
                "#,
                Vec::<Param>::new(),
            )
            .await
            .expect("pre-feature definition should remain inspectable");
        assert_eq!(metadata_is_null[0].name, "null");
    }

    #[tokio::test]
    async fn normal_startup_migrates_pre_feature_schema_idempotently() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let pre_feature = pre_feature_client(&tempdir).await;
        apply_batch(
            &pre_feature,
            r#"
            INSERT INTO workflow_definitions (
                workflow_id, workflow_version, status, definition_json, content_hash,
                last_validated_content_hash, created_at, published_at
            ) VALUES ('orders', 1, 'published', '{}', 'pre-feature-hash', NULL, 1, 1);
            "#,
        )
        .await;
        pre_feature
            .shutdown()
            .await
            .expect("pre-feature node should stop");
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;

        let first = HiqlitePersistence::for_test(tempdir.path())
            .await
            .expect("normal startup should migrate the retained database");
        assert!(!table_exists(&first.client, "waiters")
            .await
            .expect("removed waiter table inspection should succeed"));
        assert!(!table_exists(&first.client, "legacy_emit_effects")
            .await
            .expect("removed emit-effect table inspection should succeed"));
        for (table, column) in [
            ("workflow_definitions", "execution_metadata_json"),
            ("task_queue", "timeout_at"),
            ("task_queue", "continuation_state"),
            ("timers", "effect_state"),
        ] {
            assert!(
                table_has_column(&first.client, table, column)
                    .await
                    .expect("column inspection should succeed"),
                "expected {table}.{column}"
            );
        }
        assert_eq!(
            compatibility_index_names(&first.client).await,
            vec![
                "idx_event_inbox_recovery",
                "idx_step_instances_run_created",
                "idx_step_instances_run_step",
                "idx_task_queue_continuation_recovery",
                "idx_task_queue_lease_recovery",
                "idx_task_queue_run_step",
                "idx_task_queue_timeout_recovery",
                "idx_timers_sleep_run_step",
                "idx_transition_log_run_created",
            ]
        );
        first
            .client
            .shutdown()
            .await
            .expect("first node should stop");
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;

        let second = HiqlitePersistence::for_test(tempdir.path())
            .await
            .expect("normal startup migration should be idempotent");
        assert_eq!(compatibility_index_names(&second.client).await.len(), 9);
    }

    #[tokio::test]
    async fn task_and_timer_provenance_quarantines_are_durable_and_suppressible() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let persistence = HiqlitePersistence::for_test(tempdir.path())
            .await
            .expect("persistence should start");
        persistence
            .insert_run(&WorkflowRunRecord {
                run_id: "run-provenance-quarantine".to_owned(),
                workflow_id: "legacy-orders".to_owned(),
                workflow_version: 1,
                status: RUN_STATUS_RUNNING.to_owned(),
                cancel_reason: None,
                cancel_requested_by: None,
                cancel_requested_at: None,
                created_at: 10,
            })
            .await
            .expect("run should insert");
        insert_completed_pending_continuation(&persistence, "run-provenance-quarantine", 10).await;
        insert_pending_timer(
            &persistence,
            "run-provenance-quarantine",
            "legacy-sleep",
            10,
            10,
        )
        .await;
        persistence
            .client
            .execute(
                r#"
                UPDATE timers
                SET status = 'fired', effect_state = 'pending'
                WHERE run_id = 'run-provenance-quarantine'
                "#,
                Vec::<Param>::new(),
            )
            .await
            .expect("timer should become an unapplied fired effect");
        assert_eq!(
            persistence
                .quarantine_task_continuation(
                    "task-completed-before-fail",
                    "legacy_task_route_provenance_missing",
                    20,
                )
                .await
                .expect("task quarantine should apply"),
            ContinuationQuarantineResult::Applied
        );
        let timer_id = "fixture-timer-run-provenance-quarantine";
        assert_eq!(
            persistence
                .quarantine_timer_effect(timer_id, "legacy_task_route_provenance_missing", 20,)
                .await
                .expect("timer quarantine should apply"),
            ContinuationQuarantineResult::Applied
        );
        assert_eq!(
            persistence
                .quarantine_task_continuation(
                    "task-completed-before-fail",
                    "legacy_task_route_provenance_missing",
                    21,
                )
                .await
                .expect("task quarantine replay should classify"),
            ContinuationQuarantineResult::AlreadyApplied
        );
        assert!(matches!(
            persistence
                .quarantine_timer_effect(timer_id, "different_code", 21)
                .await
                .expect("conflicting timer quarantine should classify"),
            ContinuationQuarantineResult::Conflict { .. }
        ));
        assert!(persistence
            .list_pending_completed_task_continuations(30)
            .await
            .expect("task pending scan should succeed")
            .is_empty());
        assert!(persistence
            .list_due_timers(30)
            .await
            .expect("timer due scan should succeed")
            .is_empty());
        assert!(persistence
            .run_has_open_work("run-provenance-quarantine")
            .await
            .expect("quarantines should block completion"));
        assert_eq!(
            persistence
                .complete_run_if_running_and_idle("run-provenance-quarantine", 22)
                .await
                .expect("idle completion should classify"),
            MutationApplyResult::Stale
        );
        persistence
            .cancel_run_atomically("run-provenance-quarantine", None, Some("test"), 23)
            .await
            .expect("cancellation should suppress quarantines");
        let task = atomic_task_state(&persistence, "task-completed-before-fail").await;
        assert_eq!(
            task.continuation_state.as_deref(),
            Some(CONTINUATION_STATE_SUPPRESSED)
        );
        let timer = persistence
            .get_timer_record(timer_id)
            .await
            .expect("timer should load")
            .expect("timer should exist");
        assert_eq!(
            timer.effect_state.as_deref(),
            Some(TIMER_EFFECT_STATE_SUPPRESSED)
        );
        let transitions = persistence
            .list_transitions("run-provenance-quarantine")
            .await
            .expect("transitions should load");
        assert_eq!(
            transitions
                .iter()
                .filter(|transition| {
                    matches!(
                        transition.transition_type.as_str(),
                        "task_continuation_quarantined" | "timer_effect_quarantined"
                    )
                })
                .count(),
            2
        );
    }

    #[tokio::test]
    async fn concurrent_startup_reinspects_a_stale_ddl_plan() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let client = pre_feature_client(&tempdir).await;

        // Both startup paths inspect the same pre-migration schema before either one commits.
        let first_startup_plan = compatibility_migration_statements(&client)
            .await
            .expect("first startup should build a migration plan");
        let stale_second_startup_plan = compatibility_migration_statements(&client)
            .await
            .expect("second startup should build the same migration plan");
        assert_eq!(first_startup_plan.len(), 11);
        assert_eq!(stale_second_startup_plan.len(), 11);

        execute_compat_migration_transaction(&client, first_startup_plan)
            .await
            .expect("first startup should commit the migration");
        apply_compat_migration_plan(&client, stale_second_startup_plan)
            .await
            .expect("second startup should accept the exact concurrently completed schema");

        for (table, column) in [
            ("workflow_definitions", "execution_metadata_json"),
            ("task_queue", "timeout_at"),
            ("task_queue", "continuation_state"),
            ("timers", "effect_state"),
        ] {
            assert!(
                table_has_column(&client, table, column)
                    .await
                    .expect("column inspection should succeed"),
                "expected {table}.{column}"
            );
        }
        assert_eq!(
            compatibility_index_names(&client).await,
            vec![
                "idx_step_instances_run_created",
                "idx_step_instances_run_step",
                "idx_task_queue_continuation_recovery",
                "idx_task_queue_lease_recovery",
                "idx_task_queue_run_step",
                "idx_task_queue_timeout_recovery",
                "idx_timers_sleep_run_step",
            ]
        );
    }

    #[tokio::test]
    async fn malformed_same_named_index_fails_without_schema_mutation() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let client = pre_feature_client(&tempdir).await;
        client
            .execute(
                r#"
                CREATE INDEX idx_timers_sleep_run_step
                ON timers(step_id, run_id)
                WHERE timer_kind = 'retry'
                "#,
                Vec::<Param>::new(),
            )
            .await
            .expect("malformed pre-feature index should be installed");

        let error = apply_compat_migrations(&client)
            .await
            .expect_err("same-named malformed index must block migration");
        assert_eq!(
            error.to_string(),
            concat!(
                "persistence invariant violation: compatibility migration blocked by malformed index idx_timers_sleep_run_step: ",
                "expected table=timers unique=true ordered_columns=[\"run_id\", \"step_id\", \"timer_kind\"] predicate=Some(\"timer_kind = 'sleep'\"); ",
                "actual table=timers unique=false ordered_columns=[\"step_id\", \"run_id\"] predicate=Some(\"timer_kind = 'retry'\")"
            )
        );
        assert_compat_columns_absent(&client).await;
        assert_eq!(
            compatibility_index_names(&client).await,
            vec!["idx_timers_sleep_run_step"]
        );
    }

    #[tokio::test]
    async fn uppercase_literal_partial_index_fails_without_schema_mutation() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let client = pre_feature_client(&tempdir).await;
        client
            .execute(
                r#"
                CREATE UNIQUE INDEX idx_timers_sleep_run_step
                ON timers(run_id, step_id, timer_kind)
                WHERE timer_kind = 'SLEEP'
                "#,
                Vec::<Param>::new(),
            )
            .await
            .expect("uppercase-literal pre-feature index should be installed");

        let error = apply_compat_migrations(&client)
            .await
            .expect_err("case-sensitive predicate mismatch must block migration");
        assert_eq!(
            error.to_string(),
            concat!(
                "persistence invariant violation: compatibility migration blocked by malformed index idx_timers_sleep_run_step: ",
                "expected table=timers unique=true ordered_columns=[\"run_id\", \"step_id\", \"timer_kind\"] predicate=Some(\"timer_kind = 'sleep'\"); ",
                "actual table=timers unique=true ordered_columns=[\"run_id\", \"step_id\", \"timer_kind\"] predicate=Some(\"timer_kind = 'SLEEP'\")"
            )
        );
        assert_compat_columns_absent(&client).await;
        assert_eq!(
            compatibility_index_names(&client).await,
            vec!["idx_timers_sleep_run_step"]
        );
    }

    async fn pre_feature_client(tempdir: &TempDir) -> Client {
        let client = start_node(local_node_config(tempdir.path()))
            .await
            .expect("node should start");
        apply_batch(&client, PRE_FEATURE_SCHEMA).await;
        client
    }

    async fn apply_batch(client: &Client, sql: &'static str) {
        for statement_result in client
            .batch(sql)
            .await
            .expect("pre-feature schema batch should apply")
        {
            statement_result.expect("pre-feature schema statement should succeed");
        }
    }

    async fn stable_ids(client: &Client, query: &'static str) -> Vec<String> {
        client
            .query_as::<StableIdRow, _>(query, Vec::<Param>::new())
            .await
            .expect("stable row ids should remain readable")
            .into_iter()
            .map(|row| row.stable_row_id)
            .collect()
    }

    async fn compatibility_index_names(client: &Client) -> Vec<String> {
        client
            .query_as::<NameRow, _>(
                r#"
                SELECT name
                FROM sqlite_master
                WHERE type = 'index'
                    AND name IN (
                        'idx_step_instances_run_step',
                        'idx_step_instances_run_created',
                        'idx_event_inbox_recovery',
                        'idx_task_queue_continuation_recovery',
                        'idx_task_queue_lease_recovery',
                        'idx_task_queue_run_step',
                        'idx_task_queue_timeout_recovery',
                        'idx_timers_sleep_run_step',
                        'idx_transition_log_run_created'
                    )
                ORDER BY name
                "#,
                Vec::<Param>::new(),
            )
            .await
            .expect("index catalog should remain readable")
            .into_iter()
            .map(|row| row.name)
            .collect()
    }

    async fn assert_compat_columns_absent(client: &Client) {
        for (table, column) in [
            ("workflow_definitions", "execution_metadata_json"),
            ("task_queue", "timeout_at"),
            ("task_queue", "continuation_state"),
            ("timers", "effect_state"),
        ] {
            assert!(
                !table_has_column(client, table, column)
                    .await
                    .expect("column inspection should succeed"),
                "unexpected {table}.{column}"
            );
        }
    }

    #[tokio::test]
    async fn snapshotted_publication_insert_persists_definition_and_execution_metadata() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let persistence = HiqlitePersistence::for_test(tempdir.path())
            .await
            .expect("persistence should start");
        let definition = supported_definition("orders", "orders.fetch");
        let metadata = execution_metadata_json("run_task", "orders.fetch");

        persistence
            .insert_snapshotted_published_definition(
                &StoredWorkflowDefinition {
                    workflow_id: "orders".to_owned(),
                    workflow_version: 1,
                    definition: definition.clone(),
                    content_hash: "publication-envelope-hash".to_owned(),
                    created_at: 1,
                    published_at: Some(1),
                },
                &metadata,
            )
            .await
            .expect("snapshotted definition should insert");

        let inspected = persistence
            .inspect_published_definition("orders", 1)
            .await
            .expect("inspection should succeed")
            .expect("published row should exist");
        assert_eq!(
            inspected.definition_json,
            serde_json::to_string(&definition).expect("definition should serialize")
        );
        assert_eq!(
            inspected.execution_metadata_json.as_deref(),
            Some(metadata.as_str())
        );
        assert_eq!(
            inspected.classification,
            Some(StoredDefinitionClassification::SnapshottedV1)
        );
    }

    #[tokio::test]
    async fn pre_snapshot_definition_is_inspectable_and_exact_but_not_new_run_eligible() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let persistence = HiqlitePersistence::for_test(tempdir.path())
            .await
            .expect("persistence should start");
        let raw = current_definition_json("orders", 1, "orders.fetch");
        insert_raw_published_definition(&persistence, "orders", 1, &raw, None).await;

        let inspected = persistence
            .inspect_published_definition("orders", 1)
            .await
            .expect("inspection should succeed")
            .expect("row should exist");
        assert_eq!(
            inspected.classification,
            Some(StoredDefinitionClassification::PreSnapshot)
        );
        assert!(!inspected.new_run_eligible);
        assert!(persistence
            .load_published_definition_for_execution("orders", 1)
            .await
            .expect("exact pre-snapshot load should succeed")
            .is_some());
        assert_eq!(
            persistence
                .latest_eligible_published_definition("orders")
                .await
                .expect("selection should succeed"),
            LatestEligibleDefinitionSelection::NoEligiblePublishedDefinition
        );
    }

    #[tokio::test]
    async fn list_latest_eligible_is_deterministic_and_skips_newer_ineligible_rows_per_workflow() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let persistence = HiqlitePersistence::for_test(tempdir.path())
            .await
            .expect("persistence should start");

        for workflow_id in ["alpha", "beta"] {
            let current = current_definition_json(workflow_id, 1, "orders.fetch");
            let metadata = execution_metadata_json("run_task", "orders.fetch");
            insert_raw_published_definition(
                &persistence,
                workflow_id,
                1,
                &current,
                Some(&metadata),
            )
            .await;
        }
        let alpha_pre_snapshot = current_definition_json("alpha", 2, "orders.fetch");
        insert_raw_published_definition(&persistence, "alpha", 2, &alpha_pre_snapshot, None).await;
        insert_raw_published_definition(&persistence, "alpha", 3, "not-json", None).await;
        insert_raw_published_definition(&persistence, "beta", 2, "not-json", None).await;
        let gamma_pre_snapshot = current_definition_json("gamma", 1, "orders.fetch");
        insert_raw_published_definition(&persistence, "gamma", 1, &gamma_pre_snapshot, None).await;

        let selected = persistence
            .list_latest_eligible_published_definitions()
            .await
            .expect("eligible definitions should list");
        assert_eq!(
            selected
                .iter()
                .map(|definition| (
                    definition.executable().workflow_id.as_str(),
                    definition.executable().workflow_version,
                ))
                .collect::<Vec<_>>(),
            vec![("alpha", 1), ("beta", 1)]
        );

        let retained = persistence
            .inspect_published_definitions("alpha")
            .await
            .expect("retained alpha rows should remain inspectable");
        assert_eq!(
            retained
                .iter()
                .map(|row| row.workflow_version)
                .collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
    }

    #[tokio::test]
    async fn malformed_definition_is_inspectable_but_exact_execution_errors() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let persistence = HiqlitePersistence::for_test(tempdir.path())
            .await
            .expect("persistence should start");
        insert_raw_published_definition(&persistence, "broken", 1, "{", None).await;

        let inspected = persistence
            .inspect_published_definition("broken", 1)
            .await
            .expect("inspection should succeed")
            .expect("malformed row should remain visible");
        assert_eq!(inspected.definition_json, "{");
        assert_eq!(inspected.classification, None);
        assert!(!inspected.new_run_eligible);
        assert_eq!(
            inspected.decode_diagnostic.as_ref().map(|value| value.code),
            Some(StoredDefinitionDecodeDiagnosticCode::UnsupportedDefinition)
        );
        assert!(matches!(
            persistence.load_published_definition_for_execution("broken", 1).await,
            Err(PersistenceError::StoredDefinitionDecode { ref diagnostic_code, .. })
                if diagnostic_code == "unsupported_definition"
        ));
    }

    #[tokio::test]
    async fn embedded_workflow_id_mismatch_is_diagnostic_and_never_executable() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let persistence = HiqlitePersistence::for_test(tempdir.path())
            .await
            .expect("persistence should start");
        let raw = current_definition_json("embedded-orders", 1, "orders.fetch");
        let metadata = execution_metadata_json("run_task", "orders.fetch");
        insert_raw_published_definition(&persistence, "selected-orders", 1, &raw, Some(&metadata))
            .await;

        let inspected = persistence
            .inspect_published_definition("selected-orders", 1)
            .await
            .expect("inspection should succeed")
            .expect("mismatched row should remain visible");
        assert_eq!(
            inspected.decode_diagnostic.as_ref().map(|value| value.code),
            Some(StoredDefinitionDecodeDiagnosticCode::WorkflowIdMismatch)
        );
        assert!(matches!(
            persistence
                .load_published_definition_for_execution("selected-orders", 1)
                .await,
            Err(PersistenceError::StoredDefinitionDecode { ref diagnostic_code, .. })
                if diagnostic_code == "workflow_id_mismatch"
        ));
        assert_eq!(
            persistence
                .latest_eligible_published_definition("selected-orders")
                .await
                .expect("selection should succeed"),
            LatestEligibleDefinitionSelection::NoEligiblePublishedDefinition
        );
    }

    #[tokio::test]
    async fn embedded_workflow_version_mismatch_is_diagnostic_and_never_executable() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let persistence = HiqlitePersistence::for_test(tempdir.path())
            .await
            .expect("persistence should start");
        let raw = current_definition_json("orders", 1, "orders.fetch");
        let metadata = execution_metadata_json("run_task", "orders.fetch");
        insert_raw_published_definition(&persistence, "orders", 2, &raw, Some(&metadata)).await;

        let inspected = persistence
            .inspect_published_definition("orders", 2)
            .await
            .expect("inspection should succeed")
            .expect("mismatched row should remain visible");
        assert_eq!(
            inspected.decode_diagnostic.as_ref().map(|value| value.code),
            Some(StoredDefinitionDecodeDiagnosticCode::WorkflowVersionMismatch)
        );
        assert!(matches!(
            persistence
                .load_published_definition_for_execution("orders", 2)
                .await,
            Err(PersistenceError::StoredDefinitionDecode { ref diagnostic_code, .. })
                if diagnostic_code == "workflow_version_mismatch"
        ));
        assert_eq!(
            persistence
                .latest_eligible_published_definition("orders")
                .await
                .expect("selection should succeed"),
            LatestEligibleDefinitionSelection::NoEligiblePublishedDefinition
        );
    }

    #[tokio::test]
    async fn exact_execution_load_returns_none_only_when_the_row_is_missing() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let persistence = HiqlitePersistence::for_test(tempdir.path())
            .await
            .expect("persistence should start");

        assert!(persistence
            .load_published_definition_for_execution("missing", 7)
            .await
            .expect("missing row should not be a decode failure")
            .is_none());
    }

    #[tokio::test]
    async fn inspection_never_omits_rows_and_selection_distinguishes_empty_from_ineligible() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let persistence = HiqlitePersistence::for_test(tempdir.path())
            .await
            .expect("persistence should start");
        assert_eq!(
            persistence
                .latest_eligible_published_definition("missing")
                .await
                .expect("empty selection should succeed"),
            LatestEligibleDefinitionSelection::NoPublishedDefinitions
        );
        let pre_snapshot = current_definition_json("mixed", 1, "mixed.run");
        insert_raw_published_definition(&persistence, "mixed", 1, &pre_snapshot, None).await;
        let newer_pre_snapshot = current_definition_json("mixed", 2, "mixed.run");
        insert_raw_published_definition(&persistence, "mixed", 2, &newer_pre_snapshot, None).await;
        insert_raw_published_definition(&persistence, "mixed", 3, "not-json", None).await;

        let inspected = persistence
            .inspect_published_definitions("mixed")
            .await
            .expect("all retained rows should list");
        assert_eq!(
            inspected
                .iter()
                .map(|row| row.workflow_version)
                .collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
        assert_eq!(
            persistence
                .latest_eligible_published_definition("mixed")
                .await
                .expect("selection should succeed"),
            LatestEligibleDefinitionSelection::NoEligiblePublishedDefinition
        );
    }

    #[tokio::test]
    async fn recovery_work_class_scans_are_bounded_deterministic_and_progress() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let persistence = HiqlitePersistence::for_test(tempdir.path())
            .await
            .expect("persistence should start");
        apply_batch(
            &persistence.client,
            r#"
            WITH RECURSIVE seq(value) AS (
                SELECT 0 UNION ALL SELECT value + 1 FROM seq WHERE value < 100
            )
            INSERT INTO workflow_runs (
                run_id, workflow_id, workflow_version, status,
                cancel_reason, cancel_requested_by, cancel_requested_at,
                created_at, updated_at
            )
            SELECT printf('run-timeout-%03d', value), 'orders', 1, 'running',
                NULL, NULL, NULL, value, value
            FROM seq;

            WITH RECURSIVE seq(value) AS (
                SELECT 0 UNION ALL SELECT value + 1 FROM seq WHERE value < 100
            )
            INSERT INTO task_queue (
                task_id, run_id, workflow_id, workflow_version, step_id, step_kind,
                handler_id, capability, queue_name, input_json, timeout_at,
                continuation_state, status, attempt_sequence, active_attempt_id,
                active_fencing_token, leased_by_worker_id, created_at, updated_at
            )
            SELECT printf('task-timeout-%03d', value), printf('run-timeout-%03d', value),
                'orders', 1, printf('step-timeout-%03d', value), 'task',
                'orders.run', 'task', 'default', '{}', 20, NULL, 'queued', 0,
                NULL, NULL, NULL, value, value
            FROM seq;

            WITH RECURSIVE seq(value) AS (
                SELECT 0 UNION ALL SELECT value + 1 FROM seq WHERE value < 100
            )
            INSERT INTO task_queue (
                task_id, run_id, workflow_id, workflow_version, step_id, step_kind,
                handler_id, capability, queue_name, input_json, timeout_at,
                continuation_state, status, attempt_sequence, active_attempt_id,
                active_fencing_token, leased_by_worker_id, created_at, updated_at
            )
            SELECT printf('task-requeue-%03d', value), printf('run-requeue-%03d', value),
                'orders', 1, printf('step-requeue-%03d', value), 'task',
                'orders.run', 'task', 'default', '{}', NULL, NULL, 'leased', 1,
                printf('attempt-requeue-%03d', value), 1, 'worker-recovery', value, value
            FROM seq;

            WITH RECURSIVE seq(value) AS (
                SELECT 0 UNION ALL SELECT value + 1 FROM seq WHERE value < 100
            )
            INSERT INTO step_instances (
                step_instance_id, run_id, step_id, status, attempt_count,
                input_json, output_json, error_json, created_at, updated_at
            )
            SELECT printf('instance-continuation-%03d', value),
                printf('run-continuation-%03d', value),
                printf('step-continuation-%03d', value), 'completed', 1,
                NULL, printf('{"index":%d}', value), NULL, value, value
            FROM seq;

            WITH RECURSIVE seq(value) AS (
                SELECT 0 UNION ALL SELECT value + 1 FROM seq WHERE value < 100
            )
            INSERT INTO task_queue (
                task_id, run_id, workflow_id, workflow_version, step_id, step_kind,
                handler_id, capability, queue_name, input_json, timeout_at,
                continuation_state, status, attempt_sequence, active_attempt_id,
                active_fencing_token, leased_by_worker_id, created_at, updated_at
            )
            SELECT printf('task-continuation-%03d', value),
                printf('run-continuation-%03d', value), 'orders', 1,
                printf('step-continuation-%03d', value), 'task',
                'orders.run', 'task', 'default', '{}', NULL, 'pending', 'completed', 1,
                NULL, NULL, NULL, value, value
            FROM seq;

            WITH RECURSIVE seq(value) AS (
                SELECT 0 UNION ALL SELECT value + 1 FROM seq WHERE value < 100
            )
            INSERT INTO event_inbox (
                event_id, event_type, processing_state, payload_json, dedupe_key,
                created_at, updated_at, applied_at
            )
            SELECT printf('event-recovery-%03d', value), 'orders.ready', 'received',
                printf('{"index":%d}', value), NULL, value, value, NULL
            FROM seq;
            "#,
        )
        .await;

        let timed_out = persistence
            .list_timed_out_tasks(20)
            .await
            .expect("first timeout page should load");
        assert_eq!(timed_out.len(), RECOVERY_SCAN_BATCH_SIZE as usize);
        assert_eq!(timed_out.first().unwrap().task_id, "task-timeout-000");
        assert_eq!(timed_out.last().unwrap().task_id, "task-timeout-099");
        persistence
            .client
            .execute(
                "UPDATE task_queue SET status = 'timed_out' WHERE task_id BETWEEN 'task-timeout-000' AND 'task-timeout-099'",
                Vec::<Param>::new(),
            )
            .await
            .expect("first timeout page should advance");
        assert_eq!(
            persistence
                .list_timed_out_tasks(20)
                .await
                .expect("second timeout page should load")[0]
                .task_id,
            "task-timeout-100"
        );

        let requeued = persistence
            .requeue_expired_leases(20)
            .await
            .expect("first orphan lease page should requeue");
        assert_eq!(requeued.len(), RECOVERY_SCAN_BATCH_SIZE as usize);
        assert_eq!(requeued.first().unwrap().task_id, "task-requeue-000");
        assert_eq!(requeued.last().unwrap().task_id, "task-requeue-099");
        assert_eq!(
            persistence
                .requeue_expired_leases(21)
                .await
                .expect("second orphan lease page should requeue")[0]
                .task_id,
            "task-requeue-100"
        );

        let continuations = persistence
            .list_pending_completed_task_continuations(20)
            .await
            .expect("first continuation page should load");
        assert_eq!(continuations.len(), RECOVERY_SCAN_BATCH_SIZE as usize);
        assert_eq!(
            continuations.first().unwrap().task_id,
            "task-continuation-000"
        );
        assert_eq!(
            continuations.last().unwrap().task_id,
            "task-continuation-099"
        );
        persistence
            .client
            .execute(
                "UPDATE task_queue SET continuation_state = 'applied' WHERE task_id BETWEEN 'task-continuation-000' AND 'task-continuation-099'",
                Vec::<Param>::new(),
            )
            .await
            .expect("first continuation page should advance");
        assert_eq!(
            persistence
                .list_pending_completed_task_continuations(21)
                .await
                .expect("second continuation page should load")[0]
                .task_id,
            "task-continuation-100"
        );

        let received = persistence
            .list_received_events()
            .await
            .expect("first received-event page should load");
        assert_eq!(received.len(), RECOVERY_SCAN_BATCH_SIZE as usize);
        assert_eq!(received.first().unwrap().event_id, "event-recovery-000");
        assert_eq!(received.last().unwrap().event_id, "event-recovery-099");
        persistence
            .client
            .execute(
                "UPDATE event_inbox SET processing_state = 'applied' WHERE event_id BETWEEN 'event-recovery-000' AND 'event-recovery-099'",
                Vec::<Param>::new(),
            )
            .await
            .expect("first received-event page should advance");
        assert_eq!(
            persistence
                .list_received_events()
                .await
                .expect("second received-event page should load")[0]
                .event_id,
            "event-recovery-100"
        );
    }

    #[tokio::test]
    async fn requeue_expired_leases_recovers_leased_task_without_lease_row() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let persistence = HiqlitePersistence::for_test(tempdir.path())
            .await
            .expect("persistence should start");
        let now = 1_700_000_000;

        persistence
            .insert_run(&WorkflowRunRecord {
                run_id: "run-orphan".to_owned(),
                workflow_id: "orders".to_owned(),
                workflow_version: 1,
                status: "running".to_owned(),
                cancel_reason: None,
                cancel_requested_by: None,
                cancel_requested_at: None,
                created_at: now,
            })
            .await
            .expect("run should insert");
        persistence
            .client
            .execute(
                r#"
                INSERT INTO task_queue (
                    task_id,
                    run_id,
                    workflow_id,
                    workflow_version,
                    step_id,
                    step_kind,
                    handler_id,
                    capability,
                    queue_name,
                    input_json,
                    timeout_at,
                    status,
                    attempt_sequence,
                    active_attempt_id,
                    active_fencing_token,
                    leased_by_worker_id,
                    created_at,
                    updated_at
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, NULL, $11, $12, $13, $14, $15, $16, $16)
                "#,
                params!(
                    "task-orphan".to_owned(),
                    "run-orphan".to_owned(),
                    "orders".to_owned(),
                    1,
                    "reserve_inventory".to_owned(),
                    "task".to_owned(),
                    "orders.fetch".to_owned(),
                    "task".to_owned(),
                    "default".to_owned(),
                    "{}".to_owned(),
                    TASK_STATUS_LEASED,
                    1,
                    "attempt-1".to_owned(),
                    1,
                    "worker-1".to_owned(),
                    now
                ),
            )
            .await
            .expect("orphan leased task should insert");

        let requeued = persistence
            .requeue_expired_leases(now + 60)
            .await
            .expect("requeue should succeed");
        assert_eq!(requeued.len(), 1);
        assert_eq!(requeued[0].task_id, "task-orphan");

        let task = persistence
            .get_task_snapshot("task-orphan")
            .await
            .expect("task should load")
            .expect("task should exist");
        assert_eq!(task.status, TASK_STATUS_QUEUED);
        assert!(task.leased_by_worker_id.is_none());
    }

    #[tokio::test]
    async fn claim_enforces_worker_held_active_capacity_and_ignores_orphan_lease_rows() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let persistence = HiqlitePersistence::for_test(tempdir.path())
            .await
            .expect("persistence should start");
        insert_queued_poll_task(&persistence, "run-capacity", "task-first", "step-first", 1).await;
        persistence
            .client
            .execute(
                r#"
                INSERT INTO leases (
                    lease_id, task_id, attempt_id, worker_id, fencing_token,
                    expires_at, heartbeat_at
                ) VALUES ($1, $2, $3, $4, $5, $6, $7)
                "#,
                params!(
                    "orphan-lease".to_owned(),
                    "missing-task".to_owned(),
                    "attempt-orphan".to_owned(),
                    "worker-capacity".to_owned(),
                    1,
                    1_000,
                    1,
                ),
            )
            .await
            .expect("orphan legacy lease should insert");

        let poll = PollTask {
            worker_id: "worker-capacity".to_owned(),
            worker_name: "capacity-worker".to_owned(),
            queues: vec!["default".to_owned()],
            capabilities: vec!["task".to_owned()],
            max_inflight: 1,
        };
        let first = persistence
            .lease_next_task(&poll, 200, 100)
            .await
            .expect("first poll should succeed")
            .expect("orphan lease must not consume active capacity");
        assert_eq!(first.task_id, "task-first");
        assert_eq!(first.attempt_id, "attempt-1");
        assert_eq!(first.fencing_token, 1);

        insert_queued_poll_task(
            &persistence,
            "run-capacity",
            "task-second",
            "step-second",
            2,
        )
        .await;
        assert!(persistence
            .lease_next_task(&poll, 201, 101)
            .await
            .expect("capacity-limited poll should succeed")
            .is_none());

        let other_worker = PollTask {
            worker_id: "worker-independent".to_owned(),
            worker_name: "independent-worker".to_owned(),
            queues: vec!["default".to_owned()],
            capabilities: vec!["task".to_owned()],
            max_inflight: 1,
        };
        assert_eq!(
            persistence
                .lease_next_task(&other_worker, 201, 101)
                .await
                .expect("independent worker poll should succeed")
                .expect("worker capacity must be independent")
                .task_id,
            "task-second"
        );

        insert_queued_poll_task(
            &persistence,
            "run-capacity",
            "task-no-demand",
            "step-no-demand",
            3,
        )
        .await;
        let no_demand = PollTask {
            worker_id: "worker-no-demand".to_owned(),
            worker_name: "no-demand-worker".to_owned(),
            queues: vec!["default".to_owned()],
            capabilities: vec!["task".to_owned()],
            max_inflight: 0,
        };
        assert!(persistence
            .lease_next_task(&no_demand, 202, 102)
            .await
            .expect("zero-demand poll should succeed")
            .is_none());
        assert_eq!(
            atomic_task_state(&persistence, "task-no-demand")
                .await
                .status,
            TASK_STATUS_QUEUED
        );
        assert_eq!(
            persistence
                .lease_next_task(&poll, 300, 200)
                .await
                .expect("poll at the prior lease deadline should succeed")
                .expect("an expired exact lease must not consume active capacity")
                .task_id,
            "task-no-demand"
        );
    }

    #[tokio::test]
    async fn concurrent_claims_create_one_exact_task_lease_pair() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let persistence = HiqlitePersistence::for_test(tempdir.path())
            .await
            .expect("persistence should start");
        insert_queued_poll_task(
            &persistence,
            "run-concurrent-claim",
            "task-concurrent-claim",
            "step-concurrent-claim",
            1,
        )
        .await;
        let left = PollTask {
            worker_id: "worker-left".to_owned(),
            worker_name: "left".to_owned(),
            queues: vec!["default".to_owned()],
            capabilities: vec!["task".to_owned()],
            max_inflight: 1,
        };
        let right = PollTask {
            worker_id: "worker-right".to_owned(),
            worker_name: "right".to_owned(),
            ..left.clone()
        };

        let (left_result, right_result) = tokio::join!(
            persistence.lease_next_task(&left, 200, 100),
            persistence.lease_next_task(&right, 200, 100),
        );
        let claims = [left_result, right_result]
            .into_iter()
            .filter_map(|result| result.expect("concurrent poll should succeed"))
            .collect::<Vec<_>>();
        assert_eq!(claims.len(), 1);
        assert_eq!(claims[0].task_id, "task-concurrent-claim");
        assert_eq!(lease_count(&persistence, "run-concurrent-claim").await, 1);
        let task = atomic_task_state(&persistence, "task-concurrent-claim").await;
        assert_eq!(task.status, TASK_STATUS_LEASED);
        assert!(matches!(
            task.leased_by_worker_id.as_deref(),
            Some("worker-left" | "worker-right")
        ));
        let snapshot = persistence
            .get_task_snapshot("task-concurrent-claim")
            .await
            .expect("claimed task should load")
            .expect("claimed task should exist");
        assert_eq!(snapshot.active_attempt_id.as_deref(), Some("attempt-1"));
        assert_eq!(snapshot.active_fencing_token, Some(1));
        assert_eq!(claims[0].attempt_id, "attempt-1");
        assert_eq!(claims[0].fencing_token, 1);
        assert_eq!(
            persistence
                .heartbeat_lease(
                    &TaskMutationContext {
                        task_id: claims[0].task_id.clone(),
                        attempt_id: claims[0].attempt_id.clone(),
                        worker_id: task
                            .leased_by_worker_id
                            .expect("claimed task should retain its worker"),
                        fencing_token: claims[0].fencing_token,
                    },
                    201,
                    101,
                )
                .await
                .expect("exact claimed lease should heartbeat"),
            MutationApplyResult::Applied
        );
    }

    #[tokio::test]
    async fn concurrent_claims_by_one_worker_respect_max_inflight_one() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let persistence = HiqlitePersistence::for_test(tempdir.path())
            .await
            .expect("persistence should start");
        insert_queued_poll_task(
            &persistence,
            "run-same-worker-capacity",
            "task-same-worker-first",
            "step-same-worker-first",
            1,
        )
        .await;
        insert_queued_poll_task(
            &persistence,
            "run-same-worker-capacity",
            "task-same-worker-second",
            "step-same-worker-second",
            2,
        )
        .await;
        let poll = PollTask {
            worker_id: "worker-same".to_owned(),
            worker_name: "same-worker".to_owned(),
            queues: vec!["default".to_owned()],
            capabilities: vec!["task".to_owned()],
            max_inflight: 1,
        };
        let left_candidate = persistence
            .get_task_row("task-same-worker-first")
            .await
            .expect("left candidate should load")
            .expect("left candidate should exist");
        let right_candidate = persistence
            .get_task_row("task-same-worker-first")
            .await
            .expect("right candidate should load")
            .expect("right candidate should exist");

        let (left, right) = tokio::join!(
            persistence.claim_selected_task(&poll, left_candidate, 200, 100),
            persistence.claim_selected_task(&poll, right_candidate, 200, 100),
        );
        let claims = [left, right]
            .into_iter()
            .filter_map(|result| result.expect("concurrent poll should succeed"))
            .collect::<Vec<_>>();
        assert_eq!(claims.len(), 1);
        assert_eq!(claims[0].task_id, "task-same-worker-first");
        assert_eq!(
            lease_count(&persistence, "run-same-worker-capacity").await,
            1
        );
        assert_eq!(
            atomic_task_state(&persistence, "task-same-worker-first")
                .await
                .status,
            TASK_STATUS_LEASED
        );
        assert_eq!(
            atomic_task_state(&persistence, "task-same-worker-second")
                .await
                .status,
            TASK_STATUS_QUEUED
        );
    }

    #[tokio::test]
    async fn stale_selected_claim_loses_after_claim_expiry_and_requeue_without_fence_reuse() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let persistence = HiqlitePersistence::for_test(tempdir.path())
            .await
            .expect("persistence should start");
        insert_queued_poll_task(
            &persistence,
            "run-stale-selected-claim",
            "task-stale-selected-claim",
            "step-stale-selected-claim",
            1,
        )
        .await;
        let stale_row = persistence
            .get_task_row("task-stale-selected-claim")
            .await
            .expect("stale candidate should load")
            .expect("stale candidate should exist");
        let winner_poll = PollTask {
            worker_id: "worker-winner".to_owned(),
            worker_name: "winner".to_owned(),
            queues: vec!["default".to_owned()],
            capabilities: vec!["task".to_owned()],
            max_inflight: 1,
        };
        let first = persistence
            .lease_next_task(&winner_poll, 20, 10)
            .await
            .expect("winning claim should succeed")
            .expect("winning claim should assign");
        assert_eq!(first.attempt_id, "attempt-1");
        assert_eq!(first.fencing_token, 1);
        assert_eq!(
            persistence
                .requeue_expired_leases(20)
                .await
                .expect("expired first claim should requeue")
                .len(),
            1
        );

        let stale_poll = PollTask {
            worker_id: "worker-stale".to_owned(),
            worker_name: "stale".to_owned(),
            ..winner_poll.clone()
        };
        assert!(persistence
            .claim_selected_task(&stale_poll, stale_row, 100, 21)
            .await
            .expect("stale selected candidate should classify")
            .is_none());
        assert_eq!(
            lease_count(&persistence, "run-stale-selected-claim").await,
            0
        );
        let after_stale = persistence
            .get_task_snapshot("task-stale-selected-claim")
            .await
            .expect("task should load after stale claim")
            .expect("task should exist after stale claim");
        assert_eq!(after_stale.status, TASK_STATUS_QUEUED);
        assert_eq!(after_stale.attempt_sequence, 1);
        assert_eq!(after_stale.active_attempt_id.as_deref(), Some("attempt-1"));
        assert_eq!(after_stale.active_fencing_token, Some(1));
        assert!(after_stale.leased_by_worker_id.is_none());

        let second = persistence
            .lease_next_task(&stale_poll, 100, 21)
            .await
            .expect("fresh selection should succeed")
            .expect("fresh selection should claim");
        assert_eq!(second.attempt_id, "attempt-2");
        assert_eq!(second.fencing_token, 2);
        assert_eq!(
            lease_count(&persistence, "run-stale-selected-claim").await,
            1
        );
    }

    #[tokio::test]
    async fn cancellation_after_selection_prevents_task_and_lease_claim_atomically() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let persistence = HiqlitePersistence::for_test(tempdir.path())
            .await
            .expect("persistence should start");
        insert_queued_poll_task(
            &persistence,
            "run-cancel-after-selection",
            "task-cancel-after-selection",
            "step-cancel-after-selection",
            1,
        )
        .await;
        let stale_row = persistence
            .get_task_row("task-cancel-after-selection")
            .await
            .expect("selected candidate should load")
            .expect("selected candidate should exist");
        assert_eq!(
            persistence
                .cancel_run_atomically(
                    "run-cancel-after-selection",
                    Some("cancelled after poll selection"),
                    Some("test"),
                    10,
                )
                .await
                .expect("cancellation should apply"),
            MutationApplyResult::Applied
        );
        let poll = PollTask {
            worker_id: "worker-too-late".to_owned(),
            worker_name: "too-late".to_owned(),
            queues: vec!["default".to_owned()],
            capabilities: vec!["task".to_owned()],
            max_inflight: 1,
        };
        assert!(persistence
            .claim_selected_task(&poll, stale_row, 100, 11)
            .await
            .expect("stale post-cancellation claim should classify")
            .is_none());
        assert_eq!(
            persistence
                .get_run("run-cancel-after-selection")
                .await
                .expect("cancelled run should load")
                .expect("cancelled run should exist")
                .status,
            RUN_STATUS_CANCELLED
        );
        let task = persistence
            .get_task_snapshot("task-cancel-after-selection")
            .await
            .expect("cancelled task should load")
            .expect("cancelled task should exist");
        assert_eq!(task.status, TASK_STATUS_CANCELLED);
        assert!(task.active_attempt_id.is_none());
        assert!(task.active_fencing_token.is_none());
        assert!(task.leased_by_worker_id.is_none());
        assert_eq!(
            lease_count(&persistence, "run-cancel-after-selection").await,
            0
        );
    }

    #[tokio::test]
    async fn claim_quarantines_invalid_stored_numeric_state_without_leasing_or_hot_looping() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let persistence = HiqlitePersistence::for_test(tempdir.path())
            .await
            .expect("persistence should start");
        for (task_id, step_id, created_at) in [
            ("task-invalid-version", "step-invalid-version", 1),
            ("task-invalid-attempt", "step-invalid-attempt", 2),
            ("task-invalid-fence", "step-invalid-fence", 3),
        ] {
            insert_queued_poll_task(
                &persistence,
                "run-invalid-claim",
                task_id,
                step_id,
                created_at,
            )
            .await;
        }
        persistence
            .client
            .execute(
                "UPDATE task_queue SET workflow_version = -1 WHERE task_id = 'task-invalid-version'",
                Vec::<Param>::new(),
            )
            .await
            .expect("invalid version should install");
        persistence
            .client
            .execute(
                "UPDATE task_queue SET attempt_sequence = $1 WHERE task_id = 'task-invalid-attempt'",
                params!(i64::MAX),
            )
            .await
            .expect("invalid attempt sequence should install");
        persistence
            .client
            .execute(
                "UPDATE task_queue SET active_fencing_token = $1 WHERE task_id = 'task-invalid-fence'",
                params!(i64::MAX),
            )
            .await
            .expect("invalid fencing token should install");
        let poll = PollTask {
            worker_id: "worker-invalid".to_owned(),
            worker_name: "invalid-state-worker".to_owned(),
            queues: vec!["default".to_owned()],
            capabilities: vec!["task".to_owned()],
            max_inflight: 1,
        };

        for _ in 0..2 {
            assert!(persistence
                .lease_next_task(&poll, 200, 100)
                .await
                .expect("invalid-state scan should succeed")
                .is_none());
        }
        for (task_id, code) in [
            ("task-invalid-version", "invalid_workflow_version"),
            ("task-invalid-attempt", "invalid_attempt_sequence"),
            ("task-invalid-fence", "invalid_fencing_token"),
        ] {
            let task = atomic_task_state(&persistence, task_id).await;
            assert_eq!(task.status, TASK_STATUS_QUEUED);
            let expected_marker = format!("quarantined:{code}");
            assert_eq!(
                task.continuation_state.as_deref(),
                Some(expected_marker.as_str())
            );
        }
        assert_eq!(lease_count(&persistence, "run-invalid-claim").await, 0);
        assert_eq!(
            persistence
                .list_transitions("run-invalid-claim")
                .await
                .expect("quarantine transitions should load")
                .into_iter()
                .filter(|transition| transition.transition_type == "task_execution_quarantined")
                .count(),
            3
        );
        assert!(persistence
            .run_has_open_work("run-invalid-claim")
            .await
            .expect("quarantine should block idle completion"));
        assert_eq!(
            persistence
                .cancel_run_atomically("run-invalid-claim", None, Some("test"), 101)
                .await
                .expect("cancellation should suppress quarantines"),
            MutationApplyResult::Applied
        );
        assert!(!persistence
            .run_has_open_work("run-invalid-claim")
            .await
            .expect("suppressed quarantine should not remain open"));
    }

    #[derive(Debug, Deserialize)]
    struct AtomicTaskStateRow {
        status: String,
        continuation_state: Option<String>,
        leased_by_worker_id: Option<String>,
    }

    #[derive(Debug, Deserialize)]
    struct AtomicTimerStateRow {
        timer_id: String,
        due_at: i64,
        status: String,
        effect_state: Option<String>,
        payload_json: Option<String>,
    }

    #[derive(Debug, Deserialize)]
    struct CountRow {
        row_count: i64,
    }

    #[tokio::test]
    async fn completion_commits_output_continuation_transition_and_lease_release_atomically() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let persistence = HiqlitePersistence::for_test(tempdir.path())
            .await
            .expect("persistence should start");
        let lease = insert_leased_atomic_task(
            &persistence,
            "run-complete",
            "task-complete",
            "step-complete",
            Some(200),
            10,
        )
        .await;
        let output = TaskOutput {
            payload: json!({"order_id": "ord-1", "ok": true}),
        };

        assert_eq!(
            persistence
                .complete_task_with_continuation(&lease, &output, 100)
                .await
                .expect("completion should commit"),
            TaskOutcomeApplyResult::Applied
        );

        let task = atomic_task_state(&persistence, &lease.task_id).await;
        assert_eq!(task.status, TASK_STATUS_COMPLETED);
        assert_eq!(
            task.continuation_state.as_deref(),
            Some(CONTINUATION_STATE_PENDING)
        );
        let step = persistence
            .get_step_instance("run-complete", "step-complete")
            .await
            .expect("step query should succeed")
            .expect("step should exist");
        assert_eq!(step.status, TASK_STATUS_COMPLETED);
        assert_eq!(step.output, Some(output.payload.clone()));
        assert_eq!(lease_count(&persistence, "run-complete").await, 0);

        assert_eq!(
            persistence
                .complete_task_with_continuation(&lease, &output, 100)
                .await
                .expect("stable completion should reapply"),
            TaskOutcomeApplyResult::AlreadyApplied
        );
        let transitions = persistence
            .list_transitions("run-complete")
            .await
            .expect("transitions should load");
        assert_eq!(transitions.len(), 1);
        assert_eq!(
            transitions[0].transition_id,
            "task-completed:13:task-complete:9:attempt-1:2:41"
        );
        assert_eq!(transitions[0].transition_type, "task_completed");
    }

    #[tokio::test]
    async fn length_delimited_outcome_ids_do_not_alias_colon_bearing_task_and_attempt_ids() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let persistence = HiqlitePersistence::for_test(tempdir.path())
            .await
            .expect("persistence should start");
        let left = insert_leased_atomic_task_with_attempt(
            &persistence,
            "run-id-collision",
            "a:b",
            "step-left",
            "c",
            Some(200),
            10,
        )
        .await;
        let right = insert_leased_atomic_task_with_attempt(
            &persistence,
            "run-id-collision",
            "a",
            "step-right",
            "b:c",
            Some(200),
            10,
        )
        .await;

        let left_transition_id = stable_task_transition_id("completed", &left);
        let right_transition_id = stable_task_transition_id("completed", &right);
        assert_ne!(left_transition_id, right_transition_id);
        assert_ne!(
            stable_task_timer_id("retry", &left),
            stable_task_timer_id("retry", &right)
        );
        assert_ne!(
            stable_task_timeout_transition_id(&left.task_id),
            stable_task_timeout_transition_id(&right.task_id)
        );
        assert_ne!(
            stable_run_failure_transition_id("a:b", "c"),
            stable_run_failure_transition_id("a", "b:c")
        );

        for lease in [&left, &right] {
            assert_eq!(
                persistence
                    .complete_task_with_continuation(
                        lease,
                        &TaskOutput {
                            payload: json!({"task_id": lease.task_id}),
                        },
                        100,
                    )
                    .await
                    .expect("completion should commit"),
                TaskOutcomeApplyResult::Applied,
                "the second ambiguous legacy tuple must not be mistaken for a reapply"
            );
        }
        let transition_ids = persistence
            .list_transitions("run-id-collision")
            .await
            .expect("transitions should load")
            .into_iter()
            .map(|transition| transition.transition_id)
            .collect::<std::collections::HashSet<_>>();
        assert_eq!(transition_ids.len(), 2);
        assert!(transition_ids.contains(&left_transition_id));
        assert!(transition_ids.contains(&right_transition_id));
    }

    #[tokio::test]
    async fn retry_failure_commits_state_error_and_deterministic_clamped_timer_atomically() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let persistence = HiqlitePersistence::for_test(tempdir.path())
            .await
            .expect("persistence should start");
        let lease = insert_leased_atomic_task(
            &persistence,
            "run-retry",
            "task-retry",
            "step-retry",
            Some(150),
            10,
        )
        .await;
        let failure = json!({"code": "TEMPORARY", "retryable": true});

        assert_eq!(
            persistence
                .fail_task_and_schedule_retry(&lease, &failure, 175, 100)
                .await
                .expect("retry should commit"),
            TaskOutcomeApplyResult::Applied
        );

        let task = atomic_task_state(&persistence, &lease.task_id).await;
        assert_eq!(task.status, TASK_STATUS_RETRY_PENDING);
        assert!(task.leased_by_worker_id.is_none());
        let step = persistence
            .get_step_instance("run-retry", "step-retry")
            .await
            .expect("step query should succeed")
            .expect("step should exist");
        assert_eq!(step.status, TASK_STATUS_RETRY_PENDING);
        assert_eq!(step.attempt_count, 1);
        assert_eq!(step.error, Some(failure));
        let timer = atomic_timer_state(&persistence, "run-retry").await;
        assert_eq!(
            timer.timer_id,
            "task-retry-timer:10:task-retry:9:attempt-1:2:41"
        );
        assert_eq!(timer.due_at, 150);
        assert_eq!(timer.status, TIMER_STATUS_PENDING);
        assert_eq!(
            timer.effect_state.as_deref(),
            Some(TIMER_EFFECT_STATE_PENDING)
        );
        assert_eq!(
            timer
                .payload_json
                .as_deref()
                .map(serde_json::from_str::<serde_json::Value>)
                .transpose()
                .expect("timer payload should decode"),
            Some(json!({"task_id": "task-retry", "attempt_id": "attempt-1"}))
        );
        assert_eq!(lease_count(&persistence, "run-retry").await, 0);
        assert_eq!(
            persistence
                .fail_task_and_schedule_retry(
                    &lease,
                    &json!({"code": "TEMPORARY", "retryable": true}),
                    175,
                    100,
                )
                .await
                .expect("stable retry should reapply"),
            TaskOutcomeApplyResult::AlreadyApplied
        );
        assert_eq!(timer_count(&persistence, "run-retry").await, 1);
    }

    #[tokio::test]
    async fn terminal_failure_cleans_sibling_work_leases_timers_and_continuations_atomically() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let persistence = HiqlitePersistence::for_test(tempdir.path())
            .await
            .expect("persistence should start");
        let lease = insert_leased_atomic_task(
            &persistence,
            "run-fail",
            "task-fail",
            "step-fail",
            Some(200),
            10,
        )
        .await;
        insert_leased_atomic_task(
            &persistence,
            "run-fail",
            "task-sibling",
            "step-sibling",
            Some(200),
            10,
        )
        .await;
        insert_completed_pending_continuation(&persistence, "run-fail", 10).await;
        insert_pending_timer(&persistence, "run-fail", "step-sibling", 120, 10).await;
        let failure = json!({"code": "PERMANENT", "retryable": false});

        assert_eq!(
            persistence
                .fail_task_terminal(&lease, &failure, 100)
                .await
                .expect("terminal failure should commit"),
            TaskOutcomeApplyResult::Applied
        );

        assert_eq!(
            persistence
                .get_run("run-fail")
                .await
                .expect("run query should succeed")
                .expect("run should exist")
                .status,
            RUN_STATUS_FAILED
        );
        assert_eq!(
            atomic_task_state(&persistence, "task-fail").await.status,
            TASK_STATUS_FAILED
        );
        assert_eq!(
            atomic_task_state(&persistence, "task-sibling").await.status,
            TASK_STATUS_FAILED
        );
        let completed = atomic_task_state(&persistence, "task-completed-before-fail").await;
        assert_eq!(completed.status, TASK_STATUS_COMPLETED);
        assert_eq!(
            completed.continuation_state.as_deref(),
            Some(CONTINUATION_STATE_SUPPRESSED)
        );
        assert_eq!(lease_count(&persistence, "run-fail").await, 0);
        let timer = atomic_timer_state(&persistence, "run-fail").await;
        assert_eq!(timer.status, TIMER_STATUS_CANCELLED);
        assert_eq!(
            timer.effect_state.as_deref(),
            Some(TIMER_EFFECT_STATE_SUPPRESSED)
        );
        let transitions = persistence
            .list_transitions("run-fail")
            .await
            .expect("transitions should load");
        assert_eq!(transitions.len(), 2);
        assert!(transitions
            .iter()
            .any(|transition| transition.transition_type == "task_failed"));
        assert!(transitions
            .iter()
            .any(|transition| transition.transition_type == "run_failed"));
    }

    #[tokio::test]
    async fn completion_at_deadline_is_expired_and_timeout_owns_equality() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let persistence = HiqlitePersistence::for_test(tempdir.path())
            .await
            .expect("persistence should start");
        let lease = insert_leased_atomic_task(
            &persistence,
            "run-at-t",
            "task-at-t",
            "step-at-t",
            Some(100),
            10,
        )
        .await;

        assert_eq!(
            persistence
                .complete_task_with_continuation(
                    &lease,
                    &TaskOutput {
                        payload: json!({"too_late": true}),
                    },
                    100,
                )
                .await
                .expect("completion should classify"),
            TaskOutcomeApplyResult::Expired
        );
        assert_eq!(
            persistence
                .expire_task_if_due(&lease.task_id, 100)
                .await
                .expect("timeout should commit"),
            TaskOutcomeApplyResult::Applied
        );
        assert_eq!(
            atomic_task_state(&persistence, &lease.task_id).await.status,
            TASK_STATUS_TIMED_OUT
        );
    }

    #[tokio::test]
    async fn expired_lease_at_equality_cannot_heartbeat_complete_fail_or_quarantine() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let persistence = HiqlitePersistence::for_test(tempdir.path())
            .await
            .expect("persistence should start");
        let lease = insert_leased_atomic_task(
            &persistence,
            "run-expired-lease",
            "task-expired-lease",
            "step-expired-lease",
            Some(200),
            10,
        )
        .await;
        persistence
            .client
            .execute(
                "UPDATE leases SET expires_at = 40 WHERE task_id = $1",
                params!(lease.task_id.clone()),
            )
            .await
            .expect("lease expiry boundary should install");
        let failure = FailurePayload {
            classification: "application".to_owned(),
            code: "WORKER_REPORTED_FAILURE".to_owned(),
            message: "expired worker result".to_owned(),
            retryable: true,
            details: json!({"expired": true}),
        };
        let failure_value = serde_json::to_value(&failure).expect("failure should serialize");

        assert_eq!(
            persistence
                .heartbeat_lease(&lease, 100, 40)
                .await
                .expect("heartbeat at lease expiry should classify"),
            MutationApplyResult::Stale
        );
        assert_eq!(
            persistence
                .complete_task_with_continuation(
                    &lease,
                    &TaskOutput {
                        payload: json!({"too_late": true}),
                    },
                    40,
                )
                .await
                .expect("completion at lease expiry should classify"),
            TaskOutcomeApplyResult::Stale
        );
        assert_eq!(
            persistence
                .fail_task_and_schedule_retry(&lease, &failure_value, 75, 40)
                .await
                .expect("retry failure at lease expiry should classify"),
            TaskOutcomeApplyResult::Stale
        );
        assert_eq!(
            persistence
                .fail_task_terminal(&lease, &failure_value, 40)
                .await
                .expect("terminal failure at lease expiry should classify"),
            TaskOutcomeApplyResult::Stale
        );
        assert_eq!(
            persistence
                .quarantine_failed_task_execution(
                    &lease,
                    &failure,
                    "pinned_definition_missing",
                    40,
                )
                .await
                .expect("quarantine at lease expiry should classify"),
            TaskExecutionQuarantineResult::Stale
        );

        assert_eq!(
            atomic_task_state(&persistence, &lease.task_id).await.status,
            TASK_STATUS_LEASED
        );
        assert_eq!(lease_count(&persistence, "run-expired-lease").await, 1);
        assert_eq!(timer_count(&persistence, "run-expired-lease").await, 0);
        assert!(persistence
            .list_transitions("run-expired-lease")
            .await
            .expect("transitions should load")
            .is_empty());
        let step = persistence
            .get_step_instance("run-expired-lease", "step-expired-lease")
            .await
            .expect("step should load")
            .expect("step should exist");
        assert_eq!(step.status, TASK_STATUS_LEASED);
        assert!(step.error.is_none());

        assert_eq!(
            persistence
                .requeue_expired_leases(40)
                .await
                .expect("recovery should own lease-expiry equality")
                .len(),
            1
        );
        let poll = PollTask {
            worker_id: lease.worker_id.clone(),
            worker_name: "replacement-attempt".to_owned(),
            queues: vec!["default".to_owned()],
            capabilities: vec!["task".to_owned()],
            max_inflight: 1,
        };
        let replacement = persistence
            .lease_next_task(&poll, 80, 41)
            .await
            .expect("replacement poll should succeed")
            .expect("recovered task should be claimable");
        assert_eq!(replacement.attempt_id, "attempt-2");
        assert_eq!(replacement.fencing_token, 42);
        assert_eq!(
            persistence
                .complete_task_with_continuation(
                    &lease,
                    &TaskOutput {
                        payload: json!({"stale_attempt": true}),
                    },
                    41,
                )
                .await
                .expect("old attempt should remain fenced"),
            TaskOutcomeApplyResult::Stale
        );
    }

    #[tokio::test]
    async fn completion_before_deadline_wins_and_later_timeout_is_stale() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let persistence = HiqlitePersistence::for_test(tempdir.path())
            .await
            .expect("persistence should start");
        let lease = insert_leased_atomic_task(
            &persistence,
            "run-before-t",
            "task-before-t",
            "step-before-t",
            Some(100),
            10,
        )
        .await;

        assert_eq!(
            persistence
                .complete_task_with_continuation(
                    &lease,
                    &TaskOutput {
                        payload: json!({"on_time": true}),
                    },
                    99,
                )
                .await
                .expect("completion should commit"),
            TaskOutcomeApplyResult::Applied
        );
        assert_eq!(
            persistence
                .expire_task_if_due(&lease.task_id, 100)
                .await
                .expect("timeout should classify"),
            TaskOutcomeApplyResult::Stale
        );
        assert_eq!(
            atomic_task_state(&persistence, &lease.task_id).await.status,
            TASK_STATUS_COMPLETED
        );
        assert_eq!(
            persistence
                .get_run("run-before-t")
                .await
                .expect("run query should succeed")
                .expect("run should exist")
                .status,
            RUN_STATUS_RUNNING
        );
    }

    #[tokio::test]
    async fn timeout_then_late_completion_remains_terminal_and_reports_stale() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let persistence = HiqlitePersistence::for_test(tempdir.path())
            .await
            .expect("persistence should start");
        let lease = insert_leased_atomic_task(
            &persistence,
            "run-timeout-first",
            "task-timeout-first",
            "step-timeout-first",
            Some(100),
            10,
        )
        .await;

        assert_eq!(
            persistence
                .expire_task_if_due(&lease.task_id, 100)
                .await
                .expect("timeout should commit"),
            TaskOutcomeApplyResult::Applied
        );
        assert_eq!(
            persistence
                .complete_task_with_continuation(
                    &lease,
                    &TaskOutput {
                        payload: json!({"late": true}),
                    },
                    101,
                )
                .await
                .expect("late completion should classify"),
            TaskOutcomeApplyResult::Stale
        );
        assert_eq!(
            atomic_task_state(&persistence, &lease.task_id).await.status,
            TASK_STATUS_TIMED_OUT
        );
        assert_eq!(
            persistence
                .get_run("run-timeout-first")
                .await
                .expect("run query should succeed")
                .expect("run should exist")
                .status,
            RUN_STATUS_FAILED
        );
        assert_eq!(
            persistence
                .list_transitions("run-timeout-first")
                .await
                .expect("transitions should load")
                .len(),
            2
        );
    }

    #[tokio::test]
    async fn terminalized_task_past_deadline_with_stale_lease_is_stale_not_expired() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let persistence = HiqlitePersistence::for_test(tempdir.path())
            .await
            .expect("persistence should start");
        let lease = insert_leased_atomic_task(
            &persistence,
            "run-terminal-before-deadline",
            "task-terminal-before-deadline",
            "step-terminal-before-deadline",
            Some(100),
            10,
        )
        .await;

        assert_eq!(
            persistence
                .fail_task_terminal(
                    &lease,
                    &json!({"code": "PERMANENT", "retryable": false}),
                    99,
                )
                .await
                .expect("terminal failure should commit"),
            TaskOutcomeApplyResult::Applied
        );
        assert_eq!(
            persistence
                .complete_task_with_continuation(
                    &lease,
                    &TaskOutput {
                        payload: json!({"late": true}),
                    },
                    101,
                )
                .await
                .expect("stale completion should classify"),
            TaskOutcomeApplyResult::Stale
        );
    }

    #[tokio::test]
    async fn invalid_version_failure_quarantine_consumes_exact_lease_and_blocks_idle_until_cancelled(
    ) {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let persistence = HiqlitePersistence::for_test(tempdir.path())
            .await
            .expect("persistence should start");
        let lease = insert_leased_atomic_task(
            &persistence,
            "run-invalid-version-failure",
            "task-invalid-version-failure",
            "step-invalid-version-failure",
            Some(200),
            10,
        )
        .await;
        persistence
            .client
            .execute(
                "UPDATE task_queue SET workflow_version = $1 WHERE task_id = $2",
                params!(i64::from(u32::MAX) + 1, lease.task_id.clone(),),
            )
            .await
            .expect("legacy-invalid workflow version fixture should install");

        assert!(matches!(
            persistence.get_task_snapshot(&lease.task_id).await,
            Err(PersistenceError::InvalidStoredWorkflowVersion {
                entity_kind: "task",
                ref entity_id,
                workflow_version,
            }) if entity_id == &lease.task_id && workflow_version == i64::from(u32::MAX) + 1
        ));

        let failure = FailurePayload {
            classification: "application".to_owned(),
            code: "WORKER_REPORTED_FAILURE".to_owned(),
            message: "worker could not safely continue".to_owned(),
            retryable: true,
            details: json!({"attempted": true}),
        };
        let mut stale_lease = lease.clone();
        stale_lease.fencing_token += 1;
        assert_eq!(
            persistence
                .quarantine_failed_task_execution(
                    &stale_lease,
                    &failure,
                    "invalid_workflow_version",
                    100,
                )
                .await
                .expect("wrong fence should classify without mutation"),
            TaskExecutionQuarantineResult::Stale
        );
        assert_eq!(
            lease_count(&persistence, "run-invalid-version-failure").await,
            1
        );

        assert_eq!(
            persistence
                .quarantine_failed_task_execution(
                    &lease,
                    &failure,
                    "invalid_workflow_version",
                    100,
                )
                .await
                .expect("exact lease should quarantine"),
            TaskExecutionQuarantineResult::Applied
        );
        assert_eq!(
            persistence
                .quarantine_failed_task_execution(
                    &lease,
                    &failure,
                    "invalid_workflow_version",
                    101,
                )
                .await
                .expect("exact replay should classify"),
            TaskExecutionQuarantineResult::AlreadyApplied
        );

        let task = atomic_task_state(&persistence, &lease.task_id).await;
        assert_eq!(task.status, TASK_STATUS_FAILED);
        assert_eq!(
            task.continuation_state.as_deref(),
            Some("quarantined:invalid_workflow_version")
        );
        assert!(task.leased_by_worker_id.is_none());
        let step = persistence
            .get_step_instance(
                "run-invalid-version-failure",
                "step-invalid-version-failure",
            )
            .await
            .expect("step should load")
            .expect("step should exist");
        assert_eq!(step.status, TASK_STATUS_FAILED);
        assert_eq!(
            step.error,
            Some(serde_json::to_value(&failure).expect("failure should serialize"))
        );
        assert_eq!(
            lease_count(&persistence, "run-invalid-version-failure").await,
            0
        );
        assert_eq!(
            timer_count(&persistence, "run-invalid-version-failure").await,
            0
        );
        assert!(persistence
            .run_has_open_work("run-invalid-version-failure")
            .await
            .expect("manual-recovery quarantine should remain open"));
        assert_eq!(
            persistence
                .complete_run_if_running_and_idle("run-invalid-version-failure", 102)
                .await
                .expect("idle completion should classify"),
            MutationApplyResult::Stale
        );
        let transitions = persistence
            .list_transitions("run-invalid-version-failure")
            .await
            .expect("transitions should load");
        let quarantines = transitions
            .iter()
            .filter(|transition| transition.transition_type == "task_execution_quarantined")
            .collect::<Vec<_>>();
        assert_eq!(quarantines.len(), 1);
        let payload = quarantines[0]
            .payload
            .as_ref()
            .expect("quarantine transition should retain evidence");
        assert_eq!(payload["code"], "invalid_workflow_version");
        assert_eq!(payload["failure"], serde_json::to_value(&failure).unwrap());

        assert_eq!(
            persistence
                .cancel_run_atomically(
                    "run-invalid-version-failure",
                    Some("manual recovery cancelled"),
                    Some("test"),
                    103,
                )
                .await
                .expect("cancellation should suppress quarantine"),
            MutationApplyResult::Applied
        );
        assert_eq!(
            atomic_task_state(&persistence, &lease.task_id)
                .await
                .continuation_state
                .as_deref(),
            Some(CONTINUATION_STATE_SUPPRESSED)
        );
        assert!(!persistence
            .run_has_open_work("run-invalid-version-failure")
            .await
            .expect("suppressed quarantine should not remain open"));
        assert_eq!(
            persistence
                .quarantine_failed_task_execution(
                    &lease,
                    &failure,
                    "invalid_workflow_version",
                    104,
                )
                .await
                .expect("late replay after cancellation should classify"),
            TaskExecutionQuarantineResult::RunNotRunning {
                status: RUN_STATUS_CANCELLED.to_owned(),
            }
        );
    }

    async fn insert_queued_poll_task(
        persistence: &HiqlitePersistence,
        run_id: &str,
        task_id: &str,
        step_id: &str,
        now: i64,
    ) {
        persistence
            .insert_run_if_absent(&WorkflowRunRecord {
                run_id: run_id.to_owned(),
                workflow_id: "orders".to_owned(),
                workflow_version: 1,
                status: RUN_STATUS_RUNNING.to_owned(),
                cancel_reason: None,
                cancel_requested_by: None,
                cancel_requested_at: None,
                created_at: now,
            })
            .await
            .expect("poll run fixture should insert");
        persistence
            .enqueue_task(&QueuedTaskRecord {
                task_id: task_id.to_owned(),
                run_id: run_id.to_owned(),
                workflow_id: "orders".to_owned(),
                workflow_version: 1,
                step_id: step_id.to_owned(),
                step_kind: "task".to_owned(),
                handler_id: "orders.run".to_owned(),
                capability: "task".to_owned(),
                queue_name: "default".to_owned(),
                input: json!({"fixture": task_id}),
                attempt_sequence: 0,
                timeout_at: None,
                created_at: now,
            })
            .await
            .expect("poll task fixture should insert");
    }

    async fn insert_leased_atomic_task(
        persistence: &HiqlitePersistence,
        run_id: &str,
        task_id: &str,
        step_id: &str,
        timeout_at: Option<i64>,
        now: i64,
    ) -> TaskMutationContext {
        insert_leased_atomic_task_with_attempt(
            persistence,
            run_id,
            task_id,
            step_id,
            "attempt-1",
            timeout_at,
            now,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn insert_leased_atomic_task_with_attempt(
        persistence: &HiqlitePersistence,
        run_id: &str,
        task_id: &str,
        step_id: &str,
        attempt_id: &str,
        timeout_at: Option<i64>,
        now: i64,
    ) -> TaskMutationContext {
        persistence
            .insert_run_if_absent(&WorkflowRunRecord {
                run_id: run_id.to_owned(),
                workflow_id: "orders".to_owned(),
                workflow_version: 1,
                status: RUN_STATUS_RUNNING.to_owned(),
                cancel_reason: None,
                cancel_requested_by: None,
                cancel_requested_at: None,
                created_at: now,
            })
            .await
            .expect("run fixture should insert");
        persistence
            .insert_step_instance(&StepInstanceRecord {
                step_instance_id: format!("instance-{task_id}"),
                run_id: run_id.to_owned(),
                step_id: step_id.to_owned(),
                status: TASK_STATUS_LEASED.to_owned(),
                attempt_count: 1,
                input: Some(json!({"fixture": task_id})),
                output: None,
                error: None,
                created_at: now,
                updated_at: now,
            })
            .await
            .expect("step fixture should insert");
        persistence
            .enqueue_task(&QueuedTaskRecord {
                task_id: task_id.to_owned(),
                run_id: run_id.to_owned(),
                workflow_id: "orders".to_owned(),
                workflow_version: 1,
                step_id: step_id.to_owned(),
                step_kind: "task".to_owned(),
                handler_id: "orders.run".to_owned(),
                capability: "task".to_owned(),
                queue_name: "default".to_owned(),
                input: json!({"fixture": task_id}),
                attempt_sequence: 0,
                timeout_at,
                created_at: now,
            })
            .await
            .expect("task fixture should insert");
        let worker_id = format!("worker-{task_id}");
        persistence
            .client
            .execute(
                r#"
                UPDATE task_queue
                SET status = $1,
                    attempt_sequence = 1,
                    active_attempt_id = $2,
                    active_fencing_token = 41,
                    leased_by_worker_id = $3
                WHERE task_id = $4
                "#,
                params!(
                    TASK_STATUS_LEASED,
                    attempt_id.to_owned(),
                    worker_id.clone(),
                    task_id.to_owned(),
                ),
            )
            .await
            .expect("task fixture should lease");
        persistence
            .client
            .execute(
                r#"
                INSERT INTO leases (
                    lease_id, task_id, attempt_id, worker_id, fencing_token,
                    expires_at, heartbeat_at
                ) VALUES ($1, $2, $3, $4, 41, $5, $6)
                "#,
                params!(
                    format!("lease-{task_id}"),
                    task_id.to_owned(),
                    attempt_id.to_owned(),
                    worker_id.clone(),
                    now + 1_000,
                    now,
                ),
            )
            .await
            .expect("lease fixture should insert");

        TaskMutationContext {
            task_id: task_id.to_owned(),
            attempt_id: attempt_id.to_owned(),
            worker_id,
            fencing_token: 41,
        }
    }

    async fn insert_completed_pending_continuation(
        persistence: &HiqlitePersistence,
        run_id: &str,
        now: i64,
    ) {
        persistence
            .insert_step_instance(&StepInstanceRecord {
                step_instance_id: "instance-completed-before-fail".to_owned(),
                run_id: run_id.to_owned(),
                step_id: "step-completed-before-fail".to_owned(),
                status: TASK_STATUS_COMPLETED.to_owned(),
                attempt_count: 1,
                input: None,
                output: Some(json!({"saved": true})),
                error: None,
                created_at: now,
                updated_at: now,
            })
            .await
            .expect("completed step fixture should insert");
        persistence
            .enqueue_task(&QueuedTaskRecord {
                task_id: "task-completed-before-fail".to_owned(),
                run_id: run_id.to_owned(),
                workflow_id: "orders".to_owned(),
                workflow_version: 1,
                step_id: "step-completed-before-fail".to_owned(),
                step_kind: "task".to_owned(),
                handler_id: "orders.run".to_owned(),
                capability: "task".to_owned(),
                queue_name: "default".to_owned(),
                input: json!({}),
                attempt_sequence: 1,
                timeout_at: Some(200),
                created_at: now,
            })
            .await
            .expect("completed task fixture should insert");
        persistence
            .client
            .execute(
                r#"
                UPDATE task_queue
                SET status = $1, continuation_state = $2
                WHERE task_id = $3
                "#,
                params!(
                    TASK_STATUS_COMPLETED,
                    CONTINUATION_STATE_PENDING,
                    "task-completed-before-fail".to_owned(),
                ),
            )
            .await
            .expect("completed continuation fixture should update");
    }

    async fn insert_pending_timer(
        persistence: &HiqlitePersistence,
        run_id: &str,
        step_id: &str,
        due_at: i64,
        now: i64,
    ) {
        persistence
            .client
            .execute(
                r#"
                INSERT INTO timers (
                    timer_id, run_id, step_id, timer_kind, due_at, status,
                    payload_json, effect_state, created_at, updated_at
                ) VALUES ($1, $2, $3, $4, $5, $6, NULL, $7, $8, $8)
                "#,
                params!(
                    format!("fixture-timer-{run_id}"),
                    run_id.to_owned(),
                    step_id.to_owned(),
                    "retry".to_owned(),
                    due_at,
                    TIMER_STATUS_PENDING,
                    TIMER_EFFECT_STATE_PENDING,
                    now,
                ),
            )
            .await
            .expect("timer fixture should insert");
    }

    async fn atomic_task_state(
        persistence: &HiqlitePersistence,
        task_id: &str,
    ) -> AtomicTaskStateRow {
        persistence
            .client
            .query_as_optional::<AtomicTaskStateRow, _>(
                r#"
                SELECT status, continuation_state, leased_by_worker_id
                FROM task_queue
                WHERE task_id = $1
                "#,
                params!(task_id.to_owned()),
            )
            .await
            .expect("task state query should succeed")
            .expect("task should exist")
    }

    async fn atomic_timer_state(
        persistence: &HiqlitePersistence,
        run_id: &str,
    ) -> AtomicTimerStateRow {
        persistence
            .client
            .query_as_optional::<AtomicTimerStateRow, _>(
                r#"
                SELECT timer_id, due_at, status, effect_state, payload_json
                FROM timers
                WHERE run_id = $1
                "#,
                params!(run_id.to_owned()),
            )
            .await
            .expect("timer state query should succeed")
            .expect("timer should exist")
    }

    async fn lease_count(persistence: &HiqlitePersistence, run_id: &str) -> i64 {
        persistence
            .client
            .query_as_optional::<CountRow, _>(
                r#"
                SELECT COUNT(*) AS row_count
                FROM leases
                WHERE task_id IN (SELECT task_id FROM task_queue WHERE run_id = $1)
                "#,
                params!(run_id.to_owned()),
            )
            .await
            .expect("lease count should query")
            .expect("count row should exist")
            .row_count
    }

    async fn timer_count(persistence: &HiqlitePersistence, run_id: &str) -> i64 {
        persistence
            .client
            .query_as_optional::<CountRow, _>(
                "SELECT COUNT(*) AS row_count FROM timers WHERE run_id = $1",
                params!(run_id.to_owned()),
            )
            .await
            .expect("timer count should query")
            .expect("count row should exist")
            .row_count
    }

    async fn insert_raw_published_definition(
        persistence: &HiqlitePersistence,
        workflow_id: &str,
        workflow_version: u32,
        definition_json: &str,
        execution_metadata_json: Option<&str>,
    ) {
        persistence
            .client
            .execute(
                r#"
                INSERT INTO workflow_definitions (
                    workflow_id, workflow_version, status, definition_json, content_hash,
                    last_validated_content_hash, execution_metadata_json, created_at, published_at
                ) VALUES ($1, $2, $3, $4, $5, NULL, $6, $7, $7)
                "#,
                params!(
                    workflow_id.to_owned(),
                    workflow_version,
                    STATUS_PUBLISHED,
                    definition_json.to_owned(),
                    format!("{workflow_id}-v{workflow_version}"),
                    execution_metadata_json.map(ToOwned::to_owned),
                    workflow_version as i64
                ),
            )
            .await
            .expect("raw published definition should insert");
    }

    fn current_definition_json(workflow_id: &str, version: u32, handler: &str) -> String {
        let mut definition = supported_definition(workflow_id, handler);
        definition.workflow_version = version;
        serde_json::to_string(&definition).expect("current definition should serialize")
    }

    fn execution_metadata_json(step_id: &str, handler: &str) -> String {
        json!({
            "semantics_version": "v1",
            "task_routes": {
                (step_id): {
                    "handler": handler,
                    "capability": "task",
                    "queue": "default",
                    "timeout_seconds": null
                }
            }
        })
        .to_string()
    }

    fn supported_definition(workflow_id: &str, handler: &str) -> FlowDefinition {
        serde_json::from_value(json!({
            "workflow_id": workflow_id,
            "workflow_version": 1,
            "trigger": {
                "type": "event",
                "event_type": "order.created"
            },
            "defaults": {},
            "steps": [
                {
                    "id": "run_task",
                    "kind": "task",
                    "task": {
                        "handler": handler
                    }
                }
            ],
            "edges": []
        }))
        .expect("supported definition should deserialize")
    }
}

fn local_node_config(base_dir: &Path) -> NodeConfig {
    let api_addr = loopback_addr(free_port());
    let raft_addr = loopback_addr(free_port());
    NodeConfig {
        node_id: 1,
        nodes: vec![Node {
            id: 1,
            addr_raft: raft_addr.clone(),
            addr_api: api_addr.clone(),
        }],
        listen_addr_api: "127.0.0.1".into(),
        listen_addr_raft: "127.0.0.1".into(),
        data_dir: base_dir.join("hiqlite").display().to_string().into(),
        filename_db: "workflow-engine.db".into(),
        secret_api: "workflow-engine-api-secret".to_owned(),
        secret_raft: "workflow-engine-raft-secret".to_owned(),
        ..NodeConfig::default()
    }
}

fn txn_counts(results: Vec<Result<usize, hiqlite::Error>>) -> Result<Vec<usize>, PersistenceError> {
    results
        .into_iter()
        .map(|result| result.map_err(PersistenceError::Hiqlite))
        .collect()
}

fn sql_placeholders(start_index: usize, count: usize) -> String {
    (start_index..start_index + count)
        .map(|index| format!("${index}"))
        .collect::<Vec<_>>()
        .join(", ")
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("ephemeral port should bind")
        .local_addr()
        .expect("local addr should exist")
        .port()
}

fn loopback_addr(port: u16) -> String {
    format!("127.0.0.1:{port}")
}

fn parse_optional_json(value: Option<String>) -> Result<Option<Value>, PersistenceError> {
    value
        .map(|json| serde_json::from_str(&json).map_err(Into::into))
        .transpose()
}

fn json_text(value: Option<&Value>) -> Result<Option<String>, PersistenceError> {
    value
        .map(serde_json::to_string)
        .transpose()
        .map_err(Into::into)
}

fn event_inbox_conflict(
    event_id: &str,
    dedupe_key: Option<&str>,
    existing_event_id: &str,
) -> PersistenceError {
    PersistenceError::EventInboxConflict {
        event_id: event_id.to_owned(),
        dedupe_key: dedupe_key.map(ToOwned::to_owned),
        existing_event_id: existing_event_id.to_owned(),
    }
}

fn lease_matches(row: &TaskQueueRow, lease: &TaskMutationContext) -> bool {
    row.active_attempt_id.as_deref() == Some(lease.attempt_id.as_str())
        && row.active_fencing_token == Some(lease.fencing_token as i64)
        && row.leased_by_worker_id.as_deref() == Some(lease.worker_id.as_str())
}

fn stable_task_transition_id(operation: &str, lease: &TaskMutationContext) -> String {
    let fencing_token = lease.fencing_token.to_string();
    stable_outcome_id(
        &format!("task-{operation}"),
        &[&lease.task_id, &lease.attempt_id, &fencing_token],
    )
}

fn stable_task_timer_id(operation: &str, lease: &TaskMutationContext) -> String {
    let fencing_token = lease.fencing_token.to_string();
    stable_outcome_id(
        &format!("task-{operation}-timer"),
        &[&lease.task_id, &lease.attempt_id, &fencing_token],
    )
}

fn stable_task_timeout_transition_id(task_id: &str) -> String {
    stable_outcome_id("task-timed-out", &[task_id])
}

fn stable_timer_transition_id(operation: &str, timer_id: &str) -> String {
    stable_outcome_id(&format!("timer-{operation}"), &[timer_id])
}

fn stable_cancel_transition_id(operation: &str, run_id: &str) -> String {
    stable_outcome_id(&format!("run-cancel-{operation}"), &[run_id])
}

fn stable_run_completion_transition_id(run_id: &str) -> String {
    stable_outcome_id("run-completed", &[run_id])
}

fn stable_run_failure_transition_id(run_id: &str, cause_transition_id: &str) -> String {
    stable_outcome_id("run-failed", &[run_id, cause_transition_id])
}

fn stable_outcome_id(namespace: &str, components: &[&str]) -> String {
    use std::fmt::Write as _;

    let mut id = namespace.to_owned();
    for component in components {
        write!(&mut id, ":{}:{component}", component.len())
            .expect("writing a stable identifier into a String cannot fail");
    }
    id
}

fn validate_quarantine_code(stable_code: &str) -> Result<(), PersistenceError> {
    if !stable_code.is_empty()
        && stable_code.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'_' | b'-')
        })
    {
        return Ok(());
    }
    Err(PersistenceError::Invariant(format!(
        "quarantine code '{stable_code}' must be non-empty lowercase ASCII with only digits, '_' or '-'"
    )))
}

fn checked_stored_workflow_version(
    entity_kind: &'static str,
    entity_id: &str,
    workflow_version: i64,
) -> Result<u32, PersistenceError> {
    if workflow_version < 1 {
        return Err(PersistenceError::InvalidStoredWorkflowVersion {
            entity_kind,
            entity_id: entity_id.to_owned(),
            workflow_version,
        });
    }
    u32::try_from(workflow_version).map_err(|_| PersistenceError::InvalidStoredWorkflowVersion {
        entity_kind,
        entity_id: entity_id.to_owned(),
        workflow_version,
    })
}
