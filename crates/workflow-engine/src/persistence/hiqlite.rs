use std::{net::TcpListener, path::Path};

use hiqlite::{start_node, Client, Node, NodeConfig, Param};
use serde::Deserialize;
use serde_json::Value;
use thiserror::Error;
use workflow_definition::FlowDefinition;
use workflow_protocol::{PollTask, TaskMutationContext, TaskOutput};

use crate::{
    definition_store::{StoredWorkflowDefinition, WorkflowRunRecord},
    persistence::schema::WORKFLOW_ENGINE_SCHEMA,
    runtime::tasks::{TaskSnapshot, TimerRecord},
    runtime::{
        MutationApplyResult, QueuedTaskRecord, StepInstanceRecord, TaskLeaseRecord,
        TransitionRecord, WaiterRecord,
    },
};

const STATUS_DRAFT: &str = "draft";
const STATUS_PUBLISHED: &str = "published";
const TASK_STATUS_QUEUED: &str = "queued";
const TASK_STATUS_LEASED: &str = "leased";
const TASK_STATUS_COMPLETED: &str = "completed";
const TASK_STATUS_FAILED: &str = "failed";
const TASK_STATUS_RETRY_PENDING: &str = "retry_pending";

macro_rules! params {
    ( $( $param:expr ),* $(,)? ) => {
        vec![$(Param::from($param)),*]
    };
}

#[derive(Debug, Error)]
pub enum PersistenceError {
    #[error(transparent)]
    Hiqlite(#[from] hiqlite::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error("{0}")]
    Invariant(String),
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
        let next = self
            .client
            .query_as_optional::<VersionRow, _>(
                r#"
                SELECT MAX(workflow_version) + 1 AS next_workflow_version
                FROM workflow_definitions
                WHERE workflow_id = $1 AND status = $2
                "#,
                params!(workflow_id.to_owned(), STATUS_PUBLISHED),
            )
            .await?;

        Ok(next.and_then(|row| row.next_workflow_version).unwrap_or(1) as u32)
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
                    created_at,
                    published_at
                )
                VALUES ($1, $2, $3, $4, $5, NULL, $6, $7)
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

    pub async fn latest_published_definition(
        &self,
        workflow_id: &str,
    ) -> Result<Option<StoredWorkflowDefinition>, PersistenceError> {
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

        row.map(TryInto::try_into).transpose()
    }

    pub async fn list_published_versions(
        &self,
        workflow_id: &str,
    ) -> Result<Vec<StoredWorkflowDefinition>, PersistenceError> {
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
                    created_at,
                    published_at
                FROM workflow_definitions
                WHERE workflow_id = $1 AND status = $2
                ORDER BY workflow_version ASC
                "#,
                params!(workflow_id.to_owned(), STATUS_PUBLISHED),
            )
            .await?;

        rows.into_iter().map(TryInto::try_into).collect()
    }

    pub async fn list_latest_published_definitions(
        &self,
    ) -> Result<Vec<StoredWorkflowDefinition>, PersistenceError> {
        let rows = self
            .client
            .query_as::<WorkflowDefinitionRow, _>(
                r#"
                SELECT
                    wd.workflow_id,
                    wd.workflow_version,
                    wd.definition_json,
                    wd.content_hash,
                    wd.last_validated_content_hash,
                    wd.created_at,
                    wd.published_at
                FROM workflow_definitions wd
                INNER JOIN (
                    SELECT workflow_id, MAX(workflow_version) AS workflow_version
                    FROM workflow_definitions
                    WHERE status = $1
                    GROUP BY workflow_id
                ) latest
                    ON latest.workflow_id = wd.workflow_id
                    AND latest.workflow_version = wd.workflow_version
                WHERE wd.status = $1
                ORDER BY wd.workflow_id ASC
                "#,
                params!(STATUS_PUBLISHED),
            )
            .await?;

        rows.into_iter().map(TryInto::try_into).collect()
    }

    pub(crate) async fn get_published_definition(
        &self,
        workflow_id: &str,
        workflow_version: u32,
    ) -> Result<Option<StoredWorkflowDefinition>, PersistenceError> {
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

        row.map(TryInto::try_into).transpose()
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

        Ok(row.map(Into::into))
    }

    pub async fn update_run_status(
        &self,
        run_id: &str,
        status: &str,
        updated_at: i64,
    ) -> Result<(), PersistenceError> {
        self.client
            .execute(
                r#"
                UPDATE workflow_runs
                SET status = $1, updated_at = $2
                WHERE run_id = $3
                "#,
                params!(status.to_owned(), updated_at, run_id.to_owned()),
            )
            .await?;
        Ok(())
    }

    pub(crate) async fn cancel_run(
        &self,
        run_id: &str,
        reason: Option<&str>,
        requested_by: Option<&str>,
        now: i64,
    ) -> Result<bool, PersistenceError> {
        let updated = self
            .client
            .execute(
                r#"
                UPDATE workflow_runs
                SET status = $1,
                    cancel_reason = $2,
                    cancel_requested_by = $3,
                    cancel_requested_at = $4,
                    updated_at = $4
                WHERE run_id = $5
                    AND status NOT IN ($6, $7, $8, $9)
                "#,
                params!(
                    "cancel_requested".to_owned(),
                    reason.map(ToOwned::to_owned),
                    requested_by.map(ToOwned::to_owned),
                    now,
                    run_id.to_owned(),
                    "completed".to_owned(),
                    "failed".to_owned(),
                    "cancelled".to_owned(),
                    "cancel_requested".to_owned()
                ),
            )
            .await?;
        Ok(updated == 1)
    }

    pub async fn insert_step_instance(
        &self,
        step: &StepInstanceRecord,
    ) -> Result<(), PersistenceError> {
        self.client
            .execute(
                r#"
                INSERT INTO step_instances (
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
        Ok(())
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
                ORDER BY step_instances.created_at ASC
                "#,
                params!(run_id.to_owned()),
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

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn update_step_instance(
        &self,
        run_id: &str,
        step_id: &str,
        status: &str,
        attempt_count: Option<u32>,
        output: Option<&Value>,
        error: Option<&Value>,
        updated_at: i64,
    ) -> Result<(), PersistenceError> {
        let existing = self
            .get_step_instance(run_id, step_id)
            .await?
            .expect("step instance should exist before update");

        self.client
            .execute(
                r#"
                UPDATE step_instances
                SET status = $1,
                    attempt_count = $2,
                    output_json = $3,
                    error_json = $4,
                    updated_at = $5
                WHERE run_id = $6 AND step_id = $7
                "#,
                params!(
                    status.to_owned(),
                    attempt_count.unwrap_or(existing.attempt_count) as i64,
                    json_text(output)?,
                    json_text(error)?,
                    updated_at,
                    run_id.to_owned(),
                    step_id.to_owned()
                ),
            )
            .await?;
        Ok(())
    }

    pub async fn insert_waiter(&self, waiter: &WaiterRecord) -> Result<(), PersistenceError> {
        self.client
            .execute(
                r#"
                INSERT INTO waiters (
                    waiter_id,
                    run_id,
                    step_id,
                    event_type,
                    status,
                    timeout_at,
                    resume_event_id,
                    created_at,
                    updated_at
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                "#,
                params!(
                    waiter.waiter_id.clone(),
                    waiter.run_id.clone(),
                    waiter.step_id.clone(),
                    waiter.event_type.clone(),
                    waiter.status.clone(),
                    waiter.timeout_at,
                    waiter.resume_event_id.clone(),
                    waiter.created_at,
                    waiter.updated_at
                ),
            )
            .await?;
        Ok(())
    }

    pub async fn list_waiters(&self, run_id: &str) -> Result<Vec<WaiterRecord>, PersistenceError> {
        let rows = self
            .client
            .query_as::<WaiterRow, _>(
                r#"
                SELECT
                    waiter_id,
                    run_id,
                    step_id,
                    event_type,
                    status,
                    timeout_at,
                    resume_event_id,
                    created_at,
                    updated_at
                FROM waiters
                WHERE run_id = $1
                ORDER BY waiters.created_at ASC
                "#,
                params!(run_id.to_owned()),
            )
            .await?;

        Ok(rows.into_iter().map(Into::into).collect())
    }

    pub(crate) async fn list_matching_waiters(
        &self,
        event_type: &str,
        status: &str,
    ) -> Result<Vec<WaiterRecord>, PersistenceError> {
        let rows = self
            .client
            .query_as::<WaiterRow, _>(
                r#"
                SELECT
                    waiter_id,
                    run_id,
                    step_id,
                    event_type,
                    status,
                    timeout_at,
                    resume_event_id,
                    created_at,
                    updated_at
                FROM waiters
                WHERE event_type = $1 AND status = $2
                ORDER BY waiters.created_at ASC
                "#,
                params!(event_type.to_owned(), status.to_owned()),
            )
            .await?;

        Ok(rows.into_iter().map(Into::into).collect())
    }

    pub(crate) async fn get_waiter(
        &self,
        waiter_id: &str,
    ) -> Result<Option<WaiterRecord>, PersistenceError> {
        let row = self
            .client
            .query_as_optional::<WaiterRow, _>(
                r#"
                SELECT
                    waiter_id,
                    run_id,
                    step_id,
                    event_type,
                    status,
                    timeout_at,
                    resume_event_id,
                    created_at,
                    updated_at
                FROM waiters
                WHERE waiter_id = $1
                LIMIT 1
                "#,
                params!(waiter_id.to_owned()),
            )
            .await?;

        Ok(row.map(Into::into))
    }

    pub(crate) async fn mark_waiter_status(
        &self,
        waiter_id: &str,
        status: &str,
        resume_event_id: Option<&str>,
        updated_at: i64,
    ) -> Result<MutationApplyResult, PersistenceError> {
        let Some(existing) = self.get_waiter(waiter_id).await? else {
            return Ok(MutationApplyResult::Stale);
        };

        if existing.status == status {
            return Ok(MutationApplyResult::AlreadyApplied);
        }

        if existing.status != "waiting" {
            return Ok(MutationApplyResult::Stale);
        }

        self.client
            .execute(
                r#"
                UPDATE waiters
                SET status = $1,
                    resume_event_id = COALESCE($2, resume_event_id),
                    updated_at = $3
                WHERE waiter_id = $4
                "#,
                params!(
                    status.to_owned(),
                    resume_event_id.map(ToOwned::to_owned),
                    updated_at,
                    waiter_id.to_owned()
                ),
            )
            .await?;

        Ok(MutationApplyResult::Applied)
    }

    pub(crate) async fn insert_timer(&self, timer: &TimerRecord) -> Result<(), PersistenceError> {
        self.client
            .execute(
                r#"
                INSERT INTO timers (
                    timer_id,
                    run_id,
                    step_id,
                    timer_kind,
                    due_at,
                    status,
                    payload_json,
                    created_at,
                    updated_at
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                "#,
                params!(
                    timer.timer_id.clone(),
                    timer.run_id.clone(),
                    timer.step_id.clone(),
                    timer.timer_kind.clone(),
                    timer.due_at,
                    timer.status.clone(),
                    json_text(timer.payload.as_ref())?,
                    timer.created_at,
                    timer.updated_at
                ),
            )
            .await?;
        Ok(())
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
                    created_at,
                    updated_at
                FROM timers
                WHERE status = $1 AND due_at <= $2
                ORDER BY due_at ASC
                "#,
                params!("pending".to_owned(), now),
            )
            .await?;

        rows.into_iter().map(TryInto::try_into).collect()
    }

    pub(crate) async fn mark_timer_status(
        &self,
        timer_id: &str,
        status: &str,
        updated_at: i64,
    ) -> Result<MutationApplyResult, PersistenceError> {
        let row = self
            .client
            .query_as_optional::<TimerRow, _>(
                r#"
                SELECT
                    timer_id,
                    run_id,
                    step_id,
                    timer_kind,
                    due_at,
                    status,
                    payload_json,
                    created_at,
                    updated_at
                FROM timers
                WHERE timer_id = $1
                LIMIT 1
                "#,
                params!(timer_id.to_owned()),
            )
            .await?;
        let Some(existing) = row else {
            return Ok(MutationApplyResult::Stale);
        };
        if existing.status == status {
            return Ok(MutationApplyResult::AlreadyApplied);
        }
        if existing.status != "pending" {
            return Ok(MutationApplyResult::Stale);
        }
        self.client
            .execute(
                r#"
                UPDATE timers
                SET status = $1, updated_at = $2
                WHERE timer_id = $3
                "#,
                params!(status.to_owned(), updated_at, timer_id.to_owned()),
            )
            .await?;
        Ok(MutationApplyResult::Applied)
    }

    pub(crate) async fn cancel_timers_for_step(
        &self,
        run_id: &str,
        step_id: &str,
        timer_kind: &str,
        updated_at: i64,
    ) -> Result<(), PersistenceError> {
        self.client
            .execute(
                r#"
                UPDATE timers
                SET status = $1, updated_at = $2
                WHERE run_id = $3 AND step_id = $4 AND timer_kind = $5 AND status = $6
                "#,
                params!(
                    "cancelled".to_owned(),
                    updated_at,
                    run_id.to_owned(),
                    step_id.to_owned(),
                    timer_kind.to_owned(),
                    "pending".to_owned()
                ),
            )
            .await?;
        Ok(())
    }

    pub(crate) async fn cancel_pending_waiters(
        &self,
        run_id: &str,
        updated_at: i64,
    ) -> Result<(), PersistenceError> {
        self.client
            .execute(
                r#"
                UPDATE waiters
                SET status = $1, updated_at = $2
                WHERE run_id = $3 AND status = $4
                "#,
                params!(
                    "cancelled".to_owned(),
                    updated_at,
                    run_id.to_owned(),
                    "waiting".to_owned()
                ),
            )
            .await?;
        Ok(())
    }

    pub(crate) async fn cancel_pending_timers(
        &self,
        run_id: &str,
        updated_at: i64,
    ) -> Result<(), PersistenceError> {
        self.client
            .execute(
                r#"
                UPDATE timers
                SET status = $1, updated_at = $2
                WHERE run_id = $3 AND status = $4
                "#,
                params!(
                    "cancelled".to_owned(),
                    updated_at,
                    run_id.to_owned(),
                    "pending".to_owned()
                ),
            )
            .await?;
        Ok(())
    }

    pub async fn enqueue_task(&self, task: &QueuedTaskRecord) -> Result<(), PersistenceError> {
        self.client
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
                    status,
                    attempt_sequence,
                    active_attempt_id,
                    active_fencing_token,
                    leased_by_worker_id,
                    created_at,
                    updated_at
                )
                VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, NULL, NULL, NULL, $13, $13
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
                    TASK_STATUS_QUEUED,
                    task.attempt_sequence as i64,
                    task.created_at
                ),
            )
            .await?;
        Ok(())
    }

    pub async fn lease_next_task(
        &self,
        poll: &PollTask,
        lease_expiry: i64,
        leased_at: i64,
    ) -> Result<Option<TaskLeaseRecord>, PersistenceError> {
        if poll.queues.is_empty() || poll.capabilities.is_empty() {
            return Ok(None);
        }

        let queue_placeholders = sql_placeholders(3, poll.queues.len());
        let capability_placeholders =
            sql_placeholders(3 + poll.queues.len(), poll.capabilities.len());
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
                    t.status,
                    t.attempt_sequence,
                    t.active_attempt_id,
                    t.active_fencing_token,
                    t.leased_by_worker_id
                FROM task_queue t
                INNER JOIN workflow_runs r ON r.run_id = t.run_id
                WHERE t.status = $1
                    AND r.status = $2
                    AND t.queue_name IN ({queue_placeholders})
                    AND t.capability IN ({capability_placeholders})
                ORDER BY t.created_at ASC
                "#
        );
        let mut query_params = params!(TASK_STATUS_QUEUED, "running".to_owned());
        query_params.extend(poll.queues.iter().cloned().map(Param::from));
        query_params.extend(poll.capabilities.iter().cloned().map(Param::from));
        let queued_rows = self
            .client
            .query_as::<TaskQueueRow, _>(query, query_params)
            .await?;

        for row in queued_rows {
            let attempt_sequence = row.attempt_sequence + 1;
            let attempt_id = format!("attempt-{attempt_sequence}");
            let fencing_token = row.active_fencing_token.unwrap_or(0) + 1;

            let updated = self
                .client
                .execute(
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
                        "running".to_owned()
                    ),
                )
                .await?;

            if updated != 1 {
                continue;
            }

            self.client
                .execute(
                    r#"
                    INSERT OR REPLACE INTO leases (
                        lease_id,
                        task_id,
                        attempt_id,
                        worker_id,
                        fencing_token,
                        expires_at,
                        heartbeat_at
                    )
                    VALUES ($1, $2, $3, $4, $5, $6, $7)
                    "#,
                    params!(
                        format!("{}:{attempt_id}", row.task_id),
                        row.task_id.clone(),
                        attempt_id.clone(),
                        poll.worker_id.clone(),
                        fencing_token,
                        lease_expiry,
                        leased_at
                    ),
                )
                .await?;

            return Ok(Some(TaskLeaseRecord {
                task_id: row.task_id,
                run_id: row.run_id,
                workflow_id: row.workflow_id,
                workflow_version: row.workflow_version as u32,
                step_id: row.step_id,
                step_kind: row.step_kind,
                handler_id: row.handler_id,
                input: serde_json::from_str::<Value>(&row.input_json)?,
                attempt_id,
                fencing_token: fencing_token as u64,
            }));
        }

        Ok(None)
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

    pub async fn complete_task(
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

    pub async fn fail_task(
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

    pub(crate) async fn mark_task_retry_pending(
        &self,
        task_id: &str,
        updated_at: i64,
    ) -> Result<(), PersistenceError> {
        self.client
            .execute(
                r#"
                UPDATE task_queue
                SET status = $1, updated_at = $2
                WHERE task_id = $3
                "#,
                params!(TASK_STATUS_RETRY_PENDING, updated_at, task_id.to_owned()),
            )
            .await?;
        Ok(())
    }

    pub(crate) async fn requeue_task(
        &self,
        task_id: &str,
        updated_at: i64,
    ) -> Result<(), PersistenceError> {
        self.client
            .execute(
                r#"
                UPDATE task_queue
                SET status = $1,
                    leased_by_worker_id = NULL,
                    updated_at = $2
                WHERE task_id = $3
                "#,
                params!(TASK_STATUS_QUEUED, updated_at, task_id.to_owned()),
            )
            .await?;
        Ok(())
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
                    t.status,
                    t.attempt_sequence,
                    t.active_attempt_id,
                    t.active_fencing_token,
                    t.leased_by_worker_id
                FROM task_queue t
                INNER JOIN leases l
                    ON l.task_id = t.task_id
                    AND l.attempt_id = t.active_attempt_id
                    AND l.fencing_token = t.active_fencing_token
                WHERE t.status = $1 AND l.expires_at <= $2
                "#,
                params!(TASK_STATUS_LEASED, now),
            )
            .await?;

        let mut requeued = Vec::new();
        for row in rows {
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
                        "#,
                        params!(
                            TASK_STATUS_QUEUED,
                            now,
                            row.task_id.clone(),
                            TASK_STATUS_LEASED,
                            row.active_attempt_id.clone(),
                            row.active_fencing_token
                        ),
                    ),
                    (
                        r#"
                        DELETE FROM leases
                        WHERE task_id = $1 AND attempt_id = $2 AND fencing_token = $3
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
                [1, 1] => requeued.push(row.try_into()?),
                [0, 0] => {}
                _ => {
                    return Err(PersistenceError::Invariant(format!(
                        "requeue_expired_leases produced unexpected row counts: {counts:?}"
                    )))
                }
            }
        }

        Ok(requeued)
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

    pub(crate) async fn run_has_open_work(&self, run_id: &str) -> Result<bool, PersistenceError> {
        let rows = self
            .client
            .query_as::<TaskStatusRow, _>(
                r#"
                SELECT status
                FROM task_queue
                WHERE run_id = $1
                "#,
                params!(run_id.to_owned()),
            )
            .await?;

        Ok(rows.into_iter().any(|row| {
            row.status == TASK_STATUS_QUEUED
                || row.status == TASK_STATUS_LEASED
                || row.status == TASK_STATUS_RETRY_PENDING
        }) || self.has_pending_waiters(run_id).await?
            || self.has_pending_timers(run_id).await?)
    }

    pub(crate) async fn insert_event_received(
        &self,
        event_id: &str,
        event_type: &str,
        payload: &Value,
        dedupe_key: Option<&str>,
        now: i64,
    ) -> Result<(), PersistenceError> {
        self.client
            .execute(
                r#"
                INSERT INTO event_inbox (
                    event_id,
                    event_type,
                    processing_state,
                    payload_json,
                    dedupe_key,
                    created_at,
                    updated_at,
                    applied_at
                )
                VALUES ($1, $2, $3, $4, $5, $6, $6, NULL)
                "#,
                params!(
                    event_id.to_owned(),
                    event_type.to_owned(),
                    "received".to_owned(),
                    serde_json::to_string(payload)?,
                    dedupe_key.map(ToOwned::to_owned),
                    now
                ),
            )
            .await?;
        Ok(())
    }

    pub(crate) async fn find_event_by_dedupe_key(
        &self,
        dedupe_key: Option<&str>,
    ) -> Result<Option<EventInboxRecord>, PersistenceError> {
        let Some(dedupe_key) = dedupe_key else {
            return Ok(None);
        };
        let row = self
            .client
            .query_as_optional::<EventInboxRow, _>(
                r#"
                SELECT
                    event_id,
                    event_type,
                    processing_state,
                    payload_json
                FROM event_inbox
                WHERE dedupe_key = $1
                LIMIT 1
                "#,
                params!(dedupe_key.to_owned()),
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
                    payload_json
                FROM event_inbox
                WHERE processing_state = $1
                ORDER BY event_inbox.created_at ASC
                "#,
                params!("received".to_owned()),
            )
            .await?;

        rows.into_iter().map(TryInto::try_into).collect()
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
                WHERE event_id = $3
                "#,
                params!("applied".to_owned(), now, event_id.to_owned()),
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

    async fn has_pending_waiters(&self, run_id: &str) -> Result<bool, PersistenceError> {
        let rows = self
            .client
            .query_as::<TaskStatusRow, _>(
                r#"
                SELECT status
                FROM waiters
                WHERE run_id = $1
                "#,
                params!(run_id.to_owned()),
            )
            .await?;

        Ok(rows.into_iter().any(|row| row.status == "waiting"))
    }

    async fn has_pending_timers(&self, run_id: &str) -> Result<bool, PersistenceError> {
        let rows = self
            .client
            .query_as::<TaskStatusRow, _>(
                r#"
                SELECT status
                FROM timers
                WHERE run_id = $1
                "#,
                params!(run_id.to_owned()),
            )
            .await?;

        Ok(rows.into_iter().any(|row| row.status == "pending"))
    }
}

#[derive(Debug, Deserialize)]
struct WorkflowDefinitionRow {
    workflow_id: String,
    workflow_version: i64,
    definition_json: String,
    content_hash: String,
    last_validated_content_hash: Option<String>,
    created_at: i64,
    published_at: Option<i64>,
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

impl TryFrom<WorkflowDefinitionRow> for StoredWorkflowDefinition {
    type Error = PersistenceError;

    fn try_from(row: WorkflowDefinitionRow) -> Result<Self, Self::Error> {
        Ok(Self {
            workflow_id: row.workflow_id,
            workflow_version: row.workflow_version as u32,
            definition: serde_json::from_str(&row.definition_json)?,
            content_hash: row.content_hash,
            created_at: row.created_at,
            published_at: row.published_at,
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

impl From<WorkflowRunRow> for WorkflowRunRecord {
    fn from(row: WorkflowRunRow) -> Self {
        Self {
            run_id: row.run_id,
            workflow_id: row.workflow_id,
            workflow_version: row.workflow_version as u32,
            status: row.status,
            cancel_reason: row.cancel_reason,
            cancel_requested_by: row.cancel_requested_by,
            cancel_requested_at: row.cancel_requested_at,
            created_at: row.created_at,
        }
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
struct WaiterRow {
    waiter_id: String,
    run_id: String,
    step_id: String,
    event_type: String,
    status: String,
    timeout_at: Option<i64>,
    resume_event_id: Option<String>,
    created_at: i64,
    updated_at: i64,
}

impl From<WaiterRow> for WaiterRecord {
    fn from(row: WaiterRow) -> Self {
        Self {
            waiter_id: row.waiter_id,
            run_id: row.run_id,
            step_id: row.step_id,
            event_type: row.event_type,
            status: row.status,
            timeout_at: row.timeout_at,
            resume_event_id: row.resume_event_id,
            created_at: row.created_at,
            updated_at: row.updated_at,
        }
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
    created_at: i64,
    updated_at: i64,
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
}

#[derive(Debug, Deserialize)]
struct EventInboxRow {
    event_id: String,
    event_type: String,
    processing_state: String,
    payload_json: String,
}

impl TryFrom<EventInboxRow> for EventInboxRecord {
    type Error = PersistenceError;

    fn try_from(row: EventInboxRow) -> Result<Self, Self::Error> {
        Ok(Self {
            event_id: row.event_id,
            event_type: row.event_type,
            processing_state: row.processing_state,
            payload: serde_json::from_str(&row.payload_json)?,
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
    status: String,
    attempt_sequence: i64,
    active_attempt_id: Option<String>,
    active_fencing_token: Option<i64>,
    leased_by_worker_id: Option<String>,
}

impl TryFrom<TaskQueueRow> for TaskSnapshot {
    type Error = PersistenceError;

    fn try_from(row: TaskQueueRow) -> Result<Self, Self::Error> {
        Ok(Self {
            task_id: row.task_id,
            run_id: row.run_id,
            workflow_id: row.workflow_id,
            workflow_version: row.workflow_version as u32,
            step_id: row.step_id,
            step_kind: row.step_kind,
            handler_id: row.handler_id,
            capability: row.capability,
            queue_name: row.queue_name,
            input: serde_json::from_str::<Value>(&row.input_json)?,
            status: row.status,
            attempt_sequence: row.attempt_sequence as u32,
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

#[derive(Debug, Deserialize)]
struct VersionRow {
    next_workflow_version: Option<i64>,
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

fn lease_matches(row: &TaskQueueRow, lease: &TaskMutationContext) -> bool {
    row.active_attempt_id.as_deref() == Some(lease.attempt_id.as_str())
        && row.active_fencing_token == Some(lease.fencing_token as i64)
        && row.leased_by_worker_id.as_deref() == Some(lease.worker_id.as_str())
}
