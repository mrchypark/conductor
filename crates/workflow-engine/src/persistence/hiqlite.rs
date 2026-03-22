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
    runtime::tasks::{MutationApplyResult, QueuedTaskRecord, StepInstanceRecord, TaskLeaseRecord},
};

const STATUS_DRAFT: &str = "draft";
const STATUS_PUBLISHED: &str = "published";
const TASK_STATUS_QUEUED: &str = "queued";
const TASK_STATUS_LEASED: &str = "leased";
const TASK_STATUS_COMPLETED: &str = "completed";
const TASK_STATUS_FAILED: &str = "failed";

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

    pub async fn list_all_published_definitions(
        &self,
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
                WHERE status = $1
                ORDER BY workflow_id ASC, workflow_version DESC
                "#,
                params!(STATUS_PUBLISHED),
            )
            .await?;

        rows.into_iter().map(TryInto::try_into).collect()
    }

    pub async fn insert_run(&self, run: &WorkflowRunRecord) -> Result<(), PersistenceError> {
        self.client
            .execute(
                r#"
                INSERT INTO workflow_runs (
                    run_id,
                    workflow_id,
                    workflow_version,
                    status,
                    created_at,
                    updated_at
                )
                VALUES ($1, $2, $3, $4, $5, $5)
                "#,
                params!(
                    run.run_id.clone(),
                    run.workflow_id.clone(),
                    run.workflow_version,
                    run.status.clone(),
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
                SELECT run_id, workflow_id, workflow_version, status, created_at
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
                ORDER BY created_at ASC
                "#,
                params!(run_id.to_owned()),
            )
            .await?;

        rows.into_iter().map(TryInto::try_into).collect()
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
        let queued_rows = self
            .client
            .query_as::<TaskQueueRow, _>(
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
                    leased_by_worker_id,
                    created_at,
                    updated_at
                FROM task_queue
                WHERE status = $1
                ORDER BY created_at ASC
                "#,
                params!(TASK_STATUS_QUEUED),
            )
            .await?;

        let Some(row) = queued_rows.into_iter().find(|row| {
            poll.queues.iter().any(|queue| queue == &row.queue_name)
                && poll
                    .capabilities
                    .iter()
                    .any(|capability| capability == &row.capability)
        }) else {
            return Ok(None);
        };

        let attempt_sequence = row.attempt_sequence + 1;
        let attempt_id = format!("attempt-{attempt_sequence}");
        let fencing_token = row.active_fencing_token.unwrap_or(0) + 1;

        self.client
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
                "#,
                params!(
                    TASK_STATUS_LEASED,
                    attempt_sequence,
                    attempt_id.clone(),
                    fencing_token,
                    poll.worker_id.clone(),
                    leased_at,
                    row.task_id.clone()
                ),
            )
            .await?;

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
                    attempt_id.clone(),
                    row.task_id.clone(),
                    attempt_id.clone(),
                    poll.worker_id.clone(),
                    fencing_token,
                    lease_expiry,
                    leased_at
                ),
            )
            .await?;

        Ok(Some(TaskLeaseRecord {
            task_id: row.task_id,
            run_id: row.run_id,
            workflow_id: row.workflow_id,
            workflow_version: row.workflow_version as u32,
            step_id: row.step_id,
            step_kind: row.step_kind,
            handler_id: row.handler_id,
            input: serde_json::from_str(&row.input_json)?,
            attempt_id,
            fencing_token: fencing_token as u64,
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

        self.client
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

        Ok(MutationApplyResult::Applied)
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

        self.client
            .execute(
                r#"
                UPDATE task_queue
                SET status = $1, updated_at = $2
                WHERE task_id = $3
                "#,
                params!(TASK_STATUS_COMPLETED, updated_at, lease.task_id.clone()),
            )
            .await?;
        self.client
            .execute(
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
            )
            .await?;
        self.client
            .execute(
                r#"
                UPDATE step_instances
                SET status = $1,
                    attempt_count = $2,
                    output_json = $3,
                    error_json = NULL,
                    updated_at = $4
                WHERE run_id = $5 AND step_id = $6
                "#,
                params!(
                    TASK_STATUS_COMPLETED,
                    row.attempt_sequence,
                    serde_json::to_string(&result.payload)?,
                    updated_at,
                    row.run_id.clone(),
                    row.step_id.clone()
                ),
            )
            .await?;

        if self.run_has_open_tasks(&row.run_id).await? {
            self.update_run_status(&row.run_id, "running", updated_at)
                .await?;
        } else {
            self.update_run_status(&row.run_id, "completed", updated_at)
                .await?;
        }

        Ok(MutationApplyResult::Applied)
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

        self.client
            .execute(
                r#"
                UPDATE task_queue
                SET status = $1, updated_at = $2
                WHERE task_id = $3
                "#,
                params!(TASK_STATUS_FAILED, updated_at, lease.task_id.clone()),
            )
            .await?;
        self.client
            .execute(
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
            )
            .await?;
        self.client
            .execute(
                r#"
                UPDATE step_instances
                SET status = $1,
                    attempt_count = $2,
                    output_json = NULL,
                    error_json = $3,
                    updated_at = $4
                WHERE run_id = $5 AND step_id = $6
                "#,
                params!(
                    TASK_STATUS_FAILED,
                    row.attempt_sequence,
                    serde_json::to_string(failure)?,
                    updated_at,
                    row.run_id.clone(),
                    row.step_id.clone()
                ),
            )
            .await?;
        self.update_run_status(&row.run_id, "failed", updated_at)
            .await?;

        Ok(MutationApplyResult::Applied)
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
                    leased_by_worker_id,
                    created_at,
                    updated_at
                FROM task_queue
                WHERE task_id = $1
                "#,
                params!(task_id.to_owned()),
            )
            .await
            .map_err(Into::into)
    }

    async fn run_has_open_tasks(&self, run_id: &str) -> Result<bool, PersistenceError> {
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

        Ok(rows
            .into_iter()
            .any(|row| row.status == TASK_STATUS_QUEUED || row.status == TASK_STATUS_LEASED))
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
}

impl TryFrom<WorkflowDefinitionRow> for DraftDefinitionRecord {
    type Error = PersistenceError;

    fn try_from(row: WorkflowDefinitionRow) -> Result<Self, Self::Error> {
        Ok(Self {
            definition: serde_json::from_str(&row.definition_json)?,
            content_hash: row.content_hash,
            last_validated_content_hash: row.last_validated_content_hash,
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
    created_at: i64,
}

impl From<WorkflowRunRow> for WorkflowRunRecord {
    fn from(row: WorkflowRunRow) -> Self {
        Self {
            run_id: row.run_id,
            workflow_id: row.workflow_id,
            workflow_version: row.workflow_version as u32,
            status: row.status,
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
