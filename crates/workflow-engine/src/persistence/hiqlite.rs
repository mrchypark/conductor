use std::{
    net::TcpListener,
    path::{Path, PathBuf},
};

use hiqlite::{start_node, Client, Node, NodeConfig, Param};
use serde::Deserialize;
use thiserror::Error;
use workflow_definition::FlowDefinition;

use crate::{
    definition_store::{StoredWorkflowDefinition, WorkflowRunRecord},
    persistence::schema::WORKFLOW_ENGINE_SCHEMA,
};

const STATUS_DRAFT: &str = "draft";
const STATUS_PUBLISHED: &str = "published";

macro_rules! params {
    ( $( $param:expr ),* $(,)? ) => {
        {
            #[allow(unused_mut)]
            let mut params = Vec::new();
            $(
                params.push(Param::from($param));
            )*
            params
        }
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

        Ok(next
            .and_then(|row| row.next_workflow_version)
            .unwrap_or(1) as u32)
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
                VALUES ($1, $2, $3, $4, $5, $5, $6, $7)
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

    pub async fn insert_run(&self, run: &WorkflowRunRecord) -> Result<(), PersistenceError> {
        self.client
            .execute(
                r#"
                INSERT INTO workflow_runs (
                    run_id,
                    workflow_id,
                    workflow_version,
                    status,
                    created_at
                )
                VALUES ($1, $2, $3, $4, $5)
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

    pub async fn get_run(&self, run_id: &str) -> Result<Option<WorkflowRunRecord>, PersistenceError> {
        Ok(self
            .client
            .query_as_optional(
                r#"
                SELECT run_id, workflow_id, workflow_version, status, created_at
                FROM workflow_runs
                WHERE run_id = $1
                "#,
                params!(run_id.to_owned()),
            )
            .await?)
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
struct VersionRow {
    next_workflow_version: Option<i64>,
}

fn local_node_config(base_dir: &Path) -> NodeConfig {
    let api_addr = loopback_addr(free_port());
    let raft_addr = loopback_addr(free_port());
    let mut config = NodeConfig::default();
    config.node_id = 1;
    config.nodes = vec![Node {
        id: 1,
        addr_raft: raft_addr.clone(),
        addr_api: api_addr.clone(),
    }];
    config.listen_addr_api = api_addr.into();
    config.listen_addr_raft = raft_addr.into();
    config.data_dir = PathBuf::from(base_dir.join("hiqlite"))
        .display()
        .to_string()
        .into();
    config.filename_db = "workflow-engine.db".into();
    config.secret_api = "workflow-engine-api-secret".to_owned();
    config.secret_raft = "workflow-engine-raft-secret".to_owned();
    config
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
