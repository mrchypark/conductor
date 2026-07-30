mod hiqlite;
mod schema;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::stored_definition::{
    DecodedStoredDefinition, StoredDefinitionClassification, StoredDefinitionDecodeDiagnostic,
};

pub use hiqlite::{HiqlitePersistence, PersistenceError};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StoredDefinitionExecutionCompatibility {
    Compatible,
    Incompatible,
}

/// Lossless administrative view of one retained published definition row.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredDefinitionInspection {
    pub workflow_id: String,
    pub workflow_version: i64,
    pub definition_json: String,
    pub execution_metadata_json: Option<String>,
    pub content_hash: String,
    pub last_validated_content_hash: Option<String>,
    pub created_at: i64,
    pub published_at: Option<i64>,
    pub classification: Option<StoredDefinitionClassification>,
    pub execution_compatibility: StoredDefinitionExecutionCompatibility,
    pub new_run_eligible: bool,
    pub decode_diagnostic: Option<StoredDefinitionDecodeDiagnostic>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(dead_code)] // Keeps explicit no-definition outcomes for selection callers.
pub(crate) enum LatestEligibleDefinitionSelection {
    Selected(Box<DecodedStoredDefinition>),
    NoPublishedDefinitions,
    NoEligiblePublishedDefinition,
}

/// Result of an atomic task outcome transaction.
///
/// `Expired` is distinct from `Stale` so callers can hand ownership to timeout
/// recovery instead of retrying a worker completion/failure at the deadline.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskOutcomeApplyResult {
    Applied,
    AlreadyApplied,
    Stale,
    Expired,
}

/// Result of atomically consuming an exact worker lease into durable manual
/// recovery quarantine without consulting mutable execution defaults.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum TaskExecutionQuarantineResult {
    Applied,
    AlreadyApplied,
    Expired,
    Stale,
    RunNotRunning { status: String },
    Conflict { detail: String },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TimerClaimResult {
    Claimed,
    ReplayPendingEffect,
    NotClaimable,
}

/// Durable context needed to replay downstream scheduling after a completed task.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct CompletedTaskContinuation {
    pub task_id: String,
    pub run_id: String,
    pub workflow_id: String,
    pub workflow_version: u32,
    pub step_id: String,
    pub output: Value,
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(dead_code)] // Private persistence contract for provenance quarantine.
pub(crate) enum ContinuationQuarantineResult {
    Applied,
    AlreadyApplied,
    Stale(ContinuationQuarantineStaleReason),
    Conflict { existing_state: Option<String> },
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(dead_code)] // Private persistence contract for provenance quarantine.
pub(crate) enum ContinuationQuarantineStaleReason {
    SourceMissing,
    RunNotRunning {
        status: String,
    },
    SourceNotRunnable {
        status: String,
        continuation_state: Option<String>,
    },
}
