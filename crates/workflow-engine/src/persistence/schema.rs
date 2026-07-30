pub const WORKFLOW_ENGINE_SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS workflow_definitions (
    workflow_id TEXT NOT NULL,
    workflow_version INTEGER NOT NULL,
    status TEXT NOT NULL,
    definition_json TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    last_validated_content_hash TEXT,
    execution_metadata_json TEXT,
    created_at INTEGER NOT NULL,
    published_at INTEGER,
    PRIMARY KEY (workflow_id, status, workflow_version)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_workflow_definitions_draft
    ON workflow_definitions(workflow_id, status)
    WHERE status = 'draft';

CREATE INDEX IF NOT EXISTS idx_workflow_definitions_published_lookup
    ON workflow_definitions(workflow_id, status, workflow_version DESC);

CREATE TABLE IF NOT EXISTS workflow_runs (
    run_id TEXT PRIMARY KEY,
    workflow_id TEXT NOT NULL,
    workflow_version INTEGER NOT NULL,
    status TEXT NOT NULL,
    cancel_reason TEXT,
    cancel_requested_by TEXT,
    cancel_requested_at INTEGER,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS step_instances (
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

CREATE TABLE IF NOT EXISTS task_queue (
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
    timeout_at INTEGER,
    continuation_state TEXT,
    status TEXT NOT NULL,
    attempt_sequence INTEGER NOT NULL,
    active_attempt_id TEXT,
    active_fencing_token INTEGER,
    leased_by_worker_id TEXT,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_task_queue_poll
    ON task_queue(status, queue_name, capability, created_at);

CREATE TABLE IF NOT EXISTS timers (
    timer_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    step_id TEXT NOT NULL,
    timer_kind TEXT NOT NULL,
    due_at INTEGER NOT NULL,
    status TEXT NOT NULL,
    payload_json TEXT,
    effect_state TEXT,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS leases (
    lease_id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL,
    attempt_id TEXT NOT NULL,
    worker_id TEXT NOT NULL,
    fencing_token INTEGER NOT NULL,
    expires_at INTEGER NOT NULL,
    heartbeat_at INTEGER NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_leases_task_id
    ON leases(task_id);

CREATE TABLE IF NOT EXISTS event_inbox (
    event_id TEXT PRIMARY KEY,
    event_type TEXT NOT NULL,
    processing_state TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    dedupe_key TEXT,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    applied_at INTEGER
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_event_inbox_dedupe
    ON event_inbox(dedupe_key)
    WHERE dedupe_key IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_event_inbox_recovery
    ON event_inbox(processing_state, created_at, event_id);

CREATE TABLE IF NOT EXISTS transition_log (
    transition_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    transition_type TEXT NOT NULL,
    payload_json TEXT,
    created_at INTEGER NOT NULL
);
"#;
