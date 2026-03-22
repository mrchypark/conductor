pub const WORKFLOW_ENGINE_SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS workflow_definitions (
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
    created_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS step_instances (
    step_instance_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    step_id TEXT NOT NULL,
    status TEXT NOT NULL,
    created_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS task_queue (
    task_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    step_id TEXT NOT NULL,
    status TEXT NOT NULL,
    created_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS timers (
    timer_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    due_at INTEGER NOT NULL,
    status TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS waiters (
    waiter_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    status TEXT NOT NULL,
    created_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS leases (
    lease_id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL,
    worker_id TEXT NOT NULL,
    fencing_token INTEGER NOT NULL,
    expires_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS event_inbox (
    event_id TEXT PRIMARY KEY,
    event_type TEXT NOT NULL,
    processing_state TEXT NOT NULL,
    created_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS transition_log (
    transition_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    transition_type TEXT NOT NULL,
    created_at INTEGER NOT NULL
);
"#;
