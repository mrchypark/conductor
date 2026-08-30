// Package storage owns the SQLite persistence used by the local conductor.
package storage

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	_ "github.com/ncruces/go-sqlite3/driver"
)

var ErrNotFound = errors.New("execution not found")

const schemaIdentity = "conductor-execution-v1"

type Store struct {
	db             *sql.DB
	callbackTarget string
}

type ExecutionRequestRecord struct {
	ID               string
	WorkspaceID      string
	WorkflowKey      string
	WorkflowVersion  uint64
	WorkflowID       string
	TaskID           string
	NodeKey          string
	HandlerKey       string
	HandlerVersion   string
	OperationKey     string
	ConnectionID     string
	ActorType        string
	ActorID          string
	SubjectRef       []byte
	InputRef         []byte
	DeadlineAt       time.Time
	Input            []byte
	Timeout          time.Duration
	MaxAttempts      int
	RetryBackoff     time.Duration
	RetryBackoffMode string
	CorrelationID    string
	CausationID      string
	IdempotencyKey   string
}

type ExecutionRecord struct {
	ID                 string
	ExecutionRequestID string
	Status             string
	HandlerKey         string
	HandlerVersion     string
	OperationKey       string
	ConnectionID       string
	Input              []byte
	Output             []byte
	ErrorClass         string
	ErrorMessage       string
	ErrorRetryable     bool
	CreatedAt          time.Time
	UpdatedAt          time.Time
}

type LeaseRecord struct {
	ID            string    `json:"lease_id"`
	ExecutionID   string    `json:"execution_id"`
	AttemptNumber int       `json:"attempt_number"`
	WorkerID      string    `json:"worker_id"`
	FencingToken  int64     `json:"fencing_token"`
	ExpiresAt     time.Time `json:"expires_at"`
}

type CallbackDeliveryRecord struct {
	ID            string
	ExecutionID   string
	TargetURL     string
	Payload       []byte
	Status        string
	AttemptCount  int
	NextAttemptAt time.Time
	LastError     string
}

type ExecutionEventRecord struct {
	ID            string          `json:"id"`
	ExecutionID   string          `json:"execution_id"`
	Type          string          `json:"event_type"`
	AttemptNumber int             `json:"attempt_number"`
	Payload       json.RawMessage `json:"payload"`
	OccurredAt    time.Time       `json:"occurred_at"`
}

// DynamicHandlerRecord stores a versioned Kavun definition and its optional
// compiled bytecode cache. Source remains canonical; bytecode is disposable.
type DynamicHandlerRecord struct {
	HandlerKey         string
	Version            string
	ManifestJSON       []byte
	Source             []byte
	AllowedModules     []string
	MaxFrames          int
	MaxStack           int
	SourceHash         string
	Bytecode           []byte
	BytecodeHash       string
	BytecodeSourceHash string
	RuntimeABI         string
}

// Open opens a SQLite database and applies the embedded, ordered migrations.
func Open(ctx context.Context, dsn string) (*Store, error) {
	if dsn == "" {
		return nil, errors.New("sqlite dsn is required")
	}
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1) // ponytail: single-writer local runtime; raise after WAL/concurrency measurement.
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, err
	}
	s := &Store{db: db}
	if _, err := db.ExecContext(ctx, "PRAGMA foreign_keys = ON"); err != nil {
		db.Close()
		return nil, err
	}
	if _, err := db.ExecContext(ctx, "PRAGMA journal_mode = WAL; PRAGMA busy_timeout = 5000"); err != nil {
		db.Close()
		return nil, err
	}
	if err := s.migrate(ctx); err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) Close() error { return s.db.Close() }

// DB exposes the shared database for sibling persistence packages.
func (s *Store) DB() *sql.DB { return s.db }

func (s *Store) SetCallbackTarget(url string) { s.callbackTarget = url }

func (s *Store) migrate(ctx context.Context) error {
	var tableCount, metadataExists int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM sqlite_master WHERE type='table'`).Scan(&tableCount); err != nil {
		return err
	}
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='schema_metadata'`).Scan(&metadataExists); err != nil {
		return err
	}
	if tableCount > 0 && metadataExists == 0 {
		return errors.New("unsupported legacy or partially initialized database; recreate it")
	}
	if metadataExists == 1 {
		var identity string
		if err := s.db.QueryRowContext(ctx, `SELECT identity FROM schema_metadata WHERE id=1`).Scan(&identity); err != nil {
			return errors.New("invalid schema metadata; recreate the database")
		}
		if identity != schemaIdentity {
			return fmt.Errorf("unsupported schema identity %q; expected %q", identity, schemaIdentity)
		}
		return s.ensureDynamicHandlerTable(ctx)
	}
	if _, err := s.db.ExecContext(ctx, `
CREATE TABLE IF NOT EXISTS schema_metadata (
  id INTEGER PRIMARY KEY CHECK (id=1),
  identity TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS execution_requests (
  id TEXT PRIMARY KEY,
  workspace_id TEXT NOT NULL,
  workflow_key TEXT NOT NULL,
  workflow_version INTEGER NOT NULL,
  workflow_instance_id TEXT NOT NULL,
  task_id TEXT NOT NULL,
  node_key TEXT NOT NULL,
  handler_key TEXT NOT NULL,
  handler_version TEXT NOT NULL,
  operation_key TEXT NOT NULL,
  connection_id TEXT NOT NULL,
	actor_type TEXT NOT NULL,
	actor_id TEXT NOT NULL,
	subject_ref BLOB NOT NULL DEFAULT '',
	input_ref BLOB NOT NULL DEFAULT '',
	deadline_at TEXT NOT NULL DEFAULT '',
	input BLOB NOT NULL,
  timeout_ns INTEGER NOT NULL,
  max_attempts INTEGER NOT NULL,
  retry_backoff_ns INTEGER NOT NULL DEFAULT 0,
  retry_backoff_mode TEXT NOT NULL DEFAULT '',
  correlation_id TEXT NOT NULL,
  causation_id TEXT NOT NULL,
  idempotency_key TEXT NOT NULL,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS executions (
  id TEXT PRIMARY KEY,
  execution_request_id TEXT NOT NULL UNIQUE REFERENCES execution_requests(id),
 status TEXT NOT NULL CHECK (status IN ('queued', 'leased', 'running', 'retry_wait', 'succeeded', 'failed', 'timed_out', 'cancelled')),
  handler_key TEXT NOT NULL,
  handler_version TEXT NOT NULL,
  operation_key TEXT NOT NULL,
  connection_id TEXT NOT NULL,
  input BLOB NOT NULL,
  output BLOB NOT NULL DEFAULT '',
	error_class TEXT NOT NULL DEFAULT '',
	error_message TEXT NOT NULL DEFAULT '',
	error_retryable INTEGER NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS attempts (
  id TEXT PRIMARY KEY,
  execution_id TEXT NOT NULL REFERENCES executions(id),
  attempt_number INTEGER NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('succeeded', 'failed')),
  error_class TEXT NOT NULL DEFAULT '',
  error_message TEXT NOT NULL DEFAULT '',
  started_at TEXT NOT NULL,
  finished_at TEXT NOT NULL,
  UNIQUE (execution_id, attempt_number)
);
CREATE TABLE IF NOT EXISTS callbacks (
  id TEXT PRIMARY KEY,
  execution_id TEXT NOT NULL REFERENCES executions(id),
  status TEXT NOT NULL,
  payload BLOB NOT NULL,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS leases (
  lease_id TEXT PRIMARY KEY,
  execution_id TEXT NOT NULL UNIQUE REFERENCES executions(id),
  attempt_number INTEGER NOT NULL,
  worker_id TEXT NOT NULL,
  fencing_token INTEGER NOT NULL,
  expires_at TEXT NOT NULL,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  status TEXT NOT NULL CHECK (status IN ('active', 'released', 'expired'))
);
CREATE TABLE IF NOT EXISTS retry_timers (
  id TEXT PRIMARY KEY,
  execution_id TEXT NOT NULL REFERENCES executions(id),
  attempt_number INTEGER NOT NULL,
  run_at TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('scheduled', 'claimed', 'fired', 'cancelled')),
  UNIQUE (execution_id, attempt_number)
);
CREATE TABLE IF NOT EXISTS callback_deliveries (
  callback_id TEXT PRIMARY KEY REFERENCES callbacks(id),
  execution_id TEXT NOT NULL REFERENCES executions(id),
  target_url TEXT NOT NULL,
  payload BLOB NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('pending', 'sending', 'delivered', 'retry_wait', 'failed')),
  attempt_count INTEGER NOT NULL DEFAULT 0,
  next_attempt_at TEXT NOT NULL,
  last_error TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS handler_manifests (
  handler_key TEXT NOT NULL,
  version TEXT NOT NULL,
  manifest_json BLOB NOT NULL,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (handler_key, version)
);
CREATE TABLE IF NOT EXISTS connector_manifests (
  provider_key TEXT PRIMARY KEY,
  manifest_json BLOB NOT NULL,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS execution_events (
  id TEXT PRIMARY KEY,
  execution_id TEXT NOT NULL REFERENCES executions(id),
  event_type TEXT NOT NULL,
  attempt_number INTEGER NOT NULL DEFAULT 0,
  payload BLOB NOT NULL DEFAULT '{}',
  occurred_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
	);`); err != nil {
		return err
	}
	if _, err := s.db.ExecContext(ctx, `CREATE UNIQUE INDEX IF NOT EXISTS idx_execution_requests_idempotency ON execution_requests(workspace_id, idempotency_key) WHERE idempotency_key <> ''`); err != nil {
		return err
	}
	if err := s.ensureDynamicHandlerTable(ctx); err != nil {
		return err
	}
	if _, err := s.db.ExecContext(ctx, `INSERT INTO schema_metadata(id, identity) VALUES(1, ?)`, schemaIdentity); err != nil {
		return err
	}
	return nil
}

func (s *Store) ensureDynamicHandlerTable(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, `
CREATE TABLE IF NOT EXISTS dynamic_handlers (
  handler_key TEXT NOT NULL,
  version TEXT NOT NULL,
  manifest_json BLOB NOT NULL,
  source BLOB NOT NULL,
  allowed_modules_json BLOB NOT NULL DEFAULT '[]',
  max_frames INTEGER NOT NULL DEFAULT 256,
  max_stack INTEGER NOT NULL DEFAULT 2048,
  source_hash TEXT NOT NULL,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (handler_key, version)
);
CREATE TABLE IF NOT EXISTS dynamic_handler_bytecode (
  handler_key TEXT NOT NULL,
  version TEXT NOT NULL,
  bytecode BLOB NOT NULL,
  bytecode_hash TEXT NOT NULL,
  source_hash TEXT NOT NULL,
  runtime_abi TEXT NOT NULL,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (handler_key, version),
  FOREIGN KEY (handler_key, version) REFERENCES dynamic_handlers(handler_key, version) ON DELETE CASCADE
);`)
	return err
}

func (s *Store) SaveHandlerManifest(ctx context.Context, key, version string, manifest any) error {
	if key == "" || version == "" {
		return errors.New("handler key and version are required")
	}
	body, err := json.Marshal(manifest)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, `INSERT INTO handler_manifests(handler_key,version,manifest_json) VALUES(?,?,?) ON CONFLICT(handler_key,version) DO UPDATE SET manifest_json=excluded.manifest_json`, key, version, body)
	return err
}

// SaveDynamicHandler stores an immutable handler version. Re-submitting the
// exact definition is idempotent; changing source or policy for an existing
// version is rejected.
func (s *Store) SaveDynamicHandler(ctx context.Context, record DynamicHandlerRecord) (bool, error) {
	if record.HandlerKey == "" || record.Version == "" || len(record.ManifestJSON) == 0 || len(record.Source) == 0 || record.SourceHash == "" {
		return false, errors.New("dynamic handler key, version, manifest, source, and hash are required")
	}
	modules, err := json.Marshal(record.AllowedModules)
	if err != nil {
		return false, err
	}
	var existingHash string
	var existingManifest, existingModules []byte
	var existingFrames, existingStack int
	err = s.db.QueryRowContext(ctx, `SELECT source_hash,manifest_json,allowed_modules_json,max_frames,max_stack FROM dynamic_handlers WHERE handler_key=? AND version=?`, record.HandlerKey, record.Version).Scan(&existingHash, &existingManifest, &existingModules, &existingFrames, &existingStack)
	if err == nil {
		if existingHash != record.SourceHash || !bytes.Equal(existingManifest, record.ManifestJSON) || !bytes.Equal(existingModules, modules) || existingFrames != record.MaxFrames || existingStack != record.MaxStack {
			return false, errors.New("dynamic handler version already exists with different definition")
		}
		return false, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return false, err
	}
	_, err = s.db.ExecContext(ctx, `INSERT INTO dynamic_handlers(handler_key,version,manifest_json,source,allowed_modules_json,max_frames,max_stack,source_hash) VALUES(?,?,?,?,?,?,?,?)`, record.HandlerKey, record.Version, record.ManifestJSON, record.Source, modules, record.MaxFrames, record.MaxStack, record.SourceHash)
	if err != nil {
		// A concurrent identical insert is safe; the next read verifies its hash.
		var hash string
		if readErr := s.db.QueryRowContext(ctx, `SELECT source_hash FROM dynamic_handlers WHERE handler_key=? AND version=?`, record.HandlerKey, record.Version).Scan(&hash); readErr == nil && hash == record.SourceHash {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (s *Store) DeleteDynamicHandler(ctx context.Context, key, version, sourceHash string) error {
	if key == "" || version == "" || sourceHash == "" {
		return errors.New("dynamic handler key, version, and hash are required")
	}
	_, err := s.db.ExecContext(ctx, `DELETE FROM dynamic_handlers WHERE handler_key=? AND version=? AND source_hash=?`, key, version, sourceHash)
	return err
}

func (s *Store) SaveDynamicHandlerBytecode(ctx context.Context, key, version string, bytecode []byte, bytecodeHash, sourceHash, runtimeABI string) error {
	if key == "" || version == "" || len(bytecode) == 0 || bytecodeHash == "" || sourceHash == "" || runtimeABI == "" {
		return errors.New("dynamic handler bytecode, hashes, and runtime ABI are required")
	}
	_, err := s.db.ExecContext(ctx, `INSERT INTO dynamic_handler_bytecode(handler_key,version,bytecode,bytecode_hash,source_hash,runtime_abi) VALUES(?,?,?,?,?,?) ON CONFLICT(handler_key,version) DO UPDATE SET bytecode=excluded.bytecode,bytecode_hash=excluded.bytecode_hash,source_hash=excluded.source_hash,runtime_abi=excluded.runtime_abi`, key, version, bytecode, bytecodeHash, sourceHash, runtimeABI)
	return err
}

func (s *Store) ListDynamicHandlers(ctx context.Context) ([]DynamicHandlerRecord, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT d.handler_key,d.version,d.manifest_json,d.source,d.allowed_modules_json,d.max_frames,d.max_stack,d.source_hash,COALESCE(b.bytecode,''),COALESCE(b.bytecode_hash,''),COALESCE(b.source_hash,''),COALESCE(b.runtime_abi,'') FROM dynamic_handlers d LEFT JOIN dynamic_handler_bytecode b ON b.handler_key=d.handler_key AND b.version=d.version ORDER BY d.handler_key,d.version`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var records []DynamicHandlerRecord
	for rows.Next() {
		var record DynamicHandlerRecord
		var modules []byte
		if err := rows.Scan(&record.HandlerKey, &record.Version, &record.ManifestJSON, &record.Source, &modules, &record.MaxFrames, &record.MaxStack, &record.SourceHash, &record.Bytecode, &record.BytecodeHash, &record.BytecodeSourceHash, &record.RuntimeABI); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(modules, &record.AllowedModules); err != nil {
			return nil, fmt.Errorf("dynamic handler %s@%s modules: %w", record.HandlerKey, record.Version, err)
		}
		record.ManifestJSON = append([]byte(nil), record.ManifestJSON...)
		record.Source = append([]byte(nil), record.Source...)
		record.Bytecode = append([]byte(nil), record.Bytecode...)
		record.AllowedModules = append([]string(nil), record.AllowedModules...)
		records = append(records, record)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return records, nil
}

func (s *Store) SaveConnectorManifest(ctx context.Context, key string, manifest any) error {
	if key == "" {
		return errors.New("provider key is required")
	}
	body, err := json.Marshal(manifest)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, `INSERT INTO connector_manifests(provider_key,manifest_json) VALUES(?,?) ON CONFLICT(provider_key) DO UPDATE SET manifest_json=excluded.manifest_json`, key, body)
	return err
}

// CreateExecution stores the request and its single logical execution atomically.
// It returns false when the request was already stored.
func (s *Store) CreateExecution(ctx context.Context, request ExecutionRequestRecord, executionID string) (bool, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, `INSERT INTO execution_requests (
 id, workspace_id, workflow_key, workflow_version, workflow_instance_id, task_id, node_key,
	 handler_key, handler_version, operation_key, connection_id, actor_type, actor_id, subject_ref, input_ref, deadline_at, input,
	 timeout_ns, max_attempts, retry_backoff_ns, retry_backoff_mode, correlation_id, causation_id, idempotency_key
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(id) DO NOTHING`,
		request.ID, request.WorkspaceID, request.WorkflowKey, request.WorkflowVersion, request.WorkflowID,
		request.TaskID, request.NodeKey, request.HandlerKey, request.HandlerVersion, request.OperationKey,
		request.ConnectionID, request.ActorType, request.ActorID, request.SubjectRef, request.InputRef, formatTime(request.DeadlineAt), request.Input, request.Timeout.Nanoseconds(),
		request.MaxAttempts, request.RetryBackoff.Nanoseconds(), request.RetryBackoffMode, request.CorrelationID, request.CausationID, request.IdempotencyKey)
	if err != nil {
		return false, err
	}
	created, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	if created == 0 {
		return false, tx.Commit()
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO executions (
 id, execution_request_id, status, handler_key, handler_version, operation_key, connection_id, input
) VALUES (?, ?, 'queued', ?, ?, ?, ?, ?)`, executionID, request.ID, request.HandlerKey,
		request.HandlerVersion, request.OperationKey, request.ConnectionID, request.Input)
	if err != nil {
		return false, err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO execution_events(id, execution_id, event_type, payload) VALUES(?, ?, 'execution.requested', '{}')`, "event_"+executionID+":requested", executionID); err != nil {
		return false, err
	}
	return true, tx.Commit()
}

func (s *Store) GetExecutionRequest(ctx context.Context, id string) (ExecutionRequestRecord, error) {
	var r ExecutionRequestRecord
	var deadline string
	err := s.db.QueryRowContext(ctx, `SELECT id, workspace_id, workflow_key, workflow_version, workflow_instance_id,
	 task_id, node_key, handler_key, handler_version, operation_key, connection_id, actor_type, actor_id,
	 subject_ref, input_ref, deadline_at, input, timeout_ns, max_attempts, retry_backoff_ns, retry_backoff_mode, correlation_id, causation_id, idempotency_key
	 FROM execution_requests WHERE id = ?`, id).Scan(&r.ID, &r.WorkspaceID, &r.WorkflowKey, &r.WorkflowVersion, &r.WorkflowID,
		&r.TaskID, &r.NodeKey, &r.HandlerKey, &r.HandlerVersion, &r.OperationKey, &r.ConnectionID, &r.ActorType, &r.ActorID,
		&r.SubjectRef, &r.InputRef, &deadline, &r.Input, &r.Timeout, &r.MaxAttempts, &r.RetryBackoff, &r.RetryBackoffMode, &r.CorrelationID, &r.CausationID, &r.IdempotencyKey)
	if errors.Is(err, sql.ErrNoRows) {
		return ExecutionRequestRecord{}, fmt.Errorf("%w: %s", ErrNotFound, id)
	}
	if err != nil {
		return ExecutionRequestRecord{}, err
	}
	r.DeadlineAt = parseTime(deadline)
	return r, nil
}

func (s *Store) FindExecutionRequestByIdempotency(ctx context.Context, workspaceID, key string) (ExecutionRequestRecord, error) {
	var id string
	err := s.db.QueryRowContext(ctx, `SELECT id FROM execution_requests WHERE workspace_id=? AND idempotency_key=?`, workspaceID, key).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return ExecutionRequestRecord{}, ErrNotFound
	}
	if err != nil {
		return ExecutionRequestRecord{}, err
	}
	return s.GetExecutionRequest(ctx, id)
}

// AcquireLease claims a queued execution. The fencing token monotonically increases
// so a stale worker cannot complete work after its lease expires.
func (s *Store) AcquireLease(ctx context.Context, executionID, workerID string, ttl time.Duration, attemptNumber int) (LeaseRecord, error) {
	if ttl <= 0 {
		ttl = time.Minute
	}
	if attemptNumber < 1 {
		attemptNumber = 1
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return LeaseRecord{}, err
	}
	defer tx.Rollback()
	var status string
	if err := tx.QueryRowContext(ctx, `SELECT status FROM executions WHERE id = ?`, executionID).Scan(&status); errors.Is(err, sql.ErrNoRows) {
		return LeaseRecord{}, ErrNotFound
	} else if err != nil {
		return LeaseRecord{}, err
	}
	if status != "queued" && status != "retry_wait" {
		return LeaseRecord{}, fmt.Errorf("execution %s is not claimable (%s)", executionID, status)
	}
	var token int64
	_ = tx.QueryRowContext(ctx, `SELECT fencing_token FROM leases WHERE execution_id = ?`, executionID).Scan(&token)
	token++
	leaseID := fmt.Sprintf("lease_%s_%d", executionID, token)
	expires := time.Now().UTC().Add(ttl)
	if _, err := tx.ExecContext(ctx, `UPDATE executions SET status='running', updated_at=CURRENT_TIMESTAMP WHERE id=?`, executionID); err != nil {
		return LeaseRecord{}, err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO leases (lease_id, execution_id, attempt_number, worker_id, fencing_token, expires_at, status) VALUES (?, ?, ?, ?, ?, ?, 'active')
ON CONFLICT(execution_id) DO UPDATE SET lease_id=excluded.lease_id, attempt_number=excluded.attempt_number, worker_id=excluded.worker_id, fencing_token=excluded.fencing_token, expires_at=excluded.expires_at, status='active', updated_at=CURRENT_TIMESTAMP`, leaseID, executionID, attemptNumber, workerID, token, expires); err != nil {
		return LeaseRecord{}, err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO execution_events(id, execution_id, event_type, attempt_number, payload) VALUES(?, ?, 'execution.started', ?, '{}')`, "event_"+executionID+fmt.Sprintf(":started:%d", token), executionID, attemptNumber); err != nil {
		return LeaseRecord{}, err
	}
	if err := tx.Commit(); err != nil {
		return LeaseRecord{}, err
	}
	return LeaseRecord{ID: leaseID, ExecutionID: executionID, AttemptNumber: attemptNumber, WorkerID: workerID, FencingToken: token, ExpiresAt: expires}, nil
}

func (s *Store) ClaimNext(ctx context.Context, workerID string, ttl time.Duration) (ExecutionRequestRecord, LeaseRecord, error) {
	var id string
	var attempt int
	if err := s.db.QueryRowContext(ctx, `SELECT execution_request_id, COALESCE((SELECT attempt_number+1 FROM retry_timers t WHERE t.execution_id=e.id AND t.status='scheduled'), (SELECT attempt_number+1 FROM leases l WHERE l.execution_id=e.id AND l.status='expired'), 1) FROM executions e WHERE status='queued' OR (status='retry_wait' AND EXISTS (SELECT 1 FROM retry_timers t WHERE t.execution_id=e.id AND t.status='scheduled' AND datetime(t.run_at) <= datetime('now'))) ORDER BY created_at, id LIMIT 1`).Scan(&id, &attempt); errors.Is(err, sql.ErrNoRows) {
		return ExecutionRequestRecord{}, LeaseRecord{}, ErrNotFound
	} else if err != nil {
		return ExecutionRequestRecord{}, LeaseRecord{}, err
	}
	r, err := s.GetExecutionRequest(ctx, id)
	if err != nil {
		return ExecutionRequestRecord{}, LeaseRecord{}, err
	}
	e, err := s.GetExecution(ctx, "exec_"+id)
	if err != nil {
		return ExecutionRequestRecord{}, LeaseRecord{}, err
	}
	if attempt < 1 {
		attempt = 1
	}
	l, err := s.AcquireLease(ctx, e.ID, workerID, ttl, attempt)
	if err == nil {
		_, _ = s.db.ExecContext(ctx, `UPDATE retry_timers SET status='claimed' WHERE execution_id=? AND status='scheduled'`, e.ID)
	}
	return r, l, err
}

func (s *Store) ScheduleRetry(ctx context.Context, executionID string, attemptNumber int, runAt time.Time) error {
	if attemptNumber < 1 {
		return errors.New("attempt number must be positive")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `INSERT INTO retry_timers (id, execution_id, attempt_number, run_at, status) VALUES (?, ?, ?, ?, 'scheduled') ON CONFLICT(execution_id, attempt_number) DO UPDATE SET run_at=excluded.run_at, status='scheduled'`, fmt.Sprintf("retry_%s_%d", executionID, attemptNumber), executionID, attemptNumber, runAt.UTC()); err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, `UPDATE executions SET status='retry_wait', updated_at=CURRENT_TIMESTAMP WHERE id=? AND status='running'`, executionID)
	if err != nil {
		return err
	}
	if n, _ := result.RowsAffected(); n != 1 {
		return fmt.Errorf("execution %s is not running", executionID)
	}
	if _, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO execution_events(id, execution_id, event_type, attempt_number, payload) VALUES(?, ?, 'execution.retrying', ?, '{}')`, fmt.Sprintf("event_%s:retry:%d", executionID, attemptNumber), executionID, attemptNumber); err != nil {
		return err
	}
	return tx.Commit()
}

// RetryExecutionWithLease records a failed attempt and schedules its retry in
// one fenced transaction. A stale worker cannot requeue a newer attempt.
func (s *Store) RetryExecutionWithLease(ctx context.Context, leaseID string, fencingToken int64, executionID string, attemptNumber int, errorClass, errorMessage string, runAt time.Time) error {
	if attemptNumber < 1 {
		return errors.New("attempt number must be positive")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var leaseAttempt int
	if err := tx.QueryRowContext(ctx, `SELECT attempt_number FROM leases WHERE lease_id=? AND execution_id=? AND fencing_token=? AND status='active' AND expires_at > ?`, leaseID, executionID, fencingToken, time.Now().UTC()).Scan(&leaseAttempt); err != nil {
		return errors.New("lease is stale or expired")
	}
	if leaseAttempt != attemptNumber {
		return errors.New("lease does not match execution attempt")
	}
	var executionStatus string
	if err := tx.QueryRowContext(ctx, `SELECT status FROM executions WHERE id=?`, executionID).Scan(&executionStatus); err != nil {
		return err
	}
	if executionStatus == "retry_wait" {
		// A duplicate failure for the same fenced attempt is idempotent.
		return tx.Commit()
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO attempts (id, execution_id, attempt_number, status, error_class, error_message, started_at, finished_at) VALUES (?, ?, ?, 'failed', ?, ?, ?, ?) ON CONFLICT(id) DO NOTHING`, fmt.Sprintf("%s:%d", executionID, attemptNumber), executionID, attemptNumber, errorClass, errorMessage, time.Now().UTC(), time.Now().UTC()); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO retry_timers (id, execution_id, attempt_number, run_at, status) VALUES (?, ?, ?, ?, 'scheduled') ON CONFLICT(execution_id, attempt_number) DO UPDATE SET run_at=excluded.run_at, status='scheduled'`, fmt.Sprintf("retry_%s_%d", executionID, attemptNumber), executionID, attemptNumber, runAt.UTC()); err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, `UPDATE executions SET status='retry_wait', updated_at=CURRENT_TIMESTAMP WHERE id=? AND status='running'`, executionID)
	if err != nil {
		return err
	}
	if n, _ := result.RowsAffected(); n != 1 {
		return fmt.Errorf("execution %s is not running", executionID)
	}
	if _, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO execution_events(id, execution_id, event_type, attempt_number, payload) VALUES(?, ?, 'execution.retrying', ?, '{}')`, fmt.Sprintf("event_%s:retry:%d", executionID, attemptNumber), executionID, attemptNumber); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) HeartbeatLease(ctx context.Context, leaseID string, fencingToken int64, ttl time.Duration) error {
	if ttl <= 0 {
		ttl = time.Minute
	}
	result, err := s.db.ExecContext(ctx, `UPDATE leases SET expires_at=?, updated_at=CURRENT_TIMESTAMP WHERE lease_id=? AND fencing_token=? AND status='active' AND expires_at > ?`, time.Now().UTC().Add(ttl), leaseID, fencingToken, time.Now().UTC())
	if err != nil {
		return err
	}
	n, _ := result.RowsAffected()
	if n != 1 {
		return errors.New("lease is stale or expired")
	}
	return nil
}

func (s *Store) ReleaseLease(ctx context.Context, leaseID string, fencingToken int64) error {
	result, err := s.db.ExecContext(ctx, `UPDATE leases SET status='released', updated_at=CURRENT_TIMESTAMP WHERE lease_id=? AND fencing_token=? AND status='active'`, leaseID, fencingToken)
	if err != nil {
		return err
	}
	n, _ := result.RowsAffected()
	if n != 1 {
		return errors.New("lease is stale or already released")
	}
	return nil
}

func (s *Store) ActiveLease(ctx context.Context, executionID string) (LeaseRecord, error) {
	var l LeaseRecord
	var expires string
	err := s.db.QueryRowContext(ctx, `SELECT lease_id, execution_id, attempt_number, worker_id, fencing_token, expires_at FROM leases WHERE execution_id=? AND status='active' AND expires_at > ?`, executionID, time.Now().UTC()).Scan(&l.ID, &l.ExecutionID, &l.AttemptNumber, &l.WorkerID, &l.FencingToken, &expires)
	if errors.Is(err, sql.ErrNoRows) {
		return LeaseRecord{}, ErrNotFound
	}
	if err != nil {
		return LeaseRecord{}, err
	}
	l.ExpiresAt = parseTime(expires)
	return l, nil
}

func (s *Store) Lease(ctx context.Context, leaseID string) (LeaseRecord, error) {
	var l LeaseRecord
	var expires string
	err := s.db.QueryRowContext(ctx, `SELECT lease_id, execution_id, attempt_number, worker_id, fencing_token, expires_at FROM leases WHERE lease_id=?`, leaseID).Scan(&l.ID, &l.ExecutionID, &l.AttemptNumber, &l.WorkerID, &l.FencingToken, &expires)
	if errors.Is(err, sql.ErrNoRows) {
		return LeaseRecord{}, ErrNotFound
	}
	if err != nil {
		return LeaseRecord{}, err
	}
	l.ExpiresAt = parseTime(expires)
	return l, nil
}

// RecoverExpiredLeases retries abandoned work only while its persisted budget
// permits it. Exhausted work is terminal because the worker may have completed
// an external side effect before losing its lease.
func (s *Store) RecoverExpiredLeases(ctx context.Context, now time.Time) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `UPDATE leases SET status='expired', updated_at=CURRENT_TIMESTAMP WHERE status='active' AND expires_at <= ?`, now.UTC()); err != nil {
		return err
	}
	rows, err := tx.QueryContext(ctx, `SELECT e.id, l.attempt_number, r.max_attempts
		FROM executions e
		JOIN leases l ON l.execution_id=e.id
		JOIN execution_requests r ON r.id=e.execution_request_id
		WHERE e.status='running' AND l.status='expired'`)
	if err != nil {
		return err
	}
	type expiredExecution struct {
		id                   string
		attempt, maxAttempts int
	}
	var expired []expiredExecution
	for rows.Next() {
		var item expiredExecution
		if err := rows.Scan(&item.id, &item.attempt, &item.maxAttempts); err != nil {
			rows.Close()
			return err
		}
		expired = append(expired, item)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()
	for _, item := range expired {
		attempt, maxAttempts := item.attempt, item.maxAttempts
		if maxAttempts < 1 {
			maxAttempts = 1
		}
		if attempt >= maxAttempts {
			if err := s.completeExecutionTx(ctx, tx, item.id, "failed", nil, "outcome_unknown", "worker lease expired after execution may have produced side effects", false, attempt); err != nil {
				return err
			}
			continue
		}
		if _, err := tx.ExecContext(ctx, `UPDATE executions SET status='queued', updated_at=CURRENT_TIMESTAMP WHERE id=? AND status='running'`, item.id); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *Store) EnqueueCallbackDelivery(ctx context.Context, callbackID, targetURL string) error {
	if targetURL == "" {
		return nil
	}
	var executionID string
	var payload []byte
	if err := s.db.QueryRowContext(ctx, `SELECT execution_id, payload FROM callbacks WHERE id=?`, callbackID).Scan(&executionID, &payload); err != nil {
		return err
	}
	_, err := s.db.ExecContext(ctx, `INSERT INTO callback_deliveries (callback_id, execution_id, target_url, payload, status, next_attempt_at) VALUES (?, ?, ?, ?, 'pending', CURRENT_TIMESTAMP) ON CONFLICT(callback_id) DO UPDATE SET target_url=excluded.target_url`, callbackID, executionID, targetURL, payload)
	return err
}

func (s *Store) DueCallbackDeliveries(ctx context.Context, now time.Time, limit int) ([]CallbackDeliveryRecord, error) {
	if limit <= 0 {
		limit = 20
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, `SELECT callback_id, execution_id, target_url, payload, status, attempt_count, next_attempt_at, last_error FROM callback_deliveries WHERE status IN ('pending','retry_wait') AND datetime(next_attempt_at) <= datetime(?) ORDER BY next_attempt_at LIMIT ?`, now.UTC(), limit)
	if err != nil {
		return nil, err
	}
	var out []CallbackDeliveryRecord
	for rows.Next() {
		var d CallbackDeliveryRecord
		var next string
		if err := rows.Scan(&d.ID, &d.ExecutionID, &d.TargetURL, &d.Payload, &d.Status, &d.AttemptCount, &next, &d.LastError); err != nil {
			return nil, err
		}
		d.NextAttemptAt = parseTime(next)
		out = append(out, d)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	rows.Close()
	for _, d := range out {
		if _, err := tx.ExecContext(ctx, `UPDATE callback_deliveries SET status='sending', updated_at=CURRENT_TIMESTAMP WHERE callback_id=? AND status IN ('pending','retry_wait')`, d.ID); err != nil {
			return nil, err
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return out, nil
}

// RecoverCallbackDeliveries releases claims left by a crashed delivery worker.
func (s *Store) RecoverCallbackDeliveries(ctx context.Context, now time.Time, staleAge time.Duration) error {
	if staleAge <= 0 {
		staleAge = 30 * time.Second
	}
	cutoff := now.UTC().Add(-staleAge)
	_, err := s.db.ExecContext(ctx, `UPDATE callback_deliveries SET status='retry_wait', next_attempt_at=CURRENT_TIMESTAMP, last_error='stale delivery claim recovered', updated_at=CURRENT_TIMESTAMP WHERE status='sending' AND datetime(updated_at) <= datetime(?)`, cutoff)
	return err
}

func (s *Store) MarkCallbackDelivery(ctx context.Context, callbackID, status, lastError string, next time.Time) error {
	_, err := s.db.ExecContext(ctx, `UPDATE callback_deliveries SET status=?, attempt_count=attempt_count+1, last_error=?, next_attempt_at=?, updated_at=CURRENT_TIMESTAMP WHERE callback_id=? AND status='sending'`, status, lastError, next.UTC(), callbackID)
	return err
}

// CompleteExecution records the first attempt and makes the execution terminal.
func (s *Store) CompleteExecution(ctx context.Context, executionID, status string, output []byte, errorClass, errorMessage string) error {
	return s.CompleteExecutionWithAttempt(ctx, executionID, status, output, errorClass, errorMessage, false, 1)
}

func (s *Store) CompleteExecutionWithLease(ctx context.Context, leaseID string, fencingToken int64, executionID, status string, output []byte, errorClass, errorMessage string, retryable bool, attemptNumber int) error {
	if status != "succeeded" && status != "failed" && status != "timed_out" && status != "cancelled" {
		return fmt.Errorf("invalid terminal status %q", status)
	}
	if attemptNumber < 1 {
		return errors.New("attempt number must be positive")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var leaseStatus, executionStatus string
	if err := tx.QueryRowContext(ctx, `SELECT l.status, e.status FROM leases l JOIN executions e ON e.id=l.execution_id WHERE l.lease_id=? AND l.execution_id=? AND l.fencing_token=?`, leaseID, executionID, fencingToken).Scan(&leaseStatus, &executionStatus); err != nil {
		return errors.New("lease is stale or expired")
	}
	if executionStatus == "succeeded" || executionStatus == "failed" || executionStatus == "timed_out" || executionStatus == "cancelled" {
		// A duplicate completion for the same fenced attempt is idempotent.
		return tx.Commit()
	}
	if leaseStatus != "active" {
		return errors.New("lease is stale or expired")
	}
	var ok int
	if err := tx.QueryRowContext(ctx, `SELECT 1 FROM leases WHERE lease_id=? AND execution_id=? AND fencing_token=? AND status='active' AND expires_at > ?`, leaseID, executionID, fencingToken, time.Now().UTC()).Scan(&ok); err != nil {
		return errors.New("lease is stale or expired")
	}
	if err := s.completeExecutionTx(ctx, tx, executionID, status, output, errorClass, errorMessage, retryable, attemptNumber); err != nil {
		return err
	}
	return tx.Commit()
}

// CompleteExecutionWithAttempt records the terminal attempt and runtime callback atomically.
func (s *Store) CompleteExecutionWithAttempt(ctx context.Context, executionID, status string, output []byte, errorClass, errorMessage string, retryable bool, attemptNumber int) error {
	if status != "succeeded" && status != "failed" && status != "timed_out" && status != "cancelled" {
		return fmt.Errorf("invalid terminal status %q", status)
	}
	if attemptNumber < 1 {
		return errors.New("attempt number must be positive")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := s.completeExecutionTx(ctx, tx, executionID, status, output, errorClass, errorMessage, retryable, attemptNumber); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) completeExecutionTx(ctx context.Context, tx *sql.Tx, executionID, status string, output []byte, errorClass, errorMessage string, retryable bool, attemptNumber int) error {
	now := time.Now().UTC()
	attemptStatus := status
	if attemptStatus != "succeeded" {
		attemptStatus = "failed"
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO attempts (id, execution_id, attempt_number, status, error_class, error_message, started_at, finished_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(id) DO NOTHING`, fmt.Sprintf("%s:%d", executionID, attemptNumber), executionID, attemptNumber, attemptStatus, errorClass, errorMessage, now, now); err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, `UPDATE executions SET status=?, output=?, error_class=?, error_message=?, error_retryable=?, updated_at=CURRENT_TIMESTAMP WHERE id=? AND status='running'`, status, output, errorClass, errorMessage, retryable, executionID)
	if err != nil {
		return err
	}
	updated, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if updated != 1 {
		return fmt.Errorf("execution %s is not running", executionID)
	}
	var requestID, workspaceID, taskID, workflowID, correlationID, causationID string
	if err := tx.QueryRowContext(ctx, `SELECT e.execution_request_id, r.workspace_id, r.task_id, r.workflow_instance_id, r.correlation_id, r.causation_id FROM executions e JOIN execution_requests r ON r.id=e.execution_request_id WHERE e.id=?`, executionID).Scan(&requestID, &workspaceID, &taskID, &workflowID, &correlationID, &causationID); err != nil {
		return err
	}
	callback := map[string]any{"callback_id": "cb_" + executionID, "execution_request_id": requestID, "execution_id": executionID, "workspace_id": workspaceID, "provider_key": "conductor", "provider_run_ref": executionID, "runtime_status": status, "attempt_count": attemptNumber, "result": json.RawMessage(output), "error_class": nil, "error_detail": nil, "occurred_at": now.Format(time.RFC3339Nano)}
	if taskID != "" {
		callback["task_id"] = taskID
	}
	if workflowID != "" {
		callback["workflow_instance_id"] = workflowID
	}
	if correlationID != "" {
		callback["correlation_id"] = correlationID
	}
	if causationID != "" {
		callback["causation_id"] = causationID
	}
	if errorClass != "" {
		callback["error_class"] = errorClass
		callback["error_detail"] = errorMessage
	}
	payload, err := json.Marshal(callback)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO callbacks (id, execution_id, status, payload) VALUES (?, ?, ?, ?)`, "cb_"+executionID, executionID, status, payload); err != nil {
		return err
	}
	if s.callbackTarget != "" {
		if _, err := tx.ExecContext(ctx, `INSERT INTO callback_deliveries (callback_id, execution_id, target_url, payload, status, next_attempt_at) VALUES (?, ?, ?, ?, 'pending', CURRENT_TIMESTAMP) ON CONFLICT(callback_id) DO UPDATE SET target_url=excluded.target_url`, "cb_"+executionID, executionID, s.callbackTarget, payload); err != nil {
			return err
		}
	}
	_, err = tx.ExecContext(ctx, `INSERT OR IGNORE INTO execution_events(id, execution_id, event_type, attempt_number, payload) VALUES(?, ?, ?, ?, ?)`, fmt.Sprintf("event_%s:terminal", executionID), executionID, "execution."+status, attemptNumber, payload)
	return err
}

// RecordAttempt stores a non-terminal retry attempt.
func (s *Store) RecordAttempt(ctx context.Context, executionID string, attemptNumber int, errorClass, errorMessage string) error {
	if attemptNumber < 1 {
		return errors.New("attempt number must be positive")
	}
	now := time.Now().UTC()
	_, err := s.db.ExecContext(ctx, `INSERT INTO attempts (id, execution_id, attempt_number, status, error_class, error_message, started_at, finished_at)
	VALUES (?, ?, ?, 'failed', ?, ?, ?, ?) ON CONFLICT(id) DO NOTHING`, fmt.Sprintf("%s:%d", executionID, attemptNumber), executionID, attemptNumber, errorClass, errorMessage, now, now)
	return err
}

func (s *Store) GetExecution(ctx context.Context, id string) (ExecutionRecord, error) {
	var execution ExecutionRecord
	var createdAt, updatedAt string
	err := s.db.QueryRowContext(ctx, `SELECT id, execution_request_id, status, handler_key, handler_version,
	operation_key, connection_id, input, output, error_class, error_message, error_retryable, created_at, updated_at
FROM executions WHERE id = ?`, id).Scan(&execution.ID, &execution.ExecutionRequestID, &execution.Status,
		&execution.HandlerKey, &execution.HandlerVersion, &execution.OperationKey, &execution.ConnectionID,
		&execution.Input, &execution.Output, &execution.ErrorClass, &execution.ErrorMessage, &execution.ErrorRetryable, &createdAt, &updatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return ExecutionRecord{}, fmt.Errorf("%w: %s", ErrNotFound, id)
	}
	if err != nil {
		return ExecutionRecord{}, err
	}
	execution.CreatedAt, err = time.Parse("2006-01-02 15:04:05", createdAt)
	if err != nil {
		return ExecutionRecord{}, err
	}
	execution.UpdatedAt, err = time.Parse("2006-01-02 15:04:05", updatedAt)
	if err != nil {
		return ExecutionRecord{}, err
	}
	return execution, nil
}

func (s *Store) ListExecutionEvents(ctx context.Context, executionID string) ([]ExecutionEventRecord, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id, execution_id, event_type, attempt_number, payload, occurred_at FROM execution_events WHERE execution_id=? ORDER BY occurred_at, id`, executionID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var events []ExecutionEventRecord
	for rows.Next() {
		var e ExecutionEventRecord
		var occurred string
		if err := rows.Scan(&e.ID, &e.ExecutionID, &e.Type, &e.AttemptNumber, &e.Payload, &occurred); err != nil {
			return nil, err
		}
		e.OccurredAt = parseTime(occurred)
		events = append(events, e)
	}
	return events, rows.Err()
}

func formatTime(value time.Time) string {
	if value.IsZero() {
		return ""
	}
	return value.UTC().Format(time.RFC3339Nano)
}

func parseTime(value string) time.Time {
	if value == "" {
		return time.Time{}
	}
	for _, layout := range []string{time.RFC3339Nano, "2006-01-02 15:04:05.999999999-07:00", "2006-01-02 15:04:05"} {
		if t, err := time.Parse(layout, value); err == nil {
			return t.UTC()
		}
	}
	return time.Time{}
}
