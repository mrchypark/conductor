// Package storage owns Conductor's replicated execution state.
package storage

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/mrchypark/conductor/internal/persistence"
	"github.com/mrchypark/rhiza"
)

var ErrNotFound = errors.New("execution not found")

const schemaIdentity = "conductor-rhiza-v1"

type Store struct {
	db             *persistence.DB
	callbackTarget string
	ownsDB         bool
}
type ExecutionRequestRecord struct {
	ID, WorkspaceID, WorkflowKey                                                        string
	WorkflowVersion                                                                     uint64
	WorkflowID, TaskID, NodeKey, HandlerKey, HandlerVersion, OperationKey, ConnectionID string
	CredentialRevision                                                                  int64
	ActorType, ActorID                                                                  string
	SubjectRef, InputRef                                                                []byte
	DeadlineAt                                                                          time.Time
	Input                                                                               []byte
	Timeout                                                                             time.Duration
	MaxAttempts                                                                         int
	RetryBackoff                                                                        time.Duration
	RetryBackoffMode, CorrelationID, CausationID, IdempotencyKey                        string
}
type ExecutionRecord struct {
	ID, ExecutionRequestID, Status, HandlerKey, HandlerVersion, OperationKey, ConnectionID string
	Input, Output                                                                          []byte
	ErrorClass, ErrorMessage                                                               string
	ErrorRetryable                                                                         bool
	CreatedAt, UpdatedAt                                                                   time.Time
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
	ID, ExecutionID, TargetURL string
	ClaimID                    string
	Payload                    []byte
	Status                     string
	AttemptCount               int
	NextAttemptAt              time.Time
	LastError                  string
}

type CallbackRecord struct {
	ID, ExecutionID, Status string
	Payload                 []byte
}
type ExecutionEventRecord struct {
	ID            string          `json:"id"`
	ExecutionID   string          `json:"execution_id"`
	Type          string          `json:"event_type"`
	AttemptNumber int             `json:"attempt_number"`
	Payload       json.RawMessage `json:"payload"`
	OccurredAt    time.Time       `json:"occurred_at"`
}
type DynamicHandlerRecord struct {
	HandlerKey, Version                          string
	ManifestJSON, Source                         []byte
	AllowedModules                               []string
	MaxFrames, MaxStack                          int
	SourceHash                                   string
	Bytecode                                     []byte
	BytecodeHash, BytecodeSourceHash, RuntimeABI string
}
type WASMHandlerRecord struct {
	HandlerKey, Version                     string
	ManifestJSON                            []byte
	ArtifactRef, ManifestDigest, BlobDigest string
	ArtifactSize                            int64
	RuntimeABI                              string
}

func LocalConfig(dir string) persistence.Config {
	return persistence.Config{ClusterID: "conductor-local", NodeID: "n1", DataDir: dir, Members: []rhiza.Member{{ID: "n1"}}}
}
func Open(ctx context.Context, source any) (*Store, error) {
	var cfg persistence.Config
	switch value := source.(type) {
	case persistence.Config:
		cfg = value
	case string:
		cfg = LocalConfig(strings.TrimPrefix(value, "file:"))
	default:
		return nil, errors.New("Rhiza config or data directory is required")
	}
	db, e := persistence.Open(ctx, cfg)
	if e != nil {
		return nil, e
	}
	s, e := New(ctx, db)
	if e != nil {
		db.Close()
		return nil, e
	}
	s.ownsDB = true
	return s, nil
}
func New(ctx context.Context, db *persistence.DB) (*Store, error) {
	if db == nil {
		return nil, errors.New("persistence database is required")
	}
	s := &Store{db: db}
	if e := s.migrate(ctx); e != nil {
		return nil, e
	}
	return s, nil
}
func (s *Store) Close() error {
	if s.ownsDB {
		return s.db.Close()
	}
	return nil
}
func (s *Store) SetCallbackTarget(url string) { s.callbackTarget = url }

// Persistence exposes the shared Rhiza boundary, never its local SQLite handle.
func (s *Store) Persistence() *persistence.DB { return s.db }

func (s *Store) migrate(ctx context.Context) error {
	rows, err := s.db.Query(ctx, `SELECT identity FROM schema_metadata WHERE id=1`)
	if err == nil && len(rows) == 1 {
		id := str(rows[0][0])
		if id != schemaIdentity {
			return fmt.Errorf("unsupported schema identity %q; expected %q", id, schemaIdentity)
		}
		if err := s.ensureExecutionRequestCredentialSchema(ctx); err != nil {
			return err
		}
		return s.ensureWASMHandlerSchema(ctx)
	}
	ddl := []string{
		`CREATE TABLE IF NOT EXISTS schema_metadata(id INTEGER PRIMARY KEY CHECK(id=1),identity TEXT NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS execution_requests(id TEXT PRIMARY KEY,workspace_id TEXT NOT NULL,workflow_key TEXT NOT NULL,workflow_version INTEGER NOT NULL,workflow_instance_id TEXT NOT NULL,task_id TEXT NOT NULL,node_key TEXT NOT NULL,handler_key TEXT NOT NULL,handler_version TEXT NOT NULL,operation_key TEXT NOT NULL,connection_id TEXT NOT NULL,actor_type TEXT NOT NULL,actor_id TEXT NOT NULL,subject_ref BLOB NOT NULL,input_ref BLOB NOT NULL,deadline_at TEXT NOT NULL,input BLOB NOT NULL,timeout_ns INTEGER NOT NULL,max_attempts INTEGER NOT NULL,retry_backoff_ns INTEGER NOT NULL,retry_backoff_mode TEXT NOT NULL,correlation_id TEXT NOT NULL,causation_id TEXT NOT NULL,idempotency_key TEXT NOT NULL,created_at TEXT NOT NULL)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS idx_execution_requests_idempotency ON execution_requests(workspace_id,idempotency_key) WHERE idempotency_key<>''`,
		`CREATE TABLE IF NOT EXISTS execution_request_credentials(execution_request_id TEXT PRIMARY KEY REFERENCES execution_requests(id),connection_id TEXT NOT NULL,credential_revision INTEGER NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS submission_claims(request_id TEXT PRIMARY KEY,claim_id TEXT NOT NULL,created_at TEXT NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS executions(id TEXT PRIMARY KEY,execution_request_id TEXT NOT NULL UNIQUE REFERENCES execution_requests(id),status TEXT NOT NULL,handler_key TEXT NOT NULL,handler_version TEXT NOT NULL,operation_key TEXT NOT NULL,connection_id TEXT NOT NULL,input BLOB NOT NULL,output BLOB NOT NULL,error_class TEXT NOT NULL,error_message TEXT NOT NULL,error_retryable INTEGER NOT NULL,created_at TEXT NOT NULL,updated_at TEXT NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS attempts(id TEXT PRIMARY KEY,execution_id TEXT NOT NULL REFERENCES executions(id),attempt_number INTEGER NOT NULL,status TEXT NOT NULL,error_class TEXT NOT NULL,error_message TEXT NOT NULL,started_at TEXT NOT NULL,finished_at TEXT NOT NULL,UNIQUE(execution_id,attempt_number))`,
		`CREATE TABLE IF NOT EXISTS callbacks(id TEXT PRIMARY KEY,execution_id TEXT NOT NULL REFERENCES executions(id),status TEXT NOT NULL,payload BLOB NOT NULL,created_at TEXT NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS leases(lease_id TEXT PRIMARY KEY,execution_id TEXT NOT NULL UNIQUE REFERENCES executions(id),attempt_number INTEGER NOT NULL,worker_id TEXT NOT NULL,fencing_token INTEGER NOT NULL,expires_at TEXT NOT NULL,created_at TEXT NOT NULL,updated_at TEXT NOT NULL,status TEXT NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS retry_timers(id TEXT PRIMARY KEY,execution_id TEXT NOT NULL REFERENCES executions(id),attempt_number INTEGER NOT NULL,run_at TEXT NOT NULL,status TEXT NOT NULL,UNIQUE(execution_id,attempt_number))`,
		`CREATE TABLE IF NOT EXISTS callback_deliveries(callback_id TEXT PRIMARY KEY REFERENCES callbacks(id),execution_id TEXT NOT NULL REFERENCES executions(id),target_url TEXT NOT NULL,payload BLOB NOT NULL,status TEXT NOT NULL,claim_id TEXT NOT NULL,attempt_count INTEGER NOT NULL,next_attempt_at TEXT NOT NULL,last_error TEXT NOT NULL,created_at TEXT NOT NULL,updated_at TEXT NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS handler_manifests(handler_key TEXT NOT NULL,version TEXT NOT NULL,manifest_json BLOB NOT NULL,created_at TEXT NOT NULL,PRIMARY KEY(handler_key,version))`,
		`CREATE TABLE IF NOT EXISTS connector_manifests(provider_key TEXT PRIMARY KEY,manifest_json BLOB NOT NULL,created_at TEXT NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS execution_events(id TEXT PRIMARY KEY,execution_id TEXT NOT NULL REFERENCES executions(id),event_type TEXT NOT NULL,attempt_number INTEGER NOT NULL,payload BLOB NOT NULL,occurred_at TEXT NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS terminal_claims(execution_id TEXT PRIMARY KEY,claim_id TEXT NOT NULL,status TEXT NOT NULL,payload BLOB NOT NULL,created_at TEXT NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS dynamic_handlers(handler_key TEXT NOT NULL,version TEXT NOT NULL,manifest_json BLOB NOT NULL,source BLOB NOT NULL,allowed_modules_json BLOB NOT NULL,max_frames INTEGER NOT NULL,max_stack INTEGER NOT NULL,source_hash TEXT NOT NULL,created_at TEXT NOT NULL,PRIMARY KEY(handler_key,version))`,
		`CREATE TABLE IF NOT EXISTS dynamic_handler_bytecode(handler_key TEXT NOT NULL,version TEXT NOT NULL,bytecode BLOB NOT NULL,bytecode_hash TEXT NOT NULL,source_hash TEXT NOT NULL,runtime_abi TEXT NOT NULL,created_at TEXT NOT NULL,PRIMARY KEY(handler_key,version),FOREIGN KEY(handler_key,version) REFERENCES dynamic_handlers(handler_key,version) ON DELETE CASCADE)`,
	}
	ss := make([]persistence.Statement, 0, len(ddl)+1)
	for _, q := range ddl {
		ss = append(ss, st(q))
	}
	ss = append(ss, stm(`INSERT OR IGNORE INTO schema_metadata(id,identity) VALUES(1,?)`, schemaIdentity))
	if _, err = s.db.Mutate(ctx, "initialize conductor schema", ss...); err != nil {
		return err
	}
	rows, err = s.db.Query(ctx, `SELECT identity FROM schema_metadata WHERE id=1`)
	if err != nil || len(rows) != 1 || str(rows[0][0]) != schemaIdentity {
		return errors.New("conductor schema initialization was not authoritative")
	}
	if err := s.ensureExecutionRequestCredentialSchema(ctx); err != nil {
		return err
	}
	return s.ensureWASMHandlerSchema(ctx)
}

// Execution credentials are a companion record so requests keep the original
// schema while each new submission pins the connection revision it may use.
// Existing requests predate revision pinning and therefore read as revision 0.
func (s *Store) ensureExecutionRequestCredentialSchema(ctx context.Context) error {
	_, err := s.db.Mutate(ctx, "initialize execution credential schema",
		st(`CREATE TABLE IF NOT EXISTS execution_request_credentials(execution_request_id TEXT PRIMARY KEY REFERENCES execution_requests(id),connection_id TEXT NOT NULL,credential_revision INTEGER NOT NULL)`),
		st(`INSERT OR IGNORE INTO execution_request_credentials(execution_request_id,connection_id,credential_revision) SELECT id,connection_id,0 FROM execution_requests`),
	)
	return err
}

func (s *Store) ensureWASMHandlerSchema(ctx context.Context) error {
	rows, err := s.db.Query(ctx, `SELECT name FROM sqlite_master WHERE type='table' AND name='wasm_handlers'`)
	if err == nil && len(rows) == 1 {
		return nil
	}
	_, err = s.db.Mutate(ctx, "initialize wasm handler schema", st(`CREATE TABLE IF NOT EXISTS wasm_handlers(handler_key TEXT NOT NULL,version TEXT NOT NULL,manifest_json BLOB NOT NULL,artifact_ref TEXT NOT NULL,manifest_digest TEXT NOT NULL,blob_digest TEXT NOT NULL,artifact_size INTEGER NOT NULL,runtime_abi TEXT NOT NULL,created_at TEXT NOT NULL,PRIMARY KEY(handler_key,version))`))
	return err
}

func (s *Store) SaveHandlerManifest(ctx context.Context, key, version string, manifest any) error {
	if key == "" || version == "" {
		return errors.New("handler key and version are required")
	}
	b, e := json.Marshal(manifest)
	if e != nil {
		return e
	}
	_, e = s.db.Mutate(ctx, "save handler manifest", stm(`INSERT INTO handler_manifests(handler_key,version,manifest_json,created_at) VALUES(?,?,?,?) ON CONFLICT(handler_key,version) DO UPDATE SET manifest_json=excluded.manifest_json`, key, version, b, now()))
	return e
}
func (s *Store) SaveConnectorManifest(ctx context.Context, key string, manifest any) error {
	if key == "" {
		return errors.New("provider key is required")
	}
	b, e := json.Marshal(manifest)
	if e != nil {
		return e
	}
	_, e = s.db.Mutate(ctx, "save connector manifest", stm(`INSERT INTO connector_manifests(provider_key,manifest_json,created_at) VALUES(?,?,?) ON CONFLICT(provider_key) DO UPDATE SET manifest_json=excluded.manifest_json`, key, b, now()))
	return e
}
func (s *Store) SaveDynamicHandler(ctx context.Context, r DynamicHandlerRecord) (bool, error) {
	if r.HandlerKey == "" || r.Version == "" || len(r.ManifestJSON) == 0 || len(r.Source) == 0 || r.SourceHash == "" {
		return false, errors.New("dynamic handler key, version, manifest, source, and hash are required")
	}
	mods, e := json.Marshal(r.AllowedModules)
	if e != nil {
		return false, e
	}
	old, e := s.dynamic(ctx, r.HandlerKey, r.Version)
	if e == nil {
		return false, compareDynamic(old, r, mods)
	}
	if !errors.Is(e, ErrNotFound) {
		return false, e
	}
	_, e = s.db.Mutate(ctx, "save dynamic handler", stm(`INSERT OR IGNORE INTO dynamic_handlers(handler_key,version,manifest_json,source,allowed_modules_json,max_frames,max_stack,source_hash,created_at) VALUES(?,?,?,?,?,?,?,?,?)`, r.HandlerKey, r.Version, r.ManifestJSON, r.Source, mods, r.MaxFrames, r.MaxStack, r.SourceHash, now()))
	if e != nil {
		return false, e
	}
	old, e = s.dynamic(ctx, r.HandlerKey, r.Version)
	if e != nil {
		return false, e
	}
	return true, compareDynamic(old, r, mods)
}
func compareDynamic(old, r DynamicHandlerRecord, mods []byte) error {
	got, _ := json.Marshal(old.AllowedModules)
	if old.SourceHash != r.SourceHash || !bytes.Equal(old.ManifestJSON, r.ManifestJSON) || !bytes.Equal(got, mods) || old.MaxFrames != r.MaxFrames || old.MaxStack != r.MaxStack {
		return errors.New("dynamic handler version already exists with different definition")
	}
	return nil
}
func (s *Store) dynamic(ctx context.Context, key, version string) (DynamicHandlerRecord, error) {
	rows, e := s.db.Query(ctx, `SELECT manifest_json,source,allowed_modules_json,max_frames,max_stack,source_hash FROM dynamic_handlers WHERE handler_key=? AND version=?`, key, version)
	if e != nil {
		return DynamicHandlerRecord{}, e
	}
	if len(rows) == 0 {
		return DynamicHandlerRecord{}, ErrNotFound
	}
	x := rows[0]
	r := DynamicHandlerRecord{HandlerKey: key, Version: version, ManifestJSON: blob(x[0]), Source: blob(x[1]), MaxFrames: num(x[3]), MaxStack: num(x[4]), SourceHash: str(x[5])}
	if e = json.Unmarshal(blob(x[2]), &r.AllowedModules); e != nil {
		return r, e
	}
	return r, nil
}
func (s *Store) DeleteDynamicHandler(ctx context.Context, key, version, hash string) error {
	if key == "" || version == "" || hash == "" {
		return errors.New("dynamic handler key, version, and hash are required")
	}
	_, e := s.db.Mutate(ctx, "delete dynamic handler", stm(`DELETE FROM dynamic_handlers WHERE handler_key=? AND version=? AND source_hash=?`, key, version, hash))
	return e
}
func (s *Store) SaveDynamicHandlerBytecode(ctx context.Context, key, version string, code []byte, codeHash, sourceHash, abi string) error {
	if key == "" || version == "" || len(code) == 0 || codeHash == "" || sourceHash == "" || abi == "" {
		return errors.New("dynamic handler bytecode, hashes, and runtime ABI are required")
	}
	_, e := s.db.Mutate(ctx, "save dynamic handler bytecode", stm(`INSERT INTO dynamic_handler_bytecode(handler_key,version,bytecode,bytecode_hash,source_hash,runtime_abi,created_at) VALUES(?,?,?,?,?,?,?) ON CONFLICT(handler_key,version) DO UPDATE SET bytecode=excluded.bytecode,bytecode_hash=excluded.bytecode_hash,source_hash=excluded.source_hash,runtime_abi=excluded.runtime_abi,created_at=excluded.created_at`, key, version, code, codeHash, sourceHash, abi, now()))
	return e
}
func (s *Store) ListDynamicHandlers(ctx context.Context) ([]DynamicHandlerRecord, error) {
	rows, e := s.db.Query(ctx, `SELECT d.handler_key,d.version,d.manifest_json,d.source,d.allowed_modules_json,d.max_frames,d.max_stack,d.source_hash,COALESCE(b.bytecode,''),COALESCE(b.bytecode_hash,''),COALESCE(b.source_hash,''),COALESCE(b.runtime_abi,'') FROM dynamic_handlers d LEFT JOIN dynamic_handler_bytecode b ON b.handler_key=d.handler_key AND b.version=d.version ORDER BY d.handler_key,d.version`)
	if e != nil {
		return nil, e
	}
	out := make([]DynamicHandlerRecord, 0, len(rows))
	for _, x := range rows {
		r := DynamicHandlerRecord{HandlerKey: str(x[0]), Version: str(x[1]), ManifestJSON: blob(x[2]), Source: blob(x[3]), MaxFrames: num(x[5]), MaxStack: num(x[6]), SourceHash: str(x[7]), Bytecode: blob(x[8]), BytecodeHash: str(x[9]), BytecodeSourceHash: str(x[10]), RuntimeABI: str(x[11])}
		if e = json.Unmarshal(blob(x[4]), &r.AllowedModules); e != nil {
			return nil, e
		}
		out = append(out, r)
	}
	return out, nil
}

func (s *Store) SaveWASMHandler(ctx context.Context, r WASMHandlerRecord) (bool, error) {
	if r.HandlerKey == "" || r.Version == "" || len(r.ManifestJSON) == 0 || r.ArtifactRef == "" || r.ManifestDigest == "" || r.BlobDigest == "" || r.ArtifactSize <= 0 || r.RuntimeABI == "" {
		return false, errors.New("wasm handler metadata is incomplete")
	}
	old, err := s.wasmHandler(ctx, r.HandlerKey, r.Version)
	if err == nil {
		return false, compareWASMHandler(old, r)
	}
	if !errors.Is(err, ErrNotFound) {
		return false, err
	}
	_, err = s.db.Mutate(ctx, "save wasm handler", stm(`INSERT OR IGNORE INTO wasm_handlers(handler_key,version,manifest_json,artifact_ref,manifest_digest,blob_digest,artifact_size,runtime_abi,created_at) VALUES(?,?,?,?,?,?,?,?,?)`, r.HandlerKey, r.Version, r.ManifestJSON, r.ArtifactRef, r.ManifestDigest, r.BlobDigest, r.ArtifactSize, r.RuntimeABI, now()))
	if err != nil {
		return false, err
	}
	old, err = s.wasmHandler(ctx, r.HandlerKey, r.Version)
	if err != nil {
		return false, err
	}
	return true, compareWASMHandler(old, r)
}

func compareWASMHandler(old, current WASMHandlerRecord) error {
	if !bytes.Equal(old.ManifestJSON, current.ManifestJSON) || old.ArtifactRef != current.ArtifactRef || old.ManifestDigest != current.ManifestDigest || old.BlobDigest != current.BlobDigest || old.ArtifactSize != current.ArtifactSize || old.RuntimeABI != current.RuntimeABI {
		return errors.New("wasm handler version already exists with different definition")
	}
	return nil
}

func (s *Store) wasmHandler(ctx context.Context, key, version string) (WASMHandlerRecord, error) {
	rows, err := s.db.Query(ctx, `SELECT manifest_json,artifact_ref,manifest_digest,blob_digest,artifact_size,runtime_abi FROM wasm_handlers WHERE handler_key=? AND version=?`, key, version)
	if err != nil {
		return WASMHandlerRecord{}, err
	}
	if len(rows) == 0 {
		return WASMHandlerRecord{}, ErrNotFound
	}
	x := rows[0]
	return WASMHandlerRecord{HandlerKey: key, Version: version, ManifestJSON: blob(x[0]), ArtifactRef: str(x[1]), ManifestDigest: str(x[2]), BlobDigest: str(x[3]), ArtifactSize: int64(num(x[4])), RuntimeABI: str(x[5])}, nil
}

func (s *Store) DeleteWASMHandler(ctx context.Context, key, version, manifestDigest string) error {
	if key == "" || version == "" || manifestDigest == "" {
		return errors.New("wasm handler key, version, and manifest digest are required")
	}
	_, err := s.db.Mutate(ctx, "delete wasm handler", stm(`DELETE FROM wasm_handlers WHERE handler_key=? AND version=? AND manifest_digest=?`, key, version, manifestDigest))
	return err
}

func (s *Store) ListWASMHandlers(ctx context.Context) ([]WASMHandlerRecord, error) {
	rows, err := s.db.Query(ctx, `SELECT handler_key,version,manifest_json,artifact_ref,manifest_digest,blob_digest,artifact_size,runtime_abi FROM wasm_handlers ORDER BY handler_key,version`)
	if err != nil {
		return nil, err
	}
	out := make([]WASMHandlerRecord, 0, len(rows))
	for _, x := range rows {
		out = append(out, WASMHandlerRecord{HandlerKey: str(x[0]), Version: str(x[1]), ManifestJSON: blob(x[2]), ArtifactRef: str(x[3]), ManifestDigest: str(x[4]), BlobDigest: str(x[5]), ArtifactSize: int64(num(x[6])), RuntimeABI: str(x[7])})
	}
	return out, nil
}

func (s *Store) CreateExecution(ctx context.Context, r ExecutionRequestRecord, executionID string) (bool, error) {
	claim, e := opaque()
	if e != nil {
		return false, e
	}
	t := now()
	status := "queued"
	if r.HandlerKey == "connector.credentials.invoke" && r.HandlerVersion == "2" {
		status = "inline_pending"
	}
	ss := []persistence.Statement{
		stm(`INSERT OR IGNORE INTO submission_claims(request_id,claim_id,created_at) VALUES(?,?,?)`, r.ID, claim, t),
		stm(`INSERT INTO execution_requests(id,workspace_id,workflow_key,workflow_version,workflow_instance_id,task_id,node_key,handler_key,handler_version,operation_key,connection_id,actor_type,actor_id,subject_ref,input_ref,deadline_at,input,timeout_ns,max_attempts,retry_backoff_ns,retry_backoff_mode,correlation_id,causation_id,idempotency_key,created_at) SELECT ?,?,?,?,? ,?,?,?,?,? ,?,?,?,?,? ,?,?,?,?,? ,?,?,?,?,? WHERE EXISTS(SELECT 1 FROM submission_claims WHERE request_id=? AND claim_id=?)`, r.ID, r.WorkspaceID, r.WorkflowKey, int64(r.WorkflowVersion), r.WorkflowID, r.TaskID, r.NodeKey, r.HandlerKey, r.HandlerVersion, r.OperationKey, r.ConnectionID, r.ActorType, r.ActorID, encodeBlob(r.SubjectRef), encodeBlob(r.InputRef), ft(r.DeadlineAt), encodeBlob(r.Input), r.Timeout.Nanoseconds(), r.MaxAttempts, r.RetryBackoff.Nanoseconds(), r.RetryBackoffMode, r.CorrelationID, r.CausationID, r.IdempotencyKey, t, r.ID, claim),
		stm(`INSERT INTO execution_request_credentials(execution_request_id,connection_id,credential_revision) SELECT ?,?,? WHERE EXISTS(SELECT 1 FROM submission_claims WHERE request_id=? AND claim_id=?)`, r.ID, r.ConnectionID, r.CredentialRevision, r.ID, claim),
		stm(`INSERT INTO executions(id,execution_request_id,status,handler_key,handler_version,operation_key,connection_id,input,output,error_class,error_message,error_retryable,created_at,updated_at) SELECT ?,?,?,?,?,?,?,?,'','','',0,?,? WHERE EXISTS(SELECT 1 FROM submission_claims WHERE request_id=? AND claim_id=?)`, executionID, r.ID, status, r.HandlerKey, r.HandlerVersion, r.OperationKey, r.ConnectionID, encodeBlob(r.Input), t, t, r.ID, claim),
		stm(`INSERT INTO execution_events(id,execution_id,event_type,attempt_number,payload,occurred_at) SELECT ?,?,'execution.requested',0,'{}',? WHERE EXISTS(SELECT 1 FROM submission_claims WHERE request_id=? AND claim_id=?)`, `event_`+executionID+`:requested`, executionID, t, r.ID, claim)}
	if _, e = s.db.Mutate(ctx, "create execution "+r.ID+" "+claim, ss...); e != nil {
		return false, e
	}
	rows, e := s.db.Query(ctx, `SELECT claim_id FROM submission_claims WHERE request_id=?`, r.ID)
	if e != nil {
		return false, e
	}
	if len(rows) != 1 {
		return false, errors.New("execution submission outcome is unknown")
	}
	return str(rows[0][0]) == claim, nil
}
func (s *Store) GetExecutionRequest(ctx context.Context, id string) (ExecutionRequestRecord, error) {
	rows, e := s.db.Query(ctx, `SELECT r.id,r.workspace_id,r.workflow_key,r.workflow_version,r.workflow_instance_id,r.task_id,r.node_key,r.handler_key,r.handler_version,r.operation_key,r.connection_id,r.actor_type,r.actor_id,r.subject_ref,r.input_ref,r.deadline_at,r.input,r.timeout_ns,r.max_attempts,r.retry_backoff_ns,r.retry_backoff_mode,r.correlation_id,r.causation_id,r.idempotency_key,COALESCE(c.credential_revision,0) FROM execution_requests r LEFT JOIN execution_request_credentials c ON c.execution_request_id=r.id WHERE r.id=?`, id)
	if e != nil {
		return ExecutionRequestRecord{}, e
	}
	if len(rows) == 0 {
		return ExecutionRequestRecord{}, fmt.Errorf("%w: %s", ErrNotFound, id)
	}
	x := rows[0]
	r := ExecutionRequestRecord{ID: str(x[0]), WorkspaceID: str(x[1]), WorkflowKey: str(x[2]), WorkflowVersion: uint64(i64(x[3])), WorkflowID: str(x[4]), TaskID: str(x[5]), NodeKey: str(x[6]), HandlerKey: str(x[7]), HandlerVersion: str(x[8]), OperationKey: str(x[9]), ConnectionID: str(x[10]), ActorType: str(x[11]), ActorID: str(x[12]), SubjectRef: blob(x[13]), InputRef: blob(x[14]), DeadlineAt: pt(str(x[15])), Input: blob(x[16]), Timeout: time.Duration(i64(x[17])), MaxAttempts: num(x[18]), RetryBackoff: time.Duration(i64(x[19])), RetryBackoffMode: str(x[20]), CorrelationID: str(x[21]), CausationID: str(x[22]), IdempotencyKey: str(x[23]), CredentialRevision: i64(x[24])}
	return r, nil
}
func (s *Store) FindExecutionRequestByIdempotency(ctx context.Context, ws, key string) (ExecutionRequestRecord, error) {
	rows, e := s.db.Query(ctx, `SELECT id FROM execution_requests WHERE workspace_id=? AND idempotency_key=?`, ws, key)
	if e != nil {
		return ExecutionRequestRecord{}, e
	}
	if len(rows) == 0 {
		return ExecutionRequestRecord{}, ErrNotFound
	}
	return s.GetExecutionRequest(ctx, str(rows[0][0]))
}
func (s *Store) GetExecution(ctx context.Context, id string) (ExecutionRecord, error) {
	rows, e := s.db.Query(ctx, `SELECT id,execution_request_id,status,handler_key,handler_version,operation_key,connection_id,input,output,error_class,error_message,error_retryable,created_at,updated_at FROM executions WHERE id=?`, id)
	if e != nil {
		return ExecutionRecord{}, e
	}
	if len(rows) == 0 {
		return ExecutionRecord{}, fmt.Errorf("%w: %s", ErrNotFound, id)
	}
	x := rows[0]
	return ExecutionRecord{ID: str(x[0]), ExecutionRequestID: str(x[1]), Status: str(x[2]), HandlerKey: str(x[3]), HandlerVersion: str(x[4]), OperationKey: str(x[5]), ConnectionID: str(x[6]), Input: blob(x[7]), Output: blob(x[8]), ErrorClass: str(x[9]), ErrorMessage: str(x[10]), ErrorRetryable: num(x[11]) != 0, CreatedAt: pt(str(x[12])), UpdatedAt: pt(str(x[13]))}, nil
}

func (s *Store) AcquireLease(ctx context.Context, executionID, worker string, ttl time.Duration, attempt int) (LeaseRecord, error) {
	return s.acquireLease(ctx, executionID, worker, ttl, attempt, false)
}

// AcquireInlineLease claims a newly submitted synchronous-only execution once.
func (s *Store) AcquireInlineLease(ctx context.Context, executionID string, ttl time.Duration) (LeaseRecord, error) {
	return s.acquireLease(ctx, executionID, "inline:"+executionID, ttl, 1, true)
}

func (s *Store) acquireLease(ctx context.Context, executionID, worker string, ttl time.Duration, attempt int, inline bool) (LeaseRecord, error) {
	if ttl <= 0 {
		ttl = time.Minute
	}
	if attempt < 1 {
		attempt = 1
	}
	id, e := opaque()
	if e != nil {
		return LeaseRecord{}, e
	}
	id = "lease_" + id
	n := time.Now().UTC()
	t, x := ft(n), ft(n.Add(ttl))
	claimable := "e.status IN('queued','retry_wait') AND NOT (e.handler_key='connector.credentials.invoke' AND e.handler_version='2')"
	if inline {
		claimable = "e.status='inline_pending' AND e.handler_key='connector.credentials.invoke' AND e.handler_version='2'"
	}
	ss := []persistence.Statement{
		stm(`INSERT INTO leases(lease_id,execution_id,attempt_number,worker_id,fencing_token,expires_at,created_at,updated_at,status) SELECT ?,e.id,?,?,COALESCE((SELECT fencing_token FROM leases WHERE execution_id=e.id),0)+1,?,?,?,'active' FROM executions e WHERE e.id=? AND `+claimable+` ON CONFLICT(execution_id) DO UPDATE SET lease_id=excluded.lease_id,attempt_number=excluded.attempt_number,worker_id=excluded.worker_id,fencing_token=excluded.fencing_token,expires_at=excluded.expires_at,updated_at=excluded.updated_at,status='active'`, id, attempt, worker, x, t, t, executionID),
		stm(`UPDATE executions SET status='running',updated_at=? WHERE id=? AND status IN('queued','retry_wait','inline_pending') AND EXISTS(SELECT 1 FROM leases WHERE execution_id=? AND lease_id=?)`, t, executionID, executionID, id),
		stm(`UPDATE retry_timers SET status='claimed' WHERE execution_id=? AND status='scheduled' AND EXISTS(SELECT 1 FROM leases WHERE execution_id=? AND lease_id=?)`, executionID, executionID, id),
		stm(`INSERT OR IGNORE INTO execution_events(id,execution_id,event_type,attempt_number,payload,occurred_at) SELECT 'event_'||?||':started:'||fencing_token,?,'execution.started',?,'{}',? FROM leases WHERE execution_id=? AND lease_id=?`, executionID, executionID, attempt, t, executionID, id)}
	if _, e = s.db.Mutate(ctx, "acquire lease "+id, ss...); e != nil {
		return LeaseRecord{}, e
	}
	l, e := s.Lease(ctx, id)
	if errors.Is(e, ErrNotFound) {
		if _, g := s.GetExecution(ctx, executionID); errors.Is(g, ErrNotFound) {
			return l, ErrNotFound
		}
		return l, fmt.Errorf("execution %s is not claimable", executionID)
	}
	return l, e
}
func (s *Store) ClaimNext(ctx context.Context, worker string, ttl time.Duration) (ExecutionRequestRecord, LeaseRecord, error) {
	rows, e := s.db.Query(ctx, `SELECT e.execution_request_id,COALESCE((SELECT attempt_number+1 FROM retry_timers t WHERE t.execution_id=e.id AND t.status='scheduled'),(SELECT attempt_number+1 FROM leases l WHERE l.execution_id=e.id AND l.status='expired'),1),e.id FROM executions e WHERE NOT (e.handler_key='connector.credentials.invoke' AND e.handler_version='2') AND (e.status='queued' OR(e.status='retry_wait' AND EXISTS(SELECT 1 FROM retry_timers t WHERE t.execution_id=e.id AND t.status='scheduled' AND t.run_at<=?))) ORDER BY e.created_at,e.id LIMIT 1`, now())
	if e != nil {
		return ExecutionRequestRecord{}, LeaseRecord{}, e
	}
	if len(rows) == 0 {
		return ExecutionRequestRecord{}, LeaseRecord{}, ErrNotFound
	}
	x := rows[0]
	l, e := s.AcquireLease(ctx, str(x[2]), worker, ttl, num(x[1]))
	if e != nil {
		return ExecutionRequestRecord{}, LeaseRecord{}, e
	}
	r, e := s.GetExecutionRequest(ctx, str(x[0]))
	return r, l, e
}
func (s *Store) Lease(ctx context.Context, id string) (LeaseRecord, error) {
	rows, e := s.db.Query(ctx, `SELECT lease_id,execution_id,attempt_number,worker_id,fencing_token,expires_at FROM leases WHERE lease_id=?`, id)
	return leaseRows(rows, e)
}
func (s *Store) ActiveLease(ctx context.Context, id string) (LeaseRecord, error) {
	rows, e := s.db.Query(ctx, `SELECT lease_id,execution_id,attempt_number,worker_id,fencing_token,expires_at FROM leases WHERE execution_id=? AND status='active' AND expires_at>?`, id, now())
	return leaseRows(rows, e)
}
func leaseRows(rows [][]any, e error) (LeaseRecord, error) {
	if e != nil {
		return LeaseRecord{}, e
	}
	if len(rows) == 0 {
		return LeaseRecord{}, ErrNotFound
	}
	x := rows[0]
	return LeaseRecord{ID: str(x[0]), ExecutionID: str(x[1]), AttemptNumber: num(x[2]), WorkerID: str(x[3]), FencingToken: i64(x[4]), ExpiresAt: pt(str(x[5]))}, nil
}
func (s *Store) HeartbeatLease(ctx context.Context, id string, token int64, ttl time.Duration) error {
	if ttl <= 0 {
		ttl = time.Minute
	}
	n := time.Now().UTC()
	x := ft(n.Add(ttl))
	_, e := s.db.Mutate(ctx, fmt.Sprintf("heartbeat %s %d %s", id, token, ft(n)), stm(`UPDATE leases SET expires_at=?,updated_at=? WHERE lease_id=? AND fencing_token=? AND status='active' AND expires_at>?`, x, ft(n), id, token, ft(n)))
	if e != nil {
		return e
	}
	l, e := s.Lease(ctx, id)
	if e != nil || l.FencingToken != token || ft(l.ExpiresAt) != x {
		return errors.New("lease is stale or expired")
	}
	return nil
}
func (s *Store) ReleaseLease(ctx context.Context, id string, token int64) error {
	_, e := s.db.Mutate(ctx, fmt.Sprintf("release %s %d", id, token), stm(`UPDATE leases SET status='released',updated_at=? WHERE lease_id=? AND fencing_token=? AND status='active'`, now(), id, token))
	if e != nil {
		return e
	}
	l, e := s.Lease(ctx, id)
	if e != nil || l.FencingToken != token {
		return errors.New("lease is stale or already released")
	}
	return nil
}

func (s *Store) RecordAttempt(ctx context.Context, id string, a int, class, msg string) error {
	if a < 1 {
		return errors.New("attempt number must be positive")
	}
	t := now()
	_, e := s.db.Mutate(ctx, fmt.Sprintf("attempt %s %d", id, a), stm(`INSERT OR IGNORE INTO attempts(id,execution_id,attempt_number,status,error_class,error_message,started_at,finished_at) VALUES(?,?,?,'failed',?,?,?,?)`, fmt.Sprintf("%s:%d", id, a), id, a, class, msg, t, t))
	return e
}
func (s *Store) ScheduleRetry(ctx context.Context, id string, a int, run time.Time) error {
	if a < 1 {
		return errors.New("attempt number must be positive")
	}
	t := now()
	_, e := s.db.Mutate(ctx, fmt.Sprintf("retry %s %d", id, a), stm(`INSERT INTO retry_timers(id,execution_id,attempt_number,run_at,status) SELECT ?,?,?,?,'scheduled' WHERE EXISTS(SELECT 1 FROM executions WHERE id=? AND status='running') ON CONFLICT(execution_id,attempt_number) DO UPDATE SET run_at=excluded.run_at,status='scheduled'`, fmt.Sprintf("retry_%s_%d", id, a), id, a, ft(run), id), stm(`UPDATE executions SET status='retry_wait',updated_at=? WHERE id=? AND status='running'`, t, id), stm(`INSERT OR IGNORE INTO execution_events(id,execution_id,event_type,attempt_number,payload,occurred_at) SELECT ?,?,'execution.retrying',?,'{}',? WHERE EXISTS(SELECT 1 FROM executions WHERE id=? AND status='retry_wait')`, fmt.Sprintf("event_%s:retry:%d", id, a), id, a, t, id))
	if e != nil {
		return e
	}
	x, e := s.GetExecution(ctx, id)
	if e != nil {
		return e
	}
	if x.Status != "retry_wait" {
		return fmt.Errorf("execution %s is not running", id)
	}
	return nil
}
func (s *Store) RetryExecutionWithLease(ctx context.Context, lease string, token int64, id string, a int, class, msg string, run time.Time) error {
	l, e := s.ActiveLease(ctx, id)
	if e != nil || l.ID != lease || l.FencingToken != token || l.AttemptNumber != a {
		return errors.New("lease is stale or expired")
	}
	if e = s.RecordAttempt(ctx, id, a, class, msg); e != nil {
		return e
	}
	return s.ScheduleRetry(ctx, id, a, run)
}

func (s *Store) CompleteExecution(ctx context.Context, id, status string, out []byte, class, msg string) error {
	return s.CompleteExecutionWithAttempt(ctx, id, status, out, class, msg, false, 1)
}
func (s *Store) CompleteExecutionWithAttempt(ctx context.Context, id, status string, out []byte, class, msg string, retry bool, a int) error {
	return s.complete(ctx, id, status, out, class, msg, retry, a, "", 0, false, time.Now().UTC())
}
func (s *Store) CompleteExecutionWithLease(ctx context.Context, lease string, token int64, id, status string, out []byte, class, msg string, retry bool, a int) error {
	return s.complete(ctx, id, status, out, class, msg, retry, a, lease, token, false, time.Now().UTC())
}
func (s *Store) complete(ctx context.Context, id, status string, out []byte, class, msg string, retry bool, a int, lease string, token int64, expired bool, n time.Time) error {
	if status != "succeeded" && status != "failed" && status != "timed_out" && status != "cancelled" {
		return fmt.Errorf("invalid terminal status %q", status)
	}
	if a < 1 {
		return errors.New("attempt number must be positive")
	}
	rows, e := s.db.Query(ctx, `SELECT e.execution_request_id,r.workspace_id,r.task_id,r.workflow_instance_id,r.correlation_id,r.causation_id,r.handler_key,r.handler_version FROM executions e JOIN execution_requests r ON r.id=e.execution_request_id WHERE e.id=?`, id)
	if e != nil {
		return e
	}
	if len(rows) == 0 {
		return ErrNotFound
	}
	x := rows[0]
	request, ws, task, wf, corr, cause, handlerKey, handlerVersion := str(x[0]), str(x[1]), str(x[2]), str(x[3]), str(x[4]), str(x[5]), str(x[6]), str(x[7])
	t := ft(n)
	cb := map[string]any{"callback_id": "cb_" + id, "execution_request_id": request, "execution_id": id, "workspace_id": ws, "provider_key": handlerKey, "provider_run_ref": id, "handler_version": handlerVersion, "runtime_status": status, "attempt_count": a, "result": json.RawMessage(out), "error_class": nil, "error_detail": nil, "occurred_at": t}
	if task != "" {
		cb["task_id"] = task
	}
	if wf != "" {
		cb["workflow_instance_id"] = wf
	}
	if corr != "" {
		cb["correlation_id"] = corr
	}
	if cause != "" {
		cb["causation_id"] = cause
	}
	if class != "" {
		cb["error_class"] = class
		cb["error_detail"] = msg
	}
	payload, e := json.Marshal(cb)
	if e != nil {
		return e
	}
	claim := lease
	if claim == "" {
		claim = "unleased"
	}
	guard := `?=''`
	gargs := []any{lease}
	if lease != "" {
		op := ">"
		if expired {
			op = "<="
		}
		guard = `EXISTS(SELECT 1 FROM leases WHERE lease_id=? AND execution_id=? AND fencing_token=? AND status='active' AND expires_at` + op + `?)`
		gargs = []any{lease, id, token, t}
	}
	q := `INSERT OR IGNORE INTO terminal_claims(execution_id,claim_id,status,payload,created_at) SELECT ?,?,?,?,? WHERE EXISTS(SELECT 1 FROM executions WHERE id=? AND status='running') AND ` + guard
	encodedPayload := encodeBlob(payload)
	args := []any{id, claim, status, encodedPayload, t, id}
	args = append(args, gargs...)
	attemptStatus := status
	if status != "succeeded" {
		attemptStatus = "failed"
	}
	ss := []persistence.Statement{stm(q, args...), stm(`INSERT OR IGNORE INTO attempts(id,execution_id,attempt_number,status,error_class,error_message,started_at,finished_at) SELECT ?,?,?,?,?,?,?,? WHERE EXISTS(SELECT 1 FROM terminal_claims WHERE execution_id=? AND claim_id=? AND status=? AND payload=?)`, fmt.Sprintf("%s:%d", id, a), id, a, attemptStatus, class, msg, t, t, id, claim, status, encodedPayload), stm(`UPDATE executions SET status=?,output=?,error_class=?,error_message=?,error_retryable=?,updated_at=? WHERE id=? AND status='running' AND EXISTS(SELECT 1 FROM terminal_claims WHERE execution_id=? AND claim_id=? AND status=? AND payload=?)`, status, encodeBlob(out), class, msg, boolNum(retry), t, id, id, claim, status, encodedPayload), stm(`INSERT OR IGNORE INTO callbacks(id,execution_id,status,payload,created_at) SELECT ?,?,?,?,? WHERE EXISTS(SELECT 1 FROM terminal_claims WHERE execution_id=? AND claim_id=? AND status=? AND payload=?)`, `cb_`+id, id, status, encodedPayload, t, id, claim, status, encodedPayload), stm(`INSERT OR IGNORE INTO execution_events(id,execution_id,event_type,attempt_number,payload,occurred_at) SELECT ?,?,'execution.'||?,?,?,? WHERE EXISTS(SELECT 1 FROM terminal_claims WHERE execution_id=? AND claim_id=? AND status=? AND payload=?)`, `event_`+id+`:terminal`, id, status, a, encodedPayload, t, id, claim, status, encodedPayload)}
	if expired {
		ss = append(ss, stm(`UPDATE leases SET status='expired',updated_at=? WHERE lease_id=? AND fencing_token=? AND status='active' AND expires_at<=?`, t, lease, token, t))
	}
	if s.callbackTarget != "" {
		ss = append(ss, stm(`INSERT INTO callback_deliveries(callback_id,execution_id,target_url,payload,status,claim_id,attempt_count,next_attempt_at,last_error,created_at,updated_at) SELECT ?,?,?,?,'pending','',0,?,'',?,? WHERE EXISTS(SELECT 1 FROM terminal_claims WHERE execution_id=? AND claim_id=? AND status=? AND payload=?) ON CONFLICT(callback_id) DO UPDATE SET target_url=excluded.target_url`, `cb_`+id, id, s.callbackTarget, payload, t, t, t, id, claim, status, encodedPayload))
	}
	if _, e = s.db.Mutate(ctx, "complete "+id+" "+claim, ss...); e != nil {
		if status == "succeeded" && errors.Is(e, rhiza.ErrInvalidRequest) && strings.Contains(e.Error(), "encoded command exceeds") {
			return s.complete(ctx, id, "failed", nil, "output_too_large", "handler output exceeds the durable command size limit", false, a, lease, token, expired, n)
		}
		return e
	}
	rows, e = s.db.Query(ctx, `SELECT claim_id,status,payload FROM terminal_claims WHERE execution_id=?`, id)
	if e != nil {
		return e
	}
	if len(rows) != 1 || str(rows[0][0]) != claim {
		return errors.New("execution is already terminal or lease is stale")
	}
	return nil
}
func (s *Store) RecoverExpiredLeases(ctx context.Context, n time.Time) error {
	pending, e := s.db.Query(ctx, `SELECT id FROM executions WHERE status='inline_pending' AND handler_key='connector.credentials.invoke' AND handler_version='2' AND updated_at<=?`, ft(n.Add(-time.Minute)))
	if e != nil {
		return e
	}
	for _, row := range pending {
		id := str(row[0])
		lease, err := s.AcquireInlineLease(ctx, id, time.Minute)
		if err != nil {
			current, getErr := s.GetExecution(ctx, id)
			if getErr != nil || current.Status == "inline_pending" {
				return err
			}
			continue
		}
		if err := s.CompleteExecutionWithLease(ctx, lease.ID, lease.FencingToken, id, "failed", nil, "outcome_unknown", "inline execution was interrupted before its outcome was recorded", false, 1); err != nil {
			return err
		}
	}
	rows, e := s.db.Query(ctx, `SELECT e.id,l.lease_id,l.fencing_token,l.attempt_number,CASE WHEN e.handler_key='connector.credentials.invoke' AND e.handler_version='2' THEN 1 ELSE r.max_attempts END FROM executions e JOIN leases l ON l.execution_id=e.id JOIN execution_requests r ON r.id=e.execution_request_id WHERE e.status='running' AND l.status='active' AND l.expires_at<=?`, ft(n))
	if e != nil {
		return e
	}
	for _, x := range rows {
		id, lease := str(x[0]), str(x[1])
		token := i64(x[2])
		a, max := num(x[3]), num(x[4])
		if max < 1 {
			max = 1
		}
		if a >= max {
			e = s.complete(ctx, id, "failed", nil, "outcome_unknown", "worker lease expired after execution may have produced side effects", false, a, lease, token, true, n)
		} else {
			_, e = s.db.Mutate(ctx, "recover "+lease, stm(`UPDATE leases SET status='expired',updated_at=? WHERE lease_id=? AND fencing_token=? AND status='active' AND expires_at<=?`, ft(n), lease, token, ft(n)), stm(`UPDATE executions SET status='queued',updated_at=? WHERE id=? AND status='running' AND EXISTS(SELECT 1 FROM leases WHERE lease_id=? AND status='expired')`, ft(n), id, lease))
		}
		if e != nil {
			return e
		}
	}
	return nil
}

func (s *Store) EnqueueCallbackDelivery(ctx context.Context, id, url string) error {
	if url == "" {
		return nil
	}
	t := now()
	_, e := s.db.Mutate(ctx, "enqueue callback "+id, stm(`INSERT INTO callback_deliveries(callback_id,execution_id,target_url,payload,status,claim_id,attempt_count,next_attempt_at,last_error,created_at,updated_at) SELECT id,execution_id,?,payload,'pending','',0,?,'',?,? FROM callbacks WHERE id=? ON CONFLICT(callback_id) DO UPDATE SET target_url=excluded.target_url`, url, t, t, t, id))
	return e
}
func (s *Store) DueCallbackDeliveries(ctx context.Context, n time.Time, limit int) ([]CallbackDeliveryRecord, error) {
	if limit <= 0 {
		limit = 20
	}
	claim, e := opaque()
	if e != nil {
		return nil, e
	}
	rows, e := s.db.Query(ctx, `SELECT callback_id FROM callback_deliveries WHERE status IN('pending','retry_wait') AND next_attempt_at<=? ORDER BY next_attempt_at,callback_id LIMIT ?`, ft(n), limit)
	if e != nil || len(rows) == 0 {
		return nil, e
	}
	ss := make([]persistence.Statement, 0, len(rows))
	for _, x := range rows {
		ss = append(ss, stm(`UPDATE callback_deliveries SET status='sending',claim_id=?,updated_at=? WHERE callback_id=? AND status IN('pending','retry_wait') AND next_attempt_at<=?`, claim, ft(n), str(x[0]), ft(n)))
	}
	if _, e = s.db.Mutate(ctx, "claim callbacks "+claim, ss...); e != nil {
		return nil, e
	}
	rows, e = s.db.Query(ctx, `SELECT callback_id,execution_id,target_url,payload,status,attempt_count,next_attempt_at,last_error FROM callback_deliveries WHERE claim_id=? AND status='sending'`, claim)
	if e != nil {
		return nil, e
	}
	out := make([]CallbackDeliveryRecord, 0, len(rows))
	for _, x := range rows {
		out = append(out, CallbackDeliveryRecord{ID: str(x[0]), ExecutionID: str(x[1]), TargetURL: str(x[2]), ClaimID: claim, Payload: blob(x[3]), Status: str(x[4]), AttemptCount: num(x[5]), NextAttemptAt: pt(str(x[6])), LastError: str(x[7])})
	}
	return out, nil
}
func (s *Store) RecoverCallbackDeliveries(ctx context.Context, n time.Time, age time.Duration) error {
	if age <= 0 {
		age = 30 * time.Second
	}
	_, e := s.db.Mutate(ctx, "recover callbacks "+ft(n), stm(`UPDATE callback_deliveries SET status='retry_wait',claim_id='',next_attempt_at=?,last_error='stale delivery claim recovered',updated_at=? WHERE status='sending' AND updated_at<=?`, ft(n), ft(n), ft(n.Add(-age))))
	return e
}
func (s *Store) MarkCallbackDelivery(ctx context.Context, id, claim, status, last string, next time.Time) error {
	if claim == "" {
		return errors.New("callback claim is required")
	}
	t := now()
	_, e := s.db.Mutate(ctx, "mark callback "+id+" "+status+" "+t, stm(`UPDATE callback_deliveries SET status=?,claim_id='',attempt_count=attempt_count+1,last_error=?,next_attempt_at=?,updated_at=? WHERE callback_id=? AND status='sending' AND claim_id=?`, status, last, ft(next), t, id, claim))
	return e
}

func (s *Store) Callback(ctx context.Context, executionID string) (CallbackRecord, error) {
	rows, err := s.db.Query(ctx, `SELECT id,execution_id,status,payload FROM callbacks WHERE execution_id=?`, executionID)
	if err != nil {
		return CallbackRecord{}, err
	}
	if len(rows) == 0 {
		return CallbackRecord{}, ErrNotFound
	}
	return CallbackRecord{ID: str(rows[0][0]), ExecutionID: str(rows[0][1]), Status: str(rows[0][2]), Payload: blob(rows[0][3])}, nil
}

func (s *Store) CallbackDelivery(ctx context.Context, callbackID string) (CallbackDeliveryRecord, error) {
	rows, err := s.db.Query(ctx, `SELECT callback_id,execution_id,target_url,payload,status,attempt_count,next_attempt_at,last_error FROM callback_deliveries WHERE callback_id=?`, callbackID)
	if err != nil {
		return CallbackDeliveryRecord{}, err
	}
	if len(rows) == 0 {
		return CallbackDeliveryRecord{}, ErrNotFound
	}
	x := rows[0]
	return CallbackDeliveryRecord{ID: str(x[0]), ExecutionID: str(x[1]), TargetURL: str(x[2]), Payload: blob(x[3]), Status: str(x[4]), AttemptCount: num(x[5]), NextAttemptAt: pt(str(x[6])), LastError: str(x[7])}, nil
}
func (s *Store) ListExecutionEvents(ctx context.Context, id string) ([]ExecutionEventRecord, error) {
	rows, e := s.db.Query(ctx, `SELECT id,execution_id,event_type,attempt_number,payload,occurred_at FROM execution_events WHERE execution_id=? ORDER BY occurred_at,id`, id)
	if e != nil {
		return nil, e
	}
	out := make([]ExecutionEventRecord, 0, len(rows))
	for _, x := range rows {
		out = append(out, ExecutionEventRecord{ID: str(x[0]), ExecutionID: str(x[1]), Type: str(x[2]), AttemptNumber: num(x[3]), Payload: blob(x[4]), OccurredAt: pt(str(x[5]))})
	}
	return out, nil
}

func st(q string) persistence.Statement { return persistence.Statement{SQL: q} }
func stm(q string, args ...any) persistence.Statement {
	return persistence.Statement{SQL: q, Args: args}
}
func opaque() (string, error) {
	var b [16]byte
	if _, e := rand.Read(b[:]); e != nil {
		return "", e
	}
	return hex.EncodeToString(b[:]), nil
}
func str(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case []byte:
		return string(x)
	case nil:
		return ""
	default:
		return fmt.Sprint(x)
	}
}
func blob(v any) []byte {
	switch x := v.(type) {
	case []byte:
		return append([]byte(nil), x...)
	case string:
		if decoded, err := base64.StdEncoding.DecodeString(x); err == nil {
			return decoded
		}
		return []byte(x)
	}
	return nil
}
func encodeBlob(value []byte) string { return base64.StdEncoding.EncodeToString(value) }
func i64(v any) int64 {
	switch x := v.(type) {
	case int64:
		return x
	case int:
		return int64(x)
	case float64:
		return int64(x)
	case json.Number:
		n, _ := x.Int64()
		return n
	}
	return 0
}
func num(v any) int { return int(i64(v)) }
func boolNum(v bool) int {
	if v {
		return 1
	}
	return 0
}
func ft(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339Nano)
}
func now() string           { return ft(time.Now()) }
func pt(v string) time.Time { t, _ := time.Parse(time.RFC3339Nano, v); return t.UTC() }
