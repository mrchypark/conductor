// Package storage owns Conductor's replicated execution state.
package storage

import (
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
		if err := s.ensureWASMHandlerSchema(ctx); err != nil {
			return err
		}
		return s.ensureRedesignSchema(ctx)
	}
	ddl := []string{
		`CREATE TABLE IF NOT EXISTS schema_metadata(id INTEGER PRIMARY KEY CHECK(id=1),identity TEXT NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS execution_requests(id TEXT PRIMARY KEY,workspace_id TEXT NOT NULL,workflow_key TEXT NOT NULL,workflow_version INTEGER NOT NULL,workflow_instance_id TEXT NOT NULL,task_id TEXT NOT NULL,node_key TEXT NOT NULL,handler_key TEXT NOT NULL,handler_version TEXT NOT NULL,operation_key TEXT NOT NULL,connection_id TEXT NOT NULL,actor_type TEXT NOT NULL,actor_id TEXT NOT NULL,subject_ref BLOB NOT NULL,input_ref BLOB NOT NULL,deadline_at TEXT NOT NULL,input BLOB NOT NULL,timeout_ns INTEGER NOT NULL,max_attempts INTEGER NOT NULL,retry_backoff_ns INTEGER NOT NULL,retry_backoff_mode TEXT NOT NULL,correlation_id TEXT NOT NULL,causation_id TEXT NOT NULL,idempotency_key TEXT NOT NULL,created_at TEXT NOT NULL,credential_ref TEXT NOT NULL DEFAULT '',priority INTEGER NOT NULL DEFAULT 0,expires_at TEXT NOT NULL DEFAULT '',delegation_token BLOB NOT NULL DEFAULT x'',attempt_id TEXT NOT NULL DEFAULT '')`,
		`CREATE UNIQUE INDEX IF NOT EXISTS idx_execution_requests_idempotency ON execution_requests(workspace_id,idempotency_key) WHERE idempotency_key<>''`,
		`CREATE TABLE IF NOT EXISTS execution_request_credentials(execution_request_id TEXT PRIMARY KEY REFERENCES execution_requests(id),connection_id TEXT NOT NULL,credential_revision INTEGER NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS submission_claims(request_id TEXT PRIMARY KEY,claim_id TEXT NOT NULL,created_at TEXT NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS executions(id TEXT PRIMARY KEY,execution_request_id TEXT NOT NULL UNIQUE REFERENCES execution_requests(id),status TEXT NOT NULL,handler_key TEXT NOT NULL,handler_version TEXT NOT NULL,operation_key TEXT NOT NULL,connection_id TEXT NOT NULL,input BLOB NOT NULL,output BLOB NOT NULL,error_class TEXT NOT NULL,error_message TEXT NOT NULL,error_retryable INTEGER NOT NULL,created_at TEXT NOT NULL,updated_at TEXT NOT NULL,workspace_id TEXT NOT NULL DEFAULT '',priority INTEGER NOT NULL DEFAULT 0,expires_at TEXT NOT NULL DEFAULT '')`,
		`CREATE INDEX IF NOT EXISTS idx_executions_claim ON executions(status,priority DESC,created_at ASC,id)`,
		`CREATE INDEX IF NOT EXISTS idx_executions_expiry ON executions(status,expires_at)`,
		`CREATE TABLE IF NOT EXISTS attempts(id TEXT PRIMARY KEY,execution_id TEXT NOT NULL REFERENCES executions(id),attempt_number INTEGER NOT NULL,status TEXT NOT NULL,error_class TEXT NOT NULL,error_message TEXT NOT NULL,started_at TEXT NOT NULL,finished_at TEXT NOT NULL,UNIQUE(execution_id,attempt_number))`,
		`CREATE TABLE IF NOT EXISTS callbacks(id TEXT PRIMARY KEY,execution_id TEXT NOT NULL REFERENCES executions(id),status TEXT NOT NULL,payload BLOB NOT NULL,created_at TEXT NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS leases(lease_id TEXT PRIMARY KEY,execution_id TEXT NOT NULL UNIQUE REFERENCES executions(id),attempt_number INTEGER NOT NULL,worker_id TEXT NOT NULL,fencing_token INTEGER NOT NULL,expires_at TEXT NOT NULL,created_at TEXT NOT NULL,updated_at TEXT NOT NULL,status TEXT NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS retry_timers(id TEXT PRIMARY KEY,execution_id TEXT NOT NULL REFERENCES executions(id),attempt_number INTEGER NOT NULL,run_at TEXT NOT NULL,status TEXT NOT NULL,UNIQUE(execution_id,attempt_number))`,
		`CREATE TABLE IF NOT EXISTS callback_deliveries(callback_id TEXT PRIMARY KEY REFERENCES callbacks(id),execution_id TEXT NOT NULL REFERENCES executions(id),target_url TEXT NOT NULL,payload BLOB NOT NULL,status TEXT NOT NULL,claim_id TEXT NOT NULL,attempt_count INTEGER NOT NULL,next_attempt_at TEXT NOT NULL,last_error TEXT NOT NULL,created_at TEXT NOT NULL,updated_at TEXT NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS handler_manifests(workspace_id TEXT NOT NULL DEFAULT '',handler_key TEXT NOT NULL,version TEXT NOT NULL,manifest_json BLOB NOT NULL,created_at TEXT NOT NULL,PRIMARY KEY(workspace_id,handler_key,version))`,
		`CREATE TABLE IF NOT EXISTS connector_manifests(provider_key TEXT PRIMARY KEY,manifest_json BLOB NOT NULL,created_at TEXT NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS execution_events(id TEXT PRIMARY KEY,execution_id TEXT NOT NULL REFERENCES executions(id),event_type TEXT NOT NULL,attempt_number INTEGER NOT NULL,payload BLOB NOT NULL,occurred_at TEXT NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS terminal_claims(execution_id TEXT PRIMARY KEY,claim_id TEXT NOT NULL,status TEXT NOT NULL,payload BLOB NOT NULL,created_at TEXT NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS starlark_handlers(workspace_id TEXT NOT NULL,handler_key TEXT NOT NULL,version TEXT NOT NULL,manifest_json BLOB NOT NULL,source BLOB NOT NULL,max_steps INTEGER NOT NULL,source_hash TEXT NOT NULL,created_at TEXT NOT NULL,PRIMARY KEY(workspace_id,handler_key,version))`,
		`CREATE TABLE IF NOT EXISTS wasm_handlers(workspace_id TEXT NOT NULL DEFAULT '',handler_key TEXT NOT NULL,version TEXT NOT NULL,manifest_json BLOB NOT NULL,artifact_ref TEXT NOT NULL,manifest_digest TEXT NOT NULL,blob_digest TEXT NOT NULL,artifact_size INTEGER NOT NULL,runtime_abi TEXT NOT NULL,created_at TEXT NOT NULL,PRIMARY KEY(workspace_id,handler_key,version))`,
		`CREATE TABLE IF NOT EXISTS custom_connectors(workspace_id TEXT NOT NULL,provider_key TEXT NOT NULL,provider_info_json BLOB NOT NULL,created_at TEXT NOT NULL,updated_at TEXT NOT NULL,PRIMARY KEY(workspace_id,provider_key))`,
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
	if err := s.ensureWASMHandlerSchema(ctx); err != nil {
		return err
	}
	return s.ensureRedesignSchema(ctx)
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
	_, err = s.db.Mutate(ctx, "initialize wasm handler schema", st(`CREATE TABLE IF NOT EXISTS wasm_handlers(workspace_id TEXT NOT NULL DEFAULT '',handler_key TEXT NOT NULL,version TEXT NOT NULL,manifest_json BLOB NOT NULL,artifact_ref TEXT NOT NULL,manifest_digest TEXT NOT NULL,blob_digest TEXT NOT NULL,artifact_size INTEGER NOT NULL,runtime_abi TEXT NOT NULL,created_at TEXT NOT NULL,PRIMARY KEY(workspace_id,handler_key,version))`))
	return err
}

func (s *Store) hasColumn(ctx context.Context, table, column string) (bool, error) {
	rows, err := s.db.Query(ctx, `PRAGMA table_info(`+table+`)`)
	if err != nil {
		return false, err
	}
	for _, row := range rows {
		if len(row) > 1 && str(row[1]) == column {
			return true, nil
		}
	}
	return false, nil
}

func (s *Store) ensureRedesignSchema(ctx context.Context) error {
	var stmts []persistence.Statement
	for _, col := range []struct {
		table, name, def string
	}{
		{"execution_requests", "credential_ref", "TEXT NOT NULL DEFAULT ''"},
		{"execution_requests", "priority", "INTEGER NOT NULL DEFAULT 0"},
		{"execution_requests", "expires_at", "TEXT NOT NULL DEFAULT ''"},
		{"execution_requests", "delegation_token", "BLOB NOT NULL DEFAULT x''"},
		{"execution_requests", "attempt_id", "TEXT NOT NULL DEFAULT ''"},
		{"executions", "workspace_id", "TEXT NOT NULL DEFAULT ''"},
		{"executions", "priority", "INTEGER NOT NULL DEFAULT 0"},
		{"executions", "expires_at", "TEXT NOT NULL DEFAULT ''"},
	} {
		ok, err := s.hasColumn(ctx, col.table, col.name)
		if err != nil {
			return err
		}
		if !ok {
			stmts = append(stmts, st(`ALTER TABLE `+col.table+` ADD COLUMN `+col.name+` `+col.def))
		}
	}
	stmts = append(stmts,
		st(`CREATE INDEX IF NOT EXISTS idx_executions_claim ON executions(status,priority DESC,created_at ASC,id)`),
		st(`CREATE INDEX IF NOT EXISTS idx_executions_expiry ON executions(status,expires_at)`),
		st(`CREATE TABLE IF NOT EXISTS starlark_handlers(workspace_id TEXT NOT NULL,handler_key TEXT NOT NULL,version TEXT NOT NULL,manifest_json BLOB NOT NULL,source BLOB NOT NULL,max_steps INTEGER NOT NULL,source_hash TEXT NOT NULL,created_at TEXT NOT NULL,PRIMARY KEY(workspace_id,handler_key,version))`),
		st(`CREATE TABLE IF NOT EXISTS custom_connectors(workspace_id TEXT NOT NULL,provider_key TEXT NOT NULL,provider_info_json BLOB NOT NULL,created_at TEXT NOT NULL,updated_at TEXT NOT NULL,PRIMARY KEY(workspace_id,provider_key))`),
	)
	if hasWS, err := s.hasColumn(ctx, "handler_manifests", "workspace_id"); err != nil {
		return err
	} else if !hasWS {
		stmts = append(stmts,
			st(`CREATE TABLE handler_manifests_new(workspace_id TEXT NOT NULL DEFAULT '',handler_key TEXT NOT NULL,version TEXT NOT NULL,manifest_json BLOB NOT NULL,created_at TEXT NOT NULL,PRIMARY KEY(workspace_id,handler_key,version))`),
			st(`INSERT INTO handler_manifests_new(workspace_id,handler_key,version,manifest_json,created_at) SELECT '',handler_key,version,manifest_json,created_at FROM handler_manifests`),
			st(`DROP TABLE handler_manifests`),
			st(`ALTER TABLE handler_manifests_new RENAME TO handler_manifests`),
		)
	}
	if hasWS, err := s.hasColumn(ctx, "wasm_handlers", "workspace_id"); err != nil {
		return err
	} else if !hasWS {
		stmts = append(stmts,
			st(`CREATE TABLE wasm_handlers_new(workspace_id TEXT NOT NULL DEFAULT '',handler_key TEXT NOT NULL,version TEXT NOT NULL,manifest_json BLOB NOT NULL,artifact_ref TEXT NOT NULL,manifest_digest TEXT NOT NULL,blob_digest TEXT NOT NULL,artifact_size INTEGER NOT NULL,runtime_abi TEXT NOT NULL,created_at TEXT NOT NULL,PRIMARY KEY(workspace_id,handler_key,version))`),
			st(`INSERT INTO wasm_handlers_new(workspace_id,handler_key,version,manifest_json,artifact_ref,manifest_digest,blob_digest,artifact_size,runtime_abi,created_at) SELECT '',handler_key,version,manifest_json,artifact_ref,manifest_digest,blob_digest,artifact_size,runtime_abi,created_at FROM wasm_handlers`),
			st(`DROP TABLE wasm_handlers`),
			st(`ALTER TABLE wasm_handlers_new RENAME TO wasm_handlers`),
		)
	}
	_, err := s.db.Mutate(ctx, "migrate redesign schema", stmts...)
	return err
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
