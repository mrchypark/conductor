package storage

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestOpenRejectsLegacyDatabase(t *testing.T) {
	dsn := "file:" + t.TempDir() + "/legacy.db"
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE old_state (id TEXT PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	_ = db.Close()
	if _, err := Open(context.Background(), dsn); err == nil {
		t.Fatal("legacy database was accepted")
	}
}

func TestOpenMigratesExecutionTables(t *testing.T) {
	store, err := Open(context.Background(), "file:"+t.TempDir()+"/conductor.db")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	for _, table := range []string{"schema_metadata", "execution_requests", "executions", "attempts", "callbacks", "leases", "retry_timers", "callback_deliveries", "handler_manifests", "connector_manifests", "execution_events", "dynamic_handlers", "dynamic_handler_bytecode"} {
		var name string
		err := store.DB().QueryRowContext(context.Background(), "SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?", table).Scan(&name)
		if err != nil || name != table {
			t.Fatalf("migration did not create %s: %v", table, err)
		}
	}
}

func TestDynamicHandlerDefinitionIsImmutableAndReloadable(t *testing.T) {
	store, err := Open(context.Background(), "file:"+t.TempDir()+"/db")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	source := []byte(`output = input`)
	hash := sha256.Sum256(source)
	record := DynamicHandlerRecord{HandlerKey: "transform.kavun", Version: "1", ManifestJSON: json.RawMessage(`{"handler_key":"transform.kavun","version":"1"}`), Source: source, AllowedModules: []string{"math"}, MaxFrames: 256, MaxStack: 2048, SourceHash: hex.EncodeToString(hash[:])}
	created, err := store.SaveDynamicHandler(context.Background(), record)
	if err != nil || !created {
		t.Fatalf("save dynamic handler: created=%v err=%v", created, err)
	}
	created, err = store.SaveDynamicHandler(context.Background(), record)
	if err != nil || created {
		t.Fatalf("identical save should be idempotent: created=%v err=%v", created, err)
	}
	bytecode := []byte("compiled")
	bytecodeHash := sha256.Sum256(bytecode)
	if err := store.SaveDynamicHandlerBytecode(context.Background(), record.HandlerKey, record.Version, bytecode, hex.EncodeToString(bytecodeHash[:]), record.SourceHash, "kavun-v0.6.4/compiler-o3/input-output"); err != nil {
		t.Fatal(err)
	}
	record.Source = []byte(`output = 1`)
	otherHash := sha256.Sum256(record.Source)
	record.SourceHash = hex.EncodeToString(otherHash[:])
	if _, err := store.SaveDynamicHandler(context.Background(), record); err == nil || !strings.Contains(err.Error(), "different definition") {
		t.Fatalf("expected immutable version conflict, got %v", err)
	}
	records, err := store.ListDynamicHandlers(context.Background())
	if err != nil || len(records) != 1 || records[0].HandlerKey != "transform.kavun" || string(records[0].Source) != `output = input` || string(records[0].Bytecode) != "compiled" || records[0].BytecodeHash != hex.EncodeToString(bytecodeHash[:]) || records[0].BytecodeSourceHash != hex.EncodeToString(hash[:]) || len(records[0].AllowedModules) != 1 {
		t.Fatalf("unexpected dynamic handler records: %#v err=%v", records, err)
	}
}

func TestOpenAddsDynamicHandlerTableToExistingConductorSchema(t *testing.T) {
	dsn := "file:" + t.TempDir() + "/db"
	store, err := Open(context.Background(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.DB().ExecContext(context.Background(), "DROP TABLE dynamic_handlers"); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(context.Background(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	var name string
	if err := reopened.DB().QueryRowContext(context.Background(), "SELECT name FROM sqlite_master WHERE type='table' AND name='dynamic_handlers'").Scan(&name); err != nil || name != "dynamic_handlers" {
		t.Fatalf("dynamic handler table was not restored: %v", err)
	}
}

func TestLeaseExpiryRecoversQueuedExecution(t *testing.T) {
	store, err := Open(context.Background(), "file:"+t.TempDir()+"/db")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if _, err := store.CreateExecution(context.Background(), ExecutionRequestRecord{ID: "r", HandlerKey: "h", HandlerVersion: "1", Input: []byte(`{}`), MaxAttempts: 2}, "exec_r"); err != nil {
		t.Fatal(err)
	}
	lease, err := store.AcquireLease(context.Background(), "exec_r", "w", time.Millisecond, 1)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(5 * time.Millisecond)
	if err := store.HeartbeatLease(context.Background(), lease.ID, lease.FencingToken, time.Second); err == nil {
		t.Fatal("expired lease heartbeat unexpectedly succeeded")
	}
	if err := store.RecoverExpiredLeases(context.Background(), time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	var status string
	if err := store.DB().QueryRowContext(context.Background(), "SELECT status FROM executions WHERE id='exec_r'").Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "queued" {
		t.Fatalf("status=%s", status)
	}
}
