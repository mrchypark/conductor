package storage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestOpenInitializesAuthoritativeSchema(t *testing.T) {
	store, err := Open(context.Background(), "file:"+t.TempDir()+"/conductor.db")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	if err := store.Persistence().Ready(context.Background()); err != nil {
		t.Fatalf("schema is not authoritative: %v", err)
	}
}

func TestStarlarkHandlerDefinitionIsImmutableAndReloadable(t *testing.T) {
	store, err := Open(context.Background(), "file:"+t.TempDir()+"/db")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	source := []byte("def run(input):\n    return input\n")
	hash := sha256.Sum256(source)
	record := StarlarkHandlerRecord{WorkspaceID: "ws-1", HandlerKey: "transform.starlark", Version: "1", ManifestJSON: json.RawMessage(`{"handler_key":"transform.starlark","version":"1"}`), Source: source, MaxSteps: 1000000, SourceHash: hex.EncodeToString(hash[:])}
	created, err := store.SaveStarlarkHandler(context.Background(), record)
	if err != nil || !created {
		t.Fatalf("save starlark handler: created=%v err=%v", created, err)
	}
	created, err = store.SaveStarlarkHandler(context.Background(), record)
	if err != nil || created {
		t.Fatalf("identical save should be idempotent: created=%v err=%v", created, err)
	}
	record.Source = []byte("def run(input):\n    return 1\n")
	otherHash := sha256.Sum256(record.Source)
	record.SourceHash = hex.EncodeToString(otherHash[:])
	if _, err := store.SaveStarlarkHandler(context.Background(), record); err == nil || !strings.Contains(err.Error(), "different definition") {
		t.Fatalf("expected immutable version conflict, got %v", err)
	}
	records, err := store.ListStarlarkHandlers(context.Background())
	if err != nil || len(records) != 1 || records[0].WorkspaceID != "ws-1" || records[0].HandlerKey != "transform.starlark" || string(records[0].Source) != string(source) {
		t.Fatalf("unexpected starlark handler records: %#v err=%v", records, err)
	}
}

func TestOpenReloadsStarlarkHandlerFromRhiza(t *testing.T) {
	dsn := "file:" + t.TempDir() + "/db"
	store, err := Open(context.Background(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	record := StarlarkHandlerRecord{WorkspaceID: "ws-1", HandlerKey: "reload.starlark", Version: "1", ManifestJSON: []byte(`{"handler_key":"reload.starlark","version":"1"}`), Source: []byte("def run(input):\n    return input\n"), SourceHash: "hash", MaxSteps: 1000}
	if _, err := store.SaveStarlarkHandler(context.Background(), record); err != nil {
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
	records, err := reopened.ListStarlarkHandlers(context.Background())
	if err != nil || len(records) != 1 || records[0].HandlerKey != record.HandlerKey || records[0].WorkspaceID != "ws-1" {
		t.Fatalf("starlark handler was not restored: %#v %v", records, err)
	}
}

func TestWASMHandlerMetadataIsImmutableAndContainsNoArtifactBytes(t *testing.T) {
	store, err := Open(context.Background(), "file:"+t.TempDir()+"/db")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	record := WASMHandlerRecord{
		HandlerKey: "transform.wasm", Version: "1",
		ManifestJSON: []byte(`{"handler_key":"transform.wasm","version":"1"}`),
		ArtifactRef:  "registry.example/functions/transform@sha256:manifest", ManifestDigest: "sha256:manifest",
		BlobDigest: "sha256:blob", ArtifactSize: 1234, RuntimeABI: "conductor:function/v1",
	}
	created, err := store.SaveWASMHandler(context.Background(), record)
	if err != nil || !created {
		t.Fatalf("save wasm handler: created=%v err=%v", created, err)
	}
	created, err = store.SaveWASMHandler(context.Background(), record)
	if err != nil || created {
		t.Fatalf("identical save should be idempotent: created=%v err=%v", created, err)
	}
	record.BlobDigest = "sha256:different"
	if _, err := store.SaveWASMHandler(context.Background(), record); err == nil || !strings.Contains(err.Error(), "different definition") {
		t.Fatalf("expected immutable version conflict, got %v", err)
	}
	records, err := store.ListWASMHandlers(context.Background())
	if err != nil || len(records) != 1 || records[0].BlobDigest != "sha256:blob" || records[0].ArtifactSize != 1234 {
		t.Fatalf("unexpected wasm handler metadata: %#v err=%v", records, err)
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
	execution, err := store.GetExecution(context.Background(), "exec_r")
	if err != nil {
		t.Fatal(err)
	}
	if execution.Status != "queued" {
		t.Fatalf("status=%s", execution.Status)
	}
}
