package storage

import (
	"context"
	"testing"
)

func TestExecutionCredentialRevisionPersistsAcrossReopen(t *testing.T) {
	ctx := context.Background()
	dsn := "file:" + t.TempDir() + "/conductor.db"
	store, err := Open(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	request := ExecutionRequestRecord{ID: "request-revision", HandlerKey: "test.handler", HandlerVersion: "1", ConnectionID: "github-primary", CredentialRevision: 7, Input: []byte(`{}`)}
	if created, err := store.CreateExecution(ctx, request, "exec_request-revision"); err != nil || !created {
		t.Fatalf("create=%v err=%v", created, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	got, err := reopened.GetExecutionRequest(ctx, request.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.ConnectionID != request.ConnectionID || got.CredentialRevision != request.CredentialRevision {
		t.Fatalf("credential pin=%q@%d, want %q@%d", got.ConnectionID, got.CredentialRevision, request.ConnectionID, request.CredentialRevision)
	}
}
