package executions

import (
	"context"
	"encoding/json"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/mrchypark/conductor/internal/handlers"
	"github.com/mrchypark/conductor/internal/storage"
)

func TestCredentialRevisionIsPinnedForDispatchAndDuplicates(t *testing.T) {
	store, err := storage.Open(context.Background(), "file:"+t.TempDir()+"/conductor.db")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	registry := handlers.NewRegistry()
	var dispatched atomic.Int64
	handler, err := handlers.NewFuncHandler(handlers.Manifest{Key: "revision.test", Version: "1"}, func(_ context.Context, request handlers.Request) (handlers.Result, error) {
		dispatched.Store(request.CredentialRevision)
		return handlers.Result{Output: json.RawMessage(`{}`)}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(handler); err != nil {
		t.Fatal(err)
	}
	service, err := NewService(store, registry)
	if err != nil {
		t.Fatal(err)
	}
	request := ExecutionRequest{ExecutionRequestID: "revision-request", HandlerKey: "revision.test", HandlerVersion: "1", ConnectionID: "github-primary", CredentialRevision: 3}
	if _, err := service.SubmitAndExecute(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if got := dispatched.Load(); got != request.CredentialRevision {
		t.Fatalf("handler credential revision=%d, want %d", got, request.CredentialRevision)
	}
	request.CredentialRevision++
	if _, err := service.Submit(context.Background(), request); err == nil || !strings.Contains(err.Error(), "idempotency_conflict") {
		t.Fatalf("revision-changing duplicate err=%v, want idempotency conflict", err)
	}
}

func TestCredentialRevisionValidation(t *testing.T) {
	base := ExecutionRequest{ExecutionRequestID: "revision-validation", HandlerKey: "revision.test", HandlerVersion: "1"}
	base.CredentialRevision = -1
	if err := validate(base); err == nil || !strings.Contains(err.Error(), "credential revision") {
		t.Fatalf("negative revision err=%v", err)
	}
	base.CredentialRevision = 1
	if err := validate(base); err == nil || !strings.Contains(err.Error(), "connection id") {
		t.Fatalf("unbound revision err=%v", err)
	}
}
