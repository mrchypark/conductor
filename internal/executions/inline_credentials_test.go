package executions

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/mrchypark/conductor/internal/handlers"
	"github.com/mrchypark/conductor/internal/storage"
)

func TestCredentialV2RunsOnlyInlineWithRequestContext(t *testing.T) {
	ctx := context.Background()
	store, err := storage.Open(ctx, "file:"+t.TempDir()+"/db")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	type tokenKey struct{}
	registry := handlers.NewRegistry()
	h, err := handlers.NewFuncHandler(handlers.Manifest{Key: "connector.credentials.invoke", Version: "2"}, func(ctx context.Context, _ handlers.Request) (handlers.Result, error) {
		if ctx.Value(tokenKey{}) != "private-token" {
			t.Error("request context was not forwarded")
		}
		return handlers.Result{Output: json.RawMessage(`{"ok":true}`)}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(h); err != nil {
		t.Fatal(err)
	}
	service, err := NewService(store, registry)
	if err != nil {
		t.Fatal(err)
	}
	req := ExecutionRequest{ExecutionRequestID: "inline", HandlerKey: "connector.credentials.invoke", HandlerVersion: "2", Input: json.RawMessage(`{}`)}
	if _, err := service.Submit(ctx, req); err == nil {
		t.Fatal("async submission accepted")
	}
	bad := req
	bad.Input = json.RawMessage(`{"token":"private-token"}`)
	if _, err := service.SubmitAndExecute(ctx, bad); err == nil {
		t.Fatal("credential in input accepted")
	}
	if _, err := store.GetExecutionRequest(ctx, req.ExecutionRequestID); err == nil {
		t.Fatal("rejected input was persisted")
	}
	for _, ref := range []func(*ExecutionRequest){func(r *ExecutionRequest) { r.SubjectRef = json.RawMessage(`{"token":"private-token"}`) }, func(r *ExecutionRequest) { r.InputRef = json.RawMessage(`{"token":"private-token"}`) }} {
		bad = req
		ref(&bad)
		if _, err := service.SubmitAndExecute(ctx, bad); err == nil {
			t.Fatal("credential reference accepted")
		}
	}
	execution, err := service.SubmitAndExecute(context.WithValue(ctx, tokenKey{}, "private-token"), req)
	if err != nil || execution.Status != "succeeded" {
		t.Fatalf("execution: %+v %v", execution, err)
	}
	if _, err := service.SubmitAndExecute(context.WithValue(ctx, tokenKey{}, "different-token"), req); err == nil {
		t.Fatal("duplicate returned a result without reauthorization")
	}
	record, err := store.GetExecutionRequest(ctx, req.ExecutionRequestID)
	if err != nil || strings.Contains(string(record.Input), "private-token") {
		t.Fatalf("stored request: %+v %v", record, err)
	}
	if _, _, err := store.ClaimNext(ctx, "worker", time.Minute); err == nil {
		t.Fatal("inline execution claimed by worker")
	}
}

func TestCanceledInlineCompletionRemainsRecoverable(t *testing.T) {
	ctx := context.Background()
	store, err := storage.Open(ctx, "file:"+t.TempDir()+"/db")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	registry := handlers.NewRegistry()
	callCtx, cancel := context.WithCancel(ctx)
	h, err := handlers.NewFuncHandler(handlers.Manifest{Key: "connector.credentials.invoke", Version: "2"}, func(context.Context, handlers.Request) (handlers.Result, error) {
		cancel()
		return handlers.Result{}, context.Canceled
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(h); err != nil {
		t.Fatal(err)
	}
	service, err := NewService(store, registry)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = service.SubmitAndExecute(callCtx, ExecutionRequest{ExecutionRequestID: "canceled-inline", HandlerKey: "connector.credentials.invoke", HandlerVersion: "2"})
	if err := store.RecoverExpiredLeases(ctx, time.Now().Add(2*time.Minute)); err != nil {
		t.Fatal(err)
	}
	execution, err := service.Get(ctx, "exec_canceled-inline")
	if err != nil || execution.Status != "failed" || execution.Error == nil || execution.Error.Class != "outcome_unknown" {
		t.Fatalf("recovery: %+v %v", execution, err)
	}
}
