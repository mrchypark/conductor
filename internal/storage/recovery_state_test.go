package storage

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"
)

// verifyRecoveryState can also check a store opened from recovered data.
func verifyRecoveryState(t *testing.T, store *Store, request ExecutionRequestRecord, executionID string) {
	t.Helper()
	ctx := context.Background()
	gotRequest, err := store.FindExecutionRequestByIdempotency(ctx, request.WorkspaceID, request.IdempotencyKey)
	if err != nil || gotRequest.ID != request.ID || string(gotRequest.Input) != string(request.Input) {
		t.Fatalf("idempotency lookup: request=%+v err=%v", gotRequest, err)
	}
	execution, err := store.GetExecution(ctx, executionID)
	if err != nil || execution.ExecutionRequestID != request.ID || execution.Status != "succeeded" || string(execution.Output) != `{"answer":42}` {
		t.Fatalf("completed execution: %+v err=%v", execution, err)
	}
	events, err := store.ListExecutionEvents(ctx, executionID)
	if err != nil {
		t.Fatal(err)
	}
	types := make(map[string]int)
	for _, event := range events {
		if event.ExecutionID != executionID {
			t.Fatalf("event belongs to another execution: %+v", event)
		}
		types[event.Type]++
	}
	if len(events) != 3 || types["execution.requested"] != 1 || types["execution.started"] != 1 || types["execution.succeeded"] != 1 {
		t.Fatalf("execution events: %+v", events)
	}
	callback, err := store.Callback(ctx, executionID)
	if err != nil || callback.ID != "cb_"+executionID || callback.Status != "succeeded" {
		t.Fatalf("callback: %+v err=%v", callback, err)
	}
	var payload struct {
		ExecutionID string          `json:"execution_id"`
		Status      string          `json:"runtime_status"`
		Result      json.RawMessage `json:"result"`
	}
	if err := json.Unmarshal(callback.Payload, &payload); err != nil || payload.ExecutionID != executionID || payload.Status != "succeeded" || string(payload.Result) != `{"answer":42}` {
		t.Fatalf("callback payload: %s err=%v", callback.Payload, err)
	}
	delivery, err := store.CallbackDelivery(ctx, callback.ID)
	if err != nil || delivery.ExecutionID != executionID || delivery.TargetURL != "https://example.test/callback" || delivery.Status != "pending" || !reflect.DeepEqual(delivery.Payload, callback.Payload) {
		t.Fatalf("callback delivery: %+v err=%v", delivery, err)
	}
}

func TestRecoveryStatePersistsAcrossReopen(t *testing.T) {
	ctx := context.Background()
	dsn := "file:" + t.TempDir() + "/db"
	store, err := Open(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	open := true
	t.Cleanup(func() {
		if open {
			if err := store.Close(); err != nil {
				t.Errorf("close store: %v", err)
			}
		}
	})
	request := ExecutionRequestRecord{ID: "recovery-request", WorkspaceID: "workspace", IdempotencyKey: "recovery-key", HandlerKey: "handler", HandlerVersion: "1", Input: []byte(`{"value":1}`), MaxAttempts: 1}
	const executionID = "recovery-execution"
	store.SetCallbackTarget("https://example.test/callback")
	if created, err := store.CreateExecution(ctx, request, executionID); err != nil || !created {
		t.Fatalf("create execution: created=%v err=%v", created, err)
	}
	lease, err := store.AcquireLease(ctx, executionID, "worker", time.Minute, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.CompleteExecutionWithLease(ctx, lease.ID, lease.FencingToken, executionID, "succeeded", []byte(`{"answer":42}`), "", "", false, 1); err != nil {
		t.Fatal(err)
	}
	verifyRecoveryState(t, store, request, executionID)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	open = false
	store, err = Open(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	open = true
	verifyRecoveryState(t, store, request, executionID)
	if _, _, err := store.ClaimNext(ctx, "recovery-worker", time.Minute); !errors.Is(err, ErrNotFound) {
		t.Fatalf("completed execution was claimable: %v", err)
	}
	if _, err := store.AcquireLease(ctx, executionID, "recovery-worker", time.Minute, 1); err == nil {
		t.Fatal("completed execution accepted a lease")
	}
	if created, err := store.CreateExecution(ctx, request, "duplicate-execution"); err != nil || created {
		t.Fatalf("replay submission: created=%v err=%v", created, err)
	}
	if _, err := store.GetExecution(ctx, "duplicate-execution"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("duplicate execution lookup: %v", err)
	}
	verifyRecoveryState(t, store, request, executionID)
}
