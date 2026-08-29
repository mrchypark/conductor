package executions

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mrchypark/conductor/internal/handlers"
	"github.com/mrchypark/conductor/internal/storage"
)

func TestSubmitAndExecuteIsIdempotent(t *testing.T) {
	store, err := storage.Open(context.Background(), "file:"+t.TempDir()+"/conductor.db")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	registry := handlers.NewRegistry()
	var calls atomic.Int32
	handler, err := handlers.NewFuncHandler(handlers.Manifest{Key: "example.run", Version: "1"}, func(_ context.Context, request handlers.Request) (handlers.Result, error) {
		calls.Add(1)
		if request.ExecutionID != "exec_er_1" {
			t.Fatalf("unexpected execution id: %s", request.ExecutionID)
		}
		return handlers.Result{Output: json.RawMessage(`{"ok":true}`)}, nil
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
	request := ExecutionRequest{ExecutionRequestID: "er_1", HandlerKey: "example.run", HandlerVersion: "1", Input: json.RawMessage(`{"value":1}`)}

	first, err := service.SubmitAndExecute(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	second, err := service.SubmitAndExecute(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 || first.ID != second.ID || first.Status != "succeeded" || string(first.Output) != `{"ok":true}` {
		t.Fatalf("unexpected idempotent result: calls=%d first=%+v second=%+v", calls.Load(), first, second)
	}
}

func TestSubmitRejectsIdempotencyConflict(t *testing.T) {
	store, err := storage.Open(context.Background(), "file:"+t.TempDir()+"/db")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	r := handlers.NewRegistry()
	h, _ := handlers.NewFuncHandler(handlers.Manifest{Key: "idempotent", Version: "1"}, func(context.Context, handlers.Request) (handlers.Result, error) {
		return handlers.Result{Output: json.RawMessage(`{}`)}, nil
	})
	_ = r.Register(h)
	s, _ := NewService(store, r)
	base := ExecutionRequest{ExecutionRequestID: "same", WorkspaceID: "w1", HandlerKey: "idempotent", HandlerVersion: "1", IdempotencyKey: "k1", Input: json.RawMessage(`{"a":1}`)}
	if _, err := s.Submit(context.Background(), base); err != nil {
		t.Fatal(err)
	}
	other := base
	other.ExecutionRequestID = "other"
	if _, err := s.Submit(context.Background(), other); err == nil || !strings.Contains(err.Error(), "idempotency_conflict") {
		t.Fatalf("expected key conflict, got %v", err)
	}
	base.Input = json.RawMessage(`{"a":2}`)
	if _, err := s.Submit(context.Background(), base); err == nil || !strings.Contains(err.Error(), "idempotency_conflict") {
		t.Fatalf("expected conflict, got %v", err)
	}
}

func TestDuplicateSubmitWaitsForTerminalResult(t *testing.T) {
	store, err := storage.Open(context.Background(), "file:"+t.TempDir()+"/db")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	r := handlers.NewRegistry()
	started, release := make(chan struct{}), make(chan struct{})
	h, _ := handlers.NewFuncHandler(handlers.Manifest{Key: "wait.test", Version: "1"}, func(context.Context, handlers.Request) (handlers.Result, error) {
		close(started)
		<-release
		return handlers.Result{Output: json.RawMessage(`{"ok":true}`)}, nil
	})
	_ = r.Register(h)
	s, _ := NewService(store, r)
	req := ExecutionRequest{ExecutionRequestID: "wait-1", HandlerKey: "wait.test", HandlerVersion: "1"}
	first := make(chan error, 1)
	go func() { _, err := s.SubmitAndExecute(context.Background(), req); first <- err }()
	<-started
	second := make(chan Execution, 1)
	go func() { e, _ := s.SubmitAndExecute(context.Background(), req); second <- e }()
	select {
	case <-second:
		t.Fatal("duplicate returned before terminal result")
	case <-time.After(30 * time.Millisecond):
	}
	close(release)
	if err := <-first; err != nil {
		t.Fatal(err)
	}
	e := <-second
	if e.Status != "succeeded" {
		t.Fatalf("status=%s", e.Status)
	}
}

func TestExecutionRequestAcceptsCanonicalComposWire(t *testing.T) {
	var request ExecutionRequest
	if err := json.Unmarshal([]byte(`{"execution_request_id":"er-wire","handler":"example.run","handler_version":"1","operation":"read","timeout_seconds":2.5,"retry_policy":{"max_attempts":3,"backoff":"exponential"},"subject_ref":{"type":"order","id":"o1"},"input_ref":{"value":1},"deadline_at":"2026-08-30T00:00:00Z"}`), &request); err != nil {
		t.Fatal(err)
	}
	if request.HandlerKey != "example.run" || request.OperationKey != "read" || request.Timeout != 2500*time.Millisecond || request.Retry.MaxAttempts != 3 || request.Retry.BackoffMode != "exponential" {
		t.Fatalf("canonical wire fields were not mapped: %+v", request)
	}
	encoded, err := json.Marshal(request)
	if err != nil || !strings.Contains(string(encoded), `"timeout_seconds":2.5`) || !strings.Contains(string(encoded), `"retry_policy"`) || strings.Contains(string(encoded), `"timeout":`) {
		t.Fatalf("canonical wire fields were not emitted: %s err=%v", encoded, err)
	}
}

func TestSubmitAndExecuteRetriesRetryableHandlerAndPersistsCallback(t *testing.T) {
	store, err := storage.Open(context.Background(), "file:"+t.TempDir()+"/conductor.db")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	registry := handlers.NewRegistry()
	var calls atomic.Int32
	handler, err := handlers.NewFuncHandler(handlers.Manifest{Key: "example.retry", Version: "1"}, func(context.Context, handlers.Request) (handlers.Result, error) {
		if calls.Add(1) == 1 {
			return handlers.Result{}, &handlers.Error{Class: "rate_limited", Message: "try again", Retryable: true}
		}
		return handlers.Result{Output: json.RawMessage(`{"ok":true}`)}, nil
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
	execution, err := service.SubmitAndExecute(context.Background(), ExecutionRequest{ExecutionRequestID: "er-retry", HandlerKey: "example.retry", HandlerVersion: "1", Retry: RetryPolicy{MaxAttempts: 2}})
	if err != nil {
		t.Fatal(err)
	}
	if execution.Status != "succeeded" || calls.Load() != 2 {
		t.Fatalf("retry did not succeed: calls=%d execution=%+v", calls.Load(), execution)
	}
	var callback []byte
	if err := store.DB().QueryRowContext(context.Background(), "SELECT payload FROM callbacks WHERE execution_id = ?", execution.ID).Scan(&callback); err != nil {
		t.Fatal(err)
	}
	var payload map[string]any
	if err := json.Unmarshal(callback, &payload); err != nil {
		t.Fatal(err)
	}
	if payload["runtime_status"] != "succeeded" || payload["attempt_count"] != float64(2) {
		t.Fatalf("unexpected callback payload: %s", callback)
	}
	var events int
	if err := store.DB().QueryRowContext(context.Background(), "SELECT COUNT(*) FROM execution_events WHERE execution_id = ?", execution.ID).Scan(&events); err != nil {
		t.Fatal(err)
	}
	if events < 3 {
		t.Fatalf("expected requested, started, and terminal events, got %d", events)
	}
}

func TestSubmitAndExecuteDoesNotRetryNonRetryableError(t *testing.T) {
	store, err := storage.Open(context.Background(), "file:"+t.TempDir()+"/conductor.db")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	registry := handlers.NewRegistry()
	var calls atomic.Int32
	handler, err := handlers.NewFuncHandler(handlers.Manifest{Key: "example.no-retry", Version: "1"}, func(context.Context, handlers.Request) (handlers.Result, error) {
		calls.Add(1)
		return handlers.Result{}, errors.New("bad input")
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
	if _, err := service.SubmitAndExecute(context.Background(), ExecutionRequest{ExecutionRequestID: "er-no-retry", HandlerKey: "example.no-retry", HandlerVersion: "1", Retry: RetryPolicy{MaxAttempts: 3}}); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 {
		t.Fatalf("non-retryable handler was retried: %d", calls.Load())
	}
}

func TestSubmitAndExecuteHonorsDeadline(t *testing.T) {
	store, err := storage.Open(context.Background(), "file:"+t.TempDir()+"/conductor.db")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	registry := handlers.NewRegistry()
	handler, err := handlers.NewFuncHandler(handlers.Manifest{Key: "example.deadline", Version: "1"}, func(context.Context, handlers.Request) (handlers.Result, error) {
		return handlers.Result{Output: json.RawMessage(`{"unexpected":true}`)}, nil
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
	execution, err := service.SubmitAndExecute(context.Background(), ExecutionRequest{ExecutionRequestID: "er-deadline", HandlerKey: "example.deadline", HandlerVersion: "1", DeadlineAt: time.Now().Add(-time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	if execution.Status != "timed_out" || execution.Error == nil || execution.Error.Class != "deadline_exceeded" {
		t.Fatalf("deadline was not enforced: %+v", execution)
	}
}

func TestManifestDefaultTimeoutIsApplied(t *testing.T) {
	store, err := storage.Open(context.Background(), "file:"+t.TempDir()+"/db")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	r := handlers.NewRegistry()
	h, _ := handlers.NewFuncHandler(handlers.Manifest{Key: "timeout.default", Version: "1", DefaultTimeout: 1}, func(ctx context.Context, _ handlers.Request) (handlers.Result, error) {
		<-ctx.Done()
		return handlers.Result{}, ctx.Err()
	})
	_ = r.Register(h)
	s, _ := NewService(store, r)
	started := time.Now()
	e, err := s.SubmitAndExecute(context.Background(), ExecutionRequest{ExecutionRequestID: "timeout-default", HandlerKey: "timeout.default", HandlerVersion: "1"})
	if err != nil {
		t.Fatal(err)
	}
	if e.Status != "timed_out" || time.Since(started) > 3*time.Second {
		t.Fatalf("default timeout not applied: %+v", e)
	}
}

func TestSubmitAndExecuteStoresStructuredFailure(t *testing.T) {
	store, err := storage.Open(context.Background(), "file:"+t.TempDir()+"/conductor.db")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	registry := handlers.NewRegistry()
	handler, err := handlers.NewFuncHandler(handlers.Manifest{Key: "example.fail", Version: "1"}, func(context.Context, handlers.Request) (handlers.Result, error) {
		return handlers.Result{}, &handlers.Error{Class: "validation_failed", Message: "missing field"}
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
	execution, err := service.SubmitAndExecute(context.Background(), ExecutionRequest{ExecutionRequestID: "er_2", HandlerKey: "example.fail", HandlerVersion: "1"})
	if err != nil {
		t.Fatal(err)
	}
	if execution.Status != "failed" || execution.Error == nil || execution.Error.Class != "validation_failed" {
		t.Fatalf("unexpected execution failure: %+v", execution)
	}
}

func TestRunWorkerClaimsQueuedExecution(t *testing.T) {
	store, err := storage.Open(context.Background(), "file:"+t.TempDir()+"/db")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	registry := handlers.NewRegistry()
	h, _ := handlers.NewFuncHandler(handlers.Manifest{Key: "worker.test", Version: "1"}, func(context.Context, handlers.Request) (handlers.Result, error) {
		return handlers.Result{Output: json.RawMessage(`{"ok":true}`)}, nil
	})
	if err := registry.Register(h); err != nil {
		t.Fatal(err)
	}
	service, _ := NewService(store, registry)
	if _, err := service.Submit(context.Background(), ExecutionRequest{ExecutionRequestID: "queued-1", HandlerKey: "worker.test", HandlerVersion: "1"}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- service.RunWorker(ctx, "w1", time.Millisecond, time.Second) }()
	deadline := time.Now().Add(time.Second)
	for {
		e, err := service.Get(context.Background(), "exec_queued-1")
		if err == nil && e.Status == "succeeded" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("worker did not finish")
		}
		time.Sleep(time.Millisecond * 5)
	}
	cancel()
	<-done
}

func TestExternalWorkerProtocolIsFencedAndIdempotent(t *testing.T) {
	store, err := storage.Open(context.Background(), "file:"+t.TempDir()+"/db")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	registry := handlers.NewRegistry()
	service, _ := NewService(store, registry)
	request := ExecutionRequest{ExecutionRequestID: "remote-1", WorkspaceID: "w1", HandlerKey: "remote.handler", HandlerVersion: "1", Input: json.RawMessage(`{"x":1}`)}
	if _, err := service.Submit(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	claim, err := service.Poll(context.Background(), "worker-1", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if claim.Lease.AttemptNumber != 1 || claim.Request.ExecutionRequestID != request.ExecutionRequestID {
		t.Fatalf("unexpected claim: %+v", claim)
	}
	if _, err := service.Complete(context.Background(), "worker-2", claim.Lease.ID, claim.Lease.FencingToken, claim.Lease.ExecutionID, json.RawMessage(`{"ok":true}`), 1); err == nil {
		t.Fatal("wrong worker completed the lease")
	}
	first, err := service.Complete(context.Background(), "worker-1", claim.Lease.ID, claim.Lease.FencingToken, claim.Lease.ExecutionID, json.RawMessage(`{"ok":true}`), 1)
	if err != nil || first.Status != "succeeded" {
		t.Fatalf("completion failed: %+v %v", first, err)
	}
	second, err := service.Complete(context.Background(), "worker-1", claim.Lease.ID, claim.Lease.FencingToken, claim.Lease.ExecutionID, json.RawMessage(`{"different":true}`), 1)
	if err != nil || second.Status != "succeeded" {
		t.Fatalf("duplicate completion was not idempotent: %+v %v", second, err)
	}
}

func TestExternalWorkerFailureSchedulesRetry(t *testing.T) {
	store, err := storage.Open(context.Background(), "file:"+t.TempDir()+"/db")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	service, _ := NewService(store, handlers.NewRegistry())
	request := ExecutionRequest{ExecutionRequestID: "remote-retry", WorkspaceID: "w1", HandlerKey: "remote.handler", HandlerVersion: "1", Retry: RetryPolicy{MaxAttempts: 2, Backoff: time.Millisecond}}
	if _, err := service.Submit(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	claim, err := service.Poll(context.Background(), "worker-1", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	e, err := service.Fail(context.Background(), "worker-1", claim.Lease.ID, claim.Lease.FencingToken, claim.Lease.ExecutionID, 1, handlers.Error{Class: "rate_limited", Message: "retry", Retryable: true})
	if err != nil || e.Status != "retry_wait" {
		t.Fatalf("retry was not scheduled: %+v %v", e, err)
	}
	if _, err := store.DB().ExecContext(context.Background(), "UPDATE retry_timers SET run_at=CURRENT_TIMESTAMP WHERE execution_id=?", claim.Lease.ExecutionID); err != nil {
		t.Fatal(err)
	}
	claim, err = service.Poll(context.Background(), "worker-1", time.Minute)
	if err != nil || claim.Lease.AttemptNumber != 2 {
		t.Fatalf("second attempt was not claimable: %+v %v", claim, err)
	}
	e, err = service.Fail(context.Background(), "worker-1", claim.Lease.ID, claim.Lease.FencingToken, claim.Lease.ExecutionID, 2, handlers.Error{Class: "validation_failed", Message: "bad input"})
	if err != nil || e.Status != "failed" {
		t.Fatalf("terminal failure was not persisted: %+v %v", e, err)
	}
}

func TestCancelQueuedExecutionPersistsTerminalCallback(t *testing.T) {
	store, err := storage.Open(context.Background(), "file:"+t.TempDir()+"/db")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	r := handlers.NewRegistry()
	h, _ := handlers.NewFuncHandler(handlers.Manifest{Key: "cancel.test", Version: "1"}, func(context.Context, handlers.Request) (handlers.Result, error) {
		return handlers.Result{Output: json.RawMessage(`{}`)}, nil
	})
	_ = r.Register(h)
	s, _ := NewService(store, r)
	if _, err := s.Submit(context.Background(), ExecutionRequest{ExecutionRequestID: "cancel-1", HandlerKey: "cancel.test", HandlerVersion: "1"}); err != nil {
		t.Fatal(err)
	}
	e, err := s.Cancel(context.Background(), "exec_cancel-1")
	if err != nil {
		t.Fatal(err)
	}
	if e.Status != "cancelled" {
		t.Fatalf("status=%s", e.Status)
	}
	var status string
	if err := store.DB().QueryRowContext(context.Background(), "SELECT status FROM callbacks WHERE execution_id=?", e.ID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "cancelled" {
		t.Fatal(status)
	}
}
