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
	select {
	case <-started:
	case err := <-first:
		t.Fatalf("first submit failed before handler start: %v", err)
	}
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
	callback, err := store.Callback(context.Background(), execution.ID)
	if err != nil {
		t.Fatal(err)
	}
	var payload map[string]any
	if err := json.Unmarshal(callback.Payload, &payload); err != nil {
		t.Fatal(err)
	}
	if payload["runtime_status"] != "succeeded" || payload["attempt_count"] != float64(2) {
		t.Fatalf("unexpected callback payload: %s", callback.Payload)
	}
	events, err := store.ListExecutionEvents(context.Background(), execution.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) < 3 {
		t.Fatalf("expected requested, started, and terminal events, got %d", len(events))
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

func TestConnectorMutationUsesOneAttemptWithoutANewPolicyModel(t *testing.T) {
	store, err := storage.Open(context.Background(), "file:"+t.TempDir()+"/db")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	registry := handlers.NewRegistry()
	var calls int
	handler, err := handlers.NewFuncHandler(handlers.Manifest{Key: "connector.invoke", Version: "1"}, func(context.Context, handlers.Request) (handlers.Result, error) {
		calls++
		return handlers.Result{}, &handlers.Error{Class: "provider_5xx", Message: "ambiguous write", Retryable: true}
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
	execution, err := service.SubmitAndExecute(context.Background(), ExecutionRequest{
		ExecutionRequestID: "mutation-budget", HandlerKey: "connector.invoke", HandlerVersion: "1", OperationKey: "records.update",
		Retry: RetryPolicy{MaxAttempts: 3, Backoff: time.Millisecond},
	})
	if err != nil {
		t.Fatal(err)
	}
	if calls != 1 || execution.Status != "failed" {
		t.Fatalf("mutation was replayed: calls=%d execution=%+v", calls, execution)
	}
	request, err := store.GetExecutionRequest(context.Background(), "mutation-budget")
	if err != nil {
		t.Fatal(err)
	}
	if request.MaxAttempts != 1 {
		t.Fatalf("mutation retry budget was not pinned to one: %+v", request)
	}
}

func TestSubmitAndExecuteUnknownHandlerVersionFailsTerminally(t *testing.T) {
	store, err := storage.Open(context.Background(), "file:"+t.TempDir()+"/db")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	service, err := NewService(store, handlers.NewRegistry())
	if err != nil {
		t.Fatal(err)
	}
	execution, err := service.SubmitAndExecute(context.Background(), ExecutionRequest{
		ExecutionRequestID: "unknown-handler",
		WorkspaceID:        "ws-unknown",
		HandlerKey:         "missing.handler",
		HandlerVersion:     "9",
		OperationKey:       "run",
	})
	if err != nil {
		t.Fatal(err)
	}
	if execution.Status != "failed" || execution.Error == nil || execution.Error.Class != "handler_not_found" {
		t.Fatalf("unknown pinned handler did not fail terminally: %+v", execution)
	}
	callback, err := store.Callback(context.Background(), execution.ID)
	if err != nil {
		t.Fatal(err)
	}
	var callbackPayload map[string]any
	if err := json.Unmarshal(callback.Payload, &callbackPayload); err != nil {
		t.Fatal(err)
	}
	callbackWorkspace, _ := callbackPayload["workspace_id"].(string)
	if callbackWorkspace != "ws-unknown" {
		t.Fatalf("callback lost execution workspace: %q", callbackWorkspace)
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

func TestExternalWorkerRejectsStaleFencingTokenAfterLeaseRecovery(t *testing.T) {
	store, err := storage.Open(context.Background(), "file:"+t.TempDir()+"/db")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	service, err := NewService(store, handlers.NewRegistry())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Submit(context.Background(), ExecutionRequest{ExecutionRequestID: "stale-fence", WorkspaceID: "ws-1", HandlerKey: "remote.handler", HandlerVersion: "1", OperationKey: "run", Retry: RetryPolicy{MaxAttempts: 2}}); err != nil {
		t.Fatal(err)
	}
	first, err := service.Poll(context.Background(), "worker-1", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.RecoverExpiredLeases(context.Background(), time.Now().Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	second, err := service.Poll(context.Background(), "worker-2", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if second.Lease.FencingToken <= first.Lease.FencingToken {
		t.Fatalf("fencing token did not advance after recovery: first=%+v second=%+v", first.Lease, second.Lease)
	}
	if second.Lease.AttemptNumber != first.Lease.AttemptNumber+1 {
		t.Fatalf("lease recovery did not advance attempt number: first=%+v second=%+v", first.Lease, second.Lease)
	}
	if _, err := service.Complete(context.Background(), "worker-1", first.Lease.ID, first.Lease.FencingToken, first.Lease.ExecutionID, json.RawMessage(`{"stale":true}`), first.Lease.AttemptNumber); err == nil {
		t.Fatalf("stale worker completion was accepted: %v", err)
	}
	terminal, err := service.Complete(context.Background(), "worker-2", second.Lease.ID, second.Lease.FencingToken, second.Lease.ExecutionID, json.RawMessage(`{"ok":true}`), second.Lease.AttemptNumber)
	if err != nil || terminal.Status != "succeeded" {
		t.Fatalf("current worker could not complete recovered execution: %+v %v", terminal, err)
	}
}

func TestExpiredLeaseExhaustsBudgetAsOutcomeUnknown(t *testing.T) {
	store, err := storage.Open(context.Background(), "file:"+t.TempDir()+"/db")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	service, err := NewService(store, handlers.NewRegistry())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Submit(context.Background(), ExecutionRequest{
		ExecutionRequestID: "expired-budget", WorkspaceID: "ws-1", HandlerKey: "connector.invoke", HandlerVersion: "1", OperationKey: "records.update",
		Retry: RetryPolicy{MaxAttempts: 3},
	}); err != nil {
		t.Fatal(err)
	}
	claim, err := service.Poll(context.Background(), "worker-1", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if claim.Lease.AttemptNumber != 1 {
		t.Fatalf("unexpected first attempt: %+v", claim.Lease)
	}
	if err := store.RecoverExpiredLeases(context.Background(), time.Now().Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	execution, err := service.Get(context.Background(), claim.Lease.ExecutionID)
	if err != nil {
		t.Fatal(err)
	}
	if execution.Status != "failed" || execution.Error == nil || execution.Error.Class != "outcome_unknown" {
		t.Fatalf("expired mutation was not terminalized safely: %+v", execution)
	}
	callback, err := store.Callback(context.Background(), claim.Lease.ExecutionID)
	if err != nil {
		t.Fatal(err)
	}
	var callbackPayload map[string]any
	if err := json.Unmarshal(callback.Payload, &callbackPayload); err != nil {
		t.Fatal(err)
	}
	callbackStatus, _ := callbackPayload["runtime_status"].(string)
	callbackClass, _ := callbackPayload["error_class"].(string)
	callbackWorkspace, _ := callbackPayload["workspace_id"].(string)
	if callbackStatus != "failed" || callbackClass != "outcome_unknown" || callbackWorkspace != "ws-1" {
		t.Fatalf("unexpected outcome callback: status=%q class=%q workspace=%q", callbackStatus, callbackClass, callbackWorkspace)
	}
	if _, err := service.Poll(context.Background(), "worker-2", time.Minute); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("expired mutation was requeued: %v", err)
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
	time.Sleep(5 * time.Millisecond)
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
	callback, err := store.Callback(context.Background(), e.ID)
	if err != nil {
		t.Fatal(err)
	}
	if callback.Status != "cancelled" {
		t.Fatal(callback.Status)
	}
}
