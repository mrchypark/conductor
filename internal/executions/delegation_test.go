package executions

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/mrchypark/conductor/internal/handlers"
	"github.com/mrchypark/conductor/internal/storage"
)

type stubTokenExchanger struct {
	calls int
	token []byte
	exp   time.Time
	err   error
}

func (s *stubTokenExchanger) ExchangeToken(_ context.Context, _ string) ([]byte, time.Time, error) {
	s.calls++
	return s.token, s.exp, s.err
}

type stubProxyHandler struct {
	manifest handlers.Manifest
	fn       func(context.Context, handlers.Request) (handlers.Result, error)
}

func (h stubProxyHandler) Manifest() handlers.Manifest { return h.manifest }
func (h stubProxyHandler) Execute(ctx context.Context, req handlers.Request) (handlers.Result, error) {
	return h.fn(ctx, req)
}

func newDelegationTestService(t *testing.T) (*Service, *storage.Store, *handlers.Registry) {
	t.Helper()
	store, err := storage.Open(context.Background(), "file:"+t.TempDir()+"/conductor.db")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	reg := handlers.NewRegistry()
	svc, err := NewService(store, reg)
	if err != nil {
		t.Fatal(err)
	}
	return svc, store, reg
}

func TestDelegationTokenLifecycleSyncAsyncAndLeakPrevention(t *testing.T) {
	svc, store, reg := newDelegationTestService(t)
	if err := reg.Register(stubProxyHandler{
		manifest: handlers.Manifest{Key: "echo.proxy", Version: "1", Idempotency: "idempotent"},
		fn: func(ctx context.Context, req handlers.Request) (handlers.Result, error) {
			proxyFn := handlers.HostConnectorFromContext(ctx)
			if proxyFn == nil {
				return handlers.Result{}, errors.New("missing host proxy")
			}
			out, err := proxyFn(ctx, json.RawMessage(`{"provider":"hubspot"}`))
			if err != nil {
				return handlers.Result{}, err
			}
			return handlers.Result{Output: out}, nil
		},
	}); err != nil {
		t.Fatal(err)
	}

	exchanger := &stubTokenExchanger{
		token: []byte("dpop-delegation-token-xyz"),
		exp:   time.Now().UTC().Add(5 * time.Minute),
	}
	svc.SetTokenExchanger(exchanger)
	var seenTokens []string
	svc.SetHostProxy(func(workspaceID, credentialRef, delegationToken string, expiresAt time.Time) handlers.HostConnectorFunc {
		seenTokens = append(seenTokens, delegationToken)
		return func(context.Context, json.RawMessage) (json.RawMessage, error) {
			return json.RawMessage(`{"ok":true}`), nil
		}
	})

	ctx := context.Background()

	// 1. Raw token in Input must be rejected before ExchangeToken.
	_, err := svc.Submit(ctx, ExecutionRequest{
		ExecutionRequestID: "req-leak",
		WorkspaceID:        "ws1",
		HandlerKey:         "echo.proxy",
		HandlerVersion:     "1",
		CredentialRef:      "grant_1",
		SubjectToken:       "secret-subject-tok",
		Input:              json.RawMessage(`{"leaked":"Bearer secret-subject-tok"}`),
	})
	var hErr *handlers.Error
	if !errors.As(err, &hErr) || hErr.Class != "validation_failed" || exchanger.calls != 0 {
		t.Fatalf("expected validation_failed without exchange call, got err=%v calls=%d", err, exchanger.calls)
	}

	// 2. Expired deadline_at must be rejected before ExchangeToken.
	_, err = svc.Submit(ctx, ExecutionRequest{
		ExecutionRequestID: "req-expired-dl",
		WorkspaceID:        "ws1",
		HandlerKey:         "echo.proxy",
		HandlerVersion:     "1",
		CredentialRef:      "grant_1",
		SubjectToken:       "secret-subject-tok",
		DeadlineAt:         time.Now().UTC().Add(-1 * time.Second),
		Input:              json.RawMessage(`{}`),
	})
	if !errors.As(err, &hErr) || hErr.Class != "validation_failed" || exchanger.calls != 0 {
		t.Fatalf("expected validation_failed for expired deadline without exchange, got err=%v calls=%d", err, exchanger.calls)
	}

	// 3. Sync execution keeps delegation token in memory only (not persisted in Rhiza).
	syncExec, err := svc.SubmitAndExecute(ctx, ExecutionRequest{
		ExecutionRequestID: "req-sync-1",
		WorkspaceID:        "ws1",
		HandlerKey:         "echo.proxy",
		HandlerVersion:     "1",
		CredentialRef:      "grant_1",
		SubjectToken:       "secret-subject-tok",
		Input:              json.RawMessage(`{"x":1}`),
	})
	if err != nil {
		t.Fatalf("SubmitAndExecute failed: %v", err)
	}
	if syncExec.Status != "succeeded" || exchanger.calls != 1 || len(seenTokens) != 1 || seenTokens[0] != "dpop-delegation-token-xyz" {
		t.Fatalf("unexpected sync result: status=%s calls=%d seen=%v", syncExec.Status, exchanger.calls, seenTokens)
	}
	syncRec, err := store.GetExecutionRequest(ctx, "req-sync-1")
	if err != nil || len(syncRec.DelegationToken) != 0 {
		t.Fatalf("sync execution must not persist delegation token: len=%d err=%v", len(syncRec.DelegationToken), err)
	}

	// 4. Duplicate idempotent request returns existing execution without calling ExchangeToken.
	dupExec, err := svc.SubmitAndExecute(ctx, ExecutionRequest{
		ExecutionRequestID: "req-sync-1",
		WorkspaceID:        "ws1",
		HandlerKey:         "echo.proxy",
		HandlerVersion:     "1",
		CredentialRef:      "grant_1",
		SubjectToken:       "secret-subject-tok",
		Input:              json.RawMessage(`{"x":1}`),
	})
	if err != nil || dupExec.ID != syncExec.ID || exchanger.calls != 1 {
		t.Fatalf("idempotent duplicate should not re-exchange: err=%v calls=%d", err, exchanger.calls)
	}

	// 5. Async execution persists delegation token in WAL, delivers it to worker, and scrubs it on completion.
	asyncExec, err := svc.Submit(ctx, ExecutionRequest{
		ExecutionRequestID: "req-async-1",
		WorkspaceID:        "ws1",
		HandlerKey:         "echo.proxy",
		HandlerVersion:     "1",
		CredentialRef:      "grant_1",
		SubjectToken:       "secret-subject-tok",
		Input:              json.RawMessage(`{"x":2}`),
	})
	if err != nil || exchanger.calls != 2 {
		t.Fatalf("Submit async failed: err=%v calls=%d", err, exchanger.calls)
	}
	asyncRec, err := store.GetExecutionRequest(ctx, "req-async-1")
	if err != nil || string(asyncRec.DelegationToken) != "dpop-delegation-token-xyz" {
		t.Fatalf("expected delegation token persisted in WAL, got %q err=%v", string(asyncRec.DelegationToken), err)
	}

	workerCtx, cancelWorker := context.WithCancel(ctx)
	go func() {
		_ = svc.RunWorker(workerCtx, "w1", 20*time.Millisecond, time.Second)
	}()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		got, err := svc.Get(ctx, asyncExec.ID)
		if err == nil && got.Status == "succeeded" {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	cancelWorker()

	got, err := svc.Get(ctx, asyncExec.ID)
	if err != nil || got.Status != "succeeded" {
		t.Fatalf("async execution did not succeed: got=%+v err=%v", got, err)
	}
	if len(seenTokens) != 2 || seenTokens[1] != "dpop-delegation-token-xyz" {
		t.Fatalf("worker did not receive delegation token: seen=%v", seenTokens)
	}
	scrubbedRec, err := store.GetExecutionRequest(ctx, "req-async-1")
	if err != nil || len(scrubbedRec.DelegationToken) != 0 {
		t.Fatalf("expected delegation token scrubbed after terminal completion, got %q err=%v", string(scrubbedRec.DelegationToken), err)
	}
}
