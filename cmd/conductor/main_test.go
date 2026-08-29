package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/mrchypark/conductor/internal/connections"
	"github.com/mrchypark/conductor/internal/executions"
	"github.com/mrchypark/conductor/internal/handlers"
	"github.com/mrchypark/conductor/internal/storage"
)

func TestExecutionHTTPVerticalSlice(t *testing.T) {
	store, err := storage.Open(context.Background(), "file:"+t.TempDir()+"/conductor.db")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	registry := handlers.NewRegistry()
	handler, err := handlers.NewFuncHandler(handlers.Manifest{Key: "payload.echo", Version: "1"}, func(_ context.Context, req handlers.Request) (handlers.Result, error) {
		return handlers.Result{Output: req.Input}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(handler); err != nil {
		t.Fatal(err)
	}
	service, err := executions.NewService(store, registry)
	if err != nil {
		t.Fatal(err)
	}
	h := newServer(service, registry, nil)

	request := `{"execution_request_id":"er-http","handler":"payload.echo","handler_version":"1","operation":"echo","input":{"ok":true}}`
	req := httptest.NewRequest(http.MethodPost, "/v1/executions", strings.NewReader(request))
	recorder := httptest.NewRecorder()
	h.ServeHTTP(recorder, req)
	if recorder.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d body=%s", recorder.Code, recorder.Body.String())
	}
	var execution map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &execution); err != nil {
		t.Fatal(err)
	}
	if execution["status"] != "succeeded" || execution["id"] != "exec_er-http" {
		t.Fatalf("unexpected execution: %#v", execution)
	}

	get := httptest.NewRequest(http.MethodGet, "/v1/executions/exec_er-http", nil)
	getRecorder := httptest.NewRecorder()
	h.ServeHTTP(getRecorder, get)
	if getRecorder.Code != http.StatusOK {
		t.Fatalf("unexpected GET status: %d body=%s", getRecorder.Code, getRecorder.Body.String())
	}
}

func TestExecutionHTTPRejectsTrailingJSON(t *testing.T) {
	store, err := storage.Open(context.Background(), "file:"+t.TempDir()+"/conductor.db")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	registry := handlers.NewRegistry()
	handler, err := handlers.NewFuncHandler(handlers.Manifest{Key: "payload.echo", Version: "1"}, func(_ context.Context, req handlers.Request) (handlers.Result, error) {
		return handlers.Result{Output: req.Input}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(handler); err != nil {
		t.Fatal(err)
	}
	service, err := executions.NewService(store, registry)
	if err != nil {
		t.Fatal(err)
	}
	recorder := httptest.NewRecorder()
	newServer(service, registry, nil).ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/v1/executions", strings.NewReader(`{"execution_request_id":"er-trailing","handler":"payload.echo","handler_version":"1"} {}`)))
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("expected trailing JSON to be rejected, got %d: %s", recorder.Code, recorder.Body.String())
	}
}

func TestWorkerHTTPProtocol(t *testing.T) {
	store, err := storage.Open(context.Background(), "file:"+t.TempDir()+"/conductor.db")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	registry := handlers.NewRegistry()
	service, _ := executions.NewService(store, registry)
	if _, err := service.Submit(context.Background(), executions.ExecutionRequest{ExecutionRequestID: "worker-http", HandlerKey: "external", HandlerVersion: "1", Input: json.RawMessage(`{"x":1}`)}); err != nil {
		t.Fatal(err)
	}
	h := newServer(service, registry, nil)
	request := httptest.NewRequest(http.MethodPost, "/v1/worker/tasks:poll", strings.NewReader(`{"worker_id":"w1"}`))
	recorder := httptest.NewRecorder()
	h.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("poll status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	var poll struct {
		Status       string `json:"status"`
		ExecutionID  string `json:"run_id"`
		AttemptID    string `json:"attempt_id"`
		FencingToken int64  `json:"fencing_token"`
		Attempt      int    `json:"-"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &poll); err != nil || poll.Status != "assigned" || poll.ExecutionID == "" || poll.AttemptID == "" {
		t.Fatalf("unexpected poll response: %s err=%v", recorder.Body, err)
	}
	completeBody := `{"worker_id":"w1","lease_id":"` + poll.AttemptID + `","fencing_token":` + fmt.Sprint(poll.FencingToken) + `,"execution_id":"` + poll.ExecutionID + `","attempt_number":1,"output":{"ok":true}}`
	complete := httptest.NewRecorder()
	h.ServeHTTP(complete, httptest.NewRequest(http.MethodPost, "/v1/worker/tasks:complete", strings.NewReader(completeBody)))
	if complete.Code != http.StatusOK || !strings.Contains(complete.Body.String(), `"succeeded"`) {
		t.Fatalf("complete status=%d body=%s", complete.Code, complete.Body.String())
	}
}

func TestDynamicKavunHandlerRegistrationRequiresServiceToken(t *testing.T) {
	store, err := storage.Open(context.Background(), "file:"+t.TempDir()+"/conductor.db")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	registry := handlers.NewRegistry()
	service, err := executions.NewService(store, registry)
	if err != nil {
		t.Fatal(err)
	}
	requestBody := `{"manifest":{"handler_key":"transform.kavun","version":"1"},"source":"output = input"}`
	unauthenticated := httptest.NewRecorder()
	newServer(service, registry, nil).ServeHTTP(unauthenticated, httptest.NewRequest(http.MethodPost, "/v1/handlers", strings.NewReader(requestBody)))
	if unauthenticated.Code != http.StatusForbidden {
		t.Fatalf("expected registration to be forbidden, got %d: %s", unauthenticated.Code, unauthenticated.Body.String())
	}

	h := newServer(service, registry, nil)
	h.serviceToken = "secret"
	h.store = store
	registered := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/handlers", strings.NewReader(requestBody))
	req.Header.Set("Authorization", "Bearer secret")
	h.ServeHTTP(registered, req)
	if registered.Code != http.StatusCreated {
		t.Fatalf("expected registration success, got %d: %s", registered.Code, registered.Body.String())
	}
	execution, err := service.SubmitAndExecute(context.Background(), executions.ExecutionRequest{ExecutionRequestID: "kavun-http", HandlerKey: "transform.kavun", HandlerVersion: "1", Input: json.RawMessage(`{"ok":true}`)})
	if err != nil || execution.Status != "succeeded" || string(execution.Output) != `{"ok":true}` {
		t.Fatalf("unexpected dynamic execution: %#v err=%v", execution, err)
	}
	restored := handlers.NewRegistry()
	if err := loadDynamicHandlers(context.Background(), store, restored); err != nil {
		t.Fatal(err)
	}
	restoredService, err := executions.NewService(store, restored)
	if err != nil {
		t.Fatal(err)
	}
	restoredExecution, err := restoredService.SubmitAndExecute(context.Background(), executions.ExecutionRequest{ExecutionRequestID: "kavun-restored", HandlerKey: "transform.kavun", HandlerVersion: "1", Input: json.RawMessage(`{"restored":true}`)})
	if err != nil || restoredExecution.Status != "succeeded" || string(restoredExecution.Output) != `{"restored":true}` {
		t.Fatalf("unexpected restored dynamic execution: %#v err=%v", restoredExecution, err)
	}
}

func TestOAuthProviderCallbackIsReachableWithServiceAuthEnabled(t *testing.T) {
	store, err := storage.Open(context.Background(), "file:"+t.TempDir()+"/conductor.db")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	service, err := executions.NewService(store, handlers.NewRegistry())
	if err != nil {
		t.Fatal(err)
	}
	h := newServer(service, handlers.NewRegistry(), nil)
	h.serviceToken = "secret"
	recorder := httptest.NewRecorder()
	h.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/v1/oauth/google/callback?state=s&code=c", nil))
	if recorder.Code == http.StatusUnauthorized {
		t.Fatalf("provider callback was blocked by service auth: %s", recorder.Body.String())
	}
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected callback dependency error, got %d: %s", recorder.Code, recorder.Body.String())
	}
}

func TestConnectionHTTPStoresEncryptedCredential(t *testing.T) {
	store, err := storage.Open(context.Background(), "file:"+t.TempDir()+"/conductor.db")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	oauthStore, err := connections.NewStore(store.DB(), []byte("01234567890123456789012345678901"))
	if err != nil {
		t.Fatal(err)
	}
	service, err := executions.NewService(store, handlers.NewRegistry())
	if err != nil {
		t.Fatal(err)
	}
	h := newServer(service, handlers.NewRegistry(), nil)
	h.serviceToken = "secret"
	h.oauthStore = oauthStore
	recorder := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/connections", strings.NewReader(`{"connection_id":"conn-api","workspace_id":"ws","provider":"slack","api_key":"api-secret","metadata":{"module":"web"}}`))
	req.Header.Set("Authorization", "Bearer secret")
	h.ServeHTTP(recorder, req)
	if recorder.Code != http.StatusCreated {
		t.Fatalf("unexpected status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	connection, err := oauthStore.LoadConnection(context.Background(), "conn-api")
	if err != nil || connection.Metadata["api_key"] != "api-secret" {
		t.Fatalf("unexpected connection=%+v err=%v", connection, err)
	}
}
