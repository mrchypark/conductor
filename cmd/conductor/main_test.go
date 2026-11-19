package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mrchypark/conductor/internal/callbacks"
	ampconnectors "github.com/mrchypark/conductor/internal/connectors"
	"github.com/mrchypark/conductor/internal/executions"
	"github.com/mrchypark/conductor/internal/handlers"
	"github.com/mrchypark/conductor/internal/storage"
	"github.com/mrchypark/rhiza"
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
	req := httptest.NewRequest(http.MethodPost, "/v1/workspaces/ws-1/executions", strings.NewReader(request))
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

	get := httptest.NewRequest(http.MethodGet, "/v1/workspaces/ws-1/executions/exec_er-http", nil)
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
	newServer(service, registry, nil).ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/v1/workspaces/ws-1/executions", strings.NewReader(`{"execution_request_id":"er-trailing","handler":"payload.echo","handler_version":"1"} {}`)))
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("expected trailing JSON to be rejected, got %d: %s", recorder.Code, recorder.Body.String())
	}
}

func TestWorkflowEventEnvelopeQueuesLinkedExecution(t *testing.T) {
	store, err := storage.Open(context.Background(), "file:"+t.TempDir()+"/conductor.db")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	registry := handlers.NewRegistry()
	handler, err := handlers.NewFuncHandler(handlers.Manifest{Key: "demo.run", Version: "1"}, func(_ context.Context, req handlers.Request) (handlers.Result, error) {
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
	body := `{"events":[{"id":"evt-1","name":"demo.run","idempotency_key":"execution-dispatch:er-event","data":{"hello":"world","subject_ref":{"type":"task","id":"task-1"},"_execution":{"workspace_id":"ws-1","execution_request_id":"er-event","task_id":"task-1"}}}]}`
	post := func() map[string]any {
		recorder := httptest.NewRecorder()
		h.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/v1/workspaces/ws-1/events", strings.NewReader(body)))
		if recorder.Code != http.StatusAccepted {
			t.Fatalf("unexpected event status=%d body=%s", recorder.Code, recorder.Body.String())
		}
		var response map[string]any
		if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
			t.Fatal(err)
		}
		return response
	}
	first, second := post(), post()
	firstEvents := first["events"].([]any)
	secondEvents := second["events"].([]any)
	if firstEvents[0].(map[string]any)["execution_id"] != "exec_er-event" || secondEvents[0].(map[string]any)["execution_id"] != "exec_er-event" {
		t.Fatalf("event was not idempotently linked: first=%#v second=%#v", first, second)
	}
	execution, err := service.Get(context.Background(), "exec_er-event")
	if err != nil || execution.Status != "queued" {
		t.Fatalf("unexpected queued execution=%+v err=%v", execution, err)
	}
	request, err := store.GetExecutionRequest(context.Background(), "er-event")
	if err != nil {
		t.Fatal(err)
	}
	if string(request.Input) != `{"hello":"world"}` || string(request.SubjectRef) != `{"type":"task","id":"task-1"}` {
		t.Fatalf("reserved Workflow context leaked into request: input=%s subject=%s", request.Input, request.SubjectRef)
	}
}

func TestWorkflowEventEnvelopeRequiresExecutionLinkage(t *testing.T) {
	store, err := storage.Open(context.Background(), "file:"+t.TempDir()+"/conductor.db")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	service, err := executions.NewService(store, handlers.NewRegistry())
	if err != nil {
		t.Fatal(err)
	}
	recorder := httptest.NewRecorder()
	newServer(service, handlers.NewRegistry(), nil).ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/v1/workspaces/ws-1/events", strings.NewReader(`{"events":[{"name":"business.ready","data":{}}]}`)))
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("expected unlinked event rejection, got %d: %s", recorder.Code, recorder.Body.String())
	}
}

func TestEventExecutionContextValidation(t *testing.T) {
	for _, data := range []string{
		`{"_execution":[]}`,
		`{"_execution":{"workspace_id":"ws","execution_request_id":"er","timeout_seconds":-1}}`,
		`{"_execution":{"workspace_id":"ws","execution_request_id":"er","deadline_at":"invalid"}}`,
	} {
		_, err := executionRequestFromEvent(eventRequest{Name: "example.run", Data: json.RawMessage(data)})
		if err == nil || !strings.Contains(err.Error(), "data._execution") {
			t.Fatalf("invalid execution context accepted: data=%s err=%v", data, err)
		}
	}
}

func TestWorkflowFunctionBindingNativeAndConnectorShareExecutionContract(t *testing.T) {
	store, err := storage.Open(context.Background(), "file:"+t.TempDir()+"/conductor.db")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	var connectorCalls int
	var connectorHeaders http.Header
	var connectorMu sync.Mutex
	connectorAPI := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/v1/connector/objects/orders/records/o1" {
			http.NotFound(w, r)
			return
		}
		connectorMu.Lock()
		connectorCalls++
		connectorHeaders = r.Header.Clone()
		connectorMu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"o1","status":"ready"}`))
	}))
	defer connectorAPI.Close()

	var callbackMu sync.Mutex
	callbacksByRequest := make(map[string]map[string]any)
	callbackAPI := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload map[string]any
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			http.Error(w, "invalid callback", http.StatusBadRequest)
			return
		}
		requestID, _ := payload["execution_request_id"].(string)
		callbackMu.Lock()
		callbacksByRequest[requestID] = payload
		callbackMu.Unlock()
		w.WriteHeader(http.StatusAccepted)
	}))
	defer callbackAPI.Close()
	store.SetCallbackTarget(callbackAPI.URL)

	registry := handlers.NewRegistry()
	var nativeLease handlers.Request
	nativeV1, err := handlers.NewFuncHandler(handlers.Manifest{Key: "native.calculate", Version: "1"}, func(_ context.Context, request handlers.Request) (handlers.Result, error) {
		nativeLease = request
		return handlers.Result{Output: json.RawMessage(`{"kind":"native","version":"1"}`)}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	nativeV2, err := handlers.NewFuncHandler(handlers.Manifest{Key: "native.calculate", Version: "2"}, func(_ context.Context, _ handlers.Request) (handlers.Result, error) {
		return handlers.Result{Output: json.RawMessage(`{"kind":"native","version":"2"}`)}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, handler := range []handlers.Handler{nativeV1, nativeV2} {
		if err := registry.Register(handler); err != nil {
			t.Fatal(err)
		}
	}
	catalog, err := ampconnectors.LoadCatalog()
	if err != nil {
		t.Fatal(err)
	}
	catalog.SetBusinessClient(&ampconnectors.BusinessClient{BaseURL: connectorAPI.URL, Client: connectorAPI.Client()})
	connectorHandler, err := catalog.NewInvokeHandlerScoped("2", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(connectorHandler); err != nil {
		t.Fatal(err)
	}
	service, err := executions.NewService(store, registry)
	if err != nil {
		t.Fatal(err)
	}
	h := newServer(service, registry, catalog)
	submitEvent := func(body string, requestID string) {
		recorder := httptest.NewRecorder()
		h.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/v1/workspaces/ws-contract/events", strings.NewReader(body)))
		if recorder.Code != http.StatusAccepted {
			t.Fatalf("canonical Workflow event was not accepted: status=%d body=%s", recorder.Code, recorder.Body.String())
		}
		execution, err := service.Get(context.Background(), "exec_"+requestID)
		version := "1"
		if requestID == "er-connector" {
			version = "2"
		}
		if err != nil || execution.Status != "queued" || execution.HandlerVersion != version {
			t.Fatalf("request was not queued with pinned handler version: execution=%+v err=%v", execution, err)
		}
	}
	submitEvent(`{"events":[{"id":"evt-native","name":"native.calculate","idempotency_key":"execution:er-native","data":{"value":2,"subject_ref":{"type":"task","id":"task-er-native"},"_execution":{"workspace_id":"ws-contract","execution_request_id":"er-native","workflow_key":"wf-contract","workflow_version":3,"workflow_instance_id":"wi-contract","task_id":"task-er-native","node_key":"node-er-native","handler_key":"native.calculate","handler_version":"1","operation_key":"calculate","actor":{"type":"user","id":"user-1"}}}}]}`, "er-native")
	submitEvent(`{"events":[{"id":"evt-connector","name":"connector.invoke","idempotency_key":"execution:er-connector","data":{"provider":"business","operation":"business.records.get","object_name":"orders","record_id":"o1","_execution":{"workspace_id":"ws-contract","execution_request_id":"er-connector","workflow_key":"wf-contract","workflow_version":3,"workflow_instance_id":"wi-contract","task_id":"task-er-connector","node_key":"node-er-connector","handler_key":"connector.invoke","handler_version":"2","operation_key":"business.records.get","actor":{"type":"user","id":"user-1"}}}}]}`, "er-connector")

	workerID := "contract-worker"
	claims := make(map[string]executions.Claim, 2)
	for i := 0; i < 2; i++ {
		claim, err := service.Poll(context.Background(), workerID, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		if claim.Lease.WorkerID != workerID || claim.Lease.FencingToken <= 0 || claim.Lease.AttemptNumber != 1 {
			t.Fatalf("invalid worker lease: %+v", claim.Lease)
		}
		claims[claim.Request.ExecutionRequestID] = claim
	}
	for _, id := range []string{"er-native", "er-connector"} {
		claim := claims[id]
		handler, err := registry.Resolve(claim.Request.HandlerKey, claim.Request.HandlerVersion)
		if err != nil {
			t.Fatal(err)
		}
		result, err := handler.Execute(context.Background(), handlers.Request{
			ExecutionID: claim.Lease.ExecutionID, ExecutionRequestID: claim.Request.ExecutionRequestID,
			WorkspaceID: claim.Request.WorkspaceID, WorkflowKey: claim.Request.WorkflowKey, WorkflowVersion: claim.Request.WorkflowVersion,
			WorkflowInstanceID: claim.Request.WorkflowInstanceID, TaskID: claim.Request.TaskID, NodeKey: claim.Request.NodeKey,
			HandlerKey: claim.Request.HandlerKey, HandlerVersion: claim.Request.HandlerVersion, OperationKey: claim.Request.OperationKey,
			ConnectionID: claim.Request.ConnectionID, SubjectRef: claim.Request.SubjectRef, InputRef: claim.Request.InputRef,
			DeadlineAt: claim.Request.DeadlineAt, AttemptNumber: claim.Lease.AttemptNumber, LeaseID: claim.Lease.ID,
			FencingToken: claim.Lease.FencingToken, IdempotencyKey: claim.Request.IdempotencyKey,
			ActorType: claim.Request.Actor.Type, ActorID: claim.Request.Actor.ID, Input: claim.Request.Input,
		})
		if err != nil {
			t.Fatalf("handler %s failed: %v", id, err)
		}
		if id == "er-native" && string(result.Output) != `{"kind":"native","version":"1"}` {
			t.Fatalf("pinned native version was not executed: %s", result.Output)
		}
		if id == "er-connector" && !strings.Contains(string(result.Output), `"status":"ready"`) {
			t.Fatalf("unexpected connector result: %s", result.Output)
		}
		terminal, err := service.Complete(context.Background(), workerID, claim.Lease.ID, claim.Lease.FencingToken, claim.Lease.ExecutionID, result.Output, claim.Lease.AttemptNumber)
		if err != nil || terminal.Status != "succeeded" {
			t.Fatalf("terminal completion failed for %s: execution=%+v err=%v", id, terminal, err)
		}
	}
	if nativeLease.LeaseID == "" || nativeLease.FencingToken <= 0 || nativeLease.AttemptNumber != 1 {
		t.Fatalf("native handler did not receive worker fencing context: %+v", nativeLease)
	}

	delivery := &callbacks.Delivery{Store: store, Client: callbackAPI.Client()}
	if err := delivery.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	callbackMu.Lock()
	defer callbackMu.Unlock()
	if len(callbacksByRequest) != 2 {
		t.Fatalf("expected callbacks for both handler kinds, got %#v", callbacksByRequest)
	}
	for _, id := range []string{"er-native", "er-connector"} {
		payload := callbacksByRequest[id]
		if payload["runtime_status"] != "succeeded" || payload["attempt_count"] != float64(1) || payload["execution_request_id"] != id || payload["workspace_id"] != "ws-contract" {
			t.Fatalf("unexpected callback for %s: %#v", id, payload)
		}
	}
	connectorMu.Lock()
	defer connectorMu.Unlock()
	if connectorCalls != 1 || connectorHeaders.Get("X-Conductor-Attempt-Id") == "" || connectorHeaders.Get("X-Conductor-Attempt-Number") != "1" || connectorHeaders.Get("X-Conductor-Execution-Request-Id") != "er-connector" {
		t.Fatalf("connector did not receive pinned execution context: calls=%d headers=%v", connectorCalls, connectorHeaders)
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
	requestBody := `{"manifest":{"handler_key":"transform.starlark","version":"1"},"source":"def run(input):\n    return input\n"}`
	h := newServer(service, registry, nil)
	h.store = store
	builtin, err := handlers.NewFuncHandler(handlers.Manifest{Key: "built/in@key", Version: "v 1"}, func(context.Context, handlers.Request) (handlers.Result, error) { return handlers.Result{}, nil })
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(builtin); err != nil {
		t.Fatal(err)
	}
	registered := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/workspaces/ws-1/handlers", strings.NewReader(requestBody))
	h.ServeHTTP(registered, req)
	if registered.Code != http.StatusCreated {
		t.Fatalf("expected registration success, got %d: %s", registered.Code, registered.Body.String())
	}
	for _, tc := range []struct {
		name, target string
		status       int
	}{
		{"stored", "/v1/workspaces/ws-1/handlers/definition?handler_key=transform.starlark&version=1", 200},
		{"other workspace", "/v1/workspaces/ws-2/handlers/definition?handler_key=transform.starlark&version=1", 404},
		{"missing", "/v1/workspaces/ws-1/handlers/definition?handler_key=missing&version=1", 404},
		{"builtin", "/v1/workspaces/ws-1/handlers/definition?handler_key=built%2Fin%40key&version=v+1", 404},
		{"duplicate", "/v1/workspaces/ws-1/handlers/definition?handler_key=x&handler_key=y&version=1", 400},
		{"missing version", "/v1/workspaces/ws-1/handlers/definition?handler_key=x", 400},
		{"invalid escape", "/v1/workspaces/ws-1/handlers/definition?handler_key=x%zz&version=1", 400},
		{"extra", "/v1/workspaces/ws-1/handlers/definition?handler_key=x&version=1&other=y", 400},
	} {
		t.Run(tc.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodGet, tc.target, nil)
			response := httptest.NewRecorder()
			h.ServeHTTP(response, request)
			if response.Code != tc.status || response.Header().Get("Cache-Control") != "no-store" {
				t.Fatalf("status=%d headers=%v body=%s", response.Code, response.Header(), response.Body.String())
			}
			if tc.status == 200 {
				var definition map[string]json.RawMessage
				if err := json.Unmarshal(response.Body.Bytes(), &definition); err != nil || string(definition["kind"]) != `"starlark"` || definition["manifest"] == nil || definition["source"] == nil || definition["max_steps"] == nil || len(definition) != 4 {
					t.Fatalf("unexpected definition: %s, err=%v", response.Body.String(), err)
				}
			}
		})
	}
	unavailable := newServer(service, registry, nil)
	request := httptest.NewRequest(http.MethodGet, "/v1/workspaces/ws-1/handlers/definition?handler_key=x&version=1", nil)
	response := httptest.NewRecorder()
	unavailable.ServeHTTP(response, request)
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected unavailable store, got %d", response.Code)
	}
	execution, err := service.SubmitAndExecute(context.Background(), executions.ExecutionRequest{WorkspaceID: "ws-1", ExecutionRequestID: "starlark-http", HandlerKey: "transform.starlark", HandlerVersion: "1", Input: json.RawMessage(`{"ok":true}`)})
	if err != nil || execution.Status != "succeeded" || string(execution.Output) != `{"ok":true}` {
		t.Fatalf("unexpected dynamic execution: %#v err=%v", execution, err)
	}
	restored := handlers.NewRegistry()
	if err := loadStarlarkHandlers(context.Background(), store, restored); err != nil {
		t.Fatal(err)
	}
	restoredService, err := executions.NewService(store, restored)
	if err != nil {
		t.Fatal(err)
	}
	restoredExecution, err := restoredService.SubmitAndExecute(context.Background(), executions.ExecutionRequest{WorkspaceID: "ws-1", ExecutionRequestID: "starlark-restored", HandlerKey: "transform.starlark", HandlerVersion: "1", Input: json.RawMessage(`{"restored":true}`)})
	if err != nil || restoredExecution.Status != "succeeded" || string(restoredExecution.Output) != `{"restored":true}` {
		t.Fatalf("unexpected restored dynamic execution: %#v err=%v", restoredExecution, err)
	}
}

func TestHandlerDefinitionEscapedIdentifiersAndIntegrity(t *testing.T) {
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
	h := newServer(service, registry, nil)
	h.store = store
	key := strings.Repeat("k", 2048) + "/@?&+\n"
	version := "v/1 &+\n"
	escapeHeavy := strings.Repeat("<", 8000)
	manifestJSON, err := json.Marshal(handlers.Manifest{Key: key, Version: version, ResourceClass: escapeHeavy})
	if err != nil || !bytes.Contains(manifestJSON, []byte(`\u003c`)) || len(manifestJSON) <= len(escapeHeavy)*5 {
		t.Fatalf("manifest escaping: size=%d err=%v", len(manifestJSON), err)
	}
	src := "def run(input):\n    return input\n"
	var body bytes.Buffer
	encoder := json.NewEncoder(&body)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(handlerRegisterRequest{Manifest: handlers.Manifest{Key: key, Version: version, ResourceClass: escapeHeavy}, Source: src}); err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "/v1/workspaces/ws-1/handlers", &body)
	response := httptest.NewRecorder()
	h.ServeHTTP(response, request)
	if response.Code != http.StatusCreated {
		t.Fatalf("registration: %d %s", response.Code, response.Body.String())
	}
	values := url.Values{"handler_key": {key}, "version": {version}}
	request = httptest.NewRequest(http.MethodGet, "/v1/workspaces/ws-1/handlers/definition?"+values.Encode(), nil)
	response = httptest.NewRecorder()
	h.ServeHTTP(response, request)
	var definition handlerRegisterRequest
	if err := json.Unmarshal(response.Body.Bytes(), &definition); response.Code != http.StatusOK || err != nil || definition.Manifest.Key != key || definition.Manifest.Version != version || definition.Manifest.ResourceClass != escapeHeavy || definition.Source != src {
		t.Fatalf("definition status=%d err=%v", response.Code, err)
	}
	manifest, _ := json.Marshal(handlers.Manifest{Key: "corrupt", Version: "1"})
	hash := sha256.Sum256([]byte("different source"))
	if _, err := store.SaveStarlarkHandler(context.Background(), storage.StarlarkHandlerRecord{WorkspaceID: "ws-1", HandlerKey: "corrupt", Version: "1", ManifestJSON: manifest, Source: []byte(src), SourceHash: fmt.Sprintf("%x", hash[:])}); err != nil {
		t.Fatal(err)
	}
	request = httptest.NewRequest(http.MethodGet, "/v1/workspaces/ws-1/handlers/definition?handler_key=corrupt&version=1", nil)
	response = httptest.NewRecorder()
	h.ServeHTTP(response, request)
	if response.Code != http.StatusInternalServerError || strings.Contains(response.Body.String(), "def run") {
		t.Fatalf("corrupt definition: %d %s", response.Code, response.Body.String())
	}
}

func TestHandlerDefinitionRealServerRequestTargetLimit(t *testing.T) {
	server := httptest.NewServer(&server{})
	defer server.Close()
	valid, err := http.NewRequest(http.MethodGet, server.URL+"/v1/workspaces/ws-1/handlers/definition?handler_key="+strings.Repeat("x", 2048)+"%2F%40%0A&version=1", nil)
	if err != nil {
		t.Fatal(err)
	}
	accepted, err := server.Client().Do(valid)
	if err != nil {
		t.Fatal(err)
	}
	accepted.Body.Close()
	if accepted.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("valid request status=%d, want %d", accepted.StatusCode, http.StatusServiceUnavailable)
	}
	request, err := http.NewRequest(http.MethodGet, server.URL+"/v1/workspaces/ws-1/handlers/definition?handler_key="+strings.Repeat("x", http.DefaultMaxHeaderBytes+8192)+"&version=1", nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err := server.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusRequestHeaderFieldsTooLarge {
		t.Fatalf("oversized request status=%d, want %d", response.StatusCode, http.StatusRequestHeaderFieldsTooLarge)
	}
}

func TestWASMHandlerMultipartUploadAndLazyRestore(t *testing.T) {
	store, err := storage.Open(context.Background(), "file:"+t.TempDir()+"/conductor.db")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	wasm, err := base64.StdEncoding.DecodeString("AGFzbQEAAAABEQNgAX8Bf2ACf38AYAJ/fwF+AwQDAAECBQMBABEGGQN/AUGAgMAAC38AQYSAwAALfwBBkIDAAAsHQAYGbWVtb3J5AgAFYWxsb2MAAAdkZWFsbG9jAAEGaW52b2tlAAIKX19kYXRhX2VuZAMBC19faGVhcF9iYXNlAwIKNQMjAQF/QQAgAEEAKAKAgMCAACIBakEHakF4cTYCgIDAgAAgAQsCAAsMACAArUIghiABrYQLCw0BAEGAgMAACwQABAAAAFQEbmFtZQAUE2NvbmR1Y3Rvcl9lY2hvLndhc20BGQMABWFsbG9jAQdkZWFsbG9jAgZpbnZva2UHEgEAD19fc3RhY2tfcG9pbnRlcgkIAQAFLmRhdGEARQlwcm9kdWNlcnMBDHByb2Nlc3NlZC1ieQEFcnVzdGMlMS45NS4wLW5pZ2h0bHkgKGQyMjJkZGM0ZCAyMDI2LTAxLTIzKQCUAQ90YXJnZXRfZmVhdHVyZXMIKwtidWxrLW1lbW9yeSsPYnVsay1tZW1vcnktb3B0KxZjYWxsLWluZGlyZWN0LW92ZXJsb25nKwptdWx0aXZhbHVlKw9tdXRhYmxlLWdsb2JhbHMrE25vbnRyYXBwaW5nLWZwdG9pbnQrD3JlZmVyZW5jZS10eXBlcysIc2lnbi1leHQ=")
	if err != nil {
		t.Fatal(err)
	}
	pushed := 0
	fetched := 0
	push := func(_ context.Context, workspaceID, key, version string, b []byte) (wasmPushedArtifact, error) {
		pushed++
		return wasmPushedArtifact{
			ArtifactRef:    "registry.example/" + workspaceID + "/transform@sha256:manifest",
			ManifestDigest: "sha256:manifest",
			BlobDigest:     "sha256:blob",
			Size:           int64(len(b)),
		}, nil
	}
	fetch := func(context.Context, string) (wasmArtifact, error) {
		fetched++
		return wasmArtifact{Bytes: wasm, ManifestDigest: "sha256:manifest", BlobDigest: "sha256:blob", Size: int64(len(wasm))}, nil
	}
	runtime := handlers.NewWASMRuntime(context.Background())
	defer runtime.Close(context.Background())
	registry := handlers.NewRegistry()
	service, _ := executions.NewService(store, registry)
	h := newServer(service, registry, nil)
	h.store, h.wasmRuntime, h.wasmFetch, h.wasmPush = store, runtime, fetch, push

	buildMultipart := func(manifestJSON, abi string, wasmData []byte) (*bytes.Buffer, string) {
		var buf bytes.Buffer
		mw := multipart.NewWriter(&buf)
		_ = mw.WriteField("manifest", manifestJSON)
		if abi != "" {
			_ = mw.WriteField("abi", abi)
		}
		fw, _ := mw.CreateFormFile("wasm", "handler.wasm")
		_, _ = fw.Write(wasmData)
		_ = mw.Close()
		return &buf, mw.FormDataContentType()
	}

	// Invalid WASM must fail pre-validation BEFORE pushing to OCI.
	badBuf, badCT := buildMultipart(`{"handler_key":"bad.wasm","version":"1"}`, "conductor:function/v1", []byte("not wasm"))
	badReq := httptest.NewRequest(http.MethodPost, "/v1/workspaces/ws-1/handlers/wasm", badBuf)
	badReq.Header.Set("Content-Type", badCT)
	badRec := httptest.NewRecorder()
	h.ServeHTTP(badRec, badReq)
	if badRec.Code != http.StatusBadRequest || pushed != 0 {
		t.Fatalf("expected 400 and 0 OCI pushes for invalid wasm, got code=%d pushed=%d body=%s", badRec.Code, pushed, badRec.Body.String())
	}

	// Valid WASM upload succeeds and pushes once.
	okBuf, okCT := buildMultipart(`{"handler_key":"transform.wasm","version":"1"}`, "conductor:function/v1", wasm)
	okReq := httptest.NewRequest(http.MethodPost, "/v1/workspaces/ws-1/handlers/wasm", okBuf)
	okReq.Header.Set("Content-Type", okCT)
	okRec := httptest.NewRecorder()
	h.ServeHTTP(okRec, okReq)
	if okRec.Code != http.StatusCreated || pushed != 1 {
		t.Fatalf("register status=%d pushed=%d body=%s", okRec.Code, pushed, okRec.Body.String())
	}

	execution, err := service.SubmitAndExecute(context.Background(), executions.ExecutionRequest{WorkspaceID: "ws-1", ExecutionRequestID: "wasm-http", HandlerKey: "transform.wasm", HandlerVersion: "1", Input: json.RawMessage(`{"ok":true}`)})
	if err != nil || execution.Status != "succeeded" || string(execution.Output) != `{"ok":true}` {
		t.Fatalf("unexpected wasm execution: %#v err=%v", execution, err)
	}

	restoredRuntime := handlers.NewWASMRuntime(context.Background())
	defer restoredRuntime.Close(context.Background())
	restored := handlers.NewRegistry()
	if err := loadWASMHandlers(context.Background(), store, restored, restoredRuntime, fetch); err != nil {
		t.Fatal(err)
	}
	if fetched != 0 {
		t.Fatalf("startup unexpectedly fetched artifact: %d", fetched)
	}
	restoredHandler, err := restored.ResolveScoped("ws-1", "transform.wasm", "1")
	if err != nil {
		t.Fatal(err)
	}
	result, err := restoredHandler.Execute(context.Background(), handlers.Request{Input: json.RawMessage(`{"restored":true}`)})
	if err != nil || string(result.Output) != `{"restored":true}` || fetched != 1 {
		t.Fatalf("lazy restored output=%s fetches=%d err=%v", result.Output, fetched, err)
	}
}

func TestOCIWASMFetcherRequiresAllowlistedDigestReference(t *testing.T) {
	digest := "sha256:" + strings.Repeat("0", 64)
	fetch := newOCIWASMFetcher("registry.example", "", "", "", false)
	if _, err := fetch(context.Background(), "other.example/functions/demo@"+digest); err == nil || !strings.Contains(err.Error(), "not allowed") {
		t.Fatalf("expected registry rejection, got %v", err)
	}
	if _, err := fetch(context.Background(), "registry.example/functions/demo:latest"); err == nil || !strings.Contains(err.Error(), "pinned") {
		t.Fatalf("expected mutable tag rejection, got %v", err)
	}
}

func TestWorkspaceHeaderAndCustomConnectorHTTP(t *testing.T) {
	store, err := storage.Open(context.Background(), "file:"+t.TempDir()+"/conductor.db")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	catalog, err := ampconnectors.LoadCatalog()
	if err != nil {
		t.Fatal(err)
	}
	service, err := executions.NewService(store, handlers.NewRegistry())
	if err != nil {
		t.Fatal(err)
	}
	h := newServer(service, handlers.NewRegistry(), catalog)
	h.store = store
	h.requireWorkspaceHeader = true

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/workspaces/ws-1/connectors", nil))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for missing X-Workspace-Id, got %d", rec.Code)
	}

	rec = httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/workspaces/ws-1/connectors", nil)
	req.Header.Set("X-Workspace-Id", "ws-2")
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for mismatched X-Workspace-Id, got %d", rec.Code)
	}

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/v1/workspaces/ws-1/connectors", strings.NewReader(`{"name":"mycrm","base_url":"https://api.mycrm.example","auth_type":"apiKey"}`))
	req.Header.Set("X-Workspace-Id", "ws-1")
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusCreated {
		t.Fatalf("expected 201 creating custom connector, got %d: %s", rec.Code, rec.Body.String())
	}

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/v1/workspaces/ws-2/connectors/mycrm", nil)
	req.Header.Set("X-Workspace-Id", "ws-2")
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for cross-workspace connector, got %d", rec.Code)
	}
}

func TestRhizaConfigLoadsHAAndS3Environment(t *testing.T) {
	t.Setenv("CONDUCTOR_RHIZA_CLUSTER_ID", "cluster-ha")
	t.Setenv("CONDUCTOR_RHIZA_NODE_ID", "n2")
	t.Setenv("CONDUCTOR_RHIZA_PEER_ADDR", "127.0.0.1:9002")
	t.Setenv("CONDUCTOR_RHIZA_PEER_TOKEN", "peer-n2")
	members := []rhiza.Member{{ID: "n1", PeerURL: "quic://127.0.0.1:9001", PublicKey: rhiza.PeerPublicKey("cluster-ha", "n1", "peer-n1")}, {ID: "n2", PeerURL: "quic://127.0.0.1:9002", PublicKey: rhiza.PeerPublicKey("cluster-ha", "n2", "peer-n2")}, {ID: "n3", PeerURL: "quic://127.0.0.1:9003", PublicKey: rhiza.PeerPublicKey("cluster-ha", "n3", "peer-n3")}}
	membersJSON, err := json.Marshal(members)
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("CONDUCTOR_RHIZA_MEMBERS", string(membersJSON))
	t.Setenv("CONDUCTOR_RHIZA_S3_ENDPOINT", "127.0.0.1:9000")
	t.Setenv("CONDUCTOR_RHIZA_S3_BUCKET", "rhiza")
	t.Setenv("CONDUCTOR_RHIZA_S3_INSECURE", "true")
	config, err := rhizaConfig(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if config.ClusterID != "cluster-ha" || config.NodeID != "n2" || len(config.Members) != 3 || config.ObjStoreProvider != "s3" || config.ObjStoreBucket != "rhiza" || !config.ObjStoreInsecure {
		t.Fatalf("unexpected Rhiza config: %+v", config)
	}
	if config.PeerToken != "peer-n2" || config.Members[1].PublicKey != rhiza.PeerPublicKey(config.ClusterID, config.NodeID, config.PeerToken) {
		t.Fatal("Rhiza peer identity configuration mismatch")
	}
}
