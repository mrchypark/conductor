package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mrchypark/conductor/internal/callbacks"
	"github.com/mrchypark/conductor/internal/connections"
	ampconnectors "github.com/mrchypark/conductor/internal/connectors"
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

func TestComposEventEnvelopeQueuesLinkedExecution(t *testing.T) {
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
	body := `{"events":[{"id":"evt-1","name":"demo.run","idempotency_key":"execution-dispatch:er-event","data":{"hello":"world","subject_ref":{"type":"task","id":"task-1"},"_compos":{"workspace_id":"ws-1","execution_request_id":"er-event","task_id":"task-1"}}}]}`
	post := func() map[string]any {
		recorder := httptest.NewRecorder()
		h.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/v1/events", strings.NewReader(body)))
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
		t.Fatalf("reserved Compos context leaked into request: input=%s subject=%s", request.Input, request.SubjectRef)
	}
}

func TestComposEventEnvelopeRequiresExecutionLinkage(t *testing.T) {
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
	newServer(service, handlers.NewRegistry(), nil).ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/v1/events", strings.NewReader(`{"events":[{"name":"business.ready","data":{}}]}`)))
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("expected unlinked event rejection, got %d: %s", recorder.Code, recorder.Body.String())
	}
}

func TestComposFunctionBindingNativeAndConnectorShareExecutionContract(t *testing.T) {
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
	catalog.SetComposClient(&ampconnectors.ComposClient{BaseURL: connectorAPI.URL, Client: connectorAPI.Client()})
	connectorHandler, err := catalog.NewInvokeHandlerScoped("1", nil, nil)
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
		h.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/v1/events", strings.NewReader(body)))
		if recorder.Code != http.StatusAccepted {
			t.Fatalf("canonical Compos event was not accepted: status=%d body=%s", recorder.Code, recorder.Body.String())
		}
		execution, err := service.Get(context.Background(), "exec_"+requestID)
		if err != nil || execution.Status != "queued" || execution.HandlerVersion != "1" {
			t.Fatalf("request was not queued with pinned handler version: execution=%+v err=%v", execution, err)
		}
	}
	submitEvent(`{"events":[{"id":"evt-native","name":"native.calculate","idempotency_key":"execution:er-native","data":{"value":2,"subject_ref":{"type":"task","id":"task-er-native"},"_compos":{"workspace_id":"ws-contract","execution_request_id":"er-native","workflow_key":"wf-contract","workflow_version":3,"workflow_instance_id":"wi-contract","task_id":"task-er-native","node_key":"node-er-native","handler_key":"native.calculate","handler_version":"1","operation_key":"calculate","actor":{"type":"user","id":"user-1"}}}}]}`, "er-native")
	submitEvent(`{"events":[{"id":"evt-connector","name":"connector.invoke","idempotency_key":"execution:er-connector","data":{"provider":"compos","operation":"records.get","object_name":"orders","record_id":"o1","_compos":{"workspace_id":"ws-contract","execution_request_id":"er-connector","workflow_key":"wf-contract","workflow_version":3,"workflow_instance_id":"wi-contract","task_id":"task-er-connector","node_key":"node-er-connector","handler_key":"connector.invoke","handler_version":"1","operation_key":"records.get","actor":{"type":"user","id":"user-1"}}}}]}`, "er-connector")

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
	oauthStore, err := connections.NewStore(context.Background(), store.Persistence(), []byte("01234567890123456789012345678901"))
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

func TestConnectorCredentialsResolverEnforcesConnectionScope(t *testing.T) {
	store, err := storage.Open(context.Background(), "file:"+t.TempDir()+"/conductor.db")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	connectionStore, err := connections.NewStore(context.Background(), store.Persistence(), []byte("01234567890123456789012345678901"))
	if err != nil {
		t.Fatal(err)
	}
	if err := connectionStore.SaveConnection(context.Background(), "conn-scope", "ws-owner", "slack", nil, nil, map[string]string{"api_key": "secret"}, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	catalog, err := ampconnectors.LoadCatalog()
	if err != nil {
		t.Fatal(err)
	}
	resolver := connectorCredentialsResolver(connectionStore, catalog)
	if _, err := resolver(context.Background(), "conn-scope", "slack", "ws-other"); err == nil || err.Error() != "connection scope mismatch" {
		t.Fatalf("workspace scope mismatch was accepted: %v", err)
	}
	if _, err := resolver(context.Background(), "conn-scope", "salesforce", "ws-owner"); err == nil || err.Error() != "connection scope mismatch" {
		t.Fatalf("provider scope mismatch was accepted: %v", err)
	}
	credentials, err := resolver(context.Background(), "conn-scope", "slack", "ws-owner")
	if err != nil || credentials.APIKey != "secret" {
		t.Fatalf("valid scoped connection was not resolved: %+v %v", credentials, err)
	}
}

func TestRhizaConfigLoadsHAAndS3Environment(t *testing.T) {
	t.Setenv("CONDUCTOR_RHIZA_CLUSTER_ID", "cluster-ha")
	t.Setenv("CONDUCTOR_RHIZA_NODE_ID", "n2")
	t.Setenv("CONDUCTOR_RHIZA_PEER_ADDR", "127.0.0.1:9002")
	t.Setenv("CONDUCTOR_RHIZA_MEMBERS", `[{"node_id":"n1","peer_url":"quic://127.0.0.1:9001","token":"secret"},{"node_id":"n2","peer_url":"quic://127.0.0.1:9002","token":"secret"},{"node_id":"n3","peer_url":"quic://127.0.0.1:9003","token":"secret"}]`)
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
}
