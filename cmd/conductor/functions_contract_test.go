package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/mrchypark/conductor/internal/callbacks"
	ampconnectors "github.com/mrchypark/conductor/internal/connectors"
	"github.com/mrchypark/conductor/internal/executions"
	"github.com/mrchypark/conductor/internal/handlers"
	"github.com/mrchypark/conductor/internal/storage"
)

func TestWorkspaceFunctionsContractAndCausationPropagation(t *testing.T) {
	store, err := storage.Open(context.Background(), "file:"+t.TempDir()+"/conductor.db")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	var webhookHeaders http.Header
	var webhookBody map[string]any
	webhookSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		webhookHeaders = r.Header.Clone()
		_ = json.NewDecoder(r.Body).Decode(&webhookBody)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer webhookSrv.Close()

	reg := handlers.NewRegistry()
	cat, err := ampconnectors.LoadCatalog()
	if err != nil {
		t.Fatal(err)
	}
	svc, err := executions.NewService(store, reg)
	if err != nil {
		t.Fatal(err)
	}
	svc.SetCallbackTarget(webhookSrv.URL)

	srv := newServer(svc, reg, cat)
	srv.store = store

	// 1. POST /v1/workspaces/ws-alpha/functions
	regReq := httptest.NewRequest(http.MethodPost, "/v1/workspaces/ws-alpha/functions", bytes.NewBufferString(`{
		"kind": "starlark",
		"manifest": {"handler_key": "fn.greet", "version": "1", "idempotency": "idempotent"},
		"source": "def run(input):\n  return {'hello': input.get('name', 'world')}\n"
	}`))
	regRec := httptest.NewRecorder()
	srv.ServeHTTP(regRec, regReq)
	if regRec.Code != http.StatusCreated {
		t.Fatalf("POST /functions status=%d body=%s", regRec.Code, regRec.Body.String())
	}

	// 2. GET /v1/workspaces/ws-alpha/functions
	listReq := httptest.NewRequest(http.MethodGet, "/v1/workspaces/ws-alpha/functions", nil)
	listRec := httptest.NewRecorder()
	srv.ServeHTTP(listRec, listReq)
	if listRec.Code != http.StatusOK {
		t.Fatalf("GET /functions status=%d", listRec.Code)
	}
	var listed []handlers.Manifest
	if err := json.Unmarshal(listRec.Body.Bytes(), &listed); err != nil || len(listed) != 1 || listed[0].Key != "fn.greet" {
		t.Fatalf("unexpected GET /functions body=%s err=%v", listRec.Body.String(), err)
	}

	// 3. POST /v1/workspaces/ws-alpha/functions/fn.greet/versions (default key from path)
	verReq := httptest.NewRequest(http.MethodPost, "/v1/workspaces/ws-alpha/functions/fn.greet/versions", bytes.NewBufferString(`{
		"kind": "starlark",
		"manifest": {"version": "2", "idempotency": "idempotent"},
		"source": "def run(input):\n  return {'hello_v2': input.get('name', 'world')}\n"
	}`))
	verRec := httptest.NewRecorder()
	srv.ServeHTTP(verRec, verReq)
	if verRec.Code != http.StatusCreated {
		t.Fatalf("POST /functions/{id}/versions status=%d body=%s", verRec.Code, verRec.Body.String())
	}

	// 4. GET /v1/workspaces/ws-alpha/functions/fn.greet/versions/2
	getVerReq := httptest.NewRequest(http.MethodGet, "/v1/workspaces/ws-alpha/functions/fn.greet/versions/2", nil)
	getVerRec := httptest.NewRecorder()
	srv.ServeHTTP(getVerRec, getVerReq)
	if getVerRec.Code != http.StatusOK {
		t.Fatalf("GET /functions/{id}/versions/{ver} status=%d body=%s", getVerRec.Code, getVerRec.Body.String())
	}
	var gotManifest handlers.Manifest
	if err := json.Unmarshal(getVerRec.Body.Bytes(), &gotManifest); err != nil || gotManifest.Key != "fn.greet" || gotManifest.Version != "2" {
		t.Fatalf("unexpected manifest: %+v err=%v", gotManifest, err)
	}

	// Cross-workspace isolation: ws-beta must not see ws-alpha function version.
	isoReq := httptest.NewRequest(http.MethodGet, "/v1/workspaces/ws-beta/functions/fn.greet/versions/2", nil)
	isoRec := httptest.NewRecorder()
	srv.ServeHTTP(isoRec, isoReq)
	if isoRec.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for cross-workspace function version, got %d", isoRec.Code)
	}

	// 5. POST /v1/workspaces/ws-alpha/executions with X-Attempt-ID & X-Causation-Event-ID
	execReq := httptest.NewRequest(http.MethodPost, "/v1/workspaces/ws-alpha/executions", bytes.NewBufferString(`{
		"execution_request_id": "req-cause-1",
		"handler_key": "fn.greet",
		"handler_version": "2",
		"input": {"name": "operable"}
	}`))
	execReq.Header.Set("X-Causation-Event-ID", "evt-cause-99")
	execReq.Header.Set("X-Attempt-ID", "att-42")
	execRec := httptest.NewRecorder()
	srv.ServeHTTP(execRec, execReq)
	if execRec.Code != http.StatusOK {
		t.Fatalf("POST /executions status=%d body=%s", execRec.Code, execRec.Body.String())
	}
	var execResp executions.Execution
	if err := json.Unmarshal(execRec.Body.Bytes(), &execResp); err != nil || execResp.Status != "succeeded" {
		t.Fatalf("unexpected execution response: %+v err=%v", execResp, err)
	}

	// 6. GET /v1/workspaces/ws-alpha/executions/{id}/events includes causation metadata
	evReq := httptest.NewRequest(http.MethodGet, "/v1/workspaces/ws-alpha/executions/"+execResp.ID+"/events", nil)
	evRec := httptest.NewRecorder()
	srv.ServeHTTP(evRec, evReq)
	if evRec.Code != http.StatusOK {
		t.Fatalf("GET /executions/{id}/events status=%d", evRec.Code)
	}
	if !bytes.Contains(evRec.Body.Bytes(), []byte("evt-cause-99")) || !bytes.Contains(evRec.Body.Bytes(), []byte("att-42")) {
		t.Fatalf("expected causation metadata in execution events, got %s", evRec.Body.String())
	}

	// 7. Webhook callback delivery propagates X-Causation-Event-ID and X-Attempt-ID headers
	delivery := &callbacks.Delivery{Store: store, Client: webhookSrv.Client(), Now: time.Now}
	if err := delivery.RunOnce(context.Background()); err != nil {
		t.Fatalf("callback delivery failed: %v", err)
	}
	if webhookHeaders.Get("X-Causation-Event-ID") != "evt-cause-99" || webhookHeaders.Get("X-Attempt-ID") != "att-42" {
		t.Fatalf("unexpected webhook causation headers: %+v", webhookHeaders)
	}
	if webhookBody["causation_id"] != "evt-cause-99" || webhookBody["attempt_id"] != "att-42" {
		t.Fatalf("unexpected webhook body causation fields: %+v", webhookBody)
	}
}
