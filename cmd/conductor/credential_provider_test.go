package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mrchypark/conductor/internal/connectors"
	"github.com/mrchypark/conductor/internal/executions"
	"github.com/mrchypark/conductor/internal/handlers"
	"github.com/mrchypark/conductor/internal/storage"
)

func TestCredentialProviderHandlerExecutesPinnedOAuthDelivery(t *testing.T) {
	const serviceToken = "conductor-service-token"
	const humanToken = "trusted-human-token"
	const upstreamToken = "upstream-token-"

	expiresAt := time.Now().Add(time.Hour).UnixMilli()
	binding := connectors.OAuthDeliveryBinding{
		GrantID: "grant-1", ProviderID: "fixture", AccountID: "account-1", ConnectionGeneration: "generation-1",
		Resource: "https://conductor.example/connections", ConsumerClientID: "conductor", OwnerSubject: "owner-1",
		Scopes: []string{"records.read"}, CredentialVersion: 1, ConsentExpiresAtUnixMS: expiresAt,
	}
	var authorityCalls, providerCalls atomic.Int32
	authority := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		call := authorityCalls.Add(1)
		if r.Method != http.MethodPost || r.URL.Path != "/auth/v1/connection-grants/grant-1/credential" || r.Header.Get("Authorization") != "Bearer "+humanToken {
			http.Error(w, "unexpected authority request", http.StatusBadRequest)
			return
		}
		if call > 2 {
			http.Error(w, "revoked", http.StatusForbidden)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"kind": "oauth2", "access_token": upstreamToken + fmt.Sprint(call), "token_type": "Bearer",
			"grant_id": binding.GrantID, "provider_id": binding.ProviderID, "connection_generation": binding.ConnectionGeneration,
			"credential_version": int64(call), "account_id": binding.AccountID, "scopes": binding.Scopes,
			"token_expires_at_unix_ms": expiresAt - 1, "consent_expires_at_unix_ms": expiresAt,
		})
	}))
	defer authority.Close()
	provider := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		call := providerCalls.Add(1)
		if r.Method != http.MethodGet || r.URL.Path != "/approved" || r.Header.Get("Authorization") != "Bearer "+upstreamToken+fmt.Sprint(call) {
			http.Error(w, "unexpected provider request", http.StatusBadRequest)
			return
		}
		_, _ = fmt.Fprintf(w, `{"ok":true,"credential_version":%d}`, call)
	}))
	defer provider.Close()
	binding.Endpoint = provider.URL + "/approved"

	dir := t.TempDir()
	tokenFile := filepath.Join(dir, "human-token")
	if err := os.WriteFile(tokenFile, []byte(humanToken+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	config := connectors.CredentialProviderConfig{Issuer: authority.URL, Bindings: []connectors.CredentialProviderBinding{{
		ConnectionID: "conn-1", WorkspaceID: "workspace-1", Revision: 1, OperationKey: "records.read", TokenFile: tokenFile, OAuth: &binding,
	}}}
	configPath := filepath.Join(dir, "credential-provider.json")
	writeConfig := func(value connectors.CredentialProviderConfig) {
		t.Helper()
		body, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(configPath, body, 0600); err != nil {
			t.Fatal(err)
		}
	}
	writeConfig(config)

	registry := handlers.NewRegistry()
	if err := loadCredentialProviderHandler(configPath, serviceToken, registry, authority.Client(), provider.Client()); err != nil {
		t.Fatal(err)
	}
	badConfig := config
	badBinding := *config.Bindings[0].OAuth
	badBinding.Scopes = []string{"records.read", "records.read"}
	badConfig.Bindings = append([]connectors.CredentialProviderBinding(nil), config.Bindings...)
	badConfig.Bindings[0].OAuth = &badBinding
	writeConfig(badConfig)
	if err := loadCredentialProviderHandler(configPath, serviceToken, handlers.NewRegistry(), authority.Client(), provider.Client()); err == nil {
		t.Fatal("duplicate scopes configured successfully")
	}
	if authorityCalls.Load() != 0 || providerCalls.Load() != 0 {
		t.Fatalf("invalid binding made network calls: authority=%d provider=%d", authorityCalls.Load(), providerCalls.Load())
	}
	writeConfig(config)

	store, err := storage.Open(context.Background(), "file:"+filepath.Join(dir, "conductor.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	service, err := executions.NewService(store, registry)
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("CONDUCTOR_SERVICE_TOKEN", serviceToken)
	server := newServer(service, registry, nil)
	workerCtx, stopWorker := context.WithCancel(context.Background())
	workerDone := make(chan error, 1)
	go func() {
		workerDone <- service.RunWorker(workerCtx, "credential-provider-test", time.Millisecond, time.Second)
	}()
	t.Cleanup(func() {
		stopWorker()
		<-workerDone
	})

	submit := func(id, workspace string, revision int64) string {
		t.Helper()
		body := fmt.Sprintf(`{"execution_request_id":%q,"workspace_id":%q,"handler":"connector.credentials.invoke","handler_version":"1","operation":"records.read","connection_id":"conn-1","credential_revision":%d,"actor":{"type":"service","id":"tester"},"idempotency_key":%q,"input":{}}`, id, workspace, revision, "idempotency-"+id)
		req := httptest.NewRequest(http.MethodPost, "/v1/executions", strings.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+serviceToken)
		req.Header.Set("Prefer", "respond-async")
		recorder := httptest.NewRecorder()
		server.ServeHTTP(recorder, req)
		if recorder.Code != http.StatusAccepted {
			t.Fatalf("submit %s: status=%d body=%s", id, recorder.Code, recorder.Body.String())
		}
		return recorder.Body.String()
	}
	waitFor := func(id, status string) executions.Execution {
		t.Helper()
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			execution, err := service.Get(context.Background(), "exec_"+id)
			if err == nil && execution.Status == status {
				return execution
			}
			time.Sleep(5 * time.Millisecond)
		}
		t.Fatalf("execution %s did not become %s", id, status)
		return executions.Execution{}
	}

	unauthorized := httptest.NewRecorder()
	server.ServeHTTP(unauthorized, httptest.NewRequest(http.MethodPost, "/v1/executions", strings.NewReader(`{}`)))
	if unauthorized.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated execution status=%d body=%s", unauthorized.Code, unauthorized.Body.String())
	}
	pinnedFailureResponse := submit("credential-provider-wrong-pin", "other-workspace", 2)
	pinnedFailure := waitFor("credential-provider-wrong-pin", "failed")
	if pinnedFailure.Error == nil || pinnedFailure.Error.Class != "connection_error" || authorityCalls.Load() != 0 || providerCalls.Load() != 0 {
		t.Fatalf("unmatched pin execution=%+v authority=%d provider=%d", pinnedFailure, authorityCalls.Load(), providerCalls.Load())
	}
	firstResponse := submit("credential-provider-1", "workspace-1", 1)
	first := waitFor("credential-provider-1", "succeeded")
	secondResponse := submit("credential-provider-2", "workspace-1", 1)
	second := waitFor("credential-provider-2", "succeeded")
	thirdResponse := submit("credential-provider-revoked", "workspace-1", 1)
	revoked := waitFor("credential-provider-revoked", "failed")
	outputVersion := func(output json.RawMessage) int {
		t.Helper()
		var value struct {
			OK      bool `json:"ok"`
			Version int  `json:"credential_version"`
		}
		if err := json.Unmarshal(output, &value); err != nil || !value.OK {
			t.Fatalf("invalid provider output=%s err=%v", output, err)
		}
		return value.Version
	}
	if outputVersion(first.Output) != 1 || outputVersion(second.Output) != 2 {
		t.Fatalf("rotation output: first=%s second=%s", first.Output, second.Output)
	}
	if revoked.Error == nil || revoked.Error.Class != "connection_error" {
		t.Fatalf("revoked execution=%+v", revoked)
	}
	if authorityCalls.Load() != 3 || providerCalls.Load() != 2 {
		t.Fatalf("calls authority=%d provider=%d", authorityCalls.Load(), providerCalls.Load())
	}
	for _, body := range []string{pinnedFailureResponse, firstResponse, secondResponse, thirdResponse, string(first.Output), string(second.Output)} {
		if strings.Contains(body, humanToken) || strings.Contains(body, upstreamToken) {
			t.Fatalf("credential leaked into execution response: %s", body)
		}
	}
}

func TestCredentialProviderConfigurationFailsClosed(t *testing.T) {
	if err := loadCredentialProviderHandler("", "", handlers.NewRegistry(), nil, nil); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "config.json")
	for _, tc := range []struct{ name, body, token string }{
		{"missing-service-auth", `{}`, ""},
		{"unknown-field", `{"issuer":"https://issuer.test","unexpected":true}`, "service"},
		{"trailing-value", `{} {}`, "service"},
		{"empty-bindings", `{"issuer":"https://issuer.test","bindings":[]}`, "service"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := os.WriteFile(path, []byte(tc.body), 0600); err != nil {
				t.Fatal(err)
			}
			registry := handlers.NewRegistry()
			if err := loadCredentialProviderHandler(path, tc.token, registry, nil, nil); err == nil {
				t.Fatal("invalid configuration accepted")
			}
			if len(registry.Manifests()) != 0 {
				t.Fatal("invalid configuration installed a handler")
			}
		})
	}
}

func TestCredentialProviderV2HTTPUsesRequestHumanToken(t *testing.T) {
	const humanToken = "request-human-token"
	expires := time.Now().Add(time.Hour).UnixMilli()
	binding := connectors.OAuthDeliveryBinding{GrantID: "grant", ProviderID: "fixture", AccountID: "account", ConnectionGeneration: "generation", Resource: "https://conductor.example/connections", ConsumerClientID: "conductor", OwnerSubject: "owner", Scopes: []string{"read"}, CredentialVersion: 1, ConsentExpiresAtUnixMS: expires}
	var calls atomic.Int32
	authority := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.Header.Get("Authorization") != "Bearer "+humanToken {
			http.Error(w, "wrong token", http.StatusUnauthorized)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"kind": "oauth2", "access_token": "upstream-secret", "token_type": "Bearer", "grant_id": binding.GrantID, "provider_id": binding.ProviderID, "connection_generation": binding.ConnectionGeneration, "credential_version": 1, "account_id": binding.AccountID, "scopes": binding.Scopes, "token_expires_at_unix_ms": expires - 1, "consent_expires_at_unix_ms": expires})
	}))
	defer authority.Close()
	provider := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer upstream-secret" {
			http.Error(w, "wrong token", http.StatusUnauthorized)
			return
		}
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer provider.Close()
	binding.Endpoint = provider.URL + "/approved"
	config := connectors.CredentialProviderConfig{Issuer: authority.URL, Bindings: []connectors.CredentialProviderBinding{{ConnectionID: "conn", WorkspaceID: "ws", Revision: 1, OperationKey: "read", OAuth: &binding}}}
	body, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	if err := os.WriteFile(path, body, 0600); err != nil {
		t.Fatal(err)
	}
	registry := handlers.NewRegistry()
	if err := loadCredentialProviderHandler(path, "service", registry, authority.Client(), provider.Client()); err != nil {
		t.Fatal(err)
	}
	store, err := storage.Open(context.Background(), "file:"+filepath.Join(dir, "conductor.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	service, err := executions.NewService(store, registry)
	if err != nil {
		t.Fatal(err)
	}
	s := &server{executions: service, registry: registry, serviceToken: "service"}
	req := httptest.NewRequest(http.MethodPost, "/v1/executions", strings.NewReader(`{"execution_request_id":"v2","workspace_id":"ws","handler":"connector.credentials.invoke","handler_version":"2","operation":"read","connection_id":"conn","credential_revision":1,"actor":{"type":"service","id":"caller"},"idempotency_key":"v2","input":{}}`))
	req.Header.Set("Authorization", "Bearer service")
	req.Header.Set("X-Conductor-Human-Authorization", "Bearer "+humanToken)
	w := httptest.NewRecorder()
	s.ServeHTTP(w, req)
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"status":"succeeded"`) || calls.Load() != 1 {
		t.Fatalf("unexpected execution status=%d body=%s calls=%d", w.Code, w.Body.String(), calls.Load())
	}
	if strings.Contains(w.Body.String(), humanToken) || strings.Contains(w.Body.String(), "upstream-secret") {
		t.Fatal("credential leaked in response")
	}
}

func TestCredentialV2RecoveryAfterRestartWithoutWorker(t *testing.T) {
	ctx := context.Background()
	dsn := "file:" + filepath.Join(t.TempDir(), "conductor.db")
	store, err := storage.Open(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	if created, err := store.CreateExecution(ctx, storage.ExecutionRequestRecord{ID: "abandoned", HandlerKey: "connector.credentials.invoke", HandlerVersion: "2"}, "exec_abandoned"); err != nil || !created {
		store.Close()
		t.Fatalf("create: %v %v", created, err)
	}
	if _, err := store.AcquireInlineLease(ctx, "exec_abandoned", time.Millisecond); err != nil {
		store.Close()
		t.Fatal(err)
	}
	if created, err := store.CreateExecution(ctx, storage.ExecutionRequestRecord{ID: "regular", HandlerKey: "example", HandlerVersion: "1", MaxAttempts: 1}, "exec_regular"); err != nil || !created {
		store.Close()
		t.Fatalf("create regular execution: %v %v", created, err)
	}
	if _, err := store.AcquireLease(ctx, "exec_regular", "remote-worker", time.Millisecond, 1); err != nil {
		store.Close()
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = storage.Open(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	recoveryCtx, cancel := context.WithCancel(ctx)
	done := startExecutionProcessing(recoveryCtx, nil, store, "", 10*time.Millisecond)
	defer func() { cancel(); <-done }()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		execution, err := store.GetExecution(ctx, "exec_abandoned")
		if err == nil && execution.Status == "failed" {
			if execution.ErrorClass != "outcome_unknown" {
				t.Fatalf("unexpected recovery error: %s", execution.ErrorClass)
			}
			if _, err := store.AcquireInlineLease(ctx, "exec_abandoned", time.Minute); err == nil {
				t.Fatal("recovered execution can be replayed")
			}
			regular, err := store.GetExecution(ctx, "exec_regular")
			if err != nil || regular.Status != "running" {
				t.Fatalf("recovery changed regular execution: %+v %v", regular, err)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("abandoned execution was not recovered without a worker")
}

func TestCredentialV2RecoveryWhileWorkerIsBusy(t *testing.T) {
	ctx := context.Background()
	store, err := storage.Open(ctx, "file:"+filepath.Join(t.TempDir(), "conductor.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	registry := handlers.NewRegistry()
	started := make(chan struct{})
	release := make(chan struct{})
	handler, err := handlers.NewFuncHandler(handlers.Manifest{Key: "busy", Version: "1"}, func(ctx context.Context, _ handlers.Request) (handlers.Result, error) {
		close(started)
		select {
		case <-release:
			return handlers.Result{Output: json.RawMessage(`{}`)}, nil
		case <-ctx.Done():
			return handlers.Result{}, ctx.Err()
		}
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
	if _, err := service.Submit(ctx, executions.ExecutionRequest{ExecutionRequestID: "busy", HandlerKey: "busy", HandlerVersion: "1"}); err != nil {
		t.Fatal(err)
	}
	workerCtx, cancel := context.WithCancel(ctx)
	done := startExecutionProcessing(workerCtx, service, store, "worker", 10*time.Millisecond)
	defer func() { cancel(); close(release); <-done }()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not start")
	}
	if created, err := store.CreateExecution(ctx, storage.ExecutionRequestRecord{ID: "inline-busy", HandlerKey: "connector.credentials.invoke", HandlerVersion: "2"}, "exec_inline-busy"); err != nil || !created {
		t.Fatalf("create inline execution: %v %v", created, err)
	}
	if _, err := store.AcquireInlineLease(ctx, "exec_inline-busy", time.Millisecond); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		execution, err := store.GetExecution(ctx, "exec_inline-busy")
		if err == nil && execution.Status == "failed" && execution.ErrorClass == "outcome_unknown" {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("inline execution was not recovered while worker was busy")
}
