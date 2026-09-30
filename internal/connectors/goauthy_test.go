package connectors

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/mrchypark/conductor/internal/handlers"
)

func TestGoAuthyHandlerPinsBindingAndReadsFreshToken(t *testing.T) {
	binding := deliveryBinding()
	var authorityCalls, providerCalls atomic.Int32
	var wantHuman atomic.Value
	wantHuman.Store("human-one")
	authority := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authorityCalls.Add(1)
		if got := r.Header.Get("Authorization"); got != "Bearer "+wantHuman.Load().(string) {
			t.Errorf("authority authorization = %q", got)
		}
		writeDelivery(t, w, binding, deliveryKey)
	}))
	defer authority.Close()
	provider := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		providerCalls.Add(1)
		if got := r.Header.Get("Authorization"); got != deliveryKey {
			t.Errorf("provider authorization = %q", got)
		}
		_, _ = w.Write([]byte(`{"items":[]}`))
	}))
	defer provider.Close()

	tokenFile := filepath.Join(t.TempDir(), "human-token")
	if err := os.WriteFile(tokenFile, []byte("human-one\n"), 0600); err != nil {
		t.Fatal(err)
	}
	handler, err := NewGoAuthyHandler(GoAuthyConfig{Issuer: authority.URL, Bindings: []GoAuthyBinding{{ConnectionID: "connection", WorkspaceID: "workspace", Revision: 3, OperationKey: "expense-statements.read", TokenFile: tokenFile, APIKey: &binding}}}, authority.Client(), rewriteClient(provider))
	if err != nil {
		t.Fatal(err)
	}
	if manifest := handler.Manifest(); manifest.Key != "connector.goauthy.invoke" || manifest.Version != "1" {
		t.Fatalf("manifest = %+v", manifest)
	}
	_, err = handler.Execute(context.Background(), handlers.Request{ConnectionID: "connection", WorkspaceID: "other", CredentialRevision: 3, OperationKey: "expense-statements.read"})
	var failure *handlers.Error
	if !errors.As(err, &failure) || failure.Class != "connection_error" || authorityCalls.Load() != 0 || providerCalls.Load() != 0 {
		t.Fatalf("mismatched execution result=%v authority=%d provider=%d", err, authorityCalls.Load(), providerCalls.Load())
	}
	for _, token := range []string{"human-one", "human-two"} {
		wantHuman.Store(token)
		if err := os.WriteFile(tokenFile, []byte(token+"\n"), 0600); err != nil {
			t.Fatal(err)
		}
		result, err := handler.Execute(context.Background(), handlers.Request{ConnectionID: "connection", WorkspaceID: "workspace", CredentialRevision: 3, OperationKey: "expense-statements.read", Input: json.RawMessage(`{ }`)})
		if err != nil || string(result.Output) != `{"items":[]}` {
			t.Fatalf("execution result=%s err=%v", result.Output, err)
		}
	}
	if authorityCalls.Load() != 2 || providerCalls.Load() != 2 {
		t.Fatalf("authority=%d provider=%d", authorityCalls.Load(), providerCalls.Load())
	}
}

func TestGoAuthyHandlerDenialBlocksProvider(t *testing.T) {
	binding := deliveryBinding()
	var providerCalls atomic.Int32
	authority := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "denied", http.StatusUnauthorized)
	}))
	defer authority.Close()
	provider := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		providerCalls.Add(1)
	}))
	defer provider.Close()

	tokenFile := filepath.Join(t.TempDir(), "human-token")
	if err := os.WriteFile(tokenFile, []byte("human-token\n"), 0600); err != nil {
		t.Fatal(err)
	}
	handler, err := NewGoAuthyHandler(GoAuthyConfig{Issuer: authority.URL, Bindings: []GoAuthyBinding{{ConnectionID: "connection", WorkspaceID: "workspace", Revision: 1, OperationKey: "expense-statements.read", TokenFile: tokenFile, APIKey: &binding}}}, authority.Client(), rewriteClient(provider))
	if err != nil {
		t.Fatal(err)
	}
	_, err = handler.Execute(context.Background(), handlers.Request{ConnectionID: "connection", WorkspaceID: "workspace", CredentialRevision: 1, OperationKey: "expense-statements.read"})
	var failure *handlers.Error
	if !errors.As(err, &failure) || failure.Class != "connection_error" || providerCalls.Load() != 0 {
		t.Fatalf("denied execution result=%v provider=%d", err, providerCalls.Load())
	}
}
