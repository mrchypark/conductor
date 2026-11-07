package connectors

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mrchypark/conductor/internal/handlers"
)

func TestCredentialProviderHandlerPinsBindingAndReadsFreshToken(t *testing.T) {
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
	handler, err := NewCredentialProviderHandler(CredentialProviderConfig{Issuer: authority.URL, Bindings: []CredentialProviderBinding{{ConnectionID: "connection", WorkspaceID: "workspace", Revision: 3, OperationKey: "expense-statements.read", TokenFile: tokenFile, APIKey: &binding}}}, authority.Client(), rewriteClient(provider))
	if err != nil {
		t.Fatal(err)
	}
	if manifest := handler.Manifest(); manifest.Key != "connector.credentials.invoke" || manifest.Version != "1" {
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

func TestCredentialProviderV2ContextToken(t *testing.T) {
	for _, kind := range []string{"api_key", "oauth"} {
		t.Run(kind, func(t *testing.T) {
			api := deliveryBinding()
			oauth := oauthBinding(time.Now())
			var authorityCalls, providerCalls atomic.Int32
			var expected atomic.Value
			authority := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				authorityCalls.Add(1)
				if r.Header.Get("Authorization") != "Bearer "+expected.Load().(string) {
					t.Error("wrong human token")
				}
				if kind == "api_key" {
					writeDelivery(t, w, api, deliveryKey)
				} else {
					writeOAuthDelivery(w, oauth, oauthDeliverySecret, oauth.CredentialVersion)
				}
			}))
			defer authority.Close()
			provider := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				providerCalls.Add(1)
				want := deliveryKey
				if kind == "oauth" {
					want = "Bearer " + oauthDeliverySecret
				}
				if r.Header.Get("Authorization") != want {
					t.Error("provider did not receive the delivered credential")
				}
				_, _ = w.Write([]byte(`{}`))
			}))
			defer provider.Close()
			binding := CredentialProviderBinding{ConnectionID: "connection", WorkspaceID: "workspace", Revision: 3, OperationKey: "read", TokenFile: "/nonexistent"}
			providerClient := rewriteClient(provider)
			if kind == "api_key" {
				binding.APIKey = &api
			} else {
				binding.OAuth = &oauth
				providerClient = rewriteGitHubClient(provider)
			}
			handler, err := NewCredentialProviderHandlerV2(CredentialProviderConfig{Issuer: authority.URL, Bindings: []CredentialProviderBinding{binding}}, authority.Client(), providerClient)
			if err != nil {
				t.Fatal(err)
			}
			if handler.Manifest().Version != "2" {
				t.Fatal("wrong version")
			}
			request := handlers.Request{ConnectionID: "connection", WorkspaceID: "workspace", CredentialRevision: 3, OperationKey: "read"}
			for _, token := range []string{"human-one", "human-two"} {
				expected.Store(token)
				if _, err := handler.Execute(WithHumanToken(context.Background(), token), request); err != nil {
					t.Fatal(err)
				}
			}
			for _, ctx := range []context.Context{context.Background(), WithHumanToken(context.Background(), "bad token"), WithHumanToken(context.Background(), strings.Repeat("x", 8193))} {
				if _, err := handler.Execute(ctx, request); err == nil || strings.Contains(err.Error(), "bad token") {
					t.Fatal("invalid token accepted or exposed")
				}
			}
			for _, mutate := range []func(*handlers.Request){func(r *handlers.Request) { r.ConnectionID = "other" }, func(r *handlers.Request) { r.WorkspaceID = "other" }, func(r *handlers.Request) { r.CredentialRevision++ }, func(r *handlers.Request) { r.OperationKey = "other" }, func(r *handlers.Request) { r.Input = json.RawMessage(`{"token":"secret"}`) }} {
				bad := request
				mutate(&bad)
				if _, err := handler.Execute(WithHumanToken(context.Background(), "human-secret"), bad); err == nil || strings.Contains(err.Error(), "human-secret") {
					t.Fatal("invalid request accepted or token exposed")
				}
			}
			if authorityCalls.Load() != 2 || providerCalls.Load() != 2 {
				t.Fatalf("calls authority=%d provider=%d", authorityCalls.Load(), providerCalls.Load())
			}
		})
	}
}

func TestCredentialProviderV3ProviderIdentity(t *testing.T) {
	binding := oauthBinding(time.Now())
	var authorityCalls, providerCalls atomic.Int32
	authority := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authorityCalls.Add(1)
		if r.Header.Get("Authorization") != "Bearer human-token" {
			t.Error("wrong human authorization")
		}
		writeOAuthDelivery(w, binding, oauthDeliverySecret, binding.CredentialVersion)
	}))
	defer authority.Close()
	provider := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		providerCalls.Add(1)
		if r.Method != http.MethodGet || r.URL.Path != "/user" || r.Header.Get("Authorization") != "Bearer "+oauthDeliverySecret {
			t.Error("wrong provider request or authorization")
		}
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer provider.Close()
	unsupported := binding
	unsupported.ProviderID = "unknown-provider-for-test"
	config := CredentialProviderConfig{Issuer: authority.URL, Bindings: []CredentialProviderBinding{
		{ConnectionID: "connection", WorkspaceID: "workspace", Revision: 1, OperationKey: "read", OAuth: &binding},
		{ConnectionID: "unsupported", WorkspaceID: "workspace", Revision: 1, OperationKey: "read", OAuth: &unsupported},
	}}
	for _, constructor := range []struct {
		version    string
		newHandler func(CredentialProviderConfig, *http.Client, *http.Client) (handlers.Handler, error)
	}{{"2", NewCredentialProviderHandlerV2}, {"3", NewCredentialProviderHandlerV3}} {
		if constructor.version == "2" {
			binding.ProviderID = unsupported.ProviderID
		} else {
			binding.ProviderID = "github"
		}
		handler, err := constructor.newHandler(config, authority.Client(), rewriteGitHubClient(provider))
		if err != nil || handler.Manifest().Version != constructor.version {
			t.Fatalf("version %s: handler=%v err=%v", constructor.version, handler, err)
		}
		result, err := handler.Execute(WithHumanToken(context.Background(), "human-token"), handlers.Request{ConnectionID: "connection", WorkspaceID: "workspace", CredentialRevision: 1, OperationKey: "read"})
		if err != nil || string(result.Output) != `{"ok":true}` {
			t.Fatalf("version %s: output=%s err=%v", constructor.version, result.Output, err)
		}
		if constructor.version == "3" {
			_, err = handler.Execute(WithHumanToken(context.Background(), "human-token"), handlers.Request{ConnectionID: "unsupported", WorkspaceID: "workspace", CredentialRevision: 1, OperationKey: "read"})
			if err == nil || authorityCalls.Load() != 2 || providerCalls.Load() != 2 {
				t.Fatalf("unsupported provider: err=%v authority=%d provider=%d", err, authorityCalls.Load(), providerCalls.Load())
			}
		}
	}
	if authorityCalls.Load() != 2 || providerCalls.Load() != 2 {
		t.Fatalf("authority=%d provider=%d", authorityCalls.Load(), providerCalls.Load())
	}
}

func TestCredentialProviderVersionsShareOAuthVersionGuard(t *testing.T) {
	for _, first := range []string{"2", "3"} {
		t.Run(first, func(t *testing.T) {
			binding := oauthBinding(time.Now())
			var authorityCalls, providerCalls atomic.Int32
			authority := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				version := binding.CredentialVersion
				if authorityCalls.Add(1) == 1 {
					version++
				}
				writeOAuthDelivery(w, binding, oauthDeliverySecret, version)
			}))
			defer authority.Close()
			provider := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				providerCalls.Add(1)
				_, _ = w.Write([]byte(`{}`))
			}))
			defer provider.Close()
			config := CredentialProviderConfig{Issuer: authority.URL, Bindings: []CredentialProviderBinding{{ConnectionID: "connection", WorkspaceID: "workspace", Revision: 1, OperationKey: "read", OAuth: &binding}}}
			v2, v3, err := NewCredentialProviderHandlers(config, authority.Client(), rewriteGitHubClient(provider))
			if err != nil {
				t.Fatal(err)
			}
			high, low := v2, v3
			if first == "3" {
				high, low = v3, v2
			}
			ctx := WithHumanToken(context.Background(), "human-token")
			request := handlers.Request{ConnectionID: "connection", WorkspaceID: "workspace", CredentialRevision: 1, OperationKey: "read"}
			if _, err := high.Execute(ctx, request); err != nil {
				t.Fatalf("newer credential: %v", err)
			}
			if _, err := low.Execute(ctx, request); err == nil || authorityCalls.Load() != 2 || providerCalls.Load() != 1 {
				t.Fatalf("older credential: err=%v authority=%d provider=%d", err, authorityCalls.Load(), providerCalls.Load())
			}
		})
	}
}

func TestCredentialProviderV3RejectsMismatchedOAuthDelivery(t *testing.T) {
	binding := oauthBinding(time.Now())
	var authorityCalls, providerCalls atomic.Int64
	authority := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authorityCalls.Add(1)
		delivery := oauthDeliveryMap(binding, oauthDeliverySecret, binding.CredentialVersion)
		delivery["account_id"] = "wrong-account"
		writeJSON(w, delivery)
	}))
	defer authority.Close()
	handler, err := NewCredentialProviderHandlerV3(CredentialProviderConfig{Issuer: authority.URL, Bindings: []CredentialProviderBinding{{ConnectionID: "connection", WorkspaceID: "workspace", Revision: 1, OperationKey: "read", OAuth: &binding}}}, authority.Client(), noNetworkClient(&providerCalls))
	if err != nil {
		t.Fatal(err)
	}
	_, err = handler.Execute(WithHumanToken(context.Background(), "human-token"), handlers.Request{ConnectionID: "connection", WorkspaceID: "workspace", CredentialRevision: 1, OperationKey: "read"})
	if err == nil || strings.Contains(err.Error(), oauthDeliverySecret) || authorityCalls.Load() != 1 || providerCalls.Load() != 0 {
		t.Fatalf("err=%v authority=%d provider=%d", err, authorityCalls.Load(), providerCalls.Load())
	}
}

func TestCredentialProviderV3OAuthDoesNotRetryOrFollowRedirects(t *testing.T) {
	for _, scenario := range []string{"authority-conflict", "authority-redirect", "provider-redirect"} {
		t.Run(scenario, func(t *testing.T) {
			binding := oauthBinding(time.Now())
			var authorityCalls, providerCalls, redirectCalls atomic.Int32
			redirect := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { redirectCalls.Add(1) }))
			defer redirect.Close()
			authority := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				authorityCalls.Add(1)
				if r.Header.Get("Authorization") != "Bearer human-token" {
					t.Error("wrong human authorization")
				}
				switch scenario {
				case "authority-conflict":
					w.WriteHeader(http.StatusConflict)
				case "authority-redirect":
					http.Redirect(w, r, redirect.URL, http.StatusTemporaryRedirect)
				default:
					writeOAuthDelivery(w, binding, oauthDeliverySecret, binding.CredentialVersion)
				}
			}))
			defer authority.Close()
			provider := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				providerCalls.Add(1)
				if r.Header.Get("Authorization") != "Bearer "+oauthDeliverySecret {
					t.Error("wrong provider authorization")
				}
				http.Redirect(w, r, redirect.URL, http.StatusTemporaryRedirect)
			}))
			defer provider.Close()
			handler, err := NewCredentialProviderHandlerV3(CredentialProviderConfig{Issuer: authority.URL, Bindings: []CredentialProviderBinding{{ConnectionID: "connection", WorkspaceID: "workspace", Revision: 1, OperationKey: "read", OAuth: &binding}}}, authority.Client(), rewriteGitHubClient(provider))
			if err != nil {
				t.Fatal(err)
			}
			_, err = handler.Execute(WithHumanToken(context.Background(), "human-token"), handlers.Request{ConnectionID: "connection", WorkspaceID: "workspace", CredentialRevision: 1, OperationKey: "read"})
			wantProvider := int32(0)
			if scenario == "provider-redirect" {
				wantProvider = 1
			}
			if err == nil || strings.Contains(err.Error(), oauthDeliverySecret) || authorityCalls.Load() != 1 || providerCalls.Load() != wantProvider || redirectCalls.Load() != 0 {
				t.Fatalf("err=%v authority=%d provider=%d redirect=%d", err, authorityCalls.Load(), providerCalls.Load(), redirectCalls.Load())
			}
		})
	}
}

func TestCredentialProviderV2ConcurrentTokens(t *testing.T) {
	binding := deliveryBinding()
	arrived := make(chan string, 2)
	release := make(chan struct{})
	authority := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token := r.Header.Get("Authorization")
		arrived <- token
		select {
		case <-release:
		case <-r.Context().Done():
			return
		}
		switch token {
		case "Bearer human-one":
			writeDelivery(t, w, binding, "provider-key-one")
		case "Bearer human-two":
			writeDelivery(t, w, binding, "provider-key-two")
		default:
			t.Error("unexpected human token")
		}
	}))
	defer authority.Close()
	provider := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Header.Get("Authorization") {
		case "provider-key-one":
			_, _ = w.Write([]byte(`{"owner":"one"}`))
		case "provider-key-two":
			_, _ = w.Write([]byte(`{"owner":"two"}`))
		default:
			t.Error("unexpected provider key")
		}
	}))
	defer provider.Close()
	handler, err := NewCredentialProviderHandlerV2(CredentialProviderConfig{Issuer: authority.URL, Bindings: []CredentialProviderBinding{{ConnectionID: "connection", WorkspaceID: "workspace", Revision: 1, OperationKey: "read", APIKey: &binding}}}, authority.Client(), rewriteClient(provider))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	request := handlers.Request{ConnectionID: "connection", WorkspaceID: "workspace", CredentialRevision: 1, OperationKey: "read"}
	results := make(chan bool, 2)
	for _, owner := range []string{"one", "two"} {
		go func() {
			result, err := handler.Execute(WithHumanToken(ctx, "human-"+owner), request)
			results <- err == nil && string(result.Output) == `{"owner":"`+owner+`"}`
		}()
	}
	for range 2 {
		select {
		case <-arrived:
		case <-ctx.Done():
			close(release)
			t.Fatal("requests did not overlap")
		}
	}
	close(release)
	for range 2 {
		if !<-results {
			t.Error("request received another request's result or failed")
		}
	}
}

func TestCredentialProviderHandlerDenialBlocksSaaS(t *testing.T) {
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
	handler, err := NewCredentialProviderHandler(CredentialProviderConfig{Issuer: authority.URL, Bindings: []CredentialProviderBinding{{ConnectionID: "connection", WorkspaceID: "workspace", Revision: 1, OperationKey: "expense-statements.read", TokenFile: tokenFile, APIKey: &binding}}}, authority.Client(), rewriteClient(provider))
	if err != nil {
		t.Fatal(err)
	}
	_, err = handler.Execute(context.Background(), handlers.Request{ConnectionID: "connection", WorkspaceID: "workspace", CredentialRevision: 1, OperationKey: "expense-statements.read"})
	var failure *handlers.Error
	if !errors.As(err, &failure) || failure.Class != "connection_error" || providerCalls.Load() != 0 {
		t.Fatalf("denied execution result=%v provider=%d", err, providerCalls.Load())
	}
}
