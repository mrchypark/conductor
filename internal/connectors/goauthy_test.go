package connectors

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/amp-labs/connectors/providers"
	"github.com/mrchypark/conductor/internal/handlers"
)

func TestGoauthyClientExchangeAndErrors(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/auth/v1/connection-grants/grant-ok/credential":
			_ = json.NewEncoder(w).Encode(DeliveredCredential{
				Kind:            "oauth2",
				AccessToken:     "secret-tok",
				TokenType:       "Bearer",
				ProviderID:      "hubspot",
				ExpiresAtUnixMS: time.Now().Add(time.Minute).UnixMilli(),
			})
		case "/auth/v1/connection-grants/grant-revoked/credential":
			w.WriteHeader(http.StatusGone)
		case "/auth/v1/connection-grants/grant-down/credential":
			w.WriteHeader(http.StatusServiceUnavailable)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	client := &GoauthyClient{BaseURL: srv.URL, Client: srv.Client()}

	cred, err := client.Exchange(context.Background(), "grant-ok", "hubspot", time.Now().Add(time.Minute))
	if err != nil || cred.AccessToken != "secret-tok" {
		t.Fatalf("unexpected exchange result: %#v err=%v", cred, err)
	}

	if _, err := client.Exchange(context.Background(), "grant-ok", "salesforce", time.Now().Add(time.Minute)); err == nil {
		t.Fatal("expected provider mismatch error")
	}

	if _, err := client.Exchange(context.Background(), "grant-ok", "hubspot", time.Now().Add(-time.Second)); err == nil {
		t.Fatal("expected expired error")
	}

	_, err = client.Exchange(context.Background(), "grant-revoked", "hubspot", time.Now().Add(time.Minute))
	var hErr *handlers.Error
	if !errors.As(err, &hErr) || hErr.Class != "auth_expired" || hErr.Retryable {
		t.Fatalf("expected non-retryable auth_expired, got %v", err)
	}

	_, err = client.Exchange(context.Background(), "grant-down", "hubspot", time.Now().Add(time.Minute))
	if !errors.As(err, &hErr) || !hErr.Retryable {
		t.Fatalf("expected retryable error on 503, got %v", err)
	}
}

func TestCustomConnectorSSRFGuards(t *testing.T) {
	cat, err := LoadCatalog()
	if err != nil {
		t.Fatal(err)
	}
	if err := cat.RegisterCustom("ws-1", "bad-http", providers.ProviderInfo{
		BaseURL:  "http://api.example.com",
		AuthType: providers.ApiKey,
	}, false); err == nil {
		t.Fatal("expected https enforcement error")
	}
	if err := cat.RegisterCustom("ws-1", "good-https", providers.ProviderInfo{
		BaseURL:  "https://api.example.com",
		AuthType: providers.ApiKey,
		ApiKeyOpts: &providers.ApiKeyOpts{
			AttachmentType: providers.Header,
			Header:         &providers.ApiKeyOptsHeader{Name: "Authorization"},
		},
	}, false); err != nil {
		t.Fatalf("unexpected error registering custom connector: %v", err)
	}
	if _, err := cat.InfoForWorkspace("ws-2", "good-https"); err == nil {
		t.Fatal("ws-2 should not resolve ws-1 custom connector")
	}

	localSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer localSrv.Close()

	ssrfClient := NewSSRFSafeHTTPClient(nil, false)
	if _, err := ssrfClient.Get(localSrv.URL); err == nil {
		t.Fatal("expected SSRF dialer to block loopback 127.0.0.1")
	}
}
