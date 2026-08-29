package connections

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	_ "github.com/ncruces/go-sqlite3/driver"
	"golang.org/x/oauth2"
)

func TestOAuthCallbackUsesPKCEConsumesStateAndPersistsEncryptedToken(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	store, err := NewStore(db, []byte("01234567890123456789012345678901"))
	if err != nil {
		t.Fatal(err)
	}

	var gotVerifier string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/token" {
			http.NotFound(w, r)
			return
		}
		_ = r.ParseForm()
		gotVerifier = r.Form.Get("code_verifier")
		if gotVerifier == "" || r.Form.Get("code") != "one-time-code" {
			http.Error(w, "missing PKCE", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"access_token":  "access-secret",
			"refresh_token": "refresh-secret",
			"token_type":    "Bearer",
			"expires_in":    3600,
		})
	}))
	defer server.Close()

	cfg := oauth2.Config{ClientID: "client", ClientSecret: "secret", Endpoint: oauth2.Endpoint{AuthURL: server.URL + "/authorize", TokenURL: server.URL + "/token"}, RedirectURL: "https://conductor.example.test/oauth/callback", Scopes: []string{"records.read"}}
	now := time.Unix(1_700_000_000, 0).UTC()
	begin, err := store.Begin(context.Background(), BeginInput{AttemptID: "attempt-1", Provider: "example", ConnectionID: "conn-1", WorkspaceID: "ws-1", RedirectURL: cfg.RedirectURL, ClientID: cfg.ClientID, Scopes: cfg.Scopes, AuthURLParams: map[string]string{"prompt": "consent", "state": "attacker", "code_challenge": "attacker"}, Now: now}, cfg.Endpoint)
	if err != nil {
		t.Fatal(err)
	}
	authURL, err := url.Parse(begin.AuthURL)
	if err != nil {
		t.Fatal(err)
	}
	if authURL.Query().Get("code_challenge_method") != "S256" || authURL.Query().Get("state") != begin.State || authURL.Query().Get("prompt") != "consent" || authURL.Query().Get("code_challenge") == "attacker" {
		t.Fatalf("auth URL missing state/PKCE: %s", begin.AuthURL)
	}

	token, err := store.Complete(context.Background(), cfg, CompleteInput{AttemptID: begin.AttemptID, State: begin.State, Code: "one-time-code", WorkspaceID: "ws-1", Metadata: map[string]string{"api_base_url": "https://api.example.test"}, Now: now.Add(time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	if token.AccessToken != "access-secret" || gotVerifier == "" {
		t.Fatalf("unexpected exchange result: %#v verifier=%q", token, gotVerifier)
	}
	if _, err := store.LoadToken(context.Background(), "conn-1"); err != nil {
		t.Fatal(err)
	}
	connection, err := store.LoadConnection(context.Background(), "conn-1")
	if err != nil || connection.Metadata["api_base_url"] != "https://api.example.test" || connection.Metadata["client_id"] != "client" {
		t.Fatalf("metadata load failed: %+v err=%v", connection, err)
	}
	var storedMetadata string
	if err := db.QueryRowContext(context.Background(), "SELECT metadata_json FROM connections WHERE connection_id='conn-1'").Scan(&storedMetadata); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(storedMetadata, "api.example.test") {
		t.Fatal("credential metadata stored in plaintext")
	}
	if _, err := store.Complete(context.Background(), cfg, CompleteInput{AttemptID: begin.AttemptID, State: begin.State, Code: "one-time-code", WorkspaceID: "ws-1", Now: now.Add(2 * time.Second)}); err != ErrInvalidState {
		t.Fatalf("expected one-time state rejection, got %v", err)
	}
}

func TestOAuthCallbackCanResolveAttemptFromState(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	store, err := NewStore(db, []byte("01234567890123456789012345678901"))
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"access_token":"access"}`))
	}))
	defer server.Close()
	cfg := oauth2.Config{ClientID: "client", Endpoint: oauth2.Endpoint{AuthURL: server.URL + "/authorize", TokenURL: server.URL + "/token"}, RedirectURL: "https://conductor.example.test/v1/oauth/example/callback"}
	now := time.Unix(1_700_000_000, 0).UTC()
	begin, err := store.Begin(context.Background(), BeginInput{AttemptID: "attempt-state", Provider: "example", ConnectionID: "conn-state", WorkspaceID: "ws-state", RedirectURL: cfg.RedirectURL, ClientID: cfg.ClientID, Now: now}, cfg.Endpoint)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Complete(context.Background(), cfg, CompleteInput{State: begin.State, Code: "code", Now: now.Add(time.Second)}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.LoadConnection(context.Background(), "conn-state"); err != nil {
		t.Fatal(err)
	}
}

func TestOAuthStateRejectsMismatchWithoutConsumingAttempt(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	store, err := NewStore(db, []byte("01234567890123456789012345678901"))
	if err != nil {
		t.Fatal(err)
	}
	cfg := oauth2.Config{ClientID: "client", Endpoint: oauth2.Endpoint{AuthURL: "https://auth.example.test", TokenURL: "https://token.example.test"}, RedirectURL: "https://conductor.example.test/callback"}
	begin, err := store.Begin(context.Background(), BeginInput{AttemptID: "attempt-2", Provider: "example", ConnectionID: "conn-2", WorkspaceID: "ws", RedirectURL: cfg.RedirectURL, ClientID: cfg.ClientID, Now: time.Unix(1_700_000_000, 0)}, cfg.Endpoint)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Complete(context.Background(), cfg, CompleteInput{AttemptID: begin.AttemptID, State: strings.Repeat("x", len(begin.State)), Code: "code", WorkspaceID: "ws", Now: time.Unix(1_700_000_001, 0)}); err != ErrInvalidState {
		t.Fatalf("expected invalid state, got %v", err)
	}
}

func TestOAuthProviderMismatchRejectsWithoutExchange(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	store, err := NewStore(db, []byte("01234567890123456789012345678901"))
	if err != nil {
		t.Fatal(err)
	}
	called := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"access_token":"access"}`))
	}))
	defer server.Close()
	cfg := oauth2.Config{ClientID: "client", Endpoint: oauth2.Endpoint{AuthURL: server.URL + "/authorize", TokenURL: server.URL + "/token"}, RedirectURL: "https://conductor.example.test/callback"}
	now := time.Unix(1_700_000_000, 0).UTC()
	begin, err := store.Begin(context.Background(), BeginInput{AttemptID: "attempt-provider", Provider: "example", ConnectionID: "conn-provider", WorkspaceID: "ws-provider", RedirectURL: cfg.RedirectURL, ClientID: cfg.ClientID, Now: now}, cfg.Endpoint)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Complete(context.Background(), cfg, CompleteInput{AttemptID: begin.AttemptID, Provider: "other", State: begin.State, Code: "code", WorkspaceID: "ws-provider", Now: now.Add(time.Second)}); err != ErrInvalidState {
		t.Fatalf("expected provider mismatch rejection, got %v", err)
	}
	if called {
		t.Fatal("provider mismatch must not exchange the authorization code")
	}
}

func TestSaveConnectionEncryptsNonOAuthCredentials(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	store, err := NewStore(db, []byte("01234567890123456789012345678901"))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.SaveConnection(context.Background(), "conn-api", "ws-api", "slack", nil, nil, map[string]string{"api_key": "secret-api-key", "module": "web"}, time.Unix(1_700_000_000, 0)); err != nil {
		t.Fatal(err)
	}
	connection, err := store.LoadConnection(context.Background(), "conn-api")
	if err != nil || connection.Token != nil || connection.Metadata["api_key"] != "secret-api-key" {
		t.Fatalf("unexpected connection=%+v err=%v", connection, err)
	}
	var storedMetadata string
	if err := db.QueryRowContext(context.Background(), "SELECT metadata_json FROM connections WHERE connection_id='conn-api'").Scan(&storedMetadata); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(storedMetadata, "secret-api-key") {
		t.Fatal("API key stored in plaintext")
	}
}
