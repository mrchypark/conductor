package connections

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"golang.org/x/oauth2"
)

func TestOAuthCallbacksKeepProviderProfilesIsolated(t *testing.T) {
	store, _ := newOAuthStore(t)
	var exchanges int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/token" {
			http.NotFound(w, r)
			return
		}
		exchanges++
		if err := r.ParseForm(); err != nil {
			t.Fatal(err)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"access_token": r.Form.Get("code")})
	}))
	defer server.Close()

	cfg := oauth2.Config{ClientID: "client", Endpoint: oauth2.Endpoint{AuthURL: server.URL + "/authorize", TokenURL: server.URL + "/token"}, RedirectURL: "https://conductor.example.test/callback"}
	now := time.Unix(1_700_000_000, 0).UTC()
	beginA, err := store.Begin(context.Background(), BeginInput{AttemptID: "github-profile-a", Provider: "github", ConnectionID: "github-a", WorkspaceID: "workspace", RedirectURL: cfg.RedirectURL, ClientID: cfg.ClientID, Now: now}, cfg.Endpoint)
	if err != nil {
		t.Fatal(err)
	}
	beginB, err := store.Begin(context.Background(), BeginInput{AttemptID: "github-profile-b", Provider: "github", ConnectionID: "github-b", WorkspaceID: "workspace", RedirectURL: cfg.RedirectURL, ClientID: cfg.ClientID, Now: now}, cfg.Endpoint)
	if err != nil {
		t.Fatal(err)
	}
	if beginA.AttemptID == beginB.AttemptID || beginA.State == beginB.State {
		t.Fatal("same-provider profiles must have distinct attempts and states")
	}

	if _, err := store.Complete(context.Background(), cfg, CompleteInput{AttemptID: beginA.AttemptID, State: beginB.State, Code: "wrong", WorkspaceID: "workspace", Now: now.Add(time.Second)}); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("mismatched attempt and state: got %v, want %v", err, ErrInvalidState)
	}
	if exchanges != 0 {
		t.Fatal("mismatched attempt and state reached the token endpoint")
	}

	if _, err := store.Complete(context.Background(), cfg, CompleteInput{AttemptID: beginA.AttemptID, State: beginA.State, Code: "profile-a-token", WorkspaceID: "workspace", Now: now.Add(time.Second)}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Complete(context.Background(), cfg, CompleteInput{AttemptID: beginB.AttemptID, State: beginB.State, Code: "profile-b-token", WorkspaceID: "workspace", Now: now.Add(time.Second)}); err != nil {
		t.Fatal(err)
	}
	if exchanges != 2 {
		t.Fatalf("token exchanges = %d, want 2", exchanges)
	}

	tokenA, err := store.LoadToken(context.Background(), "github-a")
	if err != nil {
		t.Fatal(err)
	}
	tokenB, err := store.LoadToken(context.Background(), "github-b")
	if err != nil {
		t.Fatal(err)
	}
	if tokenA.AccessToken != "profile-a-token" || tokenB.AccessToken != "profile-b-token" {
		t.Fatal("completed OAuth states did not remain bound to their connections")
	}
}

func TestOAuthCallbackRejectsExpiredProfileStateBeforeExchange(t *testing.T) {
	store, _ := newOAuthStore(t)
	exchanged := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		exchanged = true
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"access_token": "unexpected"})
	}))
	defer server.Close()

	cfg := oauth2.Config{ClientID: "client", Endpoint: oauth2.Endpoint{AuthURL: server.URL + "/authorize", TokenURL: server.URL + "/token"}, RedirectURL: "https://conductor.example.test/callback"}
	now := time.Unix(1_700_000_000, 0).UTC()
	begin, err := store.Begin(context.Background(), BeginInput{AttemptID: "github-expired-profile", Provider: "github", ConnectionID: "github-expired", WorkspaceID: "workspace", RedirectURL: cfg.RedirectURL, ClientID: cfg.ClientID, TTL: time.Second, Now: now}, cfg.Endpoint)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Complete(context.Background(), cfg, CompleteInput{AttemptID: begin.AttemptID, State: begin.State, Code: "expired", WorkspaceID: "workspace", Now: now.Add(time.Second)}); !errors.Is(err, ErrExpiredState) {
		t.Fatalf("expired state: got %v, want %v", err, ErrExpiredState)
	}
	if exchanged {
		t.Fatal("expired state reached the token endpoint")
	}
	if _, err := store.LoadConnection(context.Background(), "github-expired"); err == nil {
		t.Fatal("expired state saved a connection")
	}
}
