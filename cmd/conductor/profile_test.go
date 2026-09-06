package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mrchypark/conductor/internal/connections"
	ampconnectors "github.com/mrchypark/conductor/internal/connectors"
	"github.com/mrchypark/conductor/internal/handlers"
	"github.com/mrchypark/conductor/internal/storage"
	"golang.org/x/oauth2"
)

type profileTransport func(*http.Request) (*http.Response, error)

func (f profileTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestSelectedConnectionReachesOnlyProvider(t *testing.T) {
	ctx := context.Background()
	store, err := storage.Open(ctx, "file:"+t.TempDir()+"/db")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	profiles, err := connections.NewStore(ctx, store.Persistence(), []byte("01234567890123456789012345678901"))
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"personal", "work"} {
		if err := profiles.SaveToken(ctx, id, "ws", "github", &oauth2.Token{AccessToken: id, TokenType: "Bearer"}, nil, nil, time.Now()); err != nil {
			t.Fatal(err)
		}
	}
	var calls atomic.Int32
	provider := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.Method != "GET" || r.URL.Path != "/user" {
			t.Error("unexpected provider request")
			http.Error(w, "invalid", 400)
			return
		}
		id := r.Header.Get("Authorization")
		if id == "Bearer denied" {
			http.Error(w, "insufficient provider scope", http.StatusForbidden)
			return
		}
		if id != "Bearer personal" && id != "Bearer work" {
			t.Error("unexpected profile credential")
			http.Error(w, "invalid", 401)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"login": id[len("Bearer "):]})
	}))
	defer provider.Close()
	target, _ := url.Parse(provider.URL)
	client := provider.Client()
	base := client.Transport
	client.Transport = profileTransport(func(r *http.Request) (*http.Response, error) {
		if r.URL.Scheme != "https" || r.URL.Host != "api.github.com" {
			return nil, errors.New("unexpected provider destination")
		}
		copy := r.Clone(r.Context())
		u := *r.URL
		u.Scheme, u.Host = target.Scheme, target.Host
		copy.URL = &u
		copy.Host = target.Host
		return base.RoundTrip(copy)
	})
	catalog, err := ampconnectors.LoadCatalog()
	if err != nil {
		t.Fatal(err)
	}
	h, err := catalog.NewInvokeHandlerScoped("1", client, connectorCredentialsResolver(profiles, catalog))
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"personal", "work"} {
		r := handlers.Request{WorkspaceID: "ws", ConnectionID: id, CredentialRevision: 1, OperationKey: "http", Input: json.RawMessage(`{"provider":"github","method":"GET","path":"/user"}`)}
		out, err := h.Execute(ctx, r)
		if err != nil {
			t.Fatal(err)
		}
		var got struct {
			Login string `json:"login"`
		}
		if err := json.Unmarshal(out.Output, &got); err != nil || got.Login != id {
			t.Fatal("selected profile result mismatch")
		}
		r.WorkspaceID = "other"
		if _, err := h.Execute(ctx, r); err == nil {
			t.Fatal("other workspace accepted")
		}
	}
	if calls.Load() != 2 {
		t.Fatal("rejected request reached provider")
	}
	r := handlers.Request{WorkspaceID: "ws", ConnectionID: "personal", CredentialRevision: 1, OperationKey: "http", Input: json.RawMessage(`{"provider":"github","method":"GET","path":"/user"}`)}
	if err := profiles.SaveToken(ctx, "personal", "ws", "github", &oauth2.Token{AccessToken: "personal", TokenType: "Bearer"}, nil, nil, time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := h.Execute(ctx, r); err == nil {
		t.Fatal("old generation accepted after explicit replacement")
	}
	if calls.Load() != 2 {
		t.Fatal("stale generation reached provider")
	}
	r.CredentialRevision = 2
	if _, err := h.Execute(ctx, r); err != nil {
		t.Fatal(err)
	}
	if err := profiles.RevokeConnection(ctx, "personal"); err != nil {
		t.Fatal(err)
	}
	if _, err := h.Execute(ctx, r); err == nil {
		t.Fatal("revoked profile accepted")
	}
	if calls.Load() != 3 {
		t.Fatal("revoked profile reached provider")
	}
	work, err := profiles.LoadConnection(ctx, "work")
	if err != nil {
		t.Fatal(err)
	}
	if err := profiles.SaveTokenAtRevision(ctx, "work", "ws", "github", &oauth2.Token{AccessToken: "work", TokenType: "Bearer"}, nil, nil, time.Now(), work.Revision, work.TokenVersion); err != nil {
		t.Fatal(err)
	}
	r.ConnectionID, r.CredentialRevision = "work", 1
	if _, err := h.Execute(ctx, r); err != nil {
		t.Fatal("verified refresh invalidated pinned generation")
	}
	if err := profiles.SaveToken(ctx, "work", "ws", "github", &oauth2.Token{AccessToken: "work", Expiry: time.Now().Add(-time.Hour)}, nil, nil, time.Now()); err != nil {
		t.Fatal(err)
	}
	r.CredentialRevision = 2
	if _, err := h.Execute(ctx, r); err == nil {
		t.Fatal("expired pinned token accepted without continuity verification")
	}
	if calls.Load() != 4 {
		t.Fatal("expired pinned token reached provider")
	}
	if err := profiles.SaveToken(ctx, "denied", "ws", "github", &oauth2.Token{AccessToken: "denied", TokenType: "Bearer"}, nil, nil, time.Now()); err != nil {
		t.Fatal(err)
	}
	r.ConnectionID, r.CredentialRevision = "denied", 1
	if _, err := h.Execute(ctx, r); err == nil {
		t.Fatal("provider scope denial returned success")
	}
}
