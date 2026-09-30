package connectors

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/amp-labs/connectors/generic"
)

const oauthDeliverySecret = "oauth-delivery-secret-must-not-escape"

func TestOAuthDeliveryFeedsExistingGenericRawRequest(t *testing.T) {
	now := time.Date(2026, 9, 8, 0, 0, 0, 0, time.UTC)
	binding := oauthBinding(now)
	var authorityCalls, providerCalls atomic.Int64
	authority := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authorityCalls.Add(1)
		if r.Method != http.MethodPost || r.URL.Path != "/tenant/auth/v1/connection-grants/grant/credential" {
			t.Errorf("authority request = %s %s", r.Method, r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer trusted-human-token" {
			t.Errorf("authority auth = %q", got)
		}
		if body, _ := io.ReadAll(r.Body); len(body) != 0 {
			t.Errorf("authority body = %q", body)
		}
		writeOAuthDelivery(w, binding, oauthDeliverySecret, binding.CredentialVersion)
	}))
	defer authority.Close()
	provider := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		providerCalls.Add(1)
		if r.Method != http.MethodGet || r.URL.Path != "/user" || r.URL.RawQuery != "" {
			t.Errorf("provider request = %s %s", r.Method, r.URL.RequestURI())
		}
		if got := r.Header.Get("Authorization"); got != "Bearer "+oauthDeliverySecret {
			t.Errorf("provider auth = %q", got)
		}
		_, _ = w.Write([]byte(`{"id":1,"login":"fixture"}`))
	}))
	defer provider.Close()
	client, err := NewOAuthDeliveryClient(authority.URL+"/tenant", binding, func(_ context.Context, got OAuthTokenRequest) (string, error) {
		if got != (OAuthTokenRequest{OwnerSubject: binding.OwnerSubject, Resource: binding.Resource, ConsumerClientID: binding.ConsumerClientID}) {
			t.Errorf("source request = %#v", got)
		}
		return "trusted-human-token", nil
	}, authority.Client(), rewriteGitHubClient(provider), func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	conn, err := generic.NewConnector("github", generic.WithAuthenticatedClient(client))
	if err != nil {
		t.Fatal(err)
	}
	result, err := rawRequest(context.Background(), conn, invokeInput{Method: http.MethodGet, Path: "/user"})
	if err != nil {
		t.Fatal(err)
	}
	if string(result.Output) != `{"id":1,"login":"fixture"}` {
		t.Fatalf("output = %s", result.Output)
	}
	if authorityCalls.Load() != 1 || providerCalls.Load() != 1 {
		t.Fatalf("calls authority=%d provider=%d", authorityCalls.Load(), providerCalls.Load())
	}
}

func TestOAuthDeliveryVersionRefreshAndRevocationHaveNoFallback(t *testing.T) {
	now := time.Date(2026, 9, 8, 0, 0, 0, 0, time.UTC)
	binding := oauthBinding(now)
	binding.CredentialVersion = 9007199254740993
	var calls, providerCalls atomic.Int64
	authority := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch calls.Add(1) {
		case 1:
			writeOAuthDelivery(w, binding, oauthDeliverySecret+"-v1", binding.CredentialVersion)
		case 2:
			writeOAuthDelivery(w, binding, oauthDeliverySecret+"-v2", binding.CredentialVersion+1)
		default:
			http.Error(w, "revoked", http.StatusNotFound)
		}
	}))
	defer authority.Close()
	provider := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		providerCalls.Add(1)
		if got := r.Header.Get("Authorization"); got != "Bearer "+oauthDeliverySecret+"-v"+string(rune('0'+providerCalls.Load())) {
			t.Errorf("provider auth=%q", got)
		}
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer provider.Close()
	client, err := NewOAuthDeliveryClient(authority.URL, binding, oauthSource, authority.Client(), rewriteGitHubClient(provider), func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if _, err := oauthGet(client); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := oauthGet(client); err == nil {
		t.Fatal("request succeeded after revoke")
	}
	if providerCalls.Load() != 2 {
		t.Fatalf("provider calls=%d; want 2", providerCalls.Load())
	}
}

func TestOAuthDeliveryRejectsInvalidCredentialsBeforeProvider(t *testing.T) {
	now := time.Date(2026, 9, 8, 0, 0, 0, 0, time.UTC)
	cases := []struct {
		name   string
		mutate func(map[string]any)
	}{
		{"wrong-kind", func(m map[string]any) { m["kind"] = "api_key" }}, {"status-field", func(m map[string]any) { m["status"] = "ready" }},
		{"wrong-account", func(m map[string]any) { m["account_id"] = "other" }}, {"wrong-generation", func(m map[string]any) { m["connection_generation"] = "other" }},
		{"wrong-scopes", func(m map[string]any) { m["scopes"] = []string{"other"} }}, {"downgrade", func(m map[string]any) { m["credential_version"] = int64(6) }},
		{"expired-token", func(m map[string]any) { m["token_expires_at_unix_ms"] = now.UnixMilli() }}, {"missing", func(m map[string]any) { delete(m, "access_token") }},
		{"unknown", func(m map[string]any) { m["extra"] = true }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			binding := oauthBinding(now)
			var providerCalls atomic.Int64
			authority := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				m := oauthDeliveryMap(binding, oauthDeliverySecret, binding.CredentialVersion)
				tc.mutate(m)
				writeJSON(w, m)
			}))
			defer authority.Close()
			client, err := NewOAuthDeliveryClient(authority.URL, binding, oauthSource, authority.Client(), noNetworkClient(&providerCalls), func() time.Time { return now })
			if err != nil {
				t.Fatal(err)
			}
			if _, err = oauthGet(client); err == nil {
				t.Fatal("request succeeded")
			}
			if providerCalls.Load() != 0 {
				t.Fatalf("provider calls=%d", providerCalls.Load())
			}
		})
	}
}

func TestOAuthDeliveryRejectsRedirectsExpiryAndSecretLeaks(t *testing.T) {
	now := time.Date(2026, 9, 8, 0, 0, 0, 0, time.UTC)
	binding := oauthBinding(now)
	var sourceCalls, providerCalls atomic.Int64
	authority := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, "/redirect", http.StatusFound) }))
	defer authority.Close()
	client, err := NewOAuthDeliveryClient(authority.URL, binding, func(context.Context, OAuthTokenRequest) (string, error) {
		sourceCalls.Add(1)
		return "human-" + oauthDeliverySecret, nil
	}, authority.Client(), noNetworkClient(&providerCalls), func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	if _, err = oauthGet(client); err == nil || strings.Contains(err.Error(), oauthDeliverySecret) {
		t.Fatalf("redirect err=%v", err)
	}
	if sourceCalls.Load() != 1 || providerCalls.Load() != 0 {
		t.Fatalf("calls source=%d provider=%d", sourceCalls.Load(), providerCalls.Load())
	}
	for _, mutate := range []func(*OAuthDeliveryBinding){func(b *OAuthDeliveryBinding) { b.Endpoint = "https://api.github.com/user?x=1" }, func(b *OAuthDeliveryBinding) { b.ConsentExpiresAtUnixMS = now.UnixMilli() }} {
		b := binding
		mutate(&b)
		if _, err := NewOAuthDeliveryClient(authority.URL, b, oauthSource, authority.Client(), http.DefaultClient, func() time.Time { return now }); err == nil {
			t.Fatal("invalid binding accepted")
		}
	}
}

func TestOAuthDeliveryRejectsMalformedRequestsAndProviderLeaks(t *testing.T) {
	now := time.Date(2026, 9, 8, 0, 0, 0, 0, time.UTC)
	binding := oauthBinding(now)
	for _, body := range []string{
		`{`,
		`{"kind":"oauth2","kind":"oauth2"}`,
		`{"kind":null}`,
		`{"kind":"oauth2","extra":true}`,
	} {
		if _, err := decodeDeliveredOAuth([]byte(body)); err == nil {
			t.Fatalf("malformed credential accepted: %s", body)
		}
	}
	authority := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeOAuthDelivery(w, binding, oauthDeliverySecret, binding.CredentialVersion)
	}))
	defer authority.Close()
	provider := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/redirect", http.StatusFound)
	}))
	defer provider.Close()
	client, err := NewOAuthDeliveryClient(authority.URL, binding, oauthSource, authority.Client(), rewriteGitHubClient(provider), func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	if _, err = oauthGet(client); err == nil || strings.Contains(err.Error(), oauthDeliverySecret) {
		t.Fatalf("provider redirect err=%v", err)
	}
	for _, request := range []*http.Request{
		mustRequest(t, http.MethodPost, binding.Endpoint, nil),
		mustRequest(t, http.MethodGet, binding.Endpoint, strings.NewReader(`{}`)),
		requestWithHeader(t, binding.Endpoint, "Authorization", "Bearer caller-token"),
	} {
		if _, err := client.Do(request); err == nil {
			t.Fatal("untrusted request accepted")
		}
	}
	failed, err := NewOAuthDeliveryClient(authority.URL, binding, func(context.Context, OAuthTokenRequest) (string, error) {
		return "", errors.New("source " + oauthDeliverySecret)
	}, authority.Client(), rewriteGitHubClient(provider), func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	if _, err = oauthGet(failed); err == nil || strings.Contains(err.Error(), oauthDeliverySecret) {
		t.Fatalf("source err=%v", err)
	}
}

func oauthBinding(now time.Time) OAuthDeliveryBinding {
	return OAuthDeliveryBinding{GrantID: "grant", ProviderID: "github", AccountID: "account", ConnectionGeneration: "generation", Resource: "https://conductor.example/connections", ConsumerClientID: "conductor", OwnerSubject: "owner", Endpoint: "https://api.github.com/user", Scopes: []string{"read:user", "user:email"}, CredentialVersion: 7, ConsentExpiresAtUnixMS: now.Add(time.Hour).UnixMilli()}
}
func oauthSource(context.Context, OAuthTokenRequest) (string, error) {
	return "trusted-human-token", nil
}
func oauthDeliveryMap(b OAuthDeliveryBinding, token string, version int64) map[string]any {
	return map[string]any{"kind": "oauth2", "access_token": token, "token_type": "Bearer", "grant_id": b.GrantID, "provider_id": b.ProviderID, "connection_generation": b.ConnectionGeneration, "credential_version": version, "account_id": b.AccountID, "scopes": b.Scopes, "token_expires_at_unix_ms": b.ConsentExpiresAtUnixMS - 1, "consent_expires_at_unix_ms": b.ConsentExpiresAtUnixMS}
}
func writeOAuthDelivery(w http.ResponseWriter, b OAuthDeliveryBinding, token string, version int64) {
	writeJSON(w, oauthDeliveryMap(b, token, version))
}
func oauthGet(client *http.Client) (*http.Response, error) {
	return client.Get("https://api.github.com/user")
}
func mustRequest(t *testing.T, method, endpoint string, body io.Reader) *http.Request {
	t.Helper()
	r, err := http.NewRequest(method, endpoint, body)
	if err != nil {
		t.Fatal(err)
	}
	return r
}
func requestWithHeader(t *testing.T, endpoint, name, value string) *http.Request {
	r := mustRequest(t, http.MethodGet, endpoint, nil)
	r.Header.Set(name, value)
	return r
}
func rewriteGitHubClient(server *httptest.Server) *http.Client {
	target, _ := url.Parse(server.URL)
	c := server.Client()
	copy := *c
	copy.Transport = githubRewriteTransport{base: c.Transport, target: target}
	return &copy
}

type githubRewriteTransport struct {
	base   http.RoundTripper
	target *url.URL
}

func (t githubRewriteTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	if r.URL.Scheme != "https" || r.URL.Host != "api.github.com" {
		return nil, errors.New("unexpected provider origin")
	}
	clone := r.Clone(r.Context())
	u := *r.URL
	u.Scheme, u.Host = t.target.Scheme, t.target.Host
	clone.URL = &u
	clone.Host = ""
	return t.base.RoundTrip(clone)
}
