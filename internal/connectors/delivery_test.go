package connectors

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

const deliveryKey = "delivery-key-must-not-escape"

func TestAPIKeyDeliveryInvokeFetchesFreshCredential(t *testing.T) {
	var authorityCalls, providerCalls, sourceCalls atomic.Int64
	binding := deliveryBinding()
	binding.GrantID = "grant/one"
	authority := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authorityCalls.Add(1)
		if r.Method != http.MethodPost || r.URL.Path != "/auth/v1/connection-grants/grant/one/credential" || !strings.Contains(r.URL.EscapedPath(), "grant%2Fone") {
			t.Errorf("authority request = %s %s", r.Method, r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer consumer-token" {
			t.Errorf("authority authorization = %q", got)
		}
		if body, _ := io.ReadAll(r.Body); len(body) != 0 {
			t.Errorf("authority body = %q", body)
		}
		writeDelivery(t, w, binding, deliveryKey)
	}))
	defer authority.Close()
	provider := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		providerCalls.Add(1)
		if r.Method != http.MethodGet || r.URL.Path != "/v2/expense-statements" || r.URL.RawQuery != "page=1" {
			t.Errorf("provider request = %s %s", r.Method, r.URL.RequestURI())
		}
		if got := r.Header.Get("Authorization"); got != deliveryKey {
			t.Errorf("provider authorization = %q", got)
		}
		_, _ = w.Write([]byte(`{"items":[{"id":1}]}`))
	}))
	defer provider.Close()

	delivery, err := NewAPIKeyDelivery(authority.URL, binding, func(context.Context) (string, error) {
		sourceCalls.Add(1)
		return "consumer-token", nil
	}, authority.Client(), rewriteClient(provider))
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		got, err := delivery.Invoke(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != `{"items":[{"id":1}]}` {
			t.Fatalf("result = %s", got)
		}
	}
	if authorityCalls.Load() != 2 || providerCalls.Load() != 2 || sourceCalls.Load() != 2 {
		t.Fatalf("calls authority=%d provider=%d source=%d; want 2 each", authorityCalls.Load(), providerCalls.Load(), sourceCalls.Load())
	}
}

func TestAPIKeyDeliveryRejectsUntrustedDelivery(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(DeliveryBinding) DeliveryBinding
	}{
		{"wrong-provider-url", func(b DeliveryBinding) DeliveryBinding { b.URL = "https://example.com/v2/expense-statements"; return b }},
		{"wrong-path", func(b DeliveryBinding) DeliveryBinding { b.URL = "https://openapi.gowid.com/v2/expenses"; return b }},
		{"wrong-header", func(b DeliveryBinding) DeliveryBinding { b.Header = "X-Key"; return b }},
		{"wrong-prefix", func(b DeliveryBinding) DeliveryBinding { b.Prefix = "Bearer "; return b }},
		{"non-positive-version", func(b DeliveryBinding) DeliveryBinding { b.CredentialVersion = 0; return b }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewAPIKeyDelivery("https://issuer.example", tt.mutate(deliveryBinding()), func(context.Context) (string, error) { return "token", nil }, http.DefaultClient, http.DefaultClient)
			if err == nil {
				t.Fatal("NewAPIKeyDelivery succeeded")
			}
		})
	}
}

func TestAPIKeyDeliverySanitizesAuthorityAndProviderFailures(t *testing.T) {
	tests := []struct {
		name          string
		authorityCode int
		providerCode  int
		providerBody  string
	}{
		{"revoked", http.StatusNotFound, 0, ""},
		{"wrong-consumer", http.StatusUnauthorized, 0, ""},
		{"provider-failure", http.StatusOK, http.StatusBadGateway, "upstream " + deliveryKey},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			binding := deliveryBinding()
			authority := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if tt.authorityCode != http.StatusOK {
					http.Error(w, "authority "+deliveryKey, tt.authorityCode)
					return
				}
				writeDelivery(t, w, binding, deliveryKey)
			}))
			defer authority.Close()
			provider := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				http.Error(w, tt.providerBody, tt.providerCode)
			}))
			defer provider.Close()
			delivery, err := NewAPIKeyDelivery(authority.URL, binding, func(context.Context) (string, error) { return "consumer-token", nil }, authority.Client(), rewriteClient(provider))
			if err != nil {
				t.Fatal(err)
			}
			_, err = delivery.Invoke(context.Background())
			if err == nil {
				t.Fatal("Invoke succeeded")
			}
			if strings.Contains(err.Error(), deliveryKey) {
				t.Fatalf("secret leaked in error: %v", err)
			}
		})
	}
}

func TestAPIKeyDeliveryRejectsMismatchedOrSecretResponse(t *testing.T) {
	tests := []struct {
		name          string
		mutate        func(map[string]any)
		output        string
		callsProvider bool
	}{
		{"wrong-generation", func(m map[string]any) { m["connection_generation"] = "other" }, `{"items":[]}`, false},
		{"wrong-digest", func(m map[string]any) { m["connector_digest"] = "sha256:other" }, `{"items":[]}`, false},
		{"wrong-kind", func(m map[string]any) { m["kind"] = "oauth" }, `{"items":[]}`, false},
		{"wrong-grant", func(m map[string]any) { m["grant_id"] = "other" }, `{"items":[]}`, false},
		{"wrong-header", func(m map[string]any) { m["header"] = "X-Key" }, `{"items":[]}`, false},
		{"wrong-prefix", func(m map[string]any) { m["prefix"] = "Bearer " }, `{"items":[]}`, false},
		{"missing-prefix", func(m map[string]any) { delete(m, "prefix") }, `{"items":[]}`, false},
		{"unsafe-integer-version", func(m map[string]any) { m["credential_version"] = json.Number("9007199254740993") }, `{"items":[]}`, false},
		{"expired", func(m map[string]any) { m["consent_expires_at_unix_ms"] = time.Now().Add(-time.Minute).UnixMilli() }, `{"items":[]}`, false},
		{"response-contains-key", func(m map[string]any) {}, `{"nested":{"api_key":"` + deliveryKey + `"}}`, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			binding := deliveryBinding()
			var providerCalls atomic.Int64
			authority := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				payload := deliveryMap(binding, deliveryKey)
				tt.mutate(payload)
				writeJSON(w, payload)
			}))
			defer authority.Close()
			provider := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				providerCalls.Add(1)
				_, _ = w.Write([]byte(tt.output))
			}))
			defer provider.Close()
			delivery, err := NewAPIKeyDelivery(authority.URL, binding, func(context.Context) (string, error) { return "consumer-token", nil }, authority.Client(), rewriteClient(provider))
			if err != nil {
				t.Fatal(err)
			}
			_, err = delivery.Invoke(context.Background())
			if err == nil {
				t.Fatal("Invoke succeeded")
			}
			if strings.Contains(err.Error(), deliveryKey) {
				t.Fatalf("secret leaked in error: %v", err)
			}
			wantProviderCalls := int64(0)
			if tt.callsProvider {
				wantProviderCalls = 1
			}
			if providerCalls.Load() != wantProviderCalls {
				t.Fatalf("provider calls = %d; want %d", providerCalls.Load(), wantProviderCalls)
			}
		})
	}
}

func TestAPIKeyDeliveryRejectsScalarSecretsInOutput(t *testing.T) {
	for _, tt := range []struct {
		name, key, token, output string
		wantErr                  bool
	}{
		{"numeric key", "123456789", "human-token", `{"nested":[123456789]}`, true},
		{"numeric human token", "provider-key", "987654321", `{"nested":[987654321]}`, true},
		{"boolean key", "true", "human-token", `{"nested":[true]}`, true},
		{"null human token", "provider-key", "null", `{"nested":[null]}`, true},
		{"unrelated number", "123456789", "human-token", `{"nested":[42]}`, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			binding := deliveryBinding()
			authority := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				writeDelivery(t, w, binding, tt.key)
			}))
			defer authority.Close()
			provider := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				_, _ = w.Write([]byte(tt.output))
			}))
			defer provider.Close()
			delivery, err := NewAPIKeyDelivery(authority.URL, binding, func(context.Context) (string, error) { return tt.token, nil }, authority.Client(), rewriteClient(provider))
			if err != nil {
				t.Fatal(err)
			}
			output, err := delivery.Invoke(context.Background())
			if (err != nil) != tt.wantErr || (tt.wantErr && len(output) != 0) || (!tt.wantErr && string(output) != tt.output) {
				t.Fatal("unexpected delivery result")
			}
		})
	}
}

func TestAPIKeyDeliveryRejectsMalformedOrDuplicateCredentialEnvelope(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{"malformed", `{"kind":"api_key"`},
		{"duplicate-api-key", `{"kind":"api_key","api_key":"one","api_key":"two"}`},
		{"trailing", `{} {}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var providerCalls atomic.Int64
			authority := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = w.Write([]byte(tt.body))
			}))
			defer authority.Close()
			delivery, err := NewAPIKeyDelivery(authority.URL, deliveryBinding(), func(context.Context) (string, error) { return "consumer-token", nil }, authority.Client(), noNetworkClient(&providerCalls))
			if err != nil {
				t.Fatal(err)
			}
			if _, err = delivery.Invoke(context.Background()); err == nil {
				t.Fatal("Invoke succeeded")
			}
			if providerCalls.Load() != 0 {
				t.Fatalf("provider calls = %d; want 0", providerCalls.Load())
			}
		})
	}
}

func TestAPIKeyDeliveryAcceptsExactLargeCredentialVersion(t *testing.T) {
	binding := deliveryBinding()
	binding.CredentialVersion = 9007199254740993
	authority := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { writeDelivery(t, w, binding, deliveryKey) }))
	defer authority.Close()
	provider := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(`{"items":[]}`)) }))
	defer provider.Close()
	delivery, err := NewAPIKeyDelivery(authority.URL, binding, func(context.Context) (string, error) { return "consumer-token", nil }, authority.Client(), rewriteClient(provider))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = delivery.Invoke(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestAPIKeyDeliveryDoesNotUseCachedKeyAfterRevoke(t *testing.T) {
	binding := deliveryBinding()
	var authorityCalls, providerCalls atomic.Int64
	authority := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if authorityCalls.Add(1) == 1 {
			writeDelivery(t, w, binding, deliveryKey)
			return
		}
		http.Error(w, "revoked", http.StatusNotFound)
	}))
	defer authority.Close()
	provider := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		providerCalls.Add(1)
		_, _ = w.Write([]byte(`{"items":[]}`))
	}))
	defer provider.Close()
	delivery, err := NewAPIKeyDelivery(authority.URL, binding, func(context.Context) (string, error) { return "consumer-token", nil }, authority.Client(), rewriteClient(provider))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = delivery.Invoke(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err = delivery.Invoke(context.Background()); err == nil {
		t.Fatal("Invoke succeeded after revoke")
	}
	if providerCalls.Load() != 1 {
		t.Fatalf("provider calls = %d; want 1", providerCalls.Load())
	}
}

func TestAPIKeyDeliveryNormalizesDuplicateProviderKeys(t *testing.T) {
	binding := deliveryBinding()
	authority := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { writeDelivery(t, w, binding, deliveryKey) }))
	defer authority.Close()
	provider := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"value":"` + deliveryKey + `","value":"safe"}`))
	}))
	defer provider.Close()
	delivery, err := NewAPIKeyDelivery(authority.URL, binding, func(context.Context) (string, error) { return "consumer-token", nil }, authority.Client(), rewriteClient(provider))
	if err != nil {
		t.Fatal(err)
	}
	got, err := delivery.Invoke(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != `{"value":"safe"}` || strings.Contains(string(got), deliveryKey) {
		t.Fatalf("normalized result = %s", got)
	}
}

func TestAPIKeyDeliveryDoesNotFollowRedirectsOrCallSourceAfterFailure(t *testing.T) {
	var sourceCalls, redirectedAuthority, redirectedProvider atomic.Int64
	binding := deliveryBinding()
	authority := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/redirect" {
			redirectedAuthority.Add(1)
		}
		http.Redirect(w, r, authorityURL(r)+"/redirect", http.StatusFound)
	}))
	defer authority.Close()
	provider := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/redirect" {
			redirectedProvider.Add(1)
		}
		http.Redirect(w, r, "https://openapi.gowid.com/redirect", http.StatusFound)
	}))
	defer provider.Close()
	delivery, err := NewAPIKeyDelivery(authority.URL, binding, func(context.Context) (string, error) { sourceCalls.Add(1); return "consumer-token", nil }, authority.Client(), rewriteClient(provider))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := delivery.Invoke(context.Background()); err == nil {
		t.Fatal("authority redirect succeeded")
	}
	if redirectedAuthority.Load() != 0 {
		t.Fatal("followed authority redirect")
	}
	providerAuthority := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { writeDelivery(t, w, binding, deliveryKey) }))
	defer providerAuthority.Close()
	delivery, err = NewAPIKeyDelivery(providerAuthority.URL, binding, func(context.Context) (string, error) { return "consumer-token", nil }, providerAuthority.Client(), rewriteClient(provider))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := delivery.Invoke(context.Background()); err == nil {
		t.Fatal("provider redirect succeeded")
	}
	if redirectedProvider.Load() != 0 {
		t.Fatal("followed provider redirect")
	}

	delivery, err = NewAPIKeyDelivery(authority.URL, binding, func(context.Context) (string, error) { return "consumer-token", errors.New("source " + deliveryKey) }, authority.Client(), rewriteClient(provider))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := delivery.Invoke(context.Background()); err == nil || strings.Contains(err.Error(), deliveryKey) {
		t.Fatalf("source error = %v", err)
	}
	if sourceCalls.Load() != 1 {
		t.Fatalf("source calls = %d", sourceCalls.Load())
	}
}

func deliveryBinding() DeliveryBinding {
	return DeliveryBinding{GrantID: "grant", ProviderID: "opaque-provider", ConnectionGeneration: "generation-1", CredentialVersion: 7, ConnectorDigest: "sha256:abc", Header: "Authorization", URL: "https://openapi.gowid.com/v2/expense-statements?page=1", ConsentExpiresAtUnixMS: time.Now().Add(time.Hour).UnixMilli()}
}

func deliveryMap(b DeliveryBinding, key string) map[string]any {
	return map[string]any{"kind": "api_key", "api_key": key, "grant_id": b.GrantID, "provider_id": b.ProviderID, "connection_generation": b.ConnectionGeneration, "credential_version": b.CredentialVersion, "connector_digest": b.ConnectorDigest, "header": b.Header, "prefix": b.Prefix, "consent_expires_at_unix_ms": b.ConsentExpiresAtUnixMS}
}

func writeDelivery(t *testing.T, w http.ResponseWriter, b DeliveryBinding, key string) {
	t.Helper()
	writeJSON(w, deliveryMap(b, key))
}
func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

type rewriteTransport struct {
	base   http.RoundTripper
	target *url.URL
}

func (t rewriteTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	if r.URL.Scheme != "https" || r.URL.Host != "openapi.gowid.com" {
		return nil, errors.New("unexpected provider origin")
	}
	copy := r.Clone(r.Context())
	u := *r.URL
	u.Scheme, u.Host = t.target.Scheme, t.target.Host
	copy.URL = &u
	copy.Host = ""
	return t.base.RoundTrip(copy)
}
func rewriteClient(server *httptest.Server) *http.Client {
	target, _ := url.Parse(server.URL)
	c := server.Client()
	copy := *c
	copy.Transport = rewriteTransport{base: c.Transport, target: target}
	return &copy
}

type noNetworkTransport struct{ calls *atomic.Int64 }

func (t noNetworkTransport) RoundTrip(*http.Request) (*http.Response, error) {
	t.calls.Add(1)
	return nil, errors.New("unexpected provider request")
}

func noNetworkClient(calls *atomic.Int64) *http.Client {
	return &http.Client{Transport: noNetworkTransport{calls: calls}}
}

func authorityURL(r *http.Request) string { return "https://" + r.Host }
