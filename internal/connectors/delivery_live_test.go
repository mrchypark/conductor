package connectors

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"sync/atomic"
	"testing"
)

// Opt-in bridge to a separately owned, disposable credential-provider HTTPS fixture.
// Only credential retrieval is live; provider traffic is always local TLS.
func TestAPIKeyDeliveryCredentialProviderFixture(t *testing.T) {
	if os.Getenv("CONDUCTOR_DELIVERY_LIVE") != "1" {
		t.Skip("requires explicitly enabled disposable credential-provider fixture")
	}
	expectedKey := os.Getenv("CONDUCTOR_DELIVERY_EXPECTED_API_KEY")
	humanToken := os.Getenv("CONDUCTOR_DELIVERY_HUMAN_TOKEN")
	if expectedKey == "" || humanToken == "" || expectedKey == humanToken || expectedKey == "Bearer "+humanToken {
		t.Fatal("fixture requires distinct nonempty synthetic key and human token")
	}
	expectDenied := os.Getenv("CONDUCTOR_DELIVERY_EXPECT_DENIED") == "1"
	var binding DeliveryBinding
	if err := json.Unmarshal([]byte(os.Getenv("CONDUCTOR_DELIVERY_BINDING")), &binding); err != nil {
		t.Fatal("invalid fixture binding JSON")
	}
	ca, err := os.ReadFile(os.Getenv("CONDUCTOR_DELIVERY_CA_FILE"))
	if err != nil {
		t.Fatal("fixture CA unavailable")
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(ca) {
		t.Fatal("fixture CA invalid")
	}
	authority := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}}}
	defer authority.CloseIdleConnections()
	var calls atomic.Int64
	provider := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.Method != http.MethodGet || r.URL.Path != "/v2/expense-statements" || r.Header.Get("Authorization") != expectedKey {
			t.Error("invalid provider fixture request")
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":[]}`))
	}))
	defer provider.Close()
	local, _ := url.Parse(provider.URL)
	base := provider.Client().Transport
	providerClient := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.URL.Scheme != "https" || r.URL.Host != "openapi.gowid.com" {
			return nil, http.ErrNotSupported
		}
		copy := r.Clone(r.Context())
		endpoint := *r.URL
		endpoint.Host = local.Host
		copy.URL = &endpoint
		copy.Host = local.Host
		return base.RoundTrip(copy)
	})}
	adapter, err := NewAPIKeyDelivery(os.Getenv("CONDUCTOR_DELIVERY_ISSUER"), binding,
		func(context.Context) (string, error) { return humanToken, nil }, authority, providerClient)
	if err != nil {
		t.Fatal(err)
	}
	if expectDenied {
		result, err := adapter.Invoke(context.Background())
		if err == nil || len(result) != 0 || calls.Load() != 0 {
			t.Fatal("expected denied invocation without result or provider calls")
		}
		return
	}
	for range 2 {
		result, err := adapter.Invoke(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if string(result) != `{"data":[]}` {
			t.Fatal("unexpected provider result")
		}
	}
	if calls.Load() != 2 {
		t.Fatal("provider calls missing")
	}
}
