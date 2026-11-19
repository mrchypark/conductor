package connectors

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/amp-labs/connectors/providers"
	"github.com/mrchypark/conductor/internal/handlers"
)

func testECKey(t *testing.T) *ecdsa.PrivateKey {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate EC key: %v", err)
	}
	return key
}

func verifyDPoPHeader(t *testing.T, proof, expectedHTM, expectedHTU, expectedNonce, expectedToken string) map[string]any {
	t.Helper()
	parts := strings.Split(proof, ".")
	if len(parts) != 3 {
		t.Fatalf("invalid DPoP JWT parts: %d", len(parts))
	}
	headerBytes, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		t.Fatalf("decode DPoP header: %v", err)
	}
	var header struct {
		Typ string      `json:"typ"`
		Alg string      `json:"alg"`
		JWK ecPublicJWK `json:"jwk"`
	}
	if err := json.Unmarshal(headerBytes, &header); err != nil {
		t.Fatalf("unmarshal DPoP header: %v", err)
	}
	if header.Typ != "dpop+jwt" || header.Alg != "ES256" || header.JWK.Kty != "EC" || header.JWK.Crv != "P-256" {
		t.Fatalf("unexpected DPoP header: %+v", header)
	}
	xb, _ := base64.RawURLEncoding.DecodeString(header.JWK.X)
	yb, _ := base64.RawURLEncoding.DecodeString(header.JWK.Y)
	pub := &ecdsa.PublicKey{Curve: elliptic.P256(), X: new(big.Int).SetBytes(xb), Y: new(big.Int).SetBytes(yb)}
	sig, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil || len(sig) != 64 {
		t.Fatalf("invalid DPoP signature length: %d", len(sig))
	}
	digest := sha256.Sum256([]byte(parts[0] + "." + parts[1]))
	if !ecdsa.Verify(pub, digest[:], new(big.Int).SetBytes(sig[:32]), new(big.Int).SetBytes(sig[32:])) {
		t.Fatal("DPoP signature verification failed")
	}

	claimsBytes, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		t.Fatalf("decode DPoP claims: %v", err)
	}
	var claims map[string]any
	if err := json.Unmarshal(claimsBytes, &claims); err != nil {
		t.Fatalf("unmarshal DPoP claims: %v", err)
	}
	if claims["htm"] != expectedHTM || claims["htu"] != expectedHTU {
		t.Fatalf("unexpected htm/htu: got %v %v, want %s %s", claims["htm"], claims["htu"], expectedHTM, expectedHTU)
	}
	if expectedNonce != "" && claims["nonce"] != expectedNonce {
		t.Fatalf("unexpected nonce: got %v want %s", claims["nonce"], expectedNonce)
	}
	if expectedToken != "" {
		sum := sha256.Sum256([]byte(expectedToken))
		wantATH := base64.RawURLEncoding.EncodeToString(sum[:])
		if claims["ath"] != wantATH {
			t.Fatalf("unexpected ath: got %v want %s", claims["ath"], wantATH)
		}
	} else if _, hasATH := claims["ath"]; hasATH {
		t.Fatal("did not expect ath claim on token exchange DPoP proof")
	}
	return claims
}

func TestLoadDPoPKeyFileSingleAndRollover(t *testing.T) {
	k1 := testECKey(t)
	k2 := testECKey(t)
	pub1, _ := publicJWKFromKey(&k1.PublicKey)
	pub2, _ := publicJWKFromKey(&k2.PublicKey)
	var d1, d2 [32]byte
	k1.D.FillBytes(d1[:])
	k2.D.FillBytes(d2[:])
	jwk1 := ecPrivateJWK{Kty: "EC", Crv: "P-256", X: pub1.X, Y: pub1.Y, D: base64.RawURLEncoding.EncodeToString(d1[:])}
	jwk2 := ecPrivateJWK{Kty: "EC", Crv: "P-256", X: pub2.X, Y: pub2.Y, D: base64.RawURLEncoding.EncodeToString(d2[:])}

	dir := t.TempDir()
	singlePath := filepath.Join(dir, "single.json")
	rawSingle, _ := json.Marshal(jwk1)
	if err := os.WriteFile(singlePath, rawSingle, 0o600); err != nil {
		t.Fatal(err)
	}
	loaded1, err := loadDPoPKeyFile(singlePath)
	if err != nil || !loaded1.Equal(k1) {
		t.Fatalf("single key load failed: err=%v", err)
	}

	rolloverPath := filepath.Join(dir, "rollover.json")
	rawRoll, _ := json.Marshal(dpopKeySetFile{Current: &jwk2, Previous: &jwk1})
	if err := os.WriteFile(rolloverPath, rawRoll, 0o600); err != nil {
		t.Fatal(err)
	}
	loaded2, err := loadDPoPKeyFile(rolloverPath)
	if err != nil || !loaded2.Equal(k2) {
		t.Fatalf("rollover key load failed: err=%v", err)
	}
}

func TestGoauthyExchangeTokenDPoPNonceRetryAndErrors(t *testing.T) {
	key := testECKey(t)
	const issuer = "https://goauthy.example.internal"
	const resource = "https://goauthy.example.internal/connections"

	var attempts int
	var firstJTI string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		user, pass, ok := r.BasicAuth()
		if !ok || user != "conductor-client" || pass != "conductor-secret" {
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"error":"invalid_client"}`))
			return
		}
		if err := r.ParseForm(); err != nil {
			t.Fatal(err)
		}
		if r.Form.Get("grant_type") != "urn:ietf:params:oauth:grant-type:token-exchange" ||
			r.Form.Get("scope") != "goauthy.connections.use" ||
			r.Form.Get("resource") != resource {
			t.Fatalf("unexpected token exchange form: %v", r.Form)
		}
		switch r.Form.Get("subject_token") {
		case "subj-nonce":
			if attempts == 1 {
				claims := verifyDPoPHeader(t, r.Header.Get("DPoP"), http.MethodPost, issuer+"/oidc/token", "", "")
				firstJTI, _ = claims["jti"].(string)
				w.Header().Set("DPoP-Nonce", "nonce-1")
				w.WriteHeader(http.StatusBadRequest)
				_, _ = w.Write([]byte(`{"error":"use_dpop_nonce"}`))
				return
			}
			claims := verifyDPoPHeader(t, r.Header.Get("DPoP"), http.MethodPost, issuer+"/oidc/token", "nonce-1", "")
			if claims["jti"] == firstJTI {
				t.Fatal("expected fresh jti on DPoP nonce retry")
			}
			_, _ = w.Write([]byte(`{"access_token":"del-tok-123","token_type":"DPoP","expires_in":300}`))
		case "subj-expired":
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte(`{"error":"access_denied"}`))
		case "subj-bad-scope":
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"error":"invalid_scope"}`))
		default:
			w.WriteHeader(http.StatusServiceUnavailable)
		}
	}))
	defer srv.Close()

	client, err := NewGoauthyClientWithKey(GoauthyConfig{
		BaseURL:           srv.URL,
		Issuer:            issuer,
		ClientID:          "conductor-client",
		ClientSecret:      "conductor-secret",
		Resource:          resource,
		AllowInsecureHTTP: true,
		HTTPClient:        srv.Client(),
	}, key)
	if err != nil {
		t.Fatalf("NewGoauthyClientWithKey: %v", err)
	}

	tok, exp, err := client.ExchangeToken(context.Background(), "subj-nonce")
	if err != nil {
		t.Fatalf("ExchangeToken: %v", err)
	}
	if string(tok) != "del-tok-123" || exp.Before(time.Now().UTC().Add(200*time.Second)) {
		t.Fatalf("unexpected token/exp: %q %v", string(tok), exp)
	}
	if attempts != 2 {
		t.Fatalf("expected 2 attempts with nonce retry, got %d", attempts)
	}

	for _, tc := range []struct {
		subj      string
		wantClass string
	}{
		{"subj-expired", "auth_expired"},
		{"subj-bad-scope", "goauthy_misconfigured"},
		{"subj-503", "goauthy_unavailable"},
	} {
		_, _, err := client.ExchangeToken(context.Background(), tc.subj)
		var hErr *handlers.Error
		if !errors.As(err, &hErr) || hErr.Class != tc.wantClass {
			t.Fatalf("subject %s: got err=%v want class=%s", tc.subj, err, tc.wantClass)
		}
	}
}

func TestGoauthyFetchStatusAndRefreshRecovery(t *testing.T) {
	key := testECKey(t)
	const issuer = "https://goauthy.example.internal"
	const grantID = "grant_123"
	const delTok = "delegation-jwt-abc"

	var credCalls int
	var refreshed bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "DPoP "+delTok {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		verifyDPoPHeader(t, r.Header.Get("DPoP"), r.Method, issuer+r.URL.Path, "", delTok)

		switch r.URL.Path {
		case "/auth/v1/connection-grants/" + grantID + "/credential":
			credCalls++
			if !refreshed {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			_ = json.NewEncoder(w).Encode(DeliveredCredential{
				Kind:                   "oauth2",
				AccessToken:            "fresh-oauth-tok",
				TokenType:              "Bearer",
				ProviderID:             "hubspot",
				TokenExpiresAtUnixMs:   time.Now().UTC().Add(10 * time.Minute).UnixMilli(),
				ConsentExpiresAtUnixMs: time.Now().UTC().Add(24 * time.Hour).UnixMilli(),
			})
		case "/auth/v1/connection-grants/" + grantID + "/credential-status":
			_ = json.NewEncoder(w).Encode(CredentialStatus{
				State:        "ready",
				Version:      7,
				AllowRefresh: true,
			})
		case "/auth/v1/connection-grants/" + grantID + "/refresh":
			var body struct {
				CredentialVersion int64 `json:"credential_version"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.CredentialVersion != 7 {
				t.Fatalf("unexpected refresh body: %+v err=%v", body, err)
			}
			refreshed = true
			w.WriteHeader(http.StatusOK)
		default:
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
	}))
	defer srv.Close()

	client, err := NewGoauthyClientWithKey(GoauthyConfig{
		BaseURL:           srv.URL,
		Issuer:            issuer,
		ClientID:          "conductor-client",
		ClientSecret:      "conductor-secret",
		Resource:          issuer + "/connections",
		AllowInsecureHTTP: true,
		HTTPClient:        srv.Client(),
	}, key)
	if err != nil {
		t.Fatal(err)
	}

	delivered, err := fetchAndRecoverCredential(context.Background(), client, grantID, delTok, false)
	if err != nil {
		t.Fatalf("fetchAndRecoverCredential failed: %v", err)
	}
	if delivered.AccessToken != "fresh-oauth-tok" || credCalls != 3 || !refreshed {
		t.Fatalf("unexpected recovery result: delivered=%+v credCalls=%d refreshed=%v", delivered, credCalls, refreshed)
	}

	// Verify expiry & binding checks
	if err := validateDeliveredCredential(delivered, "hubspot", providers.Oauth2, time.Now().UTC()); err != nil {
		t.Fatalf("validateDeliveredCredential unexpected error: %v", err)
	}
	if err := validateDeliveredCredential(delivered, "salesforce", providers.Oauth2, time.Now().UTC()); err == nil {
		t.Fatal("expected provider mismatch error")
	}
}
