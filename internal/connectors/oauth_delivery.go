package connectors

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"
)

// OAuthDeliveryBinding comes from trusted host configuration and reviewed
// consent, never a model/tool input. CredentialVersion is a minimum, not an
// exact pin: verified refresh under the same authorization may advance it.
type OAuthDeliveryBinding struct {
	GrantID                string   `json:"grant_id"`
	ProviderID             string   `json:"provider_id"`
	AccountID              string   `json:"account_id"`
	ConnectionGeneration   string   `json:"connection_generation"`
	Resource               string   `json:"resource"`
	ConsumerClientID       string   `json:"consumer_client_id"`
	OwnerSubject           string   `json:"owner_subject"`
	Endpoint               string   `json:"endpoint"`
	Scopes                 []string `json:"scopes"`
	CredentialVersion      int64    `json:"credential_version"`
	ConsentExpiresAtUnixMS int64    `json:"consent_expires_at_unix_ms"`
}

// OAuthTokenRequest selects the owner's execution-consumer human authority at
// the trusted host. These labels are not authentication; GoAuthy checks the
// actual token and grant. No client-credentials or other-client token fallback.
type OAuthTokenRequest struct{ OwnerSubject, Resource, ConsumerClientID string }

// NewOAuthDeliveryClient provides an authenticated HTTP client for AMP's existing
// generic connector/JSON HTTP path. Only the reviewed, query-free HTTPS GET is
// allowed. No OAuth config, client secret, refresh token, scheduler or cache is
// installed. Injected transports and token sources must verify TLS and not log
// secrets. now is injectable for expiry tests; nil uses time.Now.
func NewOAuthDeliveryClient(issuer string, binding OAuthDeliveryBinding, source func(context.Context, OAuthTokenRequest) (string, error), authorityClient, providerClient *http.Client, now func() time.Time) (*http.Client, error) {
	if now == nil {
		now = time.Now
	}
	issuerURL, err := oauthHTTPS(issuer)
	if err != nil {
		return nil, errors.New("oauth delivery issuer invalid")
	}
	endpoint, err := oauthHTTPS(binding.Endpoint)
	if err != nil || source == nil || binding.CredentialVersion < 1 || binding.ConsentExpiresAtUnixMS <= now().UnixMilli() {
		return nil, errors.New("oauth delivery binding invalid")
	}
	for _, value := range []string{binding.GrantID, binding.ProviderID, binding.AccountID, binding.ConnectionGeneration, binding.Resource, binding.ConsumerClientID, binding.OwnerSubject} {
		if value == "" || strings.ContainsAny(value, "\r\n\x00") {
			return nil, errors.New("oauth delivery binding invalid")
		}
	}
	if binding.GrantID == "." || binding.GrantID == ".." {
		return nil, errors.New("oauth delivery grant invalid")
	}
	scopes, ok := oauthScopes(binding.Scopes)
	if !ok {
		return nil, errors.New("oauth delivery scopes invalid")
	}
	binding.Scopes = scopes
	// Preserve an issuer base path; escape opaque grant IDs as one segment.
	path, escaped := strings.TrimRight(issuerURL.Path, "/"), strings.TrimRight(issuerURL.EscapedPath(), "/")
	issuerURL.Path = path + "/auth/v1/connection-grants/" + binding.GrantID + "/credential"
	issuerURL.RawPath = escaped + "/auth/v1/connection-grants/" + url.PathEscape(binding.GrantID) + "/credential"
	authority, err := oauthHTTPClient(authorityClient)
	if err != nil {
		return nil, err
	}
	provider, err := oauthHTTPClient(providerClient)
	if err != nil {
		return nil, err
	}
	provider.Transport = &oauthDeliveryTransport{binding: binding, endpoint: endpoint, issuer: issuerURL.String(), source: source, authority: authority, provider: provider.Transport, now: now, gate: make(chan struct{}, 1), lastVersion: binding.CredentialVersion}
	return provider, nil
}

func oauthHTTPS(raw string) (*url.URL, error) {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.Opaque != "" || u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || strings.Contains(raw, "#") {
		return nil, errors.New("invalid HTTPS endpoint")
	}
	return u, nil
}

func oauthHTTPClient(client *http.Client) (*http.Client, error) {
	copy := deliveryHTTPClient(client)
	if copy.Transport == nil {
		copy.Transport = http.DefaultTransport
	}
	if transport, ok := copy.Transport.(*http.Transport); ok {
		if transport.TLSClientConfig != nil && transport.TLSClientConfig.InsecureSkipVerify {
			return nil, errors.New("oauth delivery requires verified TLS")
		}
		transport = transport.Clone()
		if transport.TLSClientConfig == nil {
			transport.TLSClientConfig = &tls.Config{}
		} else {
			transport.TLSClientConfig = transport.TLSClientConfig.Clone()
		}
		if transport.TLSClientConfig.MinVersion < tls.VersionTLS12 {
			transport.TLSClientConfig.MinVersion = tls.VersionTLS12
		}
		copy.Transport = transport
	}
	return copy, nil
}

type oauthDeliveryTransport struct {
	binding     OAuthDeliveryBinding
	endpoint    *url.URL
	issuer      string
	source      func(context.Context, OAuthTokenRequest) (string, error)
	authority   *http.Client
	provider    http.RoundTripper
	now         func() time.Time
	gate        chan struct{}
	lastVersion int64
}

func (t *oauthDeliveryTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	if request == nil || request.URL == nil || request.Method != http.MethodGet || request.URL.String() != t.endpoint.String() || (request.Host != "" && request.Host != t.endpoint.Host) || (request.Body != nil && request.Body != http.NoBody) || request.ContentLength != 0 || len(request.TransferEncoding) != 0 || len(request.Trailer) != 0 {
		return nil, errors.New("oauth delivery request not allowed")
	}
	for name := range request.Header {
		if strings.EqualFold(name, "Authorization") || strings.EqualFold(name, "Cookie") || strings.EqualFold(name, "Proxy-Authorization") {
			return nil, errors.New("oauth delivery caller credentials not allowed")
		}
	}
	// ponytail: serialize one binding to keep its observed version monotonic;
	// use per-binding clients for independent profiles, not a global lock.
	select {
	case t.gate <- struct{}{}:
		defer func() { <-t.gate }()
	case <-request.Context().Done():
		return nil, errors.New("oauth delivery canceled")
	}
	if t.binding.ConsentExpiresAtUnixMS <= t.now().UnixMilli() {
		return nil, errors.New("oauth delivery consent expired")
	}
	credential, human, err := t.credential(request.Context())
	if err != nil {
		return nil, err
	}
	if credential.Version < t.lastVersion || credential.TokenExpiry <= t.now().UnixMilli() || credential.ConsentExpiry <= t.now().UnixMilli() {
		return nil, errors.New("oauth delivery stale credential")
	}
	t.lastVersion = credential.Version
	out := request.Clone(request.Context())
	if out.Header == nil {
		out.Header = make(http.Header)
	}
	out.Header.Set("Authorization", "Bearer "+credential.AccessToken)
	response, err := t.provider.RoundTrip(out)
	if err != nil {
		return nil, errors.New("oauth delivery provider request failed")
	}
	if response == nil || response.Body == nil {
		return nil, errors.New("oauth delivery provider response invalid")
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("oauth delivery provider rejected request (HTTP %d)", response.StatusCode)
	}
	body, err := readDeliveryBody(response.Body, 1<<20)
	if err != nil || !json.Valid(body) {
		return nil, errors.New("oauth delivery provider response invalid")
	}
	var value any
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	if decoder.Decode(&value) != nil || containsDeliverySecret(value, credential.AccessToken, human) {
		return nil, errors.New("oauth delivery provider response contains credentials")
	}
	clean, err := json.Marshal(value)
	if err != nil || bytes.Contains(clean, []byte(credential.AccessToken)) || bytes.Contains(clean, []byte(human)) {
		return nil, errors.New("oauth delivery provider response contains credentials")
	}
	if credential.TokenExpiry <= t.now().UnixMilli() || credential.ConsentExpiry <= t.now().UnixMilli() {
		return nil, errors.New("oauth delivery credential expired during provider call")
	}
	// Do not expose upstream cookies/headers or response.Request.Authorization to
	// the existing connector error/logger path. Only inspected JSON is returned.
	return &http.Response{StatusCode: http.StatusOK, Status: "200 OK", Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(bytes.NewReader(clean)), ContentLength: int64(len(clean)), Request: request.Clone(request.Context())}, nil
}

func (t *oauthDeliveryTransport) CloseIdleConnections() {
	t.authority.CloseIdleConnections()
	if closer, ok := t.provider.(interface{ CloseIdleConnections() }); ok {
		closer.CloseIdleConnections()
	}
}

type deliveredOAuth struct {
	Kind, AccessToken, TokenType, GrantID, ProviderID, Generation, AccountID string
	Scopes                                                                   []string
	Version, TokenExpiry, ConsentExpiry                                      int64
}

func (deliveredOAuth) String() string   { return "[redacted OAuth delivery]" }
func (deliveredOAuth) GoString() string { return "[redacted OAuth delivery]" }
func (deliveredOAuth) MarshalJSON() ([]byte, error) {
	return []byte(`"[redacted OAuth delivery]"`), nil
}

func (t *oauthDeliveryTransport) credential(ctx context.Context) (deliveredOAuth, string, error) {
	var empty deliveredOAuth
	human, err := t.source(ctx, OAuthTokenRequest{OwnerSubject: t.binding.OwnerSubject, Resource: t.binding.Resource, ConsumerClientID: t.binding.ConsumerClientID})
	if err != nil || !oauthBearer(human) {
		return empty, "", errors.New("oauth delivery human authority unavailable")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, t.issuer, nil)
	if err != nil {
		return empty, "", errors.New("oauth delivery authority request invalid")
	}
	req.Header.Set("Authorization", "Bearer "+human)
	response, err := t.authority.Do(req)
	if err != nil {
		return empty, "", errors.New("oauth delivery authority request failed")
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return empty, "", fmt.Errorf("oauth delivery authority rejected request (HTTP %d)", response.StatusCode)
	}
	body, err := readDeliveryBody(response.Body, 64<<10)
	if err != nil {
		return empty, "", errors.New("oauth delivery credential response invalid")
	}
	value, err := decodeDeliveredOAuth(body)
	if err != nil {
		return empty, "", errors.New("oauth delivery credential response invalid")
	}
	scopes, ok := oauthScopes(value.Scopes)
	b := t.binding
	if !ok || !slices.Equal(scopes, b.Scopes) || value.Kind != "oauth2" || value.TokenType != "Bearer" || !oauthBearer(value.AccessToken) || value.AccessToken == human || value.GrantID != b.GrantID || value.ProviderID != b.ProviderID || value.Generation != b.ConnectionGeneration || value.AccountID != b.AccountID || value.Version < b.CredentialVersion || value.ConsentExpiry != b.ConsentExpiresAtUnixMS {
		return empty, "", errors.New("oauth delivery credential binding mismatch")
	}
	return value, human, nil
}

func oauthBearer(value string) bool {
	return value != "" && len(value) <= 8192 && strings.IndexFunc(value, func(r rune) bool { return r < 33 || r > 126 }) < 0
}

func oauthScopes(scopes []string) ([]string, bool) {
	if scopes == nil {
		return nil, false
	}
	copy := append([]string{}, scopes...)
	slices.Sort(copy)
	for i, scope := range copy {
		if !oauthBearer(scope) || (i > 0 && scope == copy[i-1]) {
			return nil, false
		}
	}
	return copy, true
}

// Deliberately separate from API-key delivery and credential-status: exactly
// eleven named, non-null fields; duplicate/unknown/case-alias fields fail closed.
func decodeDeliveredOAuth(body []byte) (deliveredOAuth, error) {
	var value deliveredOAuth
	decoder := json.NewDecoder(bytes.NewReader(body))
	if token, err := decoder.Token(); err != nil || token != json.Delim('{') {
		return value, errors.New("invalid OAuth envelope")
	}
	fields := make(map[string]json.RawMessage, 11)
	for decoder.More() {
		name, err := decoder.Token()
		if err != nil {
			return value, err
		}
		key, ok := name.(string)
		if !ok || fields[key] != nil {
			return value, errors.New("duplicate OAuth field")
		}
		var raw json.RawMessage
		if err := decoder.Decode(&raw); err != nil {
			return value, err
		}
		fields[key] = raw
	}
	if token, err := decoder.Token(); err != nil || token != json.Delim('}') {
		return value, errors.New("invalid OAuth envelope")
	}
	if _, err := decoder.Token(); err != io.EOF {
		return value, errors.New("trailing OAuth content")
	}
	required := []struct {
		name string
		out  any
	}{
		{"kind", &value.Kind}, {"access_token", &value.AccessToken}, {"token_type", &value.TokenType}, {"grant_id", &value.GrantID}, {"provider_id", &value.ProviderID}, {"connection_generation", &value.Generation}, {"credential_version", &value.Version}, {"account_id", &value.AccountID}, {"scopes", &value.Scopes}, {"token_expires_at_unix_ms", &value.TokenExpiry}, {"consent_expires_at_unix_ms", &value.ConsentExpiry},
	}
	if len(fields) != len(required) {
		return value, errors.New("invalid OAuth field count")
	}
	for _, field := range required {
		raw := fields[field.name]
		if len(raw) == 0 || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
			return value, errors.New("missing OAuth field")
		}
		if err := json.Unmarshal(raw, field.out); err != nil {
			return value, err
		}
	}
	return value, nil
}
