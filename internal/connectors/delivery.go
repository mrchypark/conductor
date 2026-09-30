package connectors

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// DeliveryBinding is an immutable, trusted server-side snapshot of reviewed
// consent and an operator-approved AMP mapping, never browser callback input.
// This first adapter supports only Gowid expense-statement GETs.
type DeliveryBinding struct {
	GrantID                string `json:"grant_id"`
	ProviderID             string `json:"provider_id"`
	ConnectionGeneration   string `json:"connection_generation"`
	ConnectorDigest        string `json:"connector_digest"`
	Header                 string `json:"header"`
	Prefix                 string `json:"prefix"`
	URL                    string `json:"url"`
	CredentialVersion      int64  `json:"credential_version"`
	ConsentExpiresAtUnixMS int64  `json:"consent_expires_at_unix_ms"`
}

// DeliveryTokenSource supplies this execution consumer's current human token.
// Login, owner/session binding and authorization lifecycle belong to the caller.
// A service token or another OAuth client's token is not a substitute.
type DeliveryTokenSource func(context.Context) (string, error)

type APIKeyDelivery struct {
	endpoint            string
	binding             DeliveryBinding
	source              DeliveryTokenSource
	authority, provider *http.Client
}

func NewAPIKeyDelivery(issuer string, binding DeliveryBinding, source DeliveryTokenSource, authorityClient, providerClient *http.Client) (*APIKeyDelivery, error) {
	u, err := url.Parse(issuer)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
		return nil, errors.New("delivery requires an HTTPS issuer origin")
	}
	if source == nil || binding.GrantID == "" || binding.GrantID == "." || binding.GrantID == ".." || binding.ProviderID == "" || binding.ConnectionGeneration == "" || binding.ConnectorDigest == "" || binding.CredentialVersion < 1 || binding.ConsentExpiresAtUnixMS <= time.Now().UnixMilli() {
		return nil, errors.New("delivery requires a current trusted binding and human token source")
	}
	p, err := url.Parse(binding.URL)
	if err != nil || p.Scheme != "https" || p.Host != "openapi.gowid.com" || p.User != nil || p.Fragment != "" || p.Path != "/v2/expense-statements" || p.RawPath != "" || binding.Header != "Authorization" || binding.Prefix != "" {
		return nil, errors.New("delivery binding does not match the fixed Gowid mapping")
	}
	return &APIKeyDelivery{
		endpoint: strings.TrimSuffix(issuer, "/") + "/auth/v1/connection-grants/" + url.PathEscape(binding.GrantID) + "/credential",
		binding:  binding, source: source,
		authority: deliveryHTTPClient(authorityClient), provider: deliveryHTTPClient(providerClient),
	}, nil
}

func deliveryHTTPClient(client *http.Client) *http.Client {
	if client == nil {
		client = http.DefaultClient
	}
	copy := *client
	copy.Jar = nil
	copy.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	if copy.Timeout == 0 {
		copy.Timeout = 30 * time.Second
	}
	return &copy
}

type deliveredAPIKey struct {
	Kind       string `json:"kind"`
	APIKey     string `json:"api_key"`
	GrantID    string `json:"grant_id"`
	ProviderID string `json:"provider_id"`
	Generation string `json:"connection_generation"`
	Version    int64  `json:"credential_version"`
	Digest     string `json:"connector_digest"`
	Header     string `json:"header"`
	Prefix     string `json:"prefix"`
	Expiry     int64  `json:"consent_expires_at_unix_ms"`
}

// Invoke retrieves a fresh credential for each fixed GET, with no cached-key
// fallback or application retry. It does not persist credentials or expose them
// to execution inputs. Injected transports/token sources must not log secrets.
// GoAuthy revocation cannot recall a key once delivered or undo an upstream call.
func (d *APIKeyDelivery) Invoke(ctx context.Context) (json.RawMessage, error) {
	if d.binding.ConsentExpiresAtUnixMS <= time.Now().UnixMilli() {
		return nil, errors.New("delivery consent expired")
	}
	token, err := d.source(ctx)
	if err != nil || token == "" || strings.ContainsAny(token, "\r\n") {
		return nil, errors.New("delivery human authority unavailable")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, d.endpoint, nil)
	if err != nil {
		return nil, errors.New("delivery request invalid")
	}
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := d.authority.Do(req)
	if err != nil {
		return nil, errors.New("delivery authority request failed")
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("delivery authority rejected request (HTTP %d)", resp.StatusCode)
	}
	body, err := readDeliveryBody(resp.Body, 64<<10)
	if err != nil {
		return nil, errors.New("delivery response invalid")
	}
	key, err := decodeDeliveredKey(body)
	if err != nil {
		return nil, errors.New("delivery response invalid")
	}
	b := d.binding
	if key.Kind != "api_key" || key.APIKey == "" || strings.ContainsAny(key.APIKey, "\r\n") || key.GrantID != b.GrantID || key.ProviderID != b.ProviderID || key.Generation != b.ConnectionGeneration || key.Version != b.CredentialVersion || key.Digest != b.ConnectorDigest || key.Header != b.Header || key.Prefix != b.Prefix || key.Expiry != b.ConsentExpiresAtUnixMS || key.Expiry <= time.Now().UnixMilli() {
		return nil, errors.New("delivery credential does not match trusted binding")
	}
	// Reuse the existing AMP catalog/auth implementation, not returned header
	// metadata. The reviewed binding above cannot choose an arbitrary destination.
	catalog, err := LoadCatalog()
	if err != nil {
		return nil, errors.New("delivery AMP catalog unavailable")
	}
	info, err := catalog.Info("gowid")
	if err != nil {
		return nil, errors.New("delivery AMP mapping unavailable")
	}
	client, err := newAuthClient(ctx, info, d.provider, Credentials{APIKey: key.APIKey})
	if err != nil {
		return nil, errors.New("delivery provider authentication failed")
	}
	providerReq, err := http.NewRequestWithContext(ctx, http.MethodGet, b.URL, nil)
	if err != nil {
		return nil, errors.New("delivery provider request invalid")
	}
	providerResp, err := client.Do(providerReq)
	if err != nil {
		return nil, errors.New("delivery provider request failed")
	}
	defer providerResp.Body.Close()
	if providerResp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("delivery provider rejected request (HTTP %d)", providerResp.StatusCode)
	}
	result, err := readDeliveryBody(providerResp.Body, 1<<20)
	if err != nil {
		return nil, errors.New("delivery provider response invalid")
	}
	var decoded any
	decoder := json.NewDecoder(bytes.NewReader(result))
	decoder.UseNumber()
	if !json.Valid(result) || decoder.Decode(&decoded) != nil || containsDeliverySecret(decoded, key.APIKey, token) {
		return nil, errors.New("delivery provider response invalid or contains credentials")
	}
	// Return only the inspected JSON tree; duplicate object members discarded by
	// decoding must not smuggle an uninspected value into execution output.
	return json.Marshal(decoded)
}

func readDeliveryBody(reader io.Reader, limit int64) ([]byte, error) {
	body, err := io.ReadAll(io.LimitReader(reader, limit+1))
	if err != nil || int64(len(body)) > limit {
		return nil, errors.New("invalid response size")
	}
	return body, nil
}

func decodeDeliveredKey(body []byte) (deliveredAPIKey, error) {
	var key deliveredAPIKey
	decoder := json.NewDecoder(bytes.NewReader(body))
	first, err := decoder.Token()
	if err != nil || first != json.Delim('{') {
		return key, errors.New("invalid credential object")
	}
	fields := make(map[string]json.RawMessage)
	for decoder.More() {
		name, err := decoder.Token()
		if err != nil {
			return key, err
		}
		field, ok := name.(string)
		if !ok || fields[field] != nil {
			return key, errors.New("duplicate credential field")
		}
		var value json.RawMessage
		if err := decoder.Decode(&value); err != nil {
			return key, err
		}
		fields[field] = value
	}
	if _, err := decoder.Token(); err != nil {
		return key, err
	}
	if _, err := decoder.Token(); err != io.EOF {
		return key, errors.New("trailing credential content")
	}
	required := make(map[string]json.RawMessage, 10)
	for _, field := range []string{"kind", "api_key", "grant_id", "provider_id", "connection_generation", "credential_version", "connector_digest", "header", "prefix", "consent_expires_at_unix_ms"} {
		if len(fields[field]) == 0 || bytes.Equal(bytes.TrimSpace(fields[field]), []byte("null")) {
			return key, errors.New("missing credential field")
		}
		required[field] = fields[field]
	}
	// Direct int64 decoding avoids float64 rounding above 2^53. Additive fields
	// are tolerated only after required fields and supported kind are validated.
	// Ignore additive metadata without letting case-insensitive struct matching
	// turn e.g. an unknown API_KEY field into a second credential value.
	canonical, err := json.Marshal(required)
	if err != nil {
		return key, err
	}
	err = json.Unmarshal(canonical, &key)
	return key, err
}

func containsDeliverySecret(value any, key, token string) bool {
	switch v := value.(type) {
	case string:
		return strings.Contains(v, key) || strings.Contains(v, token)
	case map[string]any:
		for name, item := range v {
			if containsDeliverySecret(name, key, token) || containsDeliverySecret(item, key, token) {
				return true
			}
		}
	case []any:
		for _, item := range v {
			if containsDeliverySecret(item, key, token) {
				return true
			}
		}
	}
	return false
}
