package connectors

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/mrchypark/conductor/internal/handlers"
	"golang.org/x/oauth2"
)

// ErrCredentialNotFound indicates GoAuthy returned HTTP 404 for /credential.
var ErrCredentialNotFound = errors.New("goauthy credential not found")

// GoauthyConfig configures the RFC 8693 + DPoP + RFC 8707 GoAuthy client.
type GoauthyConfig struct {
	BaseURL           string
	Issuer            string
	ClientID          string
	ClientSecret      string
	Resource          string
	DPoPKeyFile       string
	AllowInsecureHTTP bool
	HTTPClient        *http.Client
}

// GoauthyClient exchanges human subject tokens for DPoP-bound delegation tokens
// via RFC 8693 and fetches/refreshes scoped connection credentials from GoAuthy.
type GoauthyClient struct {
	baseURL      string
	issuer       string
	clientID     string
	clientSecret string
	resource     string
	dpopKey      *ecdsa.PrivateKey
	jwk          ecPublicJWK
	httpClient   *http.Client
	now          func() time.Time
}

type ecPublicJWK struct {
	Kty string `json:"kty"`
	Crv string `json:"crv"`
	X   string `json:"x"`
	Y   string `json:"y"`
}

type ecPrivateJWK struct {
	Kty string `json:"kty"`
	Crv string `json:"crv"`
	X   string `json:"x"`
	Y   string `json:"y"`
	D   string `json:"d"`
}

type dpopKeySetFile struct {
	Current  *ecPrivateJWK `json:"current"`
	Previous *ecPrivateJWK `json:"previous"`
}

type DeliveredCredential struct {
	Kind                   string `json:"kind"`
	AccessToken            string `json:"access_token,omitempty"`
	TokenType              string `json:"token_type,omitempty"`
	APIKey                 string `json:"api_key,omitempty"`
	Scope                  string `json:"scope,omitempty"`
	GrantID                string `json:"grant_id,omitempty"`
	ProviderID             string `json:"provider_id,omitempty"`
	ConnectorDigest        string `json:"connector_digest,omitempty"`
	Header                 string `json:"header,omitempty"`
	Prefix                 string `json:"prefix,omitempty"`
	CredentialVersion      int64  `json:"credential_version,omitempty"`
	ConnectionGeneration   string `json:"connection_generation,omitempty"`
	ProviderRevision       int64  `json:"provider_revision,omitempty"`
	TokenExpiresAtUnixMs   int64  `json:"token_expires_at_unix_ms,omitempty"`
	ConsentExpiresAtUnixMs int64  `json:"consent_expires_at_unix_ms,omitempty"`
}

type CredentialStatus struct {
	State        string `json:"state"`
	Version      int64  `json:"version"`
	AllowRefresh bool   `json:"allow_refresh"`
}

func NewGoauthyClient(cfg GoauthyConfig) (*GoauthyClient, error) {
	if strings.TrimSpace(cfg.DPoPKeyFile) == "" {
		return nil, errors.New("CONDUCTOR_GOAUTHY_DPOP_KEY_FILE is required")
	}
	key, err := loadDPoPKeyFile(cfg.DPoPKeyFile)
	if err != nil {
		return nil, fmt.Errorf("load goauthy DPoP key: %w", err)
	}
	return NewGoauthyClientWithKey(cfg, key)
}

func NewGoauthyClientWithKey(cfg GoauthyConfig, key *ecdsa.PrivateKey) (*GoauthyClient, error) {
	baseURL := strings.TrimRight(strings.TrimSpace(cfg.BaseURL), "/")
	if baseURL == "" {
		return nil, errors.New("CONDUCTOR_GOAUTHY_BASE_URL is required")
	}
	parsedBase, err := url.Parse(baseURL)
	if err != nil || parsedBase.Host == "" || parsedBase.User != nil {
		return nil, errors.New("CONDUCTOR_GOAUTHY_BASE_URL must be a valid URL")
	}
	if parsedBase.Scheme != "https" && !(cfg.AllowInsecureHTTP && parsedBase.Scheme == "http") {
		return nil, errors.New("CONDUCTOR_GOAUTHY_BASE_URL must use https")
	}

	issuer := strings.TrimRight(strings.TrimSpace(cfg.Issuer), "/")
	if issuer == "" {
		return nil, errors.New("CONDUCTOR_GOAUTHY_ISSUER is required")
	}
	parsedIssuer, err := url.Parse(issuer)
	if err != nil || parsedIssuer.Scheme != "https" || parsedIssuer.Host == "" || parsedIssuer.User != nil || parsedIssuer.RawQuery != "" || parsedIssuer.Fragment != "" {
		return nil, errors.New("CONDUCTOR_GOAUTHY_ISSUER must be an https origin URL")
	}

	if strings.TrimSpace(cfg.ClientID) == "" || strings.TrimSpace(cfg.ClientSecret) == "" {
		return nil, errors.New("CONDUCTOR_GOAUTHY_CLIENT_ID and CONDUCTOR_GOAUTHY_CLIENT_SECRET are required")
	}
	if strings.TrimSpace(cfg.Resource) == "" {
		return nil, errors.New("CONDUCTOR_GOAUTHY_RESOURCE is required")
	}
	if key == nil || key.Curve != elliptic.P256() {
		return nil, errors.New("goauthy DPoP key must be an ECDSA P-256 private key")
	}
	pubJWK, err := publicJWKFromKey(&key.PublicKey)
	if err != nil {
		return nil, err
	}
	httpClient := cfg.HTTPClient
	if httpClient == nil {
		httpClient = &http.Client{
			Timeout: 10 * time.Second,
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		}
	}
	return &GoauthyClient{
		baseURL:      baseURL,
		issuer:       issuer,
		clientID:     cfg.ClientID,
		clientSecret: cfg.ClientSecret,
		resource:     cfg.Resource,
		dpopKey:      key,
		jwk:          pubJWK,
		httpClient:   httpClient,
		now:          func() time.Time { return time.Now().UTC() },
	}, nil
}

func loadDPoPKeyFile(path string) (*ecdsa.PrivateKey, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var keySet dpopKeySetFile
	if err := json.Unmarshal(data, &keySet); err == nil && keySet.Current != nil {
		if keySet.Previous != nil {
			if _, err := parseECPrivateJWK(keySet.Previous); err != nil {
				return nil, fmt.Errorf("invalid previous DPoP key: %w", err)
			}
		}
		return parseECPrivateJWK(keySet.Current)
	}
	var single ecPrivateJWK
	if err := json.Unmarshal(data, &single); err != nil {
		return nil, fmt.Errorf("invalid DPoP JWK JSON: %w", err)
	}
	return parseECPrivateJWK(&single)
}

func parseECPrivateJWK(jwk *ecPrivateJWK) (*ecdsa.PrivateKey, error) {
	if jwk == nil || jwk.Kty != "EC" || jwk.Crv != "P-256" {
		return nil, errors.New("DPoP key must be EC P-256")
	}
	xb, err := base64.RawURLEncoding.DecodeString(jwk.X)
	if err != nil || len(xb) != 32 {
		return nil, errors.New("invalid EC P-256 x coordinate")
	}
	yb, err := base64.RawURLEncoding.DecodeString(jwk.Y)
	if err != nil || len(yb) != 32 {
		return nil, errors.New("invalid EC P-256 y coordinate")
	}
	db, err := base64.RawURLEncoding.DecodeString(jwk.D)
	if err != nil || len(db) != 32 {
		return nil, errors.New("invalid EC P-256 d coordinate")
	}
	priv := &ecdsa.PrivateKey{
		PublicKey: ecdsa.PublicKey{
			Curve: elliptic.P256(),
			X:     new(big.Int).SetBytes(xb),
			Y:     new(big.Int).SetBytes(yb),
		},
		D: new(big.Int).SetBytes(db),
	}
	if !priv.Curve.IsOnCurve(priv.X, priv.Y) {
		return nil, errors.New("EC P-256 public key is not on curve")
	}
	calcX, calcY := priv.Curve.ScalarBaseMult(db)
	if calcX.Cmp(priv.X) != 0 || calcY.Cmp(priv.Y) != 0 {
		return nil, errors.New("EC P-256 private key does not match public coordinates")
	}
	return priv, nil
}

func publicJWKFromKey(pub *ecdsa.PublicKey) (ecPublicJWK, error) {
	if pub == nil || pub.Curve != elliptic.P256() || pub.X == nil || pub.Y == nil {
		return ecPublicJWK{}, errors.New("invalid ECDSA P-256 public key")
	}
	var xb, yb [32]byte
	pub.X.FillBytes(xb[:])
	pub.Y.FillBytes(yb[:])
	return ecPublicJWK{
		Kty: "EC",
		Crv: "P-256",
		X:   base64.RawURLEncoding.EncodeToString(xb[:]),
		Y:   base64.RawURLEncoding.EncodeToString(yb[:]),
	}, nil
}

func (c *GoauthyClient) signDPoP(htm, htu, nonce, accessToken string) (string, error) {
	jtiBytes := make([]byte, 16)
	if _, err := rand.Read(jtiBytes); err != nil {
		return "", fmt.Errorf("generate DPoP jti: %w", err)
	}
	header := struct {
		Typ string      `json:"typ"`
		Alg string      `json:"alg"`
		JWK ecPublicJWK `json:"jwk"`
	}{
		Typ: "dpop+jwt",
		Alg: "ES256",
		JWK: c.jwk,
	}
	claims := struct {
		JTI   string `json:"jti"`
		HTM   string `json:"htm"`
		HTU   string `json:"htu"`
		IAT   int64  `json:"iat"`
		Nonce string `json:"nonce,omitempty"`
		ATH   string `json:"ath,omitempty"`
	}{
		JTI:   base64.RawURLEncoding.EncodeToString(jtiBytes),
		HTM:   htm,
		HTU:   htu,
		IAT:   c.now().Unix(),
		Nonce: nonce,
	}
	if accessToken != "" {
		sum := sha256.Sum256([]byte(accessToken))
		claims.ATH = base64.RawURLEncoding.EncodeToString(sum[:])
	}
	headerJSON, err := json.Marshal(header)
	if err != nil {
		return "", err
	}
	claimsJSON, err := json.Marshal(claims)
	if err != nil {
		return "", err
	}
	signingInput := base64.RawURLEncoding.EncodeToString(headerJSON) + "." + base64.RawURLEncoding.EncodeToString(claimsJSON)
	digest := sha256.Sum256([]byte(signingInput))
	r, s, err := ecdsa.Sign(rand.Reader, c.dpopKey, digest[:])
	if err != nil {
		return "", fmt.Errorf("sign DPoP proof: %w", err)
	}
	var sig [64]byte
	r.FillBytes(sig[:32])
	s.FillBytes(sig[32:])
	return signingInput + "." + base64.RawURLEncoding.EncodeToString(sig[:]), nil
}

// ExchangeToken exchanges a caller's human subject_token for a DPoP-bound
// goauthy.connections.use delegation token via RFC 8693 POST /oidc/token.
func (c *GoauthyClient) ExchangeToken(ctx context.Context, subjectToken string) ([]byte, time.Time, error) {
	if c == nil || c.dpopKey == nil {
		return nil, time.Time{}, &handlers.Error{Class: "goauthy_misconfigured", Message: "goauthy client is not configured"}
	}
	if strings.TrimSpace(subjectToken) == "" {
		return nil, time.Time{}, &handlers.Error{Class: "unauthorized", Message: "subject_token is required"}
	}
	endpoint := c.baseURL + "/oidc/token"
	htu := c.issuer + "/oidc/token"
	form := url.Values{
		"grant_type":           {"urn:ietf:params:oauth:grant-type:token-exchange"},
		"subject_token":        {subjectToken},
		"subject_token_type":   {"urn:ietf:params:oauth:token-type:access_token"},
		"requested_token_type": {"urn:ietf:params:oauth:token-type:access_token"},
		"scope":                {"goauthy.connections.use"},
		"resource":             {c.resource},
	}.Encode()

	nonce := ""
	for attempt := 0; attempt < 2; attempt++ {
		proof, err := c.signDPoP(http.MethodPost, htu, nonce, "")
		if err != nil {
			return nil, time.Time{}, &handlers.Error{Class: "goauthy_misconfigured", Message: err.Error()}
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(form))
		if err != nil {
			return nil, time.Time{}, &handlers.Error{Class: "goauthy_misconfigured", Message: "invalid token exchange request"}
		}
		req.SetBasicAuth(c.clientID, c.clientSecret)
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		req.Header.Set("DPoP", proof)

		resp, err := c.httpClient.Do(req)
		if err != nil {
			if ctx.Err() != nil {
				return nil, time.Time{}, ctx.Err()
			}
			return nil, time.Time{}, &handlers.Error{Class: "goauthy_unavailable", Message: "goauthy token exchange failed", Retryable: true}
		}
		body, readErr := io.ReadAll(io.LimitReader(resp.Body, (64<<10)+1))
		resp.Body.Close()
		if readErr != nil || len(body) > 64<<10 {
			return nil, time.Time{}, &handlers.Error{Class: "goauthy_unavailable", Message: "goauthy token exchange response invalid", Retryable: true}
		}
		if resp.StatusCode == http.StatusOK {
			var tokResp struct {
				AccessToken string `json:"access_token"`
				TokenType   string `json:"token_type"`
				ExpiresIn   int64  `json:"expires_in"`
			}
			if err := json.Unmarshal(body, &tokResp); err != nil || tokResp.AccessToken == "" || tokResp.ExpiresIn <= 0 {
				return nil, time.Time{}, &handlers.Error{Class: "goauthy_unavailable", Message: "invalid goauthy token exchange response", Retryable: true}
			}
			return []byte(tokResp.AccessToken), c.now().Add(time.Duration(tokResp.ExpiresIn) * time.Second), nil
		}

		var errResp struct {
			Error string `json:"error"`
		}
		_ = json.Unmarshal(body, &errResp)
		if resp.StatusCode == http.StatusBadRequest && errResp.Error == "use_dpop_nonce" {
			nextNonce := strings.TrimSpace(resp.Header.Get("DPoP-Nonce"))
			if attempt == 0 && nextNonce != "" {
				nonce = nextNonce
				continue
			}
			return nil, time.Time{}, &handlers.Error{Class: "goauthy_unavailable", Message: "goauthy DPoP nonce negotiation failed", Retryable: true}
		}
		return nil, time.Time{}, mapTokenExchangeError(resp.StatusCode, errResp.Error)
	}
	return nil, time.Time{}, &handlers.Error{Class: "goauthy_unavailable", Message: "goauthy token exchange failed", Retryable: true}
}

func mapTokenExchangeError(status int, oauthErr string) error {
	switch {
	case (status == http.StatusForbidden && oauthErr == "access_denied") || (status == http.StatusBadRequest && oauthErr == "invalid_grant"):
		return &handlers.Error{Class: "auth_expired", Message: fmt.Sprintf("goauthy token exchange rejected (%s)", oauthErr)}
	case status == http.StatusBadRequest && (oauthErr == "invalid_scope" || oauthErr == "unauthorized_client" || oauthErr == "invalid_target" || oauthErr == "invalid_dpop_proof" || oauthErr == "invalid_request"):
		return &handlers.Error{Class: "goauthy_misconfigured", Message: fmt.Sprintf("goauthy token exchange misconfigured (%s)", oauthErr)}
	case status == http.StatusUnauthorized && oauthErr == "invalid_client":
		return &handlers.Error{Class: "goauthy_misconfigured", Message: "goauthy client credentials rejected"}
	case status == http.StatusUnauthorized || status == http.StatusForbidden:
		return &handlers.Error{Class: "auth_expired", Message: fmt.Sprintf("goauthy token exchange unauthorized (HTTP %d)", status)}
	case status >= 500 || status == http.StatusTooManyRequests:
		return &handlers.Error{Class: "goauthy_unavailable", Message: fmt.Sprintf("goauthy temporary failure (HTTP %d)", status), Retryable: true}
	default:
		return &handlers.Error{Class: "goauthy_misconfigured", Message: fmt.Sprintf("goauthy unexpected token exchange status (HTTP %d)", status)}
	}
}

func validateGrantID(grantID string) error {
	if grantID == "" || grantID == "." || grantID == ".." || strings.ContainsAny(grantID, "/?#\r\n") {
		return &handlers.Error{Class: "validation_failed", Message: "invalid credential_ref"}
	}
	return nil
}

func isInvalidDPoPProof(resp *http.Response) bool {
	auth := resp.Header.Get("WWW-Authenticate")
	return strings.Contains(auth, "DPoP") && strings.Contains(auth, `error="invalid_dpop_proof"`)
}

// FetchCredential calls POST /auth/v1/connection-grants/{grant_id}/credential with DPoP ath + jti.
func (c *GoauthyClient) FetchCredential(ctx context.Context, grantID, delegationToken string) (DeliveredCredential, error) {
	if c == nil || c.dpopKey == nil {
		return DeliveredCredential{}, &handlers.Error{Class: "goauthy_misconfigured", Message: "goauthy client is not configured"}
	}
	if err := validateGrantID(grantID); err != nil {
		return DeliveredCredential{}, err
	}
	if delegationToken == "" {
		return DeliveredCredential{}, &handlers.Error{Class: "auth_expired", Message: "missing delegation token"}
	}
	path := "/auth/v1/connection-grants/" + url.PathEscape(grantID) + "/credential"
	proof, err := c.signDPoP(http.MethodPost, c.issuer+path, "", delegationToken)
	if err != nil {
		return DeliveredCredential{}, &handlers.Error{Class: "goauthy_misconfigured", Message: err.Error()}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+path, nil)
	if err != nil {
		return DeliveredCredential{}, &handlers.Error{Class: "goauthy_misconfigured", Message: "invalid goauthy credential request"}
	}
	req.Header.Set("Authorization", "DPoP "+delegationToken)
	req.Header.Set("DPoP", proof)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return DeliveredCredential{}, ctx.Err()
		}
		return DeliveredCredential{}, &handlers.Error{Class: "goauthy_unavailable", Message: "goauthy credential request failed", Retryable: true}
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK:
		body, err := io.ReadAll(io.LimitReader(resp.Body, (64<<10)+1))
		if err != nil || len(body) > 64<<10 {
			return DeliveredCredential{}, &handlers.Error{Class: "goauthy_unavailable", Message: "goauthy credential response exceeds size limit", Retryable: true}
		}
		var delivered DeliveredCredential
		if err := json.Unmarshal(body, &delivered); err != nil {
			return DeliveredCredential{}, &handlers.Error{Class: "goauthy_unavailable", Message: "invalid goauthy credential response", Retryable: true}
		}
		return delivered, nil
	case http.StatusNotFound:
		return DeliveredCredential{}, ErrCredentialNotFound
	case http.StatusConflict:
		return DeliveredCredential{}, &handlers.Error{Class: "credential_conflict", Message: "goauthy credential rotation conflict"}
	case http.StatusServiceUnavailable, http.StatusBadGateway, http.StatusGatewayTimeout, http.StatusTooManyRequests:
		return DeliveredCredential{}, &handlers.Error{Class: "goauthy_unavailable", Message: fmt.Sprintf("goauthy temporary failure (HTTP %d)", resp.StatusCode), Retryable: true}
	case http.StatusUnauthorized:
		if isInvalidDPoPProof(resp) {
			return DeliveredCredential{}, &handlers.Error{Class: "goauthy_misconfigured", Message: "goauthy rejected DPoP proof"}
		}
		return DeliveredCredential{}, &handlers.Error{Class: "auth_expired", Message: "goauthy rejected delegation token"}
	default:
		if resp.StatusCode >= 500 {
			return DeliveredCredential{}, &handlers.Error{Class: "goauthy_unavailable", Message: fmt.Sprintf("goauthy server error (HTTP %d)", resp.StatusCode), Retryable: true}
		}
		return DeliveredCredential{}, &handlers.Error{Class: "auth_expired", Message: fmt.Sprintf("goauthy rejected credential request (HTTP %d)", resp.StatusCode)}
	}
}

// ProbeCredentialStatus calls GET /auth/v1/connection-grants/{grant_id}/credential-status.
func (c *GoauthyClient) ProbeCredentialStatus(ctx context.Context, grantID, delegationToken string) (CredentialStatus, error) {
	if c == nil || c.dpopKey == nil {
		return CredentialStatus{}, &handlers.Error{Class: "goauthy_misconfigured", Message: "goauthy client is not configured"}
	}
	if err := validateGrantID(grantID); err != nil {
		return CredentialStatus{}, err
	}
	if delegationToken == "" {
		return CredentialStatus{}, &handlers.Error{Class: "auth_expired", Message: "missing delegation token"}
	}
	path := "/auth/v1/connection-grants/" + url.PathEscape(grantID) + "/credential-status"
	proof, err := c.signDPoP(http.MethodGet, c.issuer+path, "", delegationToken)
	if err != nil {
		return CredentialStatus{}, &handlers.Error{Class: "goauthy_misconfigured", Message: err.Error()}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+path, nil)
	if err != nil {
		return CredentialStatus{}, &handlers.Error{Class: "goauthy_misconfigured", Message: "invalid goauthy status request"}
	}
	req.Header.Set("Authorization", "DPoP "+delegationToken)
	req.Header.Set("DPoP", proof)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return CredentialStatus{}, ctx.Err()
		}
		return CredentialStatus{}, &handlers.Error{Class: "goauthy_unavailable", Message: "goauthy credential-status request failed", Retryable: true}
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		body, err := io.ReadAll(io.LimitReader(resp.Body, (16<<10)+1))
		if err != nil || len(body) > 16<<10 {
			return CredentialStatus{}, &handlers.Error{Class: "goauthy_unavailable", Message: "goauthy credential-status response invalid", Retryable: true}
		}
		var status CredentialStatus
		if err := json.Unmarshal(body, &status); err != nil {
			return CredentialStatus{}, &handlers.Error{Class: "goauthy_unavailable", Message: "invalid goauthy credential-status JSON", Retryable: true}
		}
		return status, nil
	}
	if resp.StatusCode == http.StatusServiceUnavailable || resp.StatusCode >= 500 || resp.StatusCode == http.StatusTooManyRequests {
		return CredentialStatus{}, &handlers.Error{Class: "goauthy_unavailable", Message: fmt.Sprintf("goauthy temporary failure (HTTP %d)", resp.StatusCode), Retryable: true}
	}
	if resp.StatusCode == http.StatusUnauthorized && isInvalidDPoPProof(resp) {
		return CredentialStatus{}, &handlers.Error{Class: "goauthy_misconfigured", Message: "goauthy rejected DPoP proof"}
	}
	return CredentialStatus{}, &handlers.Error{Class: "auth_expired", Message: fmt.Sprintf("goauthy credential-status rejected (HTTP %d)", resp.StatusCode)}
}

// RefreshCredential calls POST /auth/v1/connection-grants/{grant_id}/refresh once.
func (c *GoauthyClient) RefreshCredential(ctx context.Context, grantID, delegationToken string, version int64) error {
	if c == nil || c.dpopKey == nil {
		return &handlers.Error{Class: "goauthy_misconfigured", Message: "goauthy client is not configured"}
	}
	if err := validateGrantID(grantID); err != nil {
		return err
	}
	if delegationToken == "" {
		return &handlers.Error{Class: "auth_expired", Message: "missing delegation token"}
	}
	path := "/auth/v1/connection-grants/" + url.PathEscape(grantID) + "/refresh"
	proof, err := c.signDPoP(http.MethodPost, c.issuer+path, "", delegationToken)
	if err != nil {
		return &handlers.Error{Class: "goauthy_misconfigured", Message: err.Error()}
	}
	payload, err := json.Marshal(struct {
		CredentialVersion int64 `json:"credential_version"`
	}{CredentialVersion: version})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+path, bytes.NewReader(payload))
	if err != nil {
		return &handlers.Error{Class: "goauthy_misconfigured", Message: "invalid goauthy refresh request"}
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "DPoP "+delegationToken)
	req.Header.Set("DPoP", proof)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return &handlers.Error{Class: "goauthy_unavailable", Message: "goauthy refresh request failed", Retryable: true}
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK:
		return nil
	case http.StatusConflict:
		return &handlers.Error{Class: "credential_refreshing", Message: "goauthy credential is currently refreshing", Retryable: true}
	case http.StatusServiceUnavailable, http.StatusTooManyRequests:
		return &handlers.Error{Class: "goauthy_unavailable", Message: fmt.Sprintf("goauthy temporary failure (HTTP %d)", resp.StatusCode), Retryable: true}
	case http.StatusUnauthorized:
		if isInvalidDPoPProof(resp) {
			return &handlers.Error{Class: "goauthy_misconfigured", Message: "goauthy rejected DPoP proof"}
		}
		return &handlers.Error{Class: "auth_expired", Message: "goauthy rejected delegation token on refresh"}
	default:
		return &handlers.Error{Class: "auth_expired", Message: fmt.Sprintf("goauthy credential refresh failed (HTTP %d)", resp.StatusCode)}
	}
}

// GoauthyTokenSource implements common.TokenSourceWithContext using GoauthyClient.
type GoauthyTokenSource struct {
	mu              sync.Mutex
	client          *GoauthyClient
	grantID         string
	delegationToken string
	expiresAt       time.Time
	cached          *oauth2.Token
}

func NewGoauthyTokenSource(client *GoauthyClient, grantID, delegationToken string, expiresAt time.Time, initial *oauth2.Token) *GoauthyTokenSource {
	return &GoauthyTokenSource{
		client:          client,
		grantID:         grantID,
		delegationToken: delegationToken,
		expiresAt:       expiresAt,
		cached:          initial,
	}
}

func (s *GoauthyTokenSource) Token() (*oauth2.Token, error) {
	return s.TokenWithContext(context.Background())
}

func (s *GoauthyTokenSource) TokenWithContext(ctx context.Context) (*oauth2.Token, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cached != nil && s.cached.Valid() {
		return s.cached, nil
	}
	if s.client == nil {
		return nil, errors.New("goauthy client is required")
	}
	if !s.expiresAt.IsZero() && !s.client.now().Before(s.expiresAt) {
		return nil, &handlers.Error{Class: "auth_expired", Message: "credential handle has expired"}
	}
	delivered, err := s.client.FetchCredential(ctx, s.grantID, s.delegationToken)
	if err != nil {
		return nil, err
	}
	if delivered.Kind != "oauth2" {
		return nil, &handlers.Error{Class: "auth_mismatch", Message: "credential_ref is not an oauth2 credential"}
	}
	tokType := delivered.TokenType
	if tokType == "" {
		tokType = "Bearer"
	}
	var expiry time.Time
	if delivered.TokenExpiresAtUnixMs > 0 {
		expiry = time.UnixMilli(delivered.TokenExpiresAtUnixMs).UTC()
	}
	tok := &oauth2.Token{
		AccessToken: delivered.AccessToken,
		TokenType:   tokType,
		Expiry:      expiry,
	}
	s.cached = tok
	return tok, nil
}
