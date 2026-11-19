package connectors

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/mrchypark/conductor/internal/handlers"
	"golang.org/x/oauth2"
)

// GoauthyClient exchanges opaque grant_id handles with GoAuthy over the internal network.
type GoauthyClient struct {
	BaseURL     string
	BearerToken string
	Client      *http.Client
}

type DeliveredCredential struct {
	Kind                 string `json:"kind"`
	AccessToken          string `json:"access_token,omitempty"`
	TokenType            string `json:"token_type,omitempty"`
	APIKey               string `json:"api_key,omitempty"`
	GrantID              string `json:"grant_id,omitempty"`
	ProviderID           string `json:"provider_id,omitempty"`
	ConnectionGeneration string `json:"connection_generation,omitempty"`
	CredentialVersion    int64  `json:"credential_version,omitempty"`
	ProviderRevision     int64  `json:"provider_revision,omitempty"`
	ExpiresAtUnixMS      int64  `json:"expires_at_unix_ms,omitempty"`
	Scope                string `json:"scope,omitempty"`
}

func (c *GoauthyClient) Exchange(ctx context.Context, grantID, expectedProvider string, expiresAt time.Time) (DeliveredCredential, error) {
	if c == nil || strings.TrimSpace(c.BaseURL) == "" {
		return DeliveredCredential{}, &handlers.Error{Class: "connection_error", Message: "goauthy client is not configured"}
	}
	if grantID == "" || grantID == "." || grantID == ".." || strings.ContainsAny(grantID, "/?#\r\n") {
		return DeliveredCredential{}, &handlers.Error{Class: "validation_failed", Message: "invalid credential_ref"}
	}
	if !expiresAt.IsZero() && !time.Now().UTC().Before(expiresAt) {
		return DeliveredCredential{}, &handlers.Error{Class: "auth_expired", Message: "credential handle has expired"}
	}
	endpoint := strings.TrimRight(c.BaseURL, "/") + "/auth/v1/connection-grants/" + url.PathEscape(grantID) + "/credential"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, nil)
	if err != nil {
		return DeliveredCredential{}, &handlers.Error{Class: "connection_error", Message: "invalid goauthy request"}
	}
	if c.BearerToken != "" {
		req.Header.Set("Authorization", "Bearer "+c.BearerToken)
	}
	client := c.Client
	if client == nil {
		client = &http.Client{
			Timeout: 10 * time.Second,
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		}
	}
	resp, err := client.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return DeliveredCredential{}, ctx.Err()
		}
		return DeliveredCredential{}, &handlers.Error{Class: "retryable", Message: "goauthy exchange request failed", Retryable: true}
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden || resp.StatusCode == http.StatusNotFound || resp.StatusCode == http.StatusGone {
		return DeliveredCredential{}, &handlers.Error{Class: "auth_expired", Message: fmt.Sprintf("goauthy rejected credential_ref (HTTP %d)", resp.StatusCode)}
	}
	if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500 {
		return DeliveredCredential{}, &handlers.Error{Class: "retryable", Message: fmt.Sprintf("goauthy temporary failure (HTTP %d)", resp.StatusCode), Retryable: true}
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return DeliveredCredential{}, &handlers.Error{Class: "connection_error", Message: fmt.Sprintf("goauthy unexpected status (HTTP %d)", resp.StatusCode)}
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, (64<<10)+1))
	if err != nil || len(body) > 64<<10 {
		return DeliveredCredential{}, &handlers.Error{Class: "connection_error", Message: "goauthy response exceeds size limit"}
	}
	var delivered DeliveredCredential
	if err := json.Unmarshal(body, &delivered); err != nil {
		return DeliveredCredential{}, &handlers.Error{Class: "connection_error", Message: "invalid goauthy credential response"}
	}
	if expectedProvider != "" && delivered.ProviderID != "" && !strings.EqualFold(delivered.ProviderID, expectedProvider) {
		return DeliveredCredential{}, &handlers.Error{Class: "permission_denied", Message: "credential_ref provider mismatch"}
	}
	if delivered.Kind == "oauth2" && delivered.AccessToken == "" {
		return DeliveredCredential{}, &handlers.Error{Class: "connection_error", Message: "goauthy oauth2 response missing access_token"}
	}
	if delivered.Kind == "api_key" && delivered.APIKey == "" {
		return DeliveredCredential{}, &handlers.Error{Class: "connection_error", Message: "goauthy api_key response missing api_key"}
	}
	return delivered, nil
}

// GoauthyTokenSource implements common.TokenSourceWithContext using GoauthyClient.
type GoauthyTokenSource struct {
	mu               sync.Mutex
	client           *GoauthyClient
	grantID          string
	expectedProvider string
	expiresAt        time.Time
	cached           *oauth2.Token
}

func NewGoauthyTokenSource(client *GoauthyClient, grantID, expectedProvider string, expiresAt time.Time, initial *oauth2.Token) *GoauthyTokenSource {
	return &GoauthyTokenSource{
		client:           client,
		grantID:          grantID,
		expectedProvider: expectedProvider,
		expiresAt:        expiresAt,
		cached:           initial,
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
	delivered, err := s.client.Exchange(ctx, s.grantID, s.expectedProvider, s.expiresAt)
	if err != nil {
		return nil, err
	}
	if delivered.Kind != "oauth2" {
		return nil, &handlers.Error{Class: "connection_error", Message: "credential_ref is not an oauth2 credential"}
	}
	tokType := delivered.TokenType
	if tokType == "" {
		tokType = "Bearer"
	}
	var expiry time.Time
	if delivered.ExpiresAtUnixMS > 0 {
		expiry = time.UnixMilli(delivered.ExpiresAtUnixMS).UTC()
	}
	tok := &oauth2.Token{
		AccessToken: delivered.AccessToken,
		TokenType:   tokType,
		Expiry:      expiry,
	}
	s.cached = tok
	return tok, nil
}
