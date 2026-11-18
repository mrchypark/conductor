package main

import (
	"context"
	"crypto/subtle"
	"errors"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/amp-labs/connectors/providers"
	"github.com/mrchypark/conductor/internal/connections"
	ampconnectors "github.com/mrchypark/conductor/internal/connectors"
	"golang.org/x/oauth2"
)

func isOAuthCallbackPath(path string) bool {
	if path == "/v1/oauth/callback" {
		return true
	}
	return strings.HasPrefix(path, "/v1/oauth/") && strings.HasSuffix(path, "/callback") && strings.TrimSuffix(strings.TrimPrefix(path, "/v1/oauth/"), "/callback") != ""
}

func (s *server) oauthBegin(w http.ResponseWriter, r *http.Request) {
	if s.oauthStore == nil || s.catalog == nil {
		writeError(w, http.StatusServiceUnavailable, "oauth store unavailable")
		return
	}
	provider := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/v1/oauth/"), "/begin")
	info, err := s.catalog.Info(provider)
	if err != nil || info.Oauth2Opts == nil {
		writeError(w, http.StatusBadRequest, "provider does not support oauth")
		return
	}
	var input connections.BeginInput
	if err := decodeBody(w, r, &input); err != nil {
		writeError(w, http.StatusBadRequest, "invalid oauth request")
		return
	}
	if input.Provider == "" {
		input.Provider = provider
	} else if input.Provider != provider {
		writeError(w, http.StatusBadRequest, "oauth provider does not match request path")
		return
	}
	if input.AuthURLParams == nil {
		input.AuthURLParams = info.Oauth2Opts.AuthURLParams
	}
	if input.ScopeQueryParam == "" {
		input.ScopeQueryParam = info.Oauth2Opts.ScopeQueryParam
	}
	result, err := s.oauthStore.Begin(r.Context(), input, oauth2.Endpoint{AuthURL: info.Oauth2Opts.AuthURL, TokenURL: info.Oauth2Opts.TokenURL})
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *server) oauthComplete(w http.ResponseWriter, r *http.Request) {
	if s.oauthStore == nil || s.catalog == nil {
		writeError(w, http.StatusServiceUnavailable, "oauth store unavailable")
		return
	}
	var input oauthCallbackInput
	providerFromPath := ""
	if isOAuthCallbackPath(r.URL.Path) && r.URL.Path != "/v1/oauth/callback" {
		providerFromPath = strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/v1/oauth/"), "/callback")
	}
	if r.Method == http.MethodGet {
		query := r.URL.Query()
		input.Provider, input.ClientID, input.RedirectURL = query.Get("provider"), query.Get("client_id"), query.Get("redirect_url")
		input.AttemptID, input.State, input.Code, input.WorkspaceID = query.Get("attempt_id"), query.Get("state"), query.Get("code"), query.Get("workspace_id")
	} else if err := decodeBody(w, r, &input); err != nil {
		writeError(w, http.StatusBadRequest, "invalid oauth callback")
		return
	}
	if s.serviceToken != "" && subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), []byte("Bearer "+s.serviceToken)) != 1 {
		input.Metadata = nil
	}
	if input.Provider == "" {
		input.Provider = providerFromPath
	} else if providerFromPath != "" && input.Provider != providerFromPath {
		writeError(w, http.StatusBadRequest, "oauth provider does not match callback path")
		return
	}
	info, err := s.catalog.Info(input.Provider)
	if err != nil || info.Oauth2Opts == nil {
		writeError(w, http.StatusBadRequest, "provider does not support oauth")
		return
	}
	secret := oauthClientSecret(input.Provider)
	cfg := oauth2.Config{ClientID: input.ClientID, ClientSecret: secret, RedirectURL: input.RedirectURL, Endpoint: oauth2.Endpoint{AuthURL: info.Oauth2Opts.AuthURL, TokenURL: info.Oauth2Opts.TokenURL}}
	if _, err := s.oauthStore.Complete(r.Context(), cfg, connections.CompleteInput{AttemptID: input.AttemptID, Provider: input.Provider, State: input.State, Code: input.Code, WorkspaceID: input.WorkspaceID, Metadata: input.Metadata}); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "connected"})
}

type oauthCallbackInput struct {
	Provider    string            `json:"provider"`
	ClientID    string            `json:"client_id"`
	RedirectURL string            `json:"redirect_url"`
	AttemptID   string            `json:"attempt_id"`
	State       string            `json:"state"`
	Code        string            `json:"code"`
	WorkspaceID string            `json:"workspace_id"`
	Metadata    map[string]string `json:"metadata"`
}

type connectionRegisterRequest struct {
	ConnectionID string            `json:"connection_id"`
	WorkspaceID  string            `json:"workspace_id"`
	Provider     string            `json:"provider"`
	Scopes       []string          `json:"scopes,omitempty"`
	OAuthToken   string            `json:"oauth_access_token,omitempty"`
	APIKey       string            `json:"api_key,omitempty"`
	BasicUser    string            `json:"basic_user,omitempty"`
	BasicPass    string            `json:"basic_pass,omitempty"`
	Metadata     map[string]string `json:"metadata,omitempty"`
}

func (s *server) registerConnection(w http.ResponseWriter, r *http.Request) {
	if s.serviceToken == "" {
		writeError(w, http.StatusForbidden, "connection registration requires CONDUCTOR_SERVICE_TOKEN")
		return
	}
	if s.oauthStore == nil {
		writeError(w, http.StatusServiceUnavailable, "connection store unavailable")
		return
	}
	var input connectionRegisterRequest
	if err := decodeBody(w, r, &input); err != nil {
		writeError(w, http.StatusBadRequest, "invalid connection request")
		return
	}
	if input.ConnectionID == "" || input.WorkspaceID == "" || input.Provider == "" {
		writeError(w, http.StatusBadRequest, "connection_id, workspace_id, and provider are required")
		return
	}
	if s.catalog != nil {
		info, err := s.catalog.Info(input.Provider)
		if err != nil {
			writeError(w, http.StatusBadRequest, "unknown connector provider")
			return
		}
		if input.OAuthToken != "" && info.AuthType != providers.Oauth2 {
			writeError(w, http.StatusBadRequest, "oauth_access_token requires an OAuth2 connector provider")
			return
		}
	}
	if input.OAuthToken != "" && (input.APIKey != "" || input.BasicUser != "" || input.BasicPass != "") {
		writeError(w, http.StatusBadRequest, "oauth_access_token cannot be combined with API key or Basic credentials")
		return
	}
	metadata := make(map[string]string, len(input.Metadata)+3)
	for key, value := range input.Metadata {
		metadata[key] = value
	}
	if input.APIKey != "" {
		metadata["api_key"] = input.APIKey
	}
	if input.BasicUser != "" {
		metadata["basic_user"] = input.BasicUser
	}
	if input.BasicPass != "" {
		metadata["basic_pass"] = input.BasicPass
	}
	if len(metadata) == 0 && input.OAuthToken == "" {
		writeError(w, http.StatusBadRequest, "at least one encrypted credential value is required")
		return
	}
	var token *oauth2.Token
	if input.OAuthToken != "" {
		token = &oauth2.Token{AccessToken: input.OAuthToken, TokenType: "Bearer"}
	}
	if err := s.oauthStore.SaveConnection(r.Context(), input.ConnectionID, input.WorkspaceID, input.Provider, token, input.Scopes, metadata, time.Now().UTC()); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, map[string]string{"connection_id": input.ConnectionID, "workspace_id": input.WorkspaceID, "provider": input.Provider})
}

func connectorCredentialsResolver(connectionStore *connections.Store, catalog *ampconnectors.Catalog) ampconnectors.ScopedCredentialsResolver {
	return func(ctx context.Context, connectionID, provider, workspace string, revision int64) (ampconnectors.Credentials, error) {
		if connectionID == "" {
			if revision != 0 {
				return ampconnectors.Credentials{}, errors.New("connection is required for credential revision")
			}
			return ampconnectors.Credentials{}, nil
		}
		if connectionStore == nil {
			return ampconnectors.Credentials{}, errors.New("connection store is not configured")
		}
		connection, err := connectionStore.LoadConnectionAtRevision(ctx, connectionID, revision)
		if err != nil {
			return ampconnectors.Credentials{}, err
		}
		if connection.Provider != provider || connection.WorkspaceID != workspace {
			return ampconnectors.Credentials{}, errors.New("connection scope mismatch")
		}
		if revision > 0 && connection.Token != nil && !connection.Token.Valid() {
			return ampconnectors.Credentials{}, errors.New("reauthorization required: provider refresh continuity is not verified")
		}
		creds := ampconnectors.Credentials{OAuthToken: connection.Token, Metadata: connection.Metadata, Custom: connection.Metadata}
		creds.Validate = func(callCtx context.Context) error {
			return connectionStore.ValidateConnectionRevision(callCtx, connection.ID, connection.Revision)
		}
		if info, err := catalog.Info(provider); err == nil && info.Oauth2Opts != nil {
			creds.OAuthConfig = &oauth2.Config{ClientID: connection.Metadata["client_id"], ClientSecret: oauthClientSecret(provider), Endpoint: oauth2.Endpoint{AuthURL: info.Oauth2Opts.AuthURL, TokenURL: info.Oauth2Opts.TokenURL}, Scopes: connection.Scopes}
		}
		creds.APIKey = connection.Metadata["api_key"]
		creds.BasicUser = connection.Metadata["basic_user"]
		creds.BasicPass = connection.Metadata["basic_pass"]
		creds.TokenUpdated = func(_, token *oauth2.Token) error {
			if connection.Token != nil && token != nil && token.AccessToken == connection.Token.AccessToken && token.RefreshToken == connection.Token.RefreshToken && token.Expiry.Equal(connection.Token.Expiry) {
				return connectionStore.ValidateConnectionRevision(ctx, connection.ID, connection.Revision)
			}
			if revision > 0 {
				return errors.New("reauthorization required: provider refresh continuity is not verified")
			}
			return connectionStore.SaveTokenAtRevision(context.Background(), connection.ID, connection.WorkspaceID, connection.Provider, token, connection.Scopes, connection.Metadata, time.Now().UTC(), connection.Revision, connection.TokenVersion)
		}
		return creds, nil
	}
}

func oauthClientSecret(provider string) string {
	key := "CONDUCTOR_OAUTH_CLIENT_SECRET_" + strings.NewReplacer("-", "_", ".", "_", "/", "_").Replace(strings.ToUpper(provider))
	if secret := os.Getenv(key); secret != "" {
		return secret
	}
	return os.Getenv("CONDUCTOR_OAUTH_CLIENT_SECRET")
}
