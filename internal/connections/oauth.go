// Package connections owns OAuth state and encrypted provider tokens.
package connections

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strings"
	"sync"
	"time"

	"golang.org/x/oauth2"
)

var (
	ErrInvalidState = errors.New("invalid oauth state")
	ErrExpiredState = errors.New("expired oauth state")
)

type Store struct {
	db        *sql.DB
	encryptor *Encryptor
}

type Connection struct {
	ID          string
	WorkspaceID string
	Provider    string
	Token       *oauth2.Token
	Scopes      []string
	Metadata    map[string]string
}

func NewStore(db *sql.DB, masterKey []byte) (*Store, error) {
	if db == nil {
		return nil, errors.New("database is required")
	}
	encryptor, err := NewEncryptor(masterKey)
	if err != nil {
		return nil, err
	}
	s := &Store{db: db, encryptor: encryptor}
	if err := s.ensureSchema(context.Background()); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Store) ensureSchema(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, `
CREATE TABLE IF NOT EXISTS oauth_attempts (
    attempt_id TEXT PRIMARY KEY,
    provider TEXT NOT NULL,
    connection_id TEXT NOT NULL,
    workspace_id TEXT NOT NULL,
    state_hash BLOB NOT NULL UNIQUE,
    pkce_verifier TEXT NOT NULL,
    redirect_uri TEXT NOT NULL,
    client_id TEXT NOT NULL,
    expires_at INTEGER NOT NULL,
    scopes_json TEXT NOT NULL,
    created_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_oauth_attempts_expiry ON oauth_attempts(expires_at);
CREATE TABLE IF NOT EXISTS connections (
    connection_id TEXT PRIMARY KEY,
    workspace_id TEXT NOT NULL,
    provider TEXT NOT NULL,
    token_ciphertext BLOB NOT NULL,
    scopes_json TEXT NOT NULL,
    metadata_json TEXT NOT NULL,
    expires_at INTEGER,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);`)
	return err
}

type BeginInput struct {
	AttemptID       string            `json:"attempt_id"`
	Provider        string            `json:"provider"`
	ConnectionID    string            `json:"connection_id"`
	WorkspaceID     string            `json:"workspace_id"`
	RedirectURL     string            `json:"redirect_url"`
	ClientID        string            `json:"client_id"`
	Scopes          []string          `json:"scopes"`
	AuthURLParams   map[string]string `json:"auth_url_params,omitempty"`
	ScopeQueryParam string            `json:"scope_query_param,omitempty"`
	TTL             time.Duration     `json:"ttl"`
	Now             time.Time         `json:"now"`
}

type BeginResult struct {
	AttemptID string    `json:"attempt_id"`
	State     string    `json:"state"`
	AuthURL   string    `json:"auth_url"`
	ExpiresAt time.Time `json:"expires_at"`
}

func (s *Store) Begin(ctx context.Context, input BeginInput, endpoint oauth2.Endpoint) (BeginResult, error) {
	if input.AttemptID == "" || input.Provider == "" || input.ConnectionID == "" || input.WorkspaceID == "" || input.RedirectURL == "" || input.ClientID == "" {
		return BeginResult{}, errors.New("attempt, provider, connection, redirect URL, and client ID are required")
	}
	if endpoint.AuthURL == "" || endpoint.TokenURL == "" {
		return BeginResult{}, errors.New("oauth authorization and token endpoints are required")
	}
	if input.TTL <= 0 {
		input.TTL = 10 * time.Minute
	}
	if input.Now.IsZero() {
		input.Now = time.Now().UTC()
	}
	if oauthSecurityParam(input.ScopeQueryParam) {
		return BeginResult{}, errors.New("scope query parameter conflicts with OAuth security parameters")
	}
	state, err := randomOpaque(32)
	if err != nil {
		return BeginResult{}, err
	}
	verifier := oauth2.GenerateVerifier()
	stateHash := hashState(state)
	expiresAt := input.Now.Add(input.TTL)
	scopes, err := json.Marshal(input.Scopes)
	if err != nil {
		return BeginResult{}, err
	}
	_, err = s.db.ExecContext(ctx, `INSERT INTO oauth_attempts
        (attempt_id, provider, connection_id, workspace_id, state_hash, pkce_verifier, redirect_uri, client_id, expires_at, scopes_json, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, input.AttemptID, input.Provider, input.ConnectionID, input.WorkspaceID,
		stateHash[:], verifier, input.RedirectURL, input.ClientID, expiresAt.Unix(), scopes, input.Now.Unix())
	if err != nil {
		return BeginResult{}, fmt.Errorf("save oauth attempt: %w", err)
	}
	cfg := oauth2.Config{ClientID: input.ClientID, Endpoint: endpoint, RedirectURL: input.RedirectURL, Scopes: input.Scopes}
	authURL := cfg.AuthCodeURL(state, oauth2.S256ChallengeOption(verifier))
	if len(input.AuthURLParams) > 0 || input.ScopeQueryParam != "" {
		parsed, _ := url.Parse(authURL)
		query := parsed.Query()
		if input.ScopeQueryParam != "" {
			query.Del("scope")
			query.Set(input.ScopeQueryParam, strings.Join(input.Scopes, " "))
		}
		for key, value := range input.AuthURLParams {
			if oauthSecurityParam(key) {
				continue
			}
			query.Set(key, value)
		}
		parsed.RawQuery = query.Encode()
		authURL = parsed.String()
	}
	return BeginResult{
		AttemptID: input.AttemptID,
		State:     state,
		AuthURL:   authURL,
		ExpiresAt: expiresAt,
	}, nil
}

type CompleteInput struct {
	AttemptID   string
	Provider    string
	State       string
	Code        string
	WorkspaceID string
	Metadata    map[string]string
	Now         time.Time
}

func (s *Store) Complete(ctx context.Context, cfg oauth2.Config, input CompleteInput) (*oauth2.Token, error) {
	if input.State == "" || input.Code == "" {
		return nil, ErrInvalidState
	}
	if input.Now.IsZero() {
		input.Now = time.Now().UTC()
	}
	want := hashState(input.State)
	if input.AttemptID == "" {
		if err := s.db.QueryRowContext(ctx, `SELECT attempt_id FROM oauth_attempts WHERE state_hash = ?`, want[:]).Scan(&input.AttemptID); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return nil, ErrInvalidState
			}
			return nil, fmt.Errorf("load oauth attempt by state: %w", err)
		}
	}
	var provider, connectionID, workspaceID, verifier, redirectURL, clientID, scopesJSON string
	var stateHash []byte
	var expiresAt int64
	err := s.db.QueryRowContext(ctx, `SELECT provider, connection_id, workspace_id, state_hash, pkce_verifier, redirect_uri, client_id, expires_at, scopes_json
        FROM oauth_attempts WHERE attempt_id = ?`, input.AttemptID).
		Scan(&provider, &connectionID, &workspaceID, &stateHash, &verifier, &redirectURL, &clientID, &expiresAt, &scopesJSON)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrInvalidState
	}
	if err != nil {
		return nil, fmt.Errorf("load oauth attempt: %w", err)
	}
	if input.Now.Unix() >= expiresAt {
		_, _ = s.db.ExecContext(ctx, `DELETE FROM oauth_attempts WHERE attempt_id = ?`, input.AttemptID)
		return nil, ErrExpiredState
	}
	if input.WorkspaceID != "" && input.WorkspaceID != workspaceID {
		return nil, ErrInvalidState
	}
	if input.Provider != "" && input.Provider != provider {
		return nil, ErrInvalidState
	}
	input.WorkspaceID = workspaceID
	if (cfg.ClientID != "" && cfg.ClientID != clientID) || (cfg.RedirectURL != "" && cfg.RedirectURL != redirectURL) {
		return nil, ErrInvalidState
	}
	cfg.ClientID = clientID
	cfg.RedirectURL = redirectURL
	var scopes []string
	if err := json.Unmarshal([]byte(scopesJSON), &scopes); err != nil {
		return nil, ErrInvalidState
	}
	cfg.Scopes = scopes
	if len(stateHash) != len(want) || subtle.ConstantTimeCompare(stateHash, want[:]) != 1 {
		return nil, ErrInvalidState
	}
	// Consume state before exchanging the code: callbacks are one-time even if
	// the provider or network fails after validation.
	result, err := s.db.ExecContext(ctx, `DELETE FROM oauth_attempts WHERE attempt_id = ? AND state_hash = ?`, input.AttemptID, stateHash)
	if err != nil {
		return nil, fmt.Errorf("consume oauth attempt: %w", err)
	}
	if affected, _ := result.RowsAffected(); affected != 1 {
		return nil, ErrInvalidState
	}
	token, err := cfg.Exchange(ctx, input.Code, oauth2.VerifierOption(verifier))
	if err != nil {
		return nil, fmt.Errorf("exchange oauth code for %s: %w", provider, err)
	}
	metadata := make(map[string]string, len(input.Metadata)+1)
	for key, value := range input.Metadata {
		metadata[key] = value
	}
	metadata["client_id"] = clientID
	if err := s.SaveToken(ctx, connectionID, input.WorkspaceID, provider, token, cfg.Scopes, metadata, input.Now); err != nil {
		return nil, err
	}
	return token, nil
}

func (s *Store) SaveToken(ctx context.Context, connectionID, workspaceID, provider string, token *oauth2.Token, scopes []string, metadata map[string]string, now time.Time) error {
	return s.SaveConnection(ctx, connectionID, workspaceID, provider, token, scopes, metadata, now)
}

// SaveConnection persists either an OAuth token or non-OAuth credentials in
// the same encrypted connection record. Metadata is encrypted because API
// keys, basic credentials, and provider-specific custom values live there.
func (s *Store) SaveConnection(ctx context.Context, connectionID, workspaceID, provider string, token *oauth2.Token, scopes []string, metadata map[string]string, now time.Time) error {
	if connectionID == "" || workspaceID == "" || provider == "" {
		return errors.New("connection, workspace, and provider are required")
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	payload := []byte("null")
	var err error
	if token != nil {
		payload, err = json.Marshal(token)
	}
	if err != nil {
		return fmt.Errorf("marshal connection token: %w", err)
	}
	ciphertext, err := s.encryptor.Seal(payload)
	if err != nil {
		return err
	}
	scopesJSON, err := json.Marshal(scopes)
	if err != nil {
		return err
	}
	metadataJSON, err := json.Marshal(metadata)
	if err != nil {
		return err
	}
	metadataCiphertext, err := s.encryptor.Seal(metadataJSON)
	if err != nil {
		return err
	}
	var expiry any
	if token != nil && !token.Expiry.IsZero() {
		expiry = token.Expiry.Unix()
	}
	_, err = s.db.ExecContext(ctx, `INSERT INTO connections
        (connection_id, workspace_id, provider, token_ciphertext, scopes_json, metadata_json, expires_at, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(connection_id) DO UPDATE SET workspace_id=excluded.workspace_id,
        provider=excluded.provider, token_ciphertext=excluded.token_ciphertext, scopes_json=excluded.scopes_json,
        metadata_json=excluded.metadata_json, expires_at=excluded.expires_at, updated_at=excluded.updated_at`,
		connectionID, workspaceID, provider, ciphertext, scopesJSON, base64.RawStdEncoding.EncodeToString(metadataCiphertext), expiry, now.Unix(), now.Unix())
	return err
}

func (s *Store) LoadToken(ctx context.Context, connectionID string) (*oauth2.Token, error) {
	var ciphertext []byte
	if err := s.db.QueryRowContext(ctx, `SELECT token_ciphertext FROM connections WHERE connection_id = ?`, connectionID).Scan(&ciphertext); err != nil {
		return nil, err
	}
	plaintext, err := s.encryptor.Open(ciphertext)
	if err != nil {
		return nil, err
	}
	return decodeToken(plaintext)
}

func (s *Store) LoadConnection(ctx context.Context, connectionID string) (Connection, error) {
	var c Connection
	var scopesJSON, metadataJSON string
	var ciphertext []byte
	if err := s.db.QueryRowContext(ctx, `SELECT connection_id, workspace_id, provider, token_ciphertext, scopes_json, metadata_json FROM connections WHERE connection_id=?`, connectionID).Scan(&c.ID, &c.WorkspaceID, &c.Provider, &ciphertext, &scopesJSON, &metadataJSON); err != nil {
		return Connection{}, err
	}
	plain, err := s.encryptor.Open(ciphertext)
	if err != nil {
		return Connection{}, err
	}
	c.Token, err = decodeToken(plain)
	if err != nil {
		return Connection{}, err
	}
	if err := json.Unmarshal([]byte(scopesJSON), &c.Scopes); err != nil {
		return Connection{}, err
	}
	decoded, err := base64.RawStdEncoding.DecodeString(metadataJSON)
	if err != nil {
		return Connection{}, errors.New("connection metadata is not encrypted")
	}
	metadataBytes, err := s.encryptor.Open(decoded)
	if err != nil {
		return Connection{}, errors.New("connection metadata cannot be decrypted")
	}
	if err := json.Unmarshal(metadataBytes, &c.Metadata); err != nil {
		return Connection{}, err
	}
	return c, nil
}

func decodeToken(plaintext []byte) (*oauth2.Token, error) {
	if string(plaintext) == "null" || len(plaintext) == 0 {
		return nil, nil
	}
	var token oauth2.Token
	if err := json.Unmarshal(plaintext, &token); err != nil {
		return nil, fmt.Errorf("unmarshal oauth token: %w", err)
	}
	return &token, nil
}

func (s *Store) TokenSource(ctx context.Context, connectionID string, cfg oauth2.Config) (oauth2.TokenSource, error) {
	token, err := s.LoadToken(ctx, connectionID)
	if err != nil {
		return nil, err
	}
	if token == nil {
		return nil, errors.New("connection has no OAuth token")
	}
	return &persistingTokenSource{base: cfg.TokenSource(ctx, token), current: token, store: s, connectionID: connectionID}, nil
}

type persistingTokenSource struct {
	mu           sync.Mutex
	base         oauth2.TokenSource
	current      *oauth2.Token
	store        *Store
	connectionID string
}

func (s *persistingTokenSource) Token() (*oauth2.Token, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	token, err := s.base.Token()
	if err != nil {
		return nil, err
	}
	if token.AccessToken != s.current.AccessToken || token.RefreshToken != s.current.RefreshToken || !token.Expiry.Equal(s.current.Expiry) {
		if err := s.store.updateToken(context.Background(), s.connectionID, token); err != nil {
			return nil, err
		}
		s.current = token
	}
	return token, nil
}

func (s *Store) updateToken(ctx context.Context, connectionID string, token *oauth2.Token) error {
	payload, err := json.Marshal(token)
	if err != nil {
		return err
	}
	ciphertext, err := s.encryptor.Seal(payload)
	if err != nil {
		return err
	}
	var expiry any
	if !token.Expiry.IsZero() {
		expiry = token.Expiry.Unix()
	}
	_, err = s.db.ExecContext(ctx, `UPDATE connections SET token_ciphertext = ?, expires_at = ?, updated_at = ? WHERE connection_id = ?`, ciphertext, expiry, time.Now().UTC().Unix(), connectionID)
	return err
}

type Encryptor struct{ aead cipher.AEAD }

func NewEncryptor(key []byte) (*Encryptor, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("master key must be 16, 24, or 32 bytes: %w", err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	return &Encryptor{aead: aead}, nil
}

func (e *Encryptor) Seal(plaintext []byte) ([]byte, error) {
	nonce := make([]byte, e.aead.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, err
	}
	return e.aead.Seal(nonce, nonce, plaintext, nil), nil
}

func (e *Encryptor) Open(ciphertext []byte) ([]byte, error) {
	if len(ciphertext) < e.aead.NonceSize() {
		return nil, errors.New("invalid encrypted token")
	}
	nonce, data := ciphertext[:e.aead.NonceSize()], ciphertext[e.aead.NonceSize():]
	return e.aead.Open(nil, nonce, data, nil)
}

func randomOpaque(size int) (string, error) {
	b := make([]byte, size)
	if _, err := io.ReadFull(rand.Reader, b); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}

func hashState(state string) [sha256.Size]byte { return sha256.Sum256([]byte(state)) }

func oauthSecurityParam(key string) bool {
	switch strings.ToLower(key) {
	case "state", "code_challenge", "code_challenge_method", "redirect_uri", "client_id", "response_type":
		return true
	default:
		return false
	}
}
