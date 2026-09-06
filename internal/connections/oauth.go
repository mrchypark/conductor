// Package connections owns OAuth state and encrypted provider tokens.
package connections

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/mrchypark/conductor/internal/persistence"
	"golang.org/x/oauth2"
)

var (
	ErrInvalidState       = errors.New("invalid oauth state")
	ErrExpiredState       = errors.New("expired oauth state")
	ErrConnectionNotFound = errors.New("connection not found")
	ErrConnectionRevoked  = errors.New("connection revoked")
	ErrConnectionRevision = errors.New("connection revision mismatch")
)

type Store struct {
	db        *persistence.DB
	encryptor *Encryptor
}

type Connection struct {
	ID           string
	WorkspaceID  string
	Provider     string
	Revision     int64
	TokenVersion int64
	Token        *oauth2.Token
	Scopes       []string
	Metadata     map[string]string
}

func NewStore(ctx context.Context, db *persistence.DB, masterKey []byte) (*Store, error) {
	if db == nil {
		return nil, errors.New("database is required")
	}
	encryptor, err := NewEncryptor(masterKey)
	if err != nil {
		return nil, err
	}
	s := &Store{db: db, encryptor: encryptor}
	if err := s.ensureSchema(ctx); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Store) ensureSchema(ctx context.Context) error {
	_, err := s.db.Mutate(ctx, "initialize oauth schema",
		persistence.Statement{SQL: `CREATE TABLE IF NOT EXISTS oauth_attempts (
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
)`},
		persistence.Statement{SQL: `CREATE INDEX IF NOT EXISTS idx_oauth_attempts_expiry ON oauth_attempts(expires_at)`},
		persistence.Statement{SQL: `CREATE TABLE IF NOT EXISTS oauth_consumptions (
    attempt_id TEXT PRIMARY KEY,
    claim_id TEXT NOT NULL,
    state_hash BLOB NOT NULL,
    consumed_at INTEGER NOT NULL
)`},
		persistence.Statement{SQL: `CREATE TABLE IF NOT EXISTS connections (
    connection_id TEXT PRIMARY KEY,
    workspace_id TEXT NOT NULL,
    provider TEXT NOT NULL,
    token_ciphertext BLOB NOT NULL,
    scopes_json TEXT NOT NULL,
    metadata_json TEXT NOT NULL,
    expires_at INTEGER,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
)`},
		persistence.Statement{SQL: `CREATE TABLE IF NOT EXISTS connection_versions (
    connection_id TEXT PRIMARY KEY,
    revision INTEGER NOT NULL,
	 token_version INTEGER NOT NULL DEFAULT 1,
    revoked INTEGER NOT NULL DEFAULT 0,
    write_id TEXT NOT NULL DEFAULT ''
)`},
		persistence.Statement{SQL: `INSERT OR IGNORE INTO connection_versions (connection_id, revision, token_version, revoked, write_id)
    SELECT connection_id, 1, 1, 0, '' FROM connections`})
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
	_, err = s.db.Mutate(ctx, "begin oauth "+input.AttemptID, persistence.Statement{SQL: `INSERT INTO oauth_attempts
        (attempt_id, provider, connection_id, workspace_id, state_hash, pkce_verifier, redirect_uri, client_id, expires_at, scopes_json, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, Args: []any{input.AttemptID, input.Provider, input.ConnectionID, input.WorkspaceID,
		base64.StdEncoding.EncodeToString(stateHash[:]), verifier, input.RedirectURL, input.ClientID, expiresAt.Unix(), string(scopes), input.Now.Unix()}})
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
		rows, err := s.db.Query(ctx, `SELECT attempt_id FROM oauth_attempts WHERE state_hash = ?`, base64.StdEncoding.EncodeToString(want[:]))
		if err != nil {
			return nil, fmt.Errorf("load oauth attempt by state: %w", err)
		}
		if len(rows) == 0 {
			return nil, ErrInvalidState
		}
		input.AttemptID = oauthString(rows[0][0])
	}
	rows, err := s.db.Query(ctx, `SELECT provider, connection_id, workspace_id, state_hash, pkce_verifier, redirect_uri, client_id, expires_at, scopes_json FROM oauth_attempts WHERE attempt_id = ?`, input.AttemptID)
	if err != nil {
		return nil, fmt.Errorf("load oauth attempt: %w", err)
	}
	if len(rows) == 0 {
		return nil, ErrInvalidState
	}
	row := rows[0]
	provider, connectionID, workspaceID := oauthString(row[0]), oauthString(row[1]), oauthString(row[2])
	storedStateHash := oauthString(row[3])
	stateHash := oauthBytes(storedStateHash)
	verifier, redirectURL, clientID := oauthString(row[4]), oauthString(row[5]), oauthString(row[6])
	expiresAt, scopesJSON := oauthInt64(row[7]), oauthString(row[8])
	if input.Now.Unix() >= expiresAt {
		_, _ = s.db.Mutate(ctx, "expire oauth "+input.AttemptID, persistence.Statement{SQL: `DELETE FROM oauth_attempts WHERE attempt_id = ? AND expires_at <= ?`, Args: []any{input.AttemptID, input.Now.Unix()}})
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
	claimID, err := oauthClaimID()
	if err != nil {
		return nil, err
	}
	_, err = s.db.Mutate(ctx, "consume oauth "+input.AttemptID+" "+claimID,
		persistence.Statement{SQL: `INSERT OR IGNORE INTO oauth_consumptions(attempt_id,claim_id,state_hash,consumed_at) SELECT attempt_id,?,?,? FROM oauth_attempts WHERE attempt_id=? AND state_hash=? AND expires_at>?`, Args: []any{claimID, storedStateHash, input.Now.Unix(), input.AttemptID, storedStateHash, input.Now.Unix()}},
		persistence.Statement{SQL: `DELETE FROM oauth_attempts WHERE attempt_id=? AND state_hash=? AND EXISTS(SELECT 1 FROM oauth_consumptions WHERE attempt_id=? AND claim_id=?)`, Args: []any{input.AttemptID, storedStateHash, input.AttemptID, claimID}})
	if err != nil {
		return nil, fmt.Errorf("consume oauth attempt: %w", err)
	}
	consumed, err := s.db.Query(ctx, `SELECT claim_id FROM oauth_consumptions WHERE attempt_id=?`, input.AttemptID)
	if err != nil {
		return nil, fmt.Errorf("verify oauth consumption: %w", err)
	}
	if len(consumed) != 1 || oauthString(consumed[0][0]) != claimID {
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

// SaveTokenAtRevision stores a verified refresh without changing the logical
// connection revision. Callers must have verified the provider identity and
// granted scopes remain valid for this connection before calling it.
func (s *Store) SaveTokenAtRevision(ctx context.Context, connectionID, workspaceID, provider string, token *oauth2.Token, scopes []string, metadata map[string]string, now time.Time, expectedRevision, expectedTokenVersion int64) error {
	if expectedRevision < 1 || expectedTokenVersion < 1 {
		return ErrConnectionRevision
	}
	return s.saveTokenAtRevision(ctx, connectionID, workspaceID, provider, token, scopes, metadata, now, expectedRevision, expectedTokenVersion)
}

// SaveConnection persists either an OAuth token or non-OAuth credentials in
// the same encrypted connection record. Metadata is encrypted because API
// keys, basic credentials, and provider-specific custom values live there.
func (s *Store) SaveConnection(ctx context.Context, connectionID, workspaceID, provider string, token *oauth2.Token, scopes []string, metadata map[string]string, now time.Time) error {
	if connectionID == "" || workspaceID == "" || provider == "" {
		return errors.New("connection, workspace, and provider are required")
	}
	revision, tokenVersion, revoked, err := s.connectionVersion(ctx, connectionID)
	if err != nil && !errors.Is(err, ErrConnectionNotFound) {
		return err
	}
	if revoked {
		return ErrConnectionRevoked
	}
	return s.saveConnection(ctx, connectionID, workspaceID, provider, token, scopes, metadata, now, revision, tokenVersion)
}

func (s *Store) LoadToken(ctx context.Context, connectionID string) (*oauth2.Token, error) {
	connection, err := s.LoadConnection(ctx, connectionID)
	if err != nil {
		return nil, err
	}
	return connection.Token, nil
}

func (s *Store) LoadConnection(ctx context.Context, connectionID string) (Connection, error) {
	return s.loadConnection(ctx, connectionID, 0)
}

// LoadConnectionAtRevision returns credentials only when the caller still
// holds the current logical connection revision. Revision zero is retained for
// legacy callers and still rejects revoked connections.
func (s *Store) LoadConnectionAtRevision(ctx context.Context, connectionID string, revision int64) (Connection, error) {
	return s.loadConnection(ctx, connectionID, revision)
}

// ValidateConnectionRevision checks a connection fence without decrypting its
// credentials. Revision zero is retained for legacy callers.
func (s *Store) ValidateConnectionRevision(ctx context.Context, connectionID string, revision int64) error {
	current, _, revoked, err := s.connectionVersion(ctx, connectionID)
	if err != nil {
		return err
	}
	if revoked {
		return ErrConnectionRevoked
	}
	if revision != 0 && revision != current {
		return ErrConnectionRevision
	}
	return nil
}

func (s *Store) loadConnection(ctx context.Context, connectionID string, expectedRevision int64) (Connection, error) {
	rows, err := s.db.Query(ctx, `SELECT c.connection_id,c.workspace_id,c.provider,c.token_ciphertext,c.scopes_json,c.metadata_json,v.revision,v.token_version,v.revoked
        FROM connections c JOIN connection_versions v ON v.connection_id=c.connection_id WHERE c.connection_id=?`, connectionID)
	if err != nil {
		return Connection{}, err
	}
	if len(rows) == 0 {
		return Connection{}, ErrConnectionNotFound
	}
	row := rows[0]
	c := Connection{ID: oauthString(row[0]), WorkspaceID: oauthString(row[1]), Provider: oauthString(row[2]), Revision: oauthInt64(row[6]), TokenVersion: oauthInt64(row[7])}
	if oauthInt64(row[8]) != 0 {
		return Connection{}, ErrConnectionRevoked
	}
	if expectedRevision != 0 && c.Revision != expectedRevision {
		return Connection{}, ErrConnectionRevision
	}
	ciphertext, scopesJSON, metadataJSON := oauthBytes(row[3]), oauthString(row[4]), oauthString(row[5])
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
	connection, err := s.LoadConnection(ctx, connectionID)
	if err != nil {
		return nil, err
	}
	if connection.Token == nil {
		return nil, errors.New("connection has no OAuth token")
	}
	return &persistingTokenSource{base: cfg.TokenSource(ctx, connection.Token), current: connection.Token, store: s, connection: connection}, nil
}

type persistingTokenSource struct {
	mu         sync.Mutex
	base       oauth2.TokenSource
	current    *oauth2.Token
	store      *Store
	connection Connection
}

func (s *persistingTokenSource) Token() (*oauth2.Token, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.store.ValidateConnectionRevision(context.Background(), s.connection.ID, s.connection.Revision); err != nil {
		return nil, err
	}
	token, err := s.base.Token()
	if err != nil {
		return nil, err
	}
	if token.AccessToken != s.current.AccessToken || token.RefreshToken != s.current.RefreshToken || !token.Expiry.Equal(s.current.Expiry) {
		if err := s.store.SaveTokenAtRevision(context.Background(), s.connection.ID, s.connection.WorkspaceID, s.connection.Provider, token, s.connection.Scopes, s.connection.Metadata, time.Now().UTC(), s.connection.Revision, s.connection.TokenVersion); err != nil {
			return nil, err
		}
		s.current = token
		s.connection.TokenVersion++
	}
	return token, nil
}

func (s *Store) updateToken(ctx context.Context, connectionID string, token *oauth2.Token) error {
	connection, err := s.LoadConnection(ctx, connectionID)
	if err != nil {
		return err
	}
	return s.SaveTokenAtRevision(ctx, connection.ID, connection.WorkspaceID, connection.Provider, token, connection.Scopes, connection.Metadata, time.Now().UTC(), connection.Revision, connection.TokenVersion)
}

// RevokeConnection tombstones a connection. Repeated revocations leave the
// tombstone unchanged, and a revoked connection can never be reactivated.
func (s *Store) RevokeConnection(ctx context.Context, connectionID string) error {
	if _, _, revoked, err := s.connectionVersion(ctx, connectionID); err != nil {
		return err
	} else if revoked {
		return nil
	}
	writeID, err := randomOpaque(16)
	if err != nil {
		return err
	}
	_, err = s.db.Mutate(ctx, "revoke connection "+connectionID, persistence.Statement{SQL: `UPDATE connection_versions
        SET revision=revision+1, token_version=token_version+1, revoked=1, write_id=?
        WHERE connection_id=? AND revoked=0`, Args: []any{writeID, connectionID}})
	if err != nil {
		return err
	}
	_, _, revoked, err := s.connectionVersion(ctx, connectionID)
	if err != nil {
		return err
	}
	if !revoked {
		return ErrConnectionRevision
	}
	return nil
}

func (s *Store) connectionVersion(ctx context.Context, connectionID string) (revision, tokenVersion int64, revoked bool, err error) {
	rows, err := s.db.Query(ctx, `SELECT revision,token_version,revoked FROM connection_versions WHERE connection_id=?`, connectionID)
	if err != nil {
		return 0, 0, false, err
	}
	if len(rows) == 0 {
		return 0, 0, false, ErrConnectionNotFound
	}
	return oauthInt64(rows[0][0]), oauthInt64(rows[0][1]), oauthInt64(rows[0][2]) != 0, nil
}

func (s *Store) saveConnection(ctx context.Context, connectionID, workspaceID, provider string, token *oauth2.Token, scopes []string, metadata map[string]string, now time.Time, currentRevision, currentTokenVersion int64) error {
	values, err := s.connectionValues(token, scopes, metadata)
	if err != nil {
		return err
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	writeID, err := randomOpaque(16)
	if err != nil {
		return err
	}
	desiredRevision := currentRevision + 1
	desiredTokenVersion := currentTokenVersion + 1
	versionStatement := persistence.Statement{SQL: `INSERT OR IGNORE INTO connection_versions (connection_id,revision,token_version,revoked,write_id)
        SELECT ?,1,1,0,? WHERE NOT EXISTS (SELECT 1 FROM connections WHERE connection_id=? AND (workspace_id<>? OR provider<>?))`, Args: []any{connectionID, writeID, connectionID, workspaceID, provider}}
	if currentRevision > 0 {
		versionStatement = persistence.Statement{SQL: `UPDATE connection_versions SET revision=revision+1,token_version=token_version+1,write_id=?
			WHERE connection_id=? AND revision=? AND token_version=? AND revoked=0 AND NOT EXISTS (SELECT 1 FROM connections WHERE connection_id=? AND (workspace_id<>? OR provider<>?))`, Args: []any{writeID, connectionID, currentRevision, currentTokenVersion, connectionID, workspaceID, provider}}
	}
	_, err = s.db.Mutate(ctx, "replace connection "+connectionID+" at "+fmt.Sprint(now.UnixNano()), versionStatement,
		persistence.Statement{SQL: `INSERT INTO connections (connection_id,workspace_id,provider,token_ciphertext,scopes_json,metadata_json,expires_at,created_at,updated_at)
            SELECT ?,?,?,?,?,?,?,?,? WHERE EXISTS (SELECT 1 FROM connection_versions WHERE connection_id=? AND revision=? AND revoked=0 AND write_id=?)
            ON CONFLICT(connection_id) DO UPDATE SET token_ciphertext=excluded.token_ciphertext,scopes_json=excluded.scopes_json,metadata_json=excluded.metadata_json,expires_at=excluded.expires_at,updated_at=excluded.updated_at
            WHERE connections.workspace_id=excluded.workspace_id AND connections.provider=excluded.provider AND EXISTS (SELECT 1 FROM connection_versions WHERE connection_id=? AND revision=? AND revoked=0 AND write_id=?)`, Args: []any{
			connectionID, workspaceID, provider, values.ciphertext, values.scopesJSON, values.metadataJSON, values.expiry, now.Unix(), now.Unix(),
			connectionID, desiredRevision, writeID, connectionID, desiredRevision, writeID}})
	if err != nil {
		return err
	}
	return s.verifyConnectionAuthority(ctx, connectionID, workspaceID, provider, desiredRevision, desiredTokenVersion, writeID)
}

func (s *Store) saveTokenAtRevision(ctx context.Context, connectionID, workspaceID, provider string, token *oauth2.Token, scopes []string, metadata map[string]string, now time.Time, expectedRevision, expectedTokenVersion int64) error {
	if connectionID == "" || workspaceID == "" || provider == "" {
		return errors.New("connection, workspace, and provider are required")
	}
	values, err := s.connectionValues(token, scopes, metadata)
	if err != nil {
		return err
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	writeID, err := randomOpaque(16)
	if err != nil {
		return err
	}
	_, err = s.db.Mutate(ctx, "refresh connection "+connectionID+" at "+fmt.Sprint(now.UnixNano()),
		persistence.Statement{SQL: `UPDATE connection_versions SET token_version=token_version+1,write_id=?
            WHERE connection_id=? AND revision=? AND token_version=? AND revoked=0
            AND EXISTS (SELECT 1 FROM connections WHERE connection_id=? AND workspace_id=? AND provider=?)`, Args: []any{writeID, connectionID, expectedRevision, expectedTokenVersion, connectionID, workspaceID, provider}},
		persistence.Statement{SQL: `UPDATE connections SET token_ciphertext=?,scopes_json=?,metadata_json=?,expires_at=?,updated_at=?
            WHERE connection_id=? AND workspace_id=? AND provider=?
            AND EXISTS (SELECT 1 FROM connection_versions WHERE connection_id=? AND revision=? AND token_version=? AND revoked=0 AND write_id=?)`, Args: []any{
			values.ciphertext, values.scopesJSON, values.metadataJSON, values.expiry, now.Unix(), connectionID, workspaceID, provider,
			connectionID, expectedRevision, expectedTokenVersion + 1, writeID}})
	if err != nil {
		return err
	}
	return s.verifyConnectionAuthority(ctx, connectionID, workspaceID, provider, expectedRevision, expectedTokenVersion+1, writeID)
}

type sealedConnection struct {
	ciphertext   string
	scopesJSON   string
	metadataJSON string
	expiry       any
}

func (s *Store) connectionValues(token *oauth2.Token, scopes []string, metadata map[string]string) (sealedConnection, error) {
	payload := []byte("null")
	var err error
	if token != nil {
		payload, err = json.Marshal(token)
	}
	if err != nil {
		return sealedConnection{}, fmt.Errorf("marshal connection token: %w", err)
	}
	ciphertext, err := s.encryptor.Seal(payload)
	if err != nil {
		return sealedConnection{}, err
	}
	scopesJSON, err := json.Marshal(scopes)
	if err != nil {
		return sealedConnection{}, err
	}
	metadataJSON, err := json.Marshal(metadata)
	if err != nil {
		return sealedConnection{}, err
	}
	metadataCiphertext, err := s.encryptor.Seal(metadataJSON)
	if err != nil {
		return sealedConnection{}, err
	}
	values := sealedConnection{ciphertext: base64.StdEncoding.EncodeToString(ciphertext), scopesJSON: string(scopesJSON), metadataJSON: base64.RawStdEncoding.EncodeToString(metadataCiphertext)}
	if token != nil && !token.Expiry.IsZero() {
		values.expiry = token.Expiry.Unix()
	}
	return values, nil
}

func (s *Store) verifyConnectionAuthority(ctx context.Context, connectionID, workspaceID, provider string, revision, tokenVersion int64, writeID string) error {
	rows, err := s.db.Query(ctx, `SELECT c.workspace_id,c.provider,v.revision,v.token_version,v.revoked,v.write_id FROM connections c JOIN connection_versions v ON v.connection_id=c.connection_id WHERE c.connection_id=?`, connectionID)
	if err != nil {
		return err
	}
	if len(rows) == 1 && oauthString(rows[0][0]) == workspaceID && oauthString(rows[0][1]) == provider && oauthInt64(rows[0][4]) != 0 {
		return ErrConnectionRevoked
	}
	if len(rows) != 1 || oauthString(rows[0][0]) != workspaceID || oauthString(rows[0][1]) != provider || oauthInt64(rows[0][2]) != revision || oauthInt64(rows[0][3]) != tokenVersion || oauthInt64(rows[0][4]) != 0 || oauthString(rows[0][5]) != writeID {
		return ErrConnectionRevision
	}
	return nil
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

func oauthClaimID() (string, error) {
	var value [16]byte
	if _, err := io.ReadFull(rand.Reader, value[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(value[:]), nil
}

func oauthString(value any) string {
	switch value := value.(type) {
	case string:
		return value
	case []byte:
		return string(value)
	case nil:
		return ""
	default:
		return fmt.Sprint(value)
	}
}

func oauthBytes(value any) []byte {
	switch value := value.(type) {
	case []byte:
		return append([]byte(nil), value...)
	case string:
		decoded, err := base64.StdEncoding.DecodeString(value)
		if err == nil {
			return decoded
		}
		return []byte(value)
	default:
		return nil
	}
}

func oauthInt64(value any) int64 {
	switch value := value.(type) {
	case int64:
		return value
	case int:
		return int64(value)
	case float64:
		return int64(value)
	case json.Number:
		result, _ := value.Int64()
		return result
	default:
		return 0
	}
}

func oauthSecurityParam(key string) bool {
	switch strings.ToLower(key) {
	case "state", "code_challenge", "code_challenge_method", "redirect_uri", "client_id", "response_type":
		return true
	default:
		return false
	}
}
