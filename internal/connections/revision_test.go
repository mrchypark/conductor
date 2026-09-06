package connections

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/mrchypark/conductor/internal/persistence"
	"github.com/mrchypark/rhiza"
	"golang.org/x/oauth2"
)

func TestConnectionProfilesFenceRotationRefreshAndRevocation(t *testing.T) {
	store, _ := newOAuthStore(t)
	ctx := context.Background()
	now := time.Unix(1_700_000_000, 0).UTC()
	for _, id := range []string{"profile-a", "profile-b"} {
		if err := store.SaveToken(ctx, id, "workspace", "github", &oauth2.Token{AccessToken: id}, []string{"read"}, nil, now); err != nil {
			t.Fatal(err)
		}
	}
	a, err := store.LoadConnection(ctx, "profile-a")
	if err != nil {
		t.Fatal(err)
	}
	b, err := store.LoadConnection(ctx, "profile-b")
	if err != nil {
		t.Fatal(err)
	}
	if a.Revision != 1 || a.TokenVersion != 1 || b.Revision != 1 || b.TokenVersion != 1 {
		t.Fatalf("unexpected initial versions: a=%d/%d b=%d/%d", a.Revision, a.TokenVersion, b.Revision, b.TokenVersion)
	}

	if err := store.SaveToken(ctx, "profile-a", "workspace", "github", &oauth2.Token{AccessToken: "rotated"}, []string{"read"}, nil, now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	rotated, err := store.LoadConnection(ctx, "profile-a")
	if err != nil {
		t.Fatal(err)
	}
	if rotated.Revision != 2 || rotated.TokenVersion != 2 {
		t.Fatalf("explicit replacement did not rotate versions: %d/%d", rotated.Revision, rotated.TokenVersion)
	}
	if err := store.SaveTokenAtRevision(ctx, "profile-a", "workspace", "github", &oauth2.Token{AccessToken: "stale"}, []string{"read"}, nil, now, a.Revision, a.TokenVersion); !errors.Is(err, ErrConnectionRevision) {
		t.Fatalf("stale refresh: got %v, want %v", err, ErrConnectionRevision)
	}
	if _, err := store.LoadConnectionAtRevision(ctx, "profile-a", a.Revision); !errors.Is(err, ErrConnectionRevision) {
		t.Fatalf("stale credential load: got %v, want %v", err, ErrConnectionRevision)
	}

	if err := store.SaveTokenAtRevision(ctx, "profile-b", "workspace", "github", &oauth2.Token{AccessToken: "refreshed"}, []string{"read"}, nil, now, b.Revision, b.TokenVersion); err != nil {
		t.Fatal(err)
	}
	if err := store.SaveTokenAtRevision(ctx, "profile-b", "workspace", "github", &oauth2.Token{AccessToken: "late"}, []string{"read"}, nil, now, b.Revision, b.TokenVersion); !errors.Is(err, ErrConnectionRevision) {
		t.Fatalf("concurrent refresh CAS: got %v, want %v", err, ErrConnectionRevision)
	}
	refreshed, err := store.LoadConnectionAtRevision(ctx, "profile-b", b.Revision)
	if err != nil {
		t.Fatal(err)
	}
	if refreshed.TokenVersion != b.TokenVersion+1 {
		t.Fatalf("refresh token version = %d, want %d", refreshed.TokenVersion, b.TokenVersion+1)
	}

	if err := store.RevokeConnection(ctx, "profile-a"); err != nil {
		t.Fatal(err)
	}
	if err := store.SaveTokenAtRevision(ctx, "profile-a", "workspace", "github", &oauth2.Token{AccessToken: "late-after-revoke"}, []string{"read"}, nil, now, rotated.Revision, rotated.TokenVersion); !errors.Is(err, ErrConnectionRevoked) {
		t.Fatalf("refresh after revoke: %v", err)
	}
	if err := store.RevokeConnection(ctx, "profile-a"); err != nil {
		t.Fatal(err)
	}
	if err := store.ValidateConnectionRevision(ctx, "profile-a", 0); !errors.Is(err, ErrConnectionRevoked) {
		t.Fatalf("revoked validation: got %v, want %v", err, ErrConnectionRevoked)
	}
	if _, err := store.LoadToken(ctx, "profile-a"); !errors.Is(err, ErrConnectionRevoked) {
		t.Fatalf("revoked token load: got %v, want %v", err, ErrConnectionRevoked)
	}
	if err := store.SaveToken(ctx, "profile-a", "workspace", "github", &oauth2.Token{AccessToken: "reactivated"}, nil, nil, now); !errors.Is(err, ErrConnectionRevoked) {
		t.Fatalf("revoked profile reactivation: got %v, want %v", err, ErrConnectionRevoked)
	}
}

func TestConnectionVersionBackfillPreservesLegacyCredential(t *testing.T) {
	ctx := context.Background()
	key := []byte("01234567890123456789012345678901")
	db, err := persistence.Open(ctx, persistence.Config{ClusterID: "legacy-oauth-test", NodeID: "n1", DataDir: t.TempDir(), Members: []rhiza.Member{{ID: "n1"}}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	encryptor, err := NewEncryptor(key)
	if err != nil {
		t.Fatal(err)
	}
	payload, _ := json.Marshal(&oauth2.Token{AccessToken: "legacy"})
	ciphertext, err := encryptor.Seal(payload)
	if err != nil {
		t.Fatal(err)
	}
	metadata, err := encryptor.Seal([]byte("null"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Mutate(ctx, "create legacy connection", persistence.Statement{SQL: `CREATE TABLE connections (
        connection_id TEXT PRIMARY KEY, workspace_id TEXT NOT NULL, provider TEXT NOT NULL, token_ciphertext BLOB NOT NULL,
        scopes_json TEXT NOT NULL, metadata_json TEXT NOT NULL, expires_at INTEGER, created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL)`}, persistence.Statement{SQL: `INSERT INTO connections
        (connection_id,workspace_id,provider,token_ciphertext,scopes_json,metadata_json,expires_at,created_at,updated_at)
	        VALUES (?,?,?,?,?,?,?,?,?)`, Args: []any{"legacy", "workspace", "github", base64.StdEncoding.EncodeToString(ciphertext), "null", base64.RawStdEncoding.EncodeToString(metadata), nil, 1, 1}})
	if err != nil {
		t.Fatal(err)
	}
	reopened, err := NewStore(ctx, db, key)
	if err != nil {
		t.Fatal(err)
	}
	connection, err := reopened.LoadConnectionAtRevision(ctx, "legacy", 1)
	if err != nil {
		t.Fatal(err)
	}
	if connection.Revision != 1 || connection.TokenVersion != 1 {
		t.Fatalf("backfill version = %d/%d, want 1/1", connection.Revision, connection.TokenVersion)
	}
}
