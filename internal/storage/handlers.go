package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
)

type StarlarkHandlerRecord struct {
	WorkspaceID, HandlerKey, Version string
	ManifestJSON, Source             []byte
	MaxSteps                         int
	SourceHash                       string
}

type WASMHandlerRecord struct {
	WorkspaceID, HandlerKey, Version        string
	ManifestJSON                            []byte
	ArtifactRef, ManifestDigest, BlobDigest string
	ArtifactSize                            int64
	RuntimeABI                              string
}

func (s *Store) SaveHandlerManifest(ctx context.Context, workspaceID, key, version string, manifest any) error {
	if key == "" || version == "" {
		return errors.New("handler key and version are required")
	}
	b, e := json.Marshal(manifest)
	if e != nil {
		return e
	}
	_, e = s.db.Mutate(ctx, "save handler manifest", stm(`INSERT INTO handler_manifests(workspace_id,handler_key,version,manifest_json,created_at) VALUES(?,?,?,?,?) ON CONFLICT(workspace_id,handler_key,version) DO UPDATE SET manifest_json=excluded.manifest_json`, workspaceID, key, version, b, now()))
	return e
}

func (s *Store) SaveConnectorManifest(ctx context.Context, key string, manifest any) error {
	if key == "" {
		return errors.New("provider key is required")
	}
	b, e := json.Marshal(manifest)
	if e != nil {
		return e
	}
	_, e = s.db.Mutate(ctx, "save connector manifest", stm(`INSERT INTO connector_manifests(provider_key,manifest_json,created_at) VALUES(?,?,?) ON CONFLICT(provider_key) DO UPDATE SET manifest_json=excluded.manifest_json`, key, b, now()))
	return e
}

func (s *Store) SaveStarlarkHandler(ctx context.Context, r StarlarkHandlerRecord) (bool, error) {
	if r.WorkspaceID == "" || r.HandlerKey == "" || r.Version == "" || len(r.ManifestJSON) == 0 || len(r.Source) == 0 || r.SourceHash == "" {
		return false, errors.New("starlark handler workspace_id, key, version, manifest, source, and hash are required")
	}
	old, e := s.GetStarlarkHandler(ctx, r.WorkspaceID, r.HandlerKey, r.Version)
	if e == nil {
		return false, compareStarlark(old, r)
	}
	if !errors.Is(e, ErrNotFound) {
		return false, e
	}
	_, e = s.db.Mutate(ctx, "save starlark handler", stm(`INSERT OR IGNORE INTO starlark_handlers(workspace_id,handler_key,version,manifest_json,source,max_steps,source_hash,created_at) VALUES(?,?,?,?,?,?,?,?)`, r.WorkspaceID, r.HandlerKey, r.Version, r.ManifestJSON, r.Source, r.MaxSteps, r.SourceHash, now()))
	if e != nil {
		return false, e
	}
	old, e = s.GetStarlarkHandler(ctx, r.WorkspaceID, r.HandlerKey, r.Version)
	if e != nil {
		return false, e
	}
	return true, compareStarlark(old, r)
}

func compareStarlark(old, r StarlarkHandlerRecord) error {
	if old.SourceHash != r.SourceHash || !bytes.Equal(old.ManifestJSON, r.ManifestJSON) || old.MaxSteps != r.MaxSteps {
		return errors.New("starlark handler version already exists with different definition")
	}
	return nil
}

func (s *Store) GetStarlarkHandler(ctx context.Context, workspaceID, key, version string) (StarlarkHandlerRecord, error) {
	rows, e := s.db.Query(ctx, `SELECT manifest_json,source,max_steps,source_hash FROM starlark_handlers WHERE workspace_id=? AND handler_key=? AND version=?`, workspaceID, key, version)
	if e != nil {
		return StarlarkHandlerRecord{}, e
	}
	if len(rows) == 0 {
		return StarlarkHandlerRecord{}, ErrNotFound
	}
	x := rows[0]
	return StarlarkHandlerRecord{WorkspaceID: workspaceID, HandlerKey: key, Version: version, ManifestJSON: blob(x[0]), Source: blob(x[1]), MaxSteps: num(x[2]), SourceHash: str(x[3])}, nil
}

func (s *Store) DeleteStarlarkHandler(ctx context.Context, workspaceID, key, version, hash string) error {
	if workspaceID == "" || key == "" || version == "" || hash == "" {
		return errors.New("starlark handler workspace_id, key, version, and hash are required")
	}
	_, e := s.db.Mutate(ctx, "delete starlark handler", stm(`DELETE FROM starlark_handlers WHERE workspace_id=? AND handler_key=? AND version=? AND source_hash=?`, workspaceID, key, version, hash))
	return e
}

func (s *Store) ListStarlarkHandlers(ctx context.Context) ([]StarlarkHandlerRecord, error) {
	rows, e := s.db.Query(ctx, `SELECT workspace_id,handler_key,version,manifest_json,source,max_steps,source_hash FROM starlark_handlers ORDER BY workspace_id,handler_key,version`)
	if e != nil {
		return nil, e
	}
	out := make([]StarlarkHandlerRecord, 0, len(rows))
	for _, x := range rows {
		out = append(out, StarlarkHandlerRecord{WorkspaceID: str(x[0]), HandlerKey: str(x[1]), Version: str(x[2]), ManifestJSON: blob(x[3]), Source: blob(x[4]), MaxSteps: num(x[5]), SourceHash: str(x[6])})
	}
	return out, nil
}

func (s *Store) SaveWASMHandler(ctx context.Context, r WASMHandlerRecord) (bool, error) {
	if r.HandlerKey == "" || r.Version == "" || len(r.ManifestJSON) == 0 || r.ArtifactRef == "" || r.ManifestDigest == "" || r.BlobDigest == "" || r.ArtifactSize <= 0 || r.RuntimeABI == "" {
		return false, errors.New("wasm handler metadata is incomplete")
	}
	old, err := s.wasmHandler(ctx, r.WorkspaceID, r.HandlerKey, r.Version)
	if err == nil {
		return false, compareWASMHandler(old, r)
	}
	if !errors.Is(err, ErrNotFound) {
		return false, err
	}
	_, err = s.db.Mutate(ctx, "save wasm handler", stm(`INSERT OR IGNORE INTO wasm_handlers(workspace_id,handler_key,version,manifest_json,artifact_ref,manifest_digest,blob_digest,artifact_size,runtime_abi,created_at) VALUES(?,?,?,?,?,?,?,?,?,?)`, r.WorkspaceID, r.HandlerKey, r.Version, r.ManifestJSON, r.ArtifactRef, r.ManifestDigest, r.BlobDigest, r.ArtifactSize, r.RuntimeABI, now()))
	if err != nil {
		return false, err
	}
	old, err = s.wasmHandler(ctx, r.WorkspaceID, r.HandlerKey, r.Version)
	if err != nil {
		return false, err
	}
	return true, compareWASMHandler(old, r)
}

func compareWASMHandler(old, current WASMHandlerRecord) error {
	if !bytes.Equal(old.ManifestJSON, current.ManifestJSON) || old.ArtifactRef != current.ArtifactRef || old.ManifestDigest != current.ManifestDigest || old.BlobDigest != current.BlobDigest || old.ArtifactSize != current.ArtifactSize || old.RuntimeABI != current.RuntimeABI {
		return errors.New("wasm handler version already exists with different definition")
	}
	return nil
}

func (s *Store) wasmHandler(ctx context.Context, workspaceID, key, version string) (WASMHandlerRecord, error) {
	rows, err := s.db.Query(ctx, `SELECT manifest_json,artifact_ref,manifest_digest,blob_digest,artifact_size,runtime_abi FROM wasm_handlers WHERE workspace_id=? AND handler_key=? AND version=?`, workspaceID, key, version)
	if err != nil {
		return WASMHandlerRecord{}, err
	}
	if len(rows) == 0 {
		return WASMHandlerRecord{}, ErrNotFound
	}
	x := rows[0]
	return WASMHandlerRecord{WorkspaceID: workspaceID, HandlerKey: key, Version: version, ManifestJSON: blob(x[0]), ArtifactRef: str(x[1]), ManifestDigest: str(x[2]), BlobDigest: str(x[3]), ArtifactSize: int64(num(x[4])), RuntimeABI: str(x[5])}, nil
}

func (s *Store) GetWASMHandler(ctx context.Context, workspaceID, key, version string) (WASMHandlerRecord, error) {
	return s.wasmHandler(ctx, workspaceID, key, version)
}

func (s *Store) DeleteWASMHandler(ctx context.Context, workspaceID, key, version, manifestDigest string) error {
	if key == "" || version == "" || manifestDigest == "" {
		return errors.New("wasm handler key, version, and manifest digest are required")
	}
	_, err := s.db.Mutate(ctx, "delete wasm handler", stm(`DELETE FROM wasm_handlers WHERE workspace_id=? AND handler_key=? AND version=? AND manifest_digest=?`, workspaceID, key, version, manifestDigest))
	return err
}

func (s *Store) ListWASMHandlers(ctx context.Context) ([]WASMHandlerRecord, error) {
	rows, err := s.db.Query(ctx, `SELECT workspace_id,handler_key,version,manifest_json,artifact_ref,manifest_digest,blob_digest,artifact_size,runtime_abi FROM wasm_handlers ORDER BY workspace_id,handler_key,version`)
	if err != nil {
		return nil, err
	}
	out := make([]WASMHandlerRecord, 0, len(rows))
	for _, x := range rows {
		out = append(out, WASMHandlerRecord{WorkspaceID: str(x[0]), HandlerKey: str(x[1]), Version: str(x[2]), ManifestJSON: blob(x[3]), ArtifactRef: str(x[4]), ManifestDigest: str(x[5]), BlobDigest: str(x[6]), ArtifactSize: int64(num(x[7])), RuntimeABI: str(x[8])})
	}
	return out, nil
}
