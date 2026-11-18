package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
)

type DynamicHandlerRecord struct {
	HandlerKey, Version                          string
	ManifestJSON, Source                         []byte
	AllowedModules                               []string
	MaxFrames, MaxStack                          int
	SourceHash                                   string
	Bytecode                                     []byte
	BytecodeHash, BytecodeSourceHash, RuntimeABI string
}
type WASMHandlerRecord struct {
	HandlerKey, Version                     string
	ManifestJSON                            []byte
	ArtifactRef, ManifestDigest, BlobDigest string
	ArtifactSize                            int64
	RuntimeABI                              string
}

func (s *Store) SaveHandlerManifest(ctx context.Context, key, version string, manifest any) error {
	if key == "" || version == "" {
		return errors.New("handler key and version are required")
	}
	b, e := json.Marshal(manifest)
	if e != nil {
		return e
	}
	_, e = s.db.Mutate(ctx, "save handler manifest", stm(`INSERT INTO handler_manifests(handler_key,version,manifest_json,created_at) VALUES(?,?,?,?) ON CONFLICT(handler_key,version) DO UPDATE SET manifest_json=excluded.manifest_json`, key, version, b, now()))
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
func (s *Store) SaveDynamicHandler(ctx context.Context, r DynamicHandlerRecord) (bool, error) {
	if r.HandlerKey == "" || r.Version == "" || len(r.ManifestJSON) == 0 || len(r.Source) == 0 || r.SourceHash == "" {
		return false, errors.New("dynamic handler key, version, manifest, source, and hash are required")
	}
	mods, e := json.Marshal(r.AllowedModules)
	if e != nil {
		return false, e
	}
	old, e := s.dynamic(ctx, r.HandlerKey, r.Version)
	if e == nil {
		return false, compareDynamic(old, r, mods)
	}
	if !errors.Is(e, ErrNotFound) {
		return false, e
	}
	_, e = s.db.Mutate(ctx, "save dynamic handler", stm(`INSERT OR IGNORE INTO dynamic_handlers(handler_key,version,manifest_json,source,allowed_modules_json,max_frames,max_stack,source_hash,created_at) VALUES(?,?,?,?,?,?,?,?,?)`, r.HandlerKey, r.Version, r.ManifestJSON, r.Source, mods, r.MaxFrames, r.MaxStack, r.SourceHash, now()))
	if e != nil {
		return false, e
	}
	old, e = s.dynamic(ctx, r.HandlerKey, r.Version)
	if e != nil {
		return false, e
	}
	return true, compareDynamic(old, r, mods)
}
func compareDynamic(old, r DynamicHandlerRecord, mods []byte) error {
	got, _ := json.Marshal(old.AllowedModules)
	if old.SourceHash != r.SourceHash || !bytes.Equal(old.ManifestJSON, r.ManifestJSON) || !bytes.Equal(got, mods) || old.MaxFrames != r.MaxFrames || old.MaxStack != r.MaxStack {
		return errors.New("dynamic handler version already exists with different definition")
	}
	return nil
}
func (s *Store) dynamic(ctx context.Context, key, version string) (DynamicHandlerRecord, error) {
	rows, e := s.db.Query(ctx, `SELECT manifest_json,source,allowed_modules_json,max_frames,max_stack,source_hash FROM dynamic_handlers WHERE handler_key=? AND version=?`, key, version)
	if e != nil {
		return DynamicHandlerRecord{}, e
	}
	if len(rows) == 0 {
		return DynamicHandlerRecord{}, ErrNotFound
	}
	x := rows[0]
	r := DynamicHandlerRecord{HandlerKey: key, Version: version, ManifestJSON: blob(x[0]), Source: blob(x[1]), MaxFrames: num(x[3]), MaxStack: num(x[4]), SourceHash: str(x[5])}
	if e = json.Unmarshal(blob(x[2]), &r.AllowedModules); e != nil {
		return r, e
	}
	return r, nil
}

func (s *Store) GetDynamicHandler(ctx context.Context, key, version string) (DynamicHandlerRecord, error) {
	return s.dynamic(ctx, key, version)
}
func (s *Store) DeleteDynamicHandler(ctx context.Context, key, version, hash string) error {
	if key == "" || version == "" || hash == "" {
		return errors.New("dynamic handler key, version, and hash are required")
	}
	_, e := s.db.Mutate(ctx, "delete dynamic handler", stm(`DELETE FROM dynamic_handlers WHERE handler_key=? AND version=? AND source_hash=?`, key, version, hash))
	return e
}
func (s *Store) SaveDynamicHandlerBytecode(ctx context.Context, key, version string, code []byte, codeHash, sourceHash, abi string) error {
	if key == "" || version == "" || len(code) == 0 || codeHash == "" || sourceHash == "" || abi == "" {
		return errors.New("dynamic handler bytecode, hashes, and runtime ABI are required")
	}
	_, e := s.db.Mutate(ctx, "save dynamic handler bytecode", stm(`INSERT INTO dynamic_handler_bytecode(handler_key,version,bytecode,bytecode_hash,source_hash,runtime_abi,created_at) VALUES(?,?,?,?,?,?,?) ON CONFLICT(handler_key,version) DO UPDATE SET bytecode=excluded.bytecode,bytecode_hash=excluded.bytecode_hash,source_hash=excluded.source_hash,runtime_abi=excluded.runtime_abi,created_at=excluded.created_at`, key, version, code, codeHash, sourceHash, abi, now()))
	return e
}
func (s *Store) ListDynamicHandlers(ctx context.Context) ([]DynamicHandlerRecord, error) {
	rows, e := s.db.Query(ctx, `SELECT d.handler_key,d.version,d.manifest_json,d.source,d.allowed_modules_json,d.max_frames,d.max_stack,d.source_hash,COALESCE(b.bytecode,''),COALESCE(b.bytecode_hash,''),COALESCE(b.source_hash,''),COALESCE(b.runtime_abi,'') FROM dynamic_handlers d LEFT JOIN dynamic_handler_bytecode b ON b.handler_key=d.handler_key AND b.version=d.version ORDER BY d.handler_key,d.version`)
	if e != nil {
		return nil, e
	}
	out := make([]DynamicHandlerRecord, 0, len(rows))
	for _, x := range rows {
		r := DynamicHandlerRecord{HandlerKey: str(x[0]), Version: str(x[1]), ManifestJSON: blob(x[2]), Source: blob(x[3]), MaxFrames: num(x[5]), MaxStack: num(x[6]), SourceHash: str(x[7]), Bytecode: blob(x[8]), BytecodeHash: str(x[9]), BytecodeSourceHash: str(x[10]), RuntimeABI: str(x[11])}
		if e = json.Unmarshal(blob(x[4]), &r.AllowedModules); e != nil {
			return nil, e
		}
		out = append(out, r)
	}
	return out, nil
}

func (s *Store) SaveWASMHandler(ctx context.Context, r WASMHandlerRecord) (bool, error) {
	if r.HandlerKey == "" || r.Version == "" || len(r.ManifestJSON) == 0 || r.ArtifactRef == "" || r.ManifestDigest == "" || r.BlobDigest == "" || r.ArtifactSize <= 0 || r.RuntimeABI == "" {
		return false, errors.New("wasm handler metadata is incomplete")
	}
	old, err := s.wasmHandler(ctx, r.HandlerKey, r.Version)
	if err == nil {
		return false, compareWASMHandler(old, r)
	}
	if !errors.Is(err, ErrNotFound) {
		return false, err
	}
	_, err = s.db.Mutate(ctx, "save wasm handler", stm(`INSERT OR IGNORE INTO wasm_handlers(handler_key,version,manifest_json,artifact_ref,manifest_digest,blob_digest,artifact_size,runtime_abi,created_at) VALUES(?,?,?,?,?,?,?,?,?)`, r.HandlerKey, r.Version, r.ManifestJSON, r.ArtifactRef, r.ManifestDigest, r.BlobDigest, r.ArtifactSize, r.RuntimeABI, now()))
	if err != nil {
		return false, err
	}
	old, err = s.wasmHandler(ctx, r.HandlerKey, r.Version)
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

func (s *Store) wasmHandler(ctx context.Context, key, version string) (WASMHandlerRecord, error) {
	rows, err := s.db.Query(ctx, `SELECT manifest_json,artifact_ref,manifest_digest,blob_digest,artifact_size,runtime_abi FROM wasm_handlers WHERE handler_key=? AND version=?`, key, version)
	if err != nil {
		return WASMHandlerRecord{}, err
	}
	if len(rows) == 0 {
		return WASMHandlerRecord{}, ErrNotFound
	}
	x := rows[0]
	return WASMHandlerRecord{HandlerKey: key, Version: version, ManifestJSON: blob(x[0]), ArtifactRef: str(x[1]), ManifestDigest: str(x[2]), BlobDigest: str(x[3]), ArtifactSize: int64(num(x[4])), RuntimeABI: str(x[5])}, nil
}

func (s *Store) DeleteWASMHandler(ctx context.Context, key, version, manifestDigest string) error {
	if key == "" || version == "" || manifestDigest == "" {
		return errors.New("wasm handler key, version, and manifest digest are required")
	}
	_, err := s.db.Mutate(ctx, "delete wasm handler", stm(`DELETE FROM wasm_handlers WHERE handler_key=? AND version=? AND manifest_digest=?`, key, version, manifestDigest))
	return err
}

func (s *Store) ListWASMHandlers(ctx context.Context) ([]WASMHandlerRecord, error) {
	rows, err := s.db.Query(ctx, `SELECT handler_key,version,manifest_json,artifact_ref,manifest_digest,blob_digest,artifact_size,runtime_abi FROM wasm_handlers ORDER BY handler_key,version`)
	if err != nil {
		return nil, err
	}
	out := make([]WASMHandlerRecord, 0, len(rows))
	for _, x := range rows {
		out = append(out, WASMHandlerRecord{HandlerKey: str(x[0]), Version: str(x[1]), ManifestJSON: blob(x[2]), ArtifactRef: str(x[3]), ManifestDigest: str(x[4]), BlobDigest: str(x[5]), ArtifactSize: int64(num(x[6])), RuntimeABI: str(x[7])})
	}
	return out, nil
}
