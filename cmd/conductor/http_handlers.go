package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/mrchypark/conductor/internal/handlers"
	"github.com/mrchypark/conductor/internal/storage"
)

func (s *server) getHandlerDefinition(w http.ResponseWriter, r *http.Request, workspaceID string) {
	if len(r.URL.RawQuery) > 3<<20 {
		writeError(w, http.StatusBadRequest, "invalid handler query")
		return
	}
	query, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil || len(query) != 2 || len(query["handler_key"]) != 1 || len(query["version"]) != 1 || query.Get("handler_key") == "" || query.Get("version") == "" || !utf8.ValidString(query.Get("handler_key")) || !utf8.ValidString(query.Get("version")) {
		writeError(w, http.StatusBadRequest, "valid handler key and exactly one version are required")
		return
	}
	if s.store == nil {
		writeError(w, http.StatusServiceUnavailable, "handler store unavailable")
		return
	}
	key, version := query.Get("handler_key"), query.Get("version")
	record, err := s.store.GetStarlarkHandler(r.Context(), workspaceID, key, version)
	if errors.Is(err, storage.ErrNotFound) {
		writeError(w, http.StatusNotFound, "stored Starlark source unavailable for handler")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to load handler definition")
		return
	}
	var manifest handlers.Manifest
	hash := sha256.Sum256(record.Source)
	if len(record.Source) == 0 || len(record.Source) > 256<<10 || !utf8.Valid(record.Source) || fmt.Sprintf("%x", hash[:]) != record.SourceHash || len(record.ManifestJSON) > (6<<20)+1024 || json.Unmarshal(record.ManifestJSON, &manifest) != nil || manifest.Key != key || manifest.Version != record.Version {
		writeError(w, http.StatusInternalServerError, "invalid stored handler definition")
		return
	}
	response, err := json.Marshal(struct {
		Kind     string            `json:"kind"`
		Manifest handlers.Manifest `json:"manifest"`
		Source   string            `json:"source"`
		MaxSteps int               `json:"max_steps"`
	}{"starlark", manifest, string(record.Source), record.MaxSteps})
	if err != nil || len(response) > (6<<20)+4096 {
		writeError(w, http.StatusInternalServerError, "handler definition exceeds response limit")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(response)
}

type handlerRegisterRequest struct {
	Kind     string            `json:"kind,omitempty"`
	Manifest handlers.Manifest `json:"manifest"`
	Source   string            `json:"source"`
	MaxSteps int               `json:"max_steps,omitempty"`
	Artifact string            `json:"artifact,omitempty"`
	ABI      string            `json:"abi,omitempty"`
}

func (s *server) registerHandler(w http.ResponseWriter, r *http.Request, workspaceID string) {
	s.registerHandlerWithKey(w, r, workspaceID, "")
}

func (s *server) registerFunctionVersion(w http.ResponseWriter, r *http.Request, workspaceID, functionID string) {
	if functionID == "" || strings.Contains(functionID, "/") {
		writeError(w, http.StatusNotFound, "function not found")
		return
	}
	s.registerHandlerWithKey(w, r, workspaceID, functionID)
}

func (s *server) getFunctionVersion(w http.ResponseWriter, _ *http.Request, workspaceID, functionID, version string) {
	if functionID == "" || version == "" || strings.Contains(functionID, "/") || strings.Contains(version, "/") {
		writeError(w, http.StatusNotFound, "function version not found")
		return
	}
	handler, err := s.registry.ResolveScoped(workspaceID, functionID, version)
	if err != nil {
		if errors.Is(err, handlers.ErrNotFound) {
			writeError(w, http.StatusNotFound, "function version not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "failed to load function version")
		return
	}
	writeJSON(w, http.StatusOK, handler.Manifest())
}

func (s *server) registerHandlerWithKey(w http.ResponseWriter, r *http.Request, workspaceID, expectedKey string) {
	if strings.HasPrefix(strings.ToLower(r.Header.Get("Content-Type")), "multipart/form-data") {
		s.registerWASMUploadHandlerWithKey(w, r, workspaceID, expectedKey)
		return
	}
	var input handlerRegisterRequest
	if err := decodeBody(w, r, &input); err != nil {
		writeError(w, http.StatusBadRequest, "invalid handler registration request")
		return
	}
	if expectedKey != "" {
		if input.Manifest.Key == "" {
			input.Manifest.Key = expectedKey
		} else if input.Manifest.Key != expectedKey {
			writeError(w, http.StatusBadRequest, "manifest handler_key does not match function id")
			return
		}
	}
	if input.Kind == "" {
		input.Kind = "starlark"
	}
	switch input.Kind {
	case "starlark":
		s.registerStarlarkHandler(w, r, workspaceID, input)
	case "wasm":
		s.registerWASMHandler(w, r, workspaceID, input)
	default:
		writeError(w, http.StatusBadRequest, "handler kind must be starlark or wasm")
	}
}

func (s *server) registerStarlarkHandler(w http.ResponseWriter, r *http.Request, workspaceID string, input handlerRegisterRequest) {
	handler, err := handlers.NewStarlarkHandler(handlers.StarlarkSpec{
		Manifest: input.Manifest,
		Source:   []byte(input.Source),
		MaxSteps: input.MaxSteps,
	})
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := s.registry.RegisterScoped(workspaceID, handler); err != nil {
		writeError(w, http.StatusConflict, err.Error())
		return
	}
	if s.store != nil {
		manifestJSON, marshalErr := json.Marshal(handler.Manifest())
		hash := sha256.Sum256([]byte(input.Source))
		created, saveErr := s.store.SaveStarlarkHandler(r.Context(), storage.StarlarkHandlerRecord{
			WorkspaceID:  workspaceID,
			HandlerKey:   handler.Manifest().Key,
			Version:      handler.Manifest().Version,
			ManifestJSON: manifestJSON,
			Source:       []byte(input.Source),
			MaxSteps:     input.MaxSteps,
			SourceHash:   fmt.Sprintf("%x", hash[:]),
		})
		if marshalErr != nil || saveErr != nil {
			if created {
				_ = s.store.DeleteStarlarkHandler(r.Context(), workspaceID, handler.Manifest().Key, handler.Manifest().Version, fmt.Sprintf("%x", hash[:]))
			}
			s.registry.UnregisterScoped(workspaceID, handler.Manifest().Key, handler.Manifest().Version)
			if marshalErr != nil {
				writeError(w, http.StatusInternalServerError, "failed to encode handler manifest")
			} else {
				writeError(w, http.StatusConflict, saveErr.Error())
			}
			return
		}
	}
	writeJSON(w, http.StatusCreated, handler.Manifest())
}

func (s *server) registerWASMUploadHandler(w http.ResponseWriter, r *http.Request, workspaceID string) {
	s.registerWASMUploadHandlerWithKey(w, r, workspaceID, "")
}

func (s *server) registerWASMUploadHandlerWithKey(w http.ResponseWriter, r *http.Request, workspaceID, expectedKey string) {
	if s.wasmRuntime == nil || s.wasmPush == nil {
		writeError(w, http.StatusServiceUnavailable, "wasm upload runtime or OCI registry is not configured")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, int64(handlers.MaxWASMArtifactBytes+(1<<20)))
	if err := r.ParseMultipartForm(int64(handlers.MaxWASMArtifactBytes + (1 << 20))); err != nil {
		writeError(w, http.StatusBadRequest, "invalid multipart wasm upload")
		return
	}
	manifestRaw := r.FormValue("manifest")
	if manifestRaw == "" {
		writeError(w, http.StatusBadRequest, "manifest form field is required")
		return
	}
	var manifest handlers.Manifest
	if err := json.Unmarshal([]byte(manifestRaw), &manifest); err != nil {
		writeError(w, http.StatusBadRequest, "invalid manifest JSON")
		return
	}
	if expectedKey != "" {
		if manifest.Key == "" {
			manifest.Key = expectedKey
		} else if manifest.Key != expectedKey {
			writeError(w, http.StatusBadRequest, "manifest handler_key does not match function id")
			return
		}
	}
	abi := r.FormValue("abi")
	if abi == "" {
		abi = handlers.WASMRuntimeABI
	}
	file, _, err := r.FormFile("wasm")
	if err != nil {
		writeError(w, http.StatusBadRequest, "wasm file part is required")
		return
	}
	defer file.Close()
	wasmBytes, err := io.ReadAll(io.LimitReader(file, int64(handlers.MaxWASMArtifactBytes+1)))
	if err != nil || len(wasmBytes) > handlers.MaxWASMArtifactBytes {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("wasm file exceeds %d bytes", handlers.MaxWASMArtifactBytes))
		return
	}
	// Pre-compile and validate ABI in memory BEFORE pushing to OCI registry.
	compiled, err := s.wasmRuntime.ValidateAndCompile(r.Context(), wasmBytes, abi)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	_ = compiled.Close(context.Background())

	pushCtx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	pushed, err := s.wasmPush(pushCtx, workspaceID, manifest.Key, manifest.Version, wasmBytes)
	if err != nil {
		writeError(w, http.StatusBadGateway, err.Error())
		return
	}
	s.finalizeWASMRegistration(w, r, workspaceID, manifest, abi, pushed.ArtifactRef, pushed.ManifestDigest, pushed.BlobDigest, pushed.Size, wasmBytes)
}

func (s *server) registerWASMHandler(w http.ResponseWriter, r *http.Request, workspaceID string, input handlerRegisterRequest) {
	if s.wasmRuntime == nil || s.wasmFetch == nil {
		writeError(w, http.StatusServiceUnavailable, "wasm runtime is not configured")
		return
	}
	if input.ABI == "" {
		input.ABI = handlers.WASMRuntimeABI
	}
	if input.ABI != handlers.WASMRuntimeABI && input.ABI != handlers.WASMRuntimeABIV2 {
		writeError(w, http.StatusBadRequest, "abi must be "+handlers.WASMRuntimeABI+" or "+handlers.WASMRuntimeABIV2)
		return
	}
	if input.Artifact == "" {
		writeError(w, http.StatusBadRequest, "artifact is required")
		return
	}
	fetchCtx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	artifact, err := s.wasmFetch(fetchCtx, input.Artifact)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	s.finalizeWASMRegistration(w, r, workspaceID, input.Manifest, input.ABI, input.Artifact, artifact.ManifestDigest, artifact.BlobDigest, artifact.Size, artifact.Bytes)
}

func (s *server) finalizeWASMRegistration(w http.ResponseWriter, r *http.Request, workspaceID string, manifest handlers.Manifest, abi, artifactRef, manifestDigest, blobDigest string, size int64, wasmBytes []byte) {
	load := func(ctx context.Context) ([]byte, error) {
		if s.wasmFetch == nil {
			return nil, errors.New("wasm artifact fetcher is unavailable")
		}
		fetched, err := s.wasmFetch(ctx, artifactRef)
		if err != nil {
			return nil, err
		}
		if fetched.ManifestDigest != manifestDigest || fetched.BlobDigest != blobDigest || fetched.Size != size {
			return nil, &handlers.Error{Class: "wasm_invalid", Message: "wasm artifact identity changed"}
		}
		return fetched.Bytes, nil
	}
	handler, err := s.wasmRuntime.NewHandler(r.Context(), handlers.WASMSpec{Manifest: manifest, ABI: abi, Load: load}, wasmBytes)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := s.registry.RegisterScoped(workspaceID, handler); err != nil {
		writeError(w, http.StatusConflict, err.Error())
		return
	}
	manifestJSON, marshalErr := json.Marshal(handler.Manifest())
	var created bool
	var saveErr error
	if s.store != nil && marshalErr == nil {
		created, saveErr = s.store.SaveWASMHandler(r.Context(), storage.WASMHandlerRecord{
			WorkspaceID: workspaceID, HandlerKey: handler.Manifest().Key, Version: handler.Manifest().Version, ManifestJSON: manifestJSON,
			ArtifactRef: artifactRef, ManifestDigest: manifestDigest, BlobDigest: blobDigest,
			ArtifactSize: size, RuntimeABI: abi,
		})
	}
	if marshalErr != nil || saveErr != nil {
		if created {
			_ = s.store.DeleteWASMHandler(r.Context(), workspaceID, handler.Manifest().Key, handler.Manifest().Version, manifestDigest)
		}
		s.registry.UnregisterScoped(workspaceID, handler.Manifest().Key, handler.Manifest().Version)
		if marshalErr != nil {
			writeError(w, http.StatusInternalServerError, "failed to encode handler manifest")
		} else {
			writeError(w, http.StatusConflict, saveErr.Error())
		}
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{
		"kind":            "wasm",
		"workspace_id":    workspaceID,
		"manifest":        handler.Manifest(),
		"artifact":        artifactRef,
		"manifest_digest": manifestDigest,
		"blob_digest":     blobDigest,
		"artifact_size":   size,
		"abi":             abi,
	})
}
