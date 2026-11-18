package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"time"
	"unicode/utf8"

	"github.com/mrchypark/conductor/internal/handlers"
	"github.com/mrchypark/conductor/internal/storage"
)

func (s *server) getHandlerDefinition(w http.ResponseWriter, r *http.Request) {
	if s.serviceToken == "" {
		writeError(w, http.StatusForbidden, "handler definition requires CONDUCTOR_SERVICE_TOKEN")
		return
	}
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
	record, err := s.store.GetDynamicHandler(r.Context(), key, version)
	if errors.Is(err, storage.ErrNotFound) {
		writeError(w, http.StatusNotFound, "stored Kavun source unavailable for handler")
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
		Kind           string            `json:"kind"`
		Manifest       handlers.Manifest `json:"manifest"`
		Source         string            `json:"source"`
		AllowedModules []string          `json:"allowed_modules"`
		MaxFrames      int               `json:"max_frames"`
		MaxStack       int               `json:"max_stack"`
	}{"kavun", manifest, string(record.Source), record.AllowedModules, record.MaxFrames, record.MaxStack})
	if err != nil || len(response) > (6<<20)+4096 {
		writeError(w, http.StatusInternalServerError, "handler definition exceeds response limit")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(response)
}

type handlerRegisterRequest struct {
	Kind           string            `json:"kind,omitempty"`
	Manifest       handlers.Manifest `json:"manifest"`
	Source         string            `json:"source"`
	AllowedModules []string          `json:"allowed_modules,omitempty"`
	MaxFrames      int               `json:"max_frames,omitempty"`
	MaxStack       int               `json:"max_stack,omitempty"`
	Artifact       string            `json:"artifact,omitempty"`
	ABI            string            `json:"abi,omitempty"`
}

// eventBatch is the wire envelope emitted by a workflow service durable outbox.
// It is intentionally an execution-only adapter: events without the required
// execution linkage are rejected instead of being interpreted as workflows.

func (s *server) registerHandler(w http.ResponseWriter, r *http.Request) {
	// Registration changes the executable surface, so it is never enabled on
	// an unauthenticated server. The global bearer check above authenticates the
	// request when CONDUCTOR_SERVICE_TOKEN is configured.
	if s.serviceToken == "" {
		writeError(w, http.StatusForbidden, "dynamic handler registration requires CONDUCTOR_SERVICE_TOKEN")
		return
	}
	var input handlerRegisterRequest
	if err := decodeBody(w, r, &input); err != nil {
		writeError(w, http.StatusBadRequest, "invalid handler registration request")
		return
	}
	if input.Kind == "" {
		input.Kind = "kavun"
	}
	switch input.Kind {
	case "kavun":
		s.registerKavunHandler(w, r, input)
	case "wasm":
		s.registerWASMHandler(w, r, input)
	default:
		writeError(w, http.StatusBadRequest, "handler kind must be kavun or wasm")
	}
}

func (s *server) registerKavunHandler(w http.ResponseWriter, r *http.Request, input handlerRegisterRequest) {
	handler, err := handlers.NewKavunHandler(handlers.KavunSpec{
		Manifest: input.Manifest, Source: []byte(input.Source), AllowedModules: input.AllowedModules,
		MaxFrames: input.MaxFrames, MaxStack: input.MaxStack,
	})
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := s.registry.Register(handler); err != nil {
		writeError(w, http.StatusConflict, err.Error())
		return
	}
	if s.store != nil {
		manifestJSON, marshalErr := json.Marshal(handler.Manifest())
		hash := sha256.Sum256([]byte(input.Source))
		created, saveErr := s.store.SaveDynamicHandler(r.Context(), storage.DynamicHandlerRecord{
			HandlerKey: handler.Manifest().Key, Version: handler.Manifest().Version, ManifestJSON: manifestJSON,
			Source: []byte(input.Source), AllowedModules: input.AllowedModules, MaxFrames: input.MaxFrames,
			MaxStack: input.MaxStack, SourceHash: fmt.Sprintf("%x", hash[:]),
		})
		if marshalErr != nil || saveErr != nil {
			if created {
				_ = s.store.DeleteDynamicHandler(r.Context(), handler.Manifest().Key, handler.Manifest().Version, fmt.Sprintf("%x", hash[:]))
			}
			s.registry.Unregister(handler.Manifest().Key, handler.Manifest().Version)
			if marshalErr != nil {
				writeError(w, http.StatusInternalServerError, "failed to encode handler manifest")
			} else {
				writeError(w, http.StatusConflict, saveErr.Error())
			}
			return
		}
		bytecode, bytecodeHash, runtimeABI, encodeErr := handler.Bytecode()
		if encodeErr != nil || s.store.SaveDynamicHandlerBytecode(r.Context(), handler.Manifest().Key, handler.Manifest().Version, bytecode, bytecodeHash, fmt.Sprintf("%x", hash[:]), runtimeABI) != nil {
			if created {
				_ = s.store.DeleteDynamicHandler(r.Context(), handler.Manifest().Key, handler.Manifest().Version, fmt.Sprintf("%x", hash[:]))
			}
			s.registry.Unregister(handler.Manifest().Key, handler.Manifest().Version)
			if encodeErr != nil {
				writeError(w, http.StatusInternalServerError, "failed to encode handler bytecode")
			} else {
				writeError(w, http.StatusInternalServerError, "failed to store handler bytecode")
			}
			return
		}
	}
	writeJSON(w, http.StatusCreated, handler.Manifest())
}

func (s *server) registerWASMHandler(w http.ResponseWriter, r *http.Request, input handlerRegisterRequest) {
	if s.wasmRuntime == nil || s.wasmFetch == nil {
		writeError(w, http.StatusServiceUnavailable, "wasm runtime is not configured")
		return
	}
	if input.ABI != handlers.WASMRuntimeABI {
		writeError(w, http.StatusBadRequest, "abi must be "+handlers.WASMRuntimeABI)
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
	load := func(ctx context.Context) ([]byte, error) {
		fetched, err := s.wasmFetch(ctx, input.Artifact)
		if err != nil {
			return nil, err
		}
		if fetched.ManifestDigest != artifact.ManifestDigest || fetched.BlobDigest != artifact.BlobDigest || fetched.Size != artifact.Size {
			return nil, &handlers.Error{Class: "wasm_invalid", Message: "wasm artifact identity changed"}
		}
		return fetched.Bytes, nil
	}
	handler, err := s.wasmRuntime.NewHandler(fetchCtx, handlers.WASMSpec{Manifest: input.Manifest, Load: load}, artifact.Bytes)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := s.registry.Register(handler); err != nil {
		writeError(w, http.StatusConflict, err.Error())
		return
	}
	manifestJSON, marshalErr := json.Marshal(handler.Manifest())
	var created bool
	var saveErr error
	if s.store != nil && marshalErr == nil {
		created, saveErr = s.store.SaveWASMHandler(r.Context(), storage.WASMHandlerRecord{
			HandlerKey: handler.Manifest().Key, Version: handler.Manifest().Version, ManifestJSON: manifestJSON,
			ArtifactRef: input.Artifact, ManifestDigest: artifact.ManifestDigest, BlobDigest: artifact.BlobDigest,
			ArtifactSize: artifact.Size, RuntimeABI: handlers.WASMRuntimeABI,
		})
	}
	if marshalErr != nil || saveErr != nil {
		if created {
			_ = s.store.DeleteWASMHandler(r.Context(), handler.Manifest().Key, handler.Manifest().Version, artifact.ManifestDigest)
		}
		s.registry.Unregister(handler.Manifest().Key, handler.Manifest().Version)
		if marshalErr != nil {
			writeError(w, http.StatusInternalServerError, "failed to encode handler manifest")
		} else {
			writeError(w, http.StatusConflict, saveErr.Error())
		}
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{"kind": "wasm", "manifest": handler.Manifest(), "artifact": input.Artifact, "manifest_digest": artifact.ManifestDigest, "blob_digest": artifact.BlobDigest, "artifact_size": artifact.Size, "abi": handlers.WASMRuntimeABI})
}
