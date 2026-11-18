package main

import (
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"strings"

	"github.com/mrchypark/conductor/internal/connections"
	ampconnectors "github.com/mrchypark/conductor/internal/connectors"
	"github.com/mrchypark/conductor/internal/executions"
	"github.com/mrchypark/conductor/internal/handlers"
	"github.com/mrchypark/conductor/internal/storage"
)

type server struct {
	executions   *executions.Service
	registry     *handlers.Registry
	catalog      *ampconnectors.Catalog
	store        *storage.Store
	serviceToken string
	oauthStore   *connections.Store
	wasmRuntime  *handlers.WASMRuntime
	wasmFetch    wasmArtifactFetcher
}

func newServer(service *executions.Service, registry *handlers.Registry, catalog *ampconnectors.Catalog) *server {
	return &server{executions: service, registry: registry, catalog: catalog, serviceToken: os.Getenv("CONDUCTOR_SERVICE_TOKEN")}
}

func (s *server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/v1/handlers/definition" {
		w.Header().Set("Cache-Control", "no-store")
	}
	if s.serviceToken != "" && !isOAuthCallbackPath(r.URL.Path) && subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), []byte("Bearer "+s.serviceToken)) != 1 {
		writeError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	if len(r.Header.Values("X-Conductor-Human-Authorization")) > 0 && (r.Method != http.MethodPost || r.URL.Path != "/v1/executions") {
		writeError(w, http.StatusBadRequest, "human authorization is only accepted for synchronous credential executions")
		return
	}
	switch {
	case r.Method == http.MethodPost && (r.URL.Path == "/v1/worker/poll" || r.URL.Path == "/v1/worker/tasks:poll"):
		s.workerPoll(w, r)
	case r.Method == http.MethodPost && (r.URL.Path == "/v1/worker/heartbeat" || r.URL.Path == "/v1/worker/tasks:heartbeat"):
		s.workerHeartbeat(w, r)
	case r.Method == http.MethodPost && (r.URL.Path == "/v1/worker/complete" || r.URL.Path == "/v1/worker/tasks:complete"):
		s.workerComplete(w, r)
	case r.Method == http.MethodPost && (r.URL.Path == "/v1/worker/fail" || r.URL.Path == "/v1/worker/tasks:fail"):
		s.workerFail(w, r)
	case r.Method == http.MethodPost && r.URL.Path == "/v1/events":
		s.ingestEvents(w, r)
	case r.Method == http.MethodPost && r.URL.Path == "/v1/executions":
		s.submitExecution(w, r)
	case r.Method == http.MethodPost && r.URL.Path == "/v1/connections":
		s.registerConnection(w, r)
	case r.Method == http.MethodPost && r.URL.Path == "/v1/handlers":
		s.registerHandler(w, r)
	case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/cancel") && strings.HasPrefix(r.URL.Path, "/v1/executions/"):
		s.cancelExecution(w, r)
	case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/events") && strings.HasPrefix(r.URL.Path, "/v1/executions/"):
		s.executionEvents(w, r)
	case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/v1/executions/"):
		s.getExecution(w, r)
	case r.Method == http.MethodGet && r.URL.Path == "/v1/handlers":
		writeJSON(w, http.StatusOK, s.registry.Manifests())
	case r.Method == http.MethodGet && r.URL.Path == "/v1/handlers/definition":
		s.getHandlerDefinition(w, r)
	case r.Method == http.MethodGet && r.URL.Path == "/v1/connectors":
		if s.catalog == nil {
			writeError(w, http.StatusServiceUnavailable, "connector catalog unavailable")
			return
		}
		writeJSON(w, http.StatusOK, s.catalog.Names())
	case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/v1/connectors/"):
		provider := strings.TrimPrefix(r.URL.Path, "/v1/connectors/")
		if provider == "" || strings.Contains(provider, "/") || s.catalog == nil {
			writeError(w, http.StatusNotFound, "connector not found")
			return
		}
		manifest, err := s.catalog.Manifest(provider)
		if err != nil {
			writeError(w, http.StatusNotFound, "connector not found")
			return
		}
		writeJSON(w, http.StatusOK, manifest)
	case r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/v1/oauth/") && strings.HasSuffix(r.URL.Path, "/begin"):
		s.oauthBegin(w, r)
	case (r.Method == http.MethodGet || r.Method == http.MethodPost) && isOAuthCallbackPath(r.URL.Path):
		s.oauthComplete(w, r)
	default:
		writeError(w, http.StatusNotFound, "route not found")
	}
}

func decodeBody(w http.ResponseWriter, r *http.Request, value any) error {
	defer r.Body.Close()
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	if err := decoder.Decode(value); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return errors.New("request must contain one JSON value")
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}
