package main

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"strings"

	ampconnectors "github.com/mrchypark/conductor/internal/connectors"
	"github.com/mrchypark/conductor/internal/executions"
	"github.com/mrchypark/conductor/internal/handlers"
	"github.com/mrchypark/conductor/internal/storage"
)

type server struct {
	executions             *executions.Service
	registry               *handlers.Registry
	catalog                *ampconnectors.Catalog
	store                  *storage.Store
	wasmRuntime            *handlers.WASMRuntime
	wasmFetch              wasmArtifactFetcher
	wasmPush               wasmArtifactPusher
	requireWorkspaceHeader bool
	allowInsecureCustom    bool
}

func newServer(service *executions.Service, registry *handlers.Registry, catalog *ampconnectors.Catalog) *server {
	return &server{
		executions:             service,
		registry:               registry,
		catalog:                catalog,
		requireWorkspaceHeader: strings.EqualFold(os.Getenv("CONDUCTOR_REQUIRE_WORKSPACE_HEADER"), "true"),
		allowInsecureCustom:    strings.EqualFold(os.Getenv("CONDUCTOR_ALLOW_INSECURE_CUSTOM_CONNECTORS"), "true"),
	}
}

func (s *server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if strings.HasSuffix(r.URL.Path, "/handlers/definition") {
		w.Header().Set("Cache-Control", "no-store")
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
	case r.Method == http.MethodGet && r.URL.Path == "/v1/connectors":
		s.listConnectors(w, r, "")
	case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/v1/connectors/"):
		provider := strings.TrimPrefix(r.URL.Path, "/v1/connectors/")
		s.getConnector(w, r, "", provider)
	case strings.HasPrefix(r.URL.Path, "/v1/workspaces/"):
		s.serveWorkspace(w, r)
	default:
		writeError(w, http.StatusNotFound, "route not found")
	}
}

func (s *server) serveWorkspace(w http.ResponseWriter, r *http.Request) {
	rest := strings.TrimPrefix(r.URL.Path, "/v1/workspaces/")
	idx := strings.IndexByte(rest, '/')
	if idx <= 0 {
		writeError(w, http.StatusNotFound, "route not found")
		return
	}
	workspaceID, subPath := rest[:idx], rest[idx:]
	if workspaceID == "" || strings.ContainsAny(workspaceID, " \t\r\n") {
		writeError(w, http.StatusBadRequest, "invalid workspace_id")
		return
	}
	headerWS := r.Header.Get("X-Workspace-Id")
	if s.requireWorkspaceHeader && headerWS == "" {
		writeError(w, http.StatusBadRequest, "X-Workspace-Id header is required")
		return
	}
	if headerWS != "" && headerWS != workspaceID {
		writeError(w, http.StatusBadRequest, "X-Workspace-Id does not match request path")
		return
	}

	switch {
	case r.Method == http.MethodPost && subPath == "/events":
		s.ingestEvents(w, r, workspaceID)
	case r.Method == http.MethodPost && subPath == "/executions":
		s.submitExecution(w, r, workspaceID)
	case r.Method == http.MethodPost && strings.HasPrefix(subPath, "/executions/") && strings.HasSuffix(subPath, "/cancel"):
		id := strings.TrimSuffix(strings.TrimPrefix(subPath, "/executions/"), "/cancel")
		s.cancelExecution(w, r, workspaceID, id)
	case r.Method == http.MethodGet && strings.HasPrefix(subPath, "/executions/") && strings.HasSuffix(subPath, "/events"):
		id := strings.TrimSuffix(strings.TrimPrefix(subPath, "/executions/"), "/events")
		s.executionEvents(w, r, workspaceID, id)
	case r.Method == http.MethodGet && strings.HasPrefix(subPath, "/executions/"):
		id := strings.TrimPrefix(subPath, "/executions/")
		s.getExecution(w, r, workspaceID, id)
	case r.Method == http.MethodPost && subPath == "/handlers":
		s.registerHandler(w, r, workspaceID)
	case r.Method == http.MethodPost && subPath == "/handlers/wasm":
		s.registerWASMUploadHandler(w, r, workspaceID)
	case r.Method == http.MethodGet && subPath == "/handlers":
		writeJSON(w, http.StatusOK, s.registry.ManifestsForWorkspace(workspaceID))
	case r.Method == http.MethodGet && subPath == "/handlers/definition":
		s.getHandlerDefinition(w, r, workspaceID)
	case r.Method == http.MethodPost && subPath == "/functions":
		s.registerHandler(w, r, workspaceID)
	case r.Method == http.MethodGet && subPath == "/functions":
		writeJSON(w, http.StatusOK, s.registry.ManifestsForWorkspace(workspaceID))
	case r.Method == http.MethodPost && strings.HasPrefix(subPath, "/functions/") && strings.HasSuffix(subPath, "/versions"):
		id := strings.TrimSuffix(strings.TrimPrefix(subPath, "/functions/"), "/versions")
		s.registerFunctionVersion(w, r, workspaceID, id)
	case (r.Method == http.MethodGet || r.Method == http.MethodDelete) && strings.HasPrefix(subPath, "/functions/"):
		fnRest := strings.TrimPrefix(subPath, "/functions/")
		parts := strings.Split(fnRest, "/")
		switch {
		case r.Method == http.MethodGet && len(parts) == 1 && parts[0] != "":
			s.listFunctionVersions(w, r, workspaceID, parts[0])
		case r.Method == http.MethodGet && len(parts) == 3 && parts[1] == "versions":
			s.getFunctionVersion(w, r, workspaceID, parts[0], parts[2])
		case r.Method == http.MethodDelete && len(parts) == 3 && parts[1] == "versions":
			s.deleteFunctionVersion(w, r, workspaceID, parts[0], parts[2])
		default:
			writeError(w, http.StatusNotFound, "route not found")
		}
	case r.Method == http.MethodGet && subPath == "/connectors":
		s.listConnectors(w, r, workspaceID)
	case r.Method == http.MethodPost && subPath == "/connectors":
		s.registerConnector(w, r, workspaceID)
	case r.Method == http.MethodGet && strings.HasPrefix(subPath, "/connectors/"):
		provider := strings.TrimPrefix(subPath, "/connectors/")
		s.getConnector(w, r, workspaceID, provider)
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
