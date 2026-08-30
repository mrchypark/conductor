package main

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/mrchypark/conductor/internal/callbacks"
	"github.com/mrchypark/conductor/internal/connections"
	ampconnectors "github.com/mrchypark/conductor/internal/connectors"
	"github.com/mrchypark/conductor/internal/executions"
	"github.com/mrchypark/conductor/internal/handlers"
	"github.com/mrchypark/conductor/internal/persistence"
	"github.com/mrchypark/conductor/internal/storage"
	"golang.org/x/oauth2"
)

type server struct {
	executions   *executions.Service
	registry     *handlers.Registry
	catalog      *ampconnectors.Catalog
	store        *storage.Store
	serviceToken string
	oauthStore   *connections.Store
}

func newServer(service *executions.Service, registry *handlers.Registry, catalog *ampconnectors.Catalog) *server {
	return &server{executions: service, registry: registry, catalog: catalog, serviceToken: os.Getenv("CONDUCTOR_SERVICE_TOKEN")}
}

func (s *server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if s.serviceToken != "" && !isOAuthCallbackPath(r.URL.Path) && subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), []byte("Bearer "+s.serviceToken)) != 1 {
		writeError(w, http.StatusUnauthorized, "unauthorized")
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

func isOAuthCallbackPath(path string) bool {
	if path == "/v1/oauth/callback" {
		return true
	}
	return strings.HasPrefix(path, "/v1/oauth/") && strings.HasSuffix(path, "/callback") && strings.TrimSuffix(strings.TrimPrefix(path, "/v1/oauth/"), "/callback") != ""
}

type kavunRegisterRequest struct {
	Manifest       handlers.Manifest `json:"manifest"`
	Source         string            `json:"source"`
	AllowedModules []string          `json:"allowed_modules,omitempty"`
	MaxFrames      int               `json:"max_frames,omitempty"`
	MaxStack       int               `json:"max_stack,omitempty"`
}

// eventBatch is the wire envelope emitted by Compos' durable-engine outbox.
// It is intentionally an execution-only adapter: events without the Compos
// execution linkage are rejected instead of being interpreted as workflows.
type eventBatch struct {
	Events []eventRequest `json:"events"`
}

type eventRequest struct {
	ID             string          `json:"id"`
	Name           string          `json:"name"`
	IdempotencyKey string          `json:"idempotency_key"`
	Data           json.RawMessage `json:"data"`
}

type eventComposContext struct {
	WorkspaceID        string
	ExecutionRequestID string
	HandlerKey         string
	HandlerVersion     string
	OperationKey       string
	ConnectionID       string
	WorkflowKey        string
	WorkflowVersion    uint64
	WorkflowInstanceID string
	TaskID             string
	NodeKey            string
	Actor              executions.Actor
	DeadlineAt         string
	TimeoutSeconds     float64
	RetryPolicy        executions.RetryPolicy
}

type eventResult struct {
	EventID            string `json:"event_id,omitempty"`
	ExecutionID        string `json:"execution_id"`
	ExecutionRequestID string `json:"execution_request_id"`
	Status             string `json:"status"`
}

func (s *server) ingestEvents(w http.ResponseWriter, r *http.Request) {
	var batch eventBatch
	if err := decodeBody(w, r, &batch); err != nil || len(batch.Events) == 0 {
		writeError(w, http.StatusBadRequest, "events must contain at least one event")
		return
	}

	requests := make([]struct {
		event   eventRequest
		request executions.ExecutionRequest
	}, len(batch.Events))
	for i, event := range batch.Events {
		request, err := executionRequestFromEvent(event)
		if err != nil {
			writeError(w, http.StatusBadRequest, fmt.Sprintf("event %q: %v", event.Name, err))
			return
		}
		requests[i].event = event
		requests[i].request = request
	}

	results := make([]eventResult, 0, len(requests))
	for _, item := range requests {
		execution, err := s.executions.Submit(r.Context(), item.request)
		if err != nil {
			writeError(w, http.StatusConflict, err.Error())
			return
		}
		results = append(results, eventResult{
			EventID: item.event.ID, ExecutionID: execution.ID,
			ExecutionRequestID: execution.ExecutionRequestID, Status: execution.Status,
		})
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"accepted": true, "events": results})
}

func executionRequestFromEvent(event eventRequest) (executions.ExecutionRequest, error) {
	if strings.TrimSpace(event.Name) == "" {
		return executions.ExecutionRequest{}, errors.New("name is required")
	}
	if len(event.Data) == 0 || !json.Valid(event.Data) {
		return executions.ExecutionRequest{}, errors.New("data must be valid JSON")
	}
	var payload map[string]json.RawMessage
	if err := json.Unmarshal(event.Data, &payload); err != nil {
		return executions.ExecutionRequest{}, errors.New("data must be a JSON object")
	}
	var compos map[string]json.RawMessage
	if raw := payload["_compos"]; len(raw) > 0 {
		if err := json.Unmarshal(raw, &compos); err != nil {
			return executions.ExecutionRequest{}, errors.New("data._compos must be a JSON object")
		}
	}
	ctx := eventComposContext{}
	if err := unmarshalEventContext(compos, &ctx); err != nil {
		return executions.ExecutionRequest{}, err
	}
	if ctx.ExecutionRequestID == "" || ctx.WorkspaceID == "" {
		return executions.ExecutionRequest{}, errors.New("data._compos.workspace_id and execution_request_id are required")
	}
	if ctx.HandlerKey == "" {
		ctx.HandlerKey = event.Name
	}
	if ctx.HandlerVersion == "" {
		ctx.HandlerVersion = "1"
	}
	if ctx.OperationKey == "" {
		ctx.OperationKey = event.Name
	}
	if event.IdempotencyKey == "" {
		event.IdempotencyKey = "execution-dispatch:" + ctx.ExecutionRequestID
	}
	input := make(map[string]json.RawMessage, len(payload))
	for key, value := range payload {
		if key != "_compos" && key != "subject_ref" {
			input[key] = value
		}
	}
	inputJSON, err := json.Marshal(input)
	if err != nil {
		return executions.ExecutionRequest{}, fmt.Errorf("marshal input: %w", err)
	}
	var subjectRef json.RawMessage
	if raw := payload["subject_ref"]; len(raw) > 0 {
		subjectRef = append(json.RawMessage(nil), raw...)
	}
	request := executions.ExecutionRequest{
		ExecutionRequestID: ctx.ExecutionRequestID, WorkspaceID: ctx.WorkspaceID,
		WorkflowKey: ctx.WorkflowKey, WorkflowVersion: ctx.WorkflowVersion,
		WorkflowInstanceID: ctx.WorkflowInstanceID, TaskID: ctx.TaskID, NodeKey: ctx.NodeKey,
		HandlerKey: ctx.HandlerKey, HandlerVersion: ctx.HandlerVersion, OperationKey: ctx.OperationKey,
		ConnectionID: ctx.ConnectionID, SubjectRef: subjectRef, Input: inputJSON,
		Actor: ctx.Actor, IdempotencyKey: event.IdempotencyKey,
		Timeout: time.Duration(ctx.TimeoutSeconds * float64(time.Second)), Retry: ctx.RetryPolicy,
	}
	if ctx.DeadlineAt != "" {
		deadline, err := time.Parse(time.RFC3339Nano, ctx.DeadlineAt)
		if err != nil {
			return executions.ExecutionRequest{}, errors.New("data._compos.deadline_at must be RFC3339")
		}
		request.DeadlineAt = deadline
	}
	return request, nil
}

func unmarshalEventContext(values map[string]json.RawMessage, target *eventComposContext) error {
	if len(values) == 0 {
		return nil
	}
	data, err := json.Marshal(values)
	if err != nil {
		return err
	}
	var wire struct {
		WorkspaceID        string                 `json:"workspace_id"`
		ExecutionRequestID string                 `json:"execution_request_id"`
		Handler            string                 `json:"handler"`
		HandlerKey         string                 `json:"handler_key"`
		HandlerVersion     string                 `json:"handler_version"`
		Operation          string                 `json:"operation"`
		OperationKey       string                 `json:"operation_key"`
		ConnectionID       string                 `json:"connection_id"`
		WorkflowKey        string                 `json:"workflow_key"`
		WorkflowVersion    uint64                 `json:"workflow_version"`
		WorkflowInstanceID string                 `json:"workflow_instance_id"`
		TaskID             string                 `json:"task_id"`
		NodeKey            string                 `json:"node_key"`
		Actor              executions.Actor       `json:"actor"`
		DeadlineAt         string                 `json:"deadline_at"`
		TimeoutSeconds     float64                `json:"timeout_seconds"`
		RetryPolicy        executions.RetryPolicy `json:"retry_policy"`
	}
	if err := json.Unmarshal(data, &wire); err != nil {
		return fmt.Errorf("invalid data._compos: %w", err)
	}
	target.WorkspaceID, target.ExecutionRequestID = wire.WorkspaceID, wire.ExecutionRequestID
	target.HandlerKey, target.HandlerVersion = wire.HandlerKey, wire.HandlerVersion
	if target.HandlerKey == "" {
		target.HandlerKey = wire.Handler
	}
	target.OperationKey = wire.OperationKey
	if target.OperationKey == "" {
		target.OperationKey = wire.Operation
	}
	target.ConnectionID, target.WorkflowKey, target.WorkflowVersion = wire.ConnectionID, wire.WorkflowKey, wire.WorkflowVersion
	target.WorkflowInstanceID, target.TaskID, target.NodeKey = wire.WorkflowInstanceID, wire.TaskID, wire.NodeKey
	target.Actor, target.DeadlineAt, target.TimeoutSeconds, target.RetryPolicy = wire.Actor, wire.DeadlineAt, wire.TimeoutSeconds, wire.RetryPolicy
	if target.TimeoutSeconds < 0 || target.TimeoutSeconds > 86400 {
		return errors.New("data._compos.timeout_seconds is out of range")
	}
	return nil
}

func (s *server) registerHandler(w http.ResponseWriter, r *http.Request) {
	// Registration changes the executable surface, so it is never enabled on
	// an unauthenticated server. The global bearer check above authenticates the
	// request when CONDUCTOR_SERVICE_TOKEN is configured.
	if s.serviceToken == "" {
		writeError(w, http.StatusForbidden, "dynamic handler registration requires CONDUCTOR_SERVICE_TOKEN")
		return
	}
	var input kavunRegisterRequest
	if err := decodeBody(w, r, &input); err != nil {
		writeError(w, http.StatusBadRequest, "invalid handler registration request")
		return
	}
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

func loadDynamicHandlers(ctx context.Context, store *storage.Store, registry *handlers.Registry) error {
	if store == nil {
		return nil
	}
	records, err := store.ListDynamicHandlers(ctx)
	if err != nil {
		return err
	}
	for _, record := range records {
		sourceHash := fmt.Sprintf("%x", sha256.Sum256(record.Source))
		if sourceHash != record.SourceHash {
			return fmt.Errorf("dynamic handler %s@%s source hash mismatch", record.HandlerKey, record.Version)
		}
		var manifest handlers.Manifest
		if err := json.Unmarshal(record.ManifestJSON, &manifest); err != nil {
			return fmt.Errorf("dynamic handler %s@%s manifest: %w", record.HandlerKey, record.Version, err)
		}
		handler, err := handlers.NewKavunHandler(handlers.KavunSpec{
			Manifest: manifest, Source: record.Source, AllowedModules: record.AllowedModules,
			MaxFrames: record.MaxFrames, MaxStack: record.MaxStack, Bytecode: record.Bytecode,
			BytecodeHash: record.BytecodeHash, SourceHash: record.SourceHash, BytecodeSourceHash: record.BytecodeSourceHash, RuntimeABI: record.RuntimeABI,
		})
		if err != nil {
			return fmt.Errorf("dynamic handler %s@%s: %w", record.HandlerKey, record.Version, err)
		}
		bytecode, bytecodeHash, runtimeABI, err := handler.Bytecode()
		if err != nil {
			return fmt.Errorf("dynamic handler %s@%s bytecode: %w", record.HandlerKey, record.Version, err)
		}
		if handler.BytecodeNeedsRefresh() || record.BytecodeHash != bytecodeHash || record.RuntimeABI != runtimeABI {
			if err := store.SaveDynamicHandlerBytecode(ctx, record.HandlerKey, record.Version, bytecode, bytecodeHash, record.SourceHash, runtimeABI); err != nil {
				return fmt.Errorf("dynamic handler %s@%s bytecode cache: %w", record.HandlerKey, record.Version, err)
			}
		}
		if err := registry.Register(handler); err != nil {
			return err
		}
	}
	return nil
}

type workerPollRequest struct {
	WorkerID        string  `json:"worker_id"`
	LeaseTTLSeconds float64 `json:"lease_ttl_seconds,omitempty"`
}

type workerHeartbeatRequest struct {
	WorkerID        string  `json:"worker_id"`
	LeaseID         string  `json:"lease_id"`
	AttemptID       string  `json:"attempt_id"`
	FencingToken    int64   `json:"fencing_token"`
	LeaseTTLSeconds float64 `json:"lease_ttl_seconds,omitempty"`
}

type workerCompleteRequest struct {
	WorkerID     string          `json:"worker_id"`
	LeaseID      string          `json:"lease_id"`
	AttemptID    string          `json:"attempt_id"`
	FencingToken int64           `json:"fencing_token"`
	ExecutionID  string          `json:"execution_id"`
	Attempt      int             `json:"attempt_number"`
	Output       json.RawMessage `json:"output"`
}

type workerFailRequest struct {
	WorkerID     string `json:"worker_id"`
	LeaseID      string `json:"lease_id"`
	AttemptID    string `json:"attempt_id"`
	FencingToken int64  `json:"fencing_token"`
	ExecutionID  string `json:"execution_id"`
	Attempt      int    `json:"attempt_number"`
	ErrorClass   string `json:"error_class"`
	ErrorMessage string `json:"error_message"`
	Retryable    bool   `json:"retryable"`
}

type workerPollResponse struct {
	Status          string                       `json:"status"`
	TaskID          string                       `json:"task_id,omitempty"`
	RunID           string                       `json:"run_id,omitempty"`
	ExecutionID     string                       `json:"execution_id,omitempty"`
	AttemptID       string                       `json:"attempt_id,omitempty"`
	FencingToken    int64                        `json:"fencing_token,omitempty"`
	WorkflowID      string                       `json:"workflow_id,omitempty"`
	WorkflowVersion uint64                       `json:"workflow_version,omitempty"`
	StepID          string                       `json:"step_id,omitempty"`
	HandlerID       string                       `json:"handler_id,omitempty"`
	Input           json.RawMessage              `json:"input,omitempty"`
	Request         *executions.ExecutionRequest `json:"request,omitempty"`
	Lease           *storage.LeaseRecord         `json:"lease,omitempty"`
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

func workerDuration(seconds float64) (time.Duration, error) {
	if seconds < 0 || seconds > 86400 {
		return 0, errors.New("lease_ttl_seconds is out of range")
	}
	if seconds == 0 {
		return time.Minute, nil
	}
	return time.Duration(seconds * float64(time.Second)), nil
}

func (s *server) workerPoll(w http.ResponseWriter, r *http.Request) {
	var input workerPollRequest
	if err := decodeBody(w, r, &input); err != nil {
		writeError(w, http.StatusBadRequest, "invalid worker poll request")
		return
	}
	ttl, err := workerDuration(input.LeaseTTLSeconds)
	if err != nil || input.WorkerID == "" {
		writeError(w, http.StatusBadRequest, "worker_id and valid lease_ttl_seconds are required")
		return
	}
	claim, err := s.executions.Poll(r.Context(), input.WorkerID, ttl)
	if errors.Is(err, storage.ErrNotFound) {
		writeJSON(w, http.StatusOK, workerPollResponse{Status: "no_task_available"})
		return
	}
	if err != nil {
		writeError(w, http.StatusConflict, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, workerPollResponse{
		Status: "assigned", TaskID: claim.Request.TaskID, RunID: claim.Lease.ExecutionID,
		ExecutionID: claim.Lease.ExecutionID,
		AttemptID:   claim.Lease.ID, FencingToken: claim.Lease.FencingToken,
		WorkflowID: claim.Request.WorkflowKey, WorkflowVersion: claim.Request.WorkflowVersion,
		StepID: claim.Request.NodeKey, HandlerID: claim.Request.HandlerKey, Input: claim.Request.Input,
		Request: &claim.Request, Lease: &claim.Lease,
	})
}

func (s *server) workerHeartbeat(w http.ResponseWriter, r *http.Request) {
	var input workerHeartbeatRequest
	if err := decodeBody(w, r, &input); err != nil {
		writeError(w, http.StatusBadRequest, "invalid worker heartbeat request")
		return
	}
	ttl, err := workerDuration(input.LeaseTTLSeconds)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if input.LeaseID == "" {
		input.LeaseID = input.AttemptID
	}
	if err := s.executions.Heartbeat(r.Context(), input.WorkerID, input.LeaseID, input.FencingToken, ttl); err != nil {
		writeError(w, http.StatusConflict, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *server) workerComplete(w http.ResponseWriter, r *http.Request) {
	var input workerCompleteRequest
	if err := decodeBody(w, r, &input); err != nil {
		writeError(w, http.StatusBadRequest, "invalid worker completion request")
		return
	}
	if input.ExecutionID == "" || input.Attempt < 1 {
		writeError(w, http.StatusBadRequest, "execution_id and attempt_number are required")
		return
	}
	if input.LeaseID == "" {
		input.LeaseID = input.AttemptID
	}
	execution, err := s.executions.Complete(r.Context(), input.WorkerID, input.LeaseID, input.FencingToken, input.ExecutionID, input.Output, input.Attempt)
	if err != nil {
		writeError(w, http.StatusConflict, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, execution)
}

func (s *server) workerFail(w http.ResponseWriter, r *http.Request) {
	var input workerFailRequest
	if err := decodeBody(w, r, &input); err != nil {
		writeError(w, http.StatusBadRequest, "invalid worker failure request")
		return
	}
	if input.ExecutionID == "" || input.Attempt < 1 || input.ErrorClass == "" {
		writeError(w, http.StatusBadRequest, "execution_id, attempt_number, and error_class are required")
		return
	}
	if input.LeaseID == "" {
		input.LeaseID = input.AttemptID
	}
	execution, err := s.executions.Fail(r.Context(), input.WorkerID, input.LeaseID, input.FencingToken, input.ExecutionID, input.Attempt, handlers.Error{Class: input.ErrorClass, Message: input.ErrorMessage, Retryable: input.Retryable})
	if err != nil {
		writeError(w, http.StatusConflict, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, execution)
}

func (s *server) executionEvents(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/v1/executions/"), "/events")
	if id == "" || strings.Contains(id, "/") {
		writeError(w, http.StatusNotFound, "execution not found")
		return
	}
	if _, err := s.executions.Get(r.Context(), id); err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			writeError(w, http.StatusNotFound, "execution not found")
		} else {
			writeError(w, http.StatusInternalServerError, "failed to load execution")
		}
		return
	}
	events, err := s.executions.Events(r.Context(), id)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to load execution events")
		return
	}
	writeJSON(w, http.StatusOK, events)
}

func (s *server) cancelExecution(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/v1/executions/"), "/cancel")
	if id == "" || strings.Contains(id, "/") {
		writeError(w, http.StatusNotFound, "execution not found")
		return
	}
	execution, err := s.executions.Cancel(r.Context(), id)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			writeError(w, http.StatusNotFound, "execution not found")
		} else {
			writeError(w, http.StatusConflict, err.Error())
		}
		return
	}
	writeJSON(w, http.StatusOK, execution)
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
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&input); err != nil {
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
	} else if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&input); err != nil {
		writeError(w, http.StatusBadRequest, "invalid oauth callback")
		return
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
		if _, err := s.catalog.Info(input.Provider); err != nil {
			writeError(w, http.StatusBadRequest, "unknown connector provider")
			return
		}
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
	if len(metadata) == 0 {
		writeError(w, http.StatusBadRequest, "at least one encrypted credential value is required")
		return
	}
	if err := s.oauthStore.SaveConnection(r.Context(), input.ConnectionID, input.WorkspaceID, input.Provider, nil, input.Scopes, metadata, time.Now().UTC()); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, map[string]string{"connection_id": input.ConnectionID, "workspace_id": input.WorkspaceID, "provider": input.Provider})
}

func (s *server) submitExecution(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	var request executions.ExecutionRequest
	if err := decoder.Decode(&request); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON request")
		return
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		writeError(w, http.StatusBadRequest, "request must contain one JSON value")
		return
	}
	var execution executions.Execution
	var err error
	if s.serviceToken != "" && (request.WorkspaceID == "" || request.Actor.Type == "" || request.Actor.ID == "" || request.IdempotencyKey == "") {
		writeError(w, http.StatusBadRequest, "workspace_id, actor, and idempotency_key are required")
		return
	}
	if strings.Contains(strings.ToLower(r.Header.Get("Prefer")), "respond-async") {
		execution, err = s.executions.Submit(r.Context(), request)
		if err == nil {
			writeJSON(w, http.StatusAccepted, execution)
		}
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
		}
		return
	}
	execution, err = s.executions.SubmitAndExecute(r.Context(), request)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, execution)
}

func (s *server) getExecution(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/v1/executions/")
	if id == "" || strings.Contains(id, "/") {
		writeError(w, http.StatusNotFound, "execution not found")
		return
	}
	execution, err := s.executions.Get(r.Context(), id)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			writeError(w, http.StatusNotFound, "execution not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "failed to load execution")
		return
	}
	writeJSON(w, http.StatusOK, execution)
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}

func connectorCredentialsResolver(connectionStore *connections.Store, catalog *ampconnectors.Catalog) ampconnectors.ScopedCredentialsResolver {
	return func(ctx context.Context, connectionID, provider, workspace string) (ampconnectors.Credentials, error) {
		if connectionID == "" {
			return ampconnectors.Credentials{}, nil
		}
		if connectionStore == nil {
			return ampconnectors.Credentials{}, errors.New("connection store is not configured")
		}
		connection, err := connectionStore.LoadConnection(ctx, connectionID)
		if err != nil {
			return ampconnectors.Credentials{}, err
		}
		if connection.Provider != provider || connection.WorkspaceID != workspace {
			return ampconnectors.Credentials{}, errors.New("connection scope mismatch")
		}
		creds := ampconnectors.Credentials{OAuthToken: connection.Token, Metadata: connection.Metadata, Custom: connection.Metadata}
		if info, err := catalog.Info(provider); err == nil && info.Oauth2Opts != nil {
			creds.OAuthConfig = &oauth2.Config{ClientID: connection.Metadata["client_id"], ClientSecret: oauthClientSecret(provider), Endpoint: oauth2.Endpoint{AuthURL: info.Oauth2Opts.AuthURL, TokenURL: info.Oauth2Opts.TokenURL}, Scopes: connection.Scopes}
		}
		creds.APIKey = connection.Metadata["api_key"]
		creds.BasicUser = connection.Metadata["basic_user"]
		creds.BasicPass = connection.Metadata["basic_pass"]
		creds.TokenUpdated = func(_, token *oauth2.Token) error {
			return connectionStore.SaveToken(context.Background(), connection.ID, connection.WorkspaceID, connection.Provider, token, connection.Scopes, connection.Metadata, time.Now().UTC())
		}
		return creds, nil
	}
}

func main() {
	addr := flag.String("addr", envOr("CONDUCTOR_ADDR", ":8080"), "HTTP listen address")
	dsn := flag.String("db", envOr("CONDUCTOR_DB", "conductor.db"), "Rhiza data directory")
	flag.Parse()

	ctx := context.Background()
	config, err := rhizaConfig(*dsn)
	if err != nil {
		log.Fatal(err)
	}
	store, err := storage.Open(ctx, config)
	if err != nil {
		log.Fatal(err)
	}
	defer store.Close()
	registry := handlers.NewRegistry()
	echo, err := handlers.NewFuncHandler(handlers.Manifest{Key: "payload.echo", Version: "1", DefaultTimeout: 30, Idempotency: "pure", ResourceClass: "small"}, func(_ context.Context, request handlers.Request) (handlers.Result, error) {
		return handlers.Result{Output: append([]byte(nil), request.Input...)}, nil
	})
	if err != nil {
		log.Fatal(err)
	}
	if err := registry.Register(echo); err != nil {
		log.Fatal(err)
	}
	if err := loadDynamicHandlers(ctx, store, registry); err != nil {
		log.Fatal(err)
	}
	service, err := executions.NewService(store, registry)
	if err != nil {
		log.Fatal(err)
	}
	service.SetCallbackTarget(os.Getenv("CONDUCTOR_CALLBACK_URL"))
	callbackDelivery := &callbacks.Delivery{Store: store, Token: os.Getenv("CONDUCTOR_CALLBACK_TOKEN")}
	workerCtx, stopWorker := context.WithCancel(ctx)
	defer stopWorker()
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-workerCtx.Done():
				return
			case <-ticker.C:
				if err := callbackDelivery.RunOnce(workerCtx); err != nil {
					log.Printf("callback delivery: %v", err)
				}
			}
		}
	}()
	if workerID := os.Getenv("CONDUCTOR_WORKER_ID"); workerID != "" {
		go func() {
			if err := service.RunWorker(workerCtx, workerID, 250*time.Millisecond, time.Minute); err != nil && !errors.Is(err, context.Canceled) {
				log.Printf("execution worker: %v", err)
			}
		}()
	}
	catalog, err := ampconnectors.LoadCatalog()
	if err != nil {
		log.Fatal(fmt.Errorf("load connector catalog: %w", err))
	}
	if base := os.Getenv("COMPOS_BASE_URL"); base != "" {
		catalog.SetComposClient(&ampconnectors.ComposClient{BaseURL: base, Token: os.Getenv("COMPOS_SERVICE_TOKEN")})
	}
	var connectionStore *connections.Store
	if key := os.Getenv("CONDUCTOR_MASTER_KEY"); key != "" {
		connectionStore, err = connections.NewStore(ctx, store.Persistence(), []byte(key))
		if err != nil {
			log.Fatal(err)
		}
	}
	resolver := connectorCredentialsResolver(connectionStore, catalog)
	connectorHandler, err := catalog.NewInvokeHandlerScoped("1", nil, resolver)
	if err != nil {
		log.Fatal(err)
	}
	if err := registry.Register(connectorHandler); err != nil {
		log.Fatal(err)
	}
	for _, manifest := range registry.Manifests() {
		if err := store.SaveHandlerManifest(ctx, manifest.Key, manifest.Version, manifest); err != nil {
			log.Fatal(err)
		}
	}
	for _, provider := range catalog.Names() {
		if info, err := catalog.Info(provider); err == nil {
			if err := store.SaveConnectorManifest(ctx, provider, info); err != nil {
				log.Fatal(err)
			}
		}
	}
	log.Printf("conductor listening on %s with %d connector providers", *addr, len(catalog.Names()))
	srv := newServer(service, registry, catalog)
	srv.store = store
	srv.oauthStore = connectionStore
	if err := http.ListenAndServe(*addr, srv); err != nil {
		log.Fatal(err)
	}
}

func rhizaConfig(dataDir string) (persistence.Config, error) {
	config := persistence.Config{
		ClusterID:            envOr("CONDUCTOR_RHIZA_CLUSTER_ID", "conductor"),
		NodeID:               envOr("CONDUCTOR_RHIZA_NODE_ID", "n1"),
		DataDir:              dataDir,
		BindAddr:             envOr("CONDUCTOR_RHIZA_BIND_ADDR", "127.0.0.1:0"),
		PeerAddr:             envOr("CONDUCTOR_RHIZA_PEER_ADDR", "127.0.0.1:0"),
		AdminToken:           os.Getenv("CONDUCTOR_RHIZA_ADMIN_TOKEN"),
		ObjStoreProvider:     os.Getenv("CONDUCTOR_RHIZA_S3_PROVIDER"),
		ObjStoreEndpoint:     os.Getenv("CONDUCTOR_RHIZA_S3_ENDPOINT"),
		ObjStoreBucket:       os.Getenv("CONDUCTOR_RHIZA_S3_BUCKET"),
		ObjStorePrefix:       os.Getenv("CONDUCTOR_RHIZA_S3_PREFIX"),
		ObjStoreRegion:       os.Getenv("CONDUCTOR_RHIZA_S3_REGION"),
		ObjStoreAccessKey:    os.Getenv("CONDUCTOR_RHIZA_S3_ACCESS_KEY"),
		ObjStoreSecretKey:    os.Getenv("CONDUCTOR_RHIZA_S3_SECRET_KEY"),
		ObjStoreSessionToken: os.Getenv("CONDUCTOR_RHIZA_S3_SESSION_TOKEN"),
	}
	if value := os.Getenv("CONDUCTOR_RHIZA_S3_INSECURE"); value != "" {
		insecure, err := strconv.ParseBool(value)
		if err != nil {
			return persistence.Config{}, fmt.Errorf("parse CONDUCTOR_RHIZA_S3_INSECURE: %w", err)
		}
		config.ObjStoreInsecure = insecure
	}
	if config.ObjStoreEndpoint != "" && config.ObjStoreProvider == "" {
		config.ObjStoreProvider = "s3"
	}
	if raw := os.Getenv("CONDUCTOR_RHIZA_MEMBERS"); raw != "" {
		if err := json.Unmarshal([]byte(raw), &config.Members); err != nil {
			return persistence.Config{}, fmt.Errorf("parse CONDUCTOR_RHIZA_MEMBERS: %w", err)
		}
	}
	return config, nil
}

func envOr(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func oauthClientSecret(provider string) string {
	key := "CONDUCTOR_OAUTH_CLIENT_SECRET_" + strings.NewReplacer("-", "_", ".", "_", "/", "_").Replace(strings.ToUpper(provider))
	if secret := os.Getenv(key); secret != "" {
		return secret
	}
	return os.Getenv("CONDUCTOR_OAUTH_CLIENT_SECRET")
}
