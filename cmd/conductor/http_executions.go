package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/mrchypark/conductor/internal/executions"
	"github.com/mrchypark/conductor/internal/handlers"
	"github.com/mrchypark/conductor/internal/storage"
)

type eventBatch struct {
	Events []eventRequest `json:"events"`
}

type eventRequest struct {
	ID             string          `json:"id"`
	Name           string          `json:"name"`
	IdempotencyKey string          `json:"idempotency_key"`
	Data           json.RawMessage `json:"data"`
}

type eventExecutionContext struct {
	CredentialRevision int64
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

func (s *server) ingestEvents(w http.ResponseWriter, r *http.Request, workspaceID string) {
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
		if workspaceID != "" && request.WorkspaceID != workspaceID {
			writeError(w, http.StatusBadRequest, fmt.Sprintf("event %q: workspace_id mismatch", event.Name))
			return
		}
		requests[i].event = event
		requests[i].request = request
	}

	headerCausation := strings.TrimSpace(r.Header.Get("X-Causation-Event-ID"))
	headerAttempt := strings.TrimSpace(r.Header.Get("X-Attempt-ID"))

	results := make([]eventResult, 0, len(requests))
	for _, item := range requests {
		if item.request.CausationID == "" && headerCausation != "" {
			item.request.CausationID = headerCausation
		}
		if item.request.AttemptID == "" && headerAttempt != "" {
			item.request.AttemptID = headerAttempt
		}
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
	var executionContext map[string]json.RawMessage
	if raw := payload["_execution"]; len(raw) > 0 {
		if err := json.Unmarshal(raw, &executionContext); err != nil {
			return executions.ExecutionRequest{}, errors.New("data._execution must be a JSON object")
		}
	}
	ctx := eventExecutionContext{}
	if err := unmarshalEventContext(executionContext, &ctx); err != nil {
		return executions.ExecutionRequest{}, err
	}
	if ctx.ExecutionRequestID == "" || ctx.WorkspaceID == "" {
		return executions.ExecutionRequest{}, errors.New("data._execution.workspace_id and execution_request_id are required")
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
		if key != "_execution" && key != "subject_ref" {
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
		ConnectionID: ctx.ConnectionID, CredentialRevision: ctx.CredentialRevision, SubjectRef: subjectRef, Input: inputJSON,
		Actor: ctx.Actor, IdempotencyKey: event.IdempotencyKey,
		Timeout: time.Duration(ctx.TimeoutSeconds * float64(time.Second)), Retry: ctx.RetryPolicy,
	}
	if ctx.DeadlineAt != "" {
		deadline, err := time.Parse(time.RFC3339Nano, ctx.DeadlineAt)
		if err != nil {
			return executions.ExecutionRequest{}, errors.New("data._execution.deadline_at must be RFC3339")
		}
		request.DeadlineAt = deadline
	}
	return request, nil
}

func unmarshalEventContext(values map[string]json.RawMessage, target *eventExecutionContext) error {
	if len(values) == 0 {
		return nil
	}
	data, err := json.Marshal(values)
	if err != nil {
		return err
	}
	var wire struct {
		CredentialRevision int64                  `json:"credential_revision"`
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
		return fmt.Errorf("invalid data._execution: %w", err)
	}
	target.WorkspaceID, target.ExecutionRequestID = wire.WorkspaceID, wire.ExecutionRequestID
	target.CredentialRevision = wire.CredentialRevision
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
		return errors.New("data._execution.timeout_seconds is out of range")
	}
	return nil
}

func (s *server) executionEvents(w http.ResponseWriter, r *http.Request, workspaceID, id string) {
	if id == "" || strings.Contains(id, "/") {
		writeError(w, http.StatusNotFound, "execution not found")
		return
	}
	if _, err := s.executions.GetForWorkspace(r.Context(), workspaceID, id); err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			writeError(w, http.StatusNotFound, "execution not found")
		} else {
			writeError(w, http.StatusInternalServerError, "failed to load execution")
		}
		return
	}
	events, err := s.executions.EventsForWorkspace(r.Context(), workspaceID, id)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to load execution events")
		return
	}
	writeJSON(w, http.StatusOK, events)
}

func (s *server) cancelExecution(w http.ResponseWriter, r *http.Request, workspaceID, id string) {
	if id == "" || strings.Contains(id, "/") {
		writeError(w, http.StatusNotFound, "execution not found")
		return
	}
	execution, err := s.executions.CancelForWorkspace(r.Context(), workspaceID, id)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			writeError(w, http.StatusNotFound, "execution not found")
		} else {
			writeError(w, http.StatusConflict, err.Error())
		}
		return
	}
	writeCausationHeaders(w, execution)
	writeJSON(w, http.StatusOK, execution)
}

func (s *server) submitExecution(w http.ResponseWriter, r *http.Request, workspaceID string) {
	defer r.Body.Close()
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	var raw json.RawMessage
	var request executions.ExecutionRequest
	if err := decoder.Decode(&raw); err != nil || json.Unmarshal(raw, &request) != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON request")
		return
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		writeError(w, http.StatusBadRequest, "request must contain one JSON value")
		return
	}
	if request.WorkspaceID == "" {
		request.WorkspaceID = workspaceID
	} else if request.WorkspaceID != workspaceID {
		writeError(w, http.StatusBadRequest, "workspace_id does not match request path")
		return
	}
	if auth := strings.TrimSpace(r.Header.Get("Authorization")); len(auth) > len("Bearer ") && strings.EqualFold(auth[:len("Bearer ")], "Bearer ") {
		request.SubjectToken = strings.TrimSpace(auth[len("Bearer "):])
	}
	if request.CausationID == "" {
		request.CausationID = strings.TrimSpace(r.Header.Get("X-Causation-Event-ID"))
	}
	if request.AttemptID == "" {
		request.AttemptID = strings.TrimSpace(r.Header.Get("X-Attempt-ID"))
	}
	var execution executions.Execution
	var err error
	if strings.Contains(strings.ToLower(r.Header.Get("Prefer")), "respond-async") {
		execution, err = s.executions.Submit(r.Context(), request)
		if err == nil {
			writeCausationHeaders(w, execution)
			writeJSON(w, http.StatusAccepted, execution)
			return
		}
		writeExecutionError(w, err)
		return
	}
	execution, err = s.executions.SubmitAndExecute(r.Context(), request)
	if err != nil {
		writeExecutionError(w, err)
		return
	}
	writeCausationHeaders(w, execution)
	writeJSON(w, http.StatusOK, execution)
}

func writeCausationHeaders(w http.ResponseWriter, execution executions.Execution) {
	if execution.CausationID != "" {
		w.Header().Set("X-Causation-Event-ID", execution.CausationID)
	}
	if execution.AttemptID != "" {
		w.Header().Set("X-Attempt-ID", execution.AttemptID)
	}
}

func writeExecutionError(w http.ResponseWriter, err error) {
	var hErr *handlers.Error
	if errors.As(err, &hErr) {
		status := http.StatusBadRequest
		switch hErr.Class {
		case "unauthorized", "auth_expired", "auth_mismatch":
			status = http.StatusUnauthorized
		case "handler_not_found", "not_found":
			status = http.StatusNotFound
		case "idempotency_conflict", "conflict":
			status = http.StatusConflict
		case "goauthy_unavailable", "goauthy_misconfigured", "credential_refreshing", "credential_conflict":
			status = http.StatusServiceUnavailable
		}
		writeJSON(w, status, map[string]any{
			"error": map[string]any{
				"class":     hErr.Class,
				"message":   hErr.Message,
				"retryable": hErr.Retryable,
			},
		})
		return
	}
	if strings.Contains(err.Error(), "idempotency_conflict") {
		writeError(w, http.StatusConflict, err.Error())
		return
	}
	writeError(w, http.StatusBadRequest, err.Error())
}

func (s *server) getExecution(w http.ResponseWriter, r *http.Request, workspaceID, id string) {
	if id == "" || strings.Contains(id, "/") {
		writeError(w, http.StatusNotFound, "execution not found")
		return
	}
	execution, err := s.executions.GetForWorkspace(r.Context(), workspaceID, id)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			writeError(w, http.StatusNotFound, "execution not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "failed to load execution")
		return
	}
	writeCausationHeaders(w, execution)
	writeJSON(w, http.StatusOK, execution)
}
