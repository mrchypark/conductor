package main

import (
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/mrchypark/conductor/internal/executions"
	"github.com/mrchypark/conductor/internal/handlers"
	"github.com/mrchypark/conductor/internal/storage"
)

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
