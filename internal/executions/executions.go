// Package executions coordinates one durable request with one local handler call.
package executions

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/mrchypark/conductor/internal/handlers"
	"github.com/mrchypark/conductor/internal/storage"
)

type Actor struct {
	Type string `json:"type"`
	ID   string `json:"id"`
}

type RetryPolicy struct {
	MaxAttempts int           `json:"max_attempts"`
	Backoff     time.Duration `json:"-"`
	BackoffMode string        `json:"backoff,omitempty"`
}

// UnmarshalJSON accepts the service contract's human-readable backoff mode
// while retaining a duration for callers that configure the runtime directly.
func (p *RetryPolicy) UnmarshalJSON(data []byte) error {
	var wire struct {
		MaxAttempts int             `json:"max_attempts"`
		Backoff     json.RawMessage `json:"backoff"`
	}
	if err := json.Unmarshal(data, &wire); err != nil {
		return err
	}
	p.MaxAttempts = wire.MaxAttempts
	p.Backoff = 0
	p.BackoffMode = ""
	if len(wire.Backoff) == 0 || string(wire.Backoff) == "null" {
		return nil
	}
	var mode string
	if err := json.Unmarshal(wire.Backoff, &mode); err == nil {
		p.BackoffMode = mode
		return nil
	}
	var duration int64
	if err := json.Unmarshal(wire.Backoff, &duration); err != nil {
		return fmt.Errorf("retry backoff must be a mode or duration in nanoseconds: %w", err)
	}
	p.Backoff = time.Duration(duration)
	return nil
}

type ExecutionRequest struct {
	ExecutionRequestID string          `json:"execution_request_id"`
	WorkspaceID        string          `json:"workspace_id"`
	WorkflowKey        string          `json:"workflow_key"`
	WorkflowVersion    uint64          `json:"workflow_version"`
	WorkflowInstanceID string          `json:"workflow_instance_id"`
	TaskID             string          `json:"task_id"`
	NodeKey            string          `json:"node_key"`
	HandlerKey         string          `json:"handler_key"`
	HandlerVersion     string          `json:"handler_version"`
	OperationKey       string          `json:"operation_key"`
	ConnectionID       string          `json:"connection_id"`
	SubjectRef         json.RawMessage `json:"subject_ref,omitempty"`
	InputRef           json.RawMessage `json:"input_ref,omitempty"`
	DeadlineAt         time.Time       `json:"deadline_at,omitempty"`
	Actor              Actor           `json:"actor"`
	Input              json.RawMessage `json:"input"`
	Timeout            time.Duration   `json:"timeout"`
	Retry              RetryPolicy     `json:"retry"`
	CorrelationID      string          `json:"correlation_id"`
	CausationID        string          `json:"causation_id"`
	IdempotencyKey     string          `json:"idempotency_key"`
}

// UnmarshalJSON keeps the internal names stable while accepting the canonical
// Compos wire aliases (handler, operation, timeout_seconds, retry_policy).
func (r *ExecutionRequest) UnmarshalJSON(data []byte) error {
	type alias ExecutionRequest
	var wire struct {
		*alias
		Handler        string          `json:"handler"`
		HandlerKey     string          `json:"handler_key"`
		Operation      string          `json:"operation"`
		OperationKey   string          `json:"operation_key"`
		TimeoutSeconds *float64        `json:"timeout_seconds"`
		RetryPolicy    json.RawMessage `json:"retry_policy"`
	}
	wire.alias = (*alias)(r)
	if err := json.Unmarshal(data, &wire); err != nil {
		return err
	}
	if r.HandlerKey == "" {
		r.HandlerKey = wire.Handler
	}
	if r.HandlerKey == "" {
		r.HandlerKey = wire.HandlerKey
	}
	if r.OperationKey == "" {
		r.OperationKey = wire.Operation
	}
	if r.OperationKey == "" {
		r.OperationKey = wire.OperationKey
	}
	if wire.TimeoutSeconds != nil {
		if *wire.TimeoutSeconds < 0 || *wire.TimeoutSeconds > float64(math.MaxInt64)/float64(time.Second) {
			return errors.New("timeout_seconds is out of range")
		}
		r.Timeout = time.Duration(*wire.TimeoutSeconds * float64(time.Second))
	}
	if len(wire.RetryPolicy) > 0 && string(wire.RetryPolicy) != "null" {
		if err := json.Unmarshal(wire.RetryPolicy, &r.Retry); err != nil {
			return err
		}
	}
	return nil
}

func (r ExecutionRequest) MarshalJSON() ([]byte, error) {
	type alias ExecutionRequest
	data, err := json.Marshal(alias(r))
	if err != nil {
		return nil, err
	}
	var wire map[string]any
	if err := json.Unmarshal(data, &wire); err != nil {
		return nil, err
	}
	delete(wire, "handler_key")
	delete(wire, "operation_key")
	delete(wire, "timeout")
	delete(wire, "retry")
	if r.DeadlineAt.IsZero() {
		delete(wire, "deadline_at")
	}
	wire["handler"] = r.HandlerKey
	if r.OperationKey != "" {
		wire["operation"] = r.OperationKey
	}
	if r.Timeout > 0 {
		wire["timeout_seconds"] = r.Timeout.Seconds()
	}
	if r.Retry.MaxAttempts > 0 || r.Retry.Backoff > 0 || r.Retry.BackoffMode != "" {
		wire["retry_policy"] = r.Retry
	}
	return json.Marshal(wire)
}

type Execution struct {
	ID                 string          `json:"id"`
	ExecutionRequestID string          `json:"execution_request_id"`
	Status             string          `json:"status"`
	HandlerKey         string          `json:"handler_key"`
	HandlerVersion     string          `json:"handler_version"`
	OperationKey       string          `json:"operation_key"`
	ConnectionID       string          `json:"connection_id"`
	Input              json.RawMessage `json:"input"`
	Output             json.RawMessage `json:"output,omitempty"`
	Error              *handlers.Error `json:"error,omitempty"`
	CreatedAt          time.Time       `json:"created_at"`
	UpdatedAt          time.Time       `json:"updated_at"`
}

type Service struct {
	store    *storage.Store
	registry *handlers.Registry
}

// Claim is the worker-facing pull protocol payload. The worker executes the
// pinned request and reports completion/failure with the returned lease token.
type Claim struct {
	Request ExecutionRequest    `json:"request"`
	Lease   storage.LeaseRecord `json:"lease"`
}

// Poll asks the durable queue for one execution. ErrNotFound means the queue
// is currently empty and is intentionally not a service failure.
func (s *Service) Poll(ctx context.Context, workerID string, leaseTTL time.Duration) (Claim, error) {
	if workerID == "" {
		return Claim{}, errors.New("worker_id is required")
	}
	request, lease, err := s.store.ClaimNext(ctx, workerID, leaseTTL)
	if err != nil {
		return Claim{}, err
	}
	return Claim{Request: fromRecord(request), Lease: lease}, nil
}

func (s *Service) Heartbeat(ctx context.Context, workerID, leaseID string, fencingToken int64, ttl time.Duration) error {
	if _, err := s.leaseForWorker(ctx, workerID, leaseID, fencingToken); err != nil {
		return err
	}
	return s.store.HeartbeatLease(ctx, leaseID, fencingToken, ttl)
}

func (s *Service) Complete(ctx context.Context, workerID, leaseID string, fencingToken int64, executionID string, output json.RawMessage, attempt int) (Execution, error) {
	if len(output) > 0 && !json.Valid(output) {
		return Execution{}, &handlers.Error{Class: "invalid_output", Message: "worker output must be valid JSON"}
	}
	lease, err := s.leaseForWorker(ctx, workerID, leaseID, fencingToken)
	if err != nil {
		return Execution{}, err
	}
	if lease.ExecutionID != executionID || lease.AttemptNumber != attempt {
		return Execution{}, errors.New("lease does not match execution attempt")
	}
	if err := s.store.CompleteExecutionWithLease(ctx, leaseID, fencingToken, executionID, "succeeded", output, "", "", false, attempt); err != nil {
		return Execution{}, err
	}
	return s.Get(ctx, executionID)
}

func (s *Service) Fail(ctx context.Context, workerID, leaseID string, fencingToken int64, executionID string, attempt int, failure handlers.Error) (Execution, error) {
	lease, err := s.leaseForWorker(ctx, workerID, leaseID, fencingToken)
	if err != nil {
		return Execution{}, err
	}
	if lease.ExecutionID != executionID || lease.AttemptNumber != attempt {
		return Execution{}, errors.New("lease does not match execution attempt")
	}
	request, err := s.store.GetExecutionRequest(ctx, strings.TrimPrefix(executionID, "exec_"))
	if err != nil {
		return Execution{}, err
	}
	maxAttempts := request.MaxAttempts
	if maxAttempts < 1 {
		maxAttempts = 1
	}
	if failure.Retryable && attempt < maxAttempts {
		delay := request.RetryBackoff
		if delay <= 0 {
			delay = time.Second
		}
		if request.RetryBackoffMode == "exponential" {
			for i := 1; i < attempt && delay <= time.Duration(math.MaxInt64/2); i++ {
				delay *= 2
			}
		}
		if err := s.store.RetryExecutionWithLease(ctx, leaseID, fencingToken, executionID, attempt, failure.Class, failure.Message, time.Now().UTC().Add(delay)); err != nil {
			return Execution{}, err
		}
		return s.Get(ctx, executionID)
	}
	status := "failed"
	if failure.Class == "timed_out" || failure.Class == "deadline_exceeded" {
		status = "timed_out"
	} else if failure.Class == "cancelled" {
		status = "cancelled"
	}
	if err := s.store.CompleteExecutionWithLease(ctx, leaseID, fencingToken, executionID, status, nil, failure.Class, failure.Message, failure.Retryable, attempt); err != nil {
		return Execution{}, err
	}
	return s.Get(ctx, executionID)
}

func (s *Service) leaseForWorker(ctx context.Context, workerID, leaseID string, fencingToken int64) (storage.LeaseRecord, error) {
	if workerID == "" || leaseID == "" {
		return storage.LeaseRecord{}, errors.New("worker_id and lease_id are required")
	}
	lease, err := s.store.Lease(ctx, leaseID)
	if err != nil {
		return storage.LeaseRecord{}, err
	}
	if lease.WorkerID != workerID || lease.FencingToken != fencingToken || lease.ExpiresAt.Before(time.Now().UTC()) {
		return storage.LeaseRecord{}, errors.New("lease is stale or owned by another worker")
	}
	return lease, nil
}

func NewService(store *storage.Store, registry *handlers.Registry) (*Service, error) {
	if store == nil || registry == nil {
		return nil, errors.New("store and registry are required")
	}
	return &Service{store: store, registry: registry}, nil
}

func (s *Service) SetCallbackTarget(url string) { s.store.SetCallbackTarget(url) }

func (s *Service) Submit(ctx context.Context, request ExecutionRequest) (Execution, error) {
	request = normalizeRetryBudget(request)
	if err := validate(request); err != nil {
		return Execution{}, err
	}
	id := "exec_" + request.ExecutionRequestID
	if err := s.checkIdempotencyKey(ctx, request); err != nil {
		return Execution{}, err
	}
	created, err := s.store.CreateExecution(ctx, toRecord(request), id)
	if err != nil {
		if conflictErr := s.checkIdempotencyKey(ctx, request); conflictErr != nil {
			return Execution{}, conflictErr
		}
		return Execution{}, err
	}
	if !created {
		if err := s.checkIdempotency(ctx, request); err != nil {
			return Execution{}, err
		}
	}
	return s.Get(ctx, id)
}

// RunWorker drains queued executions and recovers expired leases. It is a
// deliberately small polling loop; SQLite remains the durable coordination
// point until a broker is justified by throughput measurements.
func (s *Service) RunWorker(ctx context.Context, workerID string, poll, leaseTTL time.Duration) error {
	if workerID == "" {
		workerID = "worker"
	}
	if poll <= 0 {
		poll = 250 * time.Millisecond
	}
	if leaseTTL <= 0 {
		leaseTTL = time.Minute
	}
	for {
		if err := s.store.RecoverExpiredLeases(ctx, time.Now().UTC()); err != nil && !errors.Is(err, context.Canceled) {
			return err
		}
		req, lease, err := s.store.ClaimNext(ctx, workerID, leaseTTL)
		if errors.Is(err, storage.ErrNotFound) {
			t := time.NewTimer(poll)
			select {
			case <-ctx.Done():
				t.Stop()
				return ctx.Err()
			case <-t.C:
				continue
			}
		}
		if err != nil {
			return err
		}
		request := fromRecord(req)
		if err := s.executeClaim(ctx, request, lease); err != nil {
			_ = s.store.ReleaseLease(context.Background(), lease.ID, lease.FencingToken)
			return err
		}
		_ = s.store.ReleaseLease(context.Background(), lease.ID, lease.FencingToken)
	}
}

func fromRecord(r storage.ExecutionRequestRecord) ExecutionRequest {
	return ExecutionRequest{ExecutionRequestID: r.ID, WorkspaceID: r.WorkspaceID, WorkflowKey: r.WorkflowKey, WorkflowVersion: r.WorkflowVersion, WorkflowInstanceID: r.WorkflowID, TaskID: r.TaskID, NodeKey: r.NodeKey, HandlerKey: r.HandlerKey, HandlerVersion: r.HandlerVersion, OperationKey: r.OperationKey, ConnectionID: r.ConnectionID, SubjectRef: r.SubjectRef, InputRef: r.InputRef, DeadlineAt: r.DeadlineAt, Actor: Actor{Type: r.ActorType, ID: r.ActorID}, Input: r.Input, Timeout: r.Timeout, Retry: RetryPolicy{MaxAttempts: r.MaxAttempts, Backoff: r.RetryBackoff, BackoffMode: r.RetryBackoffMode}, CorrelationID: r.CorrelationID, CausationID: r.CausationID, IdempotencyKey: r.IdempotencyKey}
}

func (s *Service) executeClaim(ctx context.Context, request ExecutionRequest, lease storage.LeaseRecord) error {
	handler, err := s.registry.Resolve(request.HandlerKey, request.HandlerVersion)
	if err != nil {
		_, err = s.finishLease(ctx, lease, lease.ExecutionID, nil, &handlers.Error{Class: "handler_not_found", Message: err.Error()}, lease.AttemptNumber)
		return err
	}
	if err := handlers.ValidateJSON(handler.Manifest().InputSchema, request.Input); err != nil {
		_, finishErr := s.finishLease(ctx, lease, lease.ExecutionID, nil, &handlers.Error{Class: "validation_failed", Message: err.Error()}, lease.AttemptNumber)
		return finishErr
	}
	callCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	timeout := request.Timeout
	if timeout <= 0 && handler.Manifest().DefaultTimeout > 0 {
		timeout = time.Duration(handler.Manifest().DefaultTimeout) * time.Second
	}
	if timeout > 0 {
		callCtx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	if !request.DeadlineAt.IsZero() {
		if !time.Now().Before(request.DeadlineAt) {
			_, err = s.finishLease(ctx, lease, lease.ExecutionID, nil, &handlers.Error{Class: "deadline_exceeded", Message: "execution deadline has passed"}, lease.AttemptNumber)
			return err
		}
		deadlineCtx, deadlineCancel := context.WithDeadline(callCtx, request.DeadlineAt)
		callCtx = deadlineCtx
		defer deadlineCancel()
	}
	maxAttempts := request.Retry.MaxAttempts
	if maxAttempts < 1 {
		maxAttempts = 1
	}
	for attempt := lease.AttemptNumber; attempt <= maxAttempts; attempt++ {
		result, execErr := handler.Execute(callCtx, handlers.Request{ExecutionID: lease.ExecutionID, ExecutionRequestID: request.ExecutionRequestID, WorkspaceID: request.WorkspaceID, WorkflowKey: request.WorkflowKey, WorkflowVersion: request.WorkflowVersion, WorkflowInstanceID: request.WorkflowInstanceID, TaskID: request.TaskID, NodeKey: request.NodeKey, HandlerKey: request.HandlerKey, HandlerVersion: request.HandlerVersion, OperationKey: request.OperationKey, ConnectionID: request.ConnectionID, SubjectRef: request.SubjectRef, InputRef: request.InputRef, DeadlineAt: request.DeadlineAt, AttemptNumber: attempt, LeaseID: lease.ID, FencingToken: lease.FencingToken, IdempotencyKey: request.IdempotencyKey, ActorType: request.Actor.Type, ActorID: request.Actor.ID, Input: request.Input})
		if execErr == nil {
			if len(result.Output) > 0 && !json.Valid(result.Output) {
				_, err = s.finishLease(ctx, lease, lease.ExecutionID, nil, &handlers.Error{Class: "invalid_output", Message: "handler output must be valid JSON"}, attempt)
				return err
			}
			if err := handlers.ValidateJSON(handler.Manifest().OutputSchema, result.Output); err != nil {
				_, finishErr := s.finishLease(ctx, lease, lease.ExecutionID, nil, &handlers.Error{Class: "invalid_output", Message: err.Error()}, attempt)
				return finishErr
			}
			_, err = s.finishLease(ctx, lease, lease.ExecutionID, result.Output, nil, attempt)
			return err
		}
		failure := classify(execErr)
		if failure.Retryable && attempt < maxAttempts {
			if err := s.store.RecordAttempt(ctx, lease.ExecutionID, attempt, failure.Class, failure.Message); err != nil {
				return err
			}
			delay := request.Retry.Backoff
			if delay <= 0 {
				delay = time.Second
			}
			if request.Retry.BackoffMode == "exponential" {
				for i := 1; i < attempt && delay <= time.Duration(math.MaxInt64/2); i++ {
					delay *= 2
				}
			}
			if err := s.store.ScheduleRetry(ctx, lease.ExecutionID, attempt, time.Now().UTC().Add(delay)); err != nil {
				return err
			}
			return nil
		}
		_, err = s.finishLease(ctx, lease, lease.ExecutionID, nil, failure, attempt)
		return err
	}
	return errors.New("execution exhausted without a terminal result")
}

// SubmitAndExecute stores a request once, leases it, executes its pinned handler
// with the requested retry policy, and returns the existing execution for duplicates.
func (s *Service) SubmitAndExecute(ctx context.Context, request ExecutionRequest) (Execution, error) {
	request = normalizeRetryBudget(request)
	if err := validate(request); err != nil {
		return Execution{}, err
	}
	executionID := "exec_" + request.ExecutionRequestID
	if err := s.checkIdempotencyKey(ctx, request); err != nil {
		return Execution{}, err
	}
	created, err := s.store.CreateExecution(ctx, toRecord(request), executionID)
	if err != nil {
		if conflictErr := s.checkIdempotencyKey(ctx, request); conflictErr != nil {
			return Execution{}, conflictErr
		}
		return Execution{}, err
	}
	if !created {
		if err := s.checkIdempotency(ctx, request); err != nil {
			return Execution{}, err
		}
		return s.waitForTerminal(ctx, executionID)
	}
	leaseTTL := request.Timeout
	if leaseTTL <= 0 {
		leaseTTL = time.Minute
	}
	lease, err := s.store.AcquireLease(ctx, executionID, "inline:"+executionID, leaseTTL, 1)
	if err != nil {
		return Execution{}, err
	}
	defer s.store.ReleaseLease(context.Background(), lease.ID, lease.FencingToken)

	handler, err := s.registry.Resolve(request.HandlerKey, request.HandlerVersion)
	if err != nil {
		return s.finishLease(ctx, lease, executionID, nil, &handlers.Error{Class: "handler_not_found", Message: err.Error()}, 1)
	}
	if err := handlers.ValidateJSON(handler.Manifest().InputSchema, request.Input); err != nil {
		return s.finishLease(ctx, lease, executionID, nil, &handlers.Error{Class: "validation_failed", Message: err.Error()}, 1)
	}
	if !request.DeadlineAt.IsZero() && !time.Now().Before(request.DeadlineAt) {
		return s.finishLease(ctx, lease, executionID, nil, &handlers.Error{Class: "deadline_exceeded", Message: "execution deadline has passed"}, 1)
	}
	if request.Timeout <= 0 && handler.Manifest().DefaultTimeout > 0 {
		request.Timeout = time.Duration(handler.Manifest().DefaultTimeout) * time.Second
	}
	executionContext := ctx
	cancel := func() {}
	if request.Timeout > 0 {
		executionContext, cancel = context.WithTimeout(ctx, request.Timeout)
	}
	if !request.DeadlineAt.IsZero() {
		var deadlineCancel context.CancelFunc
		executionContext, deadlineCancel = context.WithDeadline(executionContext, request.DeadlineAt)
		previousCancel := cancel
		cancel = func() {
			deadlineCancel()
			previousCancel()
		}
	}
	defer cancel()
	maxAttempts := request.Retry.MaxAttempts
	if maxAttempts < 1 {
		maxAttempts = 1
	}
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		result, executeErr := handler.Execute(executionContext, handlers.Request{
			ExecutionID: executionID, ExecutionRequestID: request.ExecutionRequestID, WorkspaceID: request.WorkspaceID,
			WorkflowKey: request.WorkflowKey, WorkflowVersion: request.WorkflowVersion, WorkflowInstanceID: request.WorkflowInstanceID, TaskID: request.TaskID, NodeKey: request.NodeKey, HandlerKey: request.HandlerKey,
			HandlerVersion: request.HandlerVersion, OperationKey: request.OperationKey, ConnectionID: request.ConnectionID,
			SubjectRef: append(json.RawMessage(nil), request.SubjectRef...), InputRef: append(json.RawMessage(nil), request.InputRef...),
			DeadlineAt: request.DeadlineAt, AttemptNumber: attempt, LeaseID: lease.ID, FencingToken: lease.FencingToken,
			IdempotencyKey: request.IdempotencyKey, ActorType: request.Actor.Type, ActorID: request.Actor.ID,
			Input: append(json.RawMessage(nil), request.Input...),
		})
		if executeErr == nil {
			if len(result.Output) > 0 && !json.Valid(result.Output) {
				return s.finishLease(ctx, lease, executionID, nil, &handlers.Error{Class: "invalid_output", Message: "handler output must be valid JSON"}, attempt)
			}
			if err := handlers.ValidateJSON(handler.Manifest().OutputSchema, result.Output); err != nil {
				return s.finishLease(ctx, lease, executionID, nil, &handlers.Error{Class: "invalid_output", Message: err.Error()}, attempt)
			}
			return s.finishLease(ctx, lease, executionID, result.Output, nil, attempt)
		}
		classified := classify(executeErr)
		if classified.Retryable && attempt < maxAttempts {
			if err := s.store.RecordAttempt(ctx, executionID, attempt, classified.Class, classified.Message); err != nil {
				return Execution{}, err
			}
			if err := waitBackoff(executionContext, request.Retry.Backoff, attempt); err != nil {
				return s.finishLease(ctx, lease, executionID, nil, classify(err), attempt)
			}
			continue
		}
		return s.finishLease(ctx, lease, executionID, nil, classified, attempt)
	}
	return Execution{}, errors.New("execution exhausted without a terminal result")
}

func (s *Service) checkIdempotencyKey(ctx context.Context, request ExecutionRequest) error {
	if request.IdempotencyKey == "" {
		return nil
	}
	existing, err := s.store.FindExecutionRequestByIdempotency(ctx, request.WorkspaceID, request.IdempotencyKey)
	if errors.Is(err, storage.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if existing.ID != request.ExecutionRequestID {
		return errors.New("idempotency_conflict: key is already used by another execution request")
	}
	return nil
}

func (s *Service) waitForTerminal(ctx context.Context, id string) (Execution, error) {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		execution, err := s.Get(ctx, id)
		if err != nil {
			return Execution{}, err
		}
		if execution.Status == "queued" {
			return execution, nil
		}
		switch execution.Status {
		case "succeeded", "failed", "timed_out", "cancelled":
			return execution, nil
		}
		select {
		case <-ctx.Done():
			return Execution{}, ctx.Err()
		case <-ticker.C:
		}
	}
}

func (s *Service) checkIdempotency(ctx context.Context, request ExecutionRequest) error {
	record, err := s.store.GetExecutionRequest(ctx, request.ExecutionRequestID)
	if err != nil {
		return err
	}
	if record.HandlerKey != request.HandlerKey || record.HandlerVersion != request.HandlerVersion || record.OperationKey != request.OperationKey || record.WorkspaceID != request.WorkspaceID || record.WorkflowKey != request.WorkflowKey || record.WorkflowVersion != request.WorkflowVersion || record.WorkflowID != request.WorkflowInstanceID || record.TaskID != request.TaskID || record.NodeKey != request.NodeKey || record.ConnectionID != request.ConnectionID || record.ActorType != request.Actor.Type || record.ActorID != request.Actor.ID || !bytes.Equal(record.SubjectRef, request.SubjectRef) || !bytes.Equal(record.InputRef, request.InputRef) || !record.DeadlineAt.Equal(request.DeadlineAt) || record.Timeout != request.Timeout || record.MaxAttempts != request.Retry.MaxAttempts || record.RetryBackoff != request.Retry.Backoff || record.RetryBackoffMode != request.Retry.BackoffMode || record.CorrelationID != request.CorrelationID || record.CausationID != request.CausationID || record.IdempotencyKey != request.IdempotencyKey || !bytes.Equal(record.Input, request.Input) {
		return errors.New("idempotency_conflict: execution request already exists with different content")
	}
	return nil
}

func (s *Service) finish(ctx context.Context, executionID string, output json.RawMessage, failure *handlers.Error, attempt int) (Execution, error) {
	return s.finishLease(ctx, storage.LeaseRecord{}, executionID, output, failure, attempt)
}

func (s *Service) finishLease(ctx context.Context, lease storage.LeaseRecord, executionID string, output json.RawMessage, failure *handlers.Error, attempt int) (Execution, error) {
	status, class, message := "succeeded", "", ""
	retryable := false
	if failure != nil {
		status, class, message = "failed", failure.Class, failure.Message
		if failure.Class == "timed_out" || failure.Class == "deadline_exceeded" {
			status = "timed_out"
		} else if failure.Class == "cancelled" {
			status = "cancelled"
		}
		retryable = failure.Retryable
	}
	var err error
	if lease.ID != "" {
		err = s.store.CompleteExecutionWithLease(ctx, lease.ID, lease.FencingToken, executionID, status, output, class, message, retryable, attempt)
	} else {
		err = s.store.CompleteExecutionWithAttempt(ctx, executionID, status, output, class, message, retryable, attempt)
	}
	if err != nil {
		return Execution{}, err
	}
	return s.Get(ctx, executionID)
}

func (s *Service) Get(ctx context.Context, id string) (Execution, error) {
	record, err := s.store.GetExecution(ctx, id)
	if err != nil {
		return Execution{}, err
	}
	execution := Execution{ID: record.ID, ExecutionRequestID: record.ExecutionRequestID, Status: record.Status,
		HandlerKey: record.HandlerKey, HandlerVersion: record.HandlerVersion, OperationKey: record.OperationKey,
		ConnectionID: record.ConnectionID, Input: record.Input, Output: record.Output, CreatedAt: record.CreatedAt, UpdatedAt: record.UpdatedAt}
	if record.ErrorClass != "" {
		execution.Error = &handlers.Error{Class: record.ErrorClass, Message: record.ErrorMessage, Retryable: record.ErrorRetryable}
	}
	return execution, nil
}

func (s *Service) Cancel(ctx context.Context, id string) (Execution, error) {
	e, err := s.Get(ctx, id)
	if err != nil {
		return Execution{}, err
	}
	if e.Status == "succeeded" || e.Status == "failed" || e.Status == "timed_out" || e.Status == "cancelled" {
		return e, nil
	}
	lease, err := s.store.ActiveLease(ctx, id)
	if errors.Is(err, storage.ErrNotFound) {
		lease, err = s.store.AcquireLease(ctx, id, "cancel:"+id, time.Minute, 1)
	}
	if err != nil {
		return Execution{}, err
	}
	return s.finishLease(ctx, lease, id, nil, &handlers.Error{Class: "cancelled", Message: "execution cancelled"}, lease.AttemptNumber)
}

func (s *Service) Events(ctx context.Context, id string) ([]storage.ExecutionEventRecord, error) {
	return s.store.ListExecutionEvents(ctx, id)
}

func validate(request ExecutionRequest) error {
	if request.ExecutionRequestID == "" || request.HandlerKey == "" || request.HandlerVersion == "" {
		return errors.New("execution request id, handler key, and handler version are required")
	}
	if len(request.Input) > 0 && !json.Valid(request.Input) {
		return errors.New("execution input must be valid JSON")
	}
	if request.Timeout < 0 || request.Retry.MaxAttempts < 0 || request.Retry.Backoff < 0 {
		return errors.New("timeout and retry values must not be negative")
	}
	return nil
}

// Connector mutations remain executable, but Conductor does not replay them
// until a provider adapter can prove idempotency. Reads retain the caller's
// retry budget. This uses the already-pinned operation instead of adding a
// second effect or capability model.
func normalizeRetryBudget(request ExecutionRequest) ExecutionRequest {
	if request.HandlerKey != "connector.invoke" || connectorReadOperation(request.OperationKey) || request.Retry.MaxAttempts <= 1 {
		return request
	}
	request.Retry.MaxAttempts = 1
	return request
}

func connectorReadOperation(operation string) bool {
	parts := strings.Split(strings.ToLower(strings.TrimSpace(operation)), ".")
	leaf := parts[len(parts)-1]
	switch leaf {
	case "get", "read", "list", "search", "schema":
		return true
	default:
		return false
	}
}

func classify(err error) *handlers.Error {
	if errors.Is(err, context.DeadlineExceeded) {
		return &handlers.Error{Class: "timed_out", Message: err.Error()}
	}
	if errors.Is(err, context.Canceled) {
		return &handlers.Error{Class: "cancelled", Message: err.Error()}
	}
	var classified *handlers.Error
	if errors.As(err, &classified) {
		return classified
	}
	return &handlers.Error{Class: "handler_error", Message: err.Error()}
}

func waitBackoff(ctx context.Context, base time.Duration, attempt int) error {
	if base <= 0 {
		return nil
	}
	delay := base
	for i := 1; i < attempt && delay <= time.Duration(math.MaxInt64/2); i++ {
		delay *= 2
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func toRecord(request ExecutionRequest) storage.ExecutionRequestRecord {
	return storage.ExecutionRequestRecord{ID: request.ExecutionRequestID, WorkspaceID: request.WorkspaceID,
		WorkflowKey: request.WorkflowKey, WorkflowVersion: request.WorkflowVersion, WorkflowID: request.WorkflowInstanceID,
		TaskID: request.TaskID, NodeKey: request.NodeKey, HandlerKey: request.HandlerKey, HandlerVersion: request.HandlerVersion,
		OperationKey: request.OperationKey, ConnectionID: request.ConnectionID, ActorType: request.Actor.Type, ActorID: request.Actor.ID,
		SubjectRef: append([]byte(nil), request.SubjectRef...), InputRef: append([]byte(nil), request.InputRef...), DeadlineAt: request.DeadlineAt,
		Input: append([]byte(nil), request.Input...), Timeout: request.Timeout, MaxAttempts: request.Retry.MaxAttempts, RetryBackoff: request.Retry.Backoff, RetryBackoffMode: request.Retry.BackoffMode,
		CorrelationID: request.CorrelationID, CausationID: request.CausationID, IdempotencyKey: request.IdempotencyKey}
}
