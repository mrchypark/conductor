package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/mrchypark/conductor/internal/persistence"
	"github.com/mrchypark/rhiza"
)

type ExecutionRequestRecord struct {
	ID, WorkspaceID, WorkflowKey                                                        string
	WorkflowVersion                                                                     uint64
	WorkflowID, TaskID, NodeKey, HandlerKey, HandlerVersion, OperationKey, ConnectionID string
	CredentialRevision                                                                  int64
	ActorType, ActorID                                                                  string
	SubjectRef, InputRef                                                                []byte
	DeadlineAt                                                                          time.Time
	ExpiresAt                                                                           time.Time
	CredentialRef                                                                       string
	Priority                                                                            int
	Input                                                                               []byte
	Timeout                                                                             time.Duration
	MaxAttempts                                                                         int
	RetryBackoff                                                                        time.Duration
	RetryBackoffMode, CorrelationID, CausationID, IdempotencyKey                        string
}
type ExecutionRecord struct {
	ID, ExecutionRequestID, Status, HandlerKey, HandlerVersion, OperationKey, ConnectionID string
	Input, Output                                                                          []byte
	ErrorClass, ErrorMessage                                                               string
	ErrorRetryable                                                                         bool
	CreatedAt, UpdatedAt                                                                   time.Time
}

type ExecutionEventRecord struct {
	ID            string          `json:"id"`
	ExecutionID   string          `json:"execution_id"`
	Type          string          `json:"event_type"`
	AttemptNumber int             `json:"attempt_number"`
	Payload       json.RawMessage `json:"payload"`
	OccurredAt    time.Time       `json:"occurred_at"`
}

func (s *Store) CreateExecution(ctx context.Context, r ExecutionRequestRecord, executionID string) (bool, error) {
	claim, e := opaque()
	if e != nil {
		return false, e
	}
	t := now()
	status := "queued"
	ss := []persistence.Statement{
		stm(`INSERT OR IGNORE INTO submission_claims(request_id,claim_id,created_at) VALUES(?,?,?)`, r.ID, claim, t),
		stm(`INSERT INTO execution_requests(id,workspace_id,workflow_key,workflow_version,workflow_instance_id,task_id,node_key,handler_key,handler_version,operation_key,connection_id,actor_type,actor_id,subject_ref,input_ref,deadline_at,input,timeout_ns,max_attempts,retry_backoff_ns,retry_backoff_mode,correlation_id,causation_id,idempotency_key,created_at,credential_ref,priority,expires_at) SELECT ?,?,?,?,? ,?,?,?,?,? ,?,?,?,?,? ,?,?,?,?,? ,?,?,?,?,?,?,?,? WHERE EXISTS(SELECT 1 FROM submission_claims WHERE request_id=? AND claim_id=?)`, r.ID, r.WorkspaceID, r.WorkflowKey, int64(r.WorkflowVersion), r.WorkflowID, r.TaskID, r.NodeKey, r.HandlerKey, r.HandlerVersion, r.OperationKey, r.ConnectionID, r.ActorType, r.ActorID, encodeBlob(r.SubjectRef), encodeBlob(r.InputRef), ft(r.DeadlineAt), encodeBlob(r.Input), r.Timeout.Nanoseconds(), r.MaxAttempts, r.RetryBackoff.Nanoseconds(), r.RetryBackoffMode, r.CorrelationID, r.CausationID, r.IdempotencyKey, t, r.CredentialRef, r.Priority, ft(r.ExpiresAt), r.ID, claim),
		stm(`INSERT INTO execution_request_credentials(execution_request_id,connection_id,credential_revision) SELECT ?,?,? WHERE EXISTS(SELECT 1 FROM submission_claims WHERE request_id=? AND claim_id=?)`, r.ID, r.ConnectionID, r.CredentialRevision, r.ID, claim),
		stm(`INSERT INTO executions(id,execution_request_id,status,handler_key,handler_version,operation_key,connection_id,input,output,error_class,error_message,error_retryable,created_at,updated_at,workspace_id,priority,expires_at) SELECT ?,?,?,?,?,?,?,?,'','','',0,?,?,?,?,? WHERE EXISTS(SELECT 1 FROM submission_claims WHERE request_id=? AND claim_id=?)`, executionID, r.ID, status, r.HandlerKey, r.HandlerVersion, r.OperationKey, r.ConnectionID, encodeBlob(r.Input), t, t, r.WorkspaceID, r.Priority, ft(r.ExpiresAt), r.ID, claim),
		stm(`INSERT INTO execution_events(id,execution_id,event_type,attempt_number,payload,occurred_at) SELECT ?,?,'execution.requested',0,'{}',? WHERE EXISTS(SELECT 1 FROM submission_claims WHERE request_id=? AND claim_id=?)`, `event_`+executionID+`:requested`, executionID, t, r.ID, claim)}
	if _, e = s.db.Mutate(ctx, "create execution "+r.ID+" "+claim, ss...); e != nil {
		return false, e
	}
	rows, e := s.db.Query(ctx, `SELECT claim_id FROM submission_claims WHERE request_id=?`, r.ID)
	if e != nil {
		return false, e
	}
	if len(rows) != 1 {
		return false, errors.New("execution submission outcome is unknown")
	}
	return str(rows[0][0]) == claim, nil
}
func (s *Store) GetExecutionRequest(ctx context.Context, id string) (ExecutionRequestRecord, error) {
	rows, e := s.db.Query(ctx, `SELECT r.id,r.workspace_id,r.workflow_key,r.workflow_version,r.workflow_instance_id,r.task_id,r.node_key,r.handler_key,r.handler_version,r.operation_key,r.connection_id,r.actor_type,r.actor_id,r.subject_ref,r.input_ref,r.deadline_at,r.input,r.timeout_ns,r.max_attempts,r.retry_backoff_ns,r.retry_backoff_mode,r.correlation_id,r.causation_id,r.idempotency_key,COALESCE(c.credential_revision,0),r.credential_ref,r.priority,r.expires_at FROM execution_requests r LEFT JOIN execution_request_credentials c ON c.execution_request_id=r.id WHERE r.id=?`, id)
	if e != nil {
		return ExecutionRequestRecord{}, e
	}
	if len(rows) == 0 {
		return ExecutionRequestRecord{}, fmt.Errorf("%w: %s", ErrNotFound, id)
	}
	x := rows[0]
	r := ExecutionRequestRecord{ID: str(x[0]), WorkspaceID: str(x[1]), WorkflowKey: str(x[2]), WorkflowVersion: uint64(i64(x[3])), WorkflowID: str(x[4]), TaskID: str(x[5]), NodeKey: str(x[6]), HandlerKey: str(x[7]), HandlerVersion: str(x[8]), OperationKey: str(x[9]), ConnectionID: str(x[10]), ActorType: str(x[11]), ActorID: str(x[12]), SubjectRef: blob(x[13]), InputRef: blob(x[14]), DeadlineAt: pt(str(x[15])), Input: blob(x[16]), Timeout: time.Duration(i64(x[17])), MaxAttempts: num(x[18]), RetryBackoff: time.Duration(i64(x[19])), RetryBackoffMode: str(x[20]), CorrelationID: str(x[21]), CausationID: str(x[22]), IdempotencyKey: str(x[23]), CredentialRevision: i64(x[24]), CredentialRef: str(x[25]), Priority: num(x[26]), ExpiresAt: pt(str(x[27]))}
	return r, nil
}
func (s *Store) FindExecutionRequestByIdempotency(ctx context.Context, ws, key string) (ExecutionRequestRecord, error) {
	rows, e := s.db.Query(ctx, `SELECT id FROM execution_requests WHERE workspace_id=? AND idempotency_key=?`, ws, key)
	if e != nil {
		return ExecutionRequestRecord{}, e
	}
	if len(rows) == 0 {
		return ExecutionRequestRecord{}, ErrNotFound
	}
	return s.GetExecutionRequest(ctx, str(rows[0][0]))
}
func (s *Store) GetExecution(ctx context.Context, id string) (ExecutionRecord, error) {
	rows, e := s.db.Query(ctx, `SELECT id,execution_request_id,status,handler_key,handler_version,operation_key,connection_id,input,output,error_class,error_message,error_retryable,created_at,updated_at FROM executions WHERE id=?`, id)
	if e != nil {
		return ExecutionRecord{}, e
	}
	if len(rows) == 0 {
		return ExecutionRecord{}, fmt.Errorf("%w: %s", ErrNotFound, id)
	}
	x := rows[0]
	return ExecutionRecord{ID: str(x[0]), ExecutionRequestID: str(x[1]), Status: str(x[2]), HandlerKey: str(x[3]), HandlerVersion: str(x[4]), OperationKey: str(x[5]), ConnectionID: str(x[6]), Input: blob(x[7]), Output: blob(x[8]), ErrorClass: str(x[9]), ErrorMessage: str(x[10]), ErrorRetryable: num(x[11]) != 0, CreatedAt: pt(str(x[12])), UpdatedAt: pt(str(x[13]))}, nil
}

func (s *Store) CompleteExecution(ctx context.Context, id, status string, out []byte, class, msg string) error {
	return s.CompleteExecutionWithAttempt(ctx, id, status, out, class, msg, false, 1)
}
func (s *Store) CompleteExecutionWithAttempt(ctx context.Context, id, status string, out []byte, class, msg string, retry bool, a int) error {
	return s.complete(ctx, id, status, out, class, msg, retry, a, "", 0, false, time.Now().UTC())
}
func (s *Store) CompleteExecutionWithLease(ctx context.Context, lease string, token int64, id, status string, out []byte, class, msg string, retry bool, a int) error {
	return s.complete(ctx, id, status, out, class, msg, retry, a, lease, token, false, time.Now().UTC())
}
func (s *Store) complete(ctx context.Context, id, status string, out []byte, class, msg string, retry bool, a int, lease string, token int64, expired bool, n time.Time) error {
	if status != "succeeded" && status != "failed" && status != "timed_out" && status != "cancelled" {
		return fmt.Errorf("invalid terminal status %q", status)
	}
	if a < 1 {
		return errors.New("attempt number must be positive")
	}
	rows, e := s.db.Query(ctx, `SELECT e.execution_request_id,r.workspace_id,r.task_id,r.workflow_instance_id,r.correlation_id,r.causation_id,r.handler_key,r.handler_version FROM executions e JOIN execution_requests r ON r.id=e.execution_request_id WHERE e.id=?`, id)
	if e != nil {
		return e
	}
	if len(rows) == 0 {
		return ErrNotFound
	}
	x := rows[0]
	request, ws, task, wf, corr, cause, handlerKey, handlerVersion := str(x[0]), str(x[1]), str(x[2]), str(x[3]), str(x[4]), str(x[5]), str(x[6]), str(x[7])
	t := ft(n)
	cb := map[string]any{"callback_id": "cb_" + id, "execution_request_id": request, "execution_id": id, "workspace_id": ws, "provider_key": handlerKey, "provider_run_ref": id, "handler_version": handlerVersion, "runtime_status": status, "attempt_count": a, "result": json.RawMessage(out), "error_class": nil, "error_detail": nil, "occurred_at": t}
	if task != "" {
		cb["task_id"] = task
	}
	if wf != "" {
		cb["workflow_instance_id"] = wf
	}
	if corr != "" {
		cb["correlation_id"] = corr
	}
	if cause != "" {
		cb["causation_id"] = cause
	}
	if class != "" {
		cb["error_class"] = class
		cb["error_detail"] = msg
	}
	payload, e := json.Marshal(cb)
	if e != nil {
		return e
	}
	claim := lease
	if claim == "" {
		claim = "unleased"
	}
	guard := `?=''`
	gargs := []any{lease}
	if lease != "" {
		op := ">"
		if expired {
			op = "<="
		}
		guard = `EXISTS(SELECT 1 FROM leases WHERE lease_id=? AND execution_id=? AND fencing_token=? AND status='active' AND expires_at` + op + `?)`
		gargs = []any{lease, id, token, t}
	}
	q := `INSERT OR IGNORE INTO terminal_claims(execution_id,claim_id,status,payload,created_at) SELECT ?,?,?,?,? WHERE EXISTS(SELECT 1 FROM executions WHERE id=? AND status='running') AND ` + guard
	encodedPayload := encodeBlob(payload)
	args := []any{id, claim, status, encodedPayload, t, id}
	args = append(args, gargs...)
	attemptStatus := status
	if status != "succeeded" {
		attemptStatus = "failed"
	}
	ss := []persistence.Statement{stm(q, args...), stm(`INSERT OR IGNORE INTO attempts(id,execution_id,attempt_number,status,error_class,error_message,started_at,finished_at) SELECT ?,?,?,?,?,?,?,? WHERE EXISTS(SELECT 1 FROM terminal_claims WHERE execution_id=? AND claim_id=? AND status=? AND payload=?)`, fmt.Sprintf("%s:%d", id, a), id, a, attemptStatus, class, msg, t, t, id, claim, status, encodedPayload), stm(`UPDATE executions SET status=?,output=?,error_class=?,error_message=?,error_retryable=?,updated_at=? WHERE id=? AND status='running' AND EXISTS(SELECT 1 FROM terminal_claims WHERE execution_id=? AND claim_id=? AND status=? AND payload=?)`, status, encodeBlob(out), class, msg, boolNum(retry), t, id, id, claim, status, encodedPayload), stm(`INSERT OR IGNORE INTO callbacks(id,execution_id,status,payload,created_at) SELECT ?,?,?,?,? WHERE EXISTS(SELECT 1 FROM terminal_claims WHERE execution_id=? AND claim_id=? AND status=? AND payload=?)`, `cb_`+id, id, status, encodedPayload, t, id, claim, status, encodedPayload), stm(`INSERT OR IGNORE INTO execution_events(id,execution_id,event_type,attempt_number,payload,occurred_at) SELECT ?,?,'execution.'||?,?,?,? WHERE EXISTS(SELECT 1 FROM terminal_claims WHERE execution_id=? AND claim_id=? AND status=? AND payload=?)`, `event_`+id+`:terminal`, id, status, a, encodedPayload, t, id, claim, status, encodedPayload)}
	if expired {
		ss = append(ss, stm(`UPDATE leases SET status='expired',updated_at=? WHERE lease_id=? AND fencing_token=? AND status='active' AND expires_at<=?`, t, lease, token, t))
	}
	if s.callbackTarget != "" {
		ss = append(ss, stm(`INSERT INTO callback_deliveries(callback_id,execution_id,target_url,payload,status,claim_id,attempt_count,next_attempt_at,last_error,created_at,updated_at) SELECT ?,?,?,?,'pending','',0,?,'',?,? WHERE EXISTS(SELECT 1 FROM terminal_claims WHERE execution_id=? AND claim_id=? AND status=? AND payload=?) ON CONFLICT(callback_id) DO UPDATE SET target_url=excluded.target_url`, `cb_`+id, id, s.callbackTarget, payload, t, t, t, id, claim, status, encodedPayload))
	}
	if _, e = s.db.Mutate(ctx, "complete "+id+" "+claim, ss...); e != nil {
		if status == "succeeded" && errors.Is(e, rhiza.ErrInvalidRequest) && strings.Contains(e.Error(), "encoded command exceeds") {
			return s.complete(ctx, id, "failed", nil, "output_too_large", "handler output exceeds the durable command size limit", false, a, lease, token, expired, n)
		}
		return e
	}
	rows, e = s.db.Query(ctx, `SELECT claim_id,status,payload FROM terminal_claims WHERE execution_id=?`, id)
	if e != nil {
		return e
	}
	if len(rows) != 1 || str(rows[0][0]) != claim {
		return errors.New("execution is already terminal or lease is stale")
	}
	return nil
}

// CancelExecution atomically transitions a non-terminal execution to cancelled,
// releases any active worker lease, and cancels scheduled retry timers.
func (s *Store) CancelExecution(ctx context.Context, id string) error {
	rows, e := s.db.Query(ctx, `SELECT e.execution_request_id,r.workspace_id,r.task_id,r.workflow_instance_id,r.correlation_id,r.causation_id,r.handler_key,r.handler_version,COALESCE((SELECT MAX(attempt_number) FROM attempts WHERE execution_id=e.id),0)+1 FROM executions e JOIN execution_requests r ON r.id=e.execution_request_id WHERE e.id=?`, id)
	if e != nil {
		return e
	}
	if len(rows) == 0 {
		return ErrNotFound
	}
	x := rows[0]
	request, ws, task, wf, corr, cause, handlerKey, handlerVersion, attempt := str(x[0]), str(x[1]), str(x[2]), str(x[3]), str(x[4]), str(x[5]), str(x[6]), str(x[7]), num(x[8])
	if attempt < 1 {
		attempt = 1
	}
	t := now()
	cb := map[string]any{"callback_id": "cb_" + id, "execution_request_id": request, "execution_id": id, "workspace_id": ws, "provider_key": handlerKey, "provider_run_ref": id, "handler_version": handlerVersion, "runtime_status": "cancelled", "attempt_count": attempt, "result": json.RawMessage(nil), "error_class": "cancelled", "error_detail": "execution cancelled", "occurred_at": t}
	if task != "" {
		cb["task_id"] = task
	}
	if wf != "" {
		cb["workflow_instance_id"] = wf
	}
	if corr != "" {
		cb["correlation_id"] = corr
	}
	if cause != "" {
		cb["causation_id"] = cause
	}
	payload, e := json.Marshal(cb)
	if e != nil {
		return e
	}
	claim := "cancel:" + id
	encodedPayload := encodeBlob(payload)
	ss := []persistence.Statement{
		stm(`INSERT OR IGNORE INTO terminal_claims(execution_id,claim_id,status,payload,created_at) SELECT ?,?,'cancelled',?,? WHERE EXISTS(SELECT 1 FROM executions WHERE id=? AND status IN('queued','retry_wait','running','inline_pending'))`, id, claim, encodedPayload, t, id),
		stm(`INSERT OR IGNORE INTO attempts(id,execution_id,attempt_number,status,error_class,error_message,started_at,finished_at) SELECT ?,?,?,'failed','cancelled','execution cancelled',?,? WHERE EXISTS(SELECT 1 FROM terminal_claims WHERE execution_id=? AND claim_id=? AND status='cancelled' AND payload=?)`, fmt.Sprintf("%s:%d", id, attempt), id, attempt, t, t, id, claim, encodedPayload),
		stm(`UPDATE executions SET status='cancelled',output=?,error_class='cancelled',error_message='execution cancelled',error_retryable=0,updated_at=? WHERE id=? AND status IN('queued','retry_wait','running','inline_pending') AND EXISTS(SELECT 1 FROM terminal_claims WHERE execution_id=? AND claim_id=? AND status='cancelled' AND payload=?)`, encodeBlob(nil), t, id, id, claim, encodedPayload),
		stm(`UPDATE leases SET status='released',updated_at=? WHERE execution_id=? AND status='active' AND EXISTS(SELECT 1 FROM terminal_claims WHERE execution_id=? AND claim_id=? AND status='cancelled' AND payload=?)`, t, id, id, claim, encodedPayload),
		stm(`UPDATE retry_timers SET status='cancelled' WHERE execution_id=? AND status='scheduled' AND EXISTS(SELECT 1 FROM terminal_claims WHERE execution_id=? AND claim_id=? AND status='cancelled' AND payload=?)`, id, id, claim, encodedPayload),
		stm(`INSERT OR IGNORE INTO callbacks(id,execution_id,status,payload,created_at) SELECT ?,?,'cancelled',?,? WHERE EXISTS(SELECT 1 FROM terminal_claims WHERE execution_id=? AND claim_id=? AND status='cancelled' AND payload=?)`, `cb_`+id, id, encodedPayload, t, id, claim, encodedPayload),
		stm(`INSERT OR IGNORE INTO execution_events(id,execution_id,event_type,attempt_number,payload,occurred_at) SELECT ?,?,'execution.cancelled',?,?,? WHERE EXISTS(SELECT 1 FROM terminal_claims WHERE execution_id=? AND claim_id=? AND status='cancelled' AND payload=?)`, `event_`+id+`:terminal`, id, attempt, encodedPayload, t, id, claim, encodedPayload),
	}
	if s.callbackTarget != "" {
		ss = append(ss, stm(`INSERT INTO callback_deliveries(callback_id,execution_id,target_url,payload,status,claim_id,attempt_count,next_attempt_at,last_error,created_at,updated_at) SELECT ?,?,?,?,'pending','',0,?,'',?,? WHERE EXISTS(SELECT 1 FROM terminal_claims WHERE execution_id=? AND claim_id=? AND status='cancelled' AND payload=?) ON CONFLICT(callback_id) DO UPDATE SET target_url=excluded.target_url`, `cb_`+id, id, s.callbackTarget, payload, t, t, t, id, claim, encodedPayload))
	}
	if _, e = s.db.Mutate(ctx, "cancel "+id, ss...); e != nil {
		return e
	}
	return nil
}

func (s *Store) ListExecutionEvents(ctx context.Context, id string) ([]ExecutionEventRecord, error) {
	rows, e := s.db.Query(ctx, `SELECT id,execution_id,event_type,attempt_number,payload,occurred_at FROM execution_events WHERE execution_id=? ORDER BY occurred_at,id`, id)
	if e != nil {
		return nil, e
	}
	out := make([]ExecutionEventRecord, 0, len(rows))
	for _, x := range rows {
		out = append(out, ExecutionEventRecord{ID: str(x[0]), ExecutionID: str(x[1]), Type: str(x[2]), AttemptNumber: num(x[3]), Payload: blob(x[4]), OccurredAt: pt(str(x[5]))})
	}
	return out, nil
}

// GetExecutionForWorkspace loads an execution only when it belongs to workspaceID.
func (s *Store) GetExecutionForWorkspace(ctx context.Context, workspaceID, id string) (ExecutionRecord, error) {
	rows, e := s.db.Query(ctx, `SELECT e.id,e.execution_request_id,e.status,e.handler_key,e.handler_version,e.operation_key,e.connection_id,e.input,e.output,e.error_class,e.error_message,e.error_retryable,e.created_at,e.updated_at FROM executions e JOIN execution_requests r ON r.id=e.execution_request_id WHERE e.id=? AND r.workspace_id=?`, id, workspaceID)
	if e != nil {
		return ExecutionRecord{}, e
	}
	if len(rows) == 0 {
		return ExecutionRecord{}, fmt.Errorf("%w: %s", ErrNotFound, id)
	}
	x := rows[0]
	return ExecutionRecord{ID: str(x[0]), ExecutionRequestID: str(x[1]), Status: str(x[2]), HandlerKey: str(x[3]), HandlerVersion: str(x[4]), OperationKey: str(x[5]), ConnectionID: str(x[6]), Input: blob(x[7]), Output: blob(x[8]), ErrorClass: str(x[9]), ErrorMessage: str(x[10]), ErrorRetryable: num(x[11]) != 0, CreatedAt: pt(str(x[12])), UpdatedAt: pt(str(x[13]))}, nil
}

// ExpireQueuedExecutions transitions queued or retry_wait executions whose
// expires_at has passed to timed_out and enqueues their terminal callbacks.
func (s *Store) ExpireQueuedExecutions(ctx context.Context, n time.Time) error {
	nowStr := ft(n)
	rows, e := s.db.Query(ctx, `SELECT e.id,e.execution_request_id,r.workspace_id,r.task_id,r.workflow_instance_id,r.correlation_id,r.causation_id,r.handler_key,r.handler_version,COALESCE((SELECT MAX(attempt_number) FROM attempts WHERE execution_id=e.id),0)+1 FROM executions e JOIN execution_requests r ON r.id=e.execution_request_id WHERE e.status IN('queued','retry_wait') AND e.expires_at<>'' AND e.expires_at<=?`, nowStr)
	if e != nil {
		return e
	}
	var firstErr error
	for _, x := range rows {
		id, request, ws, task, wf, corr, cause, handlerKey, handlerVersion, attempt := str(x[0]), str(x[1]), str(x[2]), str(x[3]), str(x[4]), str(x[5]), str(x[6]), str(x[7]), str(x[8]), num(x[9])
		if attempt < 1 {
			attempt = 1
		}
		cb := map[string]any{"callback_id": "cb_" + id, "execution_request_id": request, "execution_id": id, "workspace_id": ws, "provider_key": handlerKey, "provider_run_ref": id, "handler_version": handlerVersion, "runtime_status": "timed_out", "attempt_count": attempt, "result": json.RawMessage(nil), "error_class": "credential_expired", "error_detail": "execution credential or deadline expired", "occurred_at": nowStr}
		if task != "" {
			cb["task_id"] = task
		}
		if wf != "" {
			cb["workflow_instance_id"] = wf
		}
		if corr != "" {
			cb["correlation_id"] = corr
		}
		if cause != "" {
			cb["causation_id"] = cause
		}
		payload, err := json.Marshal(cb)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		claim := "expire:" + id
		encodedPayload := encodeBlob(payload)
		ss := []persistence.Statement{
			stm(`INSERT OR IGNORE INTO terminal_claims(execution_id,claim_id,status,payload,created_at) SELECT ?,?,'timed_out',?,? WHERE EXISTS(SELECT 1 FROM executions WHERE id=? AND status IN('queued','retry_wait') AND expires_at<>'' AND expires_at<=?)`, id, claim, encodedPayload, nowStr, id, nowStr),
			stm(`INSERT OR IGNORE INTO attempts(id,execution_id,attempt_number,status,error_class,error_message,started_at,finished_at) SELECT ?,?,?,'failed','credential_expired','execution credential or deadline expired',?,? WHERE EXISTS(SELECT 1 FROM terminal_claims WHERE execution_id=? AND claim_id=? AND status='timed_out' AND payload=?)`, fmt.Sprintf("%s:%d", id, attempt), id, attempt, nowStr, nowStr, id, claim, encodedPayload),
			stm(`UPDATE executions SET status='timed_out',output=?,error_class='credential_expired',error_message='execution credential or deadline expired',error_retryable=0,updated_at=? WHERE id=? AND status IN('queued','retry_wait') AND EXISTS(SELECT 1 FROM terminal_claims WHERE execution_id=? AND claim_id=? AND status='timed_out' AND payload=?)`, encodeBlob(nil), nowStr, id, id, claim, encodedPayload),
			stm(`UPDATE retry_timers SET status='cancelled' WHERE execution_id=? AND status='scheduled' AND EXISTS(SELECT 1 FROM terminal_claims WHERE execution_id=? AND claim_id=? AND status='timed_out' AND payload=?)`, id, id, claim, encodedPayload),
			stm(`INSERT OR IGNORE INTO callbacks(id,execution_id,status,payload,created_at) SELECT ?,?,'timed_out',?,? WHERE EXISTS(SELECT 1 FROM terminal_claims WHERE execution_id=? AND claim_id=? AND status='timed_out' AND payload=?)`, `cb_`+id, id, encodedPayload, nowStr, id, claim, encodedPayload),
			stm(`INSERT OR IGNORE INTO execution_events(id,execution_id,event_type,attempt_number,payload,occurred_at) SELECT ?,?,'execution.timed_out',?,?,? WHERE EXISTS(SELECT 1 FROM terminal_claims WHERE execution_id=? AND claim_id=? AND status='timed_out' AND payload=?)`, `event_`+id+`:terminal`, id, attempt, encodedPayload, nowStr, id, claim, encodedPayload),
		}
		if s.callbackTarget != "" {
			ss = append(ss, stm(`INSERT INTO callback_deliveries(callback_id,execution_id,target_url,payload,status,claim_id,attempt_count,next_attempt_at,last_error,created_at,updated_at) SELECT ?,?,?,?,'pending','',0,?,'',?,? WHERE EXISTS(SELECT 1 FROM terminal_claims WHERE execution_id=? AND claim_id=? AND status='timed_out' AND payload=?) ON CONFLICT(callback_id) DO UPDATE SET target_url=excluded.target_url`, `cb_`+id, id, s.callbackTarget, payload, nowStr, nowStr, nowStr, id, claim, encodedPayload))
		}
		if _, err := s.db.Mutate(ctx, "expire "+id, ss...); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
