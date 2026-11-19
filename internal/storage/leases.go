package storage

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/mrchypark/conductor/internal/persistence"
)

type LeaseRecord struct {
	ID            string    `json:"lease_id"`
	ExecutionID   string    `json:"execution_id"`
	AttemptNumber int       `json:"attempt_number"`
	WorkerID      string    `json:"worker_id"`
	FencingToken  int64     `json:"fencing_token"`
	ExpiresAt     time.Time `json:"expires_at"`
}

func (s *Store) AcquireLease(ctx context.Context, executionID, worker string, ttl time.Duration, attempt int) (LeaseRecord, error) {
	return s.acquireLease(ctx, executionID, worker, ttl, attempt, false)
}

// AcquireInlineLease claims a newly submitted synchronous-only execution once.
func (s *Store) AcquireInlineLease(ctx context.Context, executionID string, ttl time.Duration) (LeaseRecord, error) {
	return s.acquireLease(ctx, executionID, "inline:"+executionID, ttl, 1, true)
}

func (s *Store) acquireLease(ctx context.Context, executionID, worker string, ttl time.Duration, attempt int, inline bool) (LeaseRecord, error) {
	if ttl <= 0 {
		ttl = time.Minute
	}
	if attempt < 1 {
		attempt = 1
	}
	id, e := opaque()
	if e != nil {
		return LeaseRecord{}, e
	}
	id = "lease_" + id
	n := time.Now().UTC()
	t, x := ft(n), ft(n.Add(ttl))
	claimable := "e.status IN('queued','retry_wait') AND NOT (e.handler_key='connector.credentials.invoke' AND e.handler_version IN('2','3'))"
	if inline {
		claimable = "e.status='inline_pending' AND e.handler_key='connector.credentials.invoke' AND e.handler_version IN('2','3')"
	}
	ss := []persistence.Statement{
		stm(`INSERT INTO leases(lease_id,execution_id,attempt_number,worker_id,fencing_token,expires_at,created_at,updated_at,status) SELECT ?,e.id,?,?,COALESCE((SELECT fencing_token FROM leases WHERE execution_id=e.id),0)+1,?,?,?,'active' FROM executions e WHERE e.id=? AND `+claimable+` ON CONFLICT(execution_id) DO UPDATE SET lease_id=excluded.lease_id,attempt_number=excluded.attempt_number,worker_id=excluded.worker_id,fencing_token=excluded.fencing_token,expires_at=excluded.expires_at,updated_at=excluded.updated_at,status='active'`, id, attempt, worker, x, t, t, executionID),
		stm(`UPDATE executions SET status='running',updated_at=? WHERE id=? AND status IN('queued','retry_wait','inline_pending') AND EXISTS(SELECT 1 FROM leases WHERE execution_id=? AND lease_id=?)`, t, executionID, executionID, id),
		stm(`UPDATE retry_timers SET status='claimed' WHERE execution_id=? AND status='scheduled' AND EXISTS(SELECT 1 FROM leases WHERE execution_id=? AND lease_id=?)`, executionID, executionID, id),
		stm(`INSERT OR IGNORE INTO execution_events(id,execution_id,event_type,attempt_number,payload,occurred_at) SELECT 'event_'||?||':started:'||fencing_token,?,'execution.started',?,'{}',? FROM leases WHERE execution_id=? AND lease_id=?`, executionID, executionID, attempt, t, executionID, id)}
	if _, e = s.db.Mutate(ctx, "acquire lease "+id, ss...); e != nil {
		return LeaseRecord{}, e
	}
	l, e := s.Lease(ctx, id)
	if errors.Is(e, ErrNotFound) {
		if _, g := s.GetExecution(ctx, executionID); errors.Is(g, ErrNotFound) {
			return l, ErrNotFound
		}
		return l, fmt.Errorf("execution %s is not claimable", executionID)
	}
	return l, e
}
func (s *Store) ClaimNext(ctx context.Context, worker string, ttl time.Duration) (ExecutionRequestRecord, LeaseRecord, error) {
	nowStr := now()
	rows, e := s.db.Query(ctx, `SELECT e.execution_request_id,COALESCE((SELECT attempt_number+1 FROM retry_timers t WHERE t.execution_id=e.id AND t.status='scheduled'),(SELECT attempt_number+1 FROM leases l WHERE l.execution_id=e.id AND l.status='expired'),1),e.id FROM executions e WHERE (e.expires_at='' OR e.expires_at>?) AND (e.status='queued' OR(e.status='retry_wait' AND EXISTS(SELECT 1 FROM retry_timers t WHERE t.execution_id=e.id AND t.status='scheduled' AND t.run_at<=?))) ORDER BY e.priority DESC,e.created_at ASC,e.id ASC LIMIT 1`, nowStr, nowStr)
	if e != nil {
		return ExecutionRequestRecord{}, LeaseRecord{}, e
	}
	if len(rows) == 0 {
		return ExecutionRequestRecord{}, LeaseRecord{}, ErrNotFound
	}
	x := rows[0]
	l, e := s.AcquireLease(ctx, str(x[2]), worker, ttl, num(x[1]))
	if e != nil {
		return ExecutionRequestRecord{}, LeaseRecord{}, e
	}
	r, e := s.GetExecutionRequest(ctx, str(x[0]))
	return r, l, e
}
func (s *Store) Lease(ctx context.Context, id string) (LeaseRecord, error) {
	rows, e := s.db.Query(ctx, `SELECT lease_id,execution_id,attempt_number,worker_id,fencing_token,expires_at FROM leases WHERE lease_id=?`, id)
	return leaseRows(rows, e)
}
func (s *Store) ActiveLease(ctx context.Context, id string) (LeaseRecord, error) {
	rows, e := s.db.Query(ctx, `SELECT lease_id,execution_id,attempt_number,worker_id,fencing_token,expires_at FROM leases WHERE execution_id=? AND status='active' AND expires_at>?`, id, now())
	return leaseRows(rows, e)
}
func leaseRows(rows [][]any, e error) (LeaseRecord, error) {
	if e != nil {
		return LeaseRecord{}, e
	}
	if len(rows) == 0 {
		return LeaseRecord{}, ErrNotFound
	}
	x := rows[0]
	return LeaseRecord{ID: str(x[0]), ExecutionID: str(x[1]), AttemptNumber: num(x[2]), WorkerID: str(x[3]), FencingToken: i64(x[4]), ExpiresAt: pt(str(x[5]))}, nil
}
func (s *Store) HeartbeatLease(ctx context.Context, id string, token int64, ttl time.Duration) error {
	if ttl <= 0 {
		ttl = time.Minute
	}
	n := time.Now().UTC()
	x := ft(n.Add(ttl))
	_, e := s.db.Mutate(ctx, fmt.Sprintf("heartbeat %s %d %s", id, token, ft(n)), stm(`UPDATE leases SET expires_at=?,updated_at=? WHERE lease_id=? AND fencing_token=? AND status='active' AND expires_at>?`, x, ft(n), id, token, ft(n)))
	if e != nil {
		return e
	}
	l, e := s.Lease(ctx, id)
	if e != nil || l.FencingToken != token || ft(l.ExpiresAt) != x {
		return errors.New("lease is stale or expired")
	}
	return nil
}
func (s *Store) ReleaseLease(ctx context.Context, id string, token int64) error {
	_, e := s.db.Mutate(ctx, fmt.Sprintf("release %s %d", id, token), stm(`UPDATE leases SET status='released',updated_at=? WHERE lease_id=? AND fencing_token=? AND status='active'`, now(), id, token))
	if e != nil {
		return e
	}
	l, e := s.Lease(ctx, id)
	if e != nil || l.FencingToken != token {
		return errors.New("lease is stale or already released")
	}
	return nil
}

func (s *Store) RecordAttempt(ctx context.Context, id string, a int, class, msg string) error {
	if a < 1 {
		return errors.New("attempt number must be positive")
	}
	t := now()
	_, e := s.db.Mutate(ctx, fmt.Sprintf("attempt %s %d", id, a), stm(`INSERT OR IGNORE INTO attempts(id,execution_id,attempt_number,status,error_class,error_message,started_at,finished_at) VALUES(?,?,?,'failed',?,?,?,?)`, fmt.Sprintf("%s:%d", id, a), id, a, class, msg, t, t))
	return e
}
func (s *Store) ScheduleRetry(ctx context.Context, id string, a int, run time.Time) error {
	if a < 1 {
		return errors.New("attempt number must be positive")
	}
	t := now()
	_, e := s.db.Mutate(ctx, fmt.Sprintf("retry %s %d", id, a), stm(`INSERT INTO retry_timers(id,execution_id,attempt_number,run_at,status) SELECT ?,?,?,?,'scheduled' WHERE EXISTS(SELECT 1 FROM executions WHERE id=? AND status='running') ON CONFLICT(execution_id,attempt_number) DO UPDATE SET run_at=excluded.run_at,status='scheduled'`, fmt.Sprintf("retry_%s_%d", id, a), id, a, ft(run), id), stm(`UPDATE executions SET status='retry_wait',updated_at=? WHERE id=? AND status='running'`, t, id), stm(`INSERT OR IGNORE INTO execution_events(id,execution_id,event_type,attempt_number,payload,occurred_at) SELECT ?,?,'execution.retrying',?,'{}',? WHERE EXISTS(SELECT 1 FROM executions WHERE id=? AND status='retry_wait')`, fmt.Sprintf("event_%s:retry:%d", id, a), id, a, t, id))
	if e != nil {
		return e
	}
	x, e := s.GetExecution(ctx, id)
	if e != nil {
		return e
	}
	if x.Status != "retry_wait" {
		return fmt.Errorf("execution %s is not running", id)
	}
	return nil
}
func (s *Store) RetryExecutionWithLease(ctx context.Context, lease string, token int64, id string, a int, class, msg string, run time.Time) error {
	if a < 1 {
		return errors.New("attempt number must be positive")
	}
	t := now()
	guard := `EXISTS(SELECT 1 FROM executions WHERE id=? AND status='running') AND EXISTS(SELECT 1 FROM leases WHERE lease_id=? AND execution_id=? AND fencing_token=? AND attempt_number=? AND status='active' AND expires_at>?)`
	_, e := s.db.Mutate(ctx, fmt.Sprintf("retry-leased %s %d %s", id, a, lease),
		stm(`INSERT OR IGNORE INTO attempts(id,execution_id,attempt_number,status,error_class,error_message,started_at,finished_at) SELECT ?,?,?,'failed',?,?,?,? WHERE `+guard, fmt.Sprintf("%s:%d", id, a), id, a, class, msg, t, t, id, lease, id, token, a, t),
		stm(`INSERT INTO retry_timers(id,execution_id,attempt_number,run_at,status) SELECT ?,?,?,?,'scheduled' WHERE `+guard+` ON CONFLICT(execution_id,attempt_number) DO UPDATE SET run_at=excluded.run_at,status='scheduled'`, fmt.Sprintf("retry_%s_%d", id, a), id, a, ft(run), id, lease, id, token, a, t),
		stm(`UPDATE executions SET status='retry_wait',updated_at=? WHERE id=? AND status='running' AND EXISTS(SELECT 1 FROM leases WHERE lease_id=? AND execution_id=? AND fencing_token=? AND attempt_number=? AND status='active' AND expires_at>?)`, t, id, lease, id, token, a, t),
		stm(`INSERT OR IGNORE INTO execution_events(id,execution_id,event_type,attempt_number,payload,occurred_at) SELECT ?,?,'execution.retrying',?,'{}',? WHERE EXISTS(SELECT 1 FROM executions WHERE id=? AND status='retry_wait')`, fmt.Sprintf("event_%s:retry:%d", id, a), id, a, t, id),
	)
	if e != nil {
		return e
	}
	x, e := s.GetExecution(ctx, id)
	if e != nil {
		return e
	}
	if x.Status != "retry_wait" {
		return errors.New("lease is stale or expired")
	}
	return nil
}

func (s *Store) RecoverExpiredLeases(ctx context.Context, n time.Time) error {
	return s.recoverExpiredLeases(ctx, n, false)
}

// RecoverExpiredExecutions transitions expired queued/retry_wait executions to timed_out.
func (s *Store) RecoverExpiredExecutions(ctx context.Context, n time.Time) error {
	return s.ExpireQueuedExecutions(ctx, n)
}

// RecoverExpiredInlineLeases recovers only synchronous credential invocations.
func (s *Store) RecoverExpiredInlineLeases(ctx context.Context, n time.Time) error {
	return s.recoverExpiredLeases(ctx, n, true)
}

func (s *Store) recoverExpiredLeases(ctx context.Context, n time.Time, inlineOnly bool) error {
	pending, e := s.db.Query(ctx, `SELECT id FROM executions WHERE status='inline_pending' AND handler_key='connector.credentials.invoke' AND handler_version IN('2','3') AND updated_at<=?`, ft(n.Add(-time.Minute)))
	if e != nil {
		return e
	}
	var firstErr error
	for _, row := range pending {
		id := str(row[0])
		lease, err := s.AcquireInlineLease(ctx, id, time.Minute)
		if err != nil {
			current, getErr := s.GetExecution(ctx, id)
			if (getErr != nil || current.Status == "inline_pending") && firstErr == nil {
				firstErr = err
			}
			continue
		}
		if err := s.CompleteExecutionWithLease(ctx, lease.ID, lease.FencingToken, id, "failed", nil, "outcome_unknown", "inline execution was interrupted before its outcome was recorded", false, 1); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	query := `SELECT e.id,l.lease_id,l.fencing_token,l.attempt_number,CASE WHEN e.handler_key='connector.credentials.invoke' AND e.handler_version IN('2','3') THEN 1 ELSE r.max_attempts END FROM executions e JOIN leases l ON l.execution_id=e.id JOIN execution_requests r ON r.id=e.execution_request_id WHERE e.status='running' AND l.status='active' AND l.expires_at<=?`
	if inlineOnly {
		query += ` AND e.handler_key='connector.credentials.invoke' AND e.handler_version IN('2','3')`
	}
	rows, e := s.db.Query(ctx, query, ft(n))
	if e != nil {
		return e
	}
	for _, x := range rows {
		id, lease := str(x[0]), str(x[1])
		token := i64(x[2])
		a, max := num(x[3]), num(x[4])
		if max < 1 {
			max = 1
		}
		if a >= max {
			e = s.complete(ctx, id, "failed", nil, "outcome_unknown", "worker lease expired after execution may have produced side effects", false, a, lease, token, true, n)
		} else {
			_, e = s.db.Mutate(ctx, "recover "+lease, stm(`UPDATE leases SET status='expired',updated_at=? WHERE lease_id=? AND fencing_token=? AND status='active' AND expires_at<=?`, ft(n), lease, token, ft(n)), stm(`UPDATE executions SET status='queued',updated_at=? WHERE id=? AND status='running' AND EXISTS(SELECT 1 FROM leases WHERE lease_id=? AND status='expired')`, ft(n), id, lease))
		}
		if e != nil && firstErr == nil {
			firstErr = e
		}
	}
	return firstErr
}
