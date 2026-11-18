package storage

import (
	"context"
	"errors"
	"time"

	"github.com/mrchypark/conductor/internal/persistence"
)

type CallbackDeliveryRecord struct {
	ID, ExecutionID, TargetURL string
	ClaimID                    string
	Payload                    []byte
	Status                     string
	AttemptCount               int
	NextAttemptAt              time.Time
	LastError                  string
}

type CallbackRecord struct {
	ID, ExecutionID, Status string
	Payload                 []byte
}

func (s *Store) EnqueueCallbackDelivery(ctx context.Context, id, url string) error {
	if url == "" {
		return nil
	}
	t := now()
	_, e := s.db.Mutate(ctx, "enqueue callback "+id, stm(`INSERT INTO callback_deliveries(callback_id,execution_id,target_url,payload,status,claim_id,attempt_count,next_attempt_at,last_error,created_at,updated_at) SELECT id,execution_id,?,payload,'pending','',0,?,'',?,? FROM callbacks WHERE id=? ON CONFLICT(callback_id) DO UPDATE SET target_url=excluded.target_url`, url, t, t, t, id))
	return e
}
func (s *Store) DueCallbackDeliveries(ctx context.Context, n time.Time, limit int) ([]CallbackDeliveryRecord, error) {
	if limit <= 0 {
		limit = 20
	}
	claim, e := opaque()
	if e != nil {
		return nil, e
	}
	rows, e := s.db.Query(ctx, `SELECT callback_id FROM callback_deliveries WHERE status IN('pending','retry_wait') AND next_attempt_at<=? ORDER BY next_attempt_at,callback_id LIMIT ?`, ft(n), limit)
	if e != nil || len(rows) == 0 {
		return nil, e
	}
	ss := make([]persistence.Statement, 0, len(rows))
	for _, x := range rows {
		ss = append(ss, stm(`UPDATE callback_deliveries SET status='sending',claim_id=?,updated_at=? WHERE callback_id=? AND status IN('pending','retry_wait') AND next_attempt_at<=?`, claim, ft(n), str(x[0]), ft(n)))
	}
	if _, e = s.db.Mutate(ctx, "claim callbacks "+claim, ss...); e != nil {
		return nil, e
	}
	rows, e = s.db.Query(ctx, `SELECT callback_id,execution_id,target_url,payload,status,attempt_count,next_attempt_at,last_error FROM callback_deliveries WHERE claim_id=? AND status='sending'`, claim)
	if e != nil {
		return nil, e
	}
	out := make([]CallbackDeliveryRecord, 0, len(rows))
	for _, x := range rows {
		out = append(out, CallbackDeliveryRecord{ID: str(x[0]), ExecutionID: str(x[1]), TargetURL: str(x[2]), ClaimID: claim, Payload: blob(x[3]), Status: str(x[4]), AttemptCount: num(x[5]), NextAttemptAt: pt(str(x[6])), LastError: str(x[7])})
	}
	return out, nil
}
func (s *Store) RecoverCallbackDeliveries(ctx context.Context, n time.Time, age time.Duration) error {
	if age <= 0 {
		age = 30 * time.Second
	}
	_, e := s.db.Mutate(ctx, "recover callbacks "+ft(n), stm(`UPDATE callback_deliveries SET status='retry_wait',claim_id='',next_attempt_at=?,last_error='stale delivery claim recovered',updated_at=? WHERE status='sending' AND updated_at<=?`, ft(n), ft(n), ft(n.Add(-age))))
	return e
}
func (s *Store) MarkCallbackDelivery(ctx context.Context, id, claim, status, last string, next time.Time) error {
	if claim == "" {
		return errors.New("callback claim is required")
	}
	t := now()
	_, e := s.db.Mutate(ctx, "mark callback "+id+" "+status+" "+t, stm(`UPDATE callback_deliveries SET status=?,claim_id='',attempt_count=attempt_count+1,last_error=?,next_attempt_at=?,updated_at=? WHERE callback_id=? AND status='sending' AND claim_id=?`, status, last, ft(next), t, id, claim))
	return e
}

func (s *Store) Callback(ctx context.Context, executionID string) (CallbackRecord, error) {
	rows, err := s.db.Query(ctx, `SELECT id,execution_id,status,payload FROM callbacks WHERE execution_id=?`, executionID)
	if err != nil {
		return CallbackRecord{}, err
	}
	if len(rows) == 0 {
		return CallbackRecord{}, ErrNotFound
	}
	return CallbackRecord{ID: str(rows[0][0]), ExecutionID: str(rows[0][1]), Status: str(rows[0][2]), Payload: blob(rows[0][3])}, nil
}

func (s *Store) CallbackDelivery(ctx context.Context, callbackID string) (CallbackDeliveryRecord, error) {
	rows, err := s.db.Query(ctx, `SELECT callback_id,execution_id,target_url,payload,status,attempt_count,next_attempt_at,last_error FROM callback_deliveries WHERE callback_id=?`, callbackID)
	if err != nil {
		return CallbackDeliveryRecord{}, err
	}
	if len(rows) == 0 {
		return CallbackDeliveryRecord{}, ErrNotFound
	}
	x := rows[0]
	return CallbackDeliveryRecord{ID: str(x[0]), ExecutionID: str(x[1]), TargetURL: str(x[2]), Payload: blob(x[3]), Status: str(x[4]), AttemptCount: num(x[5]), NextAttemptAt: pt(str(x[6])), LastError: str(x[7])}, nil
}
