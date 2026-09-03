// Package persistence is Conductor's narrow embedded Rhiza boundary.
package persistence

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/mrchypark/rhiza"
)

type Config = rhiza.Config
type Statement = rhiza.SQLStatement
type Receipt = rhiza.MutationReceipt

type DB struct {
	db *rhiza.DB
}

type RejectedError struct {
	Code string
}

func (e *RejectedError) Error() string {
	if e.Code == "" {
		return "replicated mutation was rejected"
	}
	return "replicated mutation was rejected: " + e.Code
}

func Open(ctx context.Context, config Config) (*DB, error) {
	db, err := rhiza.Open(ctx, config)
	if err != nil {
		return nil, err
	}
	return &DB{db: db}, nil
}

func (d *DB) Close() error { return d.db.Close() }

// LocalReady reports only local recovery and catch-up. Use Ready for traffic.
func (d *DB) LocalReady() bool { return d.db.Ready() }

// Ready proves that a linearizable read can currently cross a quorum.
func (d *DB) Ready(ctx context.Context) error {
	_, err := d.Query(ctx, `SELECT identity FROM schema_metadata WHERE id=1`)
	return err
}

// Mutate submits one deterministic atomic SQL command. Commit-unknown retries
// reuse the exact request ID, timestamp arguments, and statement payload.
func (d *DB) Mutate(ctx context.Context, operation string, statements ...Statement) (Receipt, error) {
	if operation == "" || len(statements) == 0 {
		return Receipt{}, errors.New("mutation operation and statements are required")
	}
	id, err := requestID(operation, statements)
	if err != nil {
		return Receipt{}, err
	}
	req := rhiza.ExecuteRequest{RequestID: id, Statements: statements}
	if err := rhiza.ValidateExecuteRequest(req); err != nil {
		return Receipt{}, err
	}
	savepointRetries := 0
	for {
		response, err := d.db.Execute(ctx, req)
		if err == nil {
			if string(response.Status) == "rejected" {
				return response.MutationReceipt, &RejectedError{Code: response.ErrorCode}
			}
			return response.MutationReceipt, nil
		}
		if !retryableMutationError(err, savepointRetries) {
			return Receipt{}, err
		}
		if lostRhizaSavepoint(err) {
			savepointRetries++
		}
		timer := time.NewTimer(10 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return Receipt{}, err
		case <-timer.C:
		}
	}
}

func retryableMutationError(err error, savepointRetries int) bool {
	return errors.Is(err, rhiza.ErrCommitUnknown) || errors.Is(err, rhiza.ErrNotReady) || errors.Is(err, rhiza.ErrQuorumUnavailable) ||
		lostRhizaSavepoint(err) && savepointRetries == 0
}

func lostRhizaSavepoint(err error) bool {
	return strings.Contains(err.Error(), "no such savepoint: rhiza_command")
}

func (d *DB) Query(ctx context.Context, sql string, args ...any) ([][]any, error) {
	response, err := d.db.Query(ctx, rhiza.QueryRequest{SQL: sql, Args: args, Consistency: rhiza.ConsistencyLinearizable})
	if err != nil {
		return nil, err
	}
	return response.Rows, nil
}

func (d *DB) QueryLocal(ctx context.Context, sql string, args ...any) ([][]any, error) {
	response, err := d.db.Query(ctx, rhiza.QueryRequest{SQL: sql, Args: args, Consistency: rhiza.ConsistencyLocal})
	if err != nil {
		return nil, err
	}
	return response.Rows, nil
}

func requestID(operation string, statements []Statement) (string, error) {
	payload, err := json.Marshal(struct {
		Operation  string      `json:"operation"`
		Statements []Statement `json:"statements"`
	}{operation, statements})
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:]), nil
}
