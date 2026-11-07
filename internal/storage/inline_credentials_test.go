package storage

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func TestInlineCredentialExecutionCannotBeReplayed(t *testing.T) {
	for _, version := range []string{"2", "3"} {
		t.Run(version, func(t *testing.T) {
			ctx := context.Background()
			dsn := "file:" + t.TempDir() + "/db"
			store, err := Open(ctx, dsn)
			if err != nil {
				t.Fatal(err)
			}
			req := ExecutionRequestRecord{ID: "inline", HandlerKey: "connector.credentials.invoke", HandlerVersion: version, MaxAttempts: 3}
			if created, err := store.CreateExecution(ctx, req, "exec_inline"); err != nil || !created {
				t.Fatalf("create: %v %v", created, err)
			}
			if _, _, err := store.ClaimNext(ctx, "worker", time.Minute); !errors.Is(err, ErrNotFound) {
				t.Fatalf("worker claim: %v", err)
			}
			if _, err := store.AcquireLease(ctx, "exec_inline", "worker", time.Minute, 1); err == nil {
				t.Fatal("direct worker lease accepted")
			}
			if _, err := store.AcquireInlineLease(ctx, "exec_inline", time.Millisecond); err != nil {
				t.Fatal(err)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			store, err = Open(ctx, dsn)
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			if err := store.RecoverExpiredLeases(ctx, time.Now().Add(time.Minute)); err != nil {
				t.Fatal(err)
			}
			execution, err := store.GetExecution(ctx, "exec_inline")
			if err != nil || execution.Status != "failed" || execution.ErrorClass != "outcome_unknown" {
				t.Fatalf("recovery: %+v %v", execution, err)
			}
			if _, _, err := store.ClaimNext(ctx, "worker", time.Minute); !errors.Is(err, ErrNotFound) {
				t.Fatalf("replay: %v", err)
			}
			if _, err := store.AcquireInlineLease(ctx, "exec_inline", time.Minute); err == nil {
				t.Fatal("inline replay accepted")
			}
		})
	}
}

func TestConcurrentInlineAndGeneralRecovery(t *testing.T) {
	ctx := context.Background()
	store, err := Open(ctx, "file:"+t.TempDir()+"/db")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	for _, id := range []string{"pending", "expired"} {
		req := ExecutionRequestRecord{ID: id, HandlerKey: "connector.credentials.invoke", HandlerVersion: "2", MaxAttempts: 3}
		if created, err := store.CreateExecution(ctx, req, "exec_"+id); err != nil || !created {
			t.Fatalf("create %s: %v %v", id, created, err)
		}
	}
	lease, err := store.AcquireInlineLease(ctx, "exec_expired", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.Mutate(ctx, "make inline recovery fixtures stale",
		stm(`UPDATE executions SET updated_at=? WHERE id='exec_pending'`, ft(time.Now().Add(-2*time.Minute))),
		stm(`UPDATE leases SET expires_at=? WHERE lease_id=?`, ft(time.Now().Add(-time.Minute)), lease.ID)); err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	results := make(chan error, 2)
	for _, recover := range []func(context.Context, time.Time) error{store.RecoverExpiredLeases, store.RecoverExpiredInlineLeases} {
		go func() {
			<-start
			results <- recover(ctx, time.Now().UTC())
		}()
	}
	close(start)
	for range 2 {
		if err := <-results; err != nil {
			t.Fatalf("concurrent recovery: %v", err)
		}
	}
	for _, id := range []string{"pending", "expired"} {
		executionID := "exec_" + id
		execution, err := store.GetExecution(ctx, executionID)
		if err != nil || execution.Status != "failed" || execution.ErrorClass != "outcome_unknown" {
			t.Fatalf("%s execution: %+v %v", id, execution, err)
		}
		callback, err := store.Callback(ctx, executionID)
		var payload struct {
			RuntimeStatus string `json:"runtime_status"`
			ErrorClass    string `json:"error_class"`
		}
		if err != nil || callback.Status != execution.Status || json.Unmarshal(callback.Payload, &payload) != nil || payload.RuntimeStatus != execution.Status || payload.ErrorClass != execution.ErrorClass {
			t.Fatalf("%s callback: %+v payload=%+v err=%v", id, callback, payload, err)
		}
		events, err := store.ListExecutionEvents(ctx, executionID)
		if err != nil {
			t.Fatal(err)
		}
		terminal := 0
		for _, event := range events {
			if event.Type == "execution.failed" {
				terminal++
			}
		}
		if terminal != 1 {
			t.Fatalf("%s terminal events: %d", id, terminal)
		}
		if _, err := store.AcquireInlineLease(ctx, executionID, time.Minute); err == nil {
			t.Fatalf("%s inline replay accepted", id)
		}
	}
	if _, _, err := store.ClaimNext(ctx, "worker", time.Minute); !errors.Is(err, ErrNotFound) {
		t.Fatalf("worker replay: %v", err)
	}
	_ = store.CompleteExecutionWithLease(ctx, lease.ID, lease.FencingToken, "exec_expired", "succeeded", []byte(`{}`), "", "", false, 1)
	execution, err := store.GetExecution(ctx, "exec_expired")
	if err != nil || execution.Status != "failed" || execution.ErrorClass != "outcome_unknown" {
		t.Fatalf("expired lease changed terminal state: %+v %v", execution, err)
	}
}

func TestInlinePendingRecoversAfterReopen(t *testing.T) {
	ctx := context.Background()
	dsn := "file:" + t.TempDir() + "/db"
	store, err := Open(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	if created, err := store.CreateExecution(ctx, ExecutionRequestRecord{ID: "pending", HandlerKey: "connector.credentials.invoke", HandlerVersion: "2"}, "exec_pending"); err != nil || !created {
		t.Fatalf("create: %v %v", created, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = Open(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.RecoverExpiredLeases(ctx, time.Now().Add(2*time.Minute)); err != nil {
		t.Fatal(err)
	}
	execution, err := store.GetExecution(ctx, "exec_pending")
	if err != nil || execution.Status != "failed" || execution.ErrorClass != "outcome_unknown" {
		t.Fatalf("recovery: %+v %v", execution, err)
	}
	if _, err := store.AcquireInlineLease(ctx, "exec_pending", time.Minute); err == nil {
		t.Fatal("recovered request replayed")
	}
}

func TestInlineRecoveryLeavesGeneralExecutionsUntouched(t *testing.T) {
	ctx := context.Background()
	store, err := Open(ctx, "file:"+t.TempDir()+"/db")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	for _, req := range []ExecutionRequestRecord{
		{ID: "inline", HandlerKey: "connector.credentials.invoke", HandlerVersion: "2"},
		{ID: "general", HandlerKey: "example", HandlerVersion: "1"},
	} {
		if created, err := store.CreateExecution(ctx, req, "exec_"+req.ID); err != nil || !created {
			t.Fatalf("create: %v %v", created, err)
		}
	}
	if _, err := store.AcquireInlineLease(ctx, "exec_inline", time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if _, err := store.AcquireLease(ctx, "exec_general", "worker", time.Millisecond, 1); err != nil {
		t.Fatal(err)
	}
	if err := store.RecoverExpiredInlineLeases(ctx, time.Now().Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	inline, err := store.GetExecution(ctx, "exec_inline")
	if err != nil || inline.Status != "failed" {
		t.Fatalf("inline: %+v %v", inline, err)
	}
	general, err := store.GetExecution(ctx, "exec_general")
	if err != nil || general.Status != "running" {
		t.Fatalf("general: %+v %v", general, err)
	}
	if err := store.RecoverExpiredLeases(ctx, time.Now().Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	general, err = store.GetExecution(ctx, "exec_general")
	if err != nil || general.Status != "failed" {
		t.Fatalf("general recovery: %+v %v", general, err)
	}
}
