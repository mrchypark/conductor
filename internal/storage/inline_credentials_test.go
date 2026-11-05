package storage

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestInlineCredentialExecutionCannotBeReplayed(t *testing.T) {
	ctx := context.Background()
	dsn := "file:" + t.TempDir() + "/db"
	store, err := Open(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	req := ExecutionRequestRecord{ID: "inline", HandlerKey: "connector.credentials.invoke", HandlerVersion: "2", MaxAttempts: 3}
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
