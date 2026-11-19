package storage

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestPriorityQueueAndExpirySweeper(t *testing.T) {
	store, err := Open(context.Background(), "file:"+t.TempDir()+"/db")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	ctx := context.Background()
	future := time.Now().UTC().Add(10 * time.Minute)
	past := time.Now().UTC().Add(-time.Minute)

	for _, tc := range []struct {
		id       string
		priority int
		expires  time.Time
	}{
		{"req-low", 0, future},
		{"req-expired", 100, past},
		{"req-high", 10, future},
	} {
		created, err := store.CreateExecution(ctx, ExecutionRequestRecord{
			ID:             tc.id,
			WorkspaceID:    "ws-1",
			HandlerKey:     "payload.echo",
			HandlerVersion: "1",
			ActorType:      "service",
			ActorID:        "tester",
			IdempotencyKey: "idem-" + tc.id,
			Input:          []byte(`{}`),
			Priority:       tc.priority,
			ExpiresAt:      tc.expires,
		}, "exec_"+tc.id)
		if err != nil || !created {
			t.Fatalf("create %s: created=%v err=%v", tc.id, created, err)
		}
	}

	// ClaimNext must skip req-expired (even though priority=100) and pick req-high (priority=10) before req-low (priority=0).
	req1, _, err := store.ClaimNext(ctx, "w1", time.Minute)
	if err != nil || req1.ID != "req-high" {
		t.Fatalf("expected req-high first, got %q err=%v", req1.ID, err)
	}
	req2, _, err := store.ClaimNext(ctx, "w1", time.Minute)
	if err != nil || req2.ID != "req-low" {
		t.Fatalf("expected req-low second, got %q err=%v", req2.ID, err)
	}
	if _, _, err := store.ClaimNext(ctx, "w1", time.Minute); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound when only expired remains, got %v", err)
	}

	// RecoverExpiredExecutions transitions req-expired to timed_out.
	if err := store.RecoverExpiredExecutions(ctx, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	exp, err := store.GetExecutionForWorkspace(ctx, "ws-1", "exec_req-expired")
	if err != nil || exp.Status != "timed_out" || exp.ErrorClass != "credential_expired" {
		t.Fatalf("unexpected expired execution state: %#v err=%v", exp, err)
	}
	if _, err := store.GetExecutionForWorkspace(ctx, "ws-other", "exec_req-expired"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected cross-workspace isolation ErrNotFound, got %v", err)
	}
}
