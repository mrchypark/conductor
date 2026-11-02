package storage

import (
	"context"
	"testing"
	"time"
)

func TestRecoveredCallbackClaimFencesStaleSender(t *testing.T) {
	ctx := context.Background()
	store, err := Open(ctx, "file:"+t.TempDir()+"/db")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Errorf("close store: %v", err)
		}
	})
	request := ExecutionRequestRecord{ID: "callback-request", HandlerKey: "handler", HandlerVersion: "1", Input: []byte(`{}`)}
	if created, err := store.CreateExecution(ctx, request, "callback-execution"); err != nil || !created {
		t.Fatalf("create execution: created=%v err=%v", created, err)
	}
	lease, err := store.AcquireLease(ctx, "callback-execution", "worker", time.Minute, 1)
	if err != nil {
		t.Fatal(err)
	}
	store.SetCallbackTarget("https://example.test/callback")
	if err := store.CompleteExecutionWithLease(ctx, lease.ID, lease.FencingToken, "callback-execution", "succeeded", []byte(`{}`), "", "", false, 1); err != nil {
		t.Fatal(err)
	}
	firstTime := time.Now().UTC().Add(time.Second)
	first, err := store.DueCallbackDeliveries(ctx, firstTime, 1)
	if err != nil || len(first) != 1 || first[0].ClaimID == "" {
		t.Fatalf("first claim: %+v err=%v", first, err)
	}
	recoveryTime := firstTime.Add(time.Minute)
	if err := store.RecoverCallbackDeliveries(ctx, recoveryTime, 30*time.Second); err != nil {
		t.Fatal(err)
	}
	second, err := store.DueCallbackDeliveries(ctx, recoveryTime, 1)
	if err != nil || len(second) != 1 || second[0].ClaimID == "" || second[0].ClaimID == first[0].ClaimID {
		t.Fatalf("second claim: %+v err=%v", second, err)
	}
	if err := store.MarkCallbackDelivery(ctx, first[0].ID, first[0].ClaimID, "delivered", "", recoveryTime); err != nil {
		t.Fatal(err)
	}
	got, err := store.CallbackDelivery(ctx, first[0].ID)
	if err != nil || got.Status != "sending" || got.AttemptCount != 0 {
		t.Fatalf("stale claim changed delivery: %+v err=%v", got, err)
	}
	if err := store.MarkCallbackDelivery(ctx, second[0].ID, second[0].ClaimID, "delivered", "", recoveryTime); err != nil {
		t.Fatal(err)
	}
	got, err = store.CallbackDelivery(ctx, second[0].ID)
	if err != nil || got.Status != "delivered" || got.AttemptCount != 1 {
		t.Fatalf("current claim failed to finish: %+v err=%v", got, err)
	}
}
