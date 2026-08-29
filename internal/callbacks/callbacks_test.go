package callbacks

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/mrchypark/conductor/internal/storage"
)

func TestDeliveryRetriesAndUsesStableCallbackID(t *testing.T) {
	store, err := storage.Open(context.Background(), "file:"+t.TempDir()+"/db")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	request := storage.ExecutionRequestRecord{ID: "r1", HandlerKey: "h", HandlerVersion: "1", Input: []byte(`{}`)}
	if _, err := store.CreateExecution(context.Background(), request, "exec_r1"); err != nil {
		t.Fatal(err)
	}
	lease, err := store.AcquireLease(context.Background(), "exec_r1", "t", time.Minute, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.CompleteExecutionWithLease(context.Background(), lease.ID, lease.FencingToken, "exec_r1", "succeeded", []byte(`{"ok":true}`), "", "", false, 1); err != nil {
		t.Fatal(err)
	}
	var count int
	var callbackID string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count++
		callbackID = r.Header.Get("Idempotency-Key")
		if count == 1 {
			http.Error(w, "retry", 500)
			return
		}
		var payload map[string]any
		_ = json.NewDecoder(r.Body).Decode(&payload)
		w.WriteHeader(200)
	}))
	defer server.Close()
	if err := store.EnqueueCallbackDelivery(context.Background(), "cb_exec_r1", server.URL); err != nil {
		t.Fatal(err)
	}
	d := &Delivery{Store: store, Client: server.Client()}
	if err := d.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := store.DB().ExecContext(context.Background(), "UPDATE callback_deliveries SET next_attempt_at=CURRENT_TIMESTAMP"); err != nil {
		t.Fatal(err)
	}
	if err := d.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if count != 2 || callbackID != "cb_exec_r1" {
		t.Fatalf("delivery count/id=%d/%s", count, callbackID)
	}
	var status string
	if err := store.DB().QueryRowContext(context.Background(), "SELECT status FROM callback_deliveries WHERE callback_id=?", callbackID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "delivered" {
		t.Fatal(status)
	}
}
