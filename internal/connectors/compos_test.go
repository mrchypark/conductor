package connectors

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestComposClientMapsTypedRecordOperation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/connector/objects/orders/records/o1" || r.Method != http.MethodGet {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		if r.Header.Get("Authorization") != "Bearer token" || r.Header.Get("X-Conductor-Workspace-Id") != "w1" {
			t.Fatalf("missing context headers")
		}
		_, _ = w.Write([]byte(`{"id":"o1"}`))
	}))
	defer server.Close()
	c := &ComposClient{BaseURL: server.URL, Token: "token", Client: server.Client()}
	out, err := c.Invoke(context.Background(), "records.get", map[string]any{"object_name": "orders", "record_id": "o1"}, map[string]string{"workspace_id": "w1"})
	if err != nil {
		t.Fatal(err)
	}
	var value map[string]string
	if err := json.Unmarshal(out, &value); err != nil || value["id"] != "o1" {
		t.Fatalf("output=%s err=%v", out, err)
	}
}

func TestComposClientMapsOperationsInvokeAndObjectKey(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/connector/operations/invoke" || r.Method != http.MethodPost {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer server.Close()
	c := &ComposClient{BaseURL: server.URL, Client: server.Client()}
	out, err := c.Invoke(context.Background(), "compos.operations.invoke", map[string]any{"object_key": "orders"}, nil)
	if err != nil || string(out) != `{"ok":true}` {
		t.Fatalf("unexpected output=%s err=%v", out, err)
	}
}
