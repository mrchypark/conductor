package handlers

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestValidateJSONSchemaSubset(t *testing.T) {
	schema := json.RawMessage(`{"type":"object","required":["name"],"additionalProperties":false,"properties":{"name":{"type":"string"},"count":{"type":"integer"}}}`)
	if err := ValidateJSON(schema, json.RawMessage(`{"name":"ok","count":2}`)); err != nil {
		t.Fatal(err)
	}
	if err := ValidateJSON(schema, json.RawMessage(`{"count":2}`)); err == nil || !strings.Contains(err.Error(), "required") {
		t.Fatalf("expected required-field failure, got %v", err)
	}
	if err := ValidateJSON(schema, json.RawMessage(`{"name":"ok","extra":true}`)); err == nil {
		t.Fatal("additional property was accepted")
	}
}

func TestNewFuncHandlerRejectsInvalidSchema(t *testing.T) {
	if _, err := NewFuncHandler(Manifest{Key: "bad", Version: "1", InputSchema: json.RawMessage(`[]`)}, func(_ context.Context, _ Request) (Result, error) { return Result{}, nil }); err == nil {
		t.Fatal("non-object schema was accepted")
	}
}

func TestStarlarkHandlerCompilesAndRuns(t *testing.T) {
	h, err := NewStarlarkHandler(StarlarkSpec{
		Manifest: Manifest{Key: "transform.starlark", Version: "1"},
		Source:   []byte("def run(input):\n    return {\"sum\": input[\"a\"] + input[\"b\"], \"b64\": base64.encode(\"hi\"), \"hx\": hex.encode(\"hi\")}\n"),
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := h.Execute(context.Background(), Request{Input: json.RawMessage(`{"a":2,"b":3}`)})
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err := json.Unmarshal(result.Output, &got); err != nil {
		t.Fatal(err)
	}
	if got["sum"] != float64(5) || got["b64"] != "aGk=" || got["hx"] != "6869" {
		t.Fatalf("unexpected output: %s", result.Output)
	}
}

func TestStarlarkHandlerHostConnectorCall(t *testing.T) {
	h, err := NewStarlarkHandler(StarlarkSpec{
		Manifest: Manifest{Key: "proxy.starlark", Version: "1", Idempotency: "side_effecting"},
		Source:   []byte("def run(input):\n    res = connector.call({\"provider\": \"hubspot\", \"operation\": \"read\", \"object_name\": input[\"obj\"]})\n    return {\"proxied\": res}\n"),
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx := WithHostConnector(context.Background(), func(_ context.Context, callInput json.RawMessage) (json.RawMessage, error) {
		var req map[string]any
		if err := json.Unmarshal(callInput, &req); err != nil {
			t.Fatal(err)
		}
		if req["provider"] != "hubspot" || req["object_name"] != "contacts" {
			t.Fatalf("unexpected call input: %s", callInput)
		}
		return json.RawMessage(`{"ok":true}`), nil
	})
	result, err := h.Execute(ctx, Request{Input: json.RawMessage(`{"obj":"contacts"}`)})
	if err != nil {
		t.Fatal(err)
	}
	if string(result.Output) != `{"proxied":{"ok":true}}` {
		t.Fatalf("unexpected output: %s", result.Output)
	}
}

func TestStarlarkHandlerEnforcesStepLimitAndCancellation(t *testing.T) {
	h, err := NewStarlarkHandler(StarlarkSpec{
		Manifest: Manifest{Key: "loop.starlark", Version: "1"},
		Source:   []byte("def run(input):\n    total = 0\n    for i in range(100000):\n        total += i\n    return total\n"),
		MaxSteps: 100,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.Execute(context.Background(), Request{Input: json.RawMessage(`{}`)}); err == nil {
		t.Fatal("expected step limit error")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()
	cancel()
	if _, err := h.Execute(ctx, Request{Input: json.RawMessage(`{}`)}); err == nil {
		t.Fatal("expected cancellation error")
	}
}

func TestRegistryWorkspaceIsolation(t *testing.T) {
	reg := NewRegistry()
	global, _ := NewFuncHandler(Manifest{Key: "echo", Version: "1"}, func(_ context.Context, r Request) (Result, error) {
		return Result{Output: json.RawMessage(`"global"`)}, nil
	})
	wsA, _ := NewFuncHandler(Manifest{Key: "custom", Version: "1"}, func(_ context.Context, r Request) (Result, error) {
		return Result{Output: json.RawMessage(`"wsA"`)}, nil
	})
	if err := reg.Register(global); err != nil {
		t.Fatal(err)
	}
	if err := reg.RegisterScoped("ws-a", wsA); err != nil {
		t.Fatal(err)
	}
	if _, err := reg.ResolveScoped("ws-a", "echo", "1"); err != nil {
		t.Fatalf("expected global fallback, got %v", err)
	}
	if _, err := reg.ResolveScoped("ws-a", "custom", "1"); err != nil {
		t.Fatalf("expected ws-a handler, got %v", err)
	}
	if _, err := reg.ResolveScoped("ws-b", "custom", "1"); err == nil {
		t.Fatal("ws-b should not resolve ws-a handler")
	}
}
