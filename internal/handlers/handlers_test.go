package handlers

import (
	"bytes"
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

func TestKavunHandlerCompilesAndRunsPerRequest(t *testing.T) {
	h, err := NewKavunHandler(KavunSpec{
		Manifest: Manifest{Key: "transform.kavun", Version: "1"},
		Source:   []byte(`output = {"sum": input.a + input.b}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := h.Execute(context.Background(), Request{Input: json.RawMessage(`{"a":2,"b":3}`)})
	if err != nil {
		t.Fatal(err)
	}
	if string(result.Output) != `{"sum":5}` {
		t.Fatalf("unexpected output: %s", result.Output)
	}
	bytecode, bytecodeHash, runtimeABI, err := h.Bytecode()
	if err != nil {
		t.Fatal(err)
	}
	cached, err := NewKavunHandler(KavunSpec{
		Manifest: Manifest{Key: "transform.kavun", Version: "1"}, Source: []byte(`output = {"sum": input.a + input.b}`),
		SourceHash: hashBytes([]byte(`output = {"sum": input.a + input.b}`)), Bytecode: bytecode, BytecodeHash: bytecodeHash,
		BytecodeSourceHash: hashBytes([]byte(`output = {"sum": input.a + input.b}`)), RuntimeABI: runtimeABI,
	})
	if err != nil {
		t.Fatal(err)
	}
	cachedResult, err := cached.Execute(context.Background(), Request{Input: json.RawMessage(`{"a":7,"b":8}`)})
	if err != nil || string(cachedResult.Output) != `{"sum":15}` {
		t.Fatalf("cached handler failed: %s err=%v", cachedResult.Output, err)
	}
	previousRuntime, err := NewKavunHandler(KavunSpec{
		Manifest: Manifest{Key: "transform.kavun", Version: "1"}, Source: []byte(`output = {"sum": input.a + input.b}`),
		SourceHash: hashBytes([]byte(`output = {"sum": input.a + input.b}`)), Bytecode: bytecode, BytecodeHash: bytecodeHash,
		BytecodeSourceHash: hashBytes([]byte(`output = {"sum": input.a + input.b}`)), RuntimeABI: "kavun-v0.6.4/compiler-o3/input-output",
	})
	if err != nil || !previousRuntime.BytecodeNeedsRefresh() {
		t.Fatalf("old runtime cache was not recompiled: %v", err)
	}
	if result, err := previousRuntime.Execute(context.Background(), Request{Input: json.RawMessage(`{"a":2,"b":3}`)}); err != nil || string(result.Output) != `{"sum":5}` {
		t.Fatalf("recompiled handler failed: %s err=%v", result.Output, err)
	}
	corrupt := []byte("not kavun bytecode")
	corruptHash := hashBytes(corrupt)
	refreshed, err := NewKavunHandler(KavunSpec{
		Manifest: Manifest{Key: "transform.kavun", Version: "1"}, Source: []byte(`output = {"sum": input.a + input.b}`),
		SourceHash: hashBytes([]byte(`output = {"sum": input.a + input.b}`)), Bytecode: corrupt, BytecodeHash: corruptHash,
		BytecodeSourceHash: hashBytes([]byte(`output = {"sum": input.a + input.b}`)), RuntimeABI: runtimeABI,
	})
	if err != nil || refreshed == nil || !refreshed.BytecodeNeedsRefresh() {
		t.Fatalf("corrupt bytecode was not marked for refresh: handler=%v err=%v", refreshed, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := h.Execute(ctx, Request{Input: json.RawMessage(`{"a":4,"b":1}`)}); err != nil {
		t.Fatal(err)
	}
}

func TestKavunHandlerRejectsUnsafeModules(t *testing.T) {
	if _, err := NewKavunHandler(KavunSpec{
		Manifest:       Manifest{Key: "unsafe", Version: "1"},
		Source:         []byte(`output = 1`),
		AllowedModules: []string{"os"},
	}); err == nil || !strings.Contains(err.Error(), "not allowed") {
		t.Fatalf("expected unsafe module rejection, got %v", err)
	}
	if _, err := NewKavunHandler(KavunSpec{
		Manifest: Manifest{Key: "unsafe-import", Version: "1"},
		Source:   []byte(`output = import("os")`),
	}); err == nil {
		t.Fatal("filesystem/process module import was accepted")
	}
	if _, err := NewKavunHandler(KavunSpec{
		Manifest:       Manifest{Key: "blocking-import", Version: "1"},
		Source:         []byte(`output = import("times")`),
		AllowedModules: []string{"times"},
	}); err == nil {
		t.Fatal("blocking times module import was accepted")
	}
	if _, err := NewKavunHandler(KavunSpec{Manifest: Manifest{Key: "large", Version: "1"}, Source: []byte(`output = 1`), MaxStack: 1 << 20}); err == nil {
		t.Fatal("excessive VM stack limit was accepted")
	}
}

func TestKavunBytecodeCacheRechecksAllowedModules(t *testing.T) {
	unsafeProgram, err := compileKavunProgram([]byte(`output = import("json")`), []string{"json"})
	if err != nil {
		t.Fatal(err)
	}
	var encoded bytes.Buffer
	if err := unsafeProgram.bytecode.Encode(&encoded); err != nil {
		t.Fatal(err)
	}
	source := []byte(`output = 1`)
	sourceHash := hashBytes(source)
	h, err := NewKavunHandler(KavunSpec{
		Manifest:           Manifest{Key: "cache-policy", Version: "1"},
		Source:             source,
		Bytecode:           encoded.Bytes(),
		BytecodeHash:       hashBytes(encoded.Bytes()),
		SourceHash:         sourceHash,
		BytecodeSourceHash: sourceHash,
		RuntimeABI:         kavunRuntimeABIFor(nil, defaultKavunFrames, defaultKavunStack),
	})
	if err != nil {
		t.Fatal(err)
	}
	if !h.BytecodeNeedsRefresh() {
		t.Fatal("disallowed cached module was reused")
	}
	result, err := h.Execute(context.Background(), Request{Input: json.RawMessage(`{}`)})
	if err != nil || string(result.Output) != `1` {
		t.Fatalf("fallback compile failed: output=%s err=%v", result.Output, err)
	}
}

func TestKavunBytecodeCacheReloadsGlobalsAndModules(t *testing.T) {
	source := []byte("json := import(\"json\")\nextra := \"ok\"\noutput = {\"v\": input.v, \"extra\": extra, \"raw\": string(json.encode(input))}")
	compiled, err := NewKavunHandler(KavunSpec{
		Manifest:       Manifest{Key: "cache-globals", Version: "1"},
		Source:         source,
		AllowedModules: []string{"json"},
	})
	if err != nil {
		t.Fatal(err)
	}
	var encoded bytes.Buffer
	if err := compiled.program.bytecode.Encode(&encoded); err != nil {
		t.Fatal(err)
	}
	bytecode := encoded.Bytes()
	sourceHash := hashBytes(source)
	reloaded, err := NewKavunHandler(KavunSpec{
		Manifest:           Manifest{Key: "cache-globals", Version: "1"},
		Source:             source,
		AllowedModules:     []string{"json"},
		Bytecode:           bytecode,
		BytecodeHash:       hashBytes(bytecode),
		SourceHash:         sourceHash,
		BytecodeSourceHash: sourceHash,
		RuntimeABI:         compiled.runtimeABI,
	})
	if err != nil {
		t.Fatal(err)
	}
	if reloaded.BytecodeNeedsRefresh() {
		t.Fatal("valid cached bytecode was unexpectedly discarded")
	}
	result, err := reloaded.Execute(context.Background(), Request{Input: json.RawMessage(`{"v":42}`)})
	if err != nil {
		t.Fatalf("cached execution failed: %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal(result.Output, &got); err != nil {
		t.Fatal(err)
	}
	if got["extra"] != "ok" || got["v"] != float64(42) {
		t.Fatalf("unexpected cached output: %s", result.Output)
	}
}
