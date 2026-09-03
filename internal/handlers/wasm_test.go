package handlers

import (
	"context"
	"encoding/base64"
	"errors"
	"testing"
	"time"
)

const echoWASMBase64 = "AGFzbQEAAAABEQNgAX8Bf2ACf38AYAJ/fwF+AwQDAAECBQMBABEGGQN/AUGAgMAAC38AQYSAwAALfwBBkIDAAAsHQAYGbWVtb3J5AgAFYWxsb2MAAAdkZWFsbG9jAAEGaW52b2tlAAIKX19kYXRhX2VuZAMBC19faGVhcF9iYXNlAwIKNQMjAQF/QQAgAEEAKAKAgMCAACIBakEHakF4cTYCgIDAgAAgAQsCAAsMACAArUIghiABrYQLCw0BAEGAgMAACwQABAAAAFQEbmFtZQAUE2NvbmR1Y3Rvcl9lY2hvLndhc20BGQMABWFsbG9jAQdkZWFsbG9jAgZpbnZva2UHEgEAD19fc3RhY2tfcG9pbnRlcgkIAQAFLmRhdGEARQlwcm9kdWNlcnMBDHByb2Nlc3NlZC1ieQEFcnVzdGMlMS45NS4wLW5pZ2h0bHkgKGQyMjJkZGM0ZCAyMDI2LTAxLTIzKQCUAQ90YXJnZXRfZmVhdHVyZXMIKwtidWxrLW1lbW9yeSsPYnVsay1tZW1vcnktb3B0KxZjYWxsLWluZGlyZWN0LW92ZXJsb25nKwptdWx0aXZhbHVlKw9tdXRhYmxlLWdsb2JhbHMrE25vbnRyYXBwaW5nLWZwdG9pbnQrD3JlZmVyZW5jZS10eXBlcysIc2lnbi1leHQ="
const loopWASMBase64 = "AGFzbQEAAAABEQNgAX8Bf2ACf38AYAJ/fwF+AwQDAAECBQMBABAGGQN/AUGAgMAAC38AQYCAwAALfwBBgIDAAAsHQAYGbWVtb3J5AgAFYWxsb2MAAAdkZWFsbG9jAAEGaW52b2tlAAIKX19kYXRhX2VuZAMBC19faGVhcF9iYXNlAwIKEgMFAEGACAsCAAsHAAN+DAALCwBKBG5hbWUAFBNjb25kdWN0b3JfbG9vcC53YXNtARkDAAVhbGxvYwEHZGVhbGxvYwIGaW52b2tlBxIBAA9fX3N0YWNrX3BvaW50ZXIARQlwcm9kdWNlcnMBDHByb2Nlc3NlZC1ieQEFcnVzdGMlMS45NS4wLW5pZ2h0bHkgKGQyMjJkZGM0ZCAyMDI2LTAxLTIzKQCUAQ90YXJnZXRfZmVhdHVyZXMIKwtidWxrLW1lbW9yeSsPYnVsay1tZW1vcnktb3B0KxZjYWxsLWluZGlyZWN0LW92ZXJsb25nKwptdWx0aXZhbHVlKw9tdXRhYmxlLWdsb2JhbHMrE25vbnRyYXBwaW5nLWZwdG9pbnQrD3JlZmVyZW5jZS10eXBlcysIc2lnbi1leHQ="

func wasmFixture(t *testing.T, encoded string) []byte {
	t.Helper()
	wasm, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		t.Fatal(err)
	}
	return wasm
}

func TestWASMHandlerExecutesJSONWithFreshInstances(t *testing.T) {
	runtime := NewWASMRuntime(context.Background())
	defer runtime.Close(context.Background())
	loads := 0
	load := func(context.Context) ([]byte, error) {
		loads++
		return wasmFixture(t, echoWASMBase64), nil
	}
	h, err := runtime.NewLazyHandler(WASMSpec{Manifest: Manifest{Key: "wasm.echo", Version: "1"}, Load: load})
	if err != nil {
		t.Fatal(err)
	}
	for _, input := range []string{`{"value":1}`, `{"value":2}`} {
		result, err := h.Execute(context.Background(), Request{Input: []byte(input)})
		if err != nil || string(result.Output) != input {
			t.Fatalf("output=%s err=%v", result.Output, err)
		}
	}
	if loads != 1 {
		t.Fatalf("artifact loads=%d, want 1", loads)
	}
}

func TestWASMHandlerInterruptsInfiniteLoop(t *testing.T) {
	runtime := NewWASMRuntime(context.Background())
	defer runtime.Close(context.Background())
	wasm := wasmFixture(t, loopWASMBase64)
	h, err := runtime.NewHandler(context.Background(), WASMSpec{Manifest: Manifest{Key: "wasm.loop", Version: "1"}, Load: func(context.Context) ([]byte, error) { return wasm, nil }}, wasm)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	started := time.Now()
	_, err = h.Execute(ctx, Request{Input: []byte(`{}`)})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err=%v, want deadline exceeded", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("infinite loop took %s to stop", elapsed)
	}
}

func TestWASMHandlerKeepsPureRetryBoundary(t *testing.T) {
	runtime := NewWASMRuntime(context.Background())
	defer runtime.Close(context.Background())
	if _, err := runtime.NewLazyHandler(WASMSpec{Manifest: Manifest{Key: "wasm.side-effect", Version: "1", Idempotency: "provider-defined"}, Load: func(context.Context) ([]byte, error) { return nil, nil }}); err == nil {
		t.Fatal("side-effecting wasm handler was accepted")
	}
	h, err := runtime.NewLazyHandler(WASMSpec{Manifest: Manifest{Key: "wasm.unavailable", Version: "1"}, Load: func(context.Context) ([]byte, error) { return nil, errors.New("registry unavailable") }})
	if err != nil {
		t.Fatal(err)
	}
	_, err = h.Execute(context.Background(), Request{Input: []byte(`{}`)})
	var handlerErr *Error
	if !errors.As(err, &handlerErr) || handlerErr.Class != "wasm_artifact_unavailable" || !handlerErr.Retryable {
		t.Fatalf("unexpected artifact error: %#v", err)
	}
}
