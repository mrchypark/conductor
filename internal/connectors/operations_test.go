package connectors

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/amp-labs/connectors/common"
	"github.com/mrchypark/conductor/internal/executions"
	"github.com/mrchypark/conductor/internal/handlers"
	"github.com/mrchypark/conductor/internal/storage"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) { return f(request) }

func TestConnectorErrorPreservesAmpClassification(t *testing.T) {
	checks := []struct {
		err       error
		class     string
		retryable bool
	}{
		{common.ErrAccessToken, "auth_expired", false},
		{common.ErrForbidden, "permission_denied", false},
		{common.ErrLimitExceeded, "rate_limited", true},
		{common.ErrNotFound, "not_found", false},
		{common.ErrConflict, "conflict", false},
		{common.ErrServer, "retryable", true},
	}
	for _, check := range checks {
		got := connectorError(check.err)
		if got.Class != check.class || got.Retryable != check.retryable {
			t.Errorf("connectorError(%v) = %+v, want class=%q retryable=%t", check.err, got, check.class, check.retryable)
		}
	}
	wrapped := connectorError(errors.New("provider failure"))
	if wrapped.Class != "connector_error" || wrapped.Retryable {
		t.Fatalf("unexpected unknown error classification: %+v", wrapped)
	}
}

func TestConnectorRejectsInputOperationThatDiffersFromPinnedOperation(t *testing.T) {
	catalog, err := LoadCatalog()
	if err != nil {
		t.Fatal(err)
	}
	handler, err := catalog.NewInvokeHandlerScoped("1", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = handler.Execute(context.Background(), handlers.Request{
		OperationKey: "records.get",
		Input:        json.RawMessage(`{"provider":"compos","operation":"records.delete","object_name":"orders","record_id":"o1"}`),
	})
	var failure *handlers.Error
	if !errors.As(err, &failure) || failure.Class != "invalid_input" {
		t.Fatalf("operation mismatch was not rejected: %v", err)
	}
}

func TestGowidConnectorCallsV2API(t *testing.T) {
	catalog, err := LoadCatalog()
	if err != nil {
		t.Fatal(err)
	}
	client := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		if request.URL.String() != "https://openapi.gowid.com/v2/expense-statements?startDate=20260101&endDate=20260131" {
			t.Fatalf("unexpected URL %s", request.URL)
		}
		if request.Header.Get("Authorization") != "secret" {
			t.Fatalf("unexpected authorization header %q", request.Header.Get("Authorization"))
		}
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(bytes.NewBufferString(`{"data":[]}`))}, nil
	})}
	handler, err := catalog.NewInvokeHandlerScoped("1", client, func(context.Context, string, string, string) (Credentials, error) {
		return Credentials{APIKey: "secret"}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := handler.Execute(context.Background(), handlers.Request{
		OperationKey: "http",
		Input:        json.RawMessage(`{"provider":"gowid","operation":"http","path":"/v2/expense-statements?startDate=20260101&endDate=20260131"}`),
	})
	if err != nil || string(result.Output) != `{"data":[]}` {
		t.Fatalf("unexpected result=%s err=%v", result.Output, err)
	}
}

func TestComposConnectorProviderErrorsUseExecutionRetryPolicy(t *testing.T) {
	tests := []struct {
		name           string
		operation      string
		firstStatus    int
		secondStatus   int
		wantCalls      int32
		wantStatus     string
		wantErrorClass string
	}{
		{name: "server error retries", firstStatus: http.StatusInternalServerError, secondStatus: http.StatusOK, wantCalls: 2, wantStatus: "succeeded"},
		{name: "client error does not retry", firstStatus: http.StatusBadRequest, secondStatus: http.StatusOK, wantCalls: 1, wantStatus: "failed", wantErrorClass: "compos_connector_error"},
		{name: "unknown operation is terminal", operation: "not_registered", firstStatus: http.StatusNotFound, secondStatus: http.StatusOK, wantCalls: 1, wantStatus: "failed", wantErrorClass: "compos_connector_error"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int32
			provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				status := tc.firstStatus
				if calls.Add(1) > 1 {
					status = tc.secondStatus
				}
				w.WriteHeader(status)
				if status == http.StatusOK {
					_, _ = w.Write([]byte(`{"ok":true}`))
				}
			}))
			defer provider.Close()

			catalog, err := LoadCatalog()
			if err != nil {
				t.Fatal(err)
			}
			catalog.SetComposClient(&ComposClient{BaseURL: provider.URL, Client: provider.Client()})
			handler, err := catalog.NewInvokeHandlerScoped("1", nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			registry := handlers.NewRegistry()
			if err := registry.Register(handler); err != nil {
				t.Fatal(err)
			}
			store, err := storage.Open(context.Background(), "file:"+t.TempDir()+"/db")
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			service, err := executions.NewService(store, registry)
			if err != nil {
				t.Fatal(err)
			}
			operation := tc.operation
			if operation == "" {
				operation = "records.get"
			}
			input, err := json.Marshal(map[string]any{"provider": "compos", "operation": operation, "object_name": "orders", "record_id": "o1"})
			if err != nil {
				t.Fatal(err)
			}
			execution, err := service.SubmitAndExecute(context.Background(), executions.ExecutionRequest{
				ExecutionRequestID: "provider-" + tc.name, WorkspaceID: "ws-provider",
				HandlerKey: "connector.invoke", HandlerVersion: "1", OperationKey: operation,
				Input: input,
				Retry: executions.RetryPolicy{MaxAttempts: 2, Backoff: 1},
			})
			if err != nil {
				t.Fatal(err)
			}
			if execution.Status != tc.wantStatus || calls.Load() != tc.wantCalls {
				t.Fatalf("unexpected provider execution: calls=%d execution=%+v", calls.Load(), execution)
			}
			if tc.wantErrorClass != "" && (execution.Error == nil || execution.Error.Class != tc.wantErrorClass) {
				t.Fatalf("unexpected provider error: %+v", execution.Error)
			}
		})
	}
}
