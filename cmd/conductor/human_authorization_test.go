package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHumanAuthorizationHTTPRejectsUnsafeRequests(t *testing.T) {
	const body = `{"handler":"connector.credentials.invoke","handler_version":"2","workspace_id":"w","actor":{"type":"service","id":"s"},"idempotency_key":"i"}`
	tests := []struct {
		name, method, path, body, serviceToken, authorization, prefer string
		headers                                                       []string
		want                                                          int
	}{
		{name: "service auth absent", method: "POST", path: "/v1/executions", body: body, headers: []string{"Bearer human"}, want: http.StatusUnauthorized},
		{name: "service auth invalid", method: "POST", path: "/v1/executions", body: body, serviceToken: "service", headers: []string{"Bearer human"}, want: http.StatusUnauthorized},
		{name: "missing human", method: "POST", path: "/v1/executions", body: body, serviceToken: "service", authorization: "Bearer service", want: http.StatusBadRequest},
		{name: "duplicate human", method: "POST", path: "/v1/executions", body: body, serviceToken: "service", authorization: "Bearer service", headers: []string{"Bearer first", "Bearer second"}, want: http.StatusBadRequest},
		{name: "combined human", method: "POST", path: "/v1/executions", body: body, serviceToken: "service", authorization: "Bearer service", headers: []string{"Bearer first, Bearer second"}, want: http.StatusBadRequest},
		{name: "malformed human", method: "POST", path: "/v1/executions", body: body, serviceToken: "service", authorization: "Bearer service", headers: []string{"Bearer token with spaces"}, want: http.StatusBadRequest},
		{name: "async", method: "POST", path: "/v1/executions", body: body, serviceToken: "service", authorization: "Bearer service", headers: []string{"Bearer human"}, prefer: "respond-async", want: http.StatusBadRequest},
		{name: "other handler", method: "POST", path: "/v1/executions", body: `{"handler":"other","handler_version":"1","workspace_id":"w","actor":{"type":"service","id":"s"},"idempotency_key":"i"}`, serviceToken: "service", authorization: "Bearer service", headers: []string{"Bearer human"}, want: http.StatusBadRequest},
		{name: "other route", method: "GET", path: "/v1/connectors", serviceToken: "service", authorization: "Bearer service", headers: []string{"Bearer human"}, want: http.StatusBadRequest},
		{name: "event route", method: "POST", path: "/v1/events", body: `{"events":[{"name":"connector.credentials.invoke","data":{"_execution":{"execution_request_id":"e","workspace_id":"w","handler_version":"2"}}}]}`, serviceToken: "service", authorization: "Bearer service", want: http.StatusBadRequest},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &server{serviceToken: tt.serviceToken}
			req := httptest.NewRequest(tt.method, tt.path, strings.NewReader(tt.body))
			req.Header.Set("Authorization", tt.authorization)
			req.Header.Set("Prefer", tt.prefer)
			for _, value := range tt.headers {
				req.Header.Add("X-Conductor-Human-Authorization", value)
			}
			w := httptest.NewRecorder()
			s.ServeHTTP(w, req)
			if w.Code != tt.want {
				t.Fatalf("status = %d, want %d: %s", w.Code, tt.want, w.Body.String())
			}
			if strings.Contains(w.Body.String(), "Bearer human") {
				t.Fatal("response exposed human authorization")
			}
		})
	}
}

func TestHumanAuthorizationRejectedInExecutionRequest(t *testing.T) {
	for _, tc := range []struct{ name, token, fields string }{
		{"actor with escaped token", "secret-token", `"actor":{"type":"service","id":"prefix-secret\u002dtoken"},"idempotency_key":"i"`},
		{"correlation", "secret-token", `"actor":{"type":"service","id":"s"},"idempotency_key":"i","correlation_id":"secret-token"`},
		{"idempotency", "secret-token", `"actor":{"type":"service","id":"s"},"idempotency_key":"secret-token"`},
		{"nested key", "secret-token", `"actor":{"type":"service","id":"s"},"idempotency_key":"i","input":{"secret-token":true}`},
		{"numeric field", "123456", `"actor":{"type":"service","id":"s"},"idempotency_key":"i","workflow_version":123456`},
		{"numeric retry duration", "123456", `"actor":{"type":"service","id":"s"},"idempotency_key":"i","retry_policy":{"backoff":123456}`},
		{"retry alias", "secret-token", `"actor":{"type":"service","id":"s"},"idempotency_key":"i","retry":{"backoff":123456}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := `{"handler":"connector.credentials.invoke","handler_version":"2","workspace_id":"w",` + tc.fields + `}`
			req := httptest.NewRequest(http.MethodPost, "/v1/executions", strings.NewReader(body))
			req.Header.Set("Authorization", "Bearer service")
			req.Header.Set("X-Conductor-Human-Authorization", "Bearer "+tc.token)
			w := httptest.NewRecorder()
			(&server{serviceToken: "service"}).ServeHTTP(w, req)
			if w.Code != http.StatusBadRequest || strings.Contains(w.Body.String(), tc.token) {
				t.Fatalf("request was not safely rejected: status=%d", w.Code)
			}
		})
	}
}
