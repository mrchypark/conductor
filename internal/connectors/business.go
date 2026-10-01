package connectors

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

// BusinessClient calls a business service's typed connector API over HTTPS;
// it never shares the service's database with Conductor.
type BusinessClient struct {
	BaseURL, Token string
	Client         *http.Client
}

type HTTPError struct{ StatusCode int }

func (e *HTTPError) Error() string { return fmt.Sprintf("business returned HTTP %d", e.StatusCode) }

func (c *BusinessClient) Invoke(ctx context.Context, operation string, input any, requestContext map[string]string) (json.RawMessage, error) {
	if c == nil || c.BaseURL == "" {
		return nil, errors.New("business client is not configured")
	}
	operation = strings.Trim(operation, "/")
	if operation == "" || strings.ContainsAny(operation, "/?#%") {
		return nil, errors.New("invalid business operation")
	}
	path := "/v1/connector/operations/" + operation + ":invoke"
	method := http.MethodPost
	var m map[string]any
	if raw, marshalErr := json.Marshal(input); marshalErr == nil {
		_ = json.Unmarshal(raw, &m)
	}
	if m != nil {
		object, _ := m["object_name"].(string)
		if object == "" {
			object, _ = m["object_key"].(string)
		}
		recordID, _ := m["record_id"].(string)
		if object != "" {
			o := strings.TrimPrefix(operation, "business.")
			base := "/v1/connector/objects/" + url.PathEscape(object)
			switch o {
			case "objects.get_schema":
				path = base + "/schema"
				method = http.MethodGet
			case "records.get":
				path = base + "/records/" + url.PathEscape(recordID)
				method = http.MethodGet
			case "records.list":
				path = base + "/records/query"
			case "records.create":
				path = base + "/records"
			case "records.update":
				path = base + "/records/" + url.PathEscape(recordID)
				method = http.MethodPatch
			case "records.delete":
				path = base + "/records/" + url.PathEscape(recordID)
				method = http.MethodDelete
			case "operations.invoke":
				path = "/v1/connector/operations/invoke"
			}
		}
		if strings.TrimPrefix(operation, "business.") == "operations.invoke" {
			path = "/v1/connector/operations/invoke"
		}
	}
	payload := map[string]any{"input": input, "context": requestContext}
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	requestBody := bytes.NewReader(body)
	if method == http.MethodGet || method == http.MethodDelete {
		requestBody = bytes.NewReader(nil)
	}
	req, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(c.BaseURL, "/")+path, requestBody)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	for key, value := range requestContext {
		req.Header.Set("X-Conductor-"+strings.ReplaceAll(key, "_", "-"), value)
	}
	if c.Token != "" {
		req.Header.Set("Authorization", "Bearer "+c.Token)
	}
	client := c.Client
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	out, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, &HTTPError{StatusCode: resp.StatusCode}
	}
	if len(out) == 0 {
		return json.RawMessage("null"), nil
	}
	if !json.Valid(out) {
		return nil, errors.New("business response is not valid JSON")
	}
	return json.RawMessage(out), nil
}
