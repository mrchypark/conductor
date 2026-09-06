package connectors

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	amp "github.com/amp-labs/connectors"
	"github.com/amp-labs/connectors/common"
	"github.com/mrchypark/conductor/internal/handlers"
)

// CredentialsResolver keeps provider secrets outside execution requests.
type CredentialsResolver func(context.Context, string) (Credentials, error)
type ScopedCredentialsResolver func(context.Context, string, string, string, int64) (Credentials, error)

// NewInvokeHandler exposes AMP's standard CRUD operations and its authenticated
// JSON client through the normal Conductor function boundary.
func (c *Catalog) NewInvokeHandler(version string, client *http.Client, resolve CredentialsResolver) (handlers.Handler, error) {
	return c.NewInvokeHandlerScoped(version, client, func(ctx context.Context, id, _, _ string, revision int64) (Credentials, error) {
		if revision != 0 {
			return Credentials{}, errors.New("legacy resolver does not support credential revisions")
		}
		if resolve == nil {
			return Credentials{}, nil
		}
		return resolve(ctx, id)
	})
}

func (c *Catalog) NewInvokeHandlerScoped(version string, client *http.Client, resolve ScopedCredentialsResolver) (handlers.Handler, error) {
	if c == nil {
		return nil, errors.New("connector catalog is required")
	}
	if version == "" {
		version = "1"
	}
	return handlers.NewFuncHandler(handlers.Manifest{
		Key: "connector.invoke", Version: version, DefaultTimeout: 30, Idempotency: "provider-defined", ResourceClass: "network",
	}, func(ctx context.Context, request handlers.Request) (handlers.Result, error) {
		return c.invokeScoped(ctx, request, client, resolve)
	})
}

type invokeInput struct {
	Provider   string          `json:"provider"`
	Module     string          `json:"module"`
	Operation  string          `json:"operation"`
	ObjectName string          `json:"object_name"`
	ObjectKey  string          `json:"object_key"`
	RecordID   string          `json:"record_id"`
	Record     any             `json:"record"`
	Fields     []string        `json:"fields"`
	Filter     string          `json:"filter"`
	NextPage   string          `json:"next_page"`
	Method     string          `json:"method"`
	Path       string          `json:"path"`
	Body       json.RawMessage `json:"body"`
}

func (c *Catalog) invokeScoped(ctx context.Context, request handlers.Request, client *http.Client, resolve ScopedCredentialsResolver) (handlers.Result, error) {
	var input invokeInput
	if len(request.Input) == 0 || !json.Valid(request.Input) {
		return handlers.Result{}, &handlers.Error{Class: "invalid_input", Message: "connector input must be valid JSON"}
	}
	if err := json.Unmarshal(request.Input, &input); err != nil {
		return handlers.Result{}, &handlers.Error{Class: "invalid_input", Message: err.Error()}
	}
	if input.Provider == "" {
		return handlers.Result{}, &handlers.Error{Class: "invalid_input", Message: "provider is required"}
	}
	if request.OperationKey == "" {
		return handlers.Result{}, &handlers.Error{Class: "invalid_input", Message: "pinned operation is required"}
	}
	if input.Operation != "" && input.Operation != request.OperationKey {
		return handlers.Result{}, &handlers.Error{Class: "invalid_input", Message: "input operation does not match pinned operation"}
	}
	input.Operation = request.OperationKey
	if input.ObjectName == "" {
		input.ObjectName = input.ObjectKey
	}
	operation := strings.ToLower(input.Operation)
	if parts := strings.Split(operation, "."); len(parts) > 1 {
		if input.Module == "" {
			if info, err := c.Info(input.Provider); err == nil && info.Modules != nil {
				if _, ok := (*info.Modules)[parts[0]]; ok {
					input.Module = parts[0]
				}
			}
		}
		if input.ObjectName == "" && len(parts) > 2 {
			input.ObjectName = strings.Join(parts[1:len(parts)-1], ".")
		}
		operation = parts[len(parts)-1]
	}
	if strings.EqualFold(input.Provider, "compos") {
		composOperation := strings.TrimPrefix(input.Operation, "compos.")
		requestContext := map[string]string{"workspace_id": request.WorkspaceID, "workflow_key": request.WorkflowKey, "workflow_instance_id": request.WorkflowInstanceID, "task_id": request.TaskID, "node_key": request.NodeKey, "execution_request_id": request.ExecutionRequestID, "execution_id": request.ExecutionID, "attempt_id": request.LeaseID, "attempt_number": fmt.Sprintf("%d", request.AttemptNumber), "idempotency_key": request.IdempotencyKey, "actor_type": request.ActorType, "actor_id": request.ActorID}
		if request.WorkflowVersion > 0 {
			requestContext["workflow_version"] = fmt.Sprintf("%d", request.WorkflowVersion)
		}
		output, err := c.compos.Invoke(ctx, composOperation, input, requestContext)
		if err != nil {
			retryable := false
			var httpErr *HTTPError
			if errors.As(err, &httpErr) {
				retryable = httpErr.StatusCode == http.StatusTooManyRequests || httpErr.StatusCode >= 500
			}
			return handlers.Result{}, &handlers.Error{Class: "compos_connector_error", Message: err.Error(), Retryable: retryable}
		}
		return handlers.Result{Output: output}, nil
	}
	creds := Credentials{}
	if resolve != nil {
		var err error
		creds, err = resolve(ctx, request.ConnectionID, input.Provider, request.WorkspaceID, request.CredentialRevision)
		if err != nil {
			return handlers.Result{}, &handlers.Error{Class: "connection_error", Message: err.Error()}
		}
	} else if request.CredentialRevision != 0 {
		return handlers.Result{}, &handlers.Error{Class: "connection_error", Message: "credential revision resolver is required"}
	}
	if input.Module == "" && creds.Metadata != nil {
		input.Module = creds.Metadata["module"]
	}
	conn, err := c.NewWithModule(ctx, input.Provider, input.Module, client, request.WorkspaceID, creds.Metadata, creds)
	if err != nil {
		return handlers.Result{}, connectorError(err)
	}

	switch operation {
	case "read", "list", "search":
		reader, ok := conn.(amp.ReadConnector)
		if !ok {
			return handlers.Result{}, unsupported("read")
		}
		fields := make(map[string]struct{}, len(input.Fields))
		for _, field := range input.Fields {
			fields[field] = struct{}{}
		}
		result, err := reader.Read(ctx, common.ReadParams{ObjectName: input.ObjectName, Fields: fields, Filter: input.Filter, NextPage: common.NextPageToken(input.NextPage)})
		if err != nil {
			return handlers.Result{}, connectorError(err)
		}
		return marshalResult(result)
	case "create", "update", "write", "upsert":
		writer, ok := conn.(amp.WriteConnector)
		if !ok {
			return handlers.Result{}, unsupported("write")
		}
		result, err := writer.Write(ctx, common.WriteParams{ObjectName: input.ObjectName, RecordId: input.RecordID, RecordData: input.Record})
		if err != nil {
			return handlers.Result{}, connectorError(err)
		}
		return marshalResult(result)
	case "delete", "remove":
		deleter, ok := conn.(amp.DeleteConnector)
		if !ok {
			return handlers.Result{}, unsupported("delete")
		}
		result, err := deleter.Delete(ctx, common.DeleteParams{ObjectName: input.ObjectName, RecordId: input.RecordID})
		if err != nil {
			return handlers.Result{}, connectorError(err)
		}
		return marshalResult(result)
	case "http", "raw":
		return rawRequest(ctx, conn, input)
	default:
		if input.Path != "" {
			return rawRequest(ctx, conn, input)
		}
		return handlers.Result{}, &handlers.Error{Class: "unsupported_operation", Message: fmt.Sprintf("connector operation %q is not supported", operation)}
	}
}

func rawRequest(ctx context.Context, conn amp.Connector, input invokeInput) (handlers.Result, error) {
	if input.Path == "" {
		return handlers.Result{}, &handlers.Error{Class: "invalid_input", Message: "path is required for raw connector calls"}
	}
	u, parseErr := url.Parse(input.Path)
	if parseErr != nil || u.IsAbs() || u.Host != "" || strings.HasPrefix(input.Path, "//") || u.User != nil || u.Fragment != "" || !strings.HasPrefix(u.Path, "/") {
		return handlers.Result{}, &handlers.Error{Class: "invalid_input", Message: "path must be an origin-form relative URL"}
	}
	client := conn.JSONHTTPClient()
	target, joinErr := url.JoinPath(client.HTTPClient.Base, u.EscapedPath())
	if joinErr != nil {
		return handlers.Result{}, &handlers.Error{Class: "invalid_input", Message: "invalid connector path"}
	}
	if u.RawQuery != "" {
		target += "?" + u.RawQuery
	}
	method := strings.ToUpper(input.Method)
	if method == "" {
		method = http.MethodGet
	}
	var response *common.JSONHTTPResponse
	var err error
	switch method {
	case http.MethodGet:
		response, err = client.Get(ctx, target)
	case http.MethodPost:
		response, err = client.Post(ctx, target, input.Body)
	case http.MethodPut:
		response, err = client.Put(ctx, target, input.Body)
	case http.MethodPatch:
		response, err = client.Patch(ctx, target, input.Body)
	case http.MethodDelete:
		response, err = client.Delete(ctx, target)
	default:
		return handlers.Result{}, &handlers.Error{Class: "invalid_input", Message: "method must be GET, POST, PUT, PATCH, or DELETE"}
	}
	if err != nil {
		return handlers.Result{}, connectorError(err)
	}
	if response == nil {
		return handlers.Result{Output: json.RawMessage("null")}, nil
	}
	body, ok := response.Body()
	if !ok {
		return handlers.Result{Output: json.RawMessage("null")}, nil
	}
	return handlers.Result{Output: append(json.RawMessage(nil), body.Source()...)}, nil
}

func marshalResult(value any) (handlers.Result, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return handlers.Result{}, &handlers.Error{Class: "connector_response_error", Message: err.Error()}
	}
	return handlers.Result{Output: data}, nil
}

func unsupported(operation string) error {
	return &handlers.Error{Class: "unsupported_operation", Message: fmt.Sprintf("connector does not support %s", operation)}
}

func connectorError(err error) *handlers.Error {
	if err == nil {
		return nil
	}
	class := "connector_error"
	retryable := false
	switch {
	case errors.Is(err, common.ErrAccessToken), errors.Is(err, common.ErrInvalidGrant):
		class = "auth_expired"
	case errors.Is(err, common.ErrForbidden):
		class = "permission_denied"
	case errors.Is(err, common.ErrLimitExceeded), common.ClassOf(err) == common.ErrorClassRateLimited:
		class, retryable = "rate_limited", true
	case errors.Is(err, common.ErrNotFound):
		class = "not_found"
	case errors.Is(err, common.ErrConflict):
		class = "conflict"
	case errors.Is(err, common.ErrFieldNotFound), common.ClassOf(err) == common.ErrorClassSchemaDriftField, common.ClassOf(err) == common.ErrorClassSchemaDriftObject:
		class = "schema_changed"
	case errors.Is(err, common.ErrBadRequest), errors.Is(err, common.ErrCaller), common.ClassOf(err) == common.ErrorClassBadRequest:
		class = "validation_failed"
	case errors.Is(err, common.ErrRetryable), errors.Is(err, common.ErrServer), common.ClassOf(err) == common.ErrorClassProvider5xx, common.ClassOf(err) == common.ErrorClassProviderMigration:
		class, retryable = "retryable", true
	}
	return &handlers.Error{Class: class, Message: err.Error(), Retryable: retryable}
}
