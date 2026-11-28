// Package handlers defines the executable boundary used by Conductor.
package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

var ErrNotFound = errors.New("handler not found")

// Manifest is immutable metadata for one version of an executable handler.
type Manifest struct {
	Key            string          `json:"handler_key"`
	Version        string          `json:"version"`
	InputSchema    json.RawMessage `json:"input_schema,omitempty"`
	OutputSchema   json.RawMessage `json:"output_schema,omitempty"`
	DefaultTimeout int             `json:"default_timeout_seconds,omitempty"`
	Idempotency    string          `json:"idempotency,omitempty"`
	ResourceClass  string          `json:"resource_class,omitempty"`
}

type Request struct {
	ExecutionID        string
	ExecutionRequestID string
	WorkspaceID        string
	WorkflowKey        string
	WorkflowVersion    uint64
	WorkflowInstanceID string
	TaskID             string
	NodeKey            string
	HandlerKey         string
	HandlerVersion     string
	OperationKey       string
	ConnectionID       string
	CredentialRevision int64
	CredentialRef      string
	ExpiresAt          time.Time
	SubjectRef         json.RawMessage
	InputRef           json.RawMessage
	DeadlineAt         time.Time
	AttemptNumber      int
	LeaseID            string
	FencingToken       int64
	IdempotencyKey     string
	ActorType          string
	ActorID            string
	Input              json.RawMessage
}

type Result struct {
	Output json.RawMessage
}

// Error is a structured handler failure. The execution service owns retry and
// terminal state decisions; handlers only classify the observed failure.
type Error struct {
	Class     string `json:"class"`
	Message   string `json:"message"`
	Retryable bool   `json:"retryable"`
}

func (e *Error) Error() string {
	if e == nil {
		return ""
	}
	return fmt.Sprintf("%s: %s", e.Class, e.Message)
}

type Handler interface {
	Manifest() Manifest
	Execute(context.Context, Request) (Result, error)
}

// HostConnectorFunc executes a host-mediated connector call on behalf of a guest.
type HostConnectorFunc func(ctx context.Context, callInput json.RawMessage) (json.RawMessage, error)

type hostConnectorKey struct{}

// WithHostConnector attaches a host connector proxy callback to ctx.
func WithHostConnector(ctx context.Context, fn HostConnectorFunc) context.Context {
	if fn == nil {
		return ctx
	}
	return context.WithValue(ctx, hostConnectorKey{}, fn)
}

// HostConnectorFromContext retrieves the host connector proxy callback from ctx.
func HostConnectorFromContext(ctx context.Context) HostConnectorFunc {
	if ctx == nil {
		return nil
	}
	fn, _ := ctx.Value(hostConnectorKey{}).(HostConnectorFunc)
	return fn
}

type Func func(context.Context, Request) (Result, error)

type FuncHandler struct {
	manifest Manifest
	fn       Func
}

func NewFuncHandler(manifest Manifest, fn Func) (*FuncHandler, error) {
	if manifest.Key == "" || manifest.Version == "" || fn == nil {
		return nil, errors.New("handler manifest key/version and function are required")
	}
	for name, schema := range map[string]json.RawMessage{"input_schema": manifest.InputSchema, "output_schema": manifest.OutputSchema} {
		if len(schema) > 0 && string(schema) != "null" {
			var document any
			if err := json.Unmarshal(schema, &document); err != nil {
				return nil, fmt.Errorf("%s must be valid JSON: %w", name, err)
			}
			if _, ok := document.(map[string]any); !ok {
				return nil, fmt.Errorf("%s must be a JSON object", name)
			}
		}
	}
	return &FuncHandler{manifest: manifest, fn: fn}, nil
}

func (h *FuncHandler) Manifest() Manifest { return h.manifest }

func (h *FuncHandler) Execute(ctx context.Context, req Request) (Result, error) {
	return h.fn(ctx, req)
}

type Registry struct {
	mu       sync.RWMutex
	handlers map[string]Handler
}

func NewRegistry() *Registry { return &Registry{handlers: make(map[string]Handler)} }

func key(manifest Manifest) string { return manifest.Key + "@" + manifest.Version }

func scopedKey(workspaceID, handlerKey, version string) string {
	return workspaceID + ":" + handlerKey + "@" + version
}

func (r *Registry) Register(h Handler) error {
	return r.RegisterScoped("", h)
}

func (r *Registry) RegisterScoped(workspaceID string, h Handler) error {
	if h == nil {
		return errors.New("handler is nil")
	}
	m := h.Manifest()
	if m.Key == "" || m.Version == "" {
		return errors.New("handler manifest key and version are required")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	k := scopedKey(workspaceID, m.Key, m.Version)
	if _, exists := r.handlers[k]; exists {
		return fmt.Errorf("handler %s already registered", key(m))
	}
	r.handlers[k] = h
	return nil
}

// Unregister removes one global handler version. It is used only to roll back a
// registration when durable definition storage fails.
func (r *Registry) Unregister(handlerKey, version string) bool {
	return r.UnregisterScoped("", handlerKey, version)
}

// UnregisterScoped removes one workspace-scoped handler version.
func (r *Registry) UnregisterScoped(workspaceID, handlerKey, version string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	k := scopedKey(workspaceID, handlerKey, version)
	if _, ok := r.handlers[k]; !ok {
		return false
	}
	delete(r.handlers, k)
	return true
}

func (r *Registry) Resolve(handlerKey, version string) (Handler, error) {
	return r.ResolveScoped("", handlerKey, version)
}

// ResolveScoped resolves a handler for workspaceID, falling back to global ("") built-ins.
func (r *Registry) ResolveScoped(workspaceID, handlerKey, version string) (Handler, error) {
	r.mu.RLock()
	if workspaceID != "" {
		if h, ok := r.handlers[scopedKey(workspaceID, handlerKey, version)]; ok {
			r.mu.RUnlock()
			return h, nil
		}
	}
	h, ok := r.handlers[scopedKey("", handlerKey, version)]
	r.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("%w: %s@%s", ErrNotFound, handlerKey, version)
	}
	return h, nil
}

func (r *Registry) Manifests() []Manifest {
	return r.ManifestsForWorkspace("")
}

func (r *Registry) ManifestsForWorkspace(workspaceID string) []Manifest {
	r.mu.RLock()
	seen := make(map[string]Manifest)
	prefix := ":"
	for k, h := range r.handlers {
		m := h.Manifest()
		if len(k) > len(prefix) && k[0] == ':' {
			seen[key(m)] = m
		}
	}
	if workspaceID != "" {
		wsPrefix := workspaceID + ":"
		for k, h := range r.handlers {
			if len(k) > len(wsPrefix) && k[:len(wsPrefix)] == wsPrefix {
				m := h.Manifest()
				seen[key(m)] = m
			}
		}
	}
	r.mu.RUnlock()
	manifests := make([]Manifest, 0, len(seen))
	for _, m := range seen {
		manifests = append(manifests, m)
	}
	sort.Slice(manifests, func(i, j int) bool {
		return key(manifests[i]) < key(manifests[j])
	})
	return manifests
}

// ManifestsForFunction returns all versions of handlerKey visible to workspaceID.
func (r *Registry) ManifestsForFunction(workspaceID, handlerKey string) []Manifest {
	all := r.ManifestsForWorkspace(workspaceID)
	out := make([]Manifest, 0)
	for _, m := range all {
		if m.Key == handlerKey {
			out = append(out, m)
		}
	}
	return out
}

// HasScoped reports whether workspaceID owns a specific handler version directly (excluding global built-ins).
func (r *Registry) HasScoped(workspaceID, handlerKey, version string) bool {
	if workspaceID == "" {
		return false
	}
	r.mu.RLock()
	_, ok := r.handlers[scopedKey(workspaceID, handlerKey, version)]
	r.mu.RUnlock()
	return ok
}

// ValidateJSON applies the small, deterministic JSON-Schema subset needed at
// the handler boundary: type, required, properties, additionalProperties,
// items, and enum. Complex business schemas remain owned by the business service.
func ValidateJSON(schema json.RawMessage, value json.RawMessage) error {
	if len(schema) == 0 || string(schema) == "null" {
		return nil
	}
	var document map[string]any
	if err := json.Unmarshal(schema, &document); err != nil {
		return fmt.Errorf("schema is invalid JSON: %w", err)
	}
	var decoded any
	if len(value) == 0 {
		value = json.RawMessage("null")
	}
	if err := json.Unmarshal(value, &decoded); err != nil {
		return fmt.Errorf("value is invalid JSON: %w", err)
	}
	return validateValue(document, decoded, "$")
}

func validateValue(schema map[string]any, value any, path string) error {
	if enum, ok := schema["enum"].([]any); ok {
		matched := false
		for _, candidate := range enum {
			if fmt.Sprint(candidate) == fmt.Sprint(value) {
				matched = true
				break
			}
		}
		if !matched {
			return fmt.Errorf("%s is not an allowed value", path)
		}
	}
	if kind, ok := schema["type"].(string); ok && !jsonType(kind, value) {
		return fmt.Errorf("%s must be %s", path, kind)
	}
	if required, ok := schema["required"].([]any); ok {
		object, isObject := value.(map[string]any)
		if !isObject {
			return fmt.Errorf("%s must be an object for required fields", path)
		}
		for _, item := range required {
			name, _ := item.(string)
			if _, exists := object[name]; !exists {
				return fmt.Errorf("%s.%s is required", path, name)
			}
		}
	}
	if properties, ok := schema["properties"].(map[string]any); ok {
		object, isObject := value.(map[string]any)
		if !isObject {
			return fmt.Errorf("%s must be an object", path)
		}
		for name, raw := range properties {
			child, exists := object[name]
			if !exists {
				continue
			}
			childSchema, ok := raw.(map[string]any)
			if ok {
				if err := validateValue(childSchema, child, path+"."+name); err != nil {
					return err
				}
			}
		}
		if additional, ok := schema["additionalProperties"].(bool); ok && !additional {
			for name := range object {
				if _, known := properties[name]; !known {
					return fmt.Errorf("%s.%s is not allowed", path, name)
				}
			}
		}
	}
	if items, ok := schema["items"].(map[string]any); ok {
		array, isArray := value.([]any)
		if !isArray {
			return fmt.Errorf("%s must be an array", path)
		}
		for i, child := range array {
			if err := validateValue(items, child, fmt.Sprintf("%s[%d]", path, i)); err != nil {
				return err
			}
		}
	}
	return nil
}

func jsonType(kind string, value any) bool {
	switch kind {
	case "object":
		_, ok := value.(map[string]any)
		return ok
	case "array":
		_, ok := value.([]any)
		return ok
	case "string":
		_, ok := value.(string)
		return ok
	case "number":
		_, ok := value.(float64)
		return ok
	case "integer":
		n, ok := value.(float64)
		return ok && n == float64(int64(n))
	case "boolean":
		_, ok := value.(bool)
		return ok
	case "null":
		return value == nil
	default:
		return true
	}
}
