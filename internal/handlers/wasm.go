package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"
)

const (
	WASMRuntimeABI       = "conductor:function/v1"
	WASMRuntimeABIV2     = "conductor:function/v2"
	MaxWASMArtifactBytes = 4 << 20
	MaxWASMInputBytes    = 1 << 20
	MaxWASMOutputBytes   = 64 << 10
	MaxWASMHostCalls     = 16
	MaxWASMHostPayload   = 1 << 20
	WASMMemoryLimitPages = 256 // 16 MiB
	defaultWASMTimeout   = 30
)

type WASMSpec struct {
	Manifest Manifest
	ABI      string
	Load     func(context.Context) ([]byte, error)
}

// WASMRuntime owns compiled code shared by immutable handlers. Each execution
// still gets a fresh guest instance, so guest memory is never shared.
type WASMRuntime struct {
	runtime wazero.Runtime
}

func NewWASMRuntime(ctx context.Context) *WASMRuntime {
	config := wazero.NewRuntimeConfig().
		WithCloseOnContextDone(true).
		WithMemoryLimitPages(WASMMemoryLimitPages)
	rt := wazero.NewRuntimeWithConfig(ctx, config)
	_, _ = rt.NewHostModuleBuilder("conductor").
		NewFunctionBuilder().
		WithFunc(wasmHostConnectorCall).
		Export("connector_call").
		Instantiate(ctx)
	return &WASMRuntime{runtime: rt}
}

func (r *WASMRuntime) Close(ctx context.Context) error {
	if r == nil || r.runtime == nil {
		return nil
	}
	return r.runtime.Close(ctx)
}

type WASMHandler struct {
	manifest Manifest
	abi      string
	runtime  *WASMRuntime
	load     func(context.Context) ([]byte, error)

	mu       sync.Mutex
	compiled wazero.CompiledModule
}

func (r *WASMRuntime) NewHandler(ctx context.Context, spec WASMSpec, wasm []byte) (*WASMHandler, error) {
	h, err := r.NewLazyHandler(spec)
	if err != nil {
		return nil, err
	}
	compiled, err := r.ValidateAndCompile(ctx, wasm, h.abi)
	if err != nil {
		return nil, err
	}
	instance, err := r.runtime.InstantiateModule(ctx, compiled, wazero.NewModuleConfig().WithName("").WithStartFunctions())
	if err != nil {
		_ = compiled.Close(context.Background())
		return nil, fmt.Errorf("instantiate wasm: %w", err)
	}
	_ = instance.Close(context.Background())
	h.compiled = compiled
	return h, nil
}

func (r *WASMRuntime) NewLazyHandler(spec WASMSpec) (*WASMHandler, error) {
	if r == nil || r.runtime == nil {
		return nil, errors.New("wasm runtime is required")
	}
	if _, err := NewFuncHandler(spec.Manifest, func(context.Context, Request) (Result, error) { return Result{}, nil }); err != nil {
		return nil, err
	}
	if spec.Load == nil {
		return nil, errors.New("wasm artifact loader is required")
	}
	abi := spec.ABI
	if abi == "" {
		abi = WASMRuntimeABI
	}
	if abi != WASMRuntimeABI && abi != WASMRuntimeABIV2 {
		return nil, fmt.Errorf("unsupported wasm ABI %q", abi)
	}
	manifest := spec.Manifest
	if abi == WASMRuntimeABI {
		if manifest.Idempotency != "" && manifest.Idempotency != "pure" {
			return nil, errors.New("wasm v1 handler idempotency must be pure")
		}
		if manifest.Idempotency == "" {
			manifest.Idempotency = "pure"
		}
	} else {
		switch manifest.Idempotency {
		case "":
			manifest.Idempotency = "side_effecting"
		case "pure", "idempotent", "side_effecting":
		default:
			return nil, fmt.Errorf("unsupported wasm v2 idempotency %q", manifest.Idempotency)
		}
	}
	if manifest.DefaultTimeout <= 0 {
		manifest.DefaultTimeout = defaultWASMTimeout
	}
	return &WASMHandler{manifest: manifest, abi: abi, runtime: r, load: spec.Load}, nil
}

// ValidateAndCompile compiles wasm bytes and validates exports/imports against abi.
func (r *WASMRuntime) ValidateAndCompile(ctx context.Context, wasm []byte, abi string) (wazero.CompiledModule, error) {
	if abi == "" {
		abi = WASMRuntimeABI
	}
	if len(wasm) == 0 {
		return nil, errors.New("wasm artifact is empty")
	}
	if len(wasm) > MaxWASMArtifactBytes {
		return nil, fmt.Errorf("wasm artifact exceeds %d bytes", MaxWASMArtifactBytes)
	}
	compileCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	compiled, err := r.runtime.CompileModule(compileCtx, wasm)
	if err != nil {
		return nil, fmt.Errorf("compile wasm: %w", err)
	}
	if err := validateWASMABI(compiled, abi); err != nil {
		_ = compiled.Close(context.Background())
		return nil, err
	}
	return compiled, nil
}

func validateWASMABI(compiled wazero.CompiledModule, abi string) error {
	if len(compiled.ImportedMemories()) != 0 {
		return errors.New("wasm imported memories are not allowed")
	}
	imports := compiled.ImportedFunctions()
	switch abi {
	case WASMRuntimeABI:
		if len(imports) != 0 {
			return errors.New("wasm v1 imports are not allowed")
		}
	case WASMRuntimeABIV2:
		if len(imports) > 1 {
			return errors.New("wasm v2 allows at most one host import (conductor.connector_call)")
		}
		if len(imports) == 1 {
			mod, name, _ := imports[0].Import()
			if mod != "conductor" || name != "connector_call" ||
				!sameValueTypes(imports[0].ParamTypes(), []api.ValueType{api.ValueTypeI32, api.ValueTypeI32}) ||
				!sameValueTypes(imports[0].ResultTypes(), []api.ValueType{api.ValueTypeI64}) {
				return errors.New("wasm v2 only allows import conductor.connector_call(i32, i32) -> i64")
			}
		}
	default:
		return fmt.Errorf("unsupported wasm ABI %q", abi)
	}
	memories := compiled.ExportedMemories()
	memory, ok := memories["memory"]
	if !ok || len(memories) != 1 {
		return errors.New("wasm must export exactly one memory named memory")
	}
	if memory.Min() > WASMMemoryLimitPages {
		return fmt.Errorf("wasm initial memory exceeds %d pages", WASMMemoryLimitPages)
	}
	if max, bounded := memory.Max(); bounded && max > WASMMemoryLimitPages {
		return fmt.Errorf("wasm maximum memory exceeds %d pages", WASMMemoryLimitPages)
	}
	exports := compiled.ExportedFunctions()
	for name, signature := range map[string]struct {
		params  []api.ValueType
		results []api.ValueType
	}{
		"alloc":   {[]api.ValueType{api.ValueTypeI32}, []api.ValueType{api.ValueTypeI32}},
		"invoke":  {[]api.ValueType{api.ValueTypeI32, api.ValueTypeI32}, []api.ValueType{api.ValueTypeI64}},
		"dealloc": {[]api.ValueType{api.ValueTypeI32, api.ValueTypeI32}, nil},
	} {
		fn, ok := exports[name]
		if !ok || !sameValueTypes(fn.ParamTypes(), signature.params) || !sameValueTypes(fn.ResultTypes(), signature.results) {
			return fmt.Errorf("wasm export %s has an incompatible signature", name)
		}
	}
	return nil
}

func sameValueTypes(got, want []api.ValueType) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

func (h *WASMHandler) Manifest() Manifest { return h.manifest }

func (h *WASMHandler) compiledModule(ctx context.Context) (wazero.CompiledModule, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.compiled != nil {
		return h.compiled, nil
	}
	wasm, err := h.load(ctx)
	if err != nil {
		var handlerErr *Error
		if errors.As(err, &handlerErr) {
			return nil, handlerErr
		}
		return nil, &Error{Class: "wasm_artifact_unavailable", Message: err.Error(), Retryable: true}
	}
	compiled, err := h.runtime.ValidateAndCompile(ctx, wasm, h.abi)
	if err != nil {
		return nil, &Error{Class: "wasm_invalid", Message: err.Error()}
	}
	h.compiled = compiled
	return compiled, nil
}

func (h *WASMHandler) Execute(ctx context.Context, req Request) (Result, error) {
	if len(req.Input) == 0 {
		req.Input = json.RawMessage("null")
	}
	if len(req.Input) > MaxWASMInputBytes {
		return Result{}, &Error{Class: "validation_failed", Message: fmt.Sprintf("input exceeds %d bytes", MaxWASMInputBytes)}
	}
	if !json.Valid(req.Input) {
		return Result{}, &Error{Class: "validation_failed", Message: "input must be valid JSON"}
	}
	compiled, err := h.compiledModule(ctx)
	if err != nil {
		return Result{}, err
	}
	module, err := h.runtime.runtime.InstantiateModule(ctx, compiled, wazero.NewModuleConfig().WithName("").WithStartFunctions())
	if err != nil {
		return Result{}, wasmRuntimeError(ctx, "instantiate", err)
	}
	defer module.Close(context.Background())

	alloc := module.ExportedFunction("alloc")
	invoke := module.ExportedFunction("invoke")
	dealloc := module.ExportedFunction("dealloc")
	allocated, err := alloc.Call(ctx, uint64(len(req.Input)))
	if err != nil {
		return Result{}, wasmRuntimeError(ctx, "alloc", err)
	}
	inputPtr := uint32(allocated[0])
	if !module.Memory().Write(inputPtr, req.Input) {
		return Result{}, &Error{Class: "wasm_runtime", Message: "input allocation is outside guest memory"}
	}
	defer func() {
		if ctx.Err() == nil {
			_, _ = dealloc.Call(ctx, uint64(inputPtr), uint64(len(req.Input)))
		}
	}()

	ctx = context.WithValue(ctx, wasmHostCallCountKey{}, new(int))
	result, err := invoke.Call(ctx, uint64(inputPtr), uint64(len(req.Input)))
	if err != nil {
		return Result{}, wasmRuntimeError(ctx, "invoke", err)
	}
	outputPtr, outputLen := uint32(result[0]>>32), uint32(result[0])
	if outputLen > MaxWASMOutputBytes {
		return Result{}, &Error{Class: "output_too_large", Message: fmt.Sprintf("output exceeds %d bytes", MaxWASMOutputBytes)}
	}
	output, ok := module.Memory().Read(outputPtr, outputLen)
	if !ok {
		return Result{}, &Error{Class: "wasm_runtime", Message: "output is outside guest memory"}
	}
	encoded := append([]byte(nil), output...)
	if outputPtr != inputPtr {
		defer func() {
			if ctx.Err() == nil {
				_, _ = dealloc.Call(ctx, uint64(outputPtr), uint64(outputLen))
			}
		}()
	}
	if !json.Valid(encoded) {
		return Result{}, &Error{Class: "wasm_runtime", Message: "output must be valid JSON"}
	}
	return Result{Output: encoded}, nil
}

func wasmRuntimeError(ctx context.Context, operation string, err error) error {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}
	return &Error{Class: "wasm_runtime", Message: fmt.Sprintf("%s: %v", operation, err)}
}

type wasmHostCallCountKey struct{}

func wasmHostConnectorCall(ctx context.Context, m api.Module, reqPtr, reqLen uint32) uint64 {
	writeErr := func(class, msg string, retryable bool) uint64 {
		payload, _ := json.Marshal(map[string]any{
			"error": Error{Class: class, Message: msg, Retryable: retryable},
		})
		return writeWASMHostResponse(ctx, m, payload)
	}
	if countPtr, ok := ctx.Value(wasmHostCallCountKey{}).(*int); ok && countPtr != nil {
		*countPtr++
		if *countPtr > MaxWASMHostCalls {
			return writeErr("limit_exceeded", fmt.Sprintf("exceeded maximum host connector calls (%d)", MaxWASMHostCalls), false)
		}
	}
	if reqLen > MaxWASMHostPayload {
		return writeErr("validation_failed", fmt.Sprintf("host connector request exceeds %d bytes", MaxWASMHostPayload), false)
	}
	mem := m.Memory()
	if mem == nil {
		return 0
	}
	reqBytes, ok := mem.Read(reqPtr, reqLen)
	if !ok {
		return writeErr("validation_failed", "host connector request is outside guest memory", false)
	}
	hostFn := HostConnectorFromContext(ctx)
	if hostFn == nil {
		return writeErr("connector_unavailable", "connector host proxy is unavailable for this execution", false)
	}
	respBytes, err := hostFn(ctx, append(json.RawMessage(nil), reqBytes...))
	if err != nil {
		var handlerErr *Error
		if errors.As(err, &handlerErr) {
			return writeErr(handlerErr.Class, handlerErr.Message, handlerErr.Retryable)
		}
		return writeErr("connector_error", err.Error(), false)
	}
	if len(respBytes) > MaxWASMHostPayload {
		return writeErr("output_too_large", fmt.Sprintf("host connector response exceeds %d bytes", MaxWASMHostPayload), false)
	}
	return writeWASMHostResponse(ctx, m, respBytes)
}

func writeWASMHostResponse(ctx context.Context, m api.Module, payload []byte) uint64 {
	if len(payload) == 0 {
		payload = []byte("null")
	}
	alloc := m.ExportedFunction("alloc")
	mem := m.Memory()
	if alloc == nil || mem == nil {
		return 0
	}
	res, err := alloc.Call(ctx, uint64(len(payload)))
	if err != nil || len(res) == 0 {
		return 0
	}
	respPtr := uint32(res[0])
	if uint64(respPtr)+uint64(len(payload)) > uint64(mem.Size()) || !mem.Write(respPtr, payload) {
		return 0
	}
	return (uint64(respPtr) << 32) | uint64(len(payload))
}
