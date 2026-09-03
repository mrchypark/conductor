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
	MaxWASMArtifactBytes = 4 << 20
	MaxWASMInputBytes    = 1 << 20
	MaxWASMOutputBytes   = 64 << 10
	WASMMemoryLimitPages = 256 // 16 MiB
	defaultWASMTimeout   = 30
)

type WASMSpec struct {
	Manifest Manifest
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
	return &WASMRuntime{runtime: wazero.NewRuntimeWithConfig(ctx, config)}
}

func (r *WASMRuntime) Close(ctx context.Context) error {
	if r == nil || r.runtime == nil {
		return nil
	}
	return r.runtime.Close(ctx)
}

type WASMHandler struct {
	manifest Manifest
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
	compiled, err := r.compile(ctx, wasm)
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
	manifest := spec.Manifest
	if manifest.Idempotency != "" && manifest.Idempotency != "pure" {
		return nil, errors.New("wasm handler idempotency must be pure")
	}
	if manifest.DefaultTimeout <= 0 {
		manifest.DefaultTimeout = defaultWASMTimeout
	}
	if manifest.Idempotency == "" {
		manifest.Idempotency = "pure"
	}
	return &WASMHandler{manifest: manifest, runtime: r, load: spec.Load}, nil
}

func (r *WASMRuntime) compile(ctx context.Context, wasm []byte) (wazero.CompiledModule, error) {
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
	if err := validateWASMABI(compiled); err != nil {
		_ = compiled.Close(context.Background())
		return nil, err
	}
	return compiled, nil
}

func validateWASMABI(compiled wazero.CompiledModule) error {
	if len(compiled.ImportedFunctions()) != 0 || len(compiled.ImportedMemories()) != 0 {
		return errors.New("wasm imports are not allowed")
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
	compiled, err := h.runtime.compile(ctx, wasm)
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
	defer dealloc.Call(context.Background(), uint64(inputPtr), uint64(len(req.Input)))

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
		defer dealloc.Call(context.Background(), uint64(outputPtr), uint64(outputLen))
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
