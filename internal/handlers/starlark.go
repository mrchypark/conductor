package handlers

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"time"

	starlarkjson "go.starlark.net/lib/json"
	starlarkmath "go.starlark.net/lib/math"
	"go.starlark.net/starlark"
	"go.starlark.net/starlarkstruct"
	"go.starlark.net/syntax"
)

const (
	maxStarlarkSourceBytes  = 256 << 10
	defaultStarlarkMaxSteps = 1_000_000
	maxStarlarkStepsLimit   = 10_000_000
	defaultStarlarkTimeout  = 30
	maxStarlarkHostCalls    = 16
	regStarlarkMaxSteps     = 10_000
	regStarlarkTimeout      = 2 * time.Second
)

// StarlarkSpec describes a versioned, sandboxed Starlark script handler.
// The script must define a top-level function `def run(input):` returning a
// JSON-serializable value.
type StarlarkSpec struct {
	Manifest   Manifest
	Source     []byte
	MaxSteps   int
	SourceHash string
}

type StarlarkHandler struct {
	manifest   Manifest
	program    *starlark.Program
	maxSteps   int
	sourceHash string
}

func NewStarlarkHandler(spec StarlarkSpec) (*StarlarkHandler, error) {
	if _, err := NewFuncHandler(spec.Manifest, func(context.Context, Request) (Result, error) { return Result{}, nil }); err != nil {
		return nil, err
	}
	if len(spec.Source) == 0 {
		return nil, errors.New("starlark source is required")
	}
	if len(spec.Source) > maxStarlarkSourceBytes {
		return nil, fmt.Errorf("starlark source exceeds %d bytes", maxStarlarkSourceBytes)
	}
	manifest := spec.Manifest
	if manifest.DefaultTimeout <= 0 {
		manifest.DefaultTimeout = defaultStarlarkTimeout
	}
	if manifest.Idempotency == "" {
		manifest.Idempotency = "pure"
	}
	maxSteps := spec.MaxSteps
	if maxSteps <= 0 {
		maxSteps = defaultStarlarkMaxSteps
	}
	if maxSteps > maxStarlarkStepsLimit {
		return nil, fmt.Errorf("starlark max_steps exceeds %d", maxStarlarkStepsLimit)
	}
	sourceHash := hashBytes(spec.Source)
	if spec.SourceHash != "" && spec.SourceHash != sourceHash {
		return nil, errors.New("starlark source hash mismatch")
	}
	_, prog, err := starlark.SourceProgramOptions(&syntax.FileOptions{}, "(main).star", spec.Source, starlarkPredeclared(nil).Has)
	if err != nil {
		return nil, fmt.Errorf("starlark compile: %w", err)
	}
	// Validate top-level definitions with bounded steps and timeout.
	regCtx, cancel := context.WithTimeout(context.Background(), regStarlarkTimeout)
	defer cancel()
	globals, err := runStarlarkProgram(regCtx, prog, regStarlarkMaxSteps, nil)
	if err != nil {
		return nil, fmt.Errorf("starlark init: %w", err)
	}
	runVal, ok := globals["run"]
	if !ok {
		return nil, errors.New("starlark script must define a top-level run(input) function")
	}
	if _, ok := runVal.(starlark.Callable); !ok {
		return nil, errors.New("starlark run must be a callable function")
	}
	return &StarlarkHandler{
		manifest:   manifest,
		program:    prog,
		maxSteps:   maxSteps,
		sourceHash: sourceHash,
	}, nil
}

func (h *StarlarkHandler) Manifest() Manifest { return h.manifest }

func (h *StarlarkHandler) Execute(ctx context.Context, req Request) (Result, error) {
	var inputAny any
	if len(req.Input) > 0 {
		if err := json.Unmarshal(req.Input, &inputAny); err != nil {
			return Result{}, &Error{Class: "validation_failed", Message: "input must be valid JSON"}
		}
	}
	inputVal, err := starlarkValueFromGo(inputAny)
	if err != nil {
		return Result{}, &Error{Class: "validation_failed", Message: err.Error()}
	}
	callCount := 0
	hostFn := HostConnectorFromContext(ctx)
	connectorMod := starlarkConnectorModule(ctx, hostFn, &callCount)
	globals, err := runStarlarkProgram(ctx, h.program, h.maxSteps, connectorMod)
	if err != nil {
		return Result{}, classifyStarlarkError(ctx, err)
	}
	runFn, ok := globals["run"].(starlark.Callable)
	if !ok {
		return Result{}, &Error{Class: "starlark_runtime", Message: "run function not found"}
	}
	thread := newStarlarkThread(ctx, h.maxSteps)
	done := watchStarlarkContext(ctx, thread)
	defer done()
	ret, err := starlark.Call(thread, runFn, starlark.Tuple{inputVal}, nil)
	if err != nil {
		return Result{}, classifyStarlarkError(ctx, err)
	}
	goVal, err := goValueFromStarlark(ret)
	if err != nil {
		return Result{}, &Error{Class: "starlark_runtime", Message: fmt.Sprintf("encode output: %v", err)}
	}
	encoded, err := json.Marshal(goVal)
	if err != nil {
		return Result{}, &Error{Class: "starlark_runtime", Message: fmt.Sprintf("marshal output: %v", err)}
	}
	return Result{Output: encoded}, nil
}

func classifyStarlarkError(ctx context.Context, err error) error {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}
	var handlerErr *Error
	if errors.As(err, &handlerErr) {
		return handlerErr
	}
	return &Error{Class: "starlark_runtime", Message: err.Error()}
}

func newStarlarkThread(ctx context.Context, maxSteps int) *starlark.Thread {
	thread := &starlark.Thread{Name: "conductor"}
	if maxSteps > 0 {
		thread.SetMaxExecutionSteps(uint64(maxSteps))
	}
	return thread
}

func watchStarlarkContext(ctx context.Context, thread *starlark.Thread) func() {
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		select {
		case <-ctx.Done():
			thread.Cancel(ctx.Err().Error())
		case <-stop:
		}
	}()
	return func() {
		close(stop)
		<-done
	}
}

func runStarlarkProgram(ctx context.Context, prog *starlark.Program, maxSteps int, connectorMod *starlarkstruct.Module) (starlark.StringDict, error) {
	thread := newStarlarkThread(ctx, maxSteps)
	done := watchStarlarkContext(ctx, thread)
	defer done()
	return prog.Init(thread, starlarkPredeclared(connectorMod))
}

func starlarkPredeclared(connectorMod *starlarkstruct.Module) starlark.StringDict {
	dict := starlark.StringDict{
		"json":   starlarkjson.Module,
		"math":   starlarkmath.Module,
		"base64": starlarkBase64Module,
		"hex":    starlarkHexModule,
	}
	if connectorMod != nil {
		dict["connector"] = connectorMod
	} else {
		dict["connector"] = &starlarkstruct.Module{
			Name: "connector",
			Members: starlark.StringDict{
				"call": starlark.NewBuiltin("connector.call", func(*starlark.Thread, *starlark.Builtin, starlark.Tuple, []starlark.Tuple) (starlark.Value, error) {
					return nil, errors.New("connector.call cannot be called at top-level initialization")
				}),
			},
		}
	}
	return dict
}

func starlarkConnectorModule(ctx context.Context, hostFn HostConnectorFunc, callCount *int) *starlarkstruct.Module {
	return &starlarkstruct.Module{
		Name: "connector",
		Members: starlark.StringDict{
			"call": starlark.NewBuiltin("connector.call", func(_ *starlark.Thread, _ *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
				if hostFn == nil {
					return nil, errors.New("connector host proxy is unavailable for this execution")
				}
				var reqVal starlark.Value
				if err := starlark.UnpackPositionalArgs("connector.call", args, kwargs, 1, &reqVal); err != nil {
					return nil, err
				}
				*callCount++
				if *callCount > maxStarlarkHostCalls {
					return nil, fmt.Errorf("exceeded maximum host connector calls (%d)", maxStarlarkHostCalls)
				}
				reqGo, err := goValueFromStarlark(reqVal)
				if err != nil {
					return nil, fmt.Errorf("connector.call request: %w", err)
				}
				reqBytes, err := json.Marshal(reqGo)
				if err != nil {
					return nil, fmt.Errorf("connector.call marshal: %w", err)
				}
				respBytes, err := hostFn(ctx, reqBytes)
				if err != nil {
					return nil, err
				}
				var respGo any
				if len(respBytes) > 0 {
					if err := json.Unmarshal(respBytes, &respGo); err != nil {
						return nil, fmt.Errorf("connector.call decode response: %w", err)
					}
				}
				return starlarkValueFromGo(respGo)
			}),
		},
	}
}

var starlarkBase64Module = &starlarkstruct.Module{
	Name: "base64",
	Members: starlark.StringDict{
		"encode": starlark.NewBuiltin("base64.encode", func(_ *starlark.Thread, _ *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
			var s string
			if err := starlark.UnpackPositionalArgs("base64.encode", args, kwargs, 1, &s); err != nil {
				return nil, err
			}
			return starlark.String(base64.StdEncoding.EncodeToString([]byte(s))), nil
		}),
		"decode": starlark.NewBuiltin("base64.decode", func(_ *starlark.Thread, _ *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
			var s string
			if err := starlark.UnpackPositionalArgs("base64.decode", args, kwargs, 1, &s); err != nil {
				return nil, err
			}
			decoded, err := base64.StdEncoding.DecodeString(s)
			if err != nil {
				return nil, err
			}
			return starlark.String(decoded), nil
		}),
		"url_encode": starlark.NewBuiltin("base64.url_encode", func(_ *starlark.Thread, _ *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
			var s string
			if err := starlark.UnpackPositionalArgs("base64.url_encode", args, kwargs, 1, &s); err != nil {
				return nil, err
			}
			return starlark.String(base64.RawURLEncoding.EncodeToString([]byte(s))), nil
		}),
		"url_decode": starlark.NewBuiltin("base64.url_decode", func(_ *starlark.Thread, _ *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
			var s string
			if err := starlark.UnpackPositionalArgs("base64.url_decode", args, kwargs, 1, &s); err != nil {
				return nil, err
			}
			decoded, err := base64.RawURLEncoding.DecodeString(s)
			if err != nil {
				return nil, err
			}
			return starlark.String(decoded), nil
		}),
	},
}

var starlarkHexModule = &starlarkstruct.Module{
	Name: "hex",
	Members: starlark.StringDict{
		"encode": starlark.NewBuiltin("hex.encode", func(_ *starlark.Thread, _ *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
			var s string
			if err := starlark.UnpackPositionalArgs("hex.encode", args, kwargs, 1, &s); err != nil {
				return nil, err
			}
			return starlark.String(hex.EncodeToString([]byte(s))), nil
		}),
		"decode": starlark.NewBuiltin("hex.decode", func(_ *starlark.Thread, _ *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
			var s string
			if err := starlark.UnpackPositionalArgs("hex.decode", args, kwargs, 1, &s); err != nil {
				return nil, err
			}
			decoded, err := hex.DecodeString(s)
			if err != nil {
				return nil, err
			}
			return starlark.String(decoded), nil
		}),
	},
}

func starlarkValueFromGo(v any) (starlark.Value, error) {
	switch x := v.(type) {
	case nil:
		return starlark.None, nil
	case bool:
		return starlark.Bool(x), nil
	case string:
		return starlark.String(x), nil
	case float64:
		if math.Trunc(x) == x && x >= math.MinInt64 && x <= math.MaxInt64 {
			return starlark.MakeInt64(int64(x)), nil
		}
		return starlark.Float(x), nil
	case int64:
		return starlark.MakeInt64(x), nil
	case int:
		return starlark.MakeInt(x), nil
	case []any:
		elems := make([]starlark.Value, len(x))
		for i, item := range x {
			sv, err := starlarkValueFromGo(item)
			if err != nil {
				return nil, err
			}
			elems[i] = sv
		}
		return starlark.NewList(elems), nil
	case map[string]any:
		dict := starlark.NewDict(len(x))
		for k, item := range x {
			sv, err := starlarkValueFromGo(item)
			if err != nil {
				return nil, err
			}
			if err := dict.SetKey(starlark.String(k), sv); err != nil {
				return nil, err
			}
		}
		return dict, nil
	default:
		return nil, fmt.Errorf("unsupported JSON value type %T", v)
	}
}

func goValueFromStarlark(v starlark.Value) (any, error) {
	switch x := v.(type) {
	case starlark.NoneType:
		return nil, nil
	case starlark.Bool:
		return bool(x), nil
	case starlark.Int:
		if i, ok := x.Int64(); ok {
			return i, nil
		}
		return float64(x.Float()), nil
	case starlark.Float:
		return float64(x), nil
	case starlark.String:
		return string(x), nil
	case *starlark.List:
		out := make([]any, 0, x.Len())
		for i := 0; i < x.Len(); i++ {
			gv, err := goValueFromStarlark(x.Index(i))
			if err != nil {
				return nil, err
			}
			out = append(out, gv)
		}
		return out, nil
	case starlark.Tuple:
		out := make([]any, 0, x.Len())
		for i := 0; i < x.Len(); i++ {
			gv, err := goValueFromStarlark(x.Index(i))
			if err != nil {
				return nil, err
			}
			out = append(out, gv)
		}
		return out, nil
	case *starlark.Dict:
		out := make(map[string]any, x.Len())
		for _, item := range x.Items() {
			k, ok := item[0].(starlark.String)
			if !ok {
				return nil, fmt.Errorf("dict keys must be strings, got %s", item[0].Type())
			}
			gv, err := goValueFromStarlark(item[1])
			if err != nil {
				return nil, err
			}
			out[string(k)] = gv
		}
		return out, nil
	default:
		return nil, fmt.Errorf("starlark value of type %s is not JSON-serializable", v.Type())
	}
}

func hashBytes(value []byte) string {
	hash := sha256.Sum256(value)
	return fmt.Sprintf("%x", hash[:])
}
