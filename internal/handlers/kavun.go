package handlers

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/jokruger/kavun"
	"github.com/jokruger/kavun/ast"
	"github.com/jokruger/kavun/compiler"
	"github.com/jokruger/kavun/core"
	bc "github.com/jokruger/kavun/core/bytecode"
	"github.com/jokruger/kavun/stdlib"
	"github.com/jokruger/kavun/vm"
)

const (
	maxKavunSourceBytes = 256 << 10
	defaultKavunFrames  = 256
	defaultKavunStack   = 2048
	maxKavunFrames      = 4096
	maxKavunStack       = 65536
	defaultKavunTimeout = 30
	kavunRuntimeABI     = "kavun-v0.7.1/compiler-o3/input-output"
)

// KavunSpec describes a versioned, sandboxed script handler. The script must
// read the JSON-compatible global `input` and assign the JSON-compatible
// global `output`.
type KavunSpec struct {
	Manifest           Manifest
	Source             []byte
	AllowedModules     []string
	MaxFrames          int
	MaxStack           int
	Bytecode           []byte
	BytecodeHash       string
	SourceHash         string
	BytecodeSourceHash string
	RuntimeABI         string
}

// KavunHandler compiles once at registration and creates a fresh VM with
// cloned globals for each execution; Kavun VMs are not shared between goroutines.
type KavunHandler struct {
	manifest             Manifest
	program              *kavunProgram
	maxFrames            int
	maxStack             int
	runtimeABI           string
	bytecodeNeedsRefresh bool
}

type kavunProgram struct {
	bytecode *vm.Bytecode
	bindings map[string]int
	globals  []core.Value
}

func NewKavunHandler(spec KavunSpec) (*KavunHandler, error) {
	if spec.Manifest.Key == "" || spec.Manifest.Version == "" {
		return nil, errors.New("handler manifest key and version are required")
	}
	if len(spec.Source) == 0 {
		return nil, errors.New("kavun source is required")
	}
	if len(spec.Source) > maxKavunSourceBytes {
		return nil, fmt.Errorf("kavun source exceeds %d bytes", maxKavunSourceBytes)
	}
	manifest := spec.Manifest
	if manifest.DefaultTimeout <= 0 {
		manifest.DefaultTimeout = defaultKavunTimeout
	}
	modules := spec.AllowedModules
	if modules == nil {
		modules = []string{}
	}
	for _, module := range modules {
		if !safeKavunModules[module] {
			return nil, fmt.Errorf("kavun module %q is not allowed", module)
		}
	}
	frames, stack := spec.MaxFrames, spec.MaxStack
	if frames <= 0 {
		frames = defaultKavunFrames
	}
	if stack <= 0 {
		stack = defaultKavunStack
	}
	if frames > maxKavunFrames || stack > maxKavunStack {
		return nil, fmt.Errorf("kavun resource limits exceed frames=%d and stack=%d", maxKavunFrames, maxKavunStack)
	}
	runtimeABI := kavunRuntimeABIFor(modules, frames, stack)
	sourceHash := hashBytes(spec.Source)
	if spec.SourceHash != "" && spec.SourceHash != sourceHash {
		return nil, errors.New("kavun source hash mismatch")
	}
	var program *kavunProgram
	bytecodeNeedsRefresh := len(spec.Bytecode) > 0
	cacheMetadataValid := spec.SourceHash != "" && spec.BytecodeSourceHash == spec.SourceHash
	if len(spec.Bytecode) > 0 && cacheMetadataValid && spec.BytecodeHash == hashBytes(spec.Bytecode) && spec.RuntimeABI == runtimeABI {
		program, _ = decodeKavunProgram(spec.Bytecode)
		if program != nil {
			maxGlobal, err := validateKavunBytecode(program.bytecode, modules)
			if err != nil {
				program = nil
			} else if maxGlobal+1 > len(program.globals) {
				globals := make([]core.Value, maxGlobal+1)
				for i := range globals {
					globals[i] = core.Undefined
				}
				program.globals = globals
			}
		}
		bytecodeNeedsRefresh = program == nil
	}
	var err error
	if program == nil {
		program, err = compileKavunProgram(spec.Source, modules)
		if err != nil {
			return nil, fmt.Errorf("kavun compile: %w", err)
		}
	}
	return &KavunHandler{manifest: manifest, program: program, maxFrames: frames, maxStack: stack, runtimeABI: runtimeABI, bytecodeNeedsRefresh: bytecodeNeedsRefresh}, nil
}

// Keep only deterministic, non-blocking modules. Kavun's times.sleep calls
// time.Sleep directly and cannot be interrupted by a request context.
var safeKavunModules = map[string]bool{
	"base64": true,
	"hex":    true,
	"json":   true,
	"math":   true,
	"text":   true,
}

func (h *KavunHandler) Manifest() Manifest { return h.manifest }

func (h *KavunHandler) BytecodeNeedsRefresh() bool { return h != nil && h.bytecodeNeedsRefresh }

func (h *KavunHandler) Bytecode() ([]byte, string, string, error) {
	if h == nil || h.program == nil || h.program.bytecode == nil {
		return nil, "", "", errors.New("kavun bytecode is unavailable")
	}
	var buf bytes.Buffer
	if err := h.program.bytecode.Encode(&buf); err != nil {
		return nil, "", "", err
	}
	encoded := append([]byte(nil), buf.Bytes()...)
	return encoded, hashBytes(encoded), h.runtimeABI, nil
}

func (h *KavunHandler) Execute(ctx context.Context, req Request) (Result, error) {
	var input any
	if len(req.Input) > 0 {
		if err := json.Unmarshal(req.Input, &input); err != nil {
			return Result{}, &Error{Class: "validation_failed", Message: "input must be valid JSON"}
		}
	}
	inputValue, err := kavun.ValueOf(input)
	if err != nil {
		return Result{}, &Error{Class: "validation_failed", Message: err.Error()}
	}
	output, err := h.program.run(ctx, inputValue, h.maxFrames, h.maxStack)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return Result{}, err
		}
		return Result{}, &Error{Class: "kavun_runtime", Message: err.Error()}
	}
	encoded, err := output.EncodeJSON()
	if err != nil {
		return Result{}, &Error{Class: "kavun_runtime", Message: fmt.Sprintf("encode output: %v", err)}
	}
	return Result{Output: encoded}, nil
}

func compileKavunProgram(source []byte, modules []string) (*kavunProgram, error) {
	symbols := compiler.NewSymbolTable()
	for idx, name := range vm.BuiltinFunctionNames {
		symbols.DefineBuiltin(idx, name)
	}
	globals := make([]core.Value, vm.GlobalsSize)
	bindings := make(map[string]int, 2)
	for _, name := range []string{"input", "output"} {
		symbol := symbols.Define(name)
		bindings[name] = symbol.Index
		globals[symbol.Index] = core.Undefined
	}
	if bindings["input"] != 0 || bindings["output"] != 1 {
		return nil, errors.New("kavun binding layout is incompatible")
	}
	fileSet := ast.NewFileSet()
	sourceFile := fileSet.AddFile("(main)", -1, len(source))
	compilerInstance := compiler.NewCompiler(compiler.O3(), nil, sourceFile, symbols, modules, nil, nil)
	compilerInstance.SetAssignmentMode(compiler.AssignmentModeSmart)
	compilerInstance.EnableFileImport(false)
	if err := compilerInstance.Compile(sourceFile, source, nil); err != nil {
		return nil, err
	}
	globals = globals[:symbols.MaxSymbols()+1]
	return &kavunProgram{bytecode: compilerInstance.Bytecode(), bindings: bindings, globals: globals}, nil
}

func decodeKavunProgram(encoded []byte) (*kavunProgram, error) {
	var bytecode vm.Bytecode
	if err := bytecode.Decode(bytes.NewReader(encoded)); err != nil {
		return nil, err
	}
	return &kavunProgram{bytecode: &bytecode, bindings: map[string]int{"input": 0, "output": 1}, globals: []core.Value{core.Undefined, core.Undefined}}, nil
}

func validateKavunBytecode(bytecode *vm.Bytecode, modules []string) (int, error) {
	if bytecode == nil || bytecode.MainFunction == nil {
		return 0, errors.New("kavun bytecode has no main function")
	}
	allowed := make(map[string]bool, len(modules))
	for _, module := range modules {
		allowed[module] = true
	}
	maxGlobal := 1
	visited := make(map[int]bool)
	var scan func(*core.CompiledFunction) error
	scan = func(function *core.CompiledFunction) error {
		if function == nil {
			return errors.New("kavun bytecode has a nil function")
		}
		for _, instruction := range function.Instructions {
			switch instruction.Op {
			case bc.ImportBuiltinModule:
				if instruction.Op3 > 255 {
					return fmt.Errorf("kavun bytecode references invalid module %d", instruction.Op3)
				}
				name, ok := stdlib.GetModuleName(uint8(instruction.Op3))
				if !ok || !allowed[name] {
					return fmt.Errorf("kavun bytecode imports disallowed module %q", name)
				}
			case bc.LoadGlobal, bc.StoreGlobal, bc.StoreIndexedGlobal:
				index := int(instruction.Op3)
				if index < 0 || index > maxKavunStack {
					return fmt.Errorf("kavun bytecode references invalid global %d", index)
				}
				if index > maxGlobal {
					maxGlobal = index
				}
			case bc.LoadStaticCompiledFunction:
				index := int(instruction.Op3)
				if index < 0 || index >= len(bytecode.Static.CompiledFunctions) {
					return fmt.Errorf("kavun bytecode references invalid function %d", index)
				}
				if !visited[index] {
					visited[index] = true
					if err := scan(&bytecode.Static.CompiledFunctions[index]); err != nil {
						return err
					}
				}
			}
		}
		return nil
	}
	if err := scan(bytecode.MainFunction); err != nil {
		return 0, err
	}
	return maxGlobal, nil
}

func (p *kavunProgram) run(ctx context.Context, input core.Value, maxFrames, maxStack int) (core.Value, error) {
	globals := make([]core.Value, len(p.globals))
	for i, value := range p.globals {
		cloned, err := value.Copy(true)
		if err != nil {
			return core.Undefined, err
		}
		globals[i] = cloned
	}
	globals[p.bindings["input"]] = input
	globals[p.bindings["output"]] = core.Undefined
	machine := vm.NewVM(maxFrames, maxStack)
	machine.Reset(p.bytecode, globals)
	if err := runKavunContext(ctx, machine); err != nil {
		return core.Undefined, err
	}
	return globals[p.bindings["output"]], nil
}

func runKavunContext(ctx context.Context, machine *vm.VM) (err error) {
	result := make(chan error, 1)
	go func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				result <- fmt.Errorf("kavun vm panic: %v", recovered)
			}
		}()
		result <- machine.Run()
	}()
	select {
	case <-ctx.Done():
		machine.Abort()
		<-result
		return ctx.Err()
	case err = <-result:
		return err
	}
}

func hashBytes(value []byte) string {
	hash := sha256.Sum256(value)
	return fmt.Sprintf("%x", hash[:])
}

func kavunRuntimeABIFor(modules []string, maxFrames, maxStack int) string {
	normalized := append([]string(nil), modules...)
	sort.Strings(normalized)
	return strings.Join([]string{kavunRuntimeABI, "modules=" + strings.Join(normalized, ","), "frames=" + strconv.Itoa(maxFrames), "stack=" + strconv.Itoa(maxStack)}, "|")
}
