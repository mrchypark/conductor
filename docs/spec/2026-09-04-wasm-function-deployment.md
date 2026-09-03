# WASM function deployment

## Decision

Conductor accepts side-effect-free functions as digest-pinned OCI artifacts and
executes them in-process with wazero. Rhiza stores only immutable handler and
artifact metadata. Artifact bytes and compiled machine code are disposable node
caches, so correctness does not depend on a persistent volume.

This first ABI intentionally excludes WASI, host imports, filesystem, network,
environment variables, clocks, randomness, connector credentials, and instance
pooling. Kavun remains available for small source-defined transforms.

## Artifact contract

An artifact is an OCI image manifest with:

- artifact type `application/vnd.conductor.function.v1`
- exactly one layer
- layer type `application/vnd.conductor.function.wasm.v1+wasm`
- a maximum WASM size of 4 MiB
- a `sha256` manifest digest used in the registration reference

Example publication:

```bash
oras push ghcr.io/example/conductor-functions/normalize:v1 \
  --artifact-type application/vnd.conductor.function.v1 \
  ./function.wasm:application/vnd.conductor.function.wasm.v1+wasm

oras resolve ghcr.io/example/conductor-functions/normalize:v1
```

Use the returned digest, not the mutable tag, when registering the function.

## Guest ABI

ABI identifier: `conductor:function/v1`.

The module exports exactly one memory named `memory` and these functions:

```text
alloc(length: i32) -> pointer: i32
invoke(pointer: i32, length: i32) -> packed_output: i64
dealloc(pointer: i32, length: i32)
```

`invoke` receives UTF-8 JSON. Its result packs the output pointer in the high
32 bits and output length in the low 32 bits. The output must also be valid
UTF-8 JSON. The host owns both allocations and calls `dealloc` after copying the
result.

The module must not import functions or memory. Instantiation also fails closed
if another unresolved import exists.

## Limits and lifecycle

- runtime memory ceiling: 256 WebAssembly pages (16 MiB)
- input ceiling: 1 MiB
- output ceiling: 64 KiB
- default execution timeout: 30 seconds
- compile/admission timeout: 5 seconds after download
- fresh module instance per invocation
- one compiled module cached per registered handler process

The runtime enables wazero context termination. A timeout closes the executing
guest instance. Traps and malformed output become non-retryable handler errors.
An OCI retrieval failure before the module is cached becomes
`wasm_artifact_unavailable` and is retryable.

## Registration

Configure the registry boundary before starting Conductor:

```bash
export CONDUCTOR_WASM_REGISTRIES=ghcr.io
export CONDUCTOR_WASM_REGISTRY_USERNAME=example
export CONDUCTOR_WASM_REGISTRY_PASSWORD="$GHCR_TOKEN"
# CONDUCTOR_WASM_REGISTRY_TOKEN may be used instead of username/password.
```

An unauthenticated registry on the local development machine may use HTTP:

```bash
export CONDUCTOR_WASM_REGISTRIES=127.0.0.1:5000
export CONDUCTOR_WASM_REGISTRY_PLAIN_HTTP=true
```

Plain HTTP is opt-in and must not be used for remote or shared registries.

Only exact registry hosts in `CONDUCTOR_WASM_REGISTRIES` are accepted. The
existing `CONDUCTOR_SERVICE_TOKEN` protects registration.

```bash
curl -X POST http://localhost:8080/v1/handlers \
  -H "Authorization: Bearer $CONDUCTOR_SERVICE_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{
    "kind": "wasm",
    "manifest": {
      "handler_key": "normalize.customer",
      "version": "1",
      "input_schema": {"type":"object"},
      "output_schema": {"type":"object"},
      "idempotency": "pure",
      "default_timeout_seconds": 10,
      "resource_class": "small"
    },
    "artifact": "ghcr.io/example/conductor-functions/normalize@sha256:...",
    "abi": "conductor:function/v1"
  }'
```

Registration downloads, verifies, and compiles the artifact before committing
its metadata. The pair `handler_key@version` is immutable. After restart,
Conductor restores the metadata without downloading the artifact; the first
execution fetches and verifies it, then subsequent executions reuse the
compiled module.

## Durable state

Rhiza table `wasm_handlers` contains the handler manifest, original digest-pinned
reference, manifest digest, WASM blob digest, size, and ABI. It never contains
the WASM bytes. Both manifest and blob identity are checked again after a lazy
fetch.

If the registry is unavailable after restart, uncached calls fail retryably.
Already compiled handlers continue to execute. Registry durability is therefore
an operational dependency, while node caches remain optional.

## Isolation boundary

This mode is for trusted or semi-trusted pure transforms. WebAssembly isolation
does not provide a hard per-tenant CPU quota or process crash boundary. Hostile
multi-tenant code, native libraries, connector side effects, network access, or
strict resource accounting must use a separate worker process/container. A
future connector capability must preserve Conductor's fencing and idempotency
rules and must never expose raw credentials to guest memory.
