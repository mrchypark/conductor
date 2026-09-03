# Kavun and WASM function workflow

Run commands from the Conductor repository root.

## Common prerequisites

Function registration changes the executable surface and is disabled unless
`CONDUCTOR_SERVICE_TOKEN` is configured. Never paste the token into a tracked
file.

For local API testing, start an isolated server in one terminal:

```bash
task_data_dir=$(mktemp -d)
CONDUCTOR_SERVICE_TOKEN=local-skill-token \
CONDUCTOR_WORKER_ID=local-skill-worker \
go run ./cmd/conductor -db "$task_data_dir" -addr 127.0.0.1:18080
```

Use these variables in another terminal:

```bash
export CONDUCTOR_URL=http://127.0.0.1:18080
export CONDUCTOR_SERVICE_TOKEN=local-skill-token
```

The fixed example token is only for the isolated local process. Use an injected
secret for any shared environment.

For a local unauthenticated OCI registry:

```bash
docker run -d --name conductor-wasm-registry \
  -p 127.0.0.1:5000:5000 registry:2
curl --fail http://127.0.0.1:5000/v2/
```

Check `docker inspect conductor-wasm-registry` if the Docker runtime rewrites
the loopback binding. Do not leave an unauthenticated registry exposed on a
shared network.

## Kavun

Kavun functions read the JSON-compatible global `input` and assign `output`.
File imports are disabled. The only optional modules are `base64`, `hex`,
`json`, `math`, and `text`. Source size is capped at 256 KiB; frame and stack
limits cannot exceed 4096 and 65536.

Minimal definition and registration:

```bash
curl --fail-with-body -X POST "$CONDUCTOR_URL/v1/handlers" \
  -H "Authorization: Bearer $CONDUCTOR_SERVICE_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{
    "kind": "kavun",
    "manifest": {
      "handler_key": "example.kavun.echo",
      "version": "1",
      "input_schema": {"type":"object"},
      "output_schema": {"type":"object"},
      "idempotency": "pure",
      "default_timeout_seconds": 10,
      "resource_class": "small"
    },
    "source": "output = input",
    "allowed_modules": [],
    "max_frames": 256,
    "max_stack": 2048
  }'
```

Use a new version if the source or any manifest/limit field changes.

## WASM

The ABI identifier is `conductor:function/v1`. The module exports exactly one
memory named `memory` and these functions:

```text
alloc(length: i32) -> pointer: i32
invoke(pointer: i32, length: i32) -> packed_output: i64
dealloc(pointer: i32, length: i32)
```

`invoke` receives UTF-8 JSON. Pack the output pointer into the high 32 bits and
the output length into the low 32 bits. Return valid UTF-8 JSON. Imports are not
allowed.

Current hard limits:

- artifact: 4 MiB
- linear memory: 256 pages / 16 MiB
- input: 1 MiB
- output: 64 KiB
- default timeout: 30 seconds unless the manifest sets a lower positive value

Compile with a toolchain that targets `wasm32-unknown-unknown` or another core
WebAssembly target capable of emitting the exact ABI. Do not target WASI.

Publish the binary as one OCI layer:

```bash
oras push ghcr.io/example/conductor-functions/example:v1 \
  --artifact-type application/vnd.conductor.function.v1 \
  ./function.wasm:application/vnd.conductor.function.wasm.v1+wasm

oras resolve ghcr.io/example/conductor-functions/example:v1
```

If `oras` is not installed, use its pinned Go CLI without changing this
repository's dependencies:

```bash
go run oras.land/oras/cmd/oras@v1.2.0 push \
  --plain-http 127.0.0.1:5000/conductor-functions/example:v1 \
  --artifact-type application/vnd.conductor.function.v1 \
  ./function.wasm:application/vnd.conductor.function.wasm.v1+wasm

go run oras.land/oras/cmd/oras@v1.2.0 resolve \
  --plain-http 127.0.0.1:5000/conductor-functions/example:v1
```

When invoking ORAS from a Docker container on macOS, use
`host.docker.internal:5000` for the registry and mount the real path returned by
`pwd -P`; container-local `127.0.0.1` and an unresolved `/tmp` path will not
reach the host registry or artifact.

Configure Conductor before registration. `CONDUCTOR_WASM_REGISTRIES` is an
exact comma-separated host allowlist.

```bash
export CONDUCTOR_WASM_REGISTRIES=ghcr.io
export CONDUCTOR_WASM_REGISTRY_USERNAME=example
export CONDUCTOR_WASM_REGISTRY_PASSWORD="$GHCR_TOKEN"
# CONDUCTOR_WASM_REGISTRY_TOKEN may be used instead.
```

For an unauthenticated development registry exposed only on the local machine,
omit the credential variables and set:

```bash
export CONDUCTOR_WASM_REGISTRIES=127.0.0.1:5000
export CONDUCTOR_WASM_REGISTRY_PLAIN_HTTP=true
```

Never enable plain HTTP for a remote or shared registry.

Restart the local server with those variables, then register the digest returned
by `oras resolve`:

```bash
curl --fail-with-body -X POST "$CONDUCTOR_URL/v1/handlers" \
  -H "Authorization: Bearer $CONDUCTOR_SERVICE_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{
    "kind": "wasm",
    "manifest": {
      "handler_key": "example.wasm.transform",
      "version": "1",
      "input_schema": {"type":"object"},
      "output_schema": {"type":"object"},
      "idempotency": "pure",
      "default_timeout_seconds": 10,
      "resource_class": "small"
    },
    "artifact": "ghcr.io/example/conductor-functions/example@sha256:REPLACE_WITH_DIGEST",
    "abi": "conductor:function/v1"
  }'
```

Registration fetches, verifies, compiles, and instantiates the artifact before
persisting metadata. Rhiza stores the manifest digest, blob digest, size, ABI,
and handler manifest, but not the artifact bytes. On restart, restoration is
lazy: the first execution fetches the artifact and later executions reuse the
compiled module.

## Invoke and verify

Use a unique execution request and idempotency key for every smoke test:

```bash
smoke_id="skill-smoke-$(date +%s)-$$"
curl --fail-with-body -X POST "$CONDUCTOR_URL/v1/executions" \
  -H "Authorization: Bearer $CONDUCTOR_SERVICE_TOKEN" \
  -H 'Content-Type: application/json' \
  -d "{
    \"execution_request_id\": \"$smoke_id\",
    \"workspace_id\": \"local-skill\",
    \"handler_key\": \"example.kavun.echo\",
    \"handler_version\": \"1\",
    \"operation_key\": \"invoke\",
    \"actor\": {\"type\":\"service\",\"id\":\"conductor-functions-skill\"},
    \"idempotency_key\": \"$smoke_id\",
    \"input\": {\"ok\":true}
  }"
```

Change only `handler_key` and `handler_version` to exercise the WASM handler.
The synchronous response must be terminal `succeeded`, and `output` must match
the function contract. If using `Prefer: respond-async`, poll the returned
execution ID instead of expecting a terminal response immediately.

## Focused repository checks

Run the matching checks before API smoke testing:

```bash
go test ./internal/handlers ./internal/storage ./cmd/conductor
go test -race ./internal/handlers
```

Relevant regression coverage:

- `TestKavunHandlerCompilesAndRunsPerRequest`
- `TestDynamicKavunHandlerRegistrationRequiresServiceToken`
- `TestWASMHandlerExecutesJSONWithFreshInstances`
- `TestWASMHandlerInterruptsInfiniteLoop`
- `TestWASMHandlerKeepsPureRetryBoundary`
- `TestWASMHandlerRegistrationPersistsMetadataAndReloadsLazily`
- `TestOCIWASMFetcherRequiresAllowlistedDigestReference`
- `TestWASMHandlerMetadataIsImmutableAndContainsNoArtifactBytes`

For the full WASM deployment and failure model, also read
`docs/spec/2026-09-04-wasm-function-deployment.md`.

## Failure interpretation

- HTTP 401/403: service authentication or registration boundary is missing.
- HTTP 409: the handler key/version already exists; choose a new immutable
  version rather than overwriting it.
- `wasm_artifact_unavailable`: transient registry retrieval failure; retryable
  until the compiled module is cached.
- `wasm_invalid`: ABI, import, memory, or registered artifact identity mismatch;
  fix and publish a new version.
- `output_too_large`: reduce output below the 64 KiB WASM boundary.
- timeout/cancellation: inspect runaway guest logic; never solve it by removing
  the deadline or increasing memory without evidence.
