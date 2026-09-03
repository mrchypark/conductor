---
name: conductor-functions
description: Define, register, and verify Conductor dynamic functions using Kavun source or digest-pinned WebAssembly OCI artifacts. Use for function authoring, handler registration, deployment smoke tests, ABI validation, or choosing between Kavun and WASM in this repository.
---

# Conductor Functions

Use the existing `Handler` boundary and `POST /v1/handlers`; do not build a
parallel function runner.

Read [references/workflow.md](references/workflow.md) before defining,
registering, or testing a function. It contains the current payloads, WASM ABI,
limits, and smoke-test procedure.

## Choose the engine

- Use Kavun for small JSON transforms that benefit from source registration and
  only need the existing deterministic module allowlist.
- Use WASM for compiled functions or language/toolchain portability. The first
  ABI is a side-effect-free JSON transform, not a general WASI program.
- Do not convert an existing function between engines unless the user asks.

## Preserve these boundaries

- Treat `handler_key@version` as immutable. Any source, schema, limit, ABI, or
  artifact change gets a new version.
- Register only through a service-authenticated endpoint.
- For WASM, use an allowlisted OCI registry and a `sha256` manifest reference;
  never deploy by mutable tag or store `.wasm` bytes in Rhiza.
- Keep OCI transport on HTTPS. Set `CONDUCTOR_WASM_REGISTRY_PLAIN_HTTP=true`
  only for an unauthenticated registry bound to the local development machine.
- Do not enable WASI, host imports, filesystem, network, environment, clock,
  randomness, or secrets for the v1 ABI.
- Do not put tokens in request JSON, logs, fixtures, or documentation. Read them
  from environment variables.
- Default to a local Conductor with a temporary data directory. Registration on
  a shared or production endpoint requires the user's explicit target and scope.

## Verification gate

Before reporting success:

1. Run the focused engine and HTTP/storage tests from the reference.
2. Confirm registration returned HTTP 201.
3. Invoke the exact registered key and version and verify terminal status and
   JSON output.
4. For persistence-sensitive changes, restart Conductor and invoke again.
5. Report the engine, immutable version, artifact digest when applicable, test
   commands, and any registry or live-environment step not exercised.
