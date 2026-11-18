# Conductor

Conductor is a Go runtime for durable, versioned function and connector execution.
It persists execution requests in embedded [Rhiza](https://github.com/mrchypark/rhiza),
runs a pinned `handler@version`, retries retryable failures, and records terminal
results and runtime callbacks. Local and external workers share a lease and fencing
protocol. Connector support uses the AMP connectors catalog, with encrypted
connections and OAuth/PKCE support.

Conductor owns execution. A workflow service owns workflow graphs, task completion,
and submissions; a business service owns business records, schemas, and mutations.
Those services may live in one application or separate applications. Conductor's
`POST /v1/events` adapter accepts execution-linked workflow outbox events; it does
not interpret workflow graphs.

## Run from source

Prerequisites:

- **Go 1.27.1 or newer**, matching `go.mod` (CI targets Go 1.27.x).
- Git and network access to download Go modules; curl for the example below.
- Docker with BuildKit/buildx for container builds, if needed.

The current embedded database dependencies do not require a separately installed
native database library or database server. Normal builds can use `CGO_ENABLED=0`;
Go's race detector requires CGO and a supported C toolchain.

From a checkout of this repository:

```sh
go mod download
go build -o ./conductor ./cmd/conductor
CONDUCTOR_SERVICE_TOKEN=local-development-token \
  ./conductor -addr 127.0.0.1:8080 -db ./conductor-data
```

`-db` names a Rhiza **data directory**, not a SQLite file. Keep that directory across
restarts. Without flags or environment overrides, the source binary listens on
`:8080` and uses `conductor.db` as its data directory.

In another terminal, execute the built-in echo handler:

```sh
curl --fail-with-body http://127.0.0.1:8080/v1/executions \
  -H 'Authorization: Bearer local-development-token' \
  -H 'Content-Type: application/json' \
  --data '{"execution_request_id":"er-http","workspace_id":"ws-local","handler":"payload.echo","handler_version":"1","operation":"echo","actor":{"type":"service","id":"readme-client"},"idempotency_key":"readme:er-http","input":{"ok":true}}'

curl --fail-with-body http://127.0.0.1:8080/v1/executions/exec_er-http \
  -H 'Authorization: Bearer local-development-token'
```

Authenticated submissions require `workspace_id`, an actor with `type` and `id`,
and an `idempotency_key`. The synchronous POST returns HTTP 200 with
`id: "exec_er-http"`, `status: "succeeded"`, and the echoed
input in `output`; GET retrieves the persisted execution. Use a new
`execution_request_id` and `idempotency_key` for a different request.

For queued execution, add `Prefer: respond-async`. Set `CONDUCTOR_WORKER_ID` before
starting the server to enable its local lease worker, or use an external worker.
Queued submissions return HTTP 202 and need a worker to execute them.

## Docker

The self-contained multi-stage Dockerfile builds the binary from source; no
prebuilt `target/` binary is needed. Build and run a Linux amd64 image locally:

```sh
docker buildx build --platform linux/amd64 --load -t conductor:local .
docker volume create conductor-data
docker run --rm --name conductor --platform linux/amd64 \
  -p 127.0.0.1:8080:8080 \
  -v conductor-data:/data \
  -e CONDUCTOR_DB=/data/conductor.db \
  -e CONDUCTOR_SERVICE_TOKEN=local-development-token \
  conductor:local
```

The container uses `/data/conductor.db` as its database directory within the `/data`
volume and port 8080 for HTTP. The named volume
preserves data when the container is removed; deleting the volume deletes that
stored state. The curl example above works with this container too.

The planned publication target is
`ghcr.io/mrchypark/conductor` for **linux/amd64**, tagged with the **full Git commit SHA**
on `main` pushes and with the **version tag** (such as `v0.1.1`) on `v*` release tag pushes
(no `latest` tag is published). Publication follows Go formatting, vet, tests, race
checks, and Docker build/smoke checks. Availability depends on a successful publish;
these tags are not a claim that an image has already been released.

Once published, replace `conductor:local` in the run command with a published version tag
or full commit SHA:

```sh
CONDUCTOR_IMAGE_TAG=v0.1.1
docker pull --platform linux/amd64 "ghcr.io/mrchypark/conductor:$CONDUCTOR_IMAGE_TAG"
```

## Configuration and security

Flags `-addr` and `-db` override their environment defaults. Optional integrations
are enabled through the following settings:

| Setting | Purpose / default |
| --- | --- |
| `CONDUCTOR_ADDR` | HTTP listen address; `:8080`. |
| `CONDUCTOR_DB` | Rhiza data directory; `conductor.db` from source, `/data/conductor.db` in the container. |
| `CONDUCTOR_SERVICE_TOKEN` | Incoming API Bearer token; unset by default. |
| `CONDUCTOR_WORKER_ID` | Enables the local execution worker when nonempty. |
| `CONDUCTOR_CALLBACK_URL`, `CONDUCTOR_CALLBACK_TOKEN` | Durable callback destination and optional outgoing Bearer token. |
| `CONDUCTOR_MASTER_KEY` | Raw 16, 24, or 32-byte key enabling encrypted connection storage. Preserve it across restarts. |
| `BUSINESS_SERVICE_BASE_URL`, `BUSINESS_SERVICE_TOKEN` | Enables the business connector and supplies its optional outgoing Bearer token. |
| `CONDUCTOR_CREDENTIAL_PROVIDER_CONFIG` | External credential-provider configuration; with the service token, registers `connector.credentials.invoke@1`. |
| `CONDUCTOR_OAUTH_CLIENT_SECRET`, `CONDUCTOR_OAUTH_CLIENT_SECRET_<PROVIDER>` | OAuth client secret fallback and provider override; provider names are uppercased with `-`, `.`, and `/` replaced by `_`. |
| `CONDUCTOR_WASM_REGISTRIES` | Comma-separated registry host allowlist for WASM artifacts; empty allows none. |
| `CONDUCTOR_WASM_REGISTRY_USERNAME`, `CONDUCTOR_WASM_REGISTRY_PASSWORD`, `CONDUCTOR_WASM_REGISTRY_TOKEN` | Optional host-side OCI credentials. |
| `CONDUCTOR_WASM_REGISTRY_PLAIN_HTTP` | Allows plain HTTP registry access when `true`; for controlled local development. |

When the service token is configured, API requests require
`Authorization: Bearer <token>` except OAuth callback routes, which must remain
reachable by providers. Without it, other API routes are unauthenticated;
**dynamic handler and connection registration still reject requests**. Use a
strong token and an HTTPS reverse proxy for deployment; the server itself serves
HTTP. The example token is only for local development.

Connection registration requires both service authentication and an enabled
connection store. API-key, Basic, custom metadata, and pre-existing OAuth access
tokens can be stored through that encrypted boundary. Connection and OAuth callback
responses do not return provider tokens. Connector credentials stay in host-side
execution code and must not be exposed to WASM guest code.

## API and documentation

| API | Purpose |
| --- | --- |
| `POST /v1/executions`, `GET /v1/executions/{id}` | Submit and inspect execution. |
| `POST /v1/executions/{id}/cancel`, `GET /v1/executions/{id}/events` | Cancel and inspect runtime events. |
| `GET /v1/handlers`, `POST /v1/handlers` | List manifests and register dynamic handlers. |
| `GET /v1/handlers/definition?handler_key=...&version=...` | Read one stored Kavun definition with service authentication. |
| `GET /v1/connectors`, `GET /v1/connectors/{provider}` | Discover connector providers and manifests. |
| `POST /v1/worker/tasks:poll`, `tasks:heartbeat`, `tasks:complete`, `tasks:fail` | External worker lease protocol; all four use the `/v1/worker/` prefix. Short `/v1/worker/{poll,heartbeat,complete,fail}` aliases also work. |
| `POST /v1/connections` | Register encrypted credentials. |
| `POST /v1/oauth/{provider}/begin` | Begin OAuth authorization. |
| `GET` or `POST /v1/oauth/callback` or `/v1/oauth/{provider}/callback` | Resolve OAuth attempts using returned state. |
| `POST /v1/events` | Accept an `{"events":[...]}` envelope; each event needs `data._execution.workspace_id` and `execution_request_id`. |

The definition endpoint requires a configured service token and exactly one URL-encoded
`handler_key` and `version`. It returns `kind`, `manifest`, `source`,
`allowed_modules`, `max_frames`, and `max_stack`, with `Cache-Control: no-store`.
Invalid queries return 400; an absent configured token returns 403 and an invalid
bearer token returns 401. Missing definitions, including built-in and WASM handlers,
return 404. Unavailable storage returns 503; invalid stored definitions or storage errors return 500.
The serialized response is limited to 6 MiB plus 4 KiB; stored source is checked
against its SHA-256 hash before it is returned. The HTTP server's standard 1 MiB
header limit includes the request target and headers, and clients or proxies may
impose lower limits. Consequently, some previously registered long identifiers
cannot be retrieved through this GET endpoint. The application also caps the
encoded query at 3 MiB.

See the focused documents for full contracts and deployment details:

- [System boundaries](docs/spec/2026-08-29-workflow-execution-system-overview.md)
- [Execution service design](docs/spec/2026-08-29-conductor-service-design.md)
- [Connection profiles](docs/connection-profiles.md)
- [API-key delivery](docs/api-key-delivery.md)
- [External credential-provider execution](docs/credential-provider-execution.md)
- [WASM function deployment](docs/spec/2026-09-04-wasm-function-deployment.md)

Handlers are immutable: create a new version when executable behavior, input/output
schemas, limits, pinned connector operations, or the WASM artifact digest changes.
Pin connector execution to `connector.invoke@2`; older service-specific config,
provider/operation prefixes, and event metadata names have no compatibility aliases.
Update clients and outbox producers together and drain older executions as needed.

Dynamic handlers may use Kavun source or `kind: "wasm"`. Kavun disables file imports
and default modules, with an allowlist of `base64`, `hex`, `json`, `math`, and `text`.
WASM references must use an allowlisted OCI registry and a **SHA-256 manifest digest**,
not a mutable tag. Rhiza stores artifact identity and manifest metadata, never WASM
bytes. Each invocation uses a fresh wazero guest with an unprivileged JSON ABI and
a 16 MiB memory ceiling. See the deployment document rather than treating this as
an unrestricted code-execution environment.

## Persistence, HA, and migration

Standalone operation uses embedded Rhiza without an external object store.
Multi-node HA requires an explicit shared S3-compatible store. Give each process
its own data directory, `CONDUCTOR_RHIZA_NODE_ID`, `CONDUCTOR_RHIZA_PEER_ADDR`, and
`CONDUCTOR_RHIZA_PEER_TOKEN`; use the same `CONDUCTOR_RHIZA_CLUSTER_ID` and
`CONDUCTOR_RHIZA_MEMBERS` JSON across the cluster.

Membership entries contain `node_id`, `peer_url` (`quic://host:port`), and
`public_key` (standard base64 of a 32-byte Ed25519 public key), generated with
Rhiza's `PeerPublicKey(clusterID, nodeID, peerToken)`. Each node receives only its
own peer secret. The old per-member `token` field is unsupported.

`CONDUCTOR_RHIZA_BIND_ADDR` and `CONDUCTOR_RHIZA_PEER_ADDR` default to
`127.0.0.1:0`; cluster/node IDs default to `conductor`/`n1`.
`CONDUCTOR_RHIZA_ADMIN_TOKEN` configures Rhiza's admin token. Shared object-store
settings are `CONDUCTOR_RHIZA_S3_PROVIDER`, `_ENDPOINT`, `_BUCKET`, `_PREFIX`,
`_REGION`, `_ACCESS_KEY`, `_SECRET_KEY`, `_SESSION_TOKEN`, and `_INSECURE` (each
suffix uses the `CONDUCTOR_RHIZA_S3` prefix). An endpoint without a provider selects
`s3`. These map to Rhiza's `ObjStore*` configuration.

**Rhiza v0.12.0 → v0.18.0 migration warning:** never open an existing v0.12.0
store directly with v0.18.0. SQL execution policy and WAL formats changed.
Standalone and HA deployments both require logical application-data migration to
new data directories and a separate cluster/object-store namespace. Preserve the
old binary and store, and reconcile in-flight executions and callback results
before migration. Copying old checkpoints/WALs and mixed-version HA rolling
upgrades are unsupported. Follow the
[Rhiza migration procedure](https://github.com/mrchypark/rhiza/blob/v0.18.0/docs/sql-execution-policy.md).

## Development

```sh
gofmt -w ./cmd ./internal
go vet ./...
go test ./...
go test -race ./...
```

The optional three-process HA test needs a running S3-compatible service and an
already-created bucket. Without `CONDUCTOR_HA_E2E_S3_ENDPOINT`, it skips:

```sh
CONDUCTOR_HA_E2E_S3_ENDPOINT=127.0.0.1:9000 \
CONDUCTOR_HA_E2E_S3_BUCKET=conductor-ha-e2e \
CONDUCTOR_HA_E2E_S3_ACCESS_KEY=minioadmin \
CONDUCTOR_HA_E2E_S3_SECRET_KEY=minioadmin \
go test -run '^TestThreeProcessRhizaHA$' -count=1 ./internal/storage
```

The current implementation is Go. The previous Rust implementation is retained
on the [`legacy-rust` branch](../../tree/legacy-rust).

## License

Conductor is licensed under the [MIT License](LICENSE).
