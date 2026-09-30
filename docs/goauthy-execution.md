# GoAuthy credential delivery in executions

Conductor consumes `POST /auth/v1/connection-grants/{grant_id}/credential`
through `connector.goauthy.invoke@1`. Both synchronous executions and queued
worker executions use this handler. Each provider call first obtains a fresh
credential from GoAuthy. No provider credential or human access token is stored
in the execution database, input, manifest, or callback.

This is an operator-configured integration for trusted service callers. It does
not implement browser login, consent handoff, workspace membership, or a public
multi-user API. The service caller must authorize its users before submission.
`actor` and `workspace_id` are not user-authentication evidence. GoAuthy validates
the human token and grant on every delivery; Conductor additionally checks the
configured workspace, connection, local revision, and allowed operation.

## Configuration

Set `CONDUCTOR_GOAUTHY_CONFIG` to an operator-controlled JSON file and configure
`CONDUCTOR_SERVICE_TOKEN`. Startup fails if the latter is absent or the config is
invalid. The existing local `connector.invoke@1` remains independent. There is
no fallback from a rejected GoAuthy grant to local credentials.

Example OAuth binding (replace all sample values with reviewed consent metadata):

```json
{
  "issuer": "https://id.example.com",
  "bindings": [{
    "connection_id": "github-work-read",
    "workspace_id": "workspace-1",
    "revision": 1,
    "operation_key": "github.user.read",
    "token_file": "/run/secrets/conductor-connections-token",
    "oauth": {
      "grant_id": "approved-grant",
      "provider_id": "github",
      "account_id": "reviewed-provider-account-id",
      "connection_generation": "reviewed-generation",
      "resource": "https://id.example.com/connections",
      "consumer_client_id": "conductor",
      "owner_subject": "reviewed-owner-subject",
      "endpoint": "https://api.github.com/user",
      "scopes": ["read:user"],
      "credential_version": 1,
      "consent_expires_at_unix_ms": 1893456000000
    }
  }]
}
```

The token file contains only this consumer's current **human access token** for
GoAuthy's configured connections resource with `goauthy.connections.use`.
An ID token, Conductor service token, client-credentials token, or another
consumer's token is not a replacement. Provision it through the trusted host's
OAuth/session integration. The file is read on each call, so atomic replacement
rotates the token without restarting Conductor. Restrict file access to the
Conductor process. Do not put token contents in this JSON or execution input.

For the fixed Gowid API-key adapter, replace `oauth` with `api_key`:

```json
{
  "grant_id": "approved-grant",
  "provider_id": "registered-gowid-provider",
  "connection_generation": "reviewed-generation",
  "connector_digest": "reviewed-connector-digest",
  "header": "Authorization",
  "prefix": "",
  "url": "https://openapi.gowid.com/v2/expense-statements?page=1",
  "credential_version": 1,
  "consent_expires_at_unix_ms": 1893456000000
}
```

Exactly one credential kind is allowed per binding. API-key delivery currently
supports only this fixed Gowid GET and raw Authorization header. OAuth delivery
supports only the exact, query-free HTTPS GET in its reviewed endpoint. Callers
cannot choose URLs, methods, headers, or credential files through input.

## Submit work

Send the normal service Bearer authorization to `POST /v1/executions`:

```json
{
  "execution_request_id": "github-read-001",
  "workspace_id": "workspace-1",
  "handler_key": "connector.goauthy.invoke",
  "handler_version": "1",
  "operation_key": "github.user.read",
  "connection_id": "github-work-read",
  "credential_revision": 1,
  "actor": {"type": "service", "id": "trusted-caller"},
  "idempotency_key": "github-read-001",
  "input": {}
}
```

Add `Prefer: respond-async` and set `CONDUCTOR_WORKER_ID` to use the durable queue.
Existing execution status, events, idempotency, and callbacks apply. Grant denial,
expiry, generation mismatch, or an unavailable token file fails the execution
without contacting the provider. The provider response must be bounded JSON and
must not echo the delivered credential or human token.

`revision` is Conductor's positive local authorization revision, not GoAuthy's
credential version. When replacing consent, owner, operation, or destination,
increase it (or allocate a new connection ID) and restart with the new config.
Old queued requests then fail their revision check. OAuth refresh under the same
GoAuthy generation/account/scopes may advance `credential_version`; it does not
change the local revision. API-key replacement requires an updated exact version.
Conductor does not automatically request GoAuthy refresh or replay failed delivery;
the trusted authorization lifecycle owns explicit refresh and token renewal.

## Standalone and HA

The handler uses the existing execution store and worker lease path, with no new
database or local queue. Configure the same approved bindings and revisions on
all standalone/HA API and worker nodes, and provision current human token files
on every node that executes work. Stop/drain workers while changing bindings
across nodes so an old node cannot execute under an obsolete local configuration.
GoAuthy still rechecks current grant authority on every delivery. An already
transmitted provider call cannot be recalled by a subsequent revocation.

TLS fixture tests cover HTTP submission, durable worker execution, credential
rotation, binding mismatch, and revocation. They are not evidence of browser
login, real SaaS access, or three-node HA failover; those require deployment-level
verification with the relevant external systems.
