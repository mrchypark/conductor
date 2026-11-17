# External credential provider execution

An external credential provider manages users' SaaS accounts, API keys, OAuth
tokens, account identity, consent, refresh, and revocation. Conductor requests
credentials for an explicitly approved connection and executes the selected SaaS
operation. The credential provider and the destination SaaS are separate roles;
`provider_id` in a delivery binding identifies the registered SaaS provider.

The current adapter consumes `POST /auth/v1/connection-grants/{grant_id}/credential`
through `connector.credentials.invoke@1`. Both synchronous executions and queued
worker executions use this handler. Each provider call first obtains a fresh
credential from the external credential provider. No SaaS credential or human access token is stored
in the execution database, input, manifest, or callback.

This is an operator-configured integration for trusted service callers. It does
not implement browser login, consent handoff, workspace membership, or a public
multi-user API. The service caller must authorize its users before submission.
`actor` and `workspace_id` are not user-authentication evidence. The credential provider validates
the human token and grant on every delivery; Conductor additionally checks the
configured workspace, connection, local revision, and allowed operation.

## Configuration

Set `CONDUCTOR_CREDENTIAL_PROVIDER_CONFIG` to an operator-controlled JSON file and configure
`CONDUCTOR_SERVICE_TOKEN`. Startup fails if the latter is absent or the config is
invalid. The existing local `connector.invoke@2` remains independent. There is
no fallback from a rejected credential-provider grant to local credentials.

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
the credential provider's configured resource and required credential-use scope.
The exact audience and scope identifiers belong to the configured provider's
authorization policy; Conductor does not hardcode a vendor-specific scope.
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

### Request-scoped human authorization (versions 2 and 3)

`connector.credentials.invoke@2` accepts the human access token for each request
instead of reading it from a token file. Configure `CONDUCTOR_SERVICE_TOKEN` and
authenticate the trusted caller with the normal `Authorization: Bearer <service-token>`
header. Send exactly one separate
`X-Conductor-Human-Authorization: Bearer <human-access-token>` header to
`POST /v1/executions`, and set `handler_version` to `2` in the request below.
The human token must belong to the requesting user and be intended for the
credential provider's resource and credential-use scope. The credential provider
validates it; the service token, actor, and workspace are not substitutes.

Version 3 uses the same request and header contract with `handler_version` set to
`3`. For OAuth bindings it sends the reviewed, query-free HTTPS GET through AMP's
generic connector using the authenticated delivery client. Its `provider_id` must
also identify a provider in AMP's catalog; an unknown provider fails that version 3
invocation before contacting the credential provider or SaaS, while version 2
remains available. The reviewed endpoint determines the destination. API-key
bindings use the same fixed Gowid delivery as version 2. Version 3 does not add
arbitrary operations or destinations.

Versions 2 and 3 run synchronously only. Do not use `Prefer: respond-async` or submit
them through `/v1/events` or worker routes, and omit `retry` and `retry_policy`.
The human authorization header is
rejected on other routes and for other handlers. Send it only over TLS, and prevent
proxies and request logging from recording it. It is passed in request context,
not in execution input or a durable queue; a retry requires a new authenticated
request with current human authorization.
Conductor rejects a version 2 or 3 request if its decoded execution fields contain
the human token, including as part of another value. Use the dedicated header
only; do not copy the token into actor, identifiers, or other request fields.

Execution results and events are available to service-authenticated callers.
The trusted caller must authorize each user's access to those reads externally;
Conductor does not bind result reads to the human token used for submission.

After a restart, abandoned version 2 or 3 executions are marked failed with an
unknown outcome after their lease expires, even when no local worker is configured.
They are not replayed because their human token is not retained.

The same reviewed bindings are used for all three versions. Omit `token_file` from
every binding to register versions 2 and 3; configure it for every binding to
also register version 1. Mixed file-backed and file-free bindings are rejected.

### File-backed authorization (version 1)

Send the normal service Bearer authorization to `POST /v1/executions`:

```json
{
  "execution_request_id": "github-read-001",
  "workspace_id": "workspace-1",
  "handler_key": "connector.credentials.invoke",
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

`revision` is Conductor's positive local authorization revision, not the credential provider's
credential version. When replacing consent, owner, operation, or destination,
increase it (or allocate a new connection ID) and restart with the new config.
Old queued requests then fail their revision check. OAuth refresh under the same
credential-provider generation/account/scopes may advance `credential_version`; it does not
change the local revision. API-key replacement requires an updated exact version.
Conductor does not automatically request credential refresh or replay failed delivery;
the trusted authorization lifecycle owns explicit refresh and token renewal.

## Standalone and HA

The handler uses the existing execution store and worker lease path, with no new
database or local queue. Configure the same approved bindings and revisions on
all standalone/HA API and worker nodes, and provision current human token files
on every node that executes work. Stop/drain workers while changing bindings
across nodes so an old node cannot execute under an obsolete local configuration.
The credential provider still rechecks current grant authority on every delivery. An already
transmitted provider call cannot be recalled by a subsequent revocation.

TLS fixture tests cover HTTP submission, durable worker execution, credential
rotation, binding mismatch, and revocation. They are not evidence of browser
login, real SaaS access, or three-node HA failover; those require deployment-level
verification with the relevant external systems.

## Credential-provider protocol

This is an explicit HTTPS application protocol, not an OAuth/OIDC standard
credential-delivery endpoint or automatic support for arbitrary providers. An
external provider must implement the bodyless POST route and the delivery envelope
validated by these adapters. Requests use only the consumer's current human
Bearer token, without browser cookies or redirects.

API-key responses require `kind=api_key`, `api_key`, `grant_id`, `provider_id`,
`connection_generation`, `credential_version`, `connector_digest`, `header`,
`prefix`, and `consent_expires_at_unix_ms`. OAuth responses require `kind=oauth2`,
`access_token`, `token_type=Bearer`, `grant_id`, `provider_id`,
`connection_generation`, `credential_version`, `account_id`, `scopes`,
`token_expires_at_unix_ms`, and `consent_expires_at_unix_ms`. The provider validates
current token owner, consumer, consent and connection state; Conductor validates
the returned envelope against its reviewed binding before sending it to the SaaS.
Refresh tokens and provider client secrets remain with the credential provider.

Changing to this neutral execution name requires updating deployments' config
environment variable and submitted handler key. Requests already queued for a
retired handler key are not automatically rewritten; drain them before rollout.
