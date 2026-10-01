# Connection profiles

The integration assumes an external credential provider that owns users' SaaS
account profiles and credentials, with an explicit versioned delivery contract.
This is not an OAuth/OIDC standard. It uses a common profile
model across consumers, distinguished by authentication mechanism and authorized
use, not separate AI/SaaS profile types. An OIDC login token is not a provider
credential. Conductor retains connector execution and its local credential path
for compatibility and independent testing; there is no credential migration here.
Shared databases, internal imports and simultaneous deployment are not required.
A bounded memory-only API-key delivery adapter and TLS fixtures are described in
[API-key delivery](api-key-delivery.md). Operator-configured execution wiring is
available through [credential-provider executions](credential-provider-execution.md). The public user
authorization lifecycle remains unimplemented; the local credential path is unchanged.

## Current contract

- A profile is an explicit `connection_id`. Multiple IDs may use the same provider
  in the same workspace. The caller must select the ID; there is no default-profile
  selection.
- A saved ID cannot change its workspace or provider. Credential rotation within
  that binding remains supported. Creating a different binding requires a new ID.
- Execution requests persist the selected ID and `credential_revision` (JSON
  int64 logical authorization generation), rejecting incompatible idempotent
  resubmissions. Positive generations require a connection ID. Negative values
  are invalid. Old requests retain generation zero for trusted legacy service
  compatibility; this is not a fallback for an authenticated-user API.
- Explicit replacement increases the local generation; revoked IDs cannot be
  reused. Dispatch refuses stale or revoked generations. A new generation requires
  an explicitly new execution, never an automatic new ID on timeout.
- Local schema upgrades use companion tables and backfill existing connections
  at generation one and existing executions at zero without retaining secret
  snapshots. Verified refresh uses a separate internal token CAS version, keeping
  the logical generation stable. Generic AMP refresh does not establish identity
  and scope continuity: pinned expired OAuth credentials fail closed until a
  suitable provider adapter is available.
- OAuth state identifies the stored attempt, workspace, provider and connection.
  Callback state is single-use and expires. Caller-supplied callback metadata is
  not evidence of user ownership or consent.
- The resolver checks workspace/provider equality. Credentials remain in the
  server-side AMP connector path and are not placed in execution input or WASM.

The existing service Bearer token authenticates trusted internal callers. It does
not enforce user ownership, operation-specific profile ACLs, or delegated-user
authorization. Do not expose this path as a multi-user execution API.

## Integration work remaining

Account identity must use a verified issuer and subject, with a Conductor-specific
audience and required API scope. Compos owns workspace membership; Conductor owns
execution authorization, and the external credential provider owns remote profile
ownership, consent and permitted use. `actor`, `_compos`, `subject_ref` and `X-Beesuh-*`
metadata alone grant no authority. Cross-client token exchange is not assumed.

User/organization profile ownership, recipient-enforced execution permissions,
required provider-scope checks and real provider refresh-continuity validation
remain unimplemented. The current generic AMP HTTP capability is not a
fixed-operation destination policy. The target extension must validate user,
consumer, profile, generation, consent and permitted destination/use; neither
claimed identity nor a service token alone grants access. Its credential delivery
contract supports registered API keys and OAuth access tokens. The adapters are
wired to trusted-service executions with operator-approved bindings, not a
public user-authorized API. Do not claim the tests establish these guarantees.

The local transport checks current profile generation before sending, but this
cannot recall an already transmitted credential or undo a provider side effect.
External request/DB commit atomicity is not promised.

## Local verification

`TestConnectionProfileBindingIsImmutable` checks binding rejection and independent
rotation. `TestOAuthCallbacksKeepProviderProfilesIsolated` and
`TestOAuthCallbackRejectsExpiredProfileStateBeforeExchange` check state/profile
binding and expiry before external exchange. `TestSelectedConnectionReachesOnlyProvider`
uses real standalone Rhiza, the production resolver and AMP, and a certificate-
verified TLS provider fixture to check two explicit profiles and rejection of a
different workspace before sending. Its test transport only substitutes the
fixed GitHub origin with that fixture; it does not contact GitHub.

These tests require neither another app nor an IdP. Live SaaS, optional real IdP,
three-node HA and full profile lifecycle verification are separate gates.

## Beesuh execution wire pilot (2026-09-07 KST)

Beesuh `main@8606a43` plus its local adapter/runtime changes ran
`TestConductorLiveE2E` against a real Conductor process built from
`main@909b200` plus the in-progress local changes. Both ran locally on macOS
with Go 1.27 and standalone Rhiza. The service token was supplied through a
private file and not printed. No IdP, user delegation or SaaS credential was used.

The Beesuh native mock-model/tool emitted an allowlisted `payload.echo@1` request,
read the returned execution ID through `/v1/executions/{id}`, and observed
`succeeded` with the expected echo result. HTTP replay retained the execution ID;
native replay did not dispatch the model again. Beesuh separately tested rejection
of model-supplied `_compos` and `subject_ref` without external effects.

The real Conductor worker delivered one terminal callback to a local receiver.
Its handler identity, request/execution IDs, workspace and result matched.
Replaying the identical callback directly to that fixture receiver returned 204
and left one unique record. This is receiver-fixture deduplication, not a test
of the real Compos receiver or a forced Conductor transport retry. The disposable
processes were stopped. Shared Docker services were not restarted.

The executable was built during development, so this pilot verifies the unchanged
execution wire path; final revision/refresh changes are covered by the separate
local tests, not this already-built executable. Neither this pilot nor the local
profile tests establish the complete external credential-provider integration.
