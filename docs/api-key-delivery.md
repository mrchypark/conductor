# API-key delivery adapter (local integration slice)

`internal/connectors/delivery.go` consumes credentials from an external credential
provider in memory and uses a single fixed AMP mapping: Gowid, raw `Authorization`, no prefix,
HTTPS `openapi.gowid.com/v2/expense-statements`, GET only. Query parameters may
come from the trusted snapshot, never execution/browser input. This uses the
existing catalog and AMP authentication client, not a second credential store.

The adapter is wired to trusted-service executions through the operator-configured
[`connector.credentials.invoke@1` handler](credential-provider-execution.md). This does not provide
public user authentication or consent management. The existing
service Bearer is not user delegation. Login, code+PKCE callback, session-bound
handoff/state verification, workspace authorization, confidential-client token
acquisition and refresh/offline lifecycle remain consumer integration work.
No service token or another client's token may replace the human token source.

## Calling contract

Construct `connectors.NewAPIKeyDelivery(issuer, binding, tokenSource,
authorityClient, providerClient)`, then call `Invoke(ctx)` for one fixed GET.
`DeliveryTokenSource` is `func(context.Context) (string, error)` and must return
the execution consumer's own current human token for the credential provider's
configured resource and required credential-use scope. The provider validates token owner/consumer
and current grant on every retrieval. The caller binds this source to the expected
user; job metadata is not authority. Source failures are sanitized.

The issuer must currently be an HTTPS **origin**. Base-path issuers such as
`https://id.example/tenant` are deliberately rejected, not silently rewritten.
The binding is copied by value from trusted server configuration/handoff review:

```json
{
  "grant_id": "opaque-grant-id",
  "provider_id": "operator-registered-provider-id",
  "connection_generation": "opaque-generation",
  "credential_version": 1,
  "connector_digest": "authenticated-review-digest",
  "header": "Authorization",
  "prefix": "",
  "url": "https://openapi.gowid.com/v2/expense-statements",
  "consent_expires_at_unix_ms": 1900000000000
}
```

Use actual reviewed values; the example does not create consent. Provider ID is
an explicit mapping, not an inferred AMP name. Generation stays opaque, version
is decoded directly as signed int64 (including values above 2^53). All ten
required delivery response fields must match, with `kind=api_key`, a nonempty key,
and future expiry. Duplicate/null/missing fields are rejected; additive fields
are tolerated. Rotation to a new version requires a new explicit trusted snapshot;
no automatic version acceptance or generation conversion is performed.

Every Invoke performs empty-body POST
`/auth/v1/connection-grants/{escaped-grant-id}/credential` with the current human
Bearer, then the fixed provider GET. No cookies or redirects, application retries,
cached-key fallback, credential persistence, error-body forwarding, or key/token
logging are added. Retrieval and provider JSON are bounded to 64 KiB and 1 MiB.
Errors contain only fixed messages/status codes. Successful provider JSON is
decoded, checked for literal key/token echoes, and normalized before return.
This check does not promise arbitrary encoded/derived-secret detection or a
proxy-style output projection. Injected clients/transports/token sources must not
log secrets. Ordinary Go memory is garbage-collected, not guaranteed zeroized.

Revocation/expiry blocks later retrieval, not a key already delivered or a request
already sent. There is no atomic credential-provider/SaaS transaction. Restarted/expired
user authority must fail closed; no persistent-token fallback is implemented.

## Verification and credential-provider fixture bridge

Self-contained certificate-verified TLS authority/provider fixtures:

```sh
go test -count=1 ./internal/connectors -run '^TestAPIKeyDelivery'
```

A compatible credential provider supplies a disposable real-authorization fixture. To run the optional
bridge, its parent process supplies these environment variables directly to a
child test process (no token in command arguments, files, shell history or logs):

- `CONDUCTOR_DELIVERY_LIVE=1`
- `CONDUCTOR_DELIVERY_ISSUER`: fixture HTTPS origin
- `CONDUCTOR_DELIVERY_CA_FILE`: public PEM CA certificate path
- `CONDUCTOR_DELIVERY_BINDING`: the JSON above, using exact reviewed fixture values
- `CONDUCTOR_DELIVERY_HUMAN_TOKEN`: disposable execution consumer's real use token
- `CONDUCTOR_DELIVERY_EXPECTED_API_KEY`: disposable synthetic key; provider
  Authorization must match exactly (never logged)
- `CONDUCTOR_DELIVERY_EXPECT_DENIED=1`: optional revoked-grant check; requires
  an Invoke error, empty result and zero provider requests

```sh
go test -count=1 ./internal/connectors -run '^TestAPIKeyDeliveryCredentialProviderFixture$'
```

Run from the Conductor repository. The bridge retrieves twice from the credential provider
but routes provider calls exclusively to a local TLS fixture; it never contacts
Gowid. The environment source is test-only, not a production user-token lifecycle.
Do not capture child environments or use production tokens. Without explicit
opt-in the bridge skips; that skip is not real credential-provider E2E evidence.

The credential provider's 2026-09-07 follow-up reports explicit support for the empty Authorization
prefix, removing the initial registration mismatch in its current worktree.
Its documented producer tests cover HTTPS standalone/restart delivery and local
TLS raw-header injection; the Conductor bridge subsequently passed as recorded below.
Existing Bearer consent is not converted: prefix remains part of the digest. Raw header
evidence is the pinned Gowid SDK `client/client.go` (`Authorization = APIKey`) and
Conductor's existing `TestGowidConnectorCallsV2API`, not a new live SaaS test.
OAuth credential delivery, BFF callback/state integration, user authority lifecycle,
public execution wiring, live SaaS and HA acceptance are not established here.

Local validation (2026-09-07): adapter tests including `-race`, `go vet ./...`,
and the final `go test -count=1 ./...` passed. An earlier full run failed the
existing Compos callback-count test; three isolated reruns and the final full run
passed without changing that code.

Real issuer bridge validation (2026-09-07): the credential provider's disposable fixture
ran the actual Conductor test before and after HTTPS standalone restart. Both
runs passed delivery with a real confidential-consumer human token and two local
provider GETs with exact API-key equality. After owner revocation, the credential provider checked
HTTP 404 and reran Conductor with the same token/binding in denied mode: error,
empty output and zero provider requests. The producer's source and success log
were inspected; the two delivery
subtests took 9.86s and 8.57s. Provider traffic remained local TLS, not live Gowid.
This completes the adapter's real-issuer gate, not application login, BFF state
consumption, queue/public execution wiring, or HA acceptance.

To reproduce, the credential provider's own fixture runner must start a disposable
HTTPS issuer, create reviewed delivery consent, and pass the generic
`CONDUCTOR_DELIVERY_*` environment variables above to Conductor's child test.
The fixture runner's setup commands belong to that provider's implementation.
