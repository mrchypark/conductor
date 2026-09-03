# conductor

Go 실행 런타임이 함수 실행 경계를 담당합니다. 요청은 embedded Rhiza에 한 번 저장된 뒤
등록된 `handler@version`을 실행하고, retryable 실패를 재시도하며, terminal runtime
callback을 기록합니다. AMP connectors 카탈로그와 OAuth/PKCE 저장소도 같은 경계에서
사용할 수 있습니다.

```bash
go test ./...
go run ./cmd/conductor -db ./conductor-data
```

`Prefer: respond-async` stores a queued request; set `CONDUCTOR_WORKER_ID` to
run the local lease worker. `CONDUCTOR_CALLBACK_URL` enables durable callback
delivery (optional `CONDUCTOR_CALLBACK_TOKEN`). `CONDUCTOR_MASTER_KEY` (16/24/32
bytes) enables encrypted connector connections; `COMPOS_BASE_URL` enables the
built-in Compos connector boundary.

Rhiza HA는 각 프로세스에 고유한 `CONDUCTOR_RHIZA_NODE_ID`,
`CONDUCTOR_RHIZA_PEER_ADDR`, data directory를 주고, 모든 프로세스에 동일한
`CONDUCTOR_RHIZA_CLUSTER_ID`, `CONDUCTOR_RHIZA_MEMBERS` JSON과 shared S3 설정을
사용합니다. `CONDUCTOR_RHIZA_MEMBERS`의 각 항목은 `node_id`, `peer_url`
(`quic://host:port`), `token`을 포함합니다. S3 설정은 Rhiza의
`ObjStore*` 구성에 대응하며 multi-node 기동에는 명시적인 S3-compatible store가
필수입니다.

External workers use the same lease protocol through
`POST /v1/worker/tasks:poll`, `POST /v1/worker/tasks:heartbeat`,
`POST /v1/worker/tasks:complete`, and `POST /v1/worker/tasks:fail` (the shorter
`/v1/worker/{poll,heartbeat,complete,fail}` aliases are also accepted).

OAuth endpoints are `POST /v1/oauth/{provider}/begin` and
`POST` or `GET /v1/oauth/callback`; callback responses never return provider tokens.
Provider redirects may use `GET`/`POST /v1/oauth/{provider}/callback` and resolve the
attempt from the returned state. Non-OAuth credentials are stored encrypted through
service-authenticated `POST /v1/connections` (API key, Basic, or custom metadata).
Already-installed OAuth apps may register an access token through the same encrypted
boundary with `oauth_access_token`; responses never return the token.
실행 취소는 `POST /v1/executions/{execution_id}/cancel`입니다.
runtime event 조회는 `GET /v1/executions/{execution_id}/events`입니다.
Compos durable-engine outbox는 `POST /v1/events`의 `{"events":[...]}` envelope를
사용할 수 있습니다. 각 event의 `data._compos`에는 `workspace_id`와
`execution_request_id`가 있어야 하며, 해당 event는 연결된 실행 요청으로만 변환됩니다.

동적 함수는 Kavun 소스로 `POST /v1/handlers`에 등록할 수 있습니다. 요청은
`{"manifest":{"handler_key":"...","version":"..."},"source":"output = input"}`
형식이며 `CONDUCTOR_SERVICE_TOKEN`이 설정된 서비스 인증 요청만 허용됩니다. 소스는
`input`을 읽고 `output`을 설정하는 버전 핸들러로 컴파일되고, 파일 import와 기본 모듈은
비활성화됩니다(허용 모듈: `base64`, `hex`, `json`, `math`, `text`). 정의와 소스는
Rhiza `dynamic_handlers`에 원본과 SHA-256을 저장하고, 컴파일 결과는
`dynamic_handler_bytecode`에 별도 캐시로 저장합니다. 재시작 시 캐시의 소스 해시,
바이트코드 해시, Kavun 런타임 ABI가 맞으면 즉시 복원하고, 아니면 원본을 다시
컴파일해 캐시를 갱신합니다.

컴파일된 함수는 `kind: "wasm"`으로도 등록할 수 있습니다. WASM은 허용된 OCI
registry의 `sha256` digest로 고정하고, Rhiza에는 artifact identity와 manifest만
저장합니다. 실행은 무권한 JSON ABI와 16 MiB 메모리 상한 안에서 wazero의 새 guest
instance로 수행됩니다. 빌드·push·등록 형식과 장애/보안 경계는
[WASM function deployment](docs/spec/2026-09-04-wasm-function-deployment.md)를 참고하세요.

현재 이 전환 범위는 Compos가 위임한 함수/커넥터 실행입니다. workflow graph, task
completion, submission 같은 업무 상태는 Compos가 계속 소유합니다.

설계 경계는 [Compos–Conductor 전체 설계 개요](docs/spec/2026-08-29-compos-conductor-system-overview.md),
상세 계약은 [Conductor 서비스 상세 설계](docs/spec/2026-08-29-conductor-service-design.md)를 참고하세요.

## Go checks

```bash
go test ./...
go vet ./...
```

실제 3프로세스 HA E2E는 미리 생성된 S3 bucket을 대상으로 실행합니다.

```bash
CONDUCTOR_HA_E2E_S3_ENDPOINT=127.0.0.1:9000 \
CONDUCTOR_HA_E2E_S3_BUCKET=conductor-ha-e2e \
CONDUCTOR_HA_E2E_S3_ACCESS_KEY=minioadmin \
CONDUCTOR_HA_E2E_S3_SECRET_KEY=minioadmin \
go test -run '^TestThreeProcessRhizaHA$' -count=1 ./internal/storage
```

## Development

```bash
go test ./...
go vet ./...
```
