# Conductor 서비스 상세 설계

## 목적

Conductor를 전체 업무 workflow 소유자가 아니라, Compos에서 준비된 코드 Task를 안전하고 내구성 있게 실행하는 별도 서버로 정의한다.

전체 경계는 [Compos–Conductor 전체 설계 개요](./2026-08-29-compos-conductor-system-overview.md)를 따른다.

## 최상위 결정

Conductor의 중심 추상화는 workflow가 아니라 `Execution Request → Executable Handler`다.

- Compos가 사람·에이전트·코드 Task graph를 소유한다.
- Conductor는 ready agent/code Task 한 건을 요청받아 실행한다.
- connector는 handler 구현 종류 중 하나다.
- 같은 서버 내부의 일반 Go 함수도 connector와 동일한 attempt/lease/retry 계약으로 실행한다.
- Conductor는 다음 업무 Task, assignment, submission, stage를 결정하지 않는다.

## 소유하는 상태

```text
execution request
execution
attempt
lease / fencing token
heartbeat
retry timer
handler manifest
connector manifest
connection / encrypted credential
polling cursor / webhook subscription
execution callback delivery
ingress event delivery
idempotency claim
```

Conductor는 Compos workflow definition, workflow instance, business Task graph와 record를 저장하지 않는다.

## 현재 Go 패키지 경계

```text
cmd/conductor
internal/
  executions/    request, execution, attempt, terminal result
  handlers/      executable handler interface와 registry
  connectors/    connector SDK, manifests, provider adapters
  connections/   OAuth와 encrypted credential
  callbacks/     execution 결과를 Compos로 전달
  persistence/   embedded Rhiza adapter
  storage/       replicated execution state와 schema
```

현재는 Go binary 하나와 embedded Rhiza를 사용한다. 별도 connector microservice와 broker는 실제 격리·확장 요구가 생길 때 추가한다.

## Execution 계약

Compos가 보내는 최소 요청:

```go
type ExecutionRequest struct {
    ExecutionRequestID string
    WorkspaceID        string
    WorkflowKey        string
    WorkflowVersion    uint64
    WorkflowInstanceID string
    TaskID              string
    NodeKey             string

    HandlerKey     string
    HandlerVersion string
    OperationKey   string
    ConnectionID   string

    Actor      Actor
    Input      json.RawMessage
    Timeout    time.Duration
    Retry      RetryPolicy

    CorrelationID string
    CausationID   string
    IdempotencyKey string
}
```

Conductor가 workflow 식별자를 받는 이유는 관측과 감사 문맥을 보존하기 위해서다. 이를 근거로 Compos workflow graph를 조회하거나 다음 node를 선택하지 않는다.

보장:

- 동일 `execution_request_id`는 하나의 논리 execution에 결합한다.
- handler/version/operation과 retry policy를 execution 생성 시 고정한다.
- attempt마다 fencing token을 부여한다.
- timeout, retry backoff, stale completion 거부와 crash recovery를 내구성 있게 처리한다.
- terminal result를 at-least-once callback으로 전달한다.

Compos의 durable-engine outbox가 사용하는 `POST /v1/events`는 실행 전용 어댑터로
지원한다. 요청은 `{"events":[{"name","idempotency_key","data"}]}` envelope이고,
`data._compos.workspace_id`와 `data._compos.execution_request_id`가 없으면 거부한다.
event 이름은 기본 handler/operation key가 되며, 선택적으로 `_compos.handler_key`,
`_compos.handler_version`, `connection_id`, actor와 실행 정책을 제공할 수 있다.
`_compos`와 `subject_ref`는 handler input에서 제거하고 각각 실행 문맥과 reference로
보존한다. 이 경로는 workflow event를 해석하지 않고 하나의 실행 요청만 enqueue한다.

## Handler 모델

```go
type Handler interface {
    Manifest(context.Context) (HandlerManifest, error)
    Execute(context.Context, HandlerRequest) (HandlerResult, error)
}
```

Handler는 workflow DB나 attempt 상태를 직접 수정하지 않는다. 결과 또는 구조화된 오류만 반환하고 execution/taskmanager가 상태 전이를 수행한다.

### Compos FunctionBinding 대응

`FunctionDefinition`과 immutable revision의 binding은 Compos가 소유한다. Conductor는
binding을 저장하거나 해석하는 두 번째 control plane이 아니라, Compos가 binding을
해결해 만든 leaf `ExecutionRequest`만 실행한다.

| Compos binding | Conductor 실행 계약 |
| --- | --- |
| `kernel_command` | Conductor로 보내지 않고 Compos Kernel이 직접 실행 |
| `connector` | `handler=connector.invoke`, 고정된 handler version, operation, connection과 input으로 실행 |
| `native` | 등록된 local/external `handler_key@version`으로 실행 |
| `composite` | Compos가 leaf Function/Task 실행으로 전개한다. 한 프로세스 안에서 원자적으로 끝나는 구현이면 하나의 versioned native handler로 패키징 |
| `workflow` | Compos workflow orchestrator가 소유하며 Conductor는 ready leaf만 실행 |

따라서 Conductor에는 별도 `FunctionBinding` 테이블이나 `composite` graph runtime을
추가하지 않는다. connector와 native handler는 같은 attempt/lease/retry/fencing 계약을
공유하고, composite의 부분 실행·다음 단계·업무 완료 진실은 Compos에 남는다. 어느
handler도 Compos 저장소를 직접 수정하지 않으며, 상태 변경은 built-in Compos connector를
통해 Compos API와 Kernel 검증을 거친다.

### Local Handler

같은 Conductor 서버에서 실행하는 일반 Go 코드다.

예:

```text
estimate.calculate_total
document.render_pdf
payload.transform
schema.validate
archive.compress
```

Local handler에도 다음 규칙이 적용된다.

- 명시적 input/output schema
- versioned manifest
- timeout과 cancellation
- idempotency contract
- resource limit
- 구조화된 오류
- 민감 입력/output redaction

즉 “커넥터가 아닌 코드”는 예외 경로가 아니라 기본 handler다.

### Kavun 동적 Handler

런타임에 추가해야 하는 변환 함수는 Kavun 소스를 `handler_key@version`으로 컴파일해
기존 Local Handler 계약에 등록한다. 소스는 `input` 전역을 읽고 `output` 전역을
설정하며, 실행마다 전용 VM과 복제된 전역 상태를 사용한다. 파일 import와 OS/프로세스
모듈은 허용하지 않고, 호스트 sleep처럼 취소할 수 없는 blocking 모듈도 허용하지 않는다.
소스 크기·프레임·스택 상한과 context 취소를 적용한다.

등록 API는 서비스 토큰이 설정된 서버의 `POST /v1/handlers`로 제한한다. 정의와 소스는
Rhiza에 해시와 함께 저장하고, 컴파일 결과는 별도 bytecode 캐시로 저장한다. 재시작 시
캐시의 소스 해시·바이트코드 해시·runtime/compiler ABI가 맞으면 즉시 복원하고, 아니면
원본을 다시 컴파일해 캐시를 갱신한다. 메모리·CPU 격리가 필요한
비신뢰 tenant 코드는 이 경로에 직접 노출하지 않고 별도 worker sandbox로 분리한다.

바이트코드는 원본의 선택적 컴파일 캐시로 취급한다. Kavun의 `vm.Bytecode` 직렬화 API를
사용하고, 실행에 필요한 전역 바인딩은 conductor의 검증된 low-level adapter가 복원한다.
`source_sha256`와 별도로 `bytecode_sha256`, runtime/compiler ABI, 정책(허용 모듈·리소스
한도)을 포함한 ABI 문자열을 저장한다.

### Connector Handler

내부·외부 서비스 API를 호출한다.

```text
connector.invoke
  connector: compos
  operation: compos.records.get

connector.invoke
  connector: gmail
  operation: gmail.messages.send
```

Connector handler가 공통으로 처리하는 것:

- connection resolve
- credential injection
- provider request/response normalization
- pagination
- rate limit와 provider error classification
- token refresh callback
- operation별 idempotency 전달

### Agent Handler

bounded agent 실행을 담당할 수 있다.

```text
agent.run
agent.evaluate
```

Compos가 instruction version, context policy, tool allowlist와 feedback 원본을 소유한다. Conductor는 요청된 실행을 수행하고 trace/result reference를 반환한다. agent가 tool을 호출하면 같은 connector/local handler operation을 사용할 수 있다.

## Handler manifest

```json
{
  "handler_key": "estimate.calculate_total",
  "version": "2",
  "input_schema": {},
  "output_schema": {},
  "default_timeout_seconds": 10,
  "default_retry_policy": {
    "max_attempts": 1
  },
  "idempotency": "pure",
  "resource_class": "small"
}
```

Manifest 변경은 새 version으로 등록한다. 이미 생성된 execution은 live default 변경으로 재해석하지 않는다.

## Connector와 Connection

```text
connector   Gmail이라는 API capability
operation   gmail.messages.send
connection  특정 workspace/user가 승인한 Google 계정
```

Conductor가 소유하는 connection 상태:

- workspace와 owner scope
- connector/provider account identity
- encrypted token 또는 API key
- granted scopes
- refresh metadata
- polling interval/cursor
- webhook subscription/expiry
- enabled, disabled와 health

Token과 key는 application-level encryption 후 저장하고 master key는 DB 외부 secret source에서 주입한다.
OAuth provider redirect는 `GET`/`POST /v1/oauth/{provider}/callback`으로 받고 state로
저장된 시도를 조회한다. API key·Basic·Custom credential은 서비스 인증된
`POST /v1/connections`에서 같은 암호화 connection 레코드로 저장한다.

## Built-in Compos Connector

Compos connector 구현은 Conductor에 내장한다.

특성:

- marketplace에서 설치하지 않는다.
- 사용자 OAuth connection을 요구하지 않는다.
- service identity와 delegated actor를 함께 전송한다.
- 업무대장별 connector를 만들지 않고 `object_key`를 입력으로 받는다.
- 모든 read/write는 Compos API를 사용하며 DB를 공유하지 않는다.

초기 operation:

```text
compos.objects.get_schema
compos.records.get
compos.records.list
compos.records.create
compos.records.update
compos.records.delete
compos.operations.invoke
```

상태 전이, 승인, 발행 같은 변경은 `operations.invoke`를 우선한다. Conductor는 Compos 권한이나 precondition을 대신 판정하지 않는다.

요청 문맥:

```text
Conductor service identity
workspace_id
actor_type + actor_id
workflow_key + workflow_version
workflow_instance_id + task_id
execution_request_id + execution_id + attempt_id
correlation_id + causation_id
idempotency_key
```

## 오류와 재시도

공통 오류 분류:

```text
rate_limited
auth_expired
permission_denied
validation_failed
not_found
schema_changed
conflict
retryable
permanent
```

책임:

- handler/connector는 오류 사실, safe detail과 retry hint를 반환한다.
- execution engine은 attempt 수, backoff, timeout과 terminal failure를 결정한다.
- `permission_denied`, `validation_failed`, `idempotency_conflict`는 기본적으로 재시도하지 않는다.
- provider `Retry-After`는 정책 입력이지 retry owner가 아니다.
- 재시도에서도 같은 논리 side effect에는 안정적인 idempotency key를 전달한다.

## Lease와 실행 상태

최소 상태:

```text
queued
leased
running
retry_wait
succeeded
failed
cancelled
```

Poll/worker 경계가 필요할 경우:

```text
poll
heartbeat
complete
fail
```

초기 in-process handler도 같은 내부 protocol을 사용한다. 외부 worker가 필요할 때 transport만 HTTP로 확장하고 상태 모델은 바꾸지 않는다.

Conductor의 sleep은 retry timer나 실행 내부 delay에 한정한다. 사람 대기, SLA 대기, 다음 업무 단계 시각 같은 business wait는 Compos workflow가 소유한다.

## Execution callback

Terminal callback:

```json
{
  "callback_id": "cb_01",
  "execution_request_id": "er_01",
  "execution_id": "exec_01",
  "runtime_status": "succeeded",
  "attempt_count": 2,
  "result": {},
  "error_class": null,
  "occurred_at": "2026-08-29T12:00:00Z"
}
```

규칙:

- callback delivery는 재시도하고 stable `callback_id`를 사용한다.
- callback은 runtime fact이며 Compos business command가 아니다.
- Conductor는 callback 뒤 다음 Task를 생성하거나 실행하지 않는다.
- Compos가 결과 contract와 submission/completion을 평가한다.
- workflow Step에서 실행한 Compos connector command는 terminal callback과 별개의 명시적 side effect다.

## Polling과 Webhook Ingress

Credential과 provider cursor가 필요한 외부 수신은 Conductor connections가 수행할 수 있다.

```text
connection scheduler
→ connector read 또는 webhook receive
→ raw item/ref와 cursor 내구성 반영
→ normalized ingress event
→ Compos inbox 전달
→ Compos automation binding 평가
```

Conductor는 ingress event를 받아 어떤 business workflow를 시작할지 결정하지 않는다. Compos가 dedupe, filter와 workflow instance 생성을 소유한다.

첫 외부 수직 기능은 Gmail read와 기본 5분 polling이다. 새 원시자료를 먼저 내구성 있게 반영한 뒤 cursor를 전진시키고 Compos로 전달한다.

## 저장소

현재 Rhiza schema 범위:

```text
execution_requests
executions
attempts
leases
retry_timers
handler_manifests
connector_manifests
connections
polling_cursors
webhook_subscriptions
ingress_items
ingress_deliveries
callback_deliveries
idempotency_claims
```

다음은 저장하지 않는다.

```text
Compos workflow definitions/versions
Compos workflow instances
Compos Task graph
Compos records
Compos submissions
```

Execution input에는 필요한 bounded snapshot과 안정적인 reference만 저장한다. 최신 업무 데이터가 필요하면 Compos connector로 조회한다.

## 보안

- credential은 로그, callback, workflow input/output에 포함하지 않는다.
- handler manifest는 secret field와 redaction policy를 선언한다.
- connection 조회와 실행에서 workspace/owner scope를 검사한다.
- local handler도 connector와 같은 trust boundary 검증을 받는다.
- unrestricted arbitrary source code upload/execute는 허용하지 않는다. 서비스 토큰으로
  등록하는 제한된 Kavun handler만 예외로 두며, 비신뢰 tenant 실행은 별도 sandbox가
  준비되기 전까지 노출하지 않는다.

## 관측성

모든 로그와 trace가 공유할 식별자:

```text
execution_request_id
execution_id
attempt_id
handler_key + version
connector_key + operation_key
workspace_id
workflow_instance_id + task_id
correlation_id + causation_id
```

민감 payload 전체를 기본 로그에 남기지 않고 redacted summary와 별도 result reference를 사용한다.

## 범위 원칙

- 전체 workflow definition/version의 원본은 Conductor가 아니라 Compos다.
- task/sleep/branch를 포함한 업무 graph를 Conductor가 진행하지 않는다.
- Conductor는 ready code Task 한 건의 execution request를 처리한다.
- connector와 local handler가 동일한 실행 계약을 사용한다.

Conductor는 business state를 직접 변경하지 않는다. 명시적인 `compos` connector operation 요청은 허용하지만 실제 권한, 검증과 mutation은 Compos API가 수행한다.

전체 business workflow engine, connector microservice, message broker와 dynamic plugin loading은 구현하지 않는다.
