# Compos–Conductor 전체 설계 개요

## 문서 상태

- 상태: 현재 목표 아키텍처
- 적용 대상: 별도 서버로 배포되는 `Compos`와 `Conductor`
- 같은 문서의 사본을 두 저장소에 둔다.
- 이 문서는 기존의 “Conductor가 전체 업무 workflow를 소유한다”는 가정보다 우선한다.

## 한 문장 결정

`Compos`는 사람·에이전트·코드 Task를 아우르는 전체 업무 workflow를 소유하고, `Conductor`는 그중 실행 준비가 끝난 코드 Task 한 건을 내구성 있게 실행한다. 커넥터 호출은 Conductor가 지원하는 코드 실행 방식 중 하나다.

## 배포 구조

```text
┌────────────────────────────────────┐
│ Compos 서버                        │
│                                    │
│ 업무대장과 레코드                   │
│ Workflow definition/version/run    │
│ 사람·에이전트·코드 Task graph       │
│ Task readiness·assignment·completion│
│ 업무 처리 UI·rule·automation binding│
└─────────────────┬──────────────────┘
                  │ ready code Task
                  ▼
┌────────────────────────────────────┐
│ Conductor 서버                     │
│                                    │
│ execution request                  │
│ attempt·lease·heartbeat·retry      │
│ handler registry                   │
│ connection/OAuth                   │
│ connector execution               │
└───────────────┬───────────┬────────┘
                │           │
        local code       connector
        handler          operation
                            │
                    ┌───────┼────────┐
                    ▼       ▼        ▼
                  Compos  Gmail    Slack …
                    │
                    └── HTTPS ─────► Compos API
```

두 서버는 데이터베이스를 공유하지 않는다. API, 이벤트, 안정적인 식별자만 공유한다.

## 핵심 개념과 원본

| 개념 | 의미 | 원본 소유자 |
| --- | --- | --- |
| Object/업무대장 | 사용자 정의 데이터 구조와 레코드 | Compos |
| Workflow definition | 전체 업무 처리의 Task graph | Compos |
| Workflow instance | 실제 진행 중인 업무 처리 한 건 | Compos |
| Task | 사람·에이전트·코드가 책임지는 수행 단위 | Compos |
| Execution request | 준비된 코드 Task의 실행 요청 | Compos |
| Execution/attempt | 코드 Task의 내구성 있는 실행 상태 | Conductor |
| Handler | 서버 내부 코드 실행 단위 | Conductor |
| Connector | 내부·외부 시스템 호출 handler의 adapter | Conductor |
| Connection | OAuth, API key, polling cursor, webhook 구독 | Conductor |
| Submission/completion | 업무 Task의 결과와 완료 진실 | Compos |

## Go 비유

```text
Compos Object       = struct와 저장된 값
Compos operation    = 허용된 method signature와 도메인 규칙
Compos Workflow     = 사람·에이전트·코드 호출을 조합한 업무 method
Compos Task         = method를 구성하는 책임 단위
Conductor execution = 준비된 코드 Task의 method body 실행
Connector           = 실행 코드가 다른 시스템을 호출하는 adapter
Automation binding  = event/schedule을 Compos Workflow에 연결하는 등록
```

Workflow가 Compos에 있어야 사람의 대기, assignment, agent feedback, submission, 업무 상태와 코드 실행을 한 그래프에서 표현할 수 있다. Conductor가 이를 소유하면 업무 의미와 실행 메커니즘이 섞이고 두 서버가 모두 workflow control plane이 된다.

## Task 종류

Compos workflow node는 최소한 다음 executor kind를 가진다.

```text
human
agent
code
```

- `human`: Compos가 assignment, interactive surface, submission과 완료를 관리한다.
- `agent`: Compos가 지침, context, feedback과 결과 계약을 관리한다. bounded agent 실행은 code Task처럼 Conductor에 위임할 수 있다.
- `code`: Compos가 readiness와 업무 결과 계약을 관리하고 실제 실행은 Conductor에 요청한다.

Conductor는 사람 Task가 끝나기를 기다리거나 다음 업무 Task를 선택하지 않는다. callback을 받은 Compos가 workflow를 다시 평가하고 다음 Task를 ready로 만든다.

## 코드 Task 실행 방식

Conductor는 코드 Task를 실행하는 공통 handler 계약을 제공한다.

```text
local handler
  - 같은 Conductor 서버 안의 일반 Go 코드
  - 계산, 변환, 문서 생성, 검증

connector handler
  - Compos, Gmail, Slack 등 API 호출
  - connection과 provider 오류 정규화 사용

agent handler
  - agent runtime 호출과 tool execution
  - 업무 지침·feedback 원본은 Compos가 소유
```

커넥터가 없는 순수 서버 내부 코드도 정상적인 실행 대상이다. 따라서 Conductor의 중심 추상화는 `Connector`가 아니라 `Executable Handler`이며, connector는 handler의 한 종류다.

## 소유권

### Compos가 소유한다

- Object, field, relation, view와 record
- workflow draft와 immutable published version
- workflow instance와 Task graph
- 사람·에이전트·코드 Task의 readiness와 dependency
- assignment, instruction, submission과 completion truth
- rule, stage progression과 automation binding
- 어떤 code Task가 어떤 handler/operation을 호출하는지에 대한 실행 명세
- 업무 변경 outbox event와 실행 결과의 비즈니스 해석

### Conductor가 소유한다

- execution request, attempt, lease, heartbeat와 retry timer
- handler manifest, version, queue, timeout과 resource limit
- local code handler 실행
- connector SDK, operation catalog와 실행
- 외부 connection, OAuth token, 갱신, cursor와 webhook subscription
- provider 오류 정규화
- execution callback과 inspection

### 금지되는 중복

- Conductor는 Compos workflow definition이나 workflow instance를 복제하지 않는다.
- Compos는 attempt, lease, retry timer를 직접 관리하지 않는다.
- Conductor는 다음 업무 Task를 선택하거나 업무 완료를 판정하지 않는다.
- Conductor는 Compos DB를 직접 읽거나 쓰지 않는다.

## 실행 계약

Compos는 code Task가 ready가 되었을 때 완결된 execution request를 만든다.

```json
{
  "execution_request_id": "er_01",
  "workspace_id": "ws_01",
  "workflow_instance_id": "wi_01",
  "task_id": "task_01",
  "handler": "connector.invoke",
  "handler_version": "1",
  "operation": "gmail.messages.send",
  "connection_id": "conn_01",
  "actor": { "type": "user", "id": "user_01" },
  "input": {},
  "timeout_seconds": 30,
  "retry_policy": { "max_attempts": 3 },
  "idempotency_key": "execution-request:er_01"
}
```

local code는 같은 계약에서 handler만 달라진다.

```json
{
  "handler": "estimate.calculate_total",
  "handler_version": "2",
  "input": {}
}
```

Conductor는 요청된 handler를 실행하고 구조화된 결과를 callback한다. Compos는 결과를 submission, Task 완료, 다음 workflow 전이의 입력으로 해석한다. `execution.succeeded` 자체는 업무 Task 완료가 아니다.

## Compos 네이티브 커넥터

`compos` 커넥터 구현은 Conductor에 있고, Compos는 API와 권한·검증 계약을 제공한다.

```text
code Task
→ Conductor compos connector
→ Compos command/query API
→ 권한·전제조건·schema 검증
→ record mutation + outbox
```

업무대장마다 커넥터를 만들지 않는다. 하나의 built-in `compos` 커넥터가 `object_key`를 입력받는다.

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

상태 전이, 승인, 발행처럼 의미 있는 변경은 raw patch보다 `operations.invoke`를 우선한다. 실제 권한과 변경 규칙은 항상 Compos가 판정한다.

## Automation과 외부 수신

자동화 정의와 workflow binding은 Compos가 소유한다.

```text
Compos record event ─┐
schedule ────────────┼─→ Compos automation binding → workflow instance
external ingress ────┘
```

외부 polling과 webhook subscription은 credential/cursor를 가진 Conductor `connections`가 수행할 수 있다. 다만 새 원시자료를 업무상 어떻게 처리할지는 결정하지 않고 정규화된 ingress event를 Compos에 전달한다. Compos가 dedupe, rule 평가와 workflow 시작을 결정한다.

초기에는 broker 없이 HTTPS와 outbox/inbox 재시도를 사용한다. `event_id`, `correlation_id`, `causation_id`, `workflow_instance_id`, `execution_request_id`로 중복과 재진입을 제어한다.

## 전체 흐름

1. 사용자가 Compos에서 사람·에이전트·코드 Task를 포함한 workflow를 구성하고 publish한다.
2. Compos가 event, schedule, 수동 실행으로 workflow instance를 만든다.
3. Compos가 dependency를 평가해 사람·에이전트·코드 Task를 ready로 만든다.
4. 사람 Task는 Compos interactive path에서 수행한다.
5. 실행이 필요한 agent/code Task는 Compos가 execution request로 Conductor에 보낸다.
6. Conductor가 local handler 또는 connector handler를 내구성 있게 실행한다.
7. Conductor가 실행 결과를 Compos에 callback한다.
8. Compos가 결과 계약을 검증하고 Task completion과 다음 Task readiness를 결정한다.

## 첫 수직 검증

```text
Compos 견적 workflow
├─ human: 견적 검토
├─ code: estimate.calculate_total        → Conductor local handler
├─ agent: 견적 문안 생성                 → Conductor agent handler
├─ code: gmail.messages.send             → Conductor connector handler
└─ code: compos.operations.invoke        → Conductor compos connector
```

검증 항목:

- human 완료 전 code Task가 실행되지 않음
- 동일 execution request가 하나의 논리 실행만 만듦
- local handler와 connector handler의 동일한 attempt/lease 계약
- delegated actor 권한과 Compos typed operation 검증
- retry 후에도 외부 side effect와 Compos mutation이 중복되지 않음
- execution success와 Compos Task completion의 분리
- callback 후 Compos만 다음 Task를 ready로 만듦

## 상세 문서

- Compos: `docs/specs/2026-08-29-compos-service-design.md`
- Conductor: `docs/spec/2026-08-29-conductor-service-design.md`
