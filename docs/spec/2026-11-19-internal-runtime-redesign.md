# Conductor 내부 실행 런타임 재설계 (External Auth, Starlark + WASM & `amp-labs/connectors` Host-Proxy)

*   **Author**: Chanyub.Park
*   **Status**: Proposed
*   **Date**: 2026-11-19

--------------------------------------------------------------------------------

## Overview

Conductor는 내장 Raft 기반 분산 SQLite 엔진인 Rhiza(`v0.18.0`, [`internal/persistence/db.go`](../../internal/persistence/db.go)) 위에 구축된 내구성(Durable) 단일 액션 실행 런타임이다. 이 설계는 Conductor를 **`goauthy`**(인증·자격증명 위임 서비스), **`grant`**(정책 기반 RBAC 인가 서비스), **`exposure`**(엣지 API 게이트웨이: `goauthy`로 호출자 토큰을 검증하고 `grant`로 경로 인가를 확인한 뒤 내부망으로 워크스페이스·액터 헤더를 전달) 뒤의 내부망 전용 실행 런타임으로 재구성한다.

요청자는 `goauthy` 불투명 자격증명 핸들(`credential_ref` = `grant_id`), 실행 마감 시각(`deadline_at`), 우선순위(`priority`)를 전달해 동기 및 비동기 실행을 요청한다. 모든 `credential_ref` 기반 자격증명 위임·교환 경로(동기·비동기 모두 포함)는 [RFC 8693](https://datatracker.ietf.org/doc/html/rfc8693)(Token Exchange), [RFC 9449](https://datatracker.ietf.org/doc/html/rfc9449)(DPoP), [RFC 8707](https://datatracker.ietf.org/doc/html/rfc8707)(Resource Indicators) 표준을 채택하며, 동기는 교환된 위임 토큰을 메모리에만 유지하고 비동기는 송신자 제한 DPoP 위임 토큰만 Rhiza(WAL)에 저장한다. 런타임 함수는 **Starlark**(`go.starlark.net`)와 **WASM `conductor:function/v2`**(호스트 프록시 임포트 지원)를 함께 제공하며, 두 런타임 모두 Conductor가 소유한 `amp-labs/connectors` 및 동적 커스텀 커넥터 호스트 프록시를 통해 외부 SaaS API를 호출한다.

### References

*   기준 서비스 설계: [`docs/spec/2026-08-29-conductor-service-design.md`](./2026-08-29-conductor-service-design.md)
*   기존 WASM 배포 설계: [`docs/spec/2026-09-04-wasm-function-deployment.md`](./2026-09-04-wasm-function-deployment.md)
*   GoAuthy 위임·DPoP 계약(타 저장소 참조; `POST /oidc/token` RFC 8693 DPoP 교환 및 `/auth/v1/connection-grants/{grant_id}/*` DPoP 검증): `goauthy/docs/connection-use-grants.md` (`## Planned Extensions`)

--------------------------------------------------------------------------------

## The Problem

기존 Conductor 아키텍처(`v1`)는 다음 5가지 구조적 병목을 가진다.

1.  **인증·권한 모델 부재**: 단일 전역 정적 토큰(`CONDUCTOR_SERVICE_TOKEN`)만으로 인증하며, 모든 핸들러·실행·커넥터가 워크스페이스 격리 없이 전역 네임스페이스를 공유한다.
2.  **자체 자격증명 보관 및 원문 토큰 노출**: `internal/connections`가 암호화된 자격증명을 직접 저장하고, `connector.credentials.invoke` `v2`/`v3`(`internal/connectors/credential_provider.go`)가 `X-Conductor-Human-Authorization` 원문 인간 토큰을 요구하여 비동기 큐 실행이 불가능하다(`v1`은 로컬 `TokenFile` 경로 기반).
3.  **미구현 우선순위/TTL 스케줄링 및 인라인 2단계 쓰기**: `ClaimNext`가 FIFO(`created_at, id`)만 지원하며 `deadline_at`은 실행 시점에만 확인한다. 또한 기존 동기 실행(`SubmitAndExecute`)은 실행 레코드 삽입과 인라인 리스 획득(`AcquireInlineLease` / `AcquireLease`)을 두 번의 별도 Rhiza 쓰기로 분리하여 중간 크래시 시 고아 실행이 남거나 2배의 Raft 지연이 발생한다.
4.  **비표준 동적 스크립트(Kavun) 및 순수 연산 전용 WASM `v1`**: 커스텀 `Kavun` 바이트코드 VM이 언어 호환성과 유지보수성을 저해하고, 기존 WASM `v1`은 모든 호스트 임포트를 차단해 커넥터 호출을 할 수 없다. 또한 OCI 레지스트리 외부 푸시 이후 별도 메타데이터 등록이 필요하다.
5.  **컴파일 타임 고정 커넥터 카탈로그**: `amp-labs/connectors`(AMP, 내장 SaaS 커넥터 Go 라이브러리) 카탈로그와 내장 `"gowid"`(Gowid 법인카드 지출내역 API 커넥터) 외에 테넌트 도메인(`zendesk`, `salesforce` 등)이나 사내 API용 커스텀 커넥터(`ProviderInfo`)를 런타임에 등록할 수 없다.

--------------------------------------------------------------------------------

## Goals

1.  **내부망 전용 Fail-Closed 경계 & 스키마 수준 멀티테넌시**: `CONDUCTOR_INTERNAL_SECRET` 또는 mTLS로 게이트웨이 트래픽만 수용하고, `workspace_id` 기반 고유 제약으로 모든 워크스페이스 상태를 물리적으로 격리한다(타 워크스페이스 ID 조회·수정·취소 `404`, `X-Workspace-Id` 불일치 `403`).
2.  **표준 RFC 8693 + DPoP + RFC 8707 무비밀 위임**: `goauthy` `POST /oidc/token`을 통해 `scope=goauthy.connections.use`, `resource=CONDUCTOR_GOAUTHY_RESOURCE`, `DPoP` 바인딩된 위임 토큰만 메모리에 유지하거나 Rhiza(WAL)에 저장한다. 아웃박스 릴레이용 `POST .../events` 수집은 새 `/v1/workspaces/{workspace_id}/events` 경로(`credential_ref == ""` 전용)로 이전한다.
3.  **우선순위·마감 시각 기반 큐잉 & 원자적 인라인 리스**: `priority DESC, created_at ASC` 클레임, 미지정 시 `5m` 기본 `deadline_at`, 만료 전이(`failed`/`deadline_exceeded`), 인라인 실행의 단일 트랜잭션 리스(`CreateAndLeaseInlineExecution`), 1회성 완료 락(`terminal_claims`)을 보장한다.
4.  **Starlark + WASM `v2`(직접 업로드 → OCI Push) & 호스트 프록시**: `Kavun`을 Starlark로 대체하고, `multipart/form-data` WASM 업로드 검증·OCI 푸시와 비재진입 `conductor:function/v2` 호스트 프록시 ABI를 제공한다.
5.  **런타임 커스텀 커넥터 + SSRF·리다이렉트 방어**: `POST /v1/workspaces/{workspace_id}/connectors`로 동적 `ProviderInfo`(AMP 템플릿 변수 또는 커스텀 `raw` HTTPS 엔드포인트)를 등록하고, 허용 호스트 목록(`CONDUCTOR_ALLOWED_CUSTOM_CONNECTOR_HOSTS`)과 SSRF/리다이렉트/응답 크기(`240 KiB`) 차단을 적용한다.

--------------------------------------------------------------------------------

## Non-Goals

*   **사용자/역할 CRUD 또는 OAuth 인가 서버**: 사용자·역할·RBAC는 `goauthy`와 `grant`가 소유한다.
*   **워크플로 오케스트레이션(DAG/상태 머신)**: 단일 액션 실행기만 유지한다.
*   **임의 아웃바운드 HTTP/TCP 프록시**: 게스트 코드는 등록된 커넥터(내장 AMP/`gowid` 또는 허용된 커스텀 커넥터)만 호출할 수 있다.
*   **게스트 내 WASM 컴파일/패키지 다운로드**: `.wasm` 바이너리는 외부에서 컴파일해 업로드한다.
*   **레거시 자격증명·스크립트·구 경로 유지**: `/v1/executions*`, `/v1/events`, `/v1/handlers*`, `/v1/worker/{poll,...}`, `internal/connections`, `connector.credentials.invoke`(`v1/v2/v3`), `CONDUCTOR_SERVICE_TOKEN`, `CONDUCTOR_MASTER_KEY`, `Kavun`은 최종 통합 단계(Phase 4)에서 일괄 제거한다(구현 단계는 Phase 0~4 참조).

--------------------------------------------------------------------------------

## Critical User Journeys (CUJs)

*   **CUJ 1 — 동기 Starlark + 외부 커넥터 실행**:
    *   `exposure`가 `goauthy`·`grant` 인가 후 `X-Workspace-Id`, `X-Actor-Type`, `X-Actor-Id`, `Authorization: Bearer <subject_token>`과 함께 `POST /v1/workspaces/{workspace_id}/executions`를 호출한다.
    *   Conductor는 `goauthy` `POST /oidc/token`(RFC 8693 + DPoP)으로 위임 토큰(`DelegationToken`)을 메모리로만 교환하고 원문 `<subject_token>`은 즉시 폐기한다.
    *   `CreateAndLeaseInlineExecution`으로 단일 트랜잭션 리스를 선점하고 Starlark 핸들러를 실행한다. 게스트가 `connector.call(...)`를 호출하면 호스트가 `goauthy` `POST .../credential`(DPoP)로 액세스 토큰/API 키를 받아 외부 SaaS를 호출하고 결과를 게스트에 반환한다.
*   **CUJ 2 — 비동기 WASM `v2` 업로드·실행 및 콜백**:
    *   개발자가 `POST /v1/workspaces/{workspace_id}/handlers/wasm`(`multipart/form-data`)로 `.wasm`을 업로드하면 Conductor가 `ValidateAndCompile` 후 내부 OCI 레지스트리에 푸시하고 다이제스트 고정 참조를 Rhiza에 저장한다.
    *   요청자가 `Prefer: respond-async`와 `credential_ref`, `priority`, `deadline_at`으로 실행을 제출하면 Conductor는 RFC 8693 + DPoP 위임 토큰만 Rhiza(WAL)에 저장하고 `202 Accepted`를 반환한다.
    *   내부 워커가 `priority DESC, created_at ASC` 순으로 작업을 클레임해 WASM 게스트를 실행하며, 게스트가 `conductor.connector_call`을 호출할 때마다 호스트 프록시가 `goauthy` `/credential`(DPoP `ath` + `jti` 검증)을 호출해 외부 API를 실행한 뒤, 터미널 완료 시 콜백을 전달한다(요청별 `callback_url` 우선, 미지정 시 `CONDUCTOR_CALLBACK_URL`; `CONDUCTOR_CALLBACK_TOKEN`은 `CONDUCTOR_CALLBACK_URL`에만 부착; 지수 백오프 재시도).
*   **CUJ 3 — 타임아웃·선점 방지·멱등성 재시도**:
    *   명시적 `deadline_at`이 없는 요청은 `now + 5m` TTL을 자동 부여받는다. 만료된 대기 작업은 스위퍼가 `failed`(`deadline_exceeded`)으로 종결한다.
    *   네트워크 재시도로 동일 `execution_request_id` 또는 `idempotency_key`가 재전송되면, Conductor는 `/oidc/token`을 재호출하지 않고 `execution_requests`의 저장된 응답/상태를 즉시 반환한다(`credential_ref`/`priority`/`deadline_at` 변경 시 `409 Conflict`).

--------------------------------------------------------------------------------

## Technical Plan (Architecture)

### 1. System Layout & Trust Boundaries

```text
[Client / Agent] ──▶ [exposure Edge Gateway] ◀──(AuthN/AuthZ)──▶ [goauthy] / [grant]
                           │
                           │ (Internal Token/mTLS + X-Workspace-Id, X-Actor-*, Bearer <subject_token>)
                           └─────────────────────────────────────────┐
[External Workers] ──(Bearer <CONDUCTOR_WORKER_TOKEN>)───────────────┴──▶ [Conductor HTTP API]
                                                                               │          │
                                                   (POST /oidc/token,          │          │ (Submit / Query / Admin CRUD)
                                                    subject_token ->           ▼          ▼
                                                    DelegationToken)      [goauthy]   [Rhiza (conductor-rhiza-v2)] ──▶ [Callback Delivery Loop]
                                                                                          ▲                                     │
                                                                                          │ (Claim / Read State)                ▼
                                                                                          ▼                             [Callback Target]
                                                              [Conductor Runtime (Sync + Internal Worker Engine)]
                                                                     │                    │                   │
                                                       (Digest Pull) ▼                    ▼ (Host Proxy)      ▼ ("business", Trusted ACL)
                                                         [Internal OCI Registry]  [Host Connector Proxy]   [BusinessClient]
                                                                                    │       │                 │
                                                           (POST .../credential     ▼       ▼ (SSRF /         ▼
                                                            & .../refresh)     [goauthy]   [amp-labs/gowid/  [BUSINESS_SERVICE_BASE_URL]
                                                                                            Custom Info]
                                                                                                 │
                                                                                                 ▼
                                                                                         [External APIs]
```

*   **주요 개념 및 내장 컴포넌트**:
    *   `grant` / `exposure`: `grant`는 경로·액터 기반 RBAC 인가 엔진이고, `exposure`는 `goauthy` 토큰 검증과 `grant` 인가 결과를 바탕으로 `X-Workspace-Id`, `X-Actor-Type`, `X-Actor-Id`를 주입해 전달한다(`exposure`는 외부 요청의 `X-Conductor-Service-Caller` 헤더를 엣지에서 항상 제거한다).
    *   **External Workers**: 미등록 외부 핸들러(`external_claimable = 1`, `credential_ref == ""` 전용)를 `/v1/worker/tasks:poll`로 폴링해 실행하는 신뢰된 멀티테넌트 워커 프로세스다(`CONDUCTOR_WORKER_TOKEN` 보유, 응답에 `workspace_id`·`execution_request_id` 포함).
    *   `submission_claims` / `terminal_claims`: `submission_claims(workspace_id, execution_request_id)`는 제출 멱등성 원자 잠금 테이블이고, `terminal_claims(execution_id)`는 실행 단위 1회성 완료 보장을 위한 원자 잠금 테이블이다.
    *   `payload.echo@1`: 입력 JSON을 그대로 출력으로 되돌려 헬스체크·스모크 테스트에 사용하는 시스템 내장(`workspace_id = ""`) 순수 핸들러.
    *   `connector.invoke@2`: 워크스페이스 격리 하에서 `provider`, `operation_key`로 커넥터(`amp-labs/connectors`, 내장 `"gowid"`, 등록된 `custom_connectors`)를 호출하는 시스템 내장(`workspace_id = ""`) 핸들러.
        *   **`"business"` 보호**: `"business"`는 사내 비즈니스 백엔드(`BUSINESS_SERVICE_BASE_URL` + `BUSINESS_SERVICE_TOKEN` 사용, SSRF/`240 KiB` 제한 제외)용 내장 커넥터다. 일반 워크스페이스 호출자의 `connector.invoke@2`에서는 차단(`forbidden_provider`)하며, 전용 `CONDUCTOR_INTERNAL_WORKFLOW_SECRET`(`X-Conductor-Service-Caller-Token` 상수시간 검증) 또는 전용 mTLS SAN(`CONDUCTOR_INTERNAL_WORKFLOW_SAN`)으로 인증된 `X-Conductor-Service-Caller: internal-workflow`에만 허용한다. Starlark/WASM 호스트 프록시에서는 항상 차단한다.
    *   `expected_connector_digest`: GoAuthy가 `api_key` 커넥터에 대해 canonical JSON(`version`, `purpose`, `id`, `header`, `prefix`, `operations`)으로 발급(계산)하는 **43자 unpadded `base64.RawURLEncoding` SHA-256 다이제스트**다. Conductor는 `"api_key"` 커스텀 커넥터 등록 시 이를 필수로 요구하고(`"oauth2"` 커스텀 커넥터에서는 `expected_connector_digest` 및 `api_key_opts` 등록을 금지), 호출 시 GoAuthy `/credential` 응답(`DeliveredCredential`)의 `connector_digest`, `header`(대소문자 무시), `prefix`(`"" | "Bearer " | "Token "`, 공백 보존 정확 일치)와 대조한다.

### 2. Core Flows & Component Contracts

#### A. 내부망 인증 경계, 경로 격리, 메타데이터 조회 및 레거시 처분

*   **게이트웨이 및 워커 인증 (Fail-Closed)**:
    *   외부 워커 전용 엔드포인트(`POST /v1/worker/tasks:poll`, `:heartbeat`, `:complete`, `:fail`)는 `CONDUCTOR_WORKER_TOKEN` 정적 상수시간 Bearer 비교로만 인증한다(미설정 `403`, 불일치 `401`).
    *   나머지 모든 `/v1/*` 비즈니스 엔드포인트는 `CONDUCTOR_INTERNAL_SECRET`(`X-Conductor-Internal-Token` 상수시간 비교) 또는 신뢰된 클라이언트 mTLS 인증서(`CONDUCTOR_INTERNAL_CLIENT_CA_FILE`) 중 하나 이상이 설정되어야 기동하며, 내부 인증(`401`/`403`)을 먼저 수행한 뒤 워크스페이스/액터 헤더 검증(`400`/`403`)을 실행한다.
    *   모든 오류 응답은 통일된 JSON 포맷 `{"error": "...", "error_class": "..."}`를 사용한다.
*   **식별자·헤더·경로 검증 및 워크스페이스 격리**:
    *   경로의 `{workspace_id}`가 `"_legacy_v1"`(구 `v1` 격리 예약 식별자)이면 즉시 `403` `forbidden_workspace`를 반환하고, 그 외에는 `^[a-zA-Z0-9][a-zA-Z0-9_-]{0,63}$`로 검증한다(`400`).
    *   `X-Workspace-Id` 헤더는 필수이며 경로 `{workspace_id}`와 일치해야 한다(누락 `400`, 불일치 `403` `workspace_mismatch`).
    *   `X-Actor-Type`, `X-Actor-Id` 헤더는 모든 워크스페이스 엔드포인트에서 필수다(누락 시 `400` `invalid_actor`). 단, 전용 `CONDUCTOR_OUTBOX_RELAY_SECRET`(`X-Conductor-Service-Caller-Token`) 또는 전용 mTLS SAN(`CONDUCTOR_OUTBOX_RELAY_SAN`)으로 인증된 아웃박스 릴레이(`X-Conductor-Service-Caller: outbox-relay`)가 `POST .../events`를 호출할 때는 이벤트별 `data._execution.actor`를 허용한다.
*   **HTTP API 표면** (모든 워크스페이스 경로는 내부 인증 `401`/`403` → 헤더/경로 검증 `400`/`403` 순으로 선행 검증한다; 표 내 `{ws}`는 `{workspace_id}` 약칭):

| Method & Path | Purpose & Payload Contract | Auth / Status Codes |
| --- | --- | --- |
| `POST /v1/workspaces/{ws}/executions` | 단건 실행(`handler_key`, `handler_version`, `execution_request_id`, `idempotency_key` [선택], `input` ≤ `64 KiB`, `subject_ref` ≤ `16 KiB`, `input_ref` ≤ `16 KiB`, `workflow_key`/`workflow_version`/`workflow_instance_id`/`task_id`/`node_key`/`correlation_id`/`causation_id`, `credential_ref`, `priority` `[-100,100]`, `deadline_at`, `timeout_seconds` [시도당 타임아웃; 동기 ≤ `105s`, 비동기 ≤ `86400s`], `callback_url`). `Prefer: respond-async` 지원. | `200`/`202`; `400`, `401`, `403`, `404`, `409`, `503` |
| `POST /v1/workspaces/{ws}/events` | 비동기 아웃박스 릴레이 이벤트 배치(`events[]` ≤ `25`건, 합계 ≤ `64 KiB`; `credential_ref == ""` 전용 — 아웃박스 DB에 원문 `subject_token` 보관 금지). | `202 Accepted`; `400`, `401`, `403`, `404`, `409`, `503` |
| `GET /v1/workspaces/{ws}/executions/{id}` | 실행 상태 조회 (`executions.workspace_id = ws` 조건; 타 워크스페이스 `404`). | `200`; `400`, `401`, `403`, `404` |
| `POST /v1/workspaces/{ws}/executions/{id}/cancel` | 실행 취소 (`executions.workspace_id = ws` 조건). | `200`; `400`, `401`, `403`, `404`, `409` |
| `GET /v1/workspaces/{ws}/executions/{id}/events` | 실행 감사/상태 이벤트 조회 (`executions.workspace_id = ws`, `?limit=<=100&offset=`, 기본 `100`, JSON 배열 응답). | `200`; `400`, `401`, `403`, `404` |
| `GET /v1/workspaces/{ws}/handlers` | 내장(`kind: "builtin"`, `runtime_abi: ""`) + 워크스페이스 Starlark(`"starlark"`, `"starlark/v1"`)/WASM(`"wasm"`, `"conductor:function/v1|v2"`) 핸들러 목록 (`?limit=<=100&offset=`, 기본 `50`, JSON 배열 `{kind, manifest, runtime_abi, created_at}` 응답). | `200`; `400`, `401`, `403` |
| `POST /v1/workspaces/{ws}/handlers` | Starlark(`manifest`, `source`, `max_steps`, `allowed_modules`, `allow_connector`) 또는 WASM OCI 참조(`"kind": "wasm"`, `manifest`, `abi`, `artifact` [다이제스트 고정 OCI 참조]) 등록. `"kind": "wasm"`도 등록 시 OCI 풀 + `ValidateAndCompile` 검증 수행. 응답 `{kind, manifest, runtime_abi, artifact, created_at}`. | `201 Created`; `400`, `401`, `403`, `409` (`handler_exists` / `limit_exceeded` / `reserved_handler_key`), `502` |
| `POST /v1/workspaces/{ws}/handlers/wasm` | `multipart/form-data` (`manifest`, `abi`, `wasm` ≤ `4 MiB`; 전체 ≤ `5 MiB`) 업로드·검증·OCI 푸시·등록. 응답 `{kind: "wasm", manifest, runtime_abi, artifact, created_at}`. | `201 Created`; `400`, `401`, `403`, `409`, `502` (OCI 오류), `503` (`wasm_push_not_configured`) |
| `DELETE /v1/workspaces/{ws}/handlers/{key}/{version}` | 핸들러 소프트 삭제 (`deleted = 1` 툼스톤, 내장 핸들러 삭제 불가). | `204 No Content`; `400`, `401`, `403`, `404`, `409` |
| `GET /v1/workspaces/{ws}/connectors` | 워크스페이스 커스텀 커넥터 목록 (`?limit=<=100&offset=`, 기본 `50`, JSON 배열 `CustomConnectorRecord` 응답). | `200`; `400`, `401`, `403` |
| `POST /v1/workspaces/{ws}/connectors` | 커스텀 `ProviderInfo` 등록 (`provider_key`, `display_name`, `base_url`, `auth_type`, `expected_connector_digest`, `api_key_opts: {"header", "prefix"}`, `goauthy_provider_id`). 응답 `CustomConnectorRecord`. | `201 Created`; `400`, `401`, `403` (`forbidden_host`), `409` |
| `DELETE /v1/workspaces/{ws}/connectors/{provider_key}` | 커스텀 커넥터 삭제 (`deleted = 1`). | `204 No Content`; `400`, `401`, `403`, `404` |
| `GET /v1/connectors` & `GET /v1/connectors/{name}` | 글로벌 카탈로그 메타데이터 조회 (인증된 호출자용 읽기 전용). | `200`; `401`, `403`, `404` |
| `POST /v1/worker/tasks:poll`, `:heartbeat`, `:complete`, `:fail` | 외부 워커 폴링(`external_claimable = 1` 전용), 리스 연장(`lease_id`, `fencing_token`), 완료(`output` ≤ `64 KiB`), 실패(`error_class`, `error_message`, `retryable`). | `CONDUCTOR_WORKER_TOKEN`; `200`, `401`, `403`, `404`, `409` |

*   **`Manifest` 공통 스키마 (`manifest_json` ≤ `32 KiB`)**:
    *   기존 `handlers.Manifest` JSON 태그(`"handler_key"`, `"version"`, `"idempotency"` [`"pure"` | `"idempotent"` | `"side_effecting"`], `"default_timeout_seconds"`)를 유지하며 `"description"` 및 `"max_input_bytes"`(≤ `64 KiB`)를 추가한다. `timeout_seconds` 미지정 시 `default_timeout_seconds`(기본 `30s`)를 사용하며 전체 실행 수명은 `deadline_at`이 제한한다. 예약 키 `"payload.echo"` 및 `"connector.invoke"`는 모든 버전에서 워크스페이스 등록 `409 reserved_handler_key`로 차단한다.
*   **`POST /v1/workspaces/{workspace_id}/events` 아웃박스 배치 규칙**:
    *   업스트림 아웃박스 릴레이(`outbox-relay`)가 전달하는 `credential_ref == ""` 배치(`len(events) <= 25`, 페이로드 합계 `<= 64 KiB` 및 직렬화된 `rhiza.ExecuteRequest` 바이트 상한 `< 200,000 bytes` 사전 검사)를 처리한다(`credential_ref != ""` 포함 시 `400 validation_failed` 거부). 각 이벤트는 `id`, `name`(`conductor.execution.requested`, 그 외 이름은 `400`), `data._execution` 객체(`handler_key`, `handler_version`, `execution_request_id`, `idempotency_key`, `priority`, `deadline_at`, `timeout_seconds`, `callback_url`, `actor`, `subject_ref`, `input_ref`, `input`)를 포함한다.
    *   DB 커밋 이전에 전체 배치의 직렬화 크기·핸들러 존재 여부·`requested_deadline_at > now`·`idempotency_key` 충돌을 사전 검증(`PrecheckBatch`)한 뒤 단일 `Mutate`로 원자 커밋한다(동일 `execution_request_id` 재시도 배치는 멱등하게 기존 상태를 반환하며, 동시 `idempotency_key` `UNIQUE` 경합 시 `GetExecutionRequestByKey` 재조회 후 동일 요청이면 기존 상태 반환, 상이하면 `409 idempotency_conflict` 반환).
*   **직접 Rhiza 조회 및 컴파일 캐시**:
    *   워크스페이스 핸들러/커넥터는 `ResolveScoped(ctx, workspaceID, key, version)`로 Rhiza를 선형화 조회한다.
    *   WASM 컴파일 캐시는 표준 `wazero.NewCompilationCache()`를 사용하고, Starlark 프로그램은 `(handler_key, version, source_hash)` 키 기반 크기 제한 맵(bounded map, 최대 256개)으로 캐시한다.
*   **기존 기능 처분**:
    *   Phase 4에서 `internal/connections`, `connector.credentials.invoke`(`v1/v2/v3`), `Kavun`, `/v1/executions*`, `/v1/events`, `/v1/handlers*`, `/v1/worker/{poll,...}`, `/v1/connections`, `/v1/oauth/*`, `CONDUCTOR_SERVICE_TOKEN`, `CONDUCTOR_MASTER_KEY`를 완전 삭제한다.

#### B. GoAuthy RFC 8693 + DPoP + RFC 8707 위임·갱신 파이프라인

*   **GoAuthy 클라이언트 설정**:
    *   `CONDUCTOR_GOAUTHY_BASE_URL`, `CONDUCTOR_GOAUTHY_CLIENT_ID`, `CONDUCTOR_GOAUTHY_CLIENT_SECRET`, `CONDUCTOR_GOAUTHY_RESOURCE`(`GOAUTHY_CONNECTIONS_RESOURCE`와 일치), `CONDUCTOR_GOAUTHY_DPOP_KEY_FILE`(ES256 JWK 단일 키 또는 `{"current": <jwk>, "previous": <jwk>}` 2-키 롤오버 세트).
    *   `CONDUCTOR_GOAUTHY_ISSUER`는 `https://`여야 하며 GoAuthy 서버가 검증하는 실제 수신 오리진과 일치해야 한다(프록시 환경에서 `Host`/`X-Forwarded-Proto`가 `ISSUER`와 일치하도록 라우팅하고, `htu`는 실제 수신 오리진인 `CONDUCTOR_GOAUTHY_ISSUER` + 경로로 생성한다; 로컬 개발 시 `CONDUCTOR_ALLOW_INSECURE_CONNECTOR_HTTP=true`는 `BASE_URL` HTTP 전송에만 적용되며 커스텀 커넥터·`SafeDialContext`에는 불가).
*   **토큰 교환 및 제출 순서 (`Submit` / `SubmitAndExecute`)**:
    1.  *원문 토큰 유출 차단*: `credential_ref != ""`인데 `Authorization: Bearer <token>`이 없으면 `401 unauthorized`를 반환한다. `Bearer <token>` 존재 시 `Input`, `SubjectRef`, `InputRef` JSON 문자열 값에 `<token>`(또는 SHA-256 지문 / `Bearer <token>`)이 포함되면 `400 validation_failed`로 거부한다.
    2.  *사전 멱등성 조회 (`checkIdempotency`)*: `GetExecutionRequestByKey(workspace_id, execution_request_id)` 및 `idempotency_key` 조회. 기존 요청이 존재하면 정규 요청 해시 일치를 확인한 뒤 기록된 상태를 즉시 반환하고 `/oidc/token`을 호출하지 않는다.
    3.  *핸들러·예산·마감 검증*: 핸들러 존재 여부와 재시도 예산(`normalizeRetryBudget` — Section 2.G 참조)을 확인하고(미등록 `404 handler_not_found`, 저장소 오류 `503`), `requested_deadline_at <= now`이면 `/oidc/token` 호출 없이 `400 validation_failed`를 반환한다.
    4.  *위임 토큰 교환*: `credential_ref != ""`이면 `GoauthyClient.ExchangeToken(ctx, subjectToken)` 호출:
        *   `POST /oidc/token` (`client_secret_basic`, form: `grant_type=urn:ietf:params:oauth:grant-type:token-exchange`, `subject_token`, `subject_token_type=urn:ietf:params:oauth:token-type:access_token`, `requested_token_type=urn:ietf:params:oauth:token-type:access_token`, `scope=goauthy.connections.use`, `resource=<CONDUCTOR_GOAUTHY_RESOURCE>`), `DPoP` 증명(`htm=POST`, `htu=<CONDUCTOR_GOAUTHY_ISSUER>/oidc/token`, `jti`, `iat`; `ath` 미포함).
        *   `400 use_dpop_nonce` 수신 시 새 `jti` + `DPoP-Nonce`로 1회 즉시 재시도한다(2회째 실패 시 `goauthy_unavailable`).
        *   오류 매핑: `403 access_denied` / `400 invalid_grant` → `auth_expired`; `400 invalid_scope` / `unauthorized_client` / `invalid_target` / `invalid_dpop_proof` / `invalid_request` / `401 invalid_client` → `goauthy_misconfigured`; `503`/네트워크 오류 → `goauthy_unavailable`.
    5.  *`deadline_at` 유효값 계산 & 동시성 충돌 해소*: 요청자의 `requested_deadline_at`은 `execution_requests`에 보존하고, `effective_deadline_at = min(requested_deadline_at, delegationExpiresAt)`이 `<= now`이면 `401 auth_expired`를 반환한다. `submission_claims` / `UNIQUE` 쓰기 경합 패배 시 `checkIdempotency`를 재조회해 동일 요청이면 승리 레코드를 반환하고 상이하면 `409 idempotency_conflict`를 반환한다.
*   **호스트 프록시 자격증명 인출 및 갱신 오케스트레이션 (`CallProxy`)**:
    *   실행 중 게스트의 매 커넥터 호출(게스트 `connector.call` 기준 실행당 최대 **16회**, 초과 시 `proxy_limit_exceeded`로 즉시 중단)마다 `POST /auth/v1/connection-grants/{grant_id}/credential`을 호출한다(빈 본문, `Authorization: DPoP <delegation_token>`, `DPoP` 증명 `htm=POST`, `htu=<CONDUCTOR_GOAUTHY_ISSUER>/auth/v1/connection-grants/{grant_id}/credential`, `ath`, `jti`, `iat`; GoAuthy의 `jti` 재생 방지로 추가 nonce RTT 없음).
    *   `404 Not Found` 시: 커넥터가 `"api_key"`로 확정된 경우 즉시 `auth_expired`로 종료하고, `"oauth2"`(또는 미확정)인 경우 `GET /auth/v1/connection-grants/{grant_id}/credential-status`(`htu=<ISSUER>/.../credential-status`)를 **1회** 프로브한다(`503`→`goauthy_unavailable`, `401` `WWW-Authenticate: DPoP error="invalid_dpop_proof"`→`goauthy_misconfigured`, 그 외 `400`/`401`→`auth_expired`):
        *   `state == "refreshing"` → `credential_refreshing`(`sideEffectDispatched == false`인 경우 `retry_wait` 스케줄러로 백오프 재시도 가능, 그 외 즉시 실패).
        *   `state == "ready"` → `POST .../credential`을 **1회** 재시도하고, 재차 `404`이면 `POST /auth/v1/connection-grants/{grant_id}/refresh`(DPoP, `{"credential_version": status.Version}`)를 **1회** 호출한다. `200 OK` 시 `/credential`을 **1회** 재시도(재차 `404` 시 `auth_expired`), `409` 시 `credential_refreshing`, `503` 시 `goauthy_unavailable`, `401` `error="invalid_dpop_proof"` 시 `goauthy_misconfigured`, `404`(`allow_refresh=false` 포함)/`502`/`400`/`401`/`uncertain` 시 `auth_expired`로 종결한다.
        *   `state == "uncertain"` / `"revoked"` / `404` → `auth_expired`.
    *   그 외 `/credential` 오류 매핑: `409` → `credential_conflict`; `503`/네트워크 → `goauthy_unavailable`; `401` `WWW-Authenticate: DPoP error="invalid_dpop_proof"` → `goauthy_misconfigured`; 그 외 `401`/`400` → `auth_expired`.
    *   **만료·바인딩 검증 (`DeliveredCredential` 응답)**: `kind == "oauth2"`일 때만 `now >= min(token_expires_at_unix_ms, consent_expires_at_unix_ms)`를 검사하고(만료 시 `auth_expired`), `kind == "api_key"`는 `now >= consent_expires_at_unix_ms`만 검사한다. `DeliveredCredential`(`kind`, `provider_id`, `connector_digest`, `header`, `prefix`, `token_expires_at_unix_ms`, `consent_expires_at_unix_ms`)의 `ProviderID`(커스텀 커넥터는 `goauthy_provider_id`), `Kind` vs `AuthType`, 및 커스텀 `"api_key"` 커넥터의 `connector_digest`(43자 `base64.RawURLEncoding`), `header`, `prefix` 불일치 시 `auth_mismatch`로 즉시 중단한다. `RedactSecrets`로 비밀 노출을 차단한다(게스트에 반환되는 업스트림 SaaS 응답 본문 포함).

#### C. 큐 우선순위, 리스 선점 방지, 멱등성 및 Rhiza `< 200,000 bytes` 쓰기 예산

*   **대리 PK(`execution_requests.id`) & 멱등성**:
    *   `execution_request_id`는 요청 불변 식별자이고 `idempotency_key`는 선택적 중복 방지 키(`""` 제외 고유 인덱스)다. 동일 `idempotency_key`로 상이한 `execution_request_id` 또는 정규 요청 해시(`handler_key`, `handler_version`, `input`, `subject_ref`, `input_ref`, `timeout_seconds`, `callback_url`, `priority`, `deadline_at`, `credential_ref`) 불일치 제출 시 `409 idempotency_conflict`를 반환한다. `workspace_id != ""`인 요청의 PK는 `storage.ScopedRequestID` = `"req_" + sha256_hex(workspace_id + "\x00" + execution_request_id)[:32]`(128-bit), `storage.ScopedExecutionID` = `"exec_" + sha256_hex(workspace_id + "\x00" + execution_request_id)[:32]`를 사용한다.
    *   `UNIQUE(workspace_id, execution_request_id)`와 `submission_claims(workspace_id, execution_request_id)`로 유일성을 보장한다. 모든 조회·콜백·이벤트·클레임 쿼리는 `JOIN execution_requests r`로 원문 `r.execution_request_id`와 `r.input`을 프로젝션한다.
    *   `deadline_at` 미지정 시 `requested_deadline_at = now + 5m`, `deadline_explicit = 0`을 저장하며, `checkIdempotency`는 `DeadlineExplicit == 1`일 때 저장된 `requested_deadline_at`과 비교한다.
    *   `metadata` 문자열(`execution_request_id`, `workflow_key`, `workflow_instance_id`, `task_id`, `node_key`, `correlation_id`, `causation_id`, `idempotency_key`)은 각 `<= 256 B`, `error_message`는 `<= 1 KiB`로 제한한다.
*   **단일 트랜잭션 리스, 만료 처리 및 폴링**:
    *   동기 실행은 `CreateAndLeaseInlineExecution`으로 요청·실행·`active` 리스(`worker_id = "inline:" + workerID`, 인라인 리스 TTL `max(timeout_seconds + 30s, 60s)` 최대 `135s`, 실행 중 `15s` 백그라운드 하트비트 유지)·`execution.requested` + `execution.started` 이벤트를 단일 Rhiza `Mutate`로 생성한다.
    *   내부 비동기 워커 리스 TTL은 `60s`(백그라운드 하트비트 `15s` < `TTL/3`)로 유지한다. 실행 상태(`executions.status`)는 `queued`, `running`, `retry_wait`, `succeeded`, `failed`, `cancelled`를 사용하며(기존 `"timed_out"` 제거), 타임아웃 만료는 `status='failed', error_class='deadline_exceeded'`, 미확정 리스 만료는 `status='failed', error_class='outcome_unknown'`으로 기록한다.
    *   `ClaimNextForScope(ctx, workerID, ttl, scopeFilter, externalOnly)`는 `WHERE e.status IN ('queued','retry_wait') AND r.effective_deadline_at > ? AND (e.status='queued' OR EXISTS(SELECT 1 FROM retry_timers t WHERE t.execution_id=e.id AND t.status='scheduled' AND t.run_at<=?))` 조건으로 `priority DESC, created_at ASC, id ASC LIMIT 1`을 클레임하며, `attempt_number = COALESCE(MAX(attempts.attempt_number), 0) + 1`로 `(ExecutionRequestRecord, LeaseRecord, error)`를 반환한다(`externalOnly=true` → `external_claimable = 1`; `externalOnly=false && scopeFilter=true` → `external_claimable = 0`; `scopeFilter=false` → 필터 미적용).
    *   실행 실패 시 무조건 `ReleaseLease`를 호출하지 않으며(`finishLease`가 터미널/재시도 전이를 원자 처리), `RecoverExpiredExecutions`는 `1s ± 20%` 지터 주기로 `status IN ('queued','retry_wait') AND effective_deadline_at <= now` 작업을 `failed`(`deadline_exceeded`)으로 전이한다(`claim_id = "recover-expired:" + id`, 현재 `attempt_number` 기록, `terminal_claims`, `attempts`, `callbacks`, `execution_events`, `delegation_token = x''` 포함). `RecoverExpiredLeasesBatch`는 `inline:%` 리스·미저장 토큰 리스(`r.credential_ref <> '' AND length(r.delegation_token) = 0`)·`MaxAttempts == 1` `"side_effecting"` 리스·최종 시도 소진(`l.attempt_number >= r.max_attempts`) 리스 만료 시 `failed`(`outcome_unknown`)로 종결하고, `MaxAttempts > 1 AND l.attempt_number < r.max_attempts` 만료 리스는 `r.effective_deadline_at <= now`이면 `failed`(`deadline_exceeded`), `r.effective_deadline_at > now`이면 `status='retry_wait'` 전이와 함께 `retry_timers(execution_id, attempt_number, run_at=now, status='scheduled')` 행을 upsert한다. 활성 리스가 없는 고아 `'running'` 행은 `claim_id = "recover-orphan:" + id`로 만료 시 `deadline_exceeded` 아니면 `outcome_unknown`으로 종결한다. 터미널 종결 시 `delegation_token = x''`로 스크러빙한다.
*   **Rhiza `< 200,000 bytes` (Raft 명령 크기 상한) 단일 저장 예산**:
    *   `CreateExecution` 시 `input`(≤ `64 KiB`) + `subject_ref`(≤ `16 KiB`) + `input_ref`(≤ `16 KiB`) + `delegation_token`(≤ `8 KiB`) = 원문 최대 `104 KiB`(base64 합계 `~138.7 KiB < 200,000 bytes`)를 `execution_requests`에만 1회 저장한다(`executions.input` 중복 제거).
    *   `CompleteExecutionWithLease`(`:complete`) 시 `executions.output`(≤ `64 KiB`, base64 `~85.3 KiB`)에만 출력을 1회 저장하고, `callbacks.payload`에는 출력이 제외된 메타데이터 JSON만 저장한다(`callback_deliveries`는 `payload` 컬럼 없이 `JOIN callbacks`로 `c.payload` 조회).

#### D. 런타임 커스텀 커넥터 & SSRF 방어

*   **스키마 & 등록 (`POST /v1/workspaces/{ws}/connectors`)**:
    *   `custom_connectors` 테이블(`PRIMARY KEY (workspace_id, provider_key)`, 활성 커넥터 워크스페이스당 최대 **256개**, 초과 시 `409 limit_exceeded`)에 `provider_key`, `display_name`, `base_url`(`https://` 필수, 경로·쿼리·userinfo·IP 리터럴 금지), `auth_type`(`"oauth2"` | `"api_key"`), `goauthy_provider_id`, `expected_connector_digest`(43자 base64url; `"api_key"` 필수, `"oauth2"` 금지), `api_key_opts_json`(`header` ∈ `{"Authorization","X-API-Key"}`, `prefix` ∈ `{"","Bearer ","Token "}`; `"api_key"` 필수, `"oauth2"` 금지), `deleted`를 저장한다.
    *   `provider_key`는 글로벌 `amp-labs/connectors`, `"gowid"`, `"business"`와 충돌 시 `409 conflict`로 차단하지만, `goauthy_provider_id`는 임의의 프로바이더 식별자에 매핑할 수 있다.
    *   `base_url` 호스트가 `CONDUCTOR_ALLOWED_CUSTOM_CONNECTOR_HOSTS`(콤마 구분 정확 호스트명 또는 `*.suffix` 와일드카드, HTTPS 포트 443 전용; 미설정 시 `403 forbidden_host`)와 일치할 때만 등록한다.
*   **실행 라우팅 & SSRF 방어 (`240 KiB` 상한)**:
    *   내장 AMP 프로바이더에 테넌트 변수(`vars`, 예: `zendesk` 서브도메인)가 필요한 경우 `callInput.vars`를 `Catalog.Info(name, vars...)`에 전달해 `BaseURL`을 해석한 뒤 `CONDUCTOR_ALLOWED_CUSTOM_CONNECTOR_HOSTS`로 호스트를 검증하고, 비AMP 커스텀 커넥터는 `common.NewOAuthHTTPClient` / 기존 `rawRequest`(`internal/connectors/operations.go`)로 `"raw"`/`"http"` 호출만 허용한다(`Host` 및 hop-by-hop 헤더 차단).
    *   모든 외부 커넥터(`amp-labs/connectors`, `"gowid"`, `custom_connectors`; `goauthy`·`BusinessClient` 제외)에는 `goauthy/internal/netsec` 규약 기반 `SafeDialContext`(사설·특수 대역 차단, DNS 리바인딩 방어) + `deliveryHTTPClient` 기반 `TLS 1.2+` + `30s` 호출당 HTTP 타임아웃 + `CheckRedirect: http.ErrUseLastResponse`와 `3xx` 응답 시 `ErrSSRFBlocked`를 반환하는 래핑 `http.RoundTripper` + `limitReadCloser(240 KiB)` + `SetEscapeHTML(false)` 후 프록시 JSON `<= 255 KiB` 검사(`connector.invoke@2` 최종 출력 `<= 64 KiB`)를 적용한다.
    *   **콜백 URL SSRF 방어**: 요청별 `callback_url`은 제출 시 `https://` + `CONDUCTOR_ALLOWED_CALLBACK_HOSTS`를 동기 검증(`400`/`403`)하고 전달 시 `SafeDialContext` + `3xx` 차단 `RoundTripper`를 적용하며, 전역 `CONDUCTOR_CALLBACK_TOKEN`은 오직 `CONDUCTOR_CALLBACK_URL`에만 전송한다.

#### E. Starlark 런타임 (`go.starlark.net`) & 게스트 오류 계약

*   `POST /v1/workspaces/{ws}/handlers`(`manifest`, `source` ≤ `96 KiB` [Rhiza base64 `128 KiB` + `manifest <= 32 KiB` < `200,000 bytes`], `max_steps` ∈ `[1, 10_000_000]` 기본 `1_000_000`, `allowed_modules` ⊂ `{json, math, time, base64, hex}`, `allow_connector` bool) 등록 시 파싱·`def run(input):` 시그니처(단일 위치 인자, varargs/kwargs 금지)·내장 키(`"payload.echo"`, `"connector.invoke"`) 충돌·활성 핸들러 상한(Starlark+WASM 합산 `256`개, `409 limit_exceeded`)을 검증한다.
*   실행 시 `thread.SetMaxExecutionSteps(maxSteps)`, Go `context` 타임아웃(`thread.Cancel`), 및 Conductor `json`/`base64`/`hex` 모듈 wrapper 디코딩·직렬화 상한(`<= 1 MiB`)을 적용한다. `connector.call(req_dict)`는 단일 `starlark.Dict` 인자(직렬화 `<= 64 KiB`, `vars` 필드 선택)를 받고, `time` 모듈은 읽기 전용 UTC `now()`/`parse_time`만 노출하며(`time.sleep` 제외), `base64`/`hex` 모듈은 `encode(str)->str`/`decode(str)->str`을 제공한다.
*   **호스트-게스트 오류 계약**:
    *   *치명적 호스트 오류*(`auth_expired`, `auth_mismatch`, `proxy_limit_exceeded`, `forbidden_provider`, `goauthy_unavailable`, `goauthy_misconfigured`, `credential_refreshing`, `credential_conflict`): 게스트에 JSON을 반환하지 않고 즉시 중단한다.
    *   *도메인/업스트림 프록시 오류*(`4xx`/`5xx`/`rate_limited`/`upstream_unauthorized`, `unsupported_operation`, `ssrf_blocked`, `response_too_large`, `invalid_input`): `{"error": {"class": "...", "message": "...", "retryable": bool}}` JSON을 게스트에 반환한다. 게스트가 최상위 `{"error": ...}`를 반환하거나 미처리 예외(`state.lastProxyError` 일치)로 종료할 때 허용된 도메인 오류 클래스(`upstream_unauthorized`, `rate_limited`, `not_found`, `conflict`, `validation_failed`, `connector_error`, `handler_error`)에 한해 `*handlers.Error`로 보존한다(호스트 인프라 재시도 트리거에는 사용 불가).

#### F. WASM 직접 업로드 → OCI Push 및 비재진입 `conductor:function/v2` ABI

*   **`multipart/form-data` 업로드 (`POST /v1/workspaces/{ws}/handlers/wasm`)**:
    *   `MaxBytesReader = 5 MiB`, 타임아웃 `30s`, 파트 `manifest`(≤ `32 KiB`), `abi`(`"conductor:function/v1"` | `"conductor:function/v2"`), `wasm`(≤ `4 MiB`). `CONDUCTOR_WASM_PUSH_REGISTRY`(scheme 제외 `<host>[:port][/prefix]`) 미설정 시 `503 wasm_push_not_configured`. 기존 `CONDUCTOR_WASM_REGISTRY_{USERNAME,PASSWORD,TOKEN,PLAIN_HTTP}`와 `v1`/`v2` 미디어 타입(`application/vnd.conductor.function.v2`, `application/vnd.conductor.function.wasm.v2+wasm`)을 사용한다.
    *   충돌 검사(`409`) 및 `ValidateAndCompile`(`v1` 임포트 금지, `v2` `conductor.connector_call(i32,i32,i32,i32)->i64`만 허용, 공통 `alloc`/`invoke`/`dealloc`/`memory` ≤ 256페이지, `WithStartFunctions()`로 `_start` 자동 실행 비활성화) 통과 후에만 `oras-go/v2`로 `<workspace_id>/<handler_key>:<version>-<blob_sha256_12>` 태그로 push하고 다이제스트 고정 참조 `@<manifest_digest>`를 Rhiza에 저장한다(DB 삽입 `409` 충돌 시 최선 노력으로 태그 정리).
*   **비재진입 `conductor:function/v2` ABI**:
    *   게스트가 사전 할당한 `[req_ptr, req_ptr+req_len)`(요청 JSON `{"provider","operation_key","object_name","record_id","path","method","query","body"}`) 및 `[resp_ptr, resp_ptr+resp_cap)`를 `uint64(uint32(...))`로 확장한 뒤, 외부 HTTP 호출 **이전**에 `mod.Memory().Size()` OOB 검사(위반 시 `mod.CloseWithExitCode(ctx, 1)`, 트랩 시 `dealloc` 미호출) 및 `req_len > 64 KiB` / `resp_cap < 256 KiB` 시 `-1` 반환을 수행한다.
    *   치명적 호스트 오류 시 `mod.CloseWithExitCode(ctx, 1)`로 게스트를 즉시 중단하고, 정상/도메인 오류 시 `{"output": ...}` 또는 `{"error": ...}` UTF-8 JSON(≤ `256 KiB`)을 `resp_ptr`에 쓰고 `int64(written_len)`(`0 <= written_len <= resp_cap`)을 반환한다. 게스트 `invoke`가 허용된 도메인 `{"error": ...}`를 반환하면 `*handlers.Error`로 승격한다.

#### G. 실행 위치(`external_claimable`), `Manifest.Idempotency` 및 재시도 예산 규약

*   **실행 위치 분리**: `Starlark`, `WASM`, 내장 핸들러는 `external_claimable = 0`(내부 워커 전용), 미등록 외부 워커 핸들러는 `credential_ref == ""`일 때만 `external_claimable = 1`로 허용한다(`credential_ref != ""` 미등록 핸들러는 `404 handler_not_found`).
*   **`Manifest.Idempotency`, `MaxAttempts = 1` 및 부작용 발생 전 재시도 규약**:
    *   `allow_connector: true` Starlark 및 WASM `v2`는 기본 `"side_effecting"`(`"pure"` 금지), `"side_effecting"` 및 비-읽기 `connector.invoke@2`(읽기 판별: `operation_key` 끝 세그먼트가 `get`/`read`/`list`/`search`/`schema`이거나 `raw`/`http`에서 `method == "GET"`인 경우; `raw`/`http` 외 오퍼레이션에서 `input.Path`/`Method` 금지)는 `MaxAttempts = 1`로 고정한다(`connector.invoke@2` 내부 `Manifest.Idempotency` `"provider-defined"`는 `normalizeRetryBudget`에서 읽기/비-읽기 판별에 매핑).
    *   단, `sideEffectDispatched == false`(현재 시도에서 아직 외부 SaaS HTTP 요청이 1건도 발송되지 않은 상태)에서 발생한 `credential_refreshing`, `goauthy_unavailable`, `credential_conflict`는 외부 부작용이 없으므로 `MaxAttempts == 1` 제한을 우회해 비동기 큐 `retry_wait` 재시도를 허용한다(`attempt_number`는 증가시키되 `pre_side_effect_retries` / `retry_timers` 누적 횟수 `<= 12`회 및 첫 사전 부작용 대기 `<= 1m` 상한 내 `>= 5s` 백오프로 스케줄하며, 첫 외부 SaaS HTTP 발송 이후에는 즉시 실패 기록한다).

--------------------------------------------------------------------------------

## Alternatives Considered

| 항목 | 채택안 | 기각안 및 사유 |
| --- | --- | --- |
| A. 동적 스크립트 | `go.starlark.net` Starlark (Python 유사 제한 문법) | `Kavun` 유지(비표준 유지보수 비용), `yaegi`/`gopher-lua`(가변/비결정적 Go 런타임 노출) 기각. |
| B. 내부망 인증 | `CONDUCTOR_INTERNAL_SECRET` / mTLS + `exposure` 헤더 | `CONDUCTOR_SERVICE_TOKEN` 단일 전역 토큰 유지(워크스페이스 격리 불가) 기각. |
| C. 위임 프로토콜 | RFC 8693 + DPoP + RFC 8707 + `goauthy` `/refresh` (리소스 `jti`-only) | `X-Actor-*` 헤더 + M2M `client_credentials` 직결(Confused Deputy 위험), Conductor `POST .../refresh` 전면 금지(장기 비동기 큐 복구 불가), 리소스 호출마다 `401 use_dpop_nonce` 왕복(과도한 RTT/쓰기 부하) 기각. |
| D. WASM 등록 | `POST .../handlers/wasm` 직접 업로드 + `POST .../handlers` OCI 참조 병행 | OCI 사전 푸시만 강제 시 개발자 워크플로 단절하므로 기각. |
| E. 커넥터 격리 | `Catalog.Info` + `custom_connectors` + `SafeDialContext` | 인프로세스 커넥터가 없는 외부 워커만 허용 시 저지연 Starlark/WASM 프록시 불가하므로 기각. |
| F. `v1 -> v2` 전환 | 다운타임 컷오버 + 신규 `v2` 스토어 초기화 | 무중단 혼합 버전 운영(`v1`/`v2` 비호환), 12테이블 인플레이스 `ALTER` 마이그레이션(복잡도·크래시 위험) 기각. |

--------------------------------------------------------------------------------

## Risks & Mitigations

1.  **인프로세스 Starlark/WASM 메모리 DoS**: `SetMaxExecutionSteps`만으로 거대 문자열/디코딩 OOM을 완전히 막을 수 없음 → 입력 `64 KiB`, Starlark 소스 `96 KiB` 및 반복/디코딩 `1 MiB` 상한, WASM 동시 컴파일(`GOMAXPROCS*2`)·인스턴스(`32`) 세마포어, `256`-핸들러/커넥터 상한, 게이트웨이 레이트 리밋으로 완화하며 비신뢰 코드는 외부 워커로 격리.
2.  **자격증명 만료 및 OAuth2 토큰 탈취 위험**: 비동기 큐 대기 중 OAuth2 `access_token` 만료 → `404` 시 `credential-status` → `/credential` 1회 재시도 → `POST .../refresh`(`allow_refresh=true`) 1회 시도로 복구. 또한 `"oauth2"` 커스텀 커넥터가 `expected_connector_digest` 없이 `goauthy_provider_id`를 지정하므로 `CONDUCTOR_ALLOWED_CUSTOM_CONNECTOR_HOSTS`는 신뢰된 전용 SaaS 호스트로만 제한하고 `POST .../connectors`에 고권한 RBAC를 요구한다.
3.  **DPoP 키 로테이션 & `goauthy` Raft 쓰기 부하**: `CONDUCTOR_GOAUTHY_DPOP_KEY_FILE`은 `current`/`previous` 2-키 롤오버를 지원해 비상 키 교체 시 큐 내 기존 토큰 무효화를 방지한다. 교환 `4`쓰기 + `N`회 `/credential`(`jti` `MarkReplay` `1`쓰기) = `N+4` GoAuthy Raft 쓰기 발생 → `16`회 상한 유지.
4.  **SSRF / 리다이렉트 / 콜백 토큰 유출**: DNS 리바인딩·`302` 리다이렉트·HTML 이스케이프 팽창·임의 `callback_url` → `SafeDialContext` + `3xx` 차단 `RoundTripper` + `limitReadCloser(240 KiB)` + `SetEscapeHTML(false)` 후 `255 KiB` 검사 + `callback_url` `CONDUCTOR_CALLBACK_TOKEN` 미부착으로 방어.
5.  **다중 파드 만료 스위퍼 경합**: 동시 다중 파드의 `RecoverExpiredExecutions` 호출 경합 → `1s ± 20%` 지터 및 `LIMIT 100` 상한으로 완화.

--------------------------------------------------------------------------------

## Non-Functional Requirements

*   **보안 및 격리**: `workspace_id` 기반 스키마 격리, `execution_requests` 원문 토큰 유출 거부, `RedactSecrets`로 원문 자격증명·위임 토큰·시크릿·게스트 응답 본문 노출 차단, `CONDUCTOR_ALLOWED_CUSTOM_CONNECTOR_HOSTS`/`CONDUCTOR_ALLOWED_CALLBACK_HOSTS` Fail-Closed, `X-Conductor-Service-Caller` 엣지 스트리핑 + 역할별 분리 시크릿/mTLS 인증.
*   **성능 및 타임아웃**: HTTP 서버 `ReadHeaderTimeout 10s`, `WriteTimeout 3m`(동기 `105s` + 교환 + Raft 커밋 여유 확보), `IdleTimeout 2m`, 그레이스풀 셧다운 `15s`; 동기 실행 `timeout_seconds <= 105s`, 비동기 `<= 86400s`, 기본 `deadline_at` `5m`.
*   **확장성 및 자원 상한**: Rhiza 단일 `Mutate` `< 200,000 bytes`, `Input`/`Output` ≤ `64 KiB`, Starlark `source` ≤ `96 KiB`, `SubjectRef`/`InputRef` ≤ `16 KiB`, `DelegationToken` ≤ `8 KiB`, 메타데이터 문자열 각 `<= 256 B`, `error_message <= 1 KiB`; 워크스페이스당 활성 핸들러 `256`개(Starlark+WASM 합산)·활성 커스텀 커넥터 `256`개, 목록 조회 기본 `50`~`100` 상한; WASM 컴파일 세마포어 `runtime.GOMAXPROCS(0)*2`·동시 인스턴스 `32`개, 실행당 프록시 호출 `16`회, `/events` 배치 `25`건(`<= 64 KiB`) 상한.
*   **감사 및 관측성**: 모든 실행·프록시·생성·삭제 연산에 `log/slog` JSON 구조화 감사 로그(`event`, `execution_id`, `workspace_id`, `actor_type`, `actor_id`, `grant_id`, `error_class`)를 기록한다.

--------------------------------------------------------------------------------

## Rollout & Migration

*   **사전 추출·드레인·백업 절차 (컷오버 이전)**:
    1.  `v2` 배포 직전 `v1` 트래픽을 중지하고, 대기 중인 Kavun·`v1` WASM·`inline_pending` 실행이 모두 종료될 때까지 드레인한다.
    2.  `v1` Rhiza/SQLite 스냅샷 백업을 생성하고, `v1`은 `wasm_handlers` 내보내기 API가 없으므로 백업 DB에서 `SELECT handler_key, version, manifest_json, artifact_ref, runtime_abi FROM wasm_handlers` 쿼리로 활성 WASM 핸들러를 추출한다(구 `dynamic_handlers` Kavun 스크립트는 `v2`에서 제거되므로 필요 시 오프라인에서 Starlark `def run(input):`으로 수동 변환).
*   **단일 컷오버 & 롤백**: 혼합 버전 쓰기 비호환(`v1` 파드가 `conductor-rhiza-v2` 거부)에 따라 전 파드를 중지한 뒤 깨끗한 `conductor-rhiza-v2` 데이터 디렉터리로 일괄 컷오버한다(`v2` 바이너리는 `schema_metadata == "conductor-rhiza-v1"` 감지 시 기동을 거부). 장애 시 `v2` 전 스냅샷 복원 + `v1` 바이너리로 롤백한다. 컷오버 후 추출한 WASM 핸들러를 워크스페이스별 `POST .../handlers/wasm` 또는 `POST .../handlers`로 재등록한 뒤 트래픽을 개방한다.

--------------------------------------------------------------------------------

## Detailed Implementation Plan

### Phase 0: External Prerequisite (`goauthy` Repository)

*   대상 문서: `goauthy/docs/connection-use-grants.md` (`## Planned Extensions`).
*   `goauthy`의 `POST /oidc/token` `goauthy.connections.use` RFC 8693 + DPoP, `/auth/v1/connection-grants/{grant_id}/*` DPoP `ath`+`MarkReplay` 검증 및 HTTP 경계 직접 응답, `30s` `refresh_claimed_at_unix_ms` 리스 복구, `503` 전파가 먼저 구현·검증되어야 한다.

### Phase 1: Storage Schema (`conductor-rhiza-v2`), Workspace-Scoped Keys & Queue Mechanics

*   **Target Files**:
    *   `internal/storage/store.go` `(Modify)`
    *   `internal/storage/executions.go` `(Modify)`
    *   `internal/storage/callbacks.go` `(Modify)`
    *   `internal/storage/leases.go` `(Modify)`
    *   `internal/storage/handlers.go` `(Modify)`
    *   `internal/storage/connectors.go` `(Create)`
    *   `internal/storage/store_test.go` `(Modify)`
*   **구체 구현**:
    *   `internal/storage/store.go`: `schemaIdentity = "conductor-rhiza-v2"`(`conductor-rhiza-v1` 감지 시 오류 반환). 공통 테이블(`schema_metadata`, `attempts`, `callbacks`, `retry_timers`, `execution_events`, `terminal_claims`)은 유지하고, `v2` 신규/수정 테이블 DDL은 다음과 같다:
        ```sql
        CREATE TABLE IF NOT EXISTS execution_requests (
          id TEXT PRIMARY KEY,
          workspace_id TEXT NOT NULL,
          execution_request_id TEXT NOT NULL,
          idempotency_key TEXT NOT NULL DEFAULT '',
          handler_key TEXT NOT NULL,
          handler_version TEXT NOT NULL,
          input BLOB NOT NULL,
          subject_ref TEXT NOT NULL DEFAULT '',
          input_ref TEXT NOT NULL DEFAULT '',
          workflow_key TEXT NOT NULL DEFAULT '',
          workflow_version TEXT NOT NULL DEFAULT '',
          workflow_instance_id TEXT NOT NULL DEFAULT '',
          task_id TEXT NOT NULL DEFAULT '',
          node_key TEXT NOT NULL DEFAULT '',
          correlation_id TEXT NOT NULL DEFAULT '',
          causation_id TEXT NOT NULL DEFAULT '',
          actor_type TEXT NOT NULL,
          actor_id TEXT NOT NULL,
          credential_ref TEXT NOT NULL DEFAULT '',
          delegation_token BLOB NOT NULL DEFAULT x'',
          delegation_expires_at TEXT NOT NULL DEFAULT '',
          priority INTEGER NOT NULL DEFAULT 0,
          requested_deadline_at TEXT NOT NULL,
          effective_deadline_at TEXT NOT NULL,
          deadline_explicit INTEGER NOT NULL DEFAULT 0,
          max_attempts INTEGER NOT NULL DEFAULT 1,
          external_claimable INTEGER NOT NULL DEFAULT 0,
          callback_url TEXT NOT NULL DEFAULT '',
          created_at TEXT NOT NULL,
          UNIQUE(workspace_id, execution_request_id)
        );
        CREATE UNIQUE INDEX IF NOT EXISTS idx_execution_requests_idempotency
          ON execution_requests(workspace_id, idempotency_key) WHERE idempotency_key <> '';

        CREATE TABLE IF NOT EXISTS submission_claims (
          workspace_id TEXT NOT NULL,
          execution_request_id TEXT NOT NULL,
          execution_id TEXT NOT NULL,
          claimed_at TEXT NOT NULL,
          PRIMARY KEY (workspace_id, execution_request_id)
        );

        CREATE TABLE IF NOT EXISTS executions (
          id TEXT PRIMARY KEY,
          workspace_id TEXT NOT NULL,
          execution_request_id TEXT NOT NULL,
          handler_key TEXT NOT NULL,
          handler_version TEXT NOT NULL,
          status TEXT NOT NULL,
          attempt_count INTEGER NOT NULL DEFAULT 0,
          output BLOB,
          error_class TEXT NOT NULL DEFAULT '',
          error_message TEXT NOT NULL DEFAULT '',
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL,
          completed_at TEXT NOT NULL DEFAULT ''
        );

        CREATE TABLE IF NOT EXISTS callback_deliveries (
          id TEXT PRIMARY KEY,
          callback_id TEXT NOT NULL,
          execution_id TEXT NOT NULL,
          target_url TEXT NOT NULL,
          status TEXT NOT NULL,
          attempt_count INTEGER NOT NULL DEFAULT 0,
          next_attempt_at TEXT NOT NULL,
          last_error TEXT NOT NULL DEFAULT '',
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS starlark_handlers (
          workspace_id TEXT NOT NULL,
          handler_key TEXT NOT NULL,
          version TEXT NOT NULL,
          manifest_json TEXT NOT NULL,
          source TEXT NOT NULL,
          max_steps INTEGER NOT NULL,
          allowed_modules_json TEXT NOT NULL,
          allow_connector INTEGER NOT NULL DEFAULT 0,
          deleted INTEGER NOT NULL DEFAULT 0,
          created_at TEXT NOT NULL,
          PRIMARY KEY (workspace_id, handler_key, version)
        );

        CREATE TABLE IF NOT EXISTS wasm_handlers (
          workspace_id TEXT NOT NULL DEFAULT '',
          handler_key TEXT NOT NULL,
          version TEXT NOT NULL,
          abi TEXT NOT NULL,
          artifact TEXT NOT NULL,
          manifest_json TEXT NOT NULL,
          deleted INTEGER NOT NULL DEFAULT 0,
          created_at TEXT NOT NULL,
          PRIMARY KEY (workspace_id, handler_key, version)
        );

        CREATE TABLE IF NOT EXISTS custom_connectors (
          workspace_id TEXT NOT NULL,
          provider_key TEXT NOT NULL,
          display_name TEXT NOT NULL,
          base_url TEXT NOT NULL,
          auth_type TEXT NOT NULL,
          goauthy_provider_id TEXT NOT NULL,
          expected_connector_digest TEXT NOT NULL DEFAULT '',
          api_key_opts_json TEXT NOT NULL DEFAULT '',
          deleted INTEGER NOT NULL DEFAULT 0,
          created_at TEXT NOT NULL,
          PRIMARY KEY (workspace_id, provider_key)
        );
        ```
        Phase 1~3 빌드 호환을 위해 `dynamic_handlers`·`dynamic_handler_bytecode`·`handler_manifests`·`connector_manifests`·`execution_request_credentials` 테이블/메서드 및 `ensure*Schema`는 Phase 4에서 제거할 때까지 유지한다.
    *   `internal/storage/executions.go` & `callbacks.go`: `ScopedRequestID(ws, reqID)`/`ScopedExecutionID(ws, reqID)`(128-bit) 헬퍼, `ErrLimitExceeded`, `ExecutionRequestRecord.ExecutionRequestID` / `ExecutionRecord.WorkspaceID`, `BatchCreateResult{ExecutionID, ExecutionRequestID, Status, Created}` 추가. `CreateExecution(ctx, r ExecutionRequestRecord, executionID string) (bool, error)`, `SaveExecutionBatch(ctx, reqs []ExecutionRequestRecord) ([]BatchCreateResult, error)`(내부에서 `ScopedExecutionID(r.WorkspaceID, r.ExecutionRequestID)` 생성 및 `idempotency_key` 경합 시 재조회), `CreateAndLeaseInlineExecution(ctx, req ExecutionRequestRecord, executionID, workerID string, leaseTTL time.Duration) (LeaseRecord, bool, error)`(`bool`은 `created`; `submission_claims` 경합 패배 시 `created=false` + `checkIdempotency` 처리), `GetExecutionRequestByKey(ctx, ws, reqID) (ExecutionRequestRecord, error)`, `ListExecutionEvents(ctx, ws, id string, limit, offset int) ([]ExecutionEventRecord, error)` 구현. `GetExecution`/`complete`(기존 `"timed_out"` 분기 제거 → `"failed"` 통일)/`CancelExecution`/`ExpireExecution`은 `JOIN execution_requests r`로 `r.execution_request_id`·`r.input` 프로젝션, `DueCallbackDeliveries`/`CallbackDelivery`는 `JOIN callbacks c`로 `blob(c.payload)` 읽기. 터미널 종결 시 `delegation_token = x''` 스크러빙.
    *   `internal/storage/leases.go`: `ClaimNextForScope(ctx, workerID, ttl, scopeFilter, externalOnly bool) (ExecutionRequestRecord, LeaseRecord, error)`(`retry_timers` 포함, `attempt_number = COALESCE(MAX(attempts.attempt_number), 0) + 1`), `RecoverExpiredExecutions(ctx, now time.Time, limit int) (int, error)`(`claim_id = "recover-expired:" + id`, 현재 `attempt_number`), `RecoverExpiredLeasesBatch(ctx, now time.Time, limit int) (int, error)`(`retry_timers` upsert 포함) 추가(기존 `ClaimNext`/`AcquireInlineLease` 3-인자 및 `RecoverExpiredLeases`/`RecoverExpiredInlineLeases` 2-인자 시그니처는 Phase 4까지 호환 유지).
    *   `internal/storage/handlers.go` & `connectors.go`: `SaveStarlarkHandler`, `GetStarlarkHandler(ctx, ws, key, ver)`, `ListStarlarkHandlers(ctx, ws, limit, offset)`, `DeleteStarlarkHandler(ctx, ws, key, ver)`, `SaveWASMHandlerScoped`, `GetWASMHandlerScoped(ctx, ws, key, ver)`, `ListWASMHandlersScoped(ctx, ws, limit, offset)`, `DeleteWASMHandlerScoped(ctx, ws, key, ver)`, `SaveCustomConnector`, `GetCustomConnector(ctx, ws, key)`, `ListCustomConnectors(ctx, ws, limit, offset)`, `DeleteCustomConnector(ctx, ws, key)` 구현(기존 `ListWASMHandlers`/`DeleteWASMHandler`/`ListDynamicHandlers`/`GetDynamicHandler`/`SaveHandlerManifest`/`SaveConnectorManifest`는 Phase 4까지 유지).
*   **Checkpoint 1**: `store_test.go`(128-bit `Scoped*ID`, `idx_execution_requests_idempotency`, `RecoverExpiredExecutions`, `RecoverExpiredLeasesBatch` 고아/`inline:%`/재시도 분기, `256` 상한 `ErrLimitExceeded`, `< 200,000 bytes`, `JOIN callbacks`) 및 `go build ./... && go test -count=1 ./internal/storage/...` 통과.

### Phase 2: GoAuthy RFC 8693 + DPoP Client, Custom Connectors & SSRF Hardening

*   **Target Files**:
    *   `internal/connectors/goauthy.go` `(Create)` & `internal/connectors/goauthy_test.go` `(Create)`
    *   `internal/connectors/netsec.go` `(Create)` & `internal/connectors/netsec_test.go` `(Create)`
    *   `internal/connectors/custom.go` `(Create)` & `internal/connectors/custom_test.go` `(Create)`
    *   `internal/connectors/catalog.go` `(Modify)`
    *   `internal/connectors/operations.go` `(Modify)`
    *   `internal/connectors/delivery.go` `(Modify)`
    *   `internal/connectors/operations_test.go` `(Modify)`
    *   `internal/callbacks/callbacks.go` `(Modify)` & `internal/callbacks/callbacks_test.go` `(Modify)`
*   **구체 구현**:
    *   `internal/connectors/goauthy.go`: `GoauthyConfig{BaseURL, Issuer, ClientID, ClientSecret, Resource, DPoPKeyFile, AllowInsecureHTTP}`, `DeliveredCredential{Kind string, AccessToken string, TokenType string, APIKey string, Scope string, ProviderID string, ConnectorDigest string, Header string, Prefix string, CredentialVersion int64, ConnectionGeneration int64, ProviderRevision int64, TokenExpiresAtUnixMs int64, ConsentExpiresAtUnixMs int64}`, `CredentialStatus{State string, Version int64, AllowRefresh bool}`, `NewGoauthyClient(cfg GoauthyConfig) (*GoauthyClient, error)`와 메서드 `ExchangeToken(ctx, subjectToken string) ([]byte, time.Time, error)`, `FetchCredential(ctx, grantID, delegationToken string) (DeliveredCredential, error)`, `ProbeCredentialStatus(ctx, grantID, delegationToken string) (CredentialStatus, error)`, `RefreshCredential(ctx, grantID, delegationToken string, version int64) error`(`409`→`credential_refreshing`, `503`→`goauthy_unavailable`, `401 invalid_dpop_proof`→`goauthy_misconfigured`, `404`/`502`/`400`/`401`→`auth_expired`) 구현.
    *   `internal/connectors/netsec.go` & `internal/callbacks/callbacks.go`: `SafeDialContext`, `NewSafeHTTPClient(timeout time.Duration, allowedHosts []string) *http.Client`(`3xx` → `ErrSSRFBlocked` 래핑 `RoundTripper` + `limitReadCloser(240 KiB)`). `callbacks.Delivery`에 `AllowedCallbackHosts []string`, `DefaultTargetURL string` 추가 및 `SafeDialContext` 기반 `Client` 적용(`item.TargetURL == d.DefaultTargetURL`일 때만 `d.Token` 전송).
    *   `internal/connectors/custom.go`, `catalog.go`, `operations.go` & `delivery.go`: `APIKeyOpts{Header string, Prefix string}`, `CustomConnectorInput{ProviderKey, DisplayName, BaseURL, AuthType, GoauthyProviderID, ExpectedConnectorDigest string, APIKeyOpts *APIKeyOpts}`, `CustomConnectorRecord{WorkspaceID, ProviderKey, DisplayName, BaseURL, AuthType, GoauthyProviderID, ExpectedConnectorDigest string, APIKeyOpts *APIKeyOpts, Deleted bool, CreatedAt string}`, `WithTrustedBusinessCaller(ctx)`, `NewProxyExecutor(catalog *Catalog, store storage.Store, goauthy *GoauthyClient, allowedHosts []string) *ProxyExecutor`, `ExecuteProxyCall(ctx, ws, credRef, delToken string, callInput json.RawMessage) (json.RawMessage, error)`(`Query` 및 AMP `Vars` 파라미터 지원 포함), `RedactSecrets(s string, secrets ...string) string` 구현(게스트 응답 본문 포함 스크러빙). `delivery.go`의 `APIKeyDelivery`/`DeliveryBinding`은 Phase 4 `delivery_live_test.go`/`delivery_test.go` 정리 전까지 유지한다(`deliveryHTTPClient`는 유지).
*   **Checkpoint 2**: `goauthy_test.go`, `netsec_test.go`, `custom_test.go`, `callbacks_test.go`, `operations_test.go` 및 `go build ./... && go test -count=1 ./internal/connectors/... ./internal/callbacks/...` 통과.

### Phase 3: Starlark, WASM `v2` & Execution Service Integration

*   **Target Files**:
    *   `go.mod` `(Modify)` & `go.sum` `(Modify)`: `go.starlark.net v0.0.0-20261005163335-bcb1a1a55bf9` 추가.
    *   `internal/handlers/handlers.go` `(Modify)`
    *   `internal/handlers/starlark.go` `(Create)` & `internal/handlers/starlark_test.go` `(Create)`
    *   `internal/handlers/wasm.go` `(Modify)` & `internal/handlers/wasm_test.go` `(Modify)`
    *   `internal/executions/executions.go` `(Modify)`
    *   `internal/executions/executions_test.go` `(Modify)`
*   **구체 구현**:
    *   `internal/handlers/handlers.go`: `Manifest{..., Description, MaxInputBytes}`, `Request{..., CredentialRef string, DelegationToken string \`json:"-"\`}`, `StarlarkSpec`, `WASMSpec`(기존 `wasm.go` 타입에 `WorkspaceID`, `Deleted`, `CreatedAt` 확장), `ScopedLoader` (`LoadStarlark(ctx, ws, key, ver) (StarlarkSpec, error)`, `LoadWASM(ctx, ws, key, ver) (WASMSpec, error)`), `ResolveScoped(ctx, r *Registry, loader ScopedLoader, wasmRT *WASMRuntime, proxy ProxyCaller, ociFetch WASMFetchFunc, ws, key, ver string) (Handler, error)`, `TokenExchanger` (`ExchangeToken(ctx, subjectToken string) ([]byte, time.Time, error)`), `ProxyCaller` (`ExecuteProxyCall(ctx, ws, credRef, delToken string, callInput json.RawMessage) (json.RawMessage, error)`) 정의.
    *   `internal/handlers/starlark.go`: `NewStarlarkHandler(spec StarlarkSpec, proxy ProxyCaller) (*StarlarkHandler, error)`(`def run(input):`, `connector.call` `<= 64 KiB` / 16회, `json`/`base64`/`hex` wrapper `1 MiB` 상한, 허용 도메인 오류 승격) 및 256-엔트리 크기 제한 맵 구현.
    *   `internal/handlers/wasm.go`: `(*WASMRuntime).ValidateAndCompile(ctx, wasmBytes []byte, abi string) (blobDigest string, err error)`, `NewLazyHandler` `"side_effecting"` 허용, `conductor:function/v2` 비재진입 ABI(`context.Context`로 실행 메타 및 `ProxyCaller`/`DelegationToken` 전달, `uint64(uint32(...))`, `<= 64 KiB`/`>= 256 KiB`, `mod.CloseWithExitCode(ctx, 1)` 시 `dealloc` 미호출) 및 `wazero.NewCompilationCache()` + 세마포어(32) 구현.
    *   `internal/executions/executions.go`: `NewServiceWithRuntime(store, registry, loader, exchanger, proxy) (*Service, error)` 추가(`NewService(store, registry)`는 Phase 4 전까지 호환 유지). `Execution`/`ExecutionRequest`에 `WorkspaceID`, `CredentialRef`, `SubjectToken` (`json:"-"`), `DelegationToken` (`json:"-"`), `DelegationExpiresAt`, `Priority`, `CallbackURL` 반영(커스텀 `MarshalJSON`에서도 `SubjectToken`/`DelegationToken` 제외), `PrecheckBatch` 구현, `Fail`은 `GetExecution` → `GetExecutionRequestByKey(ctx, exec.WorkspaceID, exec.ExecutionRequestID)`로 수정(및 `Fail`/`finishLease`/`waitForTerminal`/`Cancel`의 `"timed_out"` 분기를 `"failed"` [`deadline_exceeded`]로 통일), `Events(ctx, ws, id string, limit, offset int)` 페이지네이션 및 워크스페이스 스코프 적용, `checkIdempotency`는 `GetExecutionRequestByKey` 수정, `sideEffectDispatched == false` 사전 부작용 `retry_wait` 재시도 및 `"payload.echo"`/`"connector.invoke"` 예약 구현.
*   **Checkpoint 3**: `starlark_test.go`, `wasm_test.go`, `executions_test.go` 및 `go build ./... && go test -count=1 ./internal/handlers/... ./internal/executions/...` 통과.

### Phase 4: HTTP Endpoints, WASM OCI Push, Legacy Removal & Wiring

*   **Target Files**:
    *   `go.mod` `(Modify)` & `go.sum` `(Modify)`: `github.com/jokruger/kavun` 제거 및 `go mod tidy`.
    *   `internal/storage/store.go` `(Modify)`, `internal/storage/executions.go` `(Modify)`, `internal/storage/leases.go` `(Modify)`, `internal/storage/handlers.go` `(Modify)`, `internal/connectors/delivery.go` `(Modify)`, `internal/executions/executions.go` `(Modify)`: Phase 1~3 호환 래퍼·구 `v1` 테이블/메서드 제거.
    *   `internal/handlers/handlers_test.go` `(Modify)`: `Kavun` 테스트 제거 및 Starlark/WASM `v2` 테스트로 갱신.
    *   `cmd/conductor/main.go` `(Modify)`, `server.go` `(Modify)`, `http_handlers.go` `(Modify)`, `http_connectors.go` `(Create)`, `http_connectors_test.go` `(Create)`, `http_executions.go` `(Modify)`, `http_worker.go` `(Modify)`, `wasm_artifact.go` `(Modify)`, `main_test.go` `(Modify)`.
    *   `scripts/smoke-container.sh` `(Modify)`: `docker run -e CONDUCTOR_INTERNAL_SECRET=...`, `ready()` 포함 모든 `curl`에 `X-Conductor-Internal-Token`, `X-Workspace-Id`, `X-Actor-*` 및 `/v1/workspaces/{ws}/*` 적용(반환 `id` 캡처 후 `exec_` 접두어·상태 검증).
    *   **Delete Files** (레거시 10개 파일 삭제):
        *   `internal/connectors/credential_provider.go`, `internal/connectors/credential_provider_test.go`, `internal/connectors/delivery_live_test.go`, `internal/connectors/delivery_test.go`
        *   `internal/connections/oauth.go`, `internal/connections/oauth_test.go`
        *   `internal/handlers/kavun.go`
        *   `internal/storage/inline_credentials_test.go`, `internal/executions/inline_credentials_test.go`
        *   `cmd/conductor/http_connections.go`
*   **구체 구현**:
    *   `server.go` / `main.go`: 구 라우트(`/v1/executions*`, `/v1/events`, `/v1/handlers*`, `/v1/worker/{poll,...}`, `/v1/connections`, `/v1/oauth/*`, `getHandlerDefinition`) 및 `loadDynamicHandlers`, `oauthStore`, `serviceToken` 제거; `WriteTimeout: 3m`; 내부망/mTLS/역할별 서비스 호출자 미들웨어; `1s ± 20%` `RecoverExpiredExecutions` 스위퍼; `writeErrorClass` 전체 매핑(`validation_failed`/`wasm_invalid`/`invalid_actor`/`invalid_input`/`proxy_limit_exceeded`→`400`, `unauthorized`/`auth_expired`/`auth_mismatch`→`401`, `forbidden_workspace`/`workspace_mismatch`/`forbidden_host`/`forbidden_provider`→`403`, `handler_not_found`/`not_found`→`404`, `handler_exists`/`limit_exceeded`/`reserved_handler_key`/`idempotency_conflict`/`conflict`→`409`, `oci_error`→`502`, `internal_auth_not_configured`/`wasm_push_not_configured`/`goauthy_unavailable`/`goauthy_misconfigured`/`credential_refreshing`/`credential_conflict`→`503`); `log/slog` JSON 감사 로그 출력 연결.
    *   `http_handlers.go` / `wasm_artifact.go`: Starlark/WASM `POST .../handlers`(`"kind": "wasm"` 시 OCI 풀 + `ValidateAndCompile`) 및 `POST .../handlers/wasm`(세마포어 `GOMAXPROCS*2` + `oras-go/v2` `v2` 미디어 타입 푸시; `newOCIWASMFetcher`도 등록된 `abi`에 따라 `v1`/`v2` 미디어 타입 모두 수용), `GET .../handlers` 목록 스키마 구현.
    *   `http_executions.go` / `http_worker.go` / `http_connectors.go`: 동기 `timeout_seconds <= 105s`, `/events` `<= 25`건/`64 KiB`/`credential_ref == ""` 검증, `Service.Poll`의 `ClaimNextForScope(..., true, true)` 및 `workerPollResponse`에 `workspace_id`/`execution_request_id` 포함, 내부 `RunWorker`의 `ClaimNextForScope(..., true, false)` + 인라인 `15s` 하트비트, 커스텀 커넥터 CRUD 및 `"business"` `WithTrustedBusinessCaller` ACL 검증 구현.
*   **Checkpoint 4**: `go test -race -count=1 ./...` 및 `docker build -t conductor:smoke . && sh scripts/smoke-container.sh` 통과.

--------------------------------------------------------------------------------

## Verification Plan

1.  **전체 단위·레이스 테스트**: `go test -race -count=1 ./...` (모든 패키지 통과).
2.  **고립·보안·위임 E2E**:
    *   `CONDUCTOR_INTERNAL_SECRET` 미설정/오류 `403`/`401`, `X-Workspace-Id` 불일치 `403`, 타 워크스페이스 리소스 `404`, `"business"` 일반 워크스페이스 차단(`forbidden_provider`).
    *   `goauthy` `POST /oidc/token` DPoP + `400 use_dpop_nonce` 재시도, `/credential` DPoP `ath` + `jti` 호출, `404` → `credential-status` → `/credential` 1회 재시도 → `POST .../refresh`(`allow_refresh=true`) 복구 및 `43`자 `connector_digest` 검증.
    *   `POST .../events` `<= 25`건(`<= 64 KiB`, `credential_ref == ""`) 원자 커밋 및 `requested_deadline_at` 보존 재시도.
    *   SSRF 사설/특수 대역 차단, `302` 차단, `240 KiB` 응답 캡, `callback_url` SSRF 차단 및 `CONDUCTOR_CALLBACK_TOKEN` 미부착, `< 200,000 bytes` Rhiza 예산.
3.  **컨테이너 스모크**: `docker build -t conductor:smoke . && sh scripts/smoke-container.sh`.
