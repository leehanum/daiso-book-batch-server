# Batch Service

도서 쇼핑몰 **MSA 환경**에서 **정기·대량 작업**을 담당하는 배치 서버입니다.
API 서버와 분리되어 동작하며 User Service, Coupon Service, RabbitMQ와 연동됩니다.

> 관련 저장소: [Coupon Service](https://github.com/leehanum/daiso-book-coupon-server)

---

## 기술 스택

`Java 21` `Spring Boot 3.5` `Spring Batch` `OpenFeign` `Eureka` `RabbitMQ` `ShedLock` `MySQL` `Docker` `GitHub Actions`

---

## 배치 Job

| Job | 실행 주기 | 설명 |
|---|---|---|
| **생일 쿠폰 (MSA)** | 매월 1일 00:00 | 이번 달 생일 회원에게 쿠폰 발급 |
| **생일 쿠폰 (DB)** | 수동 | 위와 동일, 성능 우선 버전 |
| 회원 등급 변경 | 매일 01:00 | 최근 90일 순수 구매액 기준 등급 재산정 |
| 휴면 계정 전환 | 매일 04:00 | 90일 미로그인 계정 휴면 처리 |

---

## 생일 쿠폰 배치 — 두 가지 방식

같은 요구사항을 **MSA 원칙을 지키는 방식**과 **성능을 우선하는 방식**으로 각각 구현했습니다.

### 1. 메시지 기반 (운영 기본값)

```
Batch ──Feign(커서 페이징)──▶ User Service
  └── chunk(1000) ──▶ RabbitMQ ──▶ Coupon Service 저장
```

- **커서 페이징**: offset 대신 `lastSeenId` 기준 조회 → 데이터가 많아져도 조회 속도 일정
- **재시도 정책**: 연결 실패·5xx는 3회 재시도, 4xx는 즉시 실패
- **Publisher Confirm**: 브로커 ACK를 받아야 chunk 성공 → 메시지 유실 방지

### 2. DB 직접 접근 + 파티셔닝

```
ManagerStep ─ user_id 범위로 4분할
  ├─ Worker 0 ┐
  ├─ Worker 1 ├─ 멀티스레드 병렬 처리
  ├─ Worker 2 │  (JdbcPagingReader → JdbcBatchWriter)
  └─ Worker 3 ┘
```

- 대상 회원 ID 범위를 나눠 **병렬 처리**
- 쿠폰 정책은 한 번만 조회해 스레드 간 공유

### 비교

| | 메시지 기반 | DB 직접 접근 |
|---|---|---|
| 서비스 경계 | 준수 | 타 서비스 DB 접근 |
| 결합도 | 낮음 | 높음 |
| 성능 | 보통 | 빠름 |
| 적합한 상황 | MSA 구조·확장성 중시 | 대량 처리 속도 중시 |

---

## 안정성 설계

| 문제 | 해결 |
|---|---|
| 서버 여러 대에서 배치 중복 실행 | **ShedLock** — DB 락으로 한 인스턴스만 실행 |
| 쿠폰 중복 발급 | 조회 시 `NOT EXISTS` + 저장 시 `INSERT IGNORE` |
| 일부 데이터 오류로 전체 중단 | `skip` / `retry` 정책으로 결함 허용 |
| 배치 실패를 늦게 인지 | 실패 시 **Dooray 웹훅 알림** |

---

## 수동 실행 API

| URL | Job |
|---|---|
| `GET /batch/birthday/msa` | 생일 쿠폰 (MSA) |
| `GET /batch/birthday/db` | 생일 쿠폰 (DB) |
| `GET /batch/grade` | 등급 변경 |
| `GET /batch/dormant` | 휴면 전환 |
