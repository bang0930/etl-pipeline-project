# 장애 테스트 및 복구 가이드

## 목적

이 문서는 주식시세 ETL 파이프라인의 외부 API, PostgreSQL, Airflow 및
Metabase 장애를 안전하게 재현하고 복구하는 절차를 기록한다.

장애 테스트에서는 실제 인증키, 데이터베이스 비밀번호와 Slack Webhook을
출력하지 않는다. 개발 데이터가 저장된 Docker Volume은 삭제하지 않는다.

## 자동화된 회귀 테스트

### API Timeout

`tests/test_extract.py`는 `requests.Timeout`을 발생시켜 다음 내용을 검증한다.

- 오류에 기준일, 페이지 번호와 요청 제한시간이 포함된다.
- 인증키, 전체 요청 URL과 원래 예외 본문은 노출되지 않는다.
- 변환된 `RuntimeError`가 상위 실행 계층으로 전달된다.

```bash
pytest -q tests/test_extract.py
```

검증 결과: `14 passed`

### PostgreSQL 연결 중단

`tests/integration/test_postgres_pipeline.py`는 격리된 PostgreSQL에서 다음
순서로 Staging·Mart 교체 트랜잭션 도중의 연결 상실을 재현한다.

1. 기존 Staging·Mart Snapshot을 생성하고 Commit한다.
2. 별도 DB 세션에서 Mart와 Staging 행을 삭제한다.
3. Commit 전에 해당 세션을 `pg_terminate_backend`로 종료한다.
4. 종료된 세션의 Commit 실패를 확인한다.
5. 기존 Staging·Mart Snapshot이 그대로 유지되는지 확인한다.

```bash
./scripts/test-postgres.sh
```

검증 결과: `84 passed, 1 skipped`

통합 테스트 DB는 `tmpfs`를 사용하고 테스트 종료 시 자동으로 제거되므로
개발 PostgreSQL과 데이터를 공유하지 않는다.

### API 페이지 정합성

`tests/test_quality.py`와 `tests/test_transform.py`는 페이지 번호의 중복·누락,
페이지별 전체 건수 불일치, 실제 item 건수 불일치와 페이지 사이의 종목 키
중복을 검증한다. 문제가 발견되면 임의로 중복을 제거하거나 누락값을
추정하지 않고 `PaginationConsistencyError`로 즉시 실패한다.

```bash
pytest -q tests/test_quality.py tests/test_transform.py
```

검증 결과: `46 passed`

## Airflow PostgreSQL 장애 훈련

### 사전 확인

장애를 발생시키기 전에 실행 중인 DAG가 없는지 확인하고 Staging·Mart의
현재 행 수를 기록한다.

```bash
docker compose exec airflow-api-server \
  airflow dags list-runs stock_price_etl --state running --output json

docker compose exec postgres sh -c \
  'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -Atc \
  "SELECT (SELECT COUNT(*) FROM staging.stock_prices), \
          (SELECT COUNT(*) FROM mart.daily_stock_rankings), \
          (SELECT MAX(base_date) FROM mart.daily_stock_rankings);"'
```

### 장애 발생

ETL PostgreSQL만 중단하고 고정 Run ID로 DAG를 실행한다. Airflow Metadata
DB와 Metabase Metadata DB는 중단하지 않는다.

```bash
docker compose stop postgres

docker compose exec airflow-api-server \
  airflow dags trigger stock_price_etl \
  --run-id failure_test_23_postgres_outage_20260811 \
  --conf '{"base_date":"20260807","num_of_rows":100}'
```

### 기대 동작

- `extract_and_validate_raw`가 DB 연결 오류로 실패한다.
- 최초 실행 이후 최대 2회 재시도한다.
- 재시도 중에는 Slack 알림을 보내지 않는다.
- 모든 재시도가 끝나면 DAG가 실패하고 Slack 알림을 한 번 전송한다.
- Transform과 Verify Task는 실행되지 않는다.
- 기존 Staging·Mart Snapshot은 유지된다.

### 2026-08-11 검증 기록

장애 발생 전 기준값은 다음과 같다.

- Staging 전체 행 수: `39,152`
- Mart 전체 행 수: `39,152`
- Mart 최신 기준일: `2026-08-07`

실행 ID `failure_test_23_postgres_outage_20260811`로 장애를 재현했다.

- 1차 실패: `16:17:39 KST`
- 2차 실패: `16:36:53 KST`
- 3차 실패: `17:06:54 KST`
- 최종 DAG 상태: `failed`
- Transform·Verify Task: `upstream_failed`

재시도 간격은 `retry_exponential_backoff` 설정에 따라 증가하며, 마지막
재시도는 `max_retry_delay`인 30분을 적용받았다. 3차 실패 후에는 더 이상
재시도하지 않고 DAG가 최종 실패로 전환됐다.

이 최초 장애 테스트에서는 최종 실패까지 약 49분이 걸려 장애 감지가
지나치게 늦다는 문제도 확인했다. 또한 Slack Connection과 Webhook 자체는
정상이었지만, DAG 실패 콜백 메시지의 `{{ exception }}` 변수가 콜백 문맥에
없어 템플릿 렌더링이 실패했다. DAG Processor 로그에서 `UndefinedError:
'exception' is undefined`와 `Callback failed`를 확인했다.

이에 재시도 정책을 2분 고정 간격, 최대 2회로 변경했다. 계속 실패하면 최초
실패 후 약 4분 뒤 최종 실패로 전환된다. Slack 메시지는 DAG 실패 콜백에서
항상 제공되는 DAG 상태와 Task 로그 링크를 사용하도록 수정했다. Webhook은
비밀 URL을 출력하지 않는 `curl` 검증에서 HTTP `200`, 응답 `ok`를 확인했다.

수정 후 `failure_test_23_slack_callback_20260811` Run ID로 동일한 장애를 다시
재현했다.

- Run 시작: `17:17:31 KST`
- 최종 실패 콜백 실행: `17:21:37 KST`
- 최종 DAG 상태: `failed`
- 최종 실패 감지 시간: 약 4분
- DAG Processor 콜백 렌더링 오류: 없음

이를 통해 2분 고정 간격 재시도와 최종 실패 콜백 실행을 확인했다. 장애 테스트
직후 ETL PostgreSQL을 다시 시작했으며 `healthy` 상태로 복구된 것을 확인했다.

PostgreSQL을 다시 시작한 직후에도 장애 전과 같은 `39,152`개의 Staging·Mart
행과 최신 기준일 `2026-08-07`이 유지됐다. 이어서 같은 기준일을
`failure_test_23_postgres_recovery_20260811` Run ID로 다시 실행했다.

- 복구 Run 시작: `17:08:29 KST`
- 복구 Run 종료: `17:08:39 KST`
- Extract·Transform·Verify Task: 모두 `success`
- 복구 후 Staging 전체 행 수: `39,152`
- 복구 후 Mart 전체 행 수: `39,152`
- 복구 후 Mart 최신 기준일: `2026-08-07`

이를 통해 DB 장애 중 후속 작업이 차단되고, 기존 Snapshot이 보존되며,
서비스 복구 후 같은 기준일을 멱등하게 재실행할 수 있음을 확인했다.

### 상태 확인

```bash
docker compose exec airflow-api-server \
  airflow tasks states-for-dag-run \
  stock_price_etl failure_test_23_postgres_outage_20260811 \
  --output json
```

### 복구

```bash
docker compose start postgres
docker compose ps postgres
```

PostgreSQL이 `healthy` 상태가 된 뒤 실패한 기준일을 다시 실행한다. 재실행
완료 후 Staging·Mart 행 수와 최신 기준일이 장애 이전과 일치하는지 확인한다.

## Metadata 재시작 검증

Airflow와 Metabase는 각각 별도 PostgreSQL Metadata DB와 Docker Volume을
사용한다. 다음 테스트에서는 `--volumes` 옵션을 사용하지 않는다.

```bash
docker compose restart \
  airflow-api-server airflow-scheduler airflow-dag-processor airflow-triggerer

docker compose restart metabase metabase-postgres
docker compose ps
```

재시작 후 다음 항목을 확인한다.

- Airflow DAG와 실행 이력이 유지된다.
- Scheduler, API Server, DAG Processor와 Triggerer가 `healthy` 상태다.
- Metabase 관리자 계정, 데이터베이스 연결, 질문과 대시보드가 유지된다.

### 2026-08-11 검증 기록

재시작 전후 다음 Metadata 개수가 동일하게 유지되는 것을 확인했다.

- Airflow `stock_price_etl` 실행 이력: `19`
- Metabase 대시보드: `2`
- Metabase 질문: `51`
- Metabase 사용자: `2`
- Metabase 데이터베이스 연결: `2`

재시작 후 Airflow가 `stock_price_etl` DAG를 다시 발견했으며, 모든 Airflow와
Metabase 관련 컨테이너가 정상 상태로 복구됐다.

## 주의 사항

- `docker compose down --volumes`는 실행하지 않는다.
- 장애 테스트 전에 실행 중인 운영 DAG가 없는지 확인한다.
- ETL PostgreSQL과 Airflow Metadata PostgreSQL을 혼동하지 않는다.
- 장애가 끝나면 중단한 서비스를 반드시 복구하고 Health Check를 확인한다.
