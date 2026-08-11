"""금융위원회 주식시세 ETL을 자동·수동 실행하는 Airflow TaskFlow DAG."""

from datetime import timedelta

import pendulum
from airflow.providers.slack.notifications.slack_webhook import SlackWebhookNotifier
from airflow.sdk import Param, dag, get_current_context, task
from airflow.timetables.trigger import CronTriggerTimetable

from main import (
    create_database_connection,
    extract_latest_available_raw,
    extract_raw_for_date,
    parse_cli_date,
    publish_snapshot_for_run,
    verify_published_snapshot,
)


# 실제 Webhook URL은 코드에 두지 않고 Airflow의 slack_default Connection에서 읽는다.
# DAG의 모든 재시도가 끝난 뒤 최종 실패 상태가 되었을 때만 이 콜백이 실행된다.
SLACK_FAILURE_NOTIFIER = SlackWebhookNotifier(
    slack_webhook_conn_id="slack_default",
    text=(
        ":red_circle: *Stock Price ETL 실패*\n"
        "*DAG*: `{{ dag.dag_id }}`\n"
        "*Run*: `{{ run_id }}`\n"
        "*Task*: `{{ task_instance.task_id }}`\n"
        "*시도 횟수*: `{{ task_instance.try_number }}`\n"
        # DAG 최종 실패 콜백에는 exception이 없을 수 있으므로 DAG 상태를 사용한다.
        # 상세 예외는 아래 Task 로그 링크에서 확인한다.
        "*상태*: `{{ dag_run.state }}`\n"
        "<{{ task_instance.log_url }}|Airflow Task 로그 열기>"
    ),
)


@dag(
    dag_id="stock_price_etl",
    description="주식시세 API를 Raw, Staging, Mart 계층으로 처리한다.",
    schedule=CronTriggerTimetable(
        "0 19 * * *",
        timezone="Asia/Seoul",
    ),
    start_date=pendulum.datetime(2023, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    on_failure_callback=SLACK_FAILURE_NOTIFIER,
    default_args={
        # 최초 실행을 제외하고 최대 2회 다시 시도한다.
        "retries": 2,
        # 장애 감지까지 지나치게 오래 걸리지 않도록 2분 간격으로 고정한다.
        # 계속 실패하면 최초 실패 후 약 4분 뒤 최종 실패 알림을 보낸다.
        "retry_delay": timedelta(minutes=2),
        "retry_exponential_backoff": False,
    },
    tags=["stock-price", "etl"],
    params={
        "base_date": Param(
            None,
            type=["null", "string"],
            pattern=r"^[0-9]{8}$",
            title="기준일자",
            description="수집할 기준일자를 YYYYMMDD 형식으로 입력합니다.",
        ),
        "num_of_rows": Param(
            100,
            type="integer",
            minimum=1,
            title="페이지당 요청 건수",
        ),
    },
)
def stock_price_etl():
    @task(task_id="extract_and_validate_raw")
    def extract_task():
        context = get_current_context()
        params = context["params"]
        base_date_value = params.get("base_date")
        num_of_rows = params["num_of_rows"]

        conn = create_database_connection()
        try:
            if base_date_value is not None:
                # 수동 재실행은 사용자가 지정한 날짜만 정확히 수집한다.
                base_date = parse_cli_date(base_date_value)
                return extract_raw_for_date(conn, base_date, num_of_rows)

            # 자동 실행은 19시 스케줄 시각을 한국 날짜로 변환한 뒤,
            # 전날부터 역순으로 API가 실제 공개한 가장 최근 거래일을 찾는다.
            logical_date = context.get("logical_date")
            if logical_date is None:
                raise ValueError(
                    "수동 실행은 base_date를 YYYYMMDD 형식으로 입력해야 합니다."
                )

            reference_date = logical_date.in_timezone("Asia/Seoul").date()
            return extract_latest_available_raw(
                conn=conn,
                reference_date=reference_date,
                num_of_rows=num_of_rows,
            )
        finally:
            conn.close()

    @task(task_id="transform_and_publish_snapshot")
    def publish_task(raw_metadata):
        conn = create_database_connection()
        try:
            return publish_snapshot_for_run(conn, raw_metadata)
        finally:
            conn.close()

    @task(task_id="verify_published_snapshot")
    def verify_task(publish_metadata):
        conn = create_database_connection()
        try:
            return verify_published_snapshot(conn, publish_metadata)
        finally:
            conn.close()

    raw_metadata = extract_task()
    publish_metadata = publish_task(raw_metadata)
    verify_task(publish_metadata)


stock_price_etl()
