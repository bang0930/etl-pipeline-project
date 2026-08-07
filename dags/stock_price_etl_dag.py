"""금융위원회 주식시세 ETL을 수동 실행하는 Airflow TaskFlow DAG."""

from airflow.sdk import Param, dag, get_current_context, task
from airflow.timetables.trigger import CronTriggerTimetable
import pendulum

from main import (
    create_database_connection,
    extract_latest_available_raw,
    extract_raw_for_date,
    parse_cli_date,
    publish_snapshot_for_run,
    verify_published_snapshot,
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
