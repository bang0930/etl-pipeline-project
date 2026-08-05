import os
from datetime import date

import psycopg2
import pytest

from extract.extract import build_raw_record, save_raw_response
from load.load import load_stock_prices
from mart.mart import build_daily_stock_rankings
from quality.validators import (
    validate_mart_rankings,
    validate_raw_batch,
    validate_staging_load,
    validate_transformed_items,
)
from transform.transform import fetch_raw_responses, transform_stock_prices


pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.environ.get("RUN_POSTGRES_INTEGRATION") != "1",
        reason="PostgreSQL 통합 테스트 환경에서만 실행합니다.",
    ),
]


@pytest.fixture
def postgres_conn():
    conn = psycopg2.connect(
        host=os.environ["POSTGRES_HOST"],
        port=os.environ["POSTGRES_PORT"],
        database=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )

    with conn.cursor() as cursor:
        cursor.execute(
            "TRUNCATE TABLE raw.stock_price_api_responses "
            "RESTART IDENTITY CASCADE"
        )
    conn.commit()

    try:
        yield conn
    finally:
        conn.rollback()
        with conn.cursor() as cursor:
            cursor.execute(
                "TRUNCATE TABLE raw.stock_price_api_responses "
                "RESTART IDENTITY CASCADE"
            )
        conn.commit()
        conn.close()


def make_stock_items_payload(stock_item, api_response_factory):
    second_item = {
        **stock_item,
        "srtnCd": "005930",
        "isinCd": "KR7005930003",
        "itmsNm": "삼성전자",
        "mkp": "14000",
        "hipr": "16000",
        "lopr": "13000",
        "trqu": "100000",
        "trPrc": "1000000000",
    }
    zero_open_item = {
        **stock_item,
        "srtnCd": "001230",
        "isinCd": "KR7001230002",
        "itmsNm": "동국제강",
        "clpr": "11400",
        "mkp": "0",
        "hipr": "0",
        "lopr": "0",
        "trqu": "0",
        "trPrc": "0",
    }
    return api_response_factory(
        items=[stock_item, second_item, zero_open_item],
        total_count=3,
    )


def save_test_raw_batch(
    conn,
    stock_item,
    api_response_factory,
    run_id="integration-run-1",
):
    payload = make_stock_items_payload(stock_item, api_response_factory)
    raw_record = build_raw_record(
        run_id=run_id,
        base_date=date(2023, 6, 1),
        page_no=1,
        num_of_rows=100,
        status_code=200,
        data=payload,
    )
    save_raw_response(conn, raw_record)
    return run_id


def test_postgres_raw_to_staging_quality_and_idempotency(
    postgres_conn,
    stock_item,
    api_response_factory,
):
    run_id = save_test_raw_batch(
        postgres_conn,
        stock_item,
        api_response_factory,
    )

    raw_responses = fetch_raw_responses(
        postgres_conn,
        run_id,
        date(2023, 6, 1),
    )
    validate_raw_batch(raw_responses)
    postgres_conn.commit()

    transformed_items = transform_stock_prices(
        postgres_conn,
        run_id,
        date(2023, 6, 1),
    )
    validate_transformed_items(
        transformed_items,
        expected_base_date=date(2023, 6, 1),
    )

    assert load_stock_prices(postgres_conn, transformed_items) == 3
    assert load_stock_prices(postgres_conn, transformed_items) == 3

    quality_result = validate_staging_load(
        postgres_conn,
        transformed_items,
    )

    mart_rows = build_daily_stock_rankings(
        postgres_conn,
        date(2023, 6, 1),
    )
    validate_mart_rankings(
        mart_rows,
        transformed_items,
        expected_base_date=date(2023, 6, 1),
    )

    # 동일 기준일을 다시 계산해도 Mart 행 수와 키는 유지되어야 한다.
    rerun_mart_rows = build_daily_stock_rankings(
        postgres_conn,
        date(2023, 6, 1),
    )
    validate_mart_rankings(
        rerun_mart_rows,
        transformed_items,
        expected_base_date=date(2023, 6, 1),
    )
    postgres_conn.commit()

    assert quality_result["row_count"] == 3
    assert quality_result["unique_isin_count"] == 3
    assert quality_result["source_response_count"] == 1

    rows_by_isin = {row["isin_code"]: row for row in mart_rows}
    assert rows_by_isin["KR700088K015"]["movement_direction"] == "DOWN"
    assert rows_by_isin["KR7005930003"]["movement_direction"] == "UP"
    assert rows_by_isin["KR7001230002"]["movement_direction"] == "UP"
    assert rows_by_isin["KR700088K015"]["movement_rank"] == 1
    assert rows_by_isin["KR7005930003"]["movement_rank"] == 1
    assert rows_by_isin["KR7001230002"]["movement_rank"] is None
    assert rows_by_isin["KR700088K015"]["trading_volume_rank"] == 2
    assert rows_by_isin["KR7005930003"]["trading_volume_rank"] == 1
    assert rows_by_isin["KR700088K015"]["trading_value_rank"] == 2
    assert rows_by_isin["KR7005930003"]["trading_value_rank"] == 1
    assert len(rerun_mart_rows) == 3


def test_postgres_constraint_failure_rolls_back_staging_batch(
    postgres_conn,
    stock_item,
    api_response_factory,
):
    run_id = save_test_raw_batch(
        postgres_conn,
        stock_item,
        api_response_factory,
        run_id="integration-run-rollback",
    )
    postgres_conn.commit()

    transformed_items = transform_stock_prices(
        postgres_conn,
        run_id,
        date(2023, 6, 1),
    )
    # 길이는 VARCHAR(6)에 맞지만 허용 문자 규칙을 위반해 CHECK를 발생시킨다.
    transformed_items[0]["short_code"] = "!!!!!!"

    with pytest.raises(psycopg2.errors.CheckViolation):
        load_stock_prices(postgres_conn, transformed_items)
    postgres_conn.rollback()

    with postgres_conn.cursor() as cursor:
        cursor.execute(
            "SELECT COUNT(*) FROM staging.stock_prices "
            "WHERE base_date = %s",
            (date(2023, 6, 1),),
        )
        staging_count = cursor.fetchone()[0]

    assert staging_count == 0
