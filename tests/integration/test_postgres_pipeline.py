import os
from datetime import date

import psycopg2
import pytest

from extract.extract import build_raw_record, save_raw_response
from load.load import delete_stock_prices_for_date, load_stock_prices
from mart.mart import (
    build_daily_stock_rankings,
    delete_daily_stock_rankings_for_date,
)
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


def replace_daily_snapshot(conn, transformed_items):
    base_date = transformed_items[0]["base_date"]
    delete_daily_stock_rankings_for_date(conn, base_date)
    delete_stock_prices_for_date(conn, base_date)
    loaded_count = load_stock_prices(conn, transformed_items)
    quality_result = validate_staging_load(conn, transformed_items)
    mart_rows = build_daily_stock_rankings(conn, base_date)
    validate_mart_rankings(
        mart_rows,
        transformed_items,
        expected_base_date=base_date,
    )
    return loaded_count, quality_result, mart_rows


def test_postgres_recollection_replaces_staging_and_mart_snapshot(
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

    loaded_count, quality_result, mart_rows = replace_daily_snapshot(
        postgres_conn,
        transformed_items,
    )
    postgres_conn.commit()

    assert loaded_count == 3
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
    assert len(mart_rows) == 3

    # 같은 기준일의 재수집 결과가 줄면 이전 종목이 남지 않아야 한다.
    reduced_items = transformed_items[:2]
    loaded_count, quality_result, mart_rows = replace_daily_snapshot(
        postgres_conn,
        reduced_items,
    )
    postgres_conn.commit()

    assert loaded_count == 2
    assert quality_result["row_count"] == 2
    assert quality_result["unique_isin_count"] == 2
    assert len(mart_rows) == 2

    remaining_isins = {item["isin_code"] for item in reduced_items}
    with postgres_conn.cursor() as cursor:
        cursor.execute(
            "SELECT isin_code FROM staging.stock_prices WHERE base_date = %s",
            (date(2023, 6, 1),),
        )
        staging_isins = {row[0] for row in cursor.fetchall()}
        cursor.execute(
            "SELECT isin_code FROM mart.daily_stock_rankings WHERE base_date = %s",
            (date(2023, 6, 1),),
        )
        mart_isins = {row[0] for row in cursor.fetchall()}

    assert staging_isins == remaining_isins
    assert mart_isins == remaining_isins


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
    replace_daily_snapshot(postgres_conn, transformed_items)
    postgres_conn.commit()

    # 길이는 VARCHAR(6)에 맞지만 허용 문자 규칙을 위반해 CHECK를 발생시킨다.
    invalid_items = [{**item} for item in transformed_items]
    invalid_items[0]["short_code"] = "!!!!!!"

    with pytest.raises(psycopg2.errors.CheckViolation):
        delete_daily_stock_rankings_for_date(
            postgres_conn,
            date(2023, 6, 1),
        )
        delete_stock_prices_for_date(
            postgres_conn,
            date(2023, 6, 1),
        )
        load_stock_prices(postgres_conn, invalid_items)
    postgres_conn.rollback()

    with postgres_conn.cursor() as cursor:
        cursor.execute(
            "SELECT COUNT(*) FROM staging.stock_prices "
            "WHERE base_date = %s",
            (date(2023, 6, 1),),
        )
        staging_count = cursor.fetchone()[0]
        cursor.execute(
            "SELECT COUNT(*) FROM mart.daily_stock_rankings "
            "WHERE base_date = %s",
            (date(2023, 6, 1),),
        )
        mart_count = cursor.fetchone()[0]

    assert staging_count == 3
    assert mart_count == 3
