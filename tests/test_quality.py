from datetime import date
from decimal import Decimal

import pytest

from quality.exceptions import DataQualityError
from quality.validators import (
    validate_raw_batch,
    validate_mart_rankings,
    validate_staging_load,
    validate_transformed_items,
)


def make_raw_response(
    api_response_factory,
    *,
    page_no,
    items,
    total_count,
    response_id=None,
):
    payload = api_response_factory(items=items, total_count=total_count)
    payload["response"]["body"]["pageNo"] = str(page_no)

    return {
        "response_id": response_id or page_no,
        "run_id": "run-1",
        "requested_base_date": date(2023, 6, 1),
        "page_no": page_no,
        "response_total_count": total_count,
        "returned_item_count": len(items),
        "payload": payload,
    }


def test_validate_raw_batch_accepts_complete_pages(
    stock_item,
    api_response_factory,
):
    second_item = {**stock_item, "isinCd": "KR7005930003"}
    raw_responses = [
        make_raw_response(
            api_response_factory,
            page_no=1,
            items=[stock_item],
            total_count=2,
        ),
        make_raw_response(
            api_response_factory,
            page_no=2,
            items=[second_item],
            total_count=2,
        ),
    ]

    assert validate_raw_batch(raw_responses) is raw_responses


def test_validate_raw_batch_rejects_empty_batch():
    with pytest.raises(DataQualityError, match="비어"):
        validate_raw_batch([])


def test_validate_raw_batch_rejects_missing_page(
    stock_item,
    api_response_factory,
):
    raw_responses = [
        make_raw_response(
            api_response_factory,
            page_no=1,
            items=[stock_item],
            total_count=2,
        ),
        make_raw_response(
            api_response_factory,
            page_no=3,
            items=[stock_item],
            total_count=2,
        ),
    ]

    with pytest.raises(DataQualityError, match="중복되거나 누락"):
        validate_raw_batch(raw_responses)


def test_validate_raw_batch_rejects_payload_count_mismatch(
    stock_item,
    api_response_factory,
):
    raw_response = make_raw_response(
        api_response_factory,
        page_no=1,
        items=[stock_item],
        total_count=1,
    )
    raw_response["returned_item_count"] = 2

    with pytest.raises(DataQualityError, match="메타데이터와 다릅니다"):
        validate_raw_batch([raw_response])


def test_validate_raw_batch_rejects_total_item_count_mismatch(
    stock_item,
    api_response_factory,
):
    raw_response = make_raw_response(
        api_response_factory,
        page_no=1,
        items=[stock_item],
        total_count=2,
    )

    with pytest.raises(DataQualityError, match="전체 item 수"):
        validate_raw_batch([raw_response])


def test_validate_transformed_items_accepts_valid_batch(transformed_item):
    items = [transformed_item]

    assert validate_transformed_items(
        items,
        expected_base_date=date(2023, 6, 1),
    ) is items


def test_validate_transformed_items_rejects_invalid_short_code(
    transformed_item,
):
    item = {**transformed_item, "short_code": "123"}

    with pytest.raises(DataQualityError, match="단축코드"):
        validate_transformed_items([item])


def test_validate_transformed_items_allows_zero_ohlc_with_close_price(
    transformed_item,
):
    item = {
        **transformed_item,
        "open_price": 0,
        "high_price": 0,
        "low_price": 0,
        "close_price": 11400,
    }

    assert validate_transformed_items([item]) == [item]


class FakeCursor:
    def __init__(self, result):
        self.result = result
        self.executed = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        return False

    def execute(self, query, params):
        self.executed = (query, params)

    def fetchone(self):
        return self.result


class FakeConnection:
    def __init__(self, result):
        self.cursor_instance = FakeCursor(result)

    def cursor(self, cursor_factory=None):
        return self.cursor_instance


def test_validate_staging_load_accepts_matching_counts(transformed_item):
    conn = FakeConnection(
        {
            "row_count": 1,
            "unique_isin_count": 1,
            "source_response_count": 1,
        }
    )

    result = validate_staging_load(conn, [transformed_item])

    assert result["row_count"] == 1
    assert conn.cursor_instance.executed[1] == (date(2023, 6, 1),)


def test_validate_staging_load_rejects_row_count_mismatch(transformed_item):
    conn = FakeConnection(
        {
            "row_count": 0,
            "unique_isin_count": 0,
            "source_response_count": 0,
        }
    )

    with pytest.raises(DataQualityError, match="적재 건수"):
        validate_staging_load(conn, [transformed_item])


def make_mart_rows(transformed_item):
    down_row = {
        "base_date": transformed_item["base_date"],
        "isin_code": transformed_item["isin_code"],
        "open_price": 15240,
        "close_price": 15080,
        "intraday_price_change": -160,
        "intraday_change_rate": Decimal("-1.049869"),
        "movement_direction": "DOWN",
        "movement_rank": 1,
        "trading_volume": 38219,
        "trading_value": 578006510,
        "trading_volume_rank": 2,
        "trading_value_rank": 2,
    }
    up_row = {
        "base_date": transformed_item["base_date"],
        "isin_code": "KR7005930003",
        "open_price": 14000,
        "close_price": 15080,
        "intraday_price_change": 1080,
        "intraday_change_rate": Decimal("7.714286"),
        "movement_direction": "UP",
        "movement_rank": 1,
        "trading_volume": 100000,
        "trading_value": 1000000000,
        "trading_volume_rank": 1,
        "trading_value_rank": 1,
    }
    staging_items = [
        transformed_item,
        {
            **transformed_item,
            "isin_code": up_row["isin_code"],
        },
    ]
    return [down_row, up_row], staging_items


def test_validate_mart_rankings_accepts_valid_rows(transformed_item):
    mart_rows, staging_items = make_mart_rows(transformed_item)

    assert validate_mart_rankings(
        mart_rows,
        staging_items,
        expected_base_date=date(2023, 6, 1),
    ) is mart_rows


def test_validate_mart_rankings_rejects_wrong_direction(transformed_item):
    mart_rows, staging_items = make_mart_rows(transformed_item)
    mart_rows[0]["movement_direction"] = "UP"

    with pytest.raises(DataQualityError, match="등락 방향"):
        validate_mart_rankings(mart_rows, staging_items)


def test_validate_mart_rankings_rejects_wrong_volume_rank(transformed_item):
    mart_rows, staging_items = make_mart_rows(transformed_item)
    mart_rows[0]["trading_volume_rank"] = 1

    with pytest.raises(DataQualityError, match="거래량 순위"):
        validate_mart_rankings(mart_rows, staging_items)


def test_validate_mart_rankings_rejects_staging_key_mismatch(transformed_item):
    mart_rows, staging_items = make_mart_rows(transformed_item)
    staging_items[1] = {
        **staging_items[1],
        "isin_code": "KR7000270009",
    }

    with pytest.raises(DataQualityError, match="키 집합"):
        validate_mart_rankings(mart_rows, staging_items)
