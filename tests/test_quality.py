from datetime import date

import pytest

from quality.exceptions import DataQualityError
from quality.validators import (
    validate_raw_batch,
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


def test_validate_transformed_items_rejects_high_below_close(
    transformed_item,
):
    item = {
        **transformed_item,
        "high_price": transformed_item["close_price"] - 1,
    }

    with pytest.raises(DataQualityError, match="고가"):
        validate_transformed_items([item])


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
