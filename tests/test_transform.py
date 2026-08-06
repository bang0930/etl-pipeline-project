from datetime import date
from decimal import Decimal

import pytest

import transform.transform as transform_module
from quality.exceptions import PaginationConsistencyError
from transform.transform import (
    extract_items,
    parse_date,
    parse_optional_decimal,
    parse_optional_integer,
    transform_stock_price_items,
    validate_transformed_data,
)


def test_parse_date_converts_yyyymmdd_string():
    assert parse_date("20230601") == date(2023, 6, 1)


@pytest.mark.parametrize("value", [None, "", "   "])
def test_parse_date_returns_none_for_missing_value(value):
    assert parse_date(value) is None


def test_parse_date_rejects_invalid_date():
    with pytest.raises(ValueError):
        parse_date("20230230")


@pytest.mark.parametrize(
    ("value", "expected"),
    [("15080", 15080), (" -150 ", -150), (0, 0)],
)
def test_parse_optional_integer(value, expected):
    assert parse_optional_integer(value) == expected


@pytest.mark.parametrize("value", [None, "", "  "])
def test_parse_optional_integer_returns_none_for_missing_value(value):
    assert parse_optional_integer(value) is None


def test_parse_optional_integer_rejects_non_numeric_value():
    with pytest.raises(ValueError):
        parse_optional_integer("not-a-number")


def test_parse_optional_decimal_uses_decimal():
    result = parse_optional_decimal("-0.98")

    assert result == Decimal("-0.98")
    assert isinstance(result, Decimal)


@pytest.mark.parametrize("value", [None, "", "  "])
def test_parse_optional_decimal_returns_none_for_missing_value(value):
    assert parse_optional_decimal(value) is None


def test_extract_items_supports_single_item_object(stock_item, api_response_factory):
    raw_responses = [
        {
            "response_id": 10,
            "payload": api_response_factory(items=stock_item),
        }
    ]

    result = extract_items(raw_responses)

    assert len(result) == 1
    assert result[0]["isinCd"] == stock_item["isinCd"]
    assert result[0]["source_response_id"] == 10


def test_extract_items_supports_item_list(stock_item, api_response_factory):
    second_item = {
        **stock_item,
        "srtnCd": "005930",
        "isinCd": "KR7005930003",
        "itmsNm": "삼성전자",
    }
    raw_responses = [
        {
            "response_id": 11,
            "payload": api_response_factory(items=[stock_item, second_item]),
        }
    ]

    result = extract_items(raw_responses)

    assert len(result) == 2
    assert {item["source_response_id"] for item in result} == {11}


def test_transform_stock_price_items_converts_names_and_types(stock_item):
    raw_item = {**stock_item, "source_response_id": 20}

    result = transform_stock_price_items([raw_item])[0]

    assert result["source_response_id"] == 20
    assert result["base_date"] == date(2023, 6, 1)
    assert result["short_code"] == "00088K"
    assert result["close_price"] == 15080
    assert result["price_change"] == -150
    assert result["change_rate"] == Decimal("-0.98")


def test_validate_transformed_data_accepts_valid_item(transformed_item):
    items = [transformed_item]

    assert validate_transformed_data(items) is items


def test_validate_transformed_data_allows_optional_change_values(transformed_item):
    item = {
        **transformed_item,
        "price_change": None,
        "change_rate": None,
    }

    assert validate_transformed_data([item]) == [item]


def test_validate_transformed_data_rejects_missing_required_field(
    transformed_item,
):
    item = {**transformed_item, "item_name": "  "}

    with pytest.raises(ValueError, match="Missing required fields"):
        validate_transformed_data([item])


def test_validate_transformed_data_rejects_duplicate_key(transformed_item):
    duplicate = {**transformed_item, "source_response_id": 2}
    items = [transformed_item, duplicate]

    with pytest.raises(PaginationConsistencyError) as error:
        validate_transformed_data(items)

    message = str(error.value)
    assert "Duplicate stock key detected across API pages" in message
    assert "key=(2023-06-01, KR700088K015)" in message
    assert "source data may have changed during pagination" in message
    assert "Retry the same base date" in message
    assert len(items) == 2


def test_transform_stock_prices_classifies_item_count_mismatch(monkeypatch):
    raw_responses = [
        {
            "response_total_count": 2,
            "payload": {"response": {"body": {"items": {"item": []}}}},
        }
    ]
    monkeypatch.setattr(
        transform_module,
        "fetch_raw_responses",
        lambda conn, run_id, base_date: raw_responses,
    )

    with pytest.raises(PaginationConsistencyError) as error:
        transform_module.transform_stock_prices(
            conn=object(),
            run_id="run-count-mismatch",
            base_date=date(2023, 6, 1),
        )

    message = str(error.value)
    assert "expected=2, actual=0" in message
    assert "Retry the same base date" in message


def test_validate_transformed_data_rejects_negative_non_negative_field(
    transformed_item,
):
    item = {**transformed_item, "trading_volume": -1}

    with pytest.raises(ValueError, match="trading_volume"):
        validate_transformed_data([item])


def test_validate_transformed_data_rejects_high_price_below_low_price(
    transformed_item,
):
    item = {
        **transformed_item,
        "high_price": 100,
        "low_price": 101,
    }

    with pytest.raises(ValueError, match="Invalid price range"):
        validate_transformed_data([item])
