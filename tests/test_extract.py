from datetime import date
from unittest.mock import Mock
from uuid import UUID

import pytest
import requests

import extract.extract as extract_module


def test_fetch_stock_price_page_sends_expected_request(
    monkeypatch,
    api_response_factory,
):
    payload = api_response_factory(items=[])
    response = Mock(status_code=200)
    response.raise_for_status.return_value = None
    response.json.return_value = payload
    get_mock = Mock(return_value=response)
    monkeypatch.setattr(extract_module.requests, "get", get_mock)

    status_code, result = extract_module.fetch_stock_price_page(
        base_date="20230601",
        page_no=2,
        num_of_rows=50,
    )

    assert status_code == 200
    assert result == payload
    get_mock.assert_called_once_with(
        f"{extract_module.api_base_url.rstrip('/')}/getStockPriceInfo",
        params={
            "serviceKey": extract_module.service_key,
            "numOfRows": 50,
            "pageNo": 2,
            "resultType": "json",
            "basDt": "20230601",
        },
        timeout=10,
    )


def test_fetch_stock_price_page_converts_http_error(monkeypatch):
    response = Mock(status_code=503)
    response.raise_for_status.side_effect = requests.HTTPError()
    monkeypatch.setattr(
        extract_module.requests,
        "get",
        Mock(return_value=response),
    )

    with pytest.raises(RuntimeError, match="status=503"):
        extract_module.fetch_stock_price_page("20230601", 1)


@pytest.mark.parametrize(
    ("items", "expected_count"),
    [([], 0), ({"isinCd": "KR7005930003"}, 1), ([{}, {}], 2)],
)
def test_build_raw_record_counts_returned_items(
    items,
    expected_count,
    api_response_factory,
):
    payload = api_response_factory(items=items, total_count=2)

    result = extract_module.build_raw_record(
        run_id="run-1",
        base_date=date(2023, 6, 1),
        page_no=1,
        num_of_rows=100,
        status_code=200,
        data=payload,
    )

    assert result["returned_item_count"] == expected_count
    assert result["response_total_count"] == 2
    assert result["payload"] is payload


def test_collect_stock_price_data_builds_raw_record(
    monkeypatch,
    stock_item,
    api_response_factory,
):
    payload = api_response_factory(items=[stock_item])
    fetch_mock = Mock(return_value=(200, payload))
    monkeypatch.setattr(
        extract_module,
        "fetch_stock_price_page",
        fetch_mock,
    )

    result = extract_module.collect_stock_price_data(
        run_id="run-2",
        base_date="20230601",
        page_no=1,
        num_of_rows=100,
    )

    assert result["requested_base_date"] == date(2023, 6, 1)
    assert result["returned_item_count"] == 1
    fetch_mock.assert_called_once_with(
        base_date="20230601",
        page_no=1,
        num_of_rows=100,
    )


def test_collect_stock_price_data_rejects_api_error(
    monkeypatch,
    api_response_factory,
):
    payload = api_response_factory(
        items=[],
        result_code="99",
        result_message="API ERROR",
    )
    monkeypatch.setattr(
        extract_module,
        "fetch_stock_price_page",
        Mock(return_value=(200, payload)),
    )

    with pytest.raises(RuntimeError, match="API ERROR"):
        extract_module.collect_stock_price_data(
            run_id="run-3",
            base_date="20230601",
            page_no=1,
        )


def test_extract_stock_prices_collects_all_pages(monkeypatch):
    collected_pages = []
    saved_pages = []

    def fake_collect(run_id, base_date, page_no, num_of_rows=100):
        collected_pages.append(page_no)
        return {
            "run_id": run_id,
            "requested_base_date": date(2023, 6, 1),
            "page_no": page_no,
            "response_total_count": 201,
        }

    def fake_save(conn, raw_record):
        saved_pages.append(raw_record["page_no"])
        return raw_record["page_no"]

    monkeypatch.setattr(
        extract_module,
        "collect_stock_price_data",
        fake_collect,
    )
    monkeypatch.setattr(
        extract_module,
        "save_raw_response",
        fake_save,
    )

    run_id, requested_base_date = extract_module.extract_stock_prices(
        conn=object(),
        base_date="20230601",
        num_of_rows=100,
    )

    UUID(run_id)
    assert requested_base_date == date(2023, 6, 1)
    assert collected_pages == [1, 2, 3]
    assert saved_pages == [1, 2, 3]


def test_extract_stock_prices_saves_one_page_for_empty_result(monkeypatch):
    collect_mock = Mock(
        return_value={
            "requested_base_date": date(2023, 6, 1),
            "response_total_count": 0,
        }
    )
    save_mock = Mock(return_value=1)
    monkeypatch.setattr(
        extract_module,
        "collect_stock_price_data",
        collect_mock,
    )
    monkeypatch.setattr(
        extract_module,
        "save_raw_response",
        save_mock,
    )

    extract_module.extract_stock_prices(
        conn=object(),
        base_date="20230601",
        num_of_rows=100,
    )

    assert collect_mock.call_count == 1
    assert save_mock.call_count == 1
