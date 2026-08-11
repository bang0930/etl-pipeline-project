import os
import subprocess
import sys
from datetime import date
from pathlib import Path
from unittest.mock import Mock
from uuid import UUID

import pytest
import requests

import extract.extract as extract_module


def test_fetch_stock_price_page_sends_expected_request(
    monkeypatch,
    api_response_factory,
):
    monkeypatch.setenv("STOCK_API_BASE_URL", "https://example.test/api")
    monkeypatch.setenv("STOCK_API_SERVICE_KEY", "test-service-key")
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
        "https://example.test/api/getStockPriceInfo",
        params={
            "serviceKey": "test-service-key",
            "numOfRows": 50,
            "pageNo": 2,
            "resultType": "json",
            "basDt": "20230601",
        },
        timeout=10,
    )


def test_fetch_stock_price_page_converts_http_error(monkeypatch):
    monkeypatch.setenv("STOCK_API_BASE_URL", "https://example.test/api")
    monkeypatch.setenv("STOCK_API_SERVICE_KEY", "test-service-key")
    response = Mock(status_code=503)
    response.raise_for_status.side_effect = requests.HTTPError()
    monkeypatch.setattr(
        extract_module.requests,
        "get",
        Mock(return_value=response),
    )

    with pytest.raises(RuntimeError, match="status=503"):
        extract_module.fetch_stock_price_page("20230601", 1)


def test_fetch_stock_price_page_converts_timeout(monkeypatch):
    monkeypatch.setenv("STOCK_API_BASE_URL", "https://example.test/api")
    monkeypatch.setenv("STOCK_API_SERVICE_KEY", "test-service-key")
    monkeypatch.setattr(
        extract_module.requests,
        "get",
        Mock(side_effect=requests.Timeout("request details must stay hidden")),
    )

    with pytest.raises(RuntimeError) as error:
        extract_module.fetch_stock_price_page("20230601", 3)

    message = str(error.value)
    assert "request timed out" in message
    assert "base_date=20230601" in message
    assert "page_no=3" in message
    assert "timeout=10s" in message
    assert "request details must stay hidden" not in message


def test_fetch_stock_price_page_converts_non_json_response(monkeypatch):
    monkeypatch.setenv("STOCK_API_BASE_URL", "https://example.test/api")
    monkeypatch.setenv("STOCK_API_SERVICE_KEY", "test-service-key")
    response = Mock(
        status_code=200,
        headers={"Content-Type": "application/xml;charset=UTF-8"},
        text=(
            "<OpenAPI_ServiceResponse>"
            "AUTHENTICATION ERROR"
            "</OpenAPI_ServiceResponse>"
        ),
    )
    response.raise_for_status.return_value = None
    response.json.side_effect = requests.exceptions.JSONDecodeError(
        "Expecting value",
        response.text,
        0,
    )
    monkeypatch.setattr(
        extract_module.requests,
        "get",
        Mock(return_value=response),
    )

    with pytest.raises(RuntimeError) as error:
        extract_module.fetch_stock_price_page("20230601", 1)

    message = str(error.value)
    assert "non-JSON response" in message
    assert "status=200" in message
    assert "content_type=application/xml;charset=UTF-8" in message
    assert "AUTHENTICATION ERROR" in message


def test_non_json_response_body_preview_is_limited(monkeypatch):
    monkeypatch.setenv("STOCK_API_BASE_URL", "https://example.test/api")
    monkeypatch.setenv("STOCK_API_SERVICE_KEY", "test-service-key")
    response_body = "x" * 300
    response = Mock(
        status_code=200,
        headers={"Content-Type": "application/xml"},
        text=response_body,
    )
    response.raise_for_status.return_value = None
    response.json.side_effect = requests.exceptions.JSONDecodeError(
        "Expecting value",
        response_body,
        0,
    )
    monkeypatch.setattr(
        extract_module.requests,
        "get",
        Mock(return_value=response),
    )

    with pytest.raises(RuntimeError) as error:
        extract_module.fetch_stock_price_page("20230601", 1)

    assert f"body={'x' * 200}..." in str(error.value)
    assert "x" * 201 not in str(error.value)


def test_extract_module_imports_without_api_environment_variables():
    app_root = Path(__file__).resolve().parents[1] / "app"
    env = os.environ.copy()
    env.pop("STOCK_API_BASE_URL", None)
    env.pop("STOCK_API_SERVICE_KEY", None)

    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "import extract.extract",
        ],
        cwd=app_root,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr


def test_fetch_stock_price_page_rejects_missing_api_environment(monkeypatch):
    monkeypatch.setattr(extract_module, "load_dotenv", Mock())
    monkeypatch.delenv("STOCK_API_BASE_URL", raising=False)
    monkeypatch.delenv("STOCK_API_SERVICE_KEY", raising=False)

    with pytest.raises(RuntimeError) as error:
        extract_module.fetch_stock_price_page("20230601", 1)

    message = str(error.value)
    assert "STOCK_API_BASE_URL" in message
    assert "STOCK_API_SERVICE_KEY" in message


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
