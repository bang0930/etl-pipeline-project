import os
from datetime import datetime
from uuid import uuid4

import requests
from dotenv import load_dotenv
from psycopg2.extras import Json


API_BASE_URL_ENV = "STOCK_API_BASE_URL"
API_SERVICE_KEY_ENV = "STOCK_API_SERVICE_KEY"
RESPONSE_BODY_PREVIEW_LENGTH = 200
REQUEST_TIMEOUT_SECONDS = 10


def get_stock_api_config():
    """실행 시점에 주식시세 API 설정을 읽고 누락 여부를 확인한다."""
    load_dotenv()

    config = {
        API_BASE_URL_ENV: os.environ.get(API_BASE_URL_ENV),
        API_SERVICE_KEY_ENV: os.environ.get(API_SERVICE_KEY_ENV),
    }
    missing_names = [name for name, value in config.items() if not value]

    if missing_names:
        raise RuntimeError(
            "Missing required stock price API environment variables: "
            + ", ".join(missing_names)
        )

    return config[API_BASE_URL_ENV], config[API_SERVICE_KEY_ENV]


def build_response_body_preview(response):
    """오류 진단에 사용할 응답 본문을 한 줄, 제한된 길이로 반환한다."""
    normalized_body = " ".join(response.text.split())
    body_preview = normalized_body[:RESPONSE_BODY_PREVIEW_LENGTH]

    if len(normalized_body) > RESPONSE_BODY_PREVIEW_LENGTH:
        body_preview += "..."

    return body_preview


def fetch_stock_price_page(base_date, page_no, num_of_rows=100):
    api_base_url, service_key = get_stock_api_config()

    try:
        response = requests.get(
            f"{api_base_url.rstrip('/')}/getStockPriceInfo",
            params={
                "serviceKey": service_key,
                "numOfRows": num_of_rows,
                "pageNo": page_no,
                "resultType": "json",
                "basDt": base_date,
            },
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
    except requests.Timeout:
        # 인증키나 전체 요청 URL은 노출하지 않고 재실행에 필요한
        # 기준일·페이지와 요청 제한시간만 오류에 남긴다.
        raise RuntimeError(
            "Stock price API request timed out: "
            f"base_date={base_date}, "
            f"page_no={page_no}, "
            f"timeout={REQUEST_TIMEOUT_SECONDS}s"
        ) from None

    try:
        response.raise_for_status()
    except requests.HTTPError:
        raise RuntimeError(
            f"Stock price API HTTP error: status={response.status_code}"
        ) from None

    try:
        data = response.json()
    except requests.exceptions.JSONDecodeError:
        content_type = response.headers.get("Content-Type", "unknown")
        body_preview = build_response_body_preview(response)
        raise RuntimeError(
            "Stock price API returned a non-JSON response: "
            f"status={response.status_code}, "
            f"content_type={content_type}, "
            f"body={body_preview}"
        ) from None

    return response.status_code, data

def collect_stock_price_data(run_id, base_date, page_no, num_of_rows=100):
    requested_base_date = datetime.strptime(
        base_date,
        "%Y%m%d",
    ).date()

    api_base_date = requested_base_date.strftime("%Y%m%d")

    status_code, data = fetch_stock_price_page(
        base_date=api_base_date,
        page_no=page_no,
        num_of_rows=num_of_rows,
    )

    header = data["response"]["header"]

    if header["resultCode"] != "00":
        raise RuntimeError(
            f"Stock price API error: {header['resultMsg']}"
        )

    raw_record = build_raw_record(
        run_id=run_id,
        base_date=requested_base_date,
        page_no=page_no,
        num_of_rows=num_of_rows,
        status_code=status_code,
        data=data,
    )

    return raw_record

def build_raw_record(run_id, base_date, page_no, num_of_rows, status_code, data):
    response_data = data["response"]
    header = response_data["header"]
    body = response_data["body"]

    items_container = body.get("items") or {}
    items = items_container.get("item") or []

    if isinstance(items, dict):
        items = [items]

    return {
        "run_id": run_id,
        "requested_base_date": base_date,
        "page_no": page_no,
        "requested_num_of_rows": num_of_rows,
        "response_total_count": int(body["totalCount"]),
        "returned_item_count": len(items),
        "http_status": status_code,
        "result_code": header["resultCode"],
        "result_message": header["resultMsg"],
        "payload": data,
    }

INSERT_RAW_RESPONSE_SQL = """
INSERT INTO raw.stock_price_api_responses (
    run_id,
    requested_base_date,
    page_no,
    requested_num_of_rows,
    response_total_count,
    returned_item_count,
    http_status,
    result_code,
    result_message,
    payload
)
VALUES (
    %s, %s, %s, %s, %s,
    %s, %s, %s, %s, %s
)
ON CONFLICT (
    run_id,
    requested_base_date,
    page_no
)
DO NOTHING
RETURNING response_id
"""

def save_raw_response(conn, raw_record):
    with conn.cursor() as cursor:
        cursor.execute(
            INSERT_RAW_RESPONSE_SQL,
            (
                raw_record["run_id"],
                raw_record["requested_base_date"],
                raw_record["page_no"],
                raw_record["requested_num_of_rows"],
                raw_record["response_total_count"],
                raw_record["returned_item_count"],
                raw_record["http_status"],
                raw_record["result_code"],
                raw_record["result_message"],
                Json(raw_record["payload"]),
            ),
        )

        result = cursor.fetchone()

    return result[0] if result else None

def extract_stock_prices(conn, base_date, num_of_rows=100):
    """기준일의 전체 주식시세 페이지를 수집해 Raw 계층에 저장한다."""
    run_id = str(uuid4())

    # 전체 페이지 수: 첫 페이지 응답의 totalCount를 확인한 뒤 계산
    first_raw_record = collect_stock_price_data(
        run_id=run_id,
        base_date=base_date,
        page_no=1,
        num_of_rows=num_of_rows,
    )

    total_count = first_raw_record["response_total_count"]
    # 조회 결과가 0건: 첫 API 응답은 Raw 수집 이력으로 한 페이지 저장
    total_pages = max(
        1,
        (total_count + num_of_rows - 1) // num_of_rows,
    )

    print(f"run_id: {run_id}")
    print(f"requested_base_date: {first_raw_record['requested_base_date']}")
    print(f"response_total_count: {total_count}")
    print(f"total_pages: {total_pages}")

    saved_page_count = 0

    # 첫 페이지는 totalCount를 확인하는 과정에서 이미 호출 -> 바로 저장
    response_id = save_raw_response(conn, first_raw_record)
    if response_id is not None:
        saved_page_count += 1

    # 첫 페이지의 중복 호출을 피하기 위해 두 번째 페이지부터 수집
    for page_no in range(2, total_pages + 1):
        raw_record = collect_stock_price_data(
            run_id=run_id,
            base_date=base_date,
            page_no=page_no,
            num_of_rows=num_of_rows,
        )
        response_id = save_raw_response(conn, raw_record)

        if response_id is not None:
            saved_page_count += 1

    print(
        f"Raw 저장 준비 완료: saved_pages={saved_page_count}, "
        f"expected_pages={total_pages}"
    )

    # Commit은 이 함수를 호출하는 main.py 또는 Airflow Task가 담당한다.
    return run_id, first_raw_record["requested_base_date"]
