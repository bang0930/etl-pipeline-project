import os

import requests
import psycopg2
from psycopg2.extras import Json
from dotenv import load_dotenv
from uuid import uuid4
from datetime import datetime

load_dotenv()

api_base_url = os.environ["STOCK_API_BASE_URL"]
service_key = os.environ["STOCK_API_SERVICE_KEY"]

def fetch_stock_price_page(base_date, page_no, num_of_rows = 100):
    response = requests.get(
        f"{api_base_url.rstrip('/')}/getStockPriceInfo",
        params={
            "serviceKey": service_key,
            "numOfRows": num_of_rows,
            "pageNo": page_no,
            "resultType": "json",
            "basDt": base_date,
        },
        timeout=10,
    )

    try:
        response.raise_for_status()
    except requests.HTTPError:
        raise RuntimeError(
            f"Stock price API HTTP error: status={response.status_code}"
        ) from None
    
    data = response.json()

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

def main():
    run_id = str(uuid4())
    base_date = "20230601"
    num_of_rows = 100

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

    conn = psycopg2.connect(
        host=os.environ["POSTGRES_HOST"],
        port=os.environ["POSTGRES_PORT"],
        database=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )

    try:
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

        # 모든 페이지 호출과 INSERT가 성공한 경우 이번 실행 전체를 확정
        conn.commit()
        print(
            f"Raw 저장 완료: saved_pages={saved_page_count}, "
            f"expected_pages={total_pages}"
        )
    except Exception:
        # 중간 페이지에서 실패하면 이번 실행에서 INSERT한 모든 페이지를 취소
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
