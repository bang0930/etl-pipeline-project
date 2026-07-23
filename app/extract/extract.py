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

def collect_stock_price_data(run_id, base_date, num_of_rows=100):
    requested_base_date = datetime.strptime(
        base_date,
        "%Y%m%d",
    ).date()

    api_base_date = requested_base_date.strftime("%Y%m%d")

    page_no = 1

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

    raw_record = collect_stock_price_data(
        run_id=run_id,
        base_date=base_date,
        num_of_rows=100,
    )

    for key, value in raw_record.items():
        if key != "payload":
            print(f"{key}: {value}")

    conn = psycopg2.connect(
            host=os.environ["POSTGRES_HOST"],
            port=os.environ["POSTGRES_PORT"],
            database=os.environ["POSTGRES_DB"],
            user=os.environ["POSTGRES_USER"],
            password=os.environ["POSTGRES_PASSWORD"],
        )

    try:
        response_id = save_raw_response(conn, raw_record)
        conn.commit()

        if response_id is None:
            print("이미 저장된 Raw 응답입니다.")
        else:
            print(f"Raw 저장 완료: response_id={response_id}")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
