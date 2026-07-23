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

def save_raw_response(conn, raw_record):
    pass

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



if __name__ == "__main__":
    main()
