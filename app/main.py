import os

import psycopg2
import argparse

from dotenv import load_dotenv

from transform.transform import (
    extract_items,
    fetch_raw_responses,
    transform_stock_price_items,
    validate_transformed_data,
)
from load.load import load_stock_prices


load_dotenv()


def main():
    parser = argparse.ArgumentParser(
        description="주식시세 ETL 통합 실행"
    )
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--base-date", required=True)
    args = parser.parse_args()

    run_id = args.run_id
    base_date = args.base_date

    conn = psycopg2.connect(
        host=os.environ["POSTGRES_HOST"],
        port=os.environ["POSTGRES_PORT"],
        database=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )

    try:
        # Transform
        raw_responses = fetch_raw_responses(conn, run_id, base_date)

        if not raw_responses:
            raise ValueError(
                "No Raw responses found: "
                f"run_id={run_id}, base_date={base_date}"
            )
        
        raw_items = extract_items(raw_responses)
        transformed_items = transform_stock_price_items(raw_items)
        validated_items = validate_transformed_data(transformed_items)

        expected_item_count = raw_responses[0]["response_total_count"]

        if len(validated_items) != expected_item_count:
            raise ValueError(
                "Item count mismatch: "
                f"expected={expected_item_count}, "
                f"actual={len(validated_items)}"
            )
        
        print(f"Raw 페이지 수: {len(raw_responses)}")
        print(f"추출 item 수: {len(raw_items)}")
        print(f"검증 item 수: {len(validated_items)}")
        # Load: 이 호출 시점에 실제 INSERT/UPDATE가 실행된다.
        loaded_count = load_stock_prices(conn, validated_items)

        conn.commit()
        print(f"Staging 적재 완료: {loaded_count}건")

    except Exception:
        conn.rollback()
        raise

    finally:
        conn.close()


if __name__ == "__main__":
    main()
    