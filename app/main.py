import argparse
import os

import psycopg2
from dotenv import load_dotenv

from extract.extract import extract_stock_prices
from load.load import load_stock_prices
from transform.transform import transform_stock_prices


load_dotenv()


def create_database_connection():
    return psycopg2.connect(
        host=os.environ["POSTGRES_HOST"],
        port=os.environ["POSTGRES_PORT"],
        database=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )


def main():
    parser = argparse.ArgumentParser(
        description="주식시세 ETL 통합 실행"
    )
    parser.add_argument(
        "--base-date",
        required=True,
        help="수집 기준일자 (예: 20230601)",
    )
    parser.add_argument(
        "--num-of-rows",
        type=int,
        default=100,
        help="API 페이지당 요청 건수 (기본값: 100)",
    )
    args = parser.parse_args()

    if args.num_of_rows <= 0:
        parser.error("--num-of-rows는 1 이상이어야 합니다.")

    conn = create_database_connection()

    try:
        # Extract: API 전체 페이지를 수집하여 Raw 계층에 저장한다.
        try:
            run_id, requested_base_date = extract_stock_prices(
                conn=conn,
                base_date=args.base_date,
                num_of_rows=args.num_of_rows,
            )
            # Raw 저장을 먼저 확정하여 후속 단계 실패 시에도 재사용할 수 있게 한다.
            conn.commit()
            print(f"Extract 완료: run_id={run_id}")
        except Exception:
            conn.rollback()
            raise

        # Transform + Load: 방금 수집한 Raw 데이터를 변환하여 Staging에 적재한다.
        try:
            validated_items = transform_stock_prices(
                conn=conn,
                run_id=run_id,
                base_date=requested_base_date,
            )
            loaded_count = load_stock_prices(
                conn,
                validated_items,
            )
            conn.commit()
            print(f"Staging 적재 완료: {loaded_count}건")
        except Exception:
            conn.rollback()
            raise

    finally:
        conn.close()


if __name__ == "__main__":
    main()
