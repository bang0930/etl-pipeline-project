import argparse
import os

import psycopg2
from dotenv import load_dotenv

from extract.extract import extract_stock_prices
from load.load import delete_stock_prices_for_date, load_stock_prices
from mart.mart import (
    build_daily_stock_rankings,
    delete_daily_stock_rankings_for_date,
)
from quality.validators import (
    validate_mart_rankings,
    validate_raw_batch,
    validate_staging_load,
    validate_transformed_items,
)
from transform.transform import fetch_raw_responses, transform_stock_prices


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

            # 저장된 Raw 페이지 전체를 다시 조회하여 페이지 누락과
            # API 메타데이터·payload 건수의 일관성을 확인한다.
            raw_responses = fetch_raw_responses(
                conn=conn,
                run_id=run_id,
                base_date=requested_base_date,
            )
            validate_raw_batch(raw_responses)

            # Raw 저장을 먼저 확정하여 후속 단계 실패 시에도 재사용할 수 있게 한다.
            conn.commit()
            print(f"Extract 및 Raw 품질 검증 완료: run_id={run_id}")
        except Exception:
            conn.rollback()
            raise

        # Transform + Publish: 기준일의 Staging과 Mart를 하나의 트랜잭션에서
        # 교체한다. 실패하면 삭제와 신규 적재가 모두 rollback된다.
        try:
            transformed_items = transform_stock_prices(
                conn=conn,
                run_id=run_id,
                base_date=requested_base_date,
            )

            # 정상 응답이 0건이면 휴장일 또는 원본의 일시적인 빈 결과일 수 있다.
            # 기존 Staging/Mart 스냅샷을 보존하고 Raw 수집 이력만 남긴다.
            if not transformed_items:
                print(
                    "수집 데이터가 0건이므로 기존 Staging/Mart를 유지합니다: "
                    f"base_date={requested_base_date}"
                )
                return

            validate_transformed_items(
                transformed_items,
                expected_base_date=requested_base_date,
            )

            # Mart가 Staging을 FK로 참조하므로 반드시 Mart부터 삭제한다.
            delete_daily_stock_rankings_for_date(conn, requested_base_date)
            delete_stock_prices_for_date(conn, requested_base_date)

            loaded_count = load_stock_prices(
                conn,
                transformed_items,
            )

            # 같은 트랜잭션에서 적재 결과를 확인한다. 검증 실패 시
            # 아래 except에서 Staging과 Mart 변경 전체를 rollback한다.
            validate_staging_load(conn, transformed_items)

            mart_rows = build_daily_stock_rankings(
                conn=conn,
                base_date=requested_base_date,
            )
            validate_mart_rankings(
                mart_rows,
                transformed_items,
                expected_base_date=requested_base_date,
            )

            conn.commit()
            print(
                "Staging/Mart 기준일 스냅샷 교체 완료: "
                f"staging={loaded_count}건, mart={len(mart_rows)}건"
            )
        except Exception:
            conn.rollback()
            raise

    finally:
        conn.close()


if __name__ == "__main__":
    main()
