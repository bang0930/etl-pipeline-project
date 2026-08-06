from datetime import datetime
from decimal import Decimal

from psycopg2.extras import RealDictCursor

from quality.exceptions import PaginationConsistencyError


FETCH_RAW_RESPONSES_SQL = """
SELECT
    response_id,
    run_id,
    requested_base_date,
    page_no,
    response_total_count,
    returned_item_count,
    payload
FROM raw.stock_price_api_responses
WHERE run_id = %s
  AND requested_base_date = %s
  AND result_code = '00'
ORDER BY page_no
"""

# raw 데이터 조회
def fetch_raw_responses(conn, run_id, base_date):
    with conn.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute(
            FETCH_RAW_RESPONSES_SQL,
            (run_id, base_date),
        )
        rows = cursor.fetchall()

    return rows

# payload에서 item 목록 추출
def extract_items(raw_responses):
    extracted_items = []

    for raw_response in raw_responses:
        payload = raw_response["payload"]

        item_container = (
            (payload.get("response") or {})
            .get("body", {})
            .get("items")
            or {}
        )

        items = item_container.get("item") or []

        # API가 item 한 건을 객체로 반환하는 경우에도 목록으로 통일
        if isinstance(items, dict):
            items = [items]

        for item in items:
            extracted_items.append(
                {
                    **item,
                    "source_response_id": raw_response["response_id"],
                }
            )

    return extracted_items


# 컬럼명 및 타입 변환
def transform_stock_price_items(raw_items):
    transformed_items = []

    for raw_item in raw_items:
        transformed_item = {
            "source_response_id": raw_item["source_response_id"],
            "base_date": parse_date(raw_item.get("basDt")),
            "short_code": raw_item.get("srtnCd"),
            "isin_code": raw_item.get("isinCd"),
            "item_name": raw_item.get("itmsNm"),
            "market_category": raw_item.get("mrktCtg"),
            "close_price": parse_optional_integer(raw_item.get("clpr")),
            "price_change": parse_optional_integer(raw_item.get("vs")),
            "change_rate": parse_optional_decimal(raw_item.get("fltRt")),
            "open_price": parse_optional_integer(raw_item.get("mkp")),
            "high_price": parse_optional_integer(raw_item.get("hipr")),
            "low_price": parse_optional_integer(raw_item.get("lopr")),
            "trading_volume": parse_optional_integer(raw_item.get("trqu")),
            "trading_value": parse_optional_integer(raw_item.get("trPrc")),
            "listed_share_count": parse_optional_integer(
                raw_item.get("lstgStCnt")
            ),
            "market_cap": parse_optional_integer(raw_item.get("mrktTotAmt")),
        }

        transformed_items.append(transformed_item)

    return transformed_items


# 필수값, 중복, 범위 검증
def validate_transformed_data(transformed_items):
    required_fields = [
        "source_response_id",
        "base_date",
        "short_code",
        "isin_code",
        "item_name",
        "market_category",
        "close_price",
        "open_price",
        "high_price",
        "low_price",
        "trading_volume",
        "trading_value",
        "listed_share_count",
        "market_cap",
    ]
    non_negative_fields = [
        "close_price",
        "open_price",
        "high_price",
        "low_price",
        "trading_volume",
        "trading_value",
        "listed_share_count",
        "market_cap",
    ]
    seen_keys = set()

    for item in transformed_items:
        # Staging에서 NOT NULL인 필드가 비어 있는지 확인
        missing_fields = [
            field
            for field in required_fields
            if is_missing(item.get(field))
        ]
        if missing_fields:
            raise ValueError(
                f"Missing required fields {missing_fields} in item: {item}"
            )

        # Staging 기본키와 동일한 기준으로 중복 확인
        item_key = (item["base_date"], item["isin_code"])
        if item_key in seen_keys:
            raise PaginationConsistencyError(
                "Duplicate stock key detected across API pages: "
                f"key=({item['base_date']}, {item['isin_code']})."
            )
        seen_keys.add(item_key)

        for field in non_negative_fields:
            if item[field] < 0:
                raise ValueError(
                    f"Invalid {field} (must be >= 0): {item[field]}"
                )

        if item["high_price"] < item["low_price"]:
            raise ValueError(
                "Invalid price range "
                f"(high_price={item['high_price']}, "
                f"low_price={item['low_price']})"
            )

    return transformed_items


def is_missing(value):
    return value is None or (
        isinstance(value, str)
        and value.strip() == ""
    )


def parse_date(value):
    if is_missing(value):
        return None

    return datetime.strptime(str(value).strip(), "%Y%m%d").date()


def parse_optional_integer(value):
    if is_missing(value):
        return None

    return int(str(value).strip())


# 부동소수점 오차를 피하기 위해 등락률을 Decimal로 변환
def parse_optional_decimal(value):
    if is_missing(value):
        return None

    return Decimal(str(value).strip())

def transform_stock_prices(conn, run_id, base_date):
    """지정한 Raw 실행 데이터를 Staging 적재 형식으로 변환한다."""
    raw_responses = fetch_raw_responses(
        conn,
        run_id,
        base_date,
    )

    if not raw_responses:
        raise ValueError(
            "No Raw responses found: "
            f"run_id={run_id}, base_date={base_date}"
        )

    raw_items = extract_items(raw_responses)
    transformed_items = transform_stock_price_items(raw_items)

    expected_item_count = raw_responses[0]["response_total_count"]
    if len(transformed_items) != expected_item_count:
        raise PaginationConsistencyError(
            "Stock item count mismatch across API pages: "
            f"expected={expected_item_count}, "
            f"actual={len(transformed_items)}."
        )

    print(f"Raw 페이지 수: {len(raw_responses)}")
    print(f"추출 item 수: {len(raw_items)}")
    print(f"변환 item 수: {len(transformed_items)}")

    return transformed_items
