import re
from decimal import Decimal

from psycopg2.extras import RealDictCursor

from transform.transform import validate_transformed_data

from quality.exceptions import DataQualityError, PaginationConsistencyError


SHORT_CODE_PATTERN = re.compile(r"^[A-Z0-9]{6}$")
ISIN_CODE_PATTERN = re.compile(r"^[A-Z]{2}[A-Z0-9]{9}[0-9]$")


FETCH_STAGING_QUALITY_SQL = """
SELECT
    COUNT(*) AS row_count,
    COUNT(DISTINCT isin_code) AS unique_isin_count,
    COUNT(DISTINCT source_response_id) AS source_response_count
FROM staging.stock_prices
WHERE base_date = %s
"""


def _payload_items(payload):
    """API payload의 item을 항상 list 형태로 반환한다."""
    try:
        body = payload["response"]["body"]
    except (KeyError, TypeError) as error:
        raise DataQualityError("Raw payload에 response.body가 없습니다.") from error

    items_container = body.get("items") or {}
    items = items_container.get("item") or []

    if isinstance(items, dict):
        return [items]
    if not isinstance(items, list):
        raise DataQualityError("Raw payload의 items.item이 list 또는 object가 아닙니다.")

    return items


def validate_raw_batch(raw_responses):
    """한 수집 실행의 Raw 페이지가 빠짐없이 일관되게 저장됐는지 검증한다."""
    if not raw_responses:
        raise DataQualityError("Raw 배치가 비어 있습니다.")

    required_fields = {
        "response_id",
        "run_id",
        "requested_base_date",
        "page_no",
        "response_total_count",
        "returned_item_count",
        "payload",
    }

    run_ids = set()
    base_dates = set()
    page_numbers = []
    total_counts = set()
    returned_item_total = 0

    for raw_response in raw_responses:
        missing_fields = required_fields.difference(raw_response)
        if missing_fields:
            raise DataQualityError(
                f"Raw 메타데이터 필드가 누락됐습니다: {sorted(missing_fields)}"
            )

        run_ids.add(raw_response["run_id"])
        base_dates.add(raw_response["requested_base_date"])

        page_no = raw_response["page_no"]
        total_count = raw_response["response_total_count"]
        returned_count = raw_response["returned_item_count"]

        if not isinstance(page_no, int) or page_no <= 0:
            raise DataQualityError(f"유효하지 않은 Raw 페이지 번호입니다: {page_no}")
        if not isinstance(total_count, int) or total_count < 0:
            raise DataQualityError(f"유효하지 않은 전체 item 수입니다: {total_count}")
        if not isinstance(returned_count, int) or returned_count < 0:
            raise DataQualityError(
                f"유효하지 않은 반환 item 수입니다: {returned_count}"
            )

        payload = raw_response["payload"]
        try:
            response = payload["response"]
            header = response["header"]
            body = response["body"]
        except (KeyError, TypeError) as error:
            raise DataQualityError(
                f"Raw {page_no}페이지의 API 응답 구조가 올바르지 않습니다."
            ) from error

        if header.get("resultCode") != "00":
            raise DataQualityError(
                f"Raw {page_no}페이지의 API 결과 코드가 성공이 아닙니다: "
                f"{header.get('resultCode')}"
            )

        payload_items = _payload_items(payload)
        if len(payload_items) != returned_count:
            raise DataQualityError(
                f"Raw {page_no}페이지 item 수가 메타데이터와 다릅니다: "
                f"metadata={returned_count}, payload={len(payload_items)}"
            )

        # DB 메타데이터와 payload 내부의 페이지 정보도 서로 일치해야 한다.
        if "pageNo" in body and int(body["pageNo"]) != page_no:
            raise DataQualityError(
                f"Raw {page_no}페이지의 payload pageNo가 일치하지 않습니다: "
                f"{body['pageNo']}"
            )
        if "totalCount" in body and int(body["totalCount"]) != total_count:
            raise DataQualityError(
                f"Raw {page_no}페이지의 payload totalCount가 일치하지 않습니다: "
                f"{body['totalCount']}"
            )

        page_numbers.append(page_no)
        total_counts.add(total_count)
        returned_item_total += returned_count

    if len(run_ids) != 1:
        raise DataQualityError(f"Raw 배치에 여러 run_id가 섞여 있습니다: {run_ids}")
    if len(base_dates) != 1:
        raise DataQualityError(
            f"Raw 배치에 여러 기준일자가 섞여 있습니다: {base_dates}"
        )
    if len(total_counts) != 1:
        raise PaginationConsistencyError(
            "Raw 페이지별 response_total_count가 다릅니다: "
            f"{total_counts}."
        )

    sorted_pages = sorted(page_numbers)
    expected_pages = list(range(1, sorted_pages[-1] + 1))
    if sorted_pages != expected_pages:
        raise PaginationConsistencyError(
            "Raw 페이지가 중복되거나 누락됐습니다: "
            f"expected={expected_pages}, actual={sorted_pages}."
        )

    expected_item_total = next(iter(total_counts))
    if returned_item_total != expected_item_total:
        raise PaginationConsistencyError(
            "Raw 전체 item 수가 일치하지 않습니다: "
            f"expected={expected_item_total}, actual={returned_item_total}."
        )

    return raw_responses


def validate_transformed_items(transformed_items, expected_base_date=None):
    """기존 행 검증에 코드 형식과 OHLC 관계 등 배치 규칙을 추가한다."""
    if not transformed_items:
        raise DataQualityError("변환 데이터가 비어 있습니다.")

    try:
        validate_transformed_data(transformed_items)
    except PaginationConsistencyError:
        # 재시도 가능한 페이지 정합성 오류 타입을 유지한다.
        raise
    except ValueError as error:
        raise DataQualityError(str(error)) from error

    base_dates = {item["base_date"] for item in transformed_items}
    if len(base_dates) != 1:
        raise DataQualityError(
            f"변환 배치에 여러 기준일자가 섞여 있습니다: {base_dates}"
        )
    if expected_base_date is not None and base_dates != {expected_base_date}:
        raise DataQualityError(
            f"요청 기준일자와 변환 데이터의 기준일자가 다릅니다: "
            f"expected={expected_base_date}, actual={next(iter(base_dates))}"
        )

    for item in transformed_items:
        short_code = item["short_code"]
        isin_code = item["isin_code"]

        if not SHORT_CODE_PATTERN.fullmatch(short_code):
            raise DataQualityError(f"단축코드 형식이 올바르지 않습니다: {short_code}")
        if not ISIN_CODE_PATTERN.fullmatch(isin_code):
            raise DataQualityError(f"ISIN 코드 형식이 올바르지 않습니다: {isin_code}")

    return transformed_items


def validate_staging_load(conn, transformed_items):
    """적재 후 Staging의 행·고유 키·Raw 출처 페이지 수를 검증한다."""
    if not transformed_items:
        raise DataQualityError("Staging 적재 검증 대상이 비어 있습니다.")

    base_dates = {item["base_date"] for item in transformed_items}
    if len(base_dates) != 1:
        raise DataQualityError(
            f"Staging 적재 검증 대상에 여러 기준일자가 섞여 있습니다: {base_dates}"
        )

    base_date = next(iter(base_dates))
    expected_row_count = len(transformed_items)
    expected_source_count = len(
        {item["source_response_id"] for item in transformed_items}
    )

    with conn.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute(FETCH_STAGING_QUALITY_SQL, (base_date,))
        result = cursor.fetchone()

    if result is None:
        raise DataQualityError(f"Staging 조회 결과가 없습니다: base_date={base_date}")

    actual_row_count = result["row_count"]
    actual_unique_count = result["unique_isin_count"]
    actual_source_count = result["source_response_count"]

    if actual_row_count != expected_row_count:
        raise DataQualityError(
            f"Staging 적재 건수가 일치하지 않습니다: "
            f"expected={expected_row_count}, actual={actual_row_count}"
        )
    if actual_unique_count != actual_row_count:
        raise DataQualityError(
            f"Staging ISIN 고유 키 수가 전체 행 수와 다릅니다: "
            f"rows={actual_row_count}, unique={actual_unique_count}"
        )
    if actual_source_count != expected_source_count:
        raise DataQualityError(
            f"Staging Raw 출처 페이지 수가 일치하지 않습니다: "
            f"expected={expected_source_count}, actual={actual_source_count}"
        )

    return result


def _dense_rank_map(values):
    """큰 값부터 동일 값에 같은 순위를 부여하는 매핑을 만든다."""
    return {
        value: rank
        for rank, value in enumerate(
            sorted(set(values), reverse=True),
            start=1,
        )
    }


def validate_mart_rankings(
    mart_rows,
    staging_items,
    expected_base_date=None,
):
    """Mart의 지표·방향·순위와 Staging 출처를 교차 검증한다."""
    if not mart_rows:
        raise DataQualityError("Mart 검증 대상이 비어 있습니다.")
    if len(mart_rows) != len(staging_items):
        raise DataQualityError(
            "Mart와 Staging의 행 수가 다릅니다: "
            f"mart={len(mart_rows)}, staging={len(staging_items)}"
        )

    required_fields = {
        "base_date",
        "isin_code",
        "open_price",
        "close_price",
        "intraday_price_change",
        "intraday_change_rate",
        "movement_direction",
        "movement_rank",
        "trading_volume",
        "trading_value",
        "trading_volume_rank",
        "trading_value_rank",
    }

    mart_keys = set()
    base_dates = set()

    for row in mart_rows:
        missing_fields = required_fields.difference(row)
        if missing_fields:
            raise DataQualityError(
                f"Mart 필드가 누락됐습니다: {sorted(missing_fields)}"
            )

        key = (row["base_date"], row["isin_code"])
        if key in mart_keys:
            raise DataQualityError(f"Mart 중복 키가 존재합니다: {key}")
        mart_keys.add(key)
        base_dates.add(row["base_date"])

        expected_change = row["close_price"] - row["open_price"]
        if row["intraday_price_change"] != expected_change:
            raise DataQualityError(
                f"Mart 장중 변동액이 올바르지 않습니다: isin_code={row['isin_code']}"
            )

        if expected_change > 0:
            expected_direction = "UP"
        elif expected_change < 0:
            expected_direction = "DOWN"
        else:
            expected_direction = "FLAT"

        if row["movement_direction"] != expected_direction:
            raise DataQualityError(
                f"Mart 등락 방향이 올바르지 않습니다: isin_code={row['isin_code']}"
            )

        if row["open_price"] == 0:
            if row["intraday_change_rate"] is not None:
                raise DataQualityError(
                    "시가가 0인 Mart 행의 변동률은 NULL이어야 합니다: "
                    f"isin_code={row['isin_code']}"
                )
        else:
            expected_rate = (
                Decimal(expected_change)
                / Decimal(row["open_price"])
                * Decimal("100")
            )
            actual_rate = row["intraday_change_rate"]
            if actual_rate is None or abs(actual_rate - expected_rate) > Decimal(
                "0.000001"
            ):
                raise DataQualityError(
                    f"Mart 장중 변동률이 올바르지 않습니다: isin_code={row['isin_code']}"
                )

    staging_keys = {
        (item["base_date"], item["isin_code"])
        for item in staging_items
    }
    if mart_keys != staging_keys:
        raise DataQualityError(
            "Mart와 Staging의 종목 키 집합이 일치하지 않습니다."
        )

    if len(base_dates) != 1:
        raise DataQualityError(
            f"Mart 배치에 여러 기준일자가 섞여 있습니다: {base_dates}"
        )
    if expected_base_date is not None and base_dates != {expected_base_date}:
        raise DataQualityError(
            "요청 기준일자와 Mart 기준일자가 다릅니다: "
            f"expected={expected_base_date}, actual={next(iter(base_dates))}"
        )

    volume_ranks = _dense_rank_map(
        row["trading_volume"] for row in mart_rows
    )
    value_ranks = _dense_rank_map(
        row["trading_value"] for row in mart_rows
    )

    movement_ranks = {}
    for direction in ("UP", "DOWN"):
        rates = [
            abs(row["intraday_change_rate"])
            for row in mart_rows
            if row["movement_direction"] == direction
            and row["intraday_change_rate"] is not None
        ]
        movement_ranks[direction] = _dense_rank_map(rates)

    for row in mart_rows:
        if row["trading_volume_rank"] != volume_ranks[row["trading_volume"]]:
            raise DataQualityError(
                f"Mart 거래량 순위가 올바르지 않습니다: isin_code={row['isin_code']}"
            )
        if row["trading_value_rank"] != value_ranks[row["trading_value"]]:
            raise DataQualityError(
                f"Mart 거래대금 순위가 올바르지 않습니다: isin_code={row['isin_code']}"
            )

        rate = row["intraday_change_rate"]
        direction = row["movement_direction"]
        if direction in ("UP", "DOWN") and rate is not None:
            expected_rank = movement_ranks[direction][abs(rate)]
        else:
            expected_rank = None

        if row["movement_rank"] != expected_rank:
            raise DataQualityError(
                f"Mart 등락 순위가 올바르지 않습니다: isin_code={row['isin_code']}"
            )

    return mart_rows
