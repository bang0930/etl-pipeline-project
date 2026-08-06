import sys
from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest


# 애플리케이션이 Docker에서 /app을 기준으로 import되므로,
# 로컬 pytest 실행에서도 같은 모듈 경로를 사용한다.
APP_ROOT = Path(__file__).resolve().parents[1] / "app"
if str(APP_ROOT) not in sys.path:
    sys.path.insert(0, str(APP_ROOT))

@pytest.fixture
def stock_item():
    return {
        "basDt": "20230601",
        "srtnCd": "00088K",
        "isinCd": "KR700088K015",
        "itmsNm": "한화3우B",
        "mrktCtg": "KOSPI",
        "clpr": "15080",
        "vs": "-150",
        "fltRt": "-0.98",
        "mkp": "15240",
        "hipr": "15250",
        "lopr": "15050",
        "trqu": "38219",
        "trPrc": "578006510",
        "lstgStCnt": "22472000",
        "mrktTotAmt": "338877760000",
    }


@pytest.fixture
def api_response_factory():
    def make_response(
        items=None,
        total_count=None,
        result_code="00",
        result_message="NORMAL SERVICE.",
    ):
        if items is None:
            items = []

        if total_count is None:
            total_count = 1 if isinstance(items, dict) else len(items)

        return {
            "response": {
                "header": {
                    "resultCode": result_code,
                    "resultMsg": result_message,
                },
                "body": {
                    "numOfRows": "100",
                    "pageNo": "1",
                    "totalCount": str(total_count),
                    "items": {"item": items},
                },
            }
        }

    return make_response


@pytest.fixture
def transformed_item():
    return {
        "source_response_id": 1,
        "base_date": date(2023, 6, 1),
        "short_code": "00088K",
        "isin_code": "KR700088K015",
        "item_name": "한화3우B",
        "market_category": "KOSPI",
        "close_price": 15080,
        "price_change": -150,
        "change_rate": Decimal("-0.98"),
        "open_price": 15240,
        "high_price": 15250,
        "low_price": 15050,
        "trading_volume": 38219,
        "trading_value": 578006510,
        "listed_share_count": 22472000,
        "market_cap": 338877760000,
    }
