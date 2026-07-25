
from psycopg2.extras import execute_values


INSERT_STOCK_PRICE_SQL = """
INSERT INTO staging.stock_prices (
    source_response_id,
    base_date,
    short_code,
    isin_code,
    item_name,
    market_category,
    close_price,
    price_change,
    change_rate,
    open_price,
    high_price,
    low_price,
    trading_volume,
    trading_value,
    listed_share_count,
    market_cap
)
VALUES %s
ON CONFLICT (base_date, isin_code)
DO UPDATE SET
    source_response_id = EXCLUDED.source_response_id,
    short_code = EXCLUDED.short_code,
    item_name = EXCLUDED.item_name,
    market_category = EXCLUDED.market_category,
    close_price = EXCLUDED.close_price,
    price_change = EXCLUDED.price_change,
    change_rate = EXCLUDED.change_rate,
    open_price = EXCLUDED.open_price,
    high_price = EXCLUDED.high_price,
    low_price = EXCLUDED.low_price,
    trading_volume = EXCLUDED.trading_volume,
    trading_value = EXCLUDED.trading_value,
    listed_share_count = EXCLUDED.listed_share_count,
    market_cap = EXCLUDED.market_cap,
    processed_at = NOW()
"""

STOCK_PRICE_VALUES_TEMPLATE = """
(
    %(source_response_id)s,
    %(base_date)s,
    %(short_code)s,
    %(isin_code)s,
    %(item_name)s,
    %(market_category)s,
    %(close_price)s,
    %(price_change)s,
    %(change_rate)s,
    %(open_price)s,
    %(high_price)s,
    %(low_price)s,
    %(trading_volume)s,
    %(trading_value)s,
    %(listed_share_count)s,
    %(market_cap)s
)
"""

def load_stock_prices(conn, transformed_items):
    if not transformed_items:
        return 0

    with conn.cursor() as cursor:
        execute_values(
            cursor,
            INSERT_STOCK_PRICE_SQL,
            transformed_items,
            template=STOCK_PRICE_VALUES_TEMPLATE,
            page_size=1000,
        )

    return len(transformed_items)