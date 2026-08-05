from psycopg2.extras import RealDictCursor


UPSERT_DAILY_STOCK_RANKINGS_SQL = """
WITH metrics AS (
    SELECT
        base_date,
        isin_code,
        short_code,
        item_name,
        market_category,
        open_price,
        close_price,
        close_price - open_price AS intraday_price_change,
        CASE
            WHEN open_price = 0 THEN NULL
            ELSE ROUND(
                (close_price - open_price)::NUMERIC
                / open_price
                * 100,
                6
            )
        END AS intraday_change_rate,
        CASE
            WHEN close_price > open_price THEN 'UP'
            WHEN close_price < open_price THEN 'DOWN'
            ELSE 'FLAT'
        END AS movement_direction,
        trading_volume,
        trading_value
    FROM staging.stock_prices
    WHERE base_date = %s
),
ranked AS (
    SELECT
        metrics.*,
        CASE
            WHEN movement_direction IN ('UP', 'DOWN')
                 AND intraday_change_rate IS NOT NULL
            THEN DENSE_RANK() OVER (
                PARTITION BY base_date, movement_direction
                ORDER BY ABS(intraday_change_rate) DESC
            )
            ELSE NULL
        END AS movement_rank,
        DENSE_RANK() OVER (
            PARTITION BY base_date
            ORDER BY trading_volume DESC
        ) AS trading_volume_rank,
        DENSE_RANK() OVER (
            PARTITION BY base_date
            ORDER BY trading_value DESC
        ) AS trading_value_rank
    FROM metrics
)
INSERT INTO mart.daily_stock_rankings (
    base_date,
    isin_code,
    short_code,
    item_name,
    market_category,
    open_price,
    close_price,
    intraday_price_change,
    intraday_change_rate,
    movement_direction,
    movement_rank,
    trading_volume,
    trading_value,
    trading_volume_rank,
    trading_value_rank
)
SELECT
    base_date,
    isin_code,
    short_code,
    item_name,
    market_category,
    open_price,
    close_price,
    intraday_price_change,
    intraday_change_rate,
    movement_direction,
    movement_rank,
    trading_volume,
    trading_value,
    trading_volume_rank,
    trading_value_rank
FROM ranked
ON CONFLICT (base_date, isin_code)
DO UPDATE SET
    short_code = EXCLUDED.short_code,
    item_name = EXCLUDED.item_name,
    market_category = EXCLUDED.market_category,
    open_price = EXCLUDED.open_price,
    close_price = EXCLUDED.close_price,
    intraday_price_change = EXCLUDED.intraday_price_change,
    intraday_change_rate = EXCLUDED.intraday_change_rate,
    movement_direction = EXCLUDED.movement_direction,
    movement_rank = EXCLUDED.movement_rank,
    trading_volume = EXCLUDED.trading_volume,
    trading_value = EXCLUDED.trading_value,
    trading_volume_rank = EXCLUDED.trading_volume_rank,
    trading_value_rank = EXCLUDED.trading_value_rank,
    calculated_at = NOW()
"""


FETCH_DAILY_STOCK_RANKINGS_SQL = """
SELECT
    base_date,
    isin_code,
    short_code,
    item_name,
    market_category,
    open_price,
    close_price,
    intraday_price_change,
    intraday_change_rate,
    movement_direction,
    movement_rank,
    trading_volume,
    trading_value,
    trading_volume_rank,
    trading_value_rank,
    calculated_at
FROM mart.daily_stock_rankings
WHERE base_date = %s
ORDER BY isin_code
"""


def load_daily_stock_rankings(conn, base_date):
    """한 기준일의 Staging 데이터를 계산해 Mart에 Upsert한다."""
    with conn.cursor() as cursor:
        cursor.execute(
            UPSERT_DAILY_STOCK_RANKINGS_SQL,
            (base_date,),
        )
        loaded_count = cursor.rowcount

    if loaded_count == 0:
        raise ValueError(
            f"No Staging stock prices found: base_date={base_date}"
        )

    return loaded_count


def fetch_daily_stock_rankings(conn, base_date):
    """검증과 조회를 위해 한 기준일의 Mart 결과를 반환한다."""
    with conn.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute(
            FETCH_DAILY_STOCK_RANKINGS_SQL,
            (base_date,),
        )
        rows = cursor.fetchall()

    return rows


def build_daily_stock_rankings(conn, base_date):
    """Mart 적재 후 같은 트랜잭션에서 계산 결과를 조회한다."""
    loaded_count = load_daily_stock_rankings(conn, base_date)
    mart_rows = fetch_daily_stock_rankings(conn, base_date)

    if len(mart_rows) != loaded_count:
        raise ValueError(
            "Mart row count mismatch: "
            f"loaded={loaded_count}, actual={len(mart_rows)}"
        )

    print(f"Mart 적재 및 기본 건수 검증 완료: {loaded_count}건")

    return mart_rows
