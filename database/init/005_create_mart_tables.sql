CREATE TABLE IF NOT EXISTS mart.daily_stock_rankings (
    -- 한 행의 기준: 기준일자 × 종목
    base_date                DATE          NOT NULL,
    isin_code                VARCHAR(12)   NOT NULL,
    short_code               VARCHAR(6)    NOT NULL,
    item_name                TEXT          NOT NULL,
    market_category          TEXT          NOT NULL,

    -- 장중 가격 변화: 종가 - 시가
    open_price               BIGINT        NOT NULL CHECK (open_price >= 0),
    close_price              BIGINT        NOT NULL CHECK (close_price >= 0),
    intraday_price_change    BIGINT        NOT NULL,
    intraday_change_rate     NUMERIC(18, 6),
    movement_direction      VARCHAR(4)    NOT NULL,
    movement_rank           BIGINT,

    -- 거래 활성도 및 일별 전체 종목 순위
    trading_volume           BIGINT        NOT NULL CHECK (trading_volume >= 0),
    trading_value            BIGINT        NOT NULL CHECK (trading_value >= 0),
    trading_volume_rank      BIGINT        NOT NULL CHECK (trading_volume_rank > 0),
    trading_value_rank       BIGINT        NOT NULL CHECK (trading_value_rank > 0),

    calculated_at            TIMESTAMPTZ   NOT NULL DEFAULT NOW(),

    CONSTRAINT pk_daily_stock_rankings
        PRIMARY KEY (base_date, isin_code),

    -- Mart 행이 어떤 Staging 종목 데이터에서 계산됐는지 보장한다.
    CONSTRAINT fk_daily_stock_rankings_staging_stock_price
        FOREIGN KEY (base_date, isin_code)
        REFERENCES staging.stock_prices (base_date, isin_code),

    CONSTRAINT ck_daily_stock_rankings_short_code_format
        CHECK (short_code ~ '^[A-Z0-9]{6}$'),

    CONSTRAINT ck_daily_stock_rankings_isin_code_format
        CHECK (isin_code ~ '^[A-Z]{2}[A-Z0-9]{9}[0-9]$'),

    CONSTRAINT ck_daily_stock_rankings_price_change
        CHECK (intraday_price_change = close_price - open_price),

    CONSTRAINT ck_daily_stock_rankings_direction
        CHECK (
            (movement_direction = 'UP' AND intraday_price_change > 0)
            OR (movement_direction = 'DOWN' AND intraday_price_change < 0)
            OR (movement_direction = 'FLAT' AND intraday_price_change = 0)
        ),

    -- 시가가 0이면 변동률을 계산할 수 없으므로 NULL만 허용한다.
    CONSTRAINT ck_daily_stock_rankings_change_rate
        CHECK (
            (open_price = 0 AND intraday_change_rate IS NULL)
            OR (open_price > 0 AND intraday_change_rate IS NOT NULL)
        ),

    -- 변동률을 계산할 수 있는 상승·하락 종목만 그룹별 순위를 갖는다.
    -- 보합 또는 시가가 0인 종목은 변동 순위에서 제외한다.
    CONSTRAINT ck_daily_stock_rankings_movement_rank
        CHECK (
            (
                movement_direction IN ('UP', 'DOWN')
                AND intraday_change_rate IS NOT NULL
                AND movement_rank > 0
            )
            OR (
                intraday_change_rate IS NULL
                AND movement_rank IS NULL
            )
            OR (
                movement_direction = 'FLAT'
                AND movement_rank IS NULL
            )
        )
);

-- 날짜별 상승·하락 TOP N 조회
CREATE INDEX IF NOT EXISTS ix_daily_stock_rankings_movement
    ON mart.daily_stock_rankings (
        base_date,
        movement_direction,
        movement_rank
    );

-- 날짜별 거래량 TOP N 조회
CREATE INDEX IF NOT EXISTS ix_daily_stock_rankings_volume
    ON mart.daily_stock_rankings (base_date, trading_volume_rank);

-- 날짜별 거래대금 TOP N 조회
CREATE INDEX IF NOT EXISTS ix_daily_stock_rankings_value
    ON mart.daily_stock_rankings (base_date, trading_value_rank);
