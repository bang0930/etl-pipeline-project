CREATE TABLE IF NOT EXISTS staging.stock_prices (
    -- 기본 식별 정보
    base_date          DATE        NOT NULL,
    short_code         VARCHAR(6)  NOT NULL,
    isin_code          VARCHAR(12) NOT NULL,
    item_name          TEXT        NOT NULL,
    market_category    TEXT        NOT NULL,

    -- 가격 정보
    close_price        BIGINT      NOT NULL CHECK (close_price >= 0),
    price_change       BIGINT,
    change_rate        NUMERIC,
    open_price         BIGINT      NOT NULL CHECK (open_price >= 0),
    high_price         BIGINT      NOT NULL CHECK (high_price >= 0),
    low_price          BIGINT      NOT NULL CHECK (low_price >= 0),

    -- 거래량 및 거래대금
    trading_volume     BIGINT      NOT NULL CHECK (trading_volume >= 0),
    trading_value      BIGINT      NOT NULL CHECK (trading_value >= 0),

    -- 주식 수 및 시가총액
    listed_share_count BIGINT      NOT NULL CHECK (listed_share_count >= 0),
    market_cap         BIGINT      NOT NULL CHECK (market_cap >= 0),

    -- Raw 출처 추적
    source_response_id BIGINT      NOT NULL,

    -- 처리 시각
    processed_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT ck_stock_prices_short_code_format
        CHECK (short_code ~ '^[0-9]{6}$'),

    CONSTRAINT ck_stock_prices_isin_code_format
        CHECK (isin_code ~ '^[A-Z]{2}[A-Z0-9]{9}[0-9]$'),

    CONSTRAINT pk_stock_prices
        PRIMARY KEY (base_date, isin_code),

    CONSTRAINT fk_stock_prices_source_response
        FOREIGN KEY (source_response_id)
        REFERENCES raw.stock_price_api_responses (response_id),

    CONSTRAINT ck_stock_prices_high_low
        CHECK (high_price >= low_price)
);

CREATE INDEX IF NOT EXISTS ix_stock_prices_source_response_id
    ON staging.stock_prices (source_response_id);


SELECT
    conname,
    contype,
    pg_get_constraintdef(oid)
FROM pg_constraint
WHERE conrelid = 'staging.stock_prices'::regclass
ORDER BY conname;