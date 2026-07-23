CREATE TABLE IF NOT EXISTS raw.stock_price_api_responses (
    response_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id TEXT NOT NULL,
    requested_base_date DATE NOT NULL,
    page_no INTEGER NOT NULL,
    requested_num_of_rows INTEGER NOT NULL,
    response_total_count INTEGER NOT NULL,
    returned_item_count INTEGER NOT NULL,
    http_status SMALLINT NOT NULL,
    result_code TEXT NOT NULL,
    result_message TEXT NOT NULL,
    collected_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    payload JSONB NOT NULL,

    CONSTRAINT uq_stock_price_api_responses_run_date_page
        UNIQUE (run_id, requested_base_date, page_no),

    CONSTRAINT ck_stock_price_api_responses_page_no
        CHECK (page_no > 0),

    CONSTRAINT ck_stock_price_api_responses_requested_rows
        CHECK (requested_num_of_rows > 0),

    CONSTRAINT ck_stock_price_api_responses_total_count
        CHECK (response_total_count >= 0),

    CONSTRAINT ck_stock_price_api_responses_returned_count
        CHECK (returned_item_count >= 0),

    CONSTRAINT ck_stock_price_api_responses_http_status
        CHECK (http_status BETWEEN 100 AND 599)
);

SELECT
    conname,
    contype,
    pg_get_constraintdef(oid)
FROM pg_constraint
WHERE conrelid = 'raw.stock_price_api_responses'::regclass
ORDER BY conname;