ALTER TABLE staging.stock_prices
DROP CONSTRAINT ck_stock_prices_short_code_format;

ALTER TABLE staging.stock_prices
ADD CONSTRAINT ck_stock_prices_short_code_format
CHECK (short_code ~ '^[A-Z0-9]{6}$');