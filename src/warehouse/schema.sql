CREATE TABLE IF NOT EXISTS dim_date (
    date_key       DATE PRIMARY KEY,
    year           INT NOT NULL,
    month          INT NOT NULL,
    day            INT NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_ticker (
    ticker_key     SERIAL PRIMARY KEY,
    ticker_symbol  VARCHAR(20) UNIQUE NOT NULL,
    listing_currency VARCHAR(3) NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_currency (
    currency_key   SERIAL PRIMARY KEY,
    currency_code  VARCHAR(3) UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS fact_stock_prices (
    fact_id        SERIAL PRIMARY KEY,
    date_key       DATE NOT NULL,
    ticker_key     INT NOT NULL,
    currency_key   INT NOT NULL,
    open_price     NUMERIC(10, 4),
    high_price     NUMERIC(10, 4),
    low_price      NUMERIC(10, 4),
    close_price    NUMERIC(10, 4),
    volume         BIGINT,
    fx_rate        NUMERIC(10, 6),
    created_at     TIMESTAMP DEFAULT NOW(),

    CONSTRAINT fk_date
       FOREIGN KEY(date_key) REFERENCES dim_date(date_key),
    CONSTRAINT fk_ticker
       FOREIGN KEY(ticker_key) REFERENCES dim_ticker(ticker_key),
    CONSTRAINT fk_currency
       FOREIGN KEY(currency_key) REFERENCES dim_currency(currency_key)
);

-- Indexes for performance:
CREATE INDEX ON fact_stock_prices (date_key);
CREATE INDEX ON fact_stock_prices (ticker_key);
CREATE INDEX ON fact_stock_prices (currency_key);

