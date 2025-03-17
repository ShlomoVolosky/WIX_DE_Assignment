CREATE TABLE IF NOT EXISTS dim_date (
  date_key      TEXT PRIMARY KEY,
  year          INTEGER,
  month         INTEGER,
  day           INTEGER
);

CREATE TABLE IF NOT EXISTS dim_ticker (
  ticker_key    INTEGER PRIMARY KEY AUTOINCREMENT,
  ticker_symbol TEXT UNIQUE,
  listing_currency TEXT
);

CREATE TABLE IF NOT EXISTS dim_currency (
  currency_key    INTEGER PRIMARY KEY AUTOINCREMENT,
  currency_code   TEXT UNIQUE
);

CREATE TABLE IF NOT EXISTS fact_stock_prices (
  fact_key      INTEGER PRIMARY KEY AUTOINCREMENT,
  date_key      TEXT,
  ticker_key    INTEGER,
  currency_key  INTEGER,
  open_price    REAL,
  high_price    REAL,
  low_price     REAL,
  close_price   REAL,
  volume        REAL,
  fx_rate       REAL
);
