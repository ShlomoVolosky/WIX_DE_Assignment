import pytest
import pandas as pd
import tempfile
import os
from sqlalchemy import create_engine
from ..pipeline.loader import DataLoader


@pytest.fixture
def loader():
    # Use an in-memory SQLite DB for demonstration
    engine_url = "sqlite:///:memory:"
    return DataLoader(engine_url)


def test_loader_create_schema_if_not_exists(loader):
    # Create a temp schema file
    schema_content = """
        CREATE TABLE IF NOT EXISTS dim_date (
            date_key TEXT PRIMARY KEY,
            year INT,
            month INT,
            day INT
        );
    """
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp.write(schema_content.encode())
        tmp.flush()
        schema_path = tmp.name

    # Run create_schema
    loader.create_schema_if_not_exists(schema_path)

    # Validate that the table is created
    with loader.engine.connect() as conn:
        result = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='dim_date';").fetchone()
    assert result is not None

    os.remove(schema_path)


def test_loader_load_dim_tables(loader):
    # Create the schema for dims first
    schema_content = """
        CREATE TABLE IF NOT EXISTS dim_date (
            date_key TEXT PRIMARY KEY,
            year INT,
            month INT,
            day INT
        );
        CREATE TABLE IF NOT EXISTS dim_ticker (
            ticker_key INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker_symbol TEXT UNIQUE,
            listing_currency TEXT
        );
        CREATE TABLE IF NOT EXISTS dim_currency (
            currency_key INTEGER PRIMARY KEY AUTOINCREMENT,
            currency_code TEXT UNIQUE
        );
    """
    with loader.engine.connect() as conn:
        for statement in schema_content.split(";"):
            stmt = statement.strip()
            if stmt:
                conn.execute(stmt)

    # Now load dimension data
    df = pd.DataFrame({
        "date": pd.to_datetime(["2023-01-01", "2023-01-02"]),
        "ticker": ["AAPL", "AAPL"],
        "target_currency": ["EUR", "EUR"]
    })
    loader.load_dim_tables(df)

    # Check results
    with loader.engine.connect() as conn:
        # dim_date
        dim_date_rows = conn.execute("SELECT COUNT(*) FROM dim_date").fetchone()[0]
        # dim_ticker
        dim_ticker_rows = conn.execute("SELECT COUNT(*) FROM dim_ticker").fetchone()[0]
        # dim_currency
        dim_currency_rows = conn.execute("SELECT COUNT(*) FROM dim_currency").fetchone()[0]

    assert dim_date_rows == 2
    assert dim_ticker_rows == 1  # Only AAPL
    assert dim_currency_rows == 1  # Only EUR


def test_loader_load_fact_stock_prices(loader):
    # Create minimal schema
    schema_content = """
        CREATE TABLE dim_date (
            date_key TEXT PRIMARY KEY,
            year INT,
            month INT,
            day INT
        );
        CREATE TABLE dim_ticker (
            ticker_key INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker_symbol TEXT UNIQUE,
            listing_currency TEXT
        );
        CREATE TABLE dim_currency (
            currency_key INTEGER PRIMARY KEY AUTOINCREMENT,
            currency_code TEXT UNIQUE
        );
        CREATE TABLE fact_stock_prices (
            fact_id INTEGER PRIMARY KEY AUTOINCREMENT,
            date_key TEXT NOT NULL,
            ticker_key INT NOT NULL,
            currency_key INT NOT NULL,
            open_price REAL,
            high_price REAL,
            low_price REAL,
            close_price REAL,
            volume REAL,
            fx_rate REAL
        );
    """
    with loader.engine.connect() as conn:
        for statement in schema_content.split(";"):
            stmt = statement.strip()
            if stmt:
                conn.execute(stmt)

        # Insert dim_date row
        conn.execute("INSERT INTO dim_date(date_key, year, month, day) VALUES('2023-01-01', 2023, 1, 1)")
        # Insert dim_ticker row
        conn.execute("INSERT INTO dim_ticker(ticker_key, ticker_symbol, listing_currency) VALUES(1, 'AAPL', 'USD')")
        # Insert dim_currency row
        conn.execute("INSERT INTO dim_currency(currency_key, currency_code) VALUES(10, 'EUR')")

    # Prepare a DF
    df = pd.DataFrame({
        "date": [pd.to_datetime("2023-01-01")],
        "ticker": ["AAPL"],
        "converted_open": [90],
        "converted_high": [95],
        "converted_low": [85],
        "converted_close": [92],
        "v": [1000],
        "rate": [0.92]
    })

    loader.load_fact_stock_prices(df, target_currency="EUR")

    # Validate
    with loader.engine.connect() as conn:
        row = conn.execute("SELECT * FROM fact_stock_prices").fetchone()
    assert row is not None
    # e.g. row: (fact_id=1, date_key='2023-01-01', ticker_key=1, currency_key=10, open_price=90, ...)
    assert row["date_key"] == "2023-01-01"
    assert row["ticker_key"] == 1
    assert row["currency_key"] == 10
    assert row["open_price"] == 90
    assert row["fx_rate"] == 0.92

