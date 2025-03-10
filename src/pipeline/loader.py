import logging
import pandas as pd
from sqlalchemy import create_engine, text

class DataLoader:
    def __init__(self, engine_url):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.engine = create_engine(engine_url)

    def create_schema_if_not_exists(self, schema_sql_path):
        """
        Execute the schema.sql file to ensure tables exist.
        """
        with open(schema_sql_path, "r") as f:
            schema_sql = f.read()
        with self.engine.connect() as conn:
            for statement in schema_sql.split(";"):
                stmt = statement.strip()
                if stmt:
                    conn.execute(text(stmt))
        self.logger.info("Schema ensured in database.")

    def load_dim_tables(self, df):
        """
        Prepare dim_date, dim_ticker, dim_currency.
        We'll do basic upserts. For large-scale, use official upsert / MERGE in Postgres etc.
        """
        with self.engine.begin() as conn:
            # Prepare dim_date
            date_dim = pd.DataFrame({
                "date_key": df["date"].astype(str),
                "year": df["date"].dt.year,
                "month": df["date"].dt.month,
                "day": df["date"].dt.day
            }).drop_duplicates(subset=["date_key"])
            date_dim.to_sql("dim_date", conn, if_exists="append", index=False)

            # Prepare dim_ticker
            ticker_dim = pd.DataFrame({
                "ticker_symbol": df["ticker"],
                "listing_currency": "USD"
            }).drop_duplicates(subset=["ticker_symbol"])
            ticker_dim.to_sql("dim_ticker", conn, if_exists="append", index=False)

            # Prepare dim_currency
            # We can pick from the 'target_currency' column or a list of unique currencies
            if "target_currency" in df.columns:
                currency_dim = pd.DataFrame({
                    "currency_code": df["target_currency"]
                }).drop_duplicates(subset=["currency_code"])
                currency_dim.to_sql("dim_currency", conn, if_exists="append", index=False)

    def load_fact_stock_prices(self, df, target_currency):
        """
        Insert fact records. Must map date->date_key, ticker->ticker_key, currency->currency_key.
        """
        with self.engine.connect() as conn:
            # Build dimension lookups
            date_map = pd.read_sql("SELECT date_key FROM dim_date", conn)
            ticker_map = pd.read_sql("SELECT ticker_key, ticker_symbol FROM dim_ticker", conn)
            curr_map = pd.read_sql("SELECT currency_key, currency_code FROM dim_currency", conn)

        # merges in memory
        df["date_key_str"] = df["date"].astype(str)
        df = df.merge(date_map, left_on="date_key_str", right_on="date_key", how="left")
        df = df.merge(ticker_map, left_on="ticker", right_on="ticker_symbol", how="left")
        df["currency_code"] = target_currency
        df = df.merge(curr_map, on="currency_code", how="left")

        # Build final fact
        fact = pd.DataFrame({
            "date_key": df["date_key"],
            "ticker_key": df["ticker_key"],
            "currency_key": df["currency_key"],
            "open_price": df["converted_open"],
            "high_price": df["converted_high"],
            "low_price": df["converted_low"],
            "close_price": df["converted_close"],
            "volume": df["v"],
            "fx_rate": df["rate"]
        })

        # Insert into fact
        with self.engine.begin() as conn:
            fact.to_sql("fact_stock_prices", conn, if_exists="append", index=False)
        self.logger.info(f"Inserted {len(fact)} rows into fact_stock_prices.")

