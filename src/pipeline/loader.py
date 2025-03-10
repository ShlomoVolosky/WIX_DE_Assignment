import logging
import pandas as pd
from sqlalchemy import create_engine, text


class DataLoader:
    def __init__(self, engine_url):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.engine = create_engine(engine_url)

    def load_dim_tables(self, df):
        with self.engine.begin() as conn:
            # Prepare dimension data
            date_dim = pd.DataFrame({
                "date_key": df["date"].astype(str),
                "year": df["date"].dt.year,
                "month": df["date"].dt.month,
                "day": df["date"].dt.day
            }).drop_duplicates(subset=["date_key"])

            # Instead of date_dim.to_sql("dim_date", conn, ...)
            # pass self.engine so that Pandas uses an engine-based connectable
            date_dim.to_sql("dim_date", self.engine, if_exists="append", index=False)

            # Similarly for ticker_dim:
            ticker_dim = pd.DataFrame({
                "ticker_symbol": df["ticker"],
                "listing_currency": "USD"
            }).drop_duplicates(subset=["ticker_symbol"])
            ticker_dim.to_sql("dim_ticker", self.engine, if_exists="append", index=False)

            # And currency_dim:
            if "target_currency" in df.columns:
                currency_dim = pd.DataFrame({
                    "currency_code": df["target_currency"]
                }).drop_duplicates(subset=["currency_code"])
                currency_dim.to_sql("dim_currency", self.engine, if_exists="append", index=False)

    def load_fact_stock_prices(self, df, target_currency):
        """
        Insert fact records. Must map date->date_key, ticker->ticker_key, currency->currency_key.
        """
        # For dimension lookups, use self.engine, not conn
        date_map = pd.read_sql("SELECT date_key FROM dim_date", self.engine)
        ticker_map = pd.read_sql("SELECT ticker_key, ticker_symbol FROM dim_ticker", self.engine)
        curr_map = pd.read_sql("SELECT currency_key, currency_code FROM dim_currency", self.engine)

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
            "open_price": df.get("converted_open"),
            "high_price": df.get("converted_high"),
            "low_price": df.get("converted_low"),
            "close_price": df.get("converted_close"),
            "volume": df.get("v"),
            "fx_rate": df.get("rate")
        })

        # Insert the fact table rows
        with self.engine.begin():
            fact.to_sql("fact_stock_prices", self.engine, if_exists="append", index=False)
        self.logger.info(f"Inserted {len(fact)} rows into fact_stock_prices.")