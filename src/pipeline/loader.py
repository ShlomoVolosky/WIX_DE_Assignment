import logging
import pandas as pd
from sqlalchemy import create_engine, text

class DataLoader:
    def __init__(self, engine_url):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.engine = create_engine(engine_url)

    def create_schema_if_not_exists(self, schema_sql_path):
        with open(schema_sql_path, "r", encoding="utf-8") as f:
            schema_sql = f.read()
        raw_conn = self.engine.raw_connection()
        try:
            cursor = raw_conn.cursor()
            for statement in schema_sql.split(";"):
                stmt = statement.strip()
                if stmt:
                    cursor.execute(stmt)
            raw_conn.commit()
        finally:
            raw_conn.close()
        self.logger.info("Schema ensured in database via create_schema_if_not_exists().")

    def load_dim_tables(self, df):
        # Make sure 'date' is datetime
        df["date"] = pd.to_datetime(df["date"], errors="coerce")

        # Prepare date_dim
        date_dim = pd.DataFrame({
            "date_key": df["date"].dt.strftime("%Y-%m-%d"),
            "year": df["date"].dt.year,
            "month": df["date"].dt.month,
            "day": df["date"].dt.day
        }).drop_duplicates(subset=["date_key"])

        # ticker_dim
        ticker_dim = pd.DataFrame({
            "ticker_symbol": df["ticker"],
            "listing_currency": "USD"
        }).drop_duplicates(subset=["ticker_symbol"])

        # optional currency_dim if "target_currency" present
        currency_dim = pd.DataFrame()
        if "target_currency" in df.columns:
            currency_dim = pd.DataFrame({
                "currency_code": df["target_currency"]
            }).drop_duplicates(subset=["currency_code"])

        raw_conn = self.engine.raw_connection()
        try:
            # Insert dimension tables
            date_dim.to_sql("dim_date", con=raw_conn, if_exists="append", index=False)
            ticker_dim.to_sql("dim_ticker", con=raw_conn, if_exists="append", index=False)
            if not currency_dim.empty:
                currency_dim.to_sql("dim_currency", con=raw_conn, if_exists="append", index=False)
            raw_conn.commit()
        finally:
            raw_conn.close()

        self.logger.info("Dimension tables loaded successfully.")

    def load_fact_stock_prices(self, df, target_currency):
        # Gather dimension maps
        raw_conn = self.engine.raw_connection()
        try:
            date_map = pd.read_sql("SELECT date_key FROM dim_date", raw_conn)
            ticker_map = pd.read_sql(
                "SELECT ticker_key, ticker_symbol FROM dim_ticker", raw_conn
            )
            curr_map = pd.read_sql(
                "SELECT currency_key, currency_code FROM dim_currency", raw_conn
            )

            df["date_key_str"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
            df = df.merge(date_map, left_on="date_key_str", right_on="date_key", how="left")
            df = df.merge(ticker_map, left_on="ticker", right_on="ticker_symbol", how="left")
            df["currency_code"] = target_currency
            df = df.merge(curr_map, on="currency_code", how="left")

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

            fact.to_sql("fact_stock_prices", con=raw_conn, if_exists="append", index=False)
            raw_conn.commit()
        finally:
            raw_conn.close()

        self.logger.info(f"Inserted {len(fact)} rows into fact_stock_prices.")
