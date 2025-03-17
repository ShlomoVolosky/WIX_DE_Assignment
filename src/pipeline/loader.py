import logging
import pandas as pd
from sqlalchemy import create_engine, text
import sqlalchemy

class DataLoader:
    def __init__(self, engine_url):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.engine = create_engine(engine_url)

    def create_schema_if_not_exists(self, schema_sql_path):
        self.logger.info(f"Ensuring schema with file: {schema_sql_path}")
        with open(schema_sql_path, "r", encoding="utf-8") as f:
            schema_sql = f.read()

        statements = [stmt.strip() for stmt in schema_sql.split(";") if stmt.strip()]

        # Run statements in a single transaction
        with self.engine.connect() as conn:
            trans = conn.begin()
            try:
                for stmt in statements:
                    conn.execute(text(stmt))
                trans.commit()
                self.logger.info("Schema ensured/created successfully.")
            except Exception as e:
                self.logger.error(f"Error creating schema: {e}")
                trans.rollback()

    def load_dim_tables(self, df):
        """
        Loads dimension tables: dim_date, dim_ticker, dim_currency (if present),
        and tries to skip duplicates in each dimension.
        """
        if df.empty:
            self.logger.warning("Empty DataFrame passed to load_dim_tables. Nothing to load.")
            return

        # Ensure 'date' is proper datetime
        df["date"] = pd.to_datetime(df["date"], errors="coerce")

        # Prepare date_dim
        date_dim = pd.DataFrame({
            "date_key": df["date"].dt.strftime("%Y-%m-%d"),
            "year": df["date"].dt.year,
            "month": df["date"].dt.month,
            "day": df["date"].dt.day
        }).drop_duplicates(subset=["date_key"]).dropna()

        # ticker_dim
        if "ticker" not in df.columns:
            self.logger.error("DataFrame missing 'ticker' column. Cannot build ticker_dim.")
            return
        ticker_dim = pd.DataFrame({
            "ticker_symbol": df["ticker"].astype(str),
            "listing_currency": "USD"
        }).drop_duplicates(subset=["ticker_symbol"]).dropna()

        # currency_dim
        currency_dim = pd.DataFrame()
        if "target_currency" in df.columns:
            currency_dim = pd.DataFrame({
                "currency_code": df["target_currency"].astype(str)
            }).drop_duplicates(subset=["currency_code"]).dropna()

        try:
            # Open a Connection (NOT raw engine) for reading and writing
            with self.engine.connect() as conn:
                # Upsert-like approach for date_dim
                existing_dates = pd.read_sql("SELECT date_key FROM dim_date", conn)
                new_dates = date_dim[~date_dim["date_key"].isin(existing_dates["date_key"])]
                if not new_dates.empty:
                    new_dates.to_sql("dim_date", con=conn, if_exists="append", index=False)

                # Upsert-like approach for ticker_dim
                existing_tickers = pd.read_sql("SELECT ticker_symbol FROM dim_ticker", conn)
                new_tickers = ticker_dim[~ticker_dim["ticker_symbol"].isin(existing_tickers["ticker_symbol"])]
                if not new_tickers.empty:
                    new_tickers.to_sql("dim_ticker", con=conn, if_exists="append", index=False)

                # currency_dim
                if not currency_dim.empty:
                    existing_currencies = pd.read_sql("SELECT currency_code FROM dim_currency", conn)
                    new_curr = currency_dim[~currency_dim["currency_code"].isin(existing_currencies["currency_code"])]
                    if not new_curr.empty:
                        new_curr.to_sql("dim_currency", con=conn, if_exists="append", index=False)

            self.logger.info("Dimension tables loaded successfully (duplicates skipped).")

        except Exception as e:
            self.logger.error(f"Error inserting dimension data: {e}")

    def load_fact_stock_prices(self, df, target_currency):
        """
        Loads the 'fact_stock_prices' table after referencing each dimension table.
        """
        if df.empty:
            self.logger.warning("Empty DataFrame passed to load_fact_stock_prices. Nothing to load.")
            return

        try:
            with self.engine.connect() as conn:
                # Read dimension references
                date_map = pd.read_sql("SELECT date_key FROM dim_date", conn)
                ticker_map = pd.read_sql("SELECT ticker_key, ticker_symbol FROM dim_ticker", conn)
                curr_map = pd.read_sql("SELECT currency_key, currency_code FROM dim_currency", conn)

                # Map the date
                df["date_key_str"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
                df = df.merge(date_map, left_on="date_key_str", right_on="date_key", how="left")

                # Map the ticker
                df = df.merge(ticker_map, left_on="ticker", right_on="ticker_symbol", how="left")

                # Map the currency
                df["currency_code"] = target_currency
                df = df.merge(curr_map, on="currency_code", how="left")

                # Build the fact table
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

                # Insert
                fact.to_sql("fact_stock_prices", con=conn, if_exists="append", index=False)
                self.logger.info(f"Inserted {len(fact)} rows into fact_stock_prices.")

        except Exception as e:
            self.logger.error(f"Error inserting fact data: {e}")
