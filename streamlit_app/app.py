import streamlit as st
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine
import yaml
import os

def load_config():
    """
    Loads config.yaml from ../src/config/config.yaml relative to this file.
    """
    # Path: [your_project]/streamlit_app/app.py -> go up one -> src/config/config.yaml
    config_path = os.path.join(
        os.path.dirname(__file__), "..", "src", "config", "config.yaml"
    )
    if not os.path.exists(config_path):
        st.error(f"Config file not found at: {config_path}")
        st.stop()
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def main():
    st.title("Stock & Currency Analysis")

    config = load_config()

    # Get database URL from config
    engine_url = config["database"].get("engine_url")
    if not engine_url:
        st.error("No 'engine_url' found in config['database']. Please check config.yaml.")
        st.stop()

    engine = create_engine(engine_url)

    # Basic user inputs
    ticker = st.text_input("Enter Ticker Symbol (e.g., AAPL)", "AAPL")
    date_str = st.date_input("Select Date")
    currency = st.selectbox("Select Currency", ["EUR", "GBP", "USD"])

    # Query button
    if st.button("Query Data"):
        # Convert date input to string in YYYY-MM-DD
        date_str = date_str.strftime("%Y-%m-%d")
        query = \
            f"""
            SELECT d.date_key, t.ticker_symbol, f.open_price, f.close_price,
                   f.high_price, f.low_price, f.volume, f.fx_rate
            FROM fact_stock_prices f
            JOIN dim_date d      ON f.date_key = d.date_key
            JOIN dim_ticker t    ON f.ticker_key = t.ticker_key
            JOIN dim_currency c  ON f.currency_key = c.currency_key
            WHERE t.ticker_symbol = '{ticker}'
              AND d.date_key = '{date_str}'
              AND c.currency_code = '{currency}'
            """
        try:
            with engine.connect() as conn:
                result = pd.read_sql(query, conn)
        except Exception as e:
            st.error(f"Database query failed: {e}")
            return

        if result.empty:
            st.warning("No data found. Make sure you have run the pipeline to load data.")
        else:
            st.dataframe(result)

if __name__ == "__main__":
    main()
