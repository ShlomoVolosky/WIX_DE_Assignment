import streamlit as st
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine
import yaml
import os

def load_config():
    config_path = os.path.join(os.path.dirname(__file__), "..", "src", "config", "config.yaml")
    with open(config_path, "r") as f:
        return yaml.safe_load(f)

def main():
    st.title("Stock & Currency Analysis")

    config = load_config()
    engine_url = config["database"]["engine_url"]
    engine = create_engine(engine_url)

    ticker = st.text_input("Enter Ticker Symbol (e.g., AAPL)", "AAPL")
    date_str = st.date_input("Select Date")
    currency = st.selectbox("Select Currency", ["EUR", "GBP", "USD"])  # Expand as needed

    if st.button("Query Data"):
        with engine.connect() as conn:
            query = f"""
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
            result = pd.read_sql(query, conn)
        if result.empty:
            st.warning("No data found.")
        else:
            st.dataframe(result)

if __name__ == "__main__":
    main()
