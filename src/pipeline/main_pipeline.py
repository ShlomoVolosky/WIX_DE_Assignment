import logging
import yaml
import os
import pandas as pd
from datetime import datetime
from polygon_extractor import PolygonExtractor
from frankfurter_extractor import FrankfurterExtractor
from transformer import DataTransformer
from loader import DataLoader

def main_pipeline():
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s"
    )

    # Load config
    config_path = os.path.join(os.path.dirname(__file__), "..", "config", "config.yaml")
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    polygon_cfg = config["polygon"]
    frankfurter_cfg = config["frankfurter"]
    db_cfg = config["database"]
    pipeline_cfg = config["pipeline"]

    # Initialize components
    polygon_ex = PolygonExtractor(api_key=polygon_cfg["api_key"], base_url=polygon_cfg["base_url"])
    frank_ex = FrankfurterExtractor(base_url=frankfurter_cfg["base_url"])
    transformer = DataTransformer()
    loader = DataLoader(engine_url=db_cfg["engine_url"])

    # Step 1: Extract
    ticker = polygon_cfg.get("default_ticker", "AAPL")
    start_date = pipeline_cfg.get("default_start_date", "2023-01-01")
    end_date = pipeline_cfg.get("default_end_date", "2023-12-31")
    df_stock = polygon_ex.get_daily_aggregates(ticker, start_date, end_date)
    df_fx = frank_ex.get_time_series(
        base_currency=frankfurter_cfg.get("default_base", "USD"),
        start_date=start_date,
        end_date=end_date,
        symbols=frankfurter_cfg.get("default_symbols", ["EUR"])
    )

    # Step 2: Transform
    df_stock = transformer.clean_stock_data(df_stock)
    df_fx = transformer.clean_fx_data(df_fx)
    # For demonstration: pick one target currency at a time
    target_currency = "EUR"
    df_merged = transformer.convert_prices(df_stock, df_fx, target_currency)

    # Step 3: Load
    #   3a. Ensure schema
    schema_path = os.path.join(os.path.dirname(__file__), "..", "warehouse", "schema.sql")
    loader.create_schema_if_not_exists(schema_path)

    #   3b. Load dimension tables
    # Convert date column to datetime for dim_date
    df_merged["date"] = pd.to_datetime(df_merged["date"])
    loader.load_dim_tables(df_merged)

    #   3c. Load fact table
    loader.load_fact_stock_prices(df_merged, target_currency=target_currency)

if __name__ == "__main__":
    main_pipeline()

