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
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s [%(name)s] %(message)s")

    # load config
    config_path = os.path.join(os.path.dirname(__file__), "..", "config", "config.yaml")
    with open(config_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    polygon_cfg = config["polygon"]
    frank_cfg = config["frankfurter"]
    db_cfg = config["database"]
    pipeline_cfg = config["pipeline"]

    polygon_ex = PolygonExtractor(api_key=polygon_cfg["api_key"],
                                  base_url=polygon_cfg["base_url"])
    frank_ex = FrankfurterExtractor(base_url=frank_cfg["base_url"])
    transformer = DataTransformer()
    loader = DataLoader(db_cfg["engine_url"])

    # 1) EXTRACT
    ticker = polygon_cfg.get("default_ticker", "AAPL")
    start_date = pipeline_cfg.get("default_start_date", "2023-01-01")
    end_date   = pipeline_cfg.get("default_end_date", "2023-12-31")

    df_stock = polygon_ex.get_daily_aggregates(ticker, start_date, end_date)
    df_fx = frank_ex.get_time_series(
        base_currency=frank_cfg.get("default_base", "USD"),
        start_date=start_date,
        end_date=end_date,
        symbols=frank_cfg.get("default_symbols", ["EUR"])
    )

    # 2) TRANSFORM
    df_stock = transformer.clean_stock_data(df_stock)
    df_fx = transformer.clean_fx_data(df_fx)
    target_currency = "EUR"  # example
    df_merged = transformer.convert_prices(df_stock, df_fx, target_currency)

    # 3) LOAD
    # If you want to run a schema creation once, do e.g.:
    # loader.create_schema_if_not_exists(os.path.join(..., "schema.sql"))

    loader.load_dim_tables(df_merged)
    loader.load_fact_stock_prices(df_merged, target_currency=target_currency)

    logging.info("Pipeline completed.")
