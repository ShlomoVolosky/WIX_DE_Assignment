import logging
import yaml
import os
import pandas as pd
from datetime import datetime
from sqlalchemy import inspect, text
from polygon_extractor import PolygonExtractor
from frankfurter_extractor import FrankfurterExtractor
from transformer import DataTransformer
from loader import DataLoader

def main_pipeline():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s"
    )

    # Load config
    config_path = os.path.join(os.path.dirname(__file__), "..", "config", "config.yaml")
    if not os.path.exists(config_path):
        logging.error(f"Config file not found at: {config_path}")
        return

    with open(config_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    polygon_cfg = config.get("polygon", {})
    frank_cfg = config.get("frankfurter", {})
    db_cfg = config.get("database", {})
    pipeline_cfg = config.get("pipeline", {})

    # Validate required keys
    if not polygon_cfg.get("api_key") or not polygon_cfg.get("base_url"):
        logging.error("Polygon config missing api_key or base_url. Check config.yaml.")
        return
    if not frank_cfg.get("base_url"):
        logging.error("Frankfurter config missing base_url. Check config.yaml.")
        return
    if not db_cfg.get("engine_url"):
        logging.error("Database config missing engine_url. Check config.yaml.")
        return

    # Initialize classes
    polygon_ex = PolygonExtractor(
        api_key=polygon_cfg["api_key"],
        base_url=polygon_cfg["base_url"]
    )
    frank_ex = FrankfurterExtractor(base_url=frank_cfg["base_url"])
    transformer = DataTransformer()
    loader = DataLoader(db_cfg["engine_url"])

    # 1) Extract
    ticker = polygon_cfg.get("default_ticker", "AAPL")
    start_date = pipeline_cfg.get("default_start_date", "2023-01-01")
    end_date   = pipeline_cfg.get("default_end_date", "2023-12-31")

    logging.info(f"Extracting stock data for {ticker} from {start_date} to {end_date}...")
    df_stock = polygon_ex.get_daily_aggregates(ticker, start_date, end_date)
    logging.info(f"Extracting FX data (Frankfurter) from {start_date} to {end_date}...")
    df_fx = frank_ex.get_time_series(
        base_currency=frank_cfg.get("default_base", "USD"),
        start_date=start_date,
        end_date=end_date,
        symbols=frank_cfg.get("default_symbols", ["EUR"])
    )
    if df_fx.empty:
        logging.warning("No FX data returned from Frankfurter. Skipping transformation for FX.")
    else:
        df_fx = transformer.clean_fx_data(df_fx)

    if df_stock.empty:
        logging.warning("No stock data was returned from Polygon. Pipeline will continue but fact table may be empty.")
    if df_fx.empty:
        logging.warning("No FX data was returned from Frankfurter. Currency conversions may be invalid.")

    # 2) Transform
    df_stock = transformer.clean_stock_data(df_stock)
    df_fx = transformer.clean_fx_data(df_fx)
    target_currency = "EUR"  # example; can make it dynamic or from config
    df_merged = transformer.convert_prices(df_stock, df_fx, target_currency)

    # 3) Load
    # Create schema if needed (only once)
    schema_path = os.path.join(os.path.dirname(__file__), "..", "warehouse", "schema.sql")
    if os.path.exists(schema_path):
        loader.create_schema_if_not_exists(schema_path)
    else:
        logging.warning(f"Schema file not found at {schema_path}. Ensure your tables exist or adjust the path.")

    # Load dimension tables
    loader.load_dim_tables(df_merged)
    # Load fact
    loader.load_fact_stock_prices(df_merged, target_currency=target_currency)

    #logging.info("Pipeline completed.")

    ##
    with loader.engine.connect() as conn:
        # Confirm row count
        count = conn.execute(text("SELECT COUNT(*) FROM fact_stock_prices")).fetchone()[0]
        print("DEBUG pipeline: fact_stock_prices count =", count)

    db_path = loader.engine.url.database  # the raw file path part
    abs_path = os.path.abspath(db_path)
    size_bytes = os.path.getsize(abs_path) if os.path.exists(abs_path) else 0
    print("DEBUG pipeline: Using DB at:", abs_path)
    print("DEBUG pipeline: DB file size =", size_bytes, "bytes")
    ##
    logging.info("Pipeline completed.")

if __name__ == "__main__":
    main_pipeline()
