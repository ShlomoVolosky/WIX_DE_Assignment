import logging
import pandas as pd

class DataTransformer:
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)

    def clean_stock_data(self, df_stock):
        df_stock.dropna(subset=["date", "ticker", "o", "h", "l", "c"], inplace=True)
        return df_stock

    def clean_fx_data(self, df_fx):
        df_fx.dropna(subset=["date", "base_currency", "target_currency", "rate"], inplace=True)
        df_fx.drop_duplicates(inplace=True)
        return df_fx

    def convert_prices(self, df_stock, df_fx, target_currency):
        self.logger.info(f"Converting from USD to {target_currency}")
        # Filter relevant FX
        relevant_fx = df_fx[
            (df_fx["base_currency"] == "USD") &
            (df_fx["target_currency"] == target_currency)
        ].copy()

        merged = pd.merge(df_stock, relevant_fx[["date", "rate"]], on="date", how="left")
        # create converted columns
        merged["converted_open"] = merged["o"] * merged["rate"]
        merged["converted_close"] = merged["c"] * merged["rate"]
        merged["converted_high"] = merged["h"] * merged["rate"]
        merged["converted_low"] = merged["l"] * merged["rate"]

        # fill missing fx rate with 1.0
        merged["rate"].fillna(1.0, inplace=True)
        return merged
