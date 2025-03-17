import logging
import pandas as pd

class DataTransformer:
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)

    def clean_stock_data(self, df_stock):
        required_cols = ["date", "ticker", "o", "h", "l", "c"]
        df_stock.dropna(subset=required_cols, inplace=True)
        return df_stock

    def clean_fx_data(self, df_fx):
        required_cols = ["date", "base_currency", "target_currency", "rate"]
        df_fx.dropna(subset=required_cols, inplace=True)
        df_fx.drop_duplicates(inplace=True)
        return df_fx

    def convert_prices(self, df_stock, df_fx, target_currency):
        """
        Convert stock prices from USD to target_currency using the Frankfurter rates.
        """
        self.logger.info(f"Converting from USD to {target_currency}")

        # Filter relevant FX rows
        relevant_fx = df_fx[
            (df_fx["base_currency"] == "USD") &
            (df_fx["target_currency"] == target_currency)
        ].copy()

        # Merge on date
        merged = pd.merge(df_stock, relevant_fx[["date", "rate"]], on="date", how="left")

        # Create new columns
        merged["converted_open"] = merged["o"] * merged["rate"]
        merged["converted_close"] = merged["c"] * merged["rate"]
        merged["converted_high"] = merged["h"] * merged["rate"]
        merged["converted_low"] = merged["l"] * merged["rate"]

        # Fill any missing rate with 1.0
        merged["rate"] = merged["rate"].fillna(1.0)
        return merged
