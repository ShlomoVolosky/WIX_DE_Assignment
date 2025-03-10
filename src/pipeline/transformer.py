import logging
import pandas as pd

class DataTransformer:
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)

    def clean_stock_data(self, df_stock):
        # Example checks
        df_stock.dropna(subset=["date", "ticker", "o", "h", "l", "c"], inplace=True)
        # Could also do outlier checks, duplicates, etc.
        df_stock.drop_duplicates(subset=["date", "ticker"], keep="last", inplace=True)
        return df_stock

    def clean_fx_data(self, df_fx):
        df_fx.dropna(subset=["date", "base_currency", "target_currency", "rate"], inplace=True)
        df_fx.drop_duplicates(inplace=True)
        return df_fx

    def convert_prices(self, df_stock, df_fx, target_currency):
        """
        Convert from stock's listing currency (assume 'USD' for example) to target_currency using Frankfurter data.
        This function merges on 'date'. Then new columns: converted_open, converted_close, etc.
        """
        self.logger.info(f"Converting stock prices from USD to {target_currency}...")

        # For demonstration, assume base=USD in df_fx. Filter that:
        relevant_fx = df_fx[
            (df_fx["base_currency"] == "USD") &
            (df_fx["target_currency"] == target_currency)
        ]

        merged = pd.merge(
            df_stock,
            relevant_fx[["date", "rate"]],
            how="left",
            on="date"
        )

        # Convert columns
        merged["converted_open"] = merged["o"] * merged["rate"]
        merged["converted_close"] = merged["c"] * merged["rate"]
        merged["converted_high"] = merged["h"] * merged["rate"]
        merged["converted_low"] = merged["l"] * merged["rate"]
        # If rate is null, fill with some default or forward-fill
        merged["rate"].fillna(1.0, inplace=True)
        return merged

