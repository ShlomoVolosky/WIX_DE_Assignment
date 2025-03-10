import requests
import logging
import pandas as pd
import datetime
from requests.exceptions import RequestException

class PolygonExtractor:
    def __init__(self, api_key, base_url):
        self.api_key = api_key
        self.base_url = base_url
        self.logger = logging.getLogger(self.__class__.__name__)

    def get_daily_aggregates(self, ticker, start_date, end_date):
        """
        Fetch daily aggregates from Polygon: /v2/aggs/ticker/{ticker}/range/1/day/{start}/{end}
        """
        url = f"{self.base_url}/aggs/ticker/{ticker}/range/1/day/{start_date}/{end_date}"
        params = {
            "adjusted": "true",
            "sort": "asc",
            "limit": 50000,
            "apiKey": self.api_key
        }
        self.logger.info(f"Requesting Polygon data for {ticker}, {start_date} to {end_date}")
        try:
            resp = requests.get(url, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()
        except RequestException as e:
            self.logger.error(f"Polygon API error: {e}")
            return pd.DataFrame()

        if data.get("status") != "OK" or "results" not in data:
            self.logger.warning(f"No results from Polygon for {ticker}.")
            return pd.DataFrame()

        df = pd.DataFrame(data["results"])
        # Convert the timestamp
        df["date"] = pd.to_datetime(df["t"], unit='ms').dt.date
        df["ticker"] = ticker
        # Return relevant columns
        return df[["date", "ticker", "o", "h", "l", "c", "v", "vw"]]

