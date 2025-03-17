import requests
import logging
import pandas as pd
from requests.exceptions import RequestException

class FrankfurterExtractor:
    def __init__(self, base_url):
        self.base_url = base_url.rstrip("/")
        self.logger = logging.getLogger(self.__class__.__name__)

    def get_time_series(self, base_currency, start_date, end_date, symbols=None):
        url = f"{self.base_url}/{start_date}..{end_date}"
        params = {"base": base_currency}
        if symbols:
            params["symbols"] = ",".join(symbols)

        self.logger.info(f"Requesting FX data base={base_currency}, {start_date}..{end_date}")
        try:
            resp = requests.get(url, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()
        except RequestException as e:
            self.logger.error(f"Frankfurter API error: {e}")
            return pd.DataFrame()

        # Example structure:
        # {
        #   "base": "USD",
        #   "start_date": "2023-01-01",
        #   "end_date": "2023-12-31",
        #   "rates": {
        #       "2023-01-01": {"EUR": 0.92, "GBP": 0.81},
        #       ...
        #   }
        # }
        records = []
        rates_dict = data.get("rates", {})
        for day_str, fx_dict in rates_dict.items():
            for curr, rate in fx_dict.items():
                records.append({
                    "date": pd.to_datetime(day_str).date(),
                    "base_currency": data.get("base", base_currency),
                    "target_currency": curr,
                    "rate": rate
                })
        return pd.DataFrame(records)
