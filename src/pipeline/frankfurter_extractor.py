import requests
import logging
import pandas as pd
from requests.exceptions import RequestException

class FrankfurterExtractor:
    def __init__(self, base_url):
        self.base_url = base_url
        self.logger = logging.getLogger(self.__class__.__name__)

    def get_time_series(self, base_currency, start_date, end_date, symbols=None):
        """
        Fetch time series from Frankfurter: /v1/{start_date}..{end_date}?base=XYZ&symbols=ABC,...
        Returns a DataFrame with columns: date, base_currency, target_currency, rate
        """
        # Endpoint: e.g. https://api.frankfurter.dev/v1/2020-01-01..2020-01-31?base=USD&symbols=EUR,GBP
        url = f"{self.base_url}/{start_date}..{end_date}"
        params = {"base": base_currency}
        if symbols:
            params["symbols"] = ",".join(symbols)

        self.logger.info(f"Requesting FX data base={base_currency}, {start_date} to {end_date}")
        try:
            resp = requests.get(url, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()
        except RequestException as e:
            self.logger.error(f"Frankfurter API error: {e}")
            return pd.DataFrame()

        # "rates" => { date_str: { 'EUR': 0.91, 'GBP': 0.77, ... }, ... }
        records = []
        for day_str, fx_dict in data.get("rates", {}).items():
            for curr, rate in fx_dict.items():
                records.append({
                    "date": pd.to_datetime(day_str).date(),
                    "base_currency": data["base"],
                    "target_currency": curr,
                    "rate": rate
                })

        return pd.DataFrame(records)

