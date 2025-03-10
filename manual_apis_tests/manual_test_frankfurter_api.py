import requests
import pandas as pd

base_currency = "USD"
start_date = "2023-01-01"
end_date = "2023-01-31"
symbols = ["EUR", "GBP"]

url = f"https://api.frankfurter.app/{start_date}..{end_date}"
params = {"base": base_currency}
if symbols:
    params["symbols"] = ",".join(symbols)


