import pytest
import pandas as pd
from WIX_DE_Assignment.src.pipeline.transformer import DataTransformer

@pytest.fixture
def transformer():
    return DataTransformer()

def test_transformer_clean_stock_data(transformer):
    sample_data = {
        "date": ["2023-01-01", "2023-01-01", None],
        "ticker": ["AAPL", "AAPL", "TSLA"],
        "o": [100, 100, None],
        "h": [110, 110, None],
        "l": [95, 95, None],
        "c": [105, 105, None],
    }
    df = pd.DataFrame(sample_data)
    cleaned = transformer.clean_stock_data(df)
    # The row with None date or None price fields is dropped
    assert len(cleaned) == 2

def test_transformer_clean_fx_data(transformer):
    fx_data = {
        "date": ["2023-01-01", None, "2023-01-02"],
        "base_currency": ["USD", "USD", None],
        "target_currency": ["EUR", "GBP", "EUR"],
        "rate": [0.93, 0.81, 0.94]
    }
    df = pd.DataFrame(fx_data)
    cleaned = transformer.clean_fx_data(df)
    # The row with None date or None target_currency is dropped
    assert len(cleaned) == 1  # Only first row is fully valid

def test_transformer_convert_prices(transformer):
    # Simple scenario: stock in USD, want to convert to EUR
    stock_data = pd.DataFrame({
        "date": pd.to_datetime(["2023-01-01", "2023-01-02"]),
        "ticker": ["AAPL", "AAPL"],
        "o": [100, 200],
        "c": [110, 210],
        "h": [115, 220],
        "l": [90, 180],
        "v": [1000, 2000]
    })
    fx_data = pd.DataFrame({
        "date": pd.to_datetime(["2023-01-01", "2023-01-02"]),
        "base_currency": ["USD", "USD"],
        "target_currency": ["EUR", "EUR"],
        "rate": [0.90, 0.95]
    })

    converted = transformer.convert_prices(stock_data, fx_data, target_currency="EUR")
    # Check sample row:
    row1 = converted.iloc[0]
    assert row1["converted_open"] == 100 * 0.90
    assert row1["converted_close"] == 110 * 0.90

    row2 = converted.iloc[1]
    assert row2["converted_open"] == 200 * 0.95

