# test_transformer.py

import pytest
import pandas as pd
from ..pipeline.transformer import DataTransformer

@pytest.fixture
def transformer():
    return DataTransformer()

def test_clean_stock_data(transformer):
    df_stock = pd.DataFrame({
        "date": ["2023-01-01", None, "2023-01-03"],
        "ticker": ["AAPL", "AAPL", None],
        "o": [100, 101, 102],
        "h": [105, 106, 107],
        "l": [99, 100, 101],
        "c": [104, 105, None],
        "v": [1000, 1000, 1000],
    })
    cleaned = transformer.clean_stock_data(df_stock.copy())
    assert len(cleaned) == 1
    assert cleaned.iloc[0]["date"] == "2023-01-01"

def test_clean_fx_data(transformer):
    df_fx = pd.DataFrame({
        "date": ["2023-01-01", "2023-01-02", None],
        "base_currency": ["USD", "USD", "USD"],
        "target_currency": ["EUR", None, "EUR"],
        "rate": [0.93, 0.94, None],
    })
    cleaned = transformer.clean_fx_data(df_fx.copy())
    assert len(cleaned) == 1
    assert cleaned.iloc[0]["target_currency"] == "EUR"

def test_convert_prices(transformer):
    df_stock = pd.DataFrame({
        "date": pd.to_datetime(["2023-01-01", "2023-01-02"]),
        "ticker": ["AAPL", "AAPL"],
        "o": [100, 200],
        "c": [110, 210],
        "h": [120, 220],
        "l": [95, 195],
    })
    df_fx = pd.DataFrame({
        "date": pd.to_datetime(["2023-01-01", "2023-01-02"]),
        "base_currency": ["USD", "USD"],
        "target_currency": ["EUR", "EUR"],
        "rate": [0.9, 0.95],
    })
    converted = transformer.convert_prices(df_stock, df_fx, target_currency="EUR")
    assert "converted_open" in converted.columns
    assert converted.loc[0, "converted_open"] == pytest.approx(90)
    assert converted.loc[1, "converted_close"] == pytest.approx(210 * 0.95)
