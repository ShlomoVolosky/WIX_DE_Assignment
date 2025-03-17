# test_frankfurter_extractor.py

import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from requests.exceptions import RequestException
from ..pipeline.frankfurter_extractor import FrankfurterExtractor

@pytest.fixture
def mock_frank_extractor():
    return FrankfurterExtractor(base_url="https://api.frankfurter.dev/v1")

@patch("requests.get")
def test_get_time_series_success(mock_get, mock_frank_extractor):
    mock_resp = MagicMock()
    mock_resp.json.return_value = {
        "base": "USD",
        "rates": {
            "2023-01-01": {"EUR": 0.93, "GBP": 0.82},
            "2023-01-02": {"EUR": 0.94, "GBP": 0.83},
        },
    }
    mock_resp.status_code = 200
    mock_get.return_value = mock_resp
    df = mock_frank_extractor.get_time_series("USD", "2023-01-01", "2023-01-02", ["EUR", "GBP"])
    assert not df.empty
    assert len(df) == 4

@patch("requests.get")
def test_get_time_series_api_error(mock_get, mock_frank_extractor):
    mock_get.side_effect = RequestException("Some network error")
    df = mock_frank_extractor.get_time_series("USD", "2023-01-01", "2023-01-02")
    assert df.empty
