# test_polygon_extractor.py

import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from requests.exceptions import RequestException
from ..pipeline.polygon_extractor import PolygonExtractor

@pytest.fixture
def mock_polygon_extractor():
    return PolygonExtractor(api_key="fake_api_key", base_url="https://api.polygon.io/v2")

@patch("requests.get")
def test_get_daily_aggregates_success(mock_get, mock_polygon_extractor):
    mock_response = MagicMock()
    mock_response.json.return_value = {
        "status": "OK",
        "results": [
            {"t": 1672531200000, "o": 100, "h": 105, "l": 99, "c": 104, "v": 5000, "vw": 102},
            {"t": 1672617600000, "o": 104, "h": 110, "l": 101, "c": 108, "v": 6000, "vw": 105},
        ],
    }
    mock_response.status_code = 200
    mock_get.return_value = mock_response
    df = mock_polygon_extractor.get_daily_aggregates("AAPL", "2023-01-01", "2023-01-02")
    assert not df.empty
    assert len(df) == 2

@patch("requests.get")
def test_get_daily_aggregates_error(mock_get, mock_polygon_extractor):
    mock_get.side_effect = RequestException("Bad request")
    df = mock_polygon_extractor.get_daily_aggregates("AAPL", "2023-01-01", "2023-01-02")
    assert df.empty
