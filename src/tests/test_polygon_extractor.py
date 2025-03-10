import pytest
import pandas as pd
from ..pipeline.polygon_extractor import PolygonExtractor

@pytest.fixture
def polygon_extractor():
    return PolygonExtractor(api_key="dummy", base_url="http://fake_api")

def test_polygon_extractor_empty_response(requests_mock, polygon_extractor):
    # Mock the API call
    requests_mock.get("http://fake_api/aggs/ticker/AAPL/range/1/day/2023-01-01/2023-01-31", json={"status":"NOT_OK"})
    df = polygon_extractor.get_daily_aggregates("AAPL", "2023-01-01", "2023-01-31")
    assert df.empty

def test_polygon_extractor_success(requests_mock, polygon_extractor):
    mock_data = {
        "status": "OK",
        "results": [
            {"t":1672531200000, "o":100, "h":110, "l":99, "c":105, "v":5000, "vw":104}
        ]
    }
    url = "http://fake_api/aggs/ticker/AAPL/range/1/day/2023-01-01/2023-01-31"
    requests_mock.get(url, json=mock_data)
    df = polygon_extractor.get_daily_aggregates("AAPL", "2023-01-01", "2023-01-31")
    assert not df.empty
    assert df.iloc[0]["o"] == 100

