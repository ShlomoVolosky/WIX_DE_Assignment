import pytest
from requests.exceptions import RequestException
from ..pipeline.frankfurter_extractor import FrankfurterExtractor

@pytest.fixture
def frank_extractor():
    return FrankfurterExtractor(base_url="http://fake_fx_api")

def test_frankfurter_extractor_empty_rates(requests_mock, frank_extractor):
    # Mock an empty "rates" response
    mock_response = {
        "base": "USD",
        "rates": {}
    }
    requests_mock.get("http://fake_fx_api/2023-01-01..2023-01-31", json=mock_response)

    df = frank_extractor.get_time_series(
        base_currency="USD",
        start_date="2023-01-01",
        end_date="2023-01-31",
        symbols=["EUR", "GBP"]
    )
    assert df.empty

def test_frankfurter_extractor_api_error(requests_mock, frank_extractor):
    requests_mock.get("http://fake_fx_api/2023-01-01..2023-01-31", exc=RequestException("Timeout"))
    df = frank_extractor.get_time_series(
        base_currency="USD",
        start_date="2023-01-01",
        end_date="2023-01-31",
        symbols=["EUR"]
    )
    assert df.empty

def test_frankfurter_extractor_success(requests_mock, frank_extractor):
    mock_response = {
        "base": "USD",
        "rates": {
            "2023-01-01": {"EUR": 0.92, "GBP": 0.81},
            "2023-01-02": {"EUR": 0.93, "GBP": 0.82}
        }
    }
    requests_mock.get("http://fake_fx_api/2023-01-01..2023-01-31", json=mock_response)

    df = frank_extractor.get_time_series("USD", "2023-01-01", "2023-01-31", ["EUR", "GBP"])
    assert not df.empty
    assert len(df) == 4  # 2 dates * 2 currencies
    # Check some values
    row1 = df.iloc[0]
    assert row1["base_currency"] == "USD"
    assert row1["rate"] in (0.92, 0.81)

