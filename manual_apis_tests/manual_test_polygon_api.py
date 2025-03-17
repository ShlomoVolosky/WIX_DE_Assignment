import requests

def test_polygon_dividends():
    # Replace with your actual Polygon.io API key
    POLYGON_API_KEY = "bbjjN0sjtckiubsIzAIPeYISDPShAI3w"

    # Endpoint for dividends
    url = "https://api.polygon.io/v3/reference/dividends"

    # We'll pass the API key as a query parameter:
    params = {
        "apiKey": POLYGON_API_KEY,
    }

    try:
        response = requests.get(url, params=params)
        response.raise_for_status()  # Raise an exception if the request was unsuccessful
        data = response.json()

        # Basic info from the response
        print("Status:", data.get("status"))
        print("Request ID:", data.get("request_id", "N/A"))
        print("Count of results returned:", len(data.get("results", [])))

        # Optionally print the first dividend record
        results = data.get("results", [])
        if results:
            first = results[0]
            print("\n--- First Dividend Record ---")
            print("Ticker:", first.get("ticker"))
            print("Cash Amount:", first.get("cash_amount"))
            print("Ex-Dividend Date:", first.get("ex_dividend_date"))
            print("Pay Date:", first.get("pay_date"))
        else:
            print("No dividend records found.")

    except requests.exceptions.RequestException as e:
        print(f"An error occurred while calling the Polygon API: {e}")


if __name__ == "__main__":
    test_polygon_dividends()
