import requests

def test_frankfurter_api():
    url = "https://api.frankfurter.dev/v1/latest"

    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        # Print the base currency, date, and a sample of the rates
        print("Base currency:", data["base"])
        print("Date:", data["date"])

        rates = data.get("rates", {})
        # Print just a few rates
        print("Some rates:")
        for currency, rate in list(rates.items())[:5]:
            print(f"  {currency}: {rate}")

    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    test_frankfurter_api()

