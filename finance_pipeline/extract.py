import logging

import requests
from finance_pipeline.utils import get_secret


def query_response(symbol: str, function: str = "TIME_SERIES_DAILY") -> dict:
    """
    Query AlphaVantage API

    Args:
        function (str): AlphaVantage function; ex: `TIME_SERIES_DAILY`
        symbol (str): stock ticker symbol

    Returns:
        data (dict): query data if successful
    """

    API_KEY = get_secret("ALPHAVANTAGE_KEY")

    url = "https://www.alphavantage.co/query"
    # url = "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=IBM&apikey=demo"
    params = {
        "function": function.upper(),
        "symbol": symbol.upper(),
        # "outputsize": "full",
        "apikey": API_KEY,
    }

    r = requests.get(url, params=params)
    # r = requests.get(url)

    if r.status_code == 200:
        return r.json()
    else:
        logging.error("API Response Invalid!")
        raise Exception(f"Error code: {r.status_code}")


if __name__ == "__main__":
    print(query_response("AAPL"))
