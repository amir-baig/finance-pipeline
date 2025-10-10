import logging

import requests
from finance_pipeline.utils import get_secret


def query_response(symbol: str, function: str = "TIME_SERIES_DAILY") -> dict:
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
        logging.error(f"API query failed with code {r.status_code}")
        return None


def run_queries(symbols: list[str], function: str = "TIME_SERIES_DAILY") -> list:
    data = []
    for symbol in symbols:
        response = query_response(symbol, function)
        if response is not None:
            data.append(response)

    return data


if __name__ == "__main__":
    print(query_response("AAPL"))
