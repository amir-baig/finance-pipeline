import requests
import sys
from datetime import datetime, timezone
from google.cloud import storage, bigquery, secretmanager

PROJECT_ID = "melodic-zoo-471022-r6"

def get_secret(secret_name: str) -> str:
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{PROJECT_ID}/secrets/{secret_name}/versions/latest"
    return client.access_secret_version(request={"name": name}).payload.data.decode("UTF-8")

def query_response(function: str, symbol: str) -> dict:
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
        "apikey": API_KEY
    }

    r = requests.get(url, params=params)
    # r = requests.get(url)

    if r.status_code == 200:
        return r.json()
    else:
        raise Exception(f"Error code: {r.status_code}")

def local_save(data: dict) -> str:
    """
    Flatten query and save to CSV
    Returns filepath
    """
    symbol = data["Meta Data"]["2. Symbol"]
    filename = f"./data/daily-{symbol}.csv"
    ingestion_time = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    with open(filename, "w") as f:
        header = "symbol,timestamp,open,high,low,close,volume,ingestion_time\n"
        f.write(header)
        for ts, vals in data["Time Series (Daily)"].items():
            ohlcv = (
                f"{ts},"
                f"{vals['1. open']},"
                f"{vals['2. high']},"
                f"{vals['3. low']},"
                f"{vals['4. close']},"
                f"{vals['5. volume']}"
            )
            line = f"{symbol},{ohlcv},{ingestion_time}\n"
            f.write(line)
    
    print("File written locally.")
    return filename

def upload_blob(
    source_file_name: str, 
    destination_blob_name: str,
    bucket_name: str = "stock-raw", 
) -> None:
    storage_client = storage.Client(project=PROJECT_ID)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    with open(source_file_name, "rb") as f:
        blob.upload_from_file(f)
    print(f"File {source_file_name} uploaded to {destination_blob_name}.")

def upload_table(filename: str) -> None:
    TABLE_ID = f"{PROJECT_ID}.financials_dataset.bronze_quotes"
    uri = f"gs://stock-raw/{filename}"

    client = bigquery.Client(project=PROJECT_ID)
    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("symbol", "STRING"),
            bigquery.SchemaField("timestamp", "STRING"),
            bigquery.SchemaField("open", "FLOAT"),
            bigquery.SchemaField("high", "FLOAT"),
            bigquery.SchemaField("low", "FLOAT"),
            bigquery.SchemaField("close", "FLOAT"),
            bigquery.SchemaField("volume", "INT64"),
            bigquery.SchemaField("ingestion_time", "STRING")
        ],
        skip_leading_rows=1,
        # write_disposition="WRITE_TRUNCATE",
        source_format=bigquery.SourceFormat.CSV
    )
    load_job = client.load_table_from_uri(uri, TABLE_ID, job_config=job_config)

    load_job.result()

    print("Uploaded to BQ")


def run_job(function, symbol):
    print(f"=== Running job for {symbol} ===")
    data = query_response(function, symbol)
    filepath = local_save(data)
    filename = filepath.split("/")[-1]
    upload_blob(filepath, filename, )
    upload_table(filename)
    print("===== Complete =====\n")

def main():
    if len(sys.argv) > 1:
        function = sys.argv[1]
        symbols = sys.argv[2:]
    else:
        function = "TIME_SERIES_DAILY"
        symbols = ["TSLA"]

    for symbol in symbols:
        run_job(function, symbol)


if __name__ == "__main__":
    main()