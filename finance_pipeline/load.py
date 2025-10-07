import logging
from datetime import datetime, timezone

from google.cloud import bigquery, storage

from finance_pipeline.config import PROJECT_ID


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

    logging.info("File written locally.")
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
    logging.info(f"File {source_file_name} uploaded to {destination_blob_name}.")


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
            bigquery.SchemaField("ingestion_time", "STRING"),
        ],
        skip_leading_rows=1,
        # write_disposition="WRITE_TRUNCATE",
        source_format=bigquery.SourceFormat.CSV,
    )
    load_job = client.load_table_from_uri(uri, TABLE_ID, job_config=job_config)

    load_job.result()

    logging.info("Uploaded to BQ")
