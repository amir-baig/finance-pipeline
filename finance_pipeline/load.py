import logging
from datetime import datetime, timezone

from google.cloud import bigquery, storage

from finance_pipeline.config import PROJECT_ID


def to_file(data: dict) -> str:
    symbol = data["Meta Data"]["2. Symbol"]
    filename = f"./data/daily-{symbol}.csv"
    ingestion_time = datetime.now(timezone.utc).strftime("%Y-%m-%filenames %H:%M:%S")

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


def save_files(data: list) -> list[str]:
    filenames = []
    for d in data:
        filenames.append(to_file(d))

    return filenames


def to_blob(
    bucket,
    source_file_name: str,
    destination_blob_name: str,
) -> str | None:
    try:
        blob = bucket.blob(destination_blob_name)
        with open(source_file_name, "rb") as f:
            blob.upload_from_file(f)
        logging.info(f"File {source_file_name} uploaded to {destination_blob_name}.")
        return destination_blob_name
    except:
        logging.error(f"Encountered error loading {source_file_name}")
        return None


def upload_blobs(
    files: list[tuple[str, str]],
    bucket_name: str = "stock-raw",
) -> list[str]:
    storage_client = storage.Client(project=PROJECT_ID)
    bucket = storage_client.bucket(bucket_name)

    filenames = []
    for source, destination in files:
        name = to_blob(bucket, source, destination)
        if name is not None:
            filenames.append(name)

    return filenames


def to_table(filenames: list[str]) -> None:
    TABLE_ID = f"{PROJECT_ID}.financials_dataset.bronze_quotes"
    uris = []
    for name in filenames:
        uris.append(f"gs://stock-raw/{name}")

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
    load_job = client.load_table_from_uri(uris, TABLE_ID, job_config=job_config)

    load_job.result()

    logging.info("Uploaded to BQ")
