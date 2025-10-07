from finance_pipeline.logs import start_logger, end_logger

from finance_pipeline.extract import query_response
from finance_pipeline.load import local_save, upload_blob, upload_table


def run_pipeline(symbol: str = None):
    start_logger()

    if symbol is None:
        symbol = "AAPL"  # TODO: Change default behavior
    data = query_response(symbol)
    filepath = local_save(data)
    filename = filepath.split("/")[-1]
    upload_blob(filepath, filename)
    upload_table(filename)

    end_logger()


if __name__ == "__main__":
    run_pipeline()