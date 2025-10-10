from finance_pipeline.logs import start_logger, end_logger

from finance_pipeline.extract import run_queries
from finance_pipeline.load import save_files, upload_blobs, to_table
from finance_pipeline.config import symbols


def run_pipeline():
    start_logger()

    data = run_queries(symbols)
    local_filenames = save_files(data)
    local_paths = [(x, x.split("/")[-1]) for x in local_filenames]
    uploaded_files = upload_blobs(local_paths)
    to_table(uploaded_files)

    end_logger()


if __name__ == "__main__":
    run_pipeline()
