import google.cloud.logging
import logging
from datetime import datetime


client = google.cloud.logging.Client()

def start_logger():
    logging.basicConfig(
        filename=f"./logs/{datetime.now().strftime('%Y%m%d%H%M%S')}.log",
        filemode="x",
        encoding="utf-8",
        format="{asctime} - {levelname} - {message}",
        style="{",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    logging.info("Started")
    client.setup_logging()
    logging.info("Cloud logging started")

def end_logger():
    client.close()
