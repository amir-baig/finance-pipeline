import logging

from fastapi import FastAPI

from finance_pipeline.main import run_pipeline

app = FastAPI()


@app.post("/")
def run():
    try:
        run_pipeline()
        message = "Success!"
    except Exception as e:
        logging.critical(f"Pipeline failed! Error:\n {e}")
        message = "Fail"

    return {"message": message}
