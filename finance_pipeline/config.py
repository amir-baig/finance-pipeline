from os import getenv
from json import load


PROJECT_ID = getenv("GOOGLE_CLOUD_PROJECT")

with open("./config.json") as f:
    data = load(f)

symbols = data["symbols"]
