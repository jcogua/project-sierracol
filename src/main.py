import os

DB_URL = os.getenv("DB_URL")
API_KEY = os.getenv("API_KEY")

from flows.etl_pipeline import run_pipeline

if __name__ == "__main__":
    run_pipeline(DB_URL, API_KEY)