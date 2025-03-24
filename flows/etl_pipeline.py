from prefect import flow, task, get_run_logger
from prefect.deployments import Deployment
from prefect.server.schemas.schedules import CronSchedule
from prefect.blocks.notifications import SlackWebhook
import pandas as pd
import requests
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from core import DATABASE_URL, API_KEY, API_BASE_URL

@task(retries=3, retry_delay_seconds=30, log_prints=True)
def extract_api():
    logger = get_run_logger()
    try:
        url = f"{API_BASE_URL}?api_key={API_KEY}&frequency=daily"
        response = requests.get(url)
        response.raise_for_status()
        response_json = response.json()
        logger.info(f"API response keys: {response_json.keys()}")
        if "response" not in response_json or "data" not in response_json["response"]:
            raise ValueError(f"No se encontró la clave 'response.data' en la respuesta: {response_json}")
        else:
            df = pd.DataFrame(response.json()["response"]["data"])
            logger.info(f"Extracted {len(df)} rows from API")
        return df
    except Exception as e:
        logger.error(f"Error during API extraction: {e}")
        raise

@task(log_prints=True)
def extract_excel():
    logger = get_run_logger()
    try:
        df_excel = pd.read_excel("data/owid-energy-data.xlsx")
        logger.info(f"Extracted {len(df_excel)} rows from Excel")
        return df_excel
    except Exception as e:
        logger.error(f"Error reading Excel file: {e}")
        raise

@task(log_prints=True)
def transform_data(df_api, df_excel):
    # API data transformations
    df_api = df_api.rename(columns={'period': 'date', 'value': 'price_usd_per_barrel'})
    df_api["date"] = pd.to_datetime(df_api["date"])
    df_api["price_usd_per_barrel"] = pd.to_numeric(df_api["price_usd_per_barrel"], errors='coerce')
    df_api = df_api[df_api["date"] >= "2014-01-01"]
    df_api = df_api.drop_duplicates(subset=["date"])
    df_api = df_api.sort_values("date").reset_index(drop=True)
    df_api["year"] = df_api["date"].dt.year
    df_api["month"] = df_api["date"].dt.month
    df_api["week_of_year"] = df_api["date"].dt.isocalendar().week
    df_api["data_source"] = "EIA_API"
    df_api["batch_run_date"] = pd.Timestamp.now().normalize()
    
    # Excel data cleaning
    df_excel = df_excel[df_excel["year"] >= 2000].dropna(subset=["country"])
    df_excel = df_excel.rename(columns=str.lower)
    df_excel = df_excel.drop_duplicates()
    numeric_cols = df_excel.select_dtypes(include=['float64', 'int64']).columns
    df_excel[numeric_cols] = df_excel[numeric_cols].fillna(0)
    df_excel["data_source"] = "OWID"
    df_excel["batch_run_date"] = pd.Timestamp.now().normalize()
    return df_api, df_excel

@task(log_prints=True)
def load_to_postgres(df_api, df_excel):
    logger = get_run_logger()
    try:
        engine = create_engine(DATABASE_URL)
        df_api.to_sql("petroleum_prices", engine, if_exists="append", index=False)
        df_excel.to_sql("energy_metrics", engine, if_exists="append", index=False)
        with engine.connect() as conn:
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_energy_country ON energy_metrics (country)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_price_date ON petroleum_prices (date)"))
        logger.info("Data loaded successfully and indexes created.")
    except SQLAlchemyError as e:
        logger.error(f"Database error: {e}")
        raise

@flow(name="Energy-Petroleum-Pipeline")
def etl_pipeline():
    df_api = extract_api()
    df_excel = extract_excel()
    df_api_clean, df_excel_clean = transform_data(df_api, df_excel)
    load_to_postgres(df_api_clean, df_excel_clean)

if __name__ == "__main__":
    deployment = Deployment.build_from_flow(
        flow=etl_pipeline,
        name="prod-deployment",
        schedule=CronSchedule(cron="0 2 * * *", timezone="UTC"),
        tags=["prod"]
    )
    deployment.apply()
