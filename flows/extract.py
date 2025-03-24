import pandas as pd
import requests
from prefect import task, get_run_logger
from prefect.tasks import task_input_hash
from datetime import timedelta
from core import API_BASE_URL, API_KEY, DATE
from flows.notifications import notify_slack

@task(retries=3, retry_delay_seconds=30, log_prints=True, cache_key_fn=task_input_hash, cache_expiration=timedelta(minutes=10))
def extract_api():
    logger = get_run_logger()
    try:
        url = f"{API_BASE_URL}?api_key={API_KEY}&frequency=daily&data[0]=value&start={DATE}"
        response = requests.get(url)
        response.raise_for_status()
        data_records = response.json()['response']['data']

        df = pd.DataFrame([{k: record.get(k, None) for k in [
            'period', 'duoarea', 'area-name', 'product', 'product-name',
            'process', 'process-name', 'series', 'series-description',
            'value', 'units']} for record in data_records])

        if df['value'].isnull().any() or df['units'].isnull().any():
            msg = "Se encontraron registros con 'value' o 'units' nulos."
            logger.warning(msg)
            notify_slack(msg)

        logger.info(f"Extracted {len(df)} rows from API")
        return df
    except Exception as e:
        notify_slack(f"❌ Error API extraction: {e}")
        raise

@task(log_prints=True, cache_key_fn=task_input_hash, cache_expiration=timedelta(minutes=10))
def extract_excel():
    logger = get_run_logger()
    try:
        df_excel = pd.read_excel("data/owid-energy-data.xlsx")
        logger.info(f"Extracted {len(df_excel)} rows from Excel")
        return df_excel
    except Exception as e:
        notify_slack(f"❌ Error Excel extraction: {e}")
        raise