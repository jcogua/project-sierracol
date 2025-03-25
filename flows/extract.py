import pandas as pd
import requests
from prefect import task, get_run_logger
from core import API_BASE_URL, API_KEY, DATE
from flows.notifications import notify_slack

@task(log_prints=True)
def extract_api():
    logger = get_run_logger()
    try:
        url = f"{API_BASE_URL}?api_key={API_KEY}&frequency=daily&data[0]=value&start={DATE}"
        response = requests.get(url)
        response.raise_for_status()
        data_records = response.json()['response']['
                                                   ']

        df = pd.DataFrame([{k: record.get(k, None) for k in [
            'period', 'duoarea', 'area-name', 'product', 'product-name',
            'process', 'process-name', 'series', 'series-description',
            'value', 'units']} for record in data_records])

        if df['value'].isnull().any() or df['units'].isnull().any():
            msg = "Found records with null 'value' or 'units'."
            logger.warning(msg)
            notify_slack(msg)

        logger.info(f"Extracted {len(df)} rows from {API_BASE_URL}")
        return df
    except Exception as e:
        notify_slack(f"❌ Error in API extraction: {e}")
        raise

@task(log_prints=True)
def extract_excel():
    logger = get_run_logger()
    try:
        df_excel = pd.read_excel("data/owid-energy-data.xlsx")
        
        
        
        logger.info(f"Extracted {len(df_excel)} rows from owid-energy-data.xlsx")
        return df_excel
    except Exception as e:
        notify_slack(f"❌ Error in Excel extraction: {e}")
        raise

@task(log_prints=True)
def extract_geogist():
    logger = get_run_logger()
    try:
        df_csv = pd.read_csv("data/countries_codes_and_coordinates.csv")
        logger.info(f"Extracted {len(df_csv)} rows from countries_codes_and_coordinates.csv")
        return df_csv
    except Exception as e:
        notify_slack(f"❌ Error in CSV extraction: {e}")
        raise
