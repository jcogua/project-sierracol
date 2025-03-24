import pandas as pd
from prefect import task, get_run_logger
from flows.notifications import notify_slack

@task(log_prints=True)
def transform_data(df_api, df_excel):
    print(" desde transform_data")
    logger = get_run_logger()
    try:
        df_api = df_api.rename(columns={
            'period': 'date', 'area-name': 'area_name', 'product-name': 'product_name',
            'process-name': 'process_name', 'series-description': 'series_description', 'value': 'price_usd_per_unit'})
        df_api['date'] = pd.to_datetime(df_api['date'], errors='coerce')
        df_api['price_usd_per_unit'] = pd.to_numeric(df_api['price_usd_per_unit'], errors='coerce')
        df_api = df_api.dropna(subset=['date', 'price_usd_per_unit']).drop_duplicates(subset=['date', 'product', 'area_name'])
        df_api['year'] = df_api['date'].dt.year
        df_api['month'] = df_api['date'].dt.month
        df_api['week_of_year'] = df_api['date'].dt.isocalendar().week
        df_api['data_source'] = 'EIA_API'
        df_api['batch_run_date'] = pd.Timestamp.now().normalize()

        df_excel = df_excel[df_excel['year'] >= 2000].dropna(subset=['country']).rename(columns=str.lower).drop_duplicates()
        numeric_cols = df_excel.select_dtypes(include=['float64', 'int64']).columns
        df_excel[numeric_cols] = df_excel[numeric_cols].fillna(0)
        df_excel['data_source'] = 'OWID'
        df_excel['batch_run_date'] = pd.Timestamp.now().normalize()

        logger.info("Transformation completed.")
        return df_api, df_excel
    except Exception as e:
        notify_slack(f"❌ Error in transformation: {e}")
        raise
