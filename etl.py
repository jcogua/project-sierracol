from prefect import flow
from prefect.deployments import Deployment
from prefect.server.schemas.schedules import CronSchedule
from flows.extract import extract_api, extract_excel, extract_geogist
from flows.transform import transform_api_data, transform_excel_data, transform_csv_data
from flows.load import load_to_postgres


# ETL
@flow(name="Energy-Petroleum-Pipeline")
def etl_pipeline():
    df_api_future = extract_api.submit()
    df_excel_future = extract_excel.submit()
    df_csv_future = extract_geogist.submit()

    df_api = df_api_future.result()
    df_excel = df_excel_future.result()
    df_csv = df_csv_future.result()
    
    df_api_clean = transform_api_data(df_api)
    df_excel_clean = transform_excel_data(df_excel)
    df_csv_clean = transform_csv_data(df_csv)
    
    load_to_postgres(df_api_clean, df_excel_clean, df_csv_clean)

if __name__ == "__main__":
    etl_pipeline()