from prefect import flow
from prefect.deployments import Deployment
from prefect.server.schemas.schedules import CronSchedule
from flows.extract import extract_api, extract_excel, extract_geogist
from flows.transform import transform_data
from flows.load import load_to_postgres

@flow(name="Energy-Petroleum-Pipeline")
def etl_pipeline():
    df_api_future = extract_api.submit()
    df_excel_future = extract_excel.submit()
    df_csv_future = extract_geogist.submit()

    df_api = df_api_future.result()
    df_excel = df_excel_future.result()
    df_csv = df_csv_future.result()

    df_api_clean, df_excel_clean = transform_data(df_api, df_excel)
    load_to_postgres(df_api_clean, df_excel_clean, df_csv)

if __name__ == "__main__":
    etl_pipeline.deploy(
        name="prod-deployment",
        work_pool_name="default-agent-pool",
        schedule=CronSchedule(cron="0 2 * * *", timezone="UTC"),
        tags=["prod"]
    )