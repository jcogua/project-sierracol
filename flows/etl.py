from prefect import flow
from prefect.deployments import Deployment
from prefect.server.schemas.schedules import CronSchedule
from flows.extract import extract_api, extract_excel
from flows.transform import transform_data
from flows.load import load_to_postgres

def build_etl_pipeline():
    @flow(name="Energy-Petroleum-Pipeline")
    def etl_pipeline():
        print("Entró a build_etl_pipeline")
        df_api_future = extract_api.submit()
        df_excel_future = extract_excel.submit()

        df_api = df_api_future.result()
        df_excel = df_excel_future.result()

        df_api_clean, df_excel_clean = transform_data(df_api, df_excel)
        load_to_postgres(df_api_clean, df_excel_clean)

    return etl_pipeline

if __name__ == "__main__":
    etl_pipeline_flow = build_etl_pipeline()
    deployment = Deployment.build_from_flow(
        flow=etl_pipeline_flow,
        name="prod-deployment",
        schedule=CronSchedule(cron="0 2 * * *", timezone="UTC"),
        tags=["prod"]
    )
    deployment.apply()