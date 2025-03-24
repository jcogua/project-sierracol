from prefect import flow, task
from prefect.schedules import CronSchedule
from prefect.blocks.notifications import SlackWebhook
import pandas as pd
import requests
from sqlalchemy import create_engine, text
from core import DATABASE_URL, API_KEY, API_BASE_URL, DATE

@task(retries=3, retry_delay_seconds=30)
def extract_api():
    logger = get_run_logger()
    url = f"{API_BASE_URL}?api_key={API_KEY}&frequency=daily"
    response = requests.get(url)
    response.raise_for_status()
    return pd.DataFrame(response.json()["data"])

@task
def transform_data(df_api, df_excel):
    # Normalización de fechas y filtrado
    df_api["date"] = pd.to_datetime(df_api["period"])
    df_api = df_api[df_api["date"] >= "2000-01-01"]
    df_excel = df_excel[df_excel["year"] >= 2000].dropna(subset=["country"])
    return df_api, df_excel

@task
def load_to_postgres(df_api, df_excel):
    engine = create_engine(DATABASE_URL, connect_args={"ssl": ssl.create_default_context(cafile="ca.pem")})
    # Carga incremental (append)
    df_api.to_sql("petroleum_prices", engine, if_exists="append", index=False)
    df_excel.to_sql("energy_metrics", engine, if_exists="append", index=False)
    # Optimización: Índices
    with engine.connect() as conn:
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_energy_country ON energy_metrics (country)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_price_date ON petroleum_prices (date)"))

@flow(name="Energy-Petroleum-Pipeline")
def etl_pipeline():
    df_api = extract_api()
    df_excel = pd.read_excel("data/owid-energy-data.xlsx")
    df_api_clean, df_excel_clean = transform_data(df_api, df_excel)
    load_to_postgres(df_api_clean, df_excel_clean)

# Configuración de alertas en Prefect
def notify_on_failure(flow, flow_run, state):
    if state.is_failed():
        slack_block = SlackWebhook.load("alert-slack")
        slack_block.notify(f"🚨 Pipeline {flow.name} falló: {state.message}")

etl_pipeline.serve(name="prod-deployment", schedule=CronSchedule("0 2 * * *"), on_failure=[notify_on_failure])

if __name__ == "__main__":
    etl_pipeline()
