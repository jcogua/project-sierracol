from prefect import flow, task
import pandas as pd
import requests
import sqlalchemy
import yaml
import os

@task
def extract_excel():
    df = pd.read_excel("data/owid-energy-data.xlsx")
    print(f"✅ Extraídos {len(df)} registros desde Excel.")
    return df

@task
def extract_api():
    api_key = os.getenv("API_KEY")
    url = f"https://api.eia.gov/v2/petroleum/pri/spt/data/?api_key={api_key}&data[0]=value&frequency=daily&start=2023-01-01"
    response = requests.get(url)
    data = response.json()
    df = pd.DataFrame(data['response']['data'])
    print(f"✅ Extraídos {len(df)} registros desde API EIA.")
    return df

@task
def transform_data(df_excel, df_api):
    df_excel_clean = df_excel.dropna(subset=['year', 'country'])
    df_excel_clean = df_excel_clean[df_excel_clean['year'] >= 2000]
    df_api_clean = df_api[['period', 'value']]
    df_api_clean.rename(columns={'period': 'date', 'value': 'price_usd'}, inplace=True)
    print("✅ Transformaciones completadas.")
    return df_excel_clean, df_api_clean

@task
def load_to_postgres(df_excel, df_api):
    db_url = os.getenv("DATABASE_URL")
    engine = sqlalchemy.create_engine(db_url)
    df_excel.to_sql("energy_data", engine, if_exists="replace", index=False)
    df_api.to_sql("petroleum_prices", engine, if_exists="replace", index=False)
    print("✅ Datos cargados en PostgreSQL.")

@flow(name="Petroleum-Energy-Pipeline")
def etl_pipeline():
    df_excel = extract_excel()
    df_api = extract_api()
    df_excel_clean, df_api_clean = transform_data(df_excel, df_api)
    load_to_postgres(df_excel_clean, df_api_clean)

if __name__ == "__main__":
    etl_pipeline()
