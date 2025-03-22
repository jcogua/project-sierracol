from prefect import flow, task
import pandas as pd
import requests
import sqlalchemy
from core import DATABASE_URL, API_KEY, API_BASE_URL, DATE

# Validar que las variables de entorno estén cargadas
if not DATABASE_URL:
    raise EnvironmentError("DATABASE_URL no está definido. Verifica las variables de entorno.")
if not API_KEY:
    raise EnvironmentError("API_KEY no está definido. Verifica las variables de entorno.")

@task
def extract_excel():
    df = pd.read_excel("data/owid-energy-data.xlsx")
    print(f"✅ Extraídos {len(df)} registros desde Excel.")
    return df

@task
def extract_api():
    url = f"{API_BASE_URL}?api_key={API_KEY}&data[0]=value&frequency=daily&start={DATE}"
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()
    df = pd.DataFrame(data['response']['data'])
    print(f"✅ Extraídos {len(df)} registros desde API EIA.")
    return df

@task
def transform_data(df_excel, df_api):
    df_excel_clean = df_excel.dropna(subset=['year', 'country'])
    df_excel_clean = df_excel_clean[df_excel_clean['year'] >= 2000]
    df_api_clean = df_api[['period', 'value']].rename(columns={'period': 'date', 'value': 'price_usd'})
    print("✅ Transformaciones completadas.")
    return df_excel_clean, df_api_clean

@task
def load_to_postgres(df_excel, df_api):
    engine = sqlalchemy.create_engine(DATABASE_URL)
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
