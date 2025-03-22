import os
import requests
import yaml
import psycopg2
from prefect import get_client


def check_config_file():
    print("\n✅ Verificando archivo de configuración...")
    if not os.path.exists("configs/config.yaml"):
        print("❌ configs/config.yaml no encontrado")
        return None
    with open("configs/config.yaml", "r") as file:
        config = yaml.safe_load(file)
    print("✔ Configuración cargada correctamente")
    return config


def check_db_connection(db_config):
    print("\n✅ Verificando conexión a la base de datos...")
    try:
        conn = psycopg2.connect(
            host=db_config['host'],
            port=db_config['port'],
            user=db_config['user'],
            password=db_config['password'],
            dbname=db_config['dbname']
        )
        conn.close()
        print("✔ Conexión exitosa a la base de datos")
    except Exception as e:
        print(f"❌ Error al conectar a la base de datos: {e}")


def check_api(api_url):
    print("\n✅ Verificando acceso a la API...")
    try:
        response = requests.get(api_url)
        if response.status_code == 200:
            print("✔ API accesible y respuesta OK")
        else:
            print(f"❌ La API respondió con status code: {response.status_code}")
    except Exception as e:
        print(f"❌ Error al acceder a la API: {e}")


def check_prefect_cloud():
    print("\n✅ Verificando conexión con Prefect Cloud...")
    try:
        client = get_client()
        workspace = client._client._base_url
        print(f"✔ Conectado a Prefect Cloud en: {workspace}")
    except Exception as e:
        print(f"❌ No se pudo conectar a Prefect Cloud: {e}")


if __name__ == "__main__":
    print("=== Diagnóstico del entorno del proyecto ===")
    config = check_config_file()
    if config:
        check_db_connection(config['database'])
        check_api(config['api']['url'])
    check_prefect_cloud()
