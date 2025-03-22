import prefect
from core import DATABASE_URL
from prefect import flow
from extraction.api_extraction import extract_api_data
from extraction.opendata_extraction import extract_open_data

@flow(name="Main Pipeline Flow")
def main_pipeline():
    api_data = extract_api_data()
    open_data = extract_open_data()
    # Aquí puedes agregar transformaciones y carga a la base de datos

if __name__ == "__main__":
    main_pipeline()