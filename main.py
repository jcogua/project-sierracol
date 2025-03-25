from flows.etl import build_etl_pipeline

if __name__ == "__main__":
    print("Entró a __main__")
    etl_pipeline = build_etl_pipeline()
    etl_pipeline()  # Aquí lo ejecutas realmente