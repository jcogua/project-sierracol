from prefect import task, get_run_logger
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from core import DATABASE_URL
from notifications import notify_slack

@task(log_prints=True)
def load_to_postgres(df_api, df_excel, df_csv):
    logger = get_run_logger()
    try:
        engine = create_engine(DATABASE_URL)
        df_api.to_sql("petroleum_prices", engine, if_exists="replace", index=False)
        notify_slack(f"✅ Database load successful - petroleum_prices")
        df_excel.to_sql("energy_metrics", engine, if_exists="replace", index=False)
        notify_slack(f"✅ Database load successful - energy_metrics")
        df_csv.to_sql("geogist", engine, if_exists="replace", index=False)
        notify_slack(f"✅ Database load successful - GEOGIST")
        with engine.connect() as conn:
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_energy_country ON energy_metrics (country)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_price_date ON petroleum_prices (date)"))
        logger.info("Data loaded and indexes created.")
    except SQLAlchemyError as e:
        notify_slack(f"❌ Database load error: {e}")
        raise