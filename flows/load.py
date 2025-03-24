from prefect import task, get_run_logger
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from core import DATABASE_URL
from flows.notifications import notify_slack

@task(log_prints=True)
def load_to_postgres(df_api, df_excel):
    print(" desde load_to_postgres")
    logger = get_run_logger()
    try:
        engine = create_engine(DATABASE_URL)
        df_api.to_sql("petroleum_prices", engine, if_exists="append", index=False)
        df_excel.to_sql("energy_metrics", engine, if_exists="append", index=False)
        with engine.connect() as conn:
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_energy_country ON energy_metrics (country)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_price_date ON petroleum_prices (date)"))
        logger.info("Data loaded and indexes created.")
    except SQLAlchemyError as e:
        notify_slack(f"❌ Database load error: {e}")
        raise