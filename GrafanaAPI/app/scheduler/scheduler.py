from prefect import serve
from app.db.postgres import get_postgres_connection
from app.scheduler.pipelines.sp500_companies_pipeline import sp500_companies_full_etl_flow
from app.scheduler.pipelines.crypto_pipeline import crypto_full_etl_flow
from app.scheduler.pipelines.stock_pipeline import stock_full_etl_flow


def is_initial_load_needed() -> bool | None:
    conn= None
    needs_load = True
    try:
        conn = get_postgres_connection()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT 1 FROM information_schema.tables 
                WHERE table_schema = 'config' AND table_name = 'stock_universe';
            """)
            if cur.fetchone():
                cur.execute("SELECT 1 FROM config.stock_universe LIMIT 1;")
                if cur.fetchone():
                    needs_load = False

    except Exception as e:
        print(f"Database check failed (assuming empty): {e}")
    finally:
        if conn is not None:
            conn.close()

    return needs_load

if __name__ == "__main__":
    print("Starting Prefect Scheduler...")

    if is_initial_load_needed():
        print("Database is empty or missing. Running initial S&P 500 Full ETL Pipeline...")
        try:
            sp500_companies_full_etl_flow()
            print("Initial ETL completed successfully.")
        except Exception as e:
            print(f"Initial ETL failed, but starting scheduler anyway: {e}")
    else:
        print("Initial S&P 500 data already exists in Postgres. Skipping initial load.")

    daily_sp500_companies_deployment = sp500_companies_full_etl_flow.to_deployment(
        name="daily-sp500-full-etl",
        cron="0 0 * * *",
        tags=["daily","sp500"],
    )

    stock_deployment = stock_full_etl_flow.to_deployment(
        name="5-min-stocks",
        cron="*/5 * * * 1-5",
        tags=["intraday", "stocks"],
    )

    crypto_deployment = crypto_full_etl_flow.to_deployment(
        name="5-min-crypto",
        cron="*/5 * * * *",
        tags=["intraday", "crypto"]
    )

    print("Serving deployments... Waiting for scheduled jobs.")
    serve(
        daily_sp500_companies_deployment,
        stock_deployment,
        crypto_deployment
    )
