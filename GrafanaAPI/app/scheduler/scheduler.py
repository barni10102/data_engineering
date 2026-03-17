from app.etl.extract.stock_data import fetch_stocks_flow
from app.etl.extract.crypto_data import fetch_crypto_flow
from prefect.runner import Runner

from app.scheduler.pipelines.sp500_companies_pipeline import sp500_companies_full_etl_flow

if __name__ == "__main__":
    print("Starting Prefect Scheduler...")

    print("Running initial S&P 500 Full ETL Pipeline...")
    try:
        sp500_companies_full_etl_flow()
        print("Initial ETL completed successfully.")
    except Exception as e:
        print(f"Initial ETL failed, but starting scheduler anyway: {e}")

    daily_sp500_companies_deployment = sp500_companies_full_etl_flow.to_deployment(
        name="daily-sp500-full-etl",
        cron="0 0 * * *",
        tags=["daily","sp500"],
    )

    stock_fetch_deployment = fetch_stocks_flow.to_deployment(
        name="5-min-stocks",
        cron="*/5 * * * *",
        parameters={"top_n": 20},
        tags=["intraday", "stocks"],
    )

    crypto_fetch_deployment = fetch_crypto_flow.to_deployment(
        name="5-min-crypto",
        cron="*/5 * * * *",
        parameters={"top_n": 20},
        tags=["intraday", "crypto"]
    )

    runner = Runner(name="assets-runner")
    runner.add_deployment(daily_sp500_companies_deployment)
    runner.add_deployment(stock_fetch_deployment)
    runner.add_deployment(crypto_fetch_deployment)

    print("Runner starting... Waiting for scheduled jobs.")
    runner.start()
