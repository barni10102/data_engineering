from prefect import flow, get_run_logger
from app.etl.extract.sp500_companies import daily_sp500_companies_update_flow
from app.etl.load.sp500_companies import load_sp500_to_postgres
from app.etl.transform.sp500_companies import transform_sp500_data


@flow(name="S&P 500 Full ETL Pipeline")
def sp500_companies_full_etl_flow():
    logger = get_run_logger()

    logger.info("Starting Full S&P 500 ETL Pipeline...")

    s3_raw_path = daily_sp500_companies_update_flow()

    if s3_raw_path:
        parquet_paths = transform_sp500_data(s3_path=s3_raw_path)
        load_sp500_to_postgres(parquet_paths=parquet_paths)
        logger.info("Full S&P 500 ETL Pipeline finished successfully!")
    else:
        logger.error("Pipeline failed: No raw data path returned from extraction.")