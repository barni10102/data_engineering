"""Prefect flow orchestrating S&P500 reference-data ETL."""

from prefect import flow, get_run_logger
from app.etl.extract.sp500_companies import daily_sp500_companies_update_flow
from app.etl.load.sp500_companies_load import load_sp500_to_postgres
from app.etl.transform.sp500_companies_transform import transform_sp500_data


@flow(name="SP 500 Full ETL Pipeline")
def sp500_companies_full_etl_flow():
    """Run full S&P500 reference ETL: extract, transform, and load current/history tables."""
    logger = get_run_logger()

    logger.info("Starting Full S&P 500 ETL Pipeline...")

    s3_raw_path = daily_sp500_companies_update_flow()

    if s3_raw_path:
        # Reference-data transform returns both symbol-level and sector-level outputs.
        transformed_data = transform_sp500_data(s3_path=s3_raw_path)
        load_sp500_to_postgres(data=transformed_data)
        logger.info("Full S&P 500 ETL Pipeline finished successfully!")
    else:
        logger.error("Pipeline failed: No raw data path returned from extraction step.")