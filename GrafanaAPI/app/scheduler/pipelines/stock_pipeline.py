from prefect import flow, get_run_logger

from app.etl.common.cache_update import update_top_movers_cache
from app.etl.common.refresh_daily_view import refresh_daily_view
from app.etl.extract.stock_data import fetch_stocks_flow
from app.etl.transform.stock_transform import transform_stock_data
from app.etl.load.stock_load import load_stock_data


@flow(name="Stock Full ETL Pipeline")
def stock_full_etl_flow():
    logger = get_run_logger()
    logger.info("Starting Full Stock ETL Pipeline...")

    s3_raw_path = fetch_stocks_flow(top_n=15)

    if s3_raw_path:
        transformed_df = transform_stock_data(s3_path=s3_raw_path)
        load_stock_data(df=transformed_df)
        refresh_daily_view()
        update_top_movers_cache(asset_type="stock")
        logger.info("Full Stock ETL Pipeline finished successfully!")
    else:
        logger.error("Pipeline failed: No raw data path returned.")