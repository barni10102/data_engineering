from prefect import flow, get_run_logger

from app.etl.common.cache_update import update_top_movers_cache
from app.etl.common.refresh_daily_view import refresh_daily_view
from app.etl.extract.crypto_data import fetch_crypto_flow
from app.etl.transform.crypto_transform import transform_crypto_data
from app.etl.load.crypto_load import load_crypto_data

@flow(name="Crypto Full ETL Pipeline")
def crypto_full_etl_flow():
    logger = get_run_logger()
    logger.info("Starting Full Crypto ETL Pipeline...")

    s3_raw_path = fetch_crypto_flow(top_n=15)

    if s3_raw_path:
        transformed_df = transform_crypto_data(s3_path=s3_raw_path)
        load_crypto_data(df=transformed_df)
        refresh_daily_view()
        update_top_movers_cache(asset_type="crypto")
        logger.info("Full Crypto ETL Pipeline finished successfully!")
    else:
        logger.error("Pipeline failed: No raw data path returned.")