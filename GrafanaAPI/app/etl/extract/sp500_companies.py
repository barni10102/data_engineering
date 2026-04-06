"""S&P500 reference-data extraction tasks using Kaggle source and MinIO landing."""

import os
import kagglehub
from datetime import datetime
from app.db.minio_client import get_minio_client
from prefect import task, flow, get_run_logger
from app.core import config


@task(retries=3, retry_delay_seconds=10)
def download_sp500_companies_from_kaggle() -> str:
    """Download the latest S&P500 company CSV from Kaggle and return local path."""
    logger = get_run_logger()
    logger.info("Downloading S&P 500 companies dataset from Kaggle...")

    file_path = kagglehub.dataset_download("andrewmvd/sp-500-stocks", "sp500_companies.csv")

    if os.path.isdir(file_path):
        file_path = os.path.join(file_path, "sp500_companies.csv")

    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Kaggle download failed: {file_path} not found.")

    logger.info(f"Successfully downloaded: {file_path}")
    return file_path


@task
def upload_kaggle_to_minio(local_path: str):
    """Upload the downloaded Kaggle CSV to MinIO raw storage and return s3 path."""
    logger = get_run_logger()
    client = get_minio_client()
    bucket_name = config.MINIO_RAW_BUCKET

    if not client.bucket_exists(bucket_name):
        logger.info(f"Creating bucket: {bucket_name}")
        client.make_bucket(bucket_name)

    timestamp_dt = datetime.now()
    # Reference data is partitioned by date to keep historical snapshots for reproducibility.
    folder_path = timestamp_dt.strftime("reference/sp500/year=%Y/month=%m/day=%d")
    object_name = f"{folder_path}/sp500_companies_{timestamp_dt.strftime('%H%M%S')}.csv"

    client.fput_object(
        bucket_name=bucket_name,
        object_name=object_name,
        file_path=local_path
    )

    try:
        os.remove(local_path)
        logger.info(f"Cleaned up local file: {local_path}")
    except Exception as e:
        logger.warning(f"Could not delete local file: {e}")

    path = f"s3://{bucket_name}/{object_name}"
    logger.info(f"Saved sp500 companies data to MinIO: {path}")
    return path

@flow(name="Daily Kaggle SP 500 Update")
def daily_sp500_companies_update_flow():
    """Run the daily S&P500 reference-data extraction flow."""
    local_csv = download_sp500_companies_from_kaggle()
    path = upload_kaggle_to_minio(local_csv)
    return path