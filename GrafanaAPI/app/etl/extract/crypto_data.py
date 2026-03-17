import json
import io
import requests
from datetime import datetime

from prefect import task, get_run_logger, flow
from app.db.minio_client import get_minio_client
from app.core import config


@task(retries=3, retry_delay_seconds=10)
def fetch_crypto_raw(top_n: int = 50) -> list[dict]:
    logger = get_run_logger()
    url = "https://api.coinpaprika.com/v1/tickers"

    logger.info(f"Fetching top {top_n} crypto assets...")
    response = requests.get(url, timeout=10)
    response.raise_for_status()

    data = response.json()
    return data[:top_n]


@task(retries=3, retry_delay_seconds=10)
def save_crypto_to_minio(data: list[dict]) -> str:
    logger = get_run_logger()

    bucket_name = config.MINIO_RAW_BUCKET

    if not data:
        logger.warning("No data to save to MinIO.")
        return ""

    client = get_minio_client()

    if not client.bucket_exists(bucket_name):
        logger.info(f"Creating MinIO bucket: {bucket_name}")
        client.make_bucket(bucket_name)

    timestamp_dt = datetime.now()
    folder_path = timestamp_dt.strftime("crypto/year=%Y/month=%m/day=%d")
    object_name = f"{folder_path}/crypto_{timestamp_dt.strftime('%H%M%S')}.json"

    json_bytes = json.dumps(data, ensure_ascii=False).encode('utf-8')
    client.put_object(
        bucket_name=bucket_name,
        object_name=object_name,
        data=io.BytesIO(json_bytes),
        length=len(json_bytes),
        content_type="application/json"
    )

    path = f"s3://{bucket_name}/{object_name}"
    logger.info(f"Saved crypto data to MinIO: {path}")
    return path


@flow(name="Crypto Extraction Pipeline")
def fetch_crypto_flow(top_n: int = 50):
    raw_data = fetch_crypto_raw(top_n=top_n)
    path = save_crypto_to_minio(data=raw_data)
    return path