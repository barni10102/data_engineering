import json
import pandas as pd
from datetime import datetime, timezone
from prefect import task, get_run_logger
from app.db.minio_client import get_minio_client


@task(retries=2, retry_delay_seconds=10)
def transform_stock_data(s3_path: str) -> pd.DataFrame | None:
    logger = get_run_logger()
    logger.info(f"Starting Pandas transformation for Stock: {s3_path}")

    path_without_scheme = s3_path.replace("s3://", "")
    bucket_name, object_name = path_without_scheme.split("/", 1)
    client = get_minio_client()

    response = None
    try:
        response = client.get_object(bucket_name, object_name)
        json_data = json.loads(response.read().decode('utf-8'))
    except Exception as e:
        logger.error(f"Failed to load JSON from MinIO: {e}")
        raise
    finally:
        if response is not None:
            response.close()
            response.release_conn()

    if not json_data:
        logger.warning("Empty JSON received.")
        return None

    clean_records = []

    for item in json_data:
        symbol = str(item.get("symbol", "")).strip().upper()
        if not symbol:
            continue

        name = str(item.get("name", "Unknown")).strip() or "Unknown"

        snapshot_ts = pd.to_datetime(item.get("datetime"), utc=True, errors="coerce")
        if pd.isna(snapshot_ts):
            continue

        close_price = pd.to_numeric(item.get("close"), errors="coerce")
        if pd.isna(close_price):
            continue

        volume = pd.to_numeric(item.get("volume"), errors="coerce")
        volume_usd = pd.to_numeric(item.get("volume_usd"), errors="coerce")
        return_24h = pd.to_numeric(item.get("return_24h"), errors="coerce")

        clean_records.append(
            {
                "asset_type": "stock",
                "symbol": symbol,
                "name": name,
                "snapshot_ts": snapshot_ts.to_pydatetime(),
                "close_price": float(close_price),
                "volume": float(volume) if not pd.isna(volume) else None,
                "volume_usd": float(volume_usd) if not pd.isna(volume_usd) else None,
                "return_24h": float(return_24h) if not pd.isna(return_24h) else None,
            }
        )
    
    if not clean_records:
        logger.warning("No valid stock rows after transform.")
        return None

    df = pd.DataFrame(clean_records)
    df = df.drop_duplicates(subset=["symbol", "snapshot_ts"], keep="last")
    logger.info(f"Stock transformation complete. Processed {len(df)} records.")

    return df