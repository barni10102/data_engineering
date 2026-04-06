"""Transform raw crypto snapshots from MinIO into normalized ETL rows."""

import json
import pandas as pd
from datetime import datetime, timezone
from prefect import task, get_run_logger
from app.db.minio_client import get_minio_client


@task(retries=2, retry_delay_seconds=10)
def transform_crypto_data(s3_path: str) -> pd.DataFrame | None:
    """Transform raw crypto JSON snapshot into normalized tabular records for load."""
    logger = get_run_logger()
    logger.info(f"Starting Pandas transformation for Crypto: {s3_path}")

    path_without_scheme = s3_path.replace("s3://", "")
    # Split canonical object-storage path to bucket and object key.
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

    # Fallback timestamp keeps rows usable when source-level timestamp is missing.
    batch_snapshot_ts = datetime.now(timezone.utc)
    clean_records = []

    for item in json_data:
        symbol = str(item.get("symbol", "")).strip().upper()
        if not symbol:
            continue

        name = str(item.get("name", "Unknown")).strip() or "Unknown"

        # Upstream payload stores market values under quotes -> USD.
        quotes = item.get("quotes") or {}
        usd = quotes.get("USD") or {}

        # Skip rows without a valid positive price because they break downstream metrics.
        price = pd.to_numeric(usd.get("price"), errors="coerce")
        if pd.isna(price) or price <= 0:
            continue

        volume_usd_raw = pd.to_numeric(usd.get("volume_24h"), errors="coerce")
        pct_change_raw = pd.to_numeric(usd.get("percent_change_24h"), errors="coerce")

        source_ts = pd.to_datetime(item.get("last_updated"), utc=True, errors="coerce")
        snapshot_ts = source_ts if not pd.isna(source_ts) else batch_snapshot_ts

        # Keep both base-asset volume and notional USD volume for downstream dashboards.
        volume = float(volume_usd_raw / price) if not pd.isna(volume_usd_raw) else None
        volume_usd = float(volume_usd_raw) if not pd.isna(volume_usd_raw) else None
        return_24h = float(pct_change_raw / 100.0) if not pd.isna(pct_change_raw) else None

        clean_records.append(
            {
                "asset_type": "crypto",
                "symbol": symbol,
                "name": name,
                "snapshot_ts": snapshot_ts.to_pydatetime(),
                "close_price": float(price),
                "volume": volume,
                "volume_usd": volume_usd,
                "return_24h": return_24h,
            }
        )
    
    if not clean_records:
        logger.warning("No valid crypto rows after transform.")
        return None

    df = pd.DataFrame(clean_records)
    # Keep latest duplicate within the same snapshot timestamp per symbol.
    df = df.drop_duplicates(subset=["symbol", "snapshot_ts"], keep="last")

    asset_count = int(df["symbol"].nunique())
    total_volume_usd = float(df["volume_usd"].fillna(0.0).sum())
    avg_return_24h_raw = df["return_24h"].dropna().mean()
    avg_return_24h = float(avg_return_24h_raw) if pd.notna(avg_return_24h_raw) else None

    logger.info(
        "Crypto transformation complete. "
        f"rows={len(df)}, assets={asset_count}, "
        f"total_volume_usd={total_volume_usd:.2f}, "
        f"avg_return_24h={(avg_return_24h if avg_return_24h is not None else 'n/a')}"
    )

    return df