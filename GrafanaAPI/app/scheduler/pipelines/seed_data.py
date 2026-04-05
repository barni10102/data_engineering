import io
import mimetypes
import zipfile
from pathlib import Path

from prefect import flow, task, get_run_logger

from app.core import config
from app.db.minio_client import get_minio_client
from app.etl.common.refresh_daily_view import refresh_daily_view
from app.etl.common.cache_update import update_top_movers_cache
from app.etl.transform.crypto_transform import transform_crypto_data
from app.etl.transform.stock_transform import transform_stock_data
from app.etl.load.crypto_load import load_crypto_data
from app.etl.load.stock_load import load_stock_data


@task
def seed_minio_from_local_zip(zip_path: str) -> dict:
    logger = get_run_logger()
    p = Path(zip_path)

    out = {
        "uploaded_files": 0,
        "crypto_paths": [],
        "stock_paths": [],
        "zip_found": p.exists(),
    }

    if not p.exists():
        logger.warning(f"Local raw archive not found: {zip_path}")
        return out

    bucket = config.MINIO_RAW_BUCKET
    client = get_minio_client()

    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)

    with zipfile.ZipFile(p, "r") as zf:
        for info in zf.infolist():
            if info.is_dir():
                continue

            name = info.filename.replace("\\", "/").lstrip("/")
            if not name.startswith("raw/"):
                continue

            object_name = name[len("raw/"):]
            if not (object_name.startswith("crypto/") or object_name.startswith("stocks/")):
                continue

            payload = zf.read(info)
            if not payload:
                continue

            content_type = mimetypes.guess_type(object_name)[0] or "application/octet-stream"
            client.put_object(
                bucket_name=bucket,
                object_name=object_name,
                data=io.BytesIO(payload),
                length=len(payload),
                content_type=content_type,
            )

            s3_path = f"s3://{bucket}/{object_name}"
            out["uploaded_files"] += 1
            if object_name.startswith("crypto/"):
                out["crypto_paths"].append(s3_path)
            else:
                out["stock_paths"].append(s3_path)

    logger.info(f"Seed upload done. Uploaded files: {out['uploaded_files']}")
    return out


@task
def load_seeded_market_data(seed: dict) -> dict:
    logger = get_run_logger()

    crypto_loaded_files = 0
    stock_loaded_files = 0

    for s3_path in sorted(seed.get("crypto_paths", [])):
        df = transform_crypto_data(s3_path=s3_path)
        if df is None or df.empty:
            continue
        load_crypto_data(df=df)
        crypto_loaded_files += 1

    for s3_path in sorted(seed.get("stock_paths", [])):
        df = transform_stock_data(s3_path=s3_path)
        if df is None or df.empty:
            continue
        load_stock_data(df=df)
        stock_loaded_files += 1

    if crypto_loaded_files > 0 or stock_loaded_files > 0:
        refresh_daily_view()
        if crypto_loaded_files > 0:
            update_top_movers_cache(asset_type="crypto")
        if stock_loaded_files > 0:
            update_top_movers_cache(asset_type="stock")

    summary = {
        "crypto_loaded_files": crypto_loaded_files,
        "stock_loaded_files": stock_loaded_files,
    }
    logger.info(f"Seed load summary: {summary}")
    return summary


@flow(name="Market Local Raw Bootstrap")
def bootstrap_market_from_local_raw_zip(zip_path: str) -> dict:
    seed = seed_minio_from_local_zip(zip_path=zip_path)
    if seed["uploaded_files"] == 0:
        return {"status": "no_seed_data", **seed}
    loaded = load_seeded_market_data(seed=seed)
    return {"status": "ok", **seed, **loaded}