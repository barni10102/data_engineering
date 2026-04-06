"""Load transformed crypto records into Redis cache and PostgreSQL DWH tables."""

import pandas as pd

from prefect import task, get_run_logger
from app.db.postgres import get_postgres_connection
from app.db.redis_client import redis_client
from app.etl.common.setup_dwh import ensure_dwh_schema_exists

CACHE_TTL_SECONDS = 360  # 6 minutes

REQUIRED_COLUMNS = {
    "asset_type",
    "symbol",
    "name",
    "snapshot_ts",
    "close_price",
    "volume",
    "volume_usd",
    "return_24h",
}

@task(retries=2, retry_delay_seconds=10)
def load_crypto_data(df: pd.DataFrame | None):
    """Load transformed crypto rows into cache and DWH tables with idempotent upserts."""
    logger = get_run_logger()

    if df is None or df.empty:
        logger.warning("No data to load for Crypto.")
        return
    
    missing_cols = REQUIRED_COLUMNS - set(df.columns)
    if missing_cols:
        raise ValueError(f"Missing required columns in crypto load: {sorted(missing_cols)}")
    
    df = df.copy()
    # Enforce required fact keys before any cache or DB writes.
    df = df.dropna(subset=["snapshot_ts", "close_price", "symbol"])
    if df.empty:
        logger.warning("No valid rows left for load after required field checks.")
        return
    
    ts_utc = pd.to_datetime(df["snapshot_ts"], utc=True, errors="coerce")
    # date_id is derived from the real snapshot timestamp (no artificial weekend shifting).
    df["date_id"] = ts_utc.dt.date
    df = df.dropna(subset=["date_id"])
    if df.empty:
        logger.warning("No valid rows left after date_id derivation.")
        return

    logger.info("Starting Postgres and Redis Load for Crypto...")

    try:
        # Latest-batch cache is used by fast API endpoints before DB fallback.
        cache_df = df.drop(columns=["date_id"])
        redis_json = cache_df.to_json(orient='records', date_format='iso')
        redis_client.set("asset:latest_batch:crypto", redis_json, ex=CACHE_TTL_SECONDS)
        logger.info("Successfully updated Redis cache: asset:latest_batch:crypto")
    except Exception as e:
        logger.error(f"Failed to update Redis: {e}")

    # DWH objects are created lazily to support first-run startup on empty databases.
    ensure_dwh_schema_exists()

    conn = get_postgres_connection()
    try:
        with conn.transaction():
            with conn.cursor() as cur:
                logger.info("Upserting dwh.date_dim from snapshot_ts dates...")
                # Upsert unique calendar keys referenced by fact rows.
                unique_dates = sorted(set(df["date_id"].tolist()))
                date_dim_values = [
                    (d, d.year, d.month, d.day, d.isoweekday() in (6, 7))
                    for d in unique_dates
                ]
                cur.executemany(
                    """
                    INSERT INTO dwh.date_dim (date_id, year, month, day, is_weekend)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (date_id) DO NOTHING;
                    """,
                    date_dim_values,
                )

                logger.info("Upserting dwh.asset_dim...")
                dim_values = [tuple(x) for x in df[["asset_type", "symbol", "name"]].to_numpy()]
                cur.executemany(
                    """
                    INSERT INTO dwh.asset_dim (asset_type, symbol, name)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (asset_type, symbol) DO UPDATE
                    SET name = EXCLUDED.name;
                    """,
                    dim_values,
                )

                # Build fast symbol->asset_id lookup to resolve foreign keys for fact inserts.
                cur.execute("SELECT asset_type, symbol, asset_id FROM dwh.asset_dim WHERE asset_type = 'crypto'")
                asset_map = {(row["asset_type"], row["symbol"]): row["asset_id"] for row in cur.fetchall()}

                fact_values = []
                for _, row in df.iterrows():
                    asset_id = asset_map.get((row["asset_type"], row["symbol"]))
                    # Skip rows that cannot be resolved to an asset dimension key.
                    if not asset_id:
                        continue

                    # Normalize pandas timestamps to plain Python datetime for psycopg binding.
                    snapshot_ts = (
                        row["snapshot_ts"].to_pydatetime()
                        if hasattr(row["snapshot_ts"], "to_pydatetime")
                        else row["snapshot_ts"]
                    )

                    fact_values.append(
                        (
                            asset_id,
                            row["date_id"],
                            snapshot_ts,
                            row["close_price"],
                            row["volume"],
                            row["volume_usd"],
                            row["return_24h"],
                        )
                    )
                
                if not fact_values:
                    logger.warning("No fact rows to upsert for Crypto after asset mapping.")
                    return

                logger.info("Upserting dwh.intraday_price_fact...")
                cur.executemany(
                    """
                    INSERT INTO dwh.intraday_price_fact (
                        asset_id, date_id, snapshot_ts, close_price, volume, volume_usd, return_24h
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (asset_id, snapshot_ts) DO UPDATE SET
                        -- Idempotent retry behavior: reruns update existing rows instead of duplicating.
                        close_price = EXCLUDED.close_price,
                        volume      = EXCLUDED.volume,
                        volume_usd  = EXCLUDED.volume_usd,
                        return_24h  = EXCLUDED.return_24h;
                    """,
                    fact_values,
                )

        logger.info("Successfully loaded Crypto data into PostgreSQL DWH!")
    except Exception as e:
        logger.error(f"Postgres load failed: {e}")
        raise
    finally:
        conn.close()