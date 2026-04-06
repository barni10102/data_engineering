"""Load transformed stock records into Redis cache and PostgreSQL DWH tables."""

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
def load_stock_data(df: pd.DataFrame | None):
    """Load transformed stock rows into Redis and PostgreSQL with idempotent writes."""
    logger = get_run_logger()

    if df is None or df.empty:
        logger.warning("No data to load for Stock.")
        return
    
    missing_cols = REQUIRED_COLUMNS - set(df.columns)
    if missing_cols:
        raise ValueError(f"Missing required columns in stock load: {sorted(missing_cols)}")
    
    df = df.copy()
    # Enforce required fact keys before cache and DB writes.
    df = df.dropna(subset=["snapshot_ts", "close_price"])
    if df.empty:
        logger.warning("No valid rows left for load after required field checks.")
        return
    
    # date_id is derived from actual market snapshot timestamps.
    df["date_id"] = df["snapshot_ts"].apply(lambda x: x.date())

    logger.info("Starting Postgres and Redis Load for Stock...")

    try:
        # Latest stock batch cache supports fast API responses between ETL runs.
        cache_df = df.drop(columns=["date_id"])
        redis_json = cache_df.to_json(orient='records', date_format='iso')
        redis_client.set("asset:latest_batch:stock", redis_json, ex=CACHE_TTL_SECONDS)
        logger.info("Successfully updated Redis cache: asset:latest_batch:stock")
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

                logger.info("Upserting dwh.asset_dim for stocks...")
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
                cur.execute("SELECT asset_type, symbol, asset_id FROM dwh.asset_dim WHERE asset_type = 'stock'")
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

                logger.info("Upserting dwh.intraday_price_fact for stocks...")
                cur.executemany(
                    """
                    INSERT INTO dwh.intraday_price_fact (
                        asset_id, date_id, snapshot_ts, close_price, volume, volume_usd, return_24h
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (asset_id, snapshot_ts) DO UPDATE SET
                        -- Idempotent reruns update existing points, avoiding duplicate facts.
                        close_price = EXCLUDED.close_price,
                        volume      = EXCLUDED.volume,
                        volume_usd  = EXCLUDED.volume_usd,
                        return_24h  = EXCLUDED.return_24h;
                    """,
                    fact_values,
                )

        logger.info("Successfully loaded Stock data into PostgreSQL DWH!")
    except Exception as e:
        logger.error(f"Postgres load failed: {e}")
        raise
    finally:
        conn.close()