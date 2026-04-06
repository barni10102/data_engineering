"""Transform S&P500 raw CSV snapshots into normalized universe and sector outputs."""

import pandas as pd
import io

from pandas import DataFrame
from prefect import task, get_run_logger
from app.db.minio_client import get_minio_client


@task(retries=2, retry_delay_seconds=10)
def transform_sp500_data(s3_path: str) -> tuple[DataFrame, DataFrame] | None:
    """Transform raw S&P500 CSV into company universe and sector aggregate outputs."""
    logger = get_run_logger()

    logger.info(f"Starting Pandas transformation for: {s3_path}")

    path_without_scheme = s3_path.replace("s3://", "")
    bucket_name, object_name = path_without_scheme.split("/", 1)

    client = get_minio_client()

    response = None
    try:
        response = client.get_object(bucket_name, object_name)
        csv_bytes = response.read()
        df = pd.read_csv(io.BytesIO(csv_bytes))
        logger.info(f"Successfully loaded {len(df)} rows from MinIO into Pandas.")
    except Exception as e:
        logger.error(f"Failed to load data from MinIO: {e}")
        raise
    finally:
        if response is not None:
            response.close()
            response.release_conn()

    if 'Symbol' not in df.columns:
        raise ValueError("Critical error: 'Symbol' column is missing from the source data!")

    # Fill missing source columns so the pipeline remains robust to minor schema drift.
    needed_cols = {'Symbol', 'Shortname', 'Sector', 'Marketcap', 'Weight'}
    for col in needed_cols:
        if col not in df.columns:
            df[col] = None

    df = df.dropna(subset=['Symbol'])
    df['symbol'] = df['Symbol'].astype(str).str.strip().str.upper()
    df = df[df['symbol'] != '']

    df['name'] = df['Shortname'].fillna('Unknown').astype(str)
    df['sector'] = df['Sector'].fillna('Unknown').astype(str)

    df['marketcap'] = pd.to_numeric(df['Marketcap'], errors='coerce').fillna(0.0)
    df['weight'] = pd.to_numeric(df['Weight'], errors='coerce').fillna(0.0)

    logger.info("Performing sector aggregations...")

    # Sector-level aggregates feed analytics endpoints and dashboard summary panels.
    sector_stats_df: pd.DataFrame = df.groupby('sector').agg(
        company_count=('symbol', 'count'),
        avg_marketcap=('marketcap', 'mean'),
        total_weight=('weight', 'sum')
    ).reset_index()

    sector_stats_df['avg_marketcap'] = sector_stats_df['avg_marketcap'].fillna(0.0)
    sector_stats_df['total_weight'] = sector_stats_df['total_weight'].fillna(0.0)

    # Universe output keeps one current record per symbol for stock selection and reference joins.
    universe_df: pd.DataFrame = df[['symbol', 'name', 'sector', 'marketcap', 'weight']].copy()

    logger.info("Pandas transformation completed successfully.")

    return universe_df, sector_stats_df