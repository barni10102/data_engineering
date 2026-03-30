import random
import time
import json
import io
from typing import Any

import yfinance as yf
from datetime import datetime

from app.db.postgres import get_postgres_connection
from app.db.redis_client import redis_client

from prefect import task, get_run_logger, flow
from app.db.minio_client import get_minio_client
from app.core import config

INFO_CACHE_TTL_SECONDS = 6 * 60 * 60


def _info_cache_key(symbol: str) -> str:
    return f"stock:info:{symbol}"


def _load_info_from_cache(symbol: str) -> dict[str, Any] | None:
    try:
        cached = redis_client.get(_info_cache_key(symbol))
        if not cached:
            return None
        data = json.loads(cached)
        return data if isinstance(data, dict) else None
    except Exception:
        return None
    

def _save_info_to_cache(symbol: str, info: dict[str, Any]) -> None:
    try:
        redis_client.set(
            _info_cache_key(symbol),
            json.dumps(info, default=str),
            ex=INFO_CACHE_TTL_SECONDS,
        )
    except Exception:
        pass


def fetch_ticker_info_with_retry(symbol: str, max_retries: int = 10, base_delay: float = 0.5) -> None | dict | dict[
    Any, Any]:
    logger = get_run_logger()
    for attempt in range(1, max_retries + 1):
        try:
            info = yf.Ticker(symbol).info
            if isinstance(info, dict) and info:
                return info
            raise ValueError("Empty info dict returned from yfinance")
        except Exception as e:
            if attempt < max_retries:
                sleep_s = base_delay * attempt + random.uniform(0.1, 0.4)
                time.sleep(sleep_s)
            else:
                logger.warning(f"Failed to fetch info for {symbol} after {max_retries} attempts. Error: {e}")
                return {}


@task(retries=3, retry_delay_seconds=5)
def get_stocks_from_db(top_n: int = 10) -> list[dict]:
    logger = get_run_logger()
    logger.info(f"Fetching top {top_n} stocks from database...")

    conn = get_postgres_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT symbol, name
                    FROM config.stock_universe
                    WHERE marketcap IS NOT NULL
                    ORDER BY marketcap DESC
                    LIMIT %s
                    """,
                    (top_n,),
                )
                rows = cur.fetchall()

        result = [{"symbol": r["symbol"], "name": r["name"]} for r in rows]
        logger.info(f"Successfully retrieved {len(result)} stocks.")
        conn.close()
        return result


    except Exception as e:
        logger.error(f"Database error while fetching stock universe: {e}")
        conn.close()
        raise e


@task(retries=3, retry_delay_seconds=30)
def fetch_stock_ohlcv(stocks: list[dict]) -> list[dict]:
    logger = get_run_logger()

    if not stocks:
        logger.warning("No stocks provided to fetch.")
        return []

    tickers = [s["symbol"] for s in stocks]
    name_by_symbol = {s["symbol"]: s["name"] for s in stocks}
    logger.info(f"Downloading yfinance data for {len(tickers)} tickers...")

    try:
        df = yf.download(
            tickers=tickers,
            period="1d",
            interval="5m",
            group_by="ticker",
            auto_adjust=False,
            progress=False,
            threads=False,
        )
    except Exception as e:
        raise RuntimeError(f"yfinance download failed: {e}")

    result = []

    if len(tickers) == 1:
        available_symbols = tickers
        df = {tickers[0]: df}
    else:
        available_symbols = list(df.columns.levels[0]) if hasattr(df.columns, "levels") else []

    for symbol in tickers:
        if symbol not in available_symbols:
            logger.debug(f"Symbol {symbol} not found in downloaded data.")
            continue

        sub = df[symbol].dropna() if len(tickers) > 1 else df[symbol].dropna()
        if sub.empty:
            continue

        last_row = sub.iloc[-1]
        first_row = sub.iloc[0]

        idx = sub.index[-1]
        ts = idx.to_pydatetime().replace(microsecond=0) if hasattr(idx, "to_pydatetime") else idx

        info = _load_info_from_cache(symbol) or {}
        

        if not info:
            fresh_info = fetch_ticker_info_with_retry(symbol)
            if fresh_info:
                info = fresh_info
                _save_info_to_cache(symbol, fresh_info)

        if regular_price is None:
            regular_price = float(last_row["Close"]) if last_row["Close"] is not None else None

        if regular_volume is None:
            regular_volume = float(last_row["Volume"]) if last_row["Volume"] is not None else None

        if regular_change_pct is None:
            first_close = float(first_row["Close"]) if first_row["Close"] is not None else None
            if first_close and regular_price:
                regular_change_pct = ((float(regular_price) / first_close) - 1.0) * 100.0

        volume_usd = (float(regular_price) * float(regular_volume)) if (regular_price and regular_volume) else None
        return_24h = (float(regular_change_pct) / 100.0) if regular_change_pct is not None else None

        result.append(
            {
                "symbol": symbol,
                "name": name_by_symbol.get(symbol),
                "datetime": ts.isoformat(),
                "open": float(last_row["Open"]),
                "high": float(last_row["High"]),
                "low": float(last_row["Low"]),
                "close": float(last_row["Close"]),
                "volume": int(last_row["Volume"]) if last_row["Volume"] is not None else 0,
                "volume_usd": volume_usd,
                "return_24h": return_24h,
            }
        )

    logger.info(f"Successfully processed OHLCV data for {len(result)} stocks.")
    return result


@task(retries=3, retry_delay_seconds=10)
def save_stocks_to_minio(data: list[dict]) -> str:
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
    folder_path = timestamp_dt.strftime("stocks/year=%Y/month=%m/day=%d")
    object_name = f"{folder_path}/stocks_{timestamp_dt.strftime('%H%M%S')}.json"

    json_bytes = json.dumps(data, ensure_ascii=False).encode('utf-8')
    data_stream = io.BytesIO(json_bytes)

    client.put_object(
        bucket_name=bucket_name,
        object_name=object_name,
        data=data_stream,
        length=len(json_bytes),
        content_type="application/json"
    )

    path = f"s3://{bucket_name}/{object_name}"
    logger.info(f"Saved stock data to MinIO at {path}")
    return path


@flow(name="Stock Extraction Pipeline")
def fetch_stocks_flow(top_n: int = 10):
    stocks = get_stocks_from_db(top_n=top_n)
    data = fetch_stock_ohlcv(stocks=stocks)
    path = save_stocks_to_minio(data=data)
    return path