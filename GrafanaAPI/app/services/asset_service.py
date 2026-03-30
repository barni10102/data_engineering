import json
from typing import List, Dict, Any
from datetime import datetime, timezone

from fastapi import HTTPException
from app.db.redis_client import redis_client
from app.db.postgres import get_postgres_connection


def _epoch_ms_to_datetime(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)


def _load_crypto_from_cache() -> List[Dict[str, Any]] | None:
    key = "asset:latest_batch:crypto"
    try:
        cached = redis_client.get(key)
    except Exception as e:
        print(f"Redis error (crypto): {e}")
        return None

    if not cached:
        return None

    try:
        data = json.loads(cached)
        if isinstance(data, list):
            return data
        else:
            return None
    except Exception as e:
        print(f"Invalid crypto cache JSON, ignoring. Error: {e}")
        return None
    

def _load_latest_crypto_from_db() -> List[Dict[str, Any]] | None:
    sql = """
        WITH latest AS (
            SELECT MAX(f.snapshot_ts) AS last_ts
            FROM dwh.intraday_price_fact f
            JOIN dwh.asset_dim ad ON ad.asset_id = f.asset_id
            WHERE ad.asset_type = 'crypto'
        )
        SELECT
            ad.asset_type,
            ad.symbol,
            ad.name,
            f.snapshot_ts,
            f.close_price,
            f.volume,
            f.volume_usd,
            f.return_24h
        FROM dwh.intraday_price_fact f
        JOIN dwh.asset_dim ad ON ad.asset_id = f.asset_id
        JOIN latest l ON l.last_ts = f.snapshot_ts
        WHERE ad.asset_type = 'crypto'
        ORDER BY ad.symbol;
    """

    conn = get_postgres_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                rows = cur.fetchall()
    finally:
        conn.close()

    if not rows:
        return None

    out: List[Dict[str, Any]] = []
    for row in rows:
        out.append(
            {
                "asset_type": row["asset_type"],
                "symbol": row["symbol"],
                "name": row.get("name"),
                "snapshot_ts": row["snapshot_ts"],
                "close_price": float(row["close_price"]) if row["close_price"] is not None else None,
                "volume": float(row["volume"]) if row["volume"] is not None else None,
                "volume_usd": float(row["volume_usd"]) if row["volume_usd"] is not None else None,
                "return_24h": float(row["return_24h"]) if row["return_24h"] is not None else None,
            }
        )
    return out


def _get_latest_crypto() -> List[Dict[str, Any]]:
    data = _load_crypto_from_cache()
    if data is not None:
        return data
    
    db_data = _load_latest_crypto_from_db()
    if db_data is None:
        return []
    
    try:
        redis_json = json.dumps(db_data, default=str, ensure_ascii=False)
        redis_client.set("asset:latest_batch:crypto", redis_json, ex=360)
    except Exception as e:
        print(f"Redis set error (crypto fallback): {e}")

    return db_data


def _load_stock_from_cache() -> List[Dict[str, Any]] | None:
    key = "asset:latest_batch:stock"
    try:
        cached = redis_client.get(key)
    except Exception as e:
        print(f"Redis error (stock): {e}")
        return None

    if not cached:
        return None

    try:
        data = json.loads(cached)
        if not isinstance(data, list):
            return None

        for item in data:
            for ts_key in ("snapshot_ts", "datetime"):
                if isinstance(item.get(ts_key), (int, float)):
                    item[ts_key] = _epoch_ms_to_datetime(int(item[ts_key]))

        return data
    except Exception as e:
        print(f"Invalid stock cache JSON, ignoring. Error: {e}")
        return None
    
def _load_latest_stock_from_db() -> List[Dict[str, Any]] | None:
    sql = """
        WITH latest AS (
            SELECT MAX(f.snapshot_ts) AS last_ts
            FROM dwh.intraday_price_fact f
            JOIN dwh.asset_dim ad ON ad.asset_id = f.asset_id
            WHERE ad.asset_type = 'stock'
        )
        SELECT
            ad.asset_type,
            ad.symbol,
            ad.name,
            f.snapshot_ts,
            f.close_price,
            f.volume,
            f.volume_usd,
            f.return_24h
        FROM dwh.intraday_price_fact f
        JOIN dwh.asset_dim ad ON ad.asset_id = f.asset_id
        JOIN latest l ON l.last_ts = f.snapshot_ts
        WHERE ad.asset_type = 'stock'
        ORDER BY ad.symbol;
    """

    conn = get_postgres_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                rows = cur.fetchall()
    finally:
        conn.close()

    if not rows:
        return None

    out: List[Dict[str, Any]] = []
    for row in rows:
        out.append(
            {
                "asset_type": row["asset_type"],
                "symbol": row["symbol"],
                "name": row.get("name"),
                "snapshot_ts": row["snapshot_ts"],
                "close_price": float(row["close_price"]) if row["close_price"] is not None else None,
                "volume": float(row["volume"]) if row["volume"] is not None else None,
                "volume_usd": float(row["volume_usd"]) if row["volume_usd"] is not None else None,
                "return_24h": float(row["return_24h"]) if row["return_24h"] is not None else None,
            }
        )
    return out


def _get_latest_stock() -> List[Dict[str, Any]]:
    data = _load_stock_from_cache()
    if data is not None:
        return data
    
    db_data = _load_latest_stock_from_db()
    if db_data is None:
        return []
    
    try:
        redis_json = json.dumps(db_data, default=str, ensure_ascii=False)
        redis_client.set("asset:latest_batch:stock", redis_json, ex=360)
    except Exception as e:
        print(f"Redis set error (stock fallback): {e}")

    return db_data


def get_latest_assets(asset_type: str) -> List[Dict[str, Any]]:
    if asset_type == "crypto":
        return _get_latest_crypto()
    elif asset_type == "stock":
        return _get_latest_stock()
    else:
        raise HTTPException(status_code=400, detail="Invalid asset_type")
