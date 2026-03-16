import json
from typing import List, Dict, Any
from fastapi import HTTPException
from app.db.redis_client import redis_client
from datetime import datetime, timezone


def _epoch_ms_to_datetime(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)


def _error_row(asset_type: str) -> List[Dict[str, Any]]:
    return [{
        "message": f"Error loading {asset_type} data from cache. Please wait a few minutes and refresh the dashboard."
    }]


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


def _get_latest_crypto() -> List[Dict[str, Any]]:
    data = _load_crypto_from_cache()
    if data is None:
        return _error_row("crypto")
    return data


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
            if isinstance(item.get("datetime"), (int, float)):
                item["datetime"] = _epoch_ms_to_datetime(item["datetime"])

        return data
    except Exception as e:
        print(f"Invalid stock cache JSON, ignoring. Error: {e}")
        return None


def _get_latest_stock() -> List[Dict[str, Any]]:
    data = _load_stock_from_cache()
    if data is None:
        return _error_row("stocks")
    return data


def get_latest_assets(asset_type: str) -> List[Dict[str, Any]]:
    if asset_type == "crypto":
        return _get_latest_crypto()
    elif asset_type == "stock":
        return _get_latest_stock()
    else:
        raise HTTPException(status_code=400, detail="Invalid asset_type")
