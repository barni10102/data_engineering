import json
from typing import List, Dict, Any

from app.db.redis_client import redis_client



def _error_row(asset_type: str) -> List[Dict[str, Any]]:
    return [{
        "message": "Error loading {asset_type} top movers from cache. "
        "Please wait a few minutes and refresh the dashboard.".format(asset_type=asset_type)
    }]


def _load_top_movers_from_cache(asset_type: str) -> List[Dict[str, Any]] | None:
    key = f"asset:top_movers:{asset_type}"

    try:
        cached = redis_client.get(key)
    except Exception as e:
        print(f"Redis error (top_movers {asset_type}): {e}")
        return None

    if not cached:
        return None

    try:
        data = json.loads(cached)
    except Exception as e:
        print(f"Invalid JSON in {key}: {e}")
        return None

    if not isinstance(data, list):
        return None

    for item in data:
        val = item.get("return_1d")
        if isinstance(val, str):
            try:
                item["return_1d"] = float(val)
            except ValueError:
                pass

        if asset_type == "all":
            if item.get("asset_type") == "crypto":
                item["signed_return"] = item["return_1d"]
            else:
                item["signed_return"] = -item["return_1d"]

        vol = item.get("volume_usd")
        if isinstance(vol, str):
            try:
                item["volume_usd"] = float(vol)
            except ValueError:
                pass

    return data


def get_top_movers(asset_type: str) -> List[Dict[str, Any]]:
    if asset_type != "crypto" and asset_type != "stock" and asset_type != "all":
        return _error_row(asset_type)

    data = _load_top_movers_from_cache(asset_type)
    if data is None:
        return _error_row(asset_type)

    return data
