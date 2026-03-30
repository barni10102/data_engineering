import json
from typing import List, Dict, Any

from app.db.postgres import get_postgres_connection
from app.db.redis_client import redis_client


def _normalize(data: List[Dict[str, Any]], asset_type: str) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []

    for item in data:
        row = dict(item)

        val = row.get("return_1d")
        if isinstance(val, str):
            try:
                row["return_1d"] = float(val)
            except ValueError:
                pass

        vol = row.get("volume_usd")
        if isinstance(vol, str):
            try:
                row["volume_usd"] = float(vol)
            except ValueError:
                pass

        if asset_type == "all":
            r = row.get("return_1d")
            if isinstance(r, (int, float)):
                row["signed_return"] = r if row.get("asset_type") == "crypto" else -r
            else:
                row["signed_return"] = None

        out.append(row)

    return out


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

    return _normalize(data, asset_type)


def _load_top_movers_from_db(asset_type: str) -> List[Dict[str, Any]]:
    queries = {
        "crypto": """
            WITH last_date AS (
                SELECT MAX(f.price_date) AS d
                FROM dwh.daily_price_fact f
                JOIN dwh.asset_dim ad ON ad.asset_id = f.asset_id
                WHERE ad.asset_type = 'crypto'
            )
            SELECT ad.asset_type, ad.symbol, ad.name, f.return_1d, f.volume_usd
            FROM dwh.daily_price_fact f
            JOIN dwh.asset_dim ad ON ad.asset_id = f.asset_id
            CROSS JOIN last_date ld
            WHERE f.price_date = ld.d
              AND ad.asset_type = 'crypto'
              AND f.return_1d IS NOT NULL
            ORDER BY f.return_1d DESC
            LIMIT 10;
        """,
        "stock": """
            WITH last_date AS (
                SELECT MAX(f.price_date) AS d
                FROM dwh.daily_price_fact f
                JOIN dwh.asset_dim ad ON ad.asset_id = f.asset_id
                WHERE ad.asset_type = 'stock'
            )
            SELECT ad.asset_type, ad.symbol, ad.name, f.return_1d, f.volume_usd
            FROM dwh.daily_price_fact f
            JOIN dwh.asset_dim ad ON ad.asset_id = f.asset_id
            CROSS JOIN last_date ld
            WHERE f.price_date = ld.d
              AND ad.asset_type = 'stock'
              AND f.return_1d IS NOT NULL
            ORDER BY f.return_1d DESC
            LIMIT 10;
        """,
        "all": """
            WITH last_dates AS (
                SELECT ad.asset_type, MAX(f.price_date) AS d
                FROM dwh.daily_price_fact f
                JOIN dwh.asset_dim ad ON ad.asset_id = f.asset_id
                WHERE ad.asset_type IN ('crypto', 'stock')
                GROUP BY ad.asset_type
            )
            SELECT ad.asset_type, ad.symbol, ad.name, f.return_1d, f.volume_usd
            FROM dwh.daily_price_fact f
            JOIN dwh.asset_dim ad ON ad.asset_id = f.asset_id
            JOIN last_dates ld
              ON ld.asset_type = ad.asset_type
             AND ld.d = f.price_date
            WHERE ad.asset_type IN ('crypto', 'stock')
              AND f.return_1d IS NOT NULL
            ORDER BY f.return_1d DESC
            LIMIT 20;
        """,
    }

    conn = get_postgres_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(queries[asset_type])
                rows = cur.fetchall()
    finally:
        conn.close()

    if not rows:
        return []

    return _normalize([dict(r) for r in rows], asset_type)


def get_top_movers(asset_type: str) -> List[Dict[str, Any]]:
    if asset_type not in ("crypto", "stock", "all"):
        return []

    data = _load_top_movers_from_cache(asset_type)
    if data is not None:
        return data
    
    return _load_top_movers_from_db(asset_type)
