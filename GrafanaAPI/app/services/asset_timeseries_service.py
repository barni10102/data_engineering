from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional, Tuple

from fastapi import HTTPException

from app.db.postgres import get_connection


def _default_from_to(
    date_from: Optional[datetime],
    date_to: Optional[datetime],
) -> tuple[datetime, datetime]:
    now = datetime.now(timezone.utc)
    if date_to is None:
        date_to = now
    if date_from is None:
        date_from = date_to - timedelta(days=7)
    return date_from, date_to


def get_asset_price_series(
    asset_type: str,
    symbol: str,
    date_from: Optional[datetime],
    date_to: Optional[datetime],
) -> dict[str, list[dict[str, Any]] | Any] | None:
    if asset_type not in ("crypto", "stock"):
        raise HTTPException(status_code=400, detail="Invalid asset_type")

    date_from, date_to = _default_from_to(date_from, date_to)

    base_sql = """
        SELECT
            ad.asset_type,
            ad.symbol,
            ad.name,
            f.snapshot_ts,
            f.close_price,
            f.volume,
            f.volume_usd
        FROM dwh.asset_dim ad
        JOIN dwh.intraday_price_fact f
          ON f.asset_id = ad.asset_id
        WHERE ad.asset_type = %s
          AND ad.symbol = %s
          AND f.snapshot_ts BETWEEN %s AND %s
        ORDER BY f.snapshot_ts ASC;
    """

    conn = get_connection()
    try:
        with conn:
            with conn.cursor() as cursor:
                cursor.execute(base_sql, (asset_type, symbol, date_from, date_to))
                rows = cursor.fetchall()
    finally:
        conn.close()


    if not rows:
        if asset_type == "crypto":
            raise HTTPException(status_code=404, detail="No data for given symbol / time range")

        sql_last = """
                SELECT max(f.snapshot_ts) AS last_ts
                FROM dwh.asset_dim ad
                JOIN dwh.intraday_price_fact f ON f.asset_id = ad.asset_id
                WHERE ad.asset_type = 'stock'
                  AND ad.symbol = %s;
            """

        conn = get_connection()
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute(sql_last, (symbol,))
                    last_row = cur.fetchone()
        finally:
            conn.close()

        last_ts = last_row["last_ts"] if last_row else None
        if last_ts is None:
            raise HTTPException(status_code=404, detail="No data at all for this symbol")

        window = date_to - date_from
        new_to = last_ts
        new_from = last_ts - window

        conn = get_connection()
        try:
            with conn:
                with conn.cursor() as cursor:
                    cursor.execute(base_sql, (asset_type, symbol, new_from, new_to))
                    rows = cursor.fetchall()
        finally:
            conn.close()


        if not rows:
            raise HTTPException(status_code=404, detail="No data for given symbol / time range")


    first = rows[0]
    asset_type = first["asset_type"]
    sym = first["symbol"]
    name = first.get("name")

    points: List[Dict[str, Any]] = []
    for row in rows:
        points.append(
            {
                "snapshot_ts": row["snapshot_ts"],
                "close_price": float(row["close_price"]) if row["close_price"] is not None else None,
                "volume": float(row["volume"]) if row["volume"] is not None else None,
                "volume_usd": float(row["volume_usd"]) if row["volume_usd"] is not None else None,
            }
        )

    return {
        "asset_type": asset_type,
        "symbol": sym,
        "name": name,
        "points": points,
    }

def get_assets_indexed_series(
    symbols: list[str],
    date_from: Optional[datetime],
    date_to: Optional[datetime],
) -> dict[str, list[dict[str, Any]]] | None:

    symbols_clean = [s.strip() for s in symbols if s.strip()]

    if not symbols_clean:
        raise HTTPException(status_code=400, detail="No symbols provided")

    date_from, date_to = _default_from_to(date_from, date_to)

    sql = """
            SELECT
                ad.asset_type,
                ad.symbol,
                ad.name,
                f.snapshot_ts,
                f.close_price
            FROM dwh.asset_dim ad
            JOIN dwh.intraday_price_fact f
              ON f.asset_id = ad.asset_id
            WHERE ad.symbol = ANY(%s)
              AND f.snapshot_ts BETWEEN %s AND %s
            ORDER BY ad.asset_type, ad.symbol, f.snapshot_ts ASC;
        """

    conn = get_connection()
    try:
        with conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, (symbols_clean, date_from, date_to))
                rows = cursor.fetchall()
    finally:
        conn.close()

    if not rows:
        raise HTTPException(status_code=404, detail="No data for given symbols / time range")

    grouped: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}
    for row in rows:
        key = (row["asset_type"], row["symbol"])
        grouped.setdefault(key, []).append(row)

    flat_series: List[Dict[str, Any]] = []

    for (asset_type, sym), sym_rows in grouped.items():
        base_price: Optional[float] = None
        for r in sym_rows:
            cp = r["close_price"]
            if cp is not None:
                base_price = float(cp)
                break

        if base_price is None or base_price == 0:
            continue

        for r in sym_rows:
            cp = r["close_price"]
            if cp is None:
                continue
            cp_float = float(cp)
            normalized = (cp_float / base_price) * 100.0

            flat_series.append(
                {
                    "asset_type": asset_type,
                    "symbol": sym,
                    "name": r.get("name"),
                    "snapshot_ts": r["snapshot_ts"],
                    "value": normalized,
                }
            )

    if not flat_series:
        raise HTTPException(status_code=404, detail="No valid data for given symbols")


    return {"series": flat_series}
