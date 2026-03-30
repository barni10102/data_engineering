from datetime import datetime, timedelta, timezone
from typing import Optional, Any, Dict, List

from fastapi import HTTPException

from app.db.postgres import get_postgres_connection


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


def get_assets_aggregated_summary(
    asset_type: Optional[str],
    date_from: Optional[datetime],
    date_to: Optional[datetime],
    limit: int = 30,
) -> Dict[str, Any]:
    if asset_type not in (None, "crypto", "stock"):
        raise HTTPException(status_code=400, detail="Invalid asset_type")

    if limit < 1 or limit > 500:
        raise HTTPException(status_code=400, detail="limit must be between 1 and 500")

    date_from, date_to = _default_from_to(date_from, date_to)

    base_sql = """
        SELECT
            ad.asset_type,
            ad.symbol,
            MAX(ad.name) AS name,
            AVG(f.return_24h) AS avg_return_24h,
            SUM(COALESCE(f.volume_usd, 0)) AS total_volume_usd,
            MAX(f.snapshot_ts) AS last_snapshot,
            COUNT(*) AS points_count
        FROM dwh.intraday_price_fact f
        JOIN dwh.asset_dim ad
          ON ad.asset_id = f.asset_id
        WHERE f.snapshot_ts BETWEEN %s AND %s
    """

    params: List[Any] = [date_from, date_to]

    if asset_type is not None:
        base_sql += " AND ad.asset_type = %s"
        params.append(asset_type)

    base_sql += """
        GROUP BY ad.asset_type, ad.symbol
        ORDER BY total_volume_usd DESC NULLS LAST
        LIMIT %s;
    """

    params.append(limit)

    conn = get_postgres_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(base_sql, tuple(params))
                rows = cur.fetchall()
    finally:
        conn.close()

    result_rows: List[Dict[str, Any]] = []
    for row in rows:
        result_rows.append(
            {
                "asset_type": row["asset_type"],
                "symbol": row["symbol"],
                "name": row.get("name"),
                "avg_return_24h": float(row["avg_return_24h"]) if row["avg_return_24h"] is not None else None,
                "total_volume_usd": float(row["total_volume_usd"]) if row["total_volume_usd"] is not None else 0.0,
                "last_snapshot": row["last_snapshot"],
                "points_count": int(row["points_count"]) if row["points_count"] is not None else 0,
            }
        )

    return {
        "from": date_from,
        "to": date_to,
        "asset_type": asset_type,
        "rows": result_rows,
    }


def get_sp500_sector_stats(
    limit: int = 20,
    sort_by: str = "company_count",
) -> Dict[str, Any]:
    if limit < 1 or limit > 200:
        raise HTTPException(status_code=400, detail="limit must be between 1 and 200")

    allowed_sort = {
        "company_count": "company_count",
        "avg_marketcap": "avg_marketcap",
        "total_weight": "total_weight",
        "updated_at": "updated_at",
    }
    if sort_by not in allowed_sort:
        raise HTTPException(
            status_code=400,
            detail="Invalid sort_by. Use one of: company_count, avg_marketcap, total_weight, updated_at",
        )

    sql = f"""
        SELECT
            sector,
            company_count,
            avg_marketcap,
            total_weight,
            updated_at
        FROM config.sector_stats
        ORDER BY {allowed_sort[sort_by]} DESC NULLS LAST
        LIMIT %s;
    """

    conn = get_postgres_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(sql, (limit,))
                rows = cur.fetchall()
    finally:
        conn.close()

    out: List[Dict[str, Any]] = []
    for row in rows:
        out.append(
            {
                "sector": row["sector"],
                "company_count": int(row["company_count"]) if row["company_count"] is not None else 0,
                "avg_marketcap": float(row["avg_marketcap"]) if row["avg_marketcap"] is not None else 0.0,
                "total_weight": float(row["total_weight"]) if row["total_weight"] is not None else 0.0,
                "updated_at": row["updated_at"],
            }
        )

    return {
        "sort_by": sort_by,
        "limit": limit,
        "rows": out,
    }