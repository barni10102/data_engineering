from typing import  Any

from fastapi import HTTPException

from app.db.postgres import get_connection



def get_assets_by_type(asset_type: str) -> list[dict[str, Any]] | None:
    if asset_type not in {"crypto", "stock"}:
        raise HTTPException(status_code=400, detail="Invalid asset_type")

    sql = """
        SELECT asset_type, symbol, name
        FROM dwh.asset_dim
        WHERE asset_type = %s
        ORDER BY symbol;
    """

    conn = get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(sql, (asset_type,))
                rows = cur.fetchall()
    finally:
        conn.close()

    return [
        {
            "asset_type": row["asset_type"],
            "symbol": row["symbol"],
            "name": row.get("name"),
        }
        for row in rows
    ]

def get_all_assets() -> list[dict[str, Any]] | None:
    sql = """
        SELECT asset_type, symbol, name
        FROM dwh.asset_dim
        ORDER BY asset_type, symbol;
    """

    conn = get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                rows = cur.fetchall()
    finally:
        conn.close()

    return [
        {
            "asset_type": row["asset_type"],
            "symbol": row["symbol"],
            "name": row.get("name"),
        }
        for row in rows
    ]
