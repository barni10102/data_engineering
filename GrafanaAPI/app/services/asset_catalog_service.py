"""Catalog-service helpers for listing available assets from dimension tables."""

from typing import  Any

from fastapi import HTTPException
from app.db.postgres import get_postgres_connection


def get_assets_by_type(asset_type: str) -> list[dict[str, Any]] | None:
    """Return symbol list filtered by asset type."""
    if asset_type not in {"crypto", "stock"}:
        raise HTTPException(status_code=400, detail="Invalid asset_type")

    # asset_dim is the single source of truth for discoverable API symbols.
    sql = """
        SELECT asset_type, symbol, name
        FROM dwh.asset_dim
        WHERE asset_type = %s
        ORDER BY symbol;
    """

    conn = get_postgres_connection()
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
    """Return symbol list across all supported asset types."""
    sql = """
        SELECT asset_type, symbol, name
        FROM dwh.asset_dim
        ORDER BY asset_type, symbol;
    """

    conn = get_postgres_connection()
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
