import math
import numpy as np
import pandas as pd

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


def get_forecast_backtest(
    asset_type: str,
    symbol: str,
    date_from: Optional[datetime],
    date_to: Optional[datetime],
    smoothing_span: int = 12,
) -> Dict[str, Any]:
    if asset_type not in ("crypto", "stock"):
        raise HTTPException(status_code=400, detail="Invalid asset_type")

    if smoothing_span < 3 or smoothing_span > 200:
        raise HTTPException(status_code=400, detail="smoothing_span must be between 3 and 200")

    symbol = symbol.strip().upper()
    if not symbol:
        raise HTTPException(status_code=400, detail="Invalid symbol")

    date_from, date_to = _default_from_to(date_from, date_to)

    base_sql = """
        SELECT
            f.snapshot_ts,
            f.close_price
        FROM dwh.asset_dim ad
        JOIN dwh.intraday_price_fact f
          ON f.asset_id = ad.asset_id
        WHERE ad.asset_type = %s
          AND ad.symbol = %s
          AND f.snapshot_ts BETWEEN %s AND %s
        ORDER BY f.snapshot_ts ASC;
    """

    conn = get_postgres_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(base_sql, (asset_type, symbol, date_from, date_to))
            rows = cur.fetchall()

        # Stock weekend fallback: shift to last available trading window.
        if not rows and asset_type == "stock":
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT MAX(f.snapshot_ts) AS last_ts
                    FROM dwh.asset_dim ad
                    JOIN dwh.intraday_price_fact f ON f.asset_id = ad.asset_id
                    WHERE ad.asset_type = 'stock'
                      AND ad.symbol = %s;
                    """,
                    (symbol,),
                )
                last_row = cur.fetchone()

            last_ts = last_row["last_ts"] if last_row else None
            if last_ts is not None:
                window = date_to - date_from
                shifted_to = last_ts
                shifted_from = last_ts - window

                with conn.cursor() as cur:
                    cur.execute(base_sql, (asset_type, symbol, shifted_from, shifted_to))
                    rows = cur.fetchall()

        if not rows:
            raise HTTPException(status_code=404, detail="No data for given symbol / time range")
    finally:
        conn.close()

    df = pd.DataFrame(rows)
    df["snapshot_ts"] = pd.to_datetime(df["snapshot_ts"], utc=True, errors="coerce")
    df["close_price"] = pd.to_numeric(df["close_price"], errors="coerce")
    df = df.dropna(subset=["snapshot_ts", "close_price"]).sort_values("snapshot_ts").drop_duplicates("snapshot_ts")

    if len(df) < 40:
        raise HTTPException(status_code=400, detail="Not enough points for backtest (need at least 40)")

    # --- EDA + Feature extraction ---
    df["ret_1"] = df["close_price"].pct_change()
    df["roll_mean_12"] = df["close_price"].rolling(12, min_periods=3).mean()
    df["roll_std_12"] = df["close_price"].rolling(12, min_periods=3).std()

    ts_local = df["snapshot_ts"].dt.tz_convert("UTC")
    hour = ts_local.dt.hour
    df["hour_sin"] = np.sin(2 * np.pi * hour / 24.0)
    df["hour_cos"] = np.cos(2 * np.pi * hour / 24.0)

    df["lag_ret_1"] = df["ret_1"].shift(1)
    df["lag_ret_3"] = df["ret_1"].shift(3)
    df["lag_ret_6"] = df["ret_1"].shift(6)

    n = len(df)
    train_end = int(n * 0.70)
    val_end = int(n * 0.85)

    # Keep splits sane for short ranges
    train_end = max(train_end, 20)
    val_end = max(val_end, train_end + 5)
    val_end = min(val_end, n - 3)

    if val_end <= train_end or (n - val_end) < 3:
        raise HTTPException(status_code=400, detail="Not enough points after split for validation/test")

    # --- Scaling (train statistics only) ---
    train_slice = df.iloc[:train_end]
    for col in ["ret_1", "roll_std_12", "lag_ret_1", "lag_ret_3", "lag_ret_6"]:
        mu = float(train_slice[col].mean(skipna=True)) if col in train_slice else 0.0
        sigma = float(train_slice[col].std(skipna=True)) if col in train_slice else 0.0
        if sigma > 1e-12:
            df[f"{col}_z"] = (df[col] - mu) / sigma
        else:
            df[f"{col}_z"] = 0.0

    # --- Time-series baseline forecast (EWMA one-step backtest) ---
    df["forecast"] = df["close_price"].ewm(span=smoothing_span, adjust=False).mean().shift(1)
    df["forecast"] = df["forecast"].bfill()

    df["residual"] = df["close_price"] - df["forecast"]
    global_resid_std = float(df["residual"].std(skipna=True))
    if math.isnan(global_resid_std):
        global_resid_std = 0.0

    df["resid_std_rolling"] = df["residual"].rolling(24, min_periods=5).std().fillna(global_resid_std)
    df["lower"] = df["forecast"] - 1.96 * df["resid_std_rolling"]
    df["upper"] = df["forecast"] + 1.96 * df["resid_std_rolling"]

    # Regime label (simple, interpretable)
    vol_z = df["roll_std_12_z"].fillna(0.0)
    ret = df["ret_1"].fillna(0.0)
    df["regime"] = np.select(
        [
            (ret > 0) & (vol_z < 0.5),
            (ret < 0) & (vol_z < 0.5),
            vol_z >= 0.5,
        ],
        [
            "uptrend_low_vol",
            "downtrend_low_vol",
            "high_vol",
        ],
        default="neutral",
    )

    # --- Metrics on test segment ---
    test_df = df.iloc[val_end:].copy()
    test_df = test_df.dropna(subset=["close_price", "forecast"])
    if test_df.empty:
        raise HTTPException(status_code=400, detail="No test points after feature generation")

    abs_err = (test_df["close_price"] - test_df["forecast"]).abs()
    sq_err = (test_df["close_price"] - test_df["forecast"]) ** 2

    mae = float(abs_err.mean())
    rmse = float(math.sqrt(sq_err.mean()))

    denom = test_df["close_price"].replace(0, np.nan).abs()
    mape_series = (abs_err / denom) * 100.0
    mape = float(mape_series.dropna().mean()) if not mape_series.dropna().empty else None

    missing_ratio = float(df[["close_price", "ret_1", "roll_std_12"]].isna().mean().mean())

    points: List[Dict[str, Any]] = []
    for _, r in df.iterrows():
        points.append(
            {
                "snapshot_ts": r["snapshot_ts"],
                "actual": float(r["close_price"]) if pd.notna(r["close_price"]) else None,
                "forecast": float(r["forecast"]) if pd.notna(r["forecast"]) else None,
                "lower": float(r["lower"]) if pd.notna(r["lower"]) else None,
                "upper": float(r["upper"]) if pd.notna(r["upper"]) else None,
                "ret_1": float(r["ret_1"]) if pd.notna(r["ret_1"]) else None,
                "roll_std_12": float(r["roll_std_12"]) if pd.notna(r["roll_std_12"]) else None,
                "regime": str(r["regime"]) if pd.notna(r["regime"]) else "neutral",
            }
        )

    return {
        "asset_type": asset_type,
        "symbol": symbol,
        "from": date_from,
        "to": date_to,
        "split": {
            "train_points": train_end,
            "validation_points": val_end - train_end,
            "test_points": n - val_end,
        },
        "eda": {
            "points_count": int(n),
            "missing_ratio": missing_ratio,
            "close_mean": float(df["close_price"].mean()),
            "close_std": float(df["close_price"].std()),
            "close_min": float(df["close_price"].min()),
            "close_max": float(df["close_price"].max()),
        },
        "metrics": {
            "mae": mae,
            "rmse": rmse,
            "mape": mape,
        },
        "points": points,
    }