from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Query

from app.services.asset_analytics_service import (
    get_assets_aggregated_summary,
    get_sp500_sector_stats,
    get_forecast_backtest,
)


router = APIRouter(prefix="/analytics", tags=["analytics"])


@router.get("/assets/summary")
def read_assets_summary(
    asset_type: Optional[str] = Query(None, description="crypto | stock | null(all)"),
    date_from: Optional[datetime] = Query(None, alias="from"),
    date_to: Optional[datetime] = Query(None, alias="to"),
    limit: int = Query(30, ge=1, le=500),
):
    return get_assets_aggregated_summary(
        asset_type=asset_type,
        date_from=date_from,
        date_to=date_to,
        limit=limit,
    )


@router.get("/sp500/sector-stats")
def read_sp500_sector_stats(
    limit: int = Query(20, ge=1, le=200),
    sort_by: str = Query("company_count", description="company_count | avg_marketcap | total_weight | updated_at"),
):
    return get_sp500_sector_stats(limit=limit, sort_by=sort_by)


@router.get("/forecast-backtest/{asset_type}/{symbol}")
def read_forecast_backtest(
    asset_type: str,
    symbol: str,
    date_from: Optional[datetime] = Query(None, alias="from"),
    date_to: Optional[datetime] = Query(None, alias="to"),
    smoothing_span: int = Query(12, ge=3, le=200),
):
    return get_forecast_backtest(
        asset_type=asset_type,
        symbol=symbol,
        date_from=date_from,
        date_to=date_to,
        smoothing_span=smoothing_span,
    )