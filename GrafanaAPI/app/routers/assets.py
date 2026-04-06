"""Assets API router for discovery, latest snapshots, and timeseries comparisons."""

from enum import Enum
from datetime import datetime
from fastapi import APIRouter, Query
from app.services.asset_service import get_latest_assets
from typing import Optional, List
from app.models.schemas import AssetPriceSeries, AssetListItem
from app.services.asset_timeseries_service import get_asset_price_series, get_assets_indexed_series
from app.services.asset_catalog_service import get_assets_by_type, get_all_assets


class AssetType(str, Enum):
    crypto = "crypto"
    stock = "stock"


router = APIRouter(prefix="/assets", tags=["assets"])

@router.get("/crypto", response_model=List[AssetListItem])
def list_crypto_assets():
    """List available crypto symbols for selectors and drilldown variables."""
    return get_assets_by_type("crypto")


@router.get("/stock", response_model=List[AssetListItem])
def list_stock_assets():
    """List available stock symbols for selectors and drilldown variables."""
    return get_assets_by_type("stock")


@router.get("/", response_model=List[AssetListItem])
def list_all_assets():
    """List all symbols across asset classes."""
    return get_all_assets()


@router.get("/{asset_type}/latest")
def read_latest_assets(asset_type: AssetType):
    """Return latest cached (or DB fallback) snapshot batch by asset type."""
    return get_latest_assets(asset_type.value)


@router.get("/{asset_type}/{symbol}/prices", response_model=AssetPriceSeries)
def get_asset_prices(
    asset_type: str,
    symbol: str,
    date_from: Optional[datetime] = Query(None, alias="from"),
    date_to: Optional[datetime] = Query(None, alias="to"),
):
    """Return detailed timeseries points for one asset in a selected interval."""
    return get_asset_price_series(asset_type=asset_type, symbol=symbol, date_from=date_from, date_to=date_to)


@router.get("/comparison")
def get_assets_comparison(
    symbols: str = Query(..., description="Comma separated list, pl. BTC,ETH,AAPL"),
    date_from: Optional[datetime] = Query(None, alias="from"),
    date_to: Optional[datetime] = Query(None, alias="to"),
):
    """Return normalized indexed series for multi-asset performance comparison."""
    # Query accepts comma-separated symbols to keep Grafana variable wiring simple.
    symbol_list = [s.strip().upper() for s in symbols.split(",") if s.strip()]
    return get_assets_indexed_series(symbol_list, date_from=date_from, date_to=date_to)
