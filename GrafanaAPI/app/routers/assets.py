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
    return get_assets_by_type("crypto")


@router.get("/stock", response_model=List[AssetListItem])
def list_stock_assets():
    return get_assets_by_type("stock")


@router.get("/", response_model=List[AssetListItem])
def list_all_assets():
    return get_all_assets()


@router.get("/{asset_type}/latest")
def read_latest_assets(asset_type: AssetType):
    return get_latest_assets(asset_type.value)


@router.get("/{asset_type}/{symbol}/prices", response_model=AssetPriceSeries)
def get_asset_prices(
    asset_type: str,
    symbol: str,
    date_from: Optional[datetime] = Query(None, alias="from"),
    date_to: Optional[datetime] = Query(None, alias="to"),
):
    return get_asset_price_series(asset_type=asset_type, symbol=symbol, date_from=date_from, date_to=date_to)


@router.get("/comparison")
def get_assets_comparison(
    symbols: str = Query(..., description="Comma separated list, pl. BTC,ETH,AAPL"),
    date_from: Optional[datetime] = Query(None, alias="from"),
    date_to: Optional[datetime] = Query(None, alias="to"),
):
    symbol_list = [s.strip().upper() for s in symbols.split(",") if s.strip()]
    return get_assets_indexed_series(symbol_list, date_from=date_from, date_to=date_to)
