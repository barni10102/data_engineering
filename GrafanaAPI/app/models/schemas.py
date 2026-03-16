from datetime import datetime
from pydantic import BaseModel
from typing import Optional, List


class PricePoint(BaseModel):
    snapshot_ts: datetime
    close_price: float
    volume: Optional[float] = None
    volume_usd: Optional[float] = None


class AssetPriceSeries(BaseModel):
    asset_type: str
    symbol: str
    name: Optional[str]
    points: List[PricePoint]


class AssetListItem(BaseModel):
    symbol: str
