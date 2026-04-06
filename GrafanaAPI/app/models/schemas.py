from datetime import datetime
from pydantic import BaseModel
from typing import Optional, List


class PricePoint(BaseModel):
    """Single timestamped market observation used in timeseries responses."""
    snapshot_ts: datetime
    close_price: float
    volume: Optional[float] = None
    volume_usd: Optional[float] = None


class AssetPriceSeries(BaseModel):
    """API response model for one asset's historical price series."""
    asset_type: str
    symbol: str
    name: Optional[str]
    points: List[PricePoint]


class AssetListItem(BaseModel):
    """Compact model for symbol-only listing endpoints."""
    symbol: str
