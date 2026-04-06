"""Router exposing leaderboard-style top-movers endpoints."""

from enum import Enum

from fastapi import APIRouter

from app.services.asset_top_movers_service import get_top_movers


class AssetType(str, Enum):
    crypto = "crypto"
    stock = "stock"
    all = "all"


# Dedicated router for leaderboard-style endpoints consumed by market-movers dashboards.
router = APIRouter(prefix="/assets", tags=["assets-top-movers"])


@router.get("/{asset_type}/top-movers")
def read_top_movers(asset_type: AssetType):
    """Return cached top movers list for crypto, stock, or combined view."""
    return get_top_movers(asset_type.value)
