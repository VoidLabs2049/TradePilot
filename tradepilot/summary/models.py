"""Pydantic v2 models for market summary API responses."""

from pydantic import BaseModel


class IndexSnapshot(BaseModel):
    """Real-time snapshot of a single market index."""

    code: str
    name: str
    close: float
    change_pct: float
    change_val: float
    volume: float
    turnover: float


class MarketBreadth(BaseModel):
    """Market-wide up/down/limit statistics."""

    total: int
    up: int
    down: int
    flat: int
    limit_up: int
    limit_up_20: int
    limit_down: int
    limit_down_20: int


class SectorRecord(BaseModel):
    """Single sector (industry or concept) snapshot."""

    code: str
    name: str
    change_pct: float
    up_count: int
    down_count: int
    leader: str


class StockRecord(BaseModel):
    """Single stock change snapshot."""

    code: str
    name: str
    change_pct: float


class DailySummaryResponse(BaseModel):
    """Full daily market summary."""

    date: str
    timestamp: str
    indices: list[IndexSnapshot]
    breadth: MarketBreadth
    industry_top: list[SectorRecord]
    industry_bottom: list[SectorRecord]
    concept_top: list[SectorRecord]
    concept_bottom: list[SectorRecord]
    stocks_top: list[StockRecord]
    stocks_bottom: list[StockRecord]


class RegimeInfo(BaseModel):
    """Market regime classification for 5-minute brief."""

    label: str
    score: float
    drivers: dict


class WatchSectorRecord(BaseModel):
    """Watchlist sector with signal classification."""

    name: str
    matched_name: str
    change_pct: float
    up_count: int
    down_count: int
    strength: float
    status: str


class WatchStockRecord(BaseModel):
    """Watchlist stock with signal classification."""

    code: str
    name: str
    price: float
    change_pct: float
    change_val: float
    turnover_rate: float
    volume_ratio: float
    status: str


class FiveMinBriefResponse(BaseModel):
    """Intraday 5-minute brief response."""

    date: str
    timestamp: str
    regime: RegimeInfo
    sector_watchlist: list[WatchSectorRecord]
    stock_watchlist: list[WatchStockRecord]
    alerts: list[str]


class WatchlistConfig(BaseModel):
    """Watchlist configuration for sectors and stocks."""

    watch_sectors: list[str] = []
    watch_stocks: list[dict] = []


class TradingStatusResponse(BaseModel):
    """Current A-share trading session status."""

    is_trading: bool
    status: str
    next_open: str | None = None
    message: str
