"""API routes for A-share market summary."""

from __future__ import annotations

import json
from pathlib import Path

from fastapi import APIRouter, Query
from loguru import logger

from tradepilot.config import DATA_ROOT
from tradepilot.summary.models import (
    DailySummaryResponse,
    FiveMinBriefResponse,
    TradingStatusResponse,
    WatchlistConfig,
)
from tradepilot.summary.service import MarketSnapshotService, get_trading_status

router = APIRouter()

_service = MarketSnapshotService(cache_ttl=60)
_WATCHLIST_PATH = DATA_ROOT / "watchlist.json"

_DEFAULT_WATCHLIST = WatchlistConfig(
    watch_sectors=["AI应用", "算力", "机器人概念", "半导体"],
    watch_stocks=[
        {"code": "600519", "name": "贵州茅台"},
        {"code": "300750", "name": "宁德时代"},
    ],
)


def _load_watchlist() -> WatchlistConfig:
    """Load watchlist from JSON file, creating default if missing."""
    if not _WATCHLIST_PATH.exists():
        _save_watchlist(_DEFAULT_WATCHLIST)
        return _DEFAULT_WATCHLIST
    try:
        data = json.loads(_WATCHLIST_PATH.read_text(encoding="utf-8"))
        return WatchlistConfig(**data)
    except Exception as exc:
        logger.warning("failed to load watchlist: {}, using default", exc)
        return _DEFAULT_WATCHLIST


def _save_watchlist(config: WatchlistConfig) -> None:
    """Persist watchlist to JSON file."""
    _WATCHLIST_PATH.parent.mkdir(parents=True, exist_ok=True)
    _WATCHLIST_PATH.write_text(
        json.dumps(config.model_dump(), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


@router.get("/daily", response_model=DailySummaryResponse)
def daily_summary(
    industry_top_n: int = Query(10, ge=1, le=50, description="Top N gaining industries"),
    industry_bottom_n: int = Query(10, ge=1, le=50, description="Top N losing industries"),
    concept_top_n: int = Query(15, ge=1, le=50, description="Top N gaining concepts"),
    concept_bottom_n: int = Query(15, ge=1, le=50, description="Top N losing concepts"),
) -> DailySummaryResponse:
    """Fetch full daily market summary."""
    return _service.get_daily_summary(
        industry_top_n=industry_top_n,
        industry_bottom_n=industry_bottom_n,
        concept_top_n=concept_top_n,
        concept_bottom_n=concept_bottom_n,
    )


@router.get("/5m", response_model=FiveMinBriefResponse)
def five_min_brief(
    sectors: str = Query("", description="Comma-separated sector names to watch"),
    stocks: str = Query("", description="Comma-separated stock codes to watch (code:name)"),
) -> FiveMinBriefResponse:
    """Fetch intraday 5-minute brief.

    If ``sectors`` and ``stocks`` query params are empty, falls back to the
    persisted watchlist configuration.
    """
    if sectors:
        watch_sectors = [s.strip() for s in sectors.split(",") if s.strip()]
    else:
        watch_sectors = []

    if stocks:
        watch_stocks = []
        for token in stocks.split(","):
            token = token.strip()
            if not token:
                continue
            if ":" in token:
                code, name = token.split(":", 1)
                watch_stocks.append({"code": code.strip(), "name": name.strip()})
            else:
                watch_stocks.append({"code": token, "name": ""})
    else:
        watch_stocks = []

    # Fall back to persisted watchlist if no params provided
    if not watch_sectors and not watch_stocks:
        config = _load_watchlist()
        watch_sectors = config.watch_sectors
        watch_stocks = config.watch_stocks

    return _service.get_5m_brief(
        watch_sectors=watch_sectors,
        watch_stocks=watch_stocks,
    )


@router.get("/trading-status", response_model=TradingStatusResponse)
def trading_status() -> TradingStatusResponse:
    """Get current A-share trading session status."""
    return get_trading_status()


@router.get("/watchlist", response_model=WatchlistConfig)
def get_watchlist() -> WatchlistConfig:
    """Get current watchlist configuration."""
    return _load_watchlist()


@router.put("/watchlist", response_model=WatchlistConfig)
def update_watchlist(config: WatchlistConfig) -> WatchlistConfig:
    """Update watchlist configuration."""
    _save_watchlist(config)
    logger.info("watchlist updated: {} sectors, {} stocks",
                len(config.watch_sectors), len(config.watch_stocks))
    return config
