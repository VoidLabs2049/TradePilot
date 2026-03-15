"""Market snapshot service for A-share summary.

Fetches real-time A-share data from akshare and assembles structured
responses for the daily summary and 5-minute brief API endpoints.
Data is cached with a short TTL to avoid hammering the upstream API.
"""

from __future__ import annotations

import math
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, time as dtime, timedelta

import akshare as ak
import pandas as pd
from loguru import logger

from tradepilot.summary.cache import SnapshotCache
from tradepilot.summary.models import (
    DailySummaryResponse,
    FiveMinBriefResponse,
    IndexSnapshot,
    MarketBreadth,
    RegimeInfo,
    SectorRecord,
    StockRecord,
    TradingStatusResponse,
    WatchSectorRecord,
    WatchStockRecord,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

TARGET_INDICES: list[tuple[str, str]] = [
    ("000001", "上证指数"),
    ("399001", "深证成指"),
    ("399006", "创业板指"),
    ("000688", "科创50"),
    ("000016", "上证50"),
    ("000300", "沪深300"),
]

_DEFAULT_BREADTH = MarketBreadth(
    total=0, up=0, down=0, flat=0,
    limit_up=0, limit_up_20=0, limit_down=0, limit_down_20=0,
)

# Trading session windows (inclusive)
_MORNING_OPEN = dtime(9, 15)
_MORNING_CLOSE = dtime(11, 30)
_AFTERNOON_OPEN = dtime(13, 0)
_AFTERNOON_CLOSE = dtime(15, 15)


# ---------------------------------------------------------------------------
# Safe conversion helpers (ported from fetch_a_share.py)
# ---------------------------------------------------------------------------


def _safe_float(value: object, default: float = 0.0) -> float:
    """Convert scalar value to float safely."""
    if value is None:
        return default
    if isinstance(value, str):
        cleaned = value.strip().replace(",", "").replace("%", "")
        if cleaned in {"", "-", "--", "nan", "NaN", "None"}:
            return default
        value = cleaned
    try:
        number = float(value)
    except (TypeError, ValueError):
        return default
    if math.isnan(number) or math.isinf(number):
        return default
    return number


def _safe_int(value: object, default: int = 0) -> int:
    """Convert scalar value to int safely."""
    return int(_safe_float(value, float(default)))


def _safe_str(value: object, default: str = "") -> str:
    """Convert scalar value to string safely."""
    if value is None:
        return default
    text = str(value).strip()
    if text in {"", "-", "--", "nan", "NaN", "None"}:
        return default
    return text


def _normalize_code(code: object) -> str:
    """Normalize security code into plain numeric string."""
    value = _safe_str(code)
    if not value:
        return ""
    if "." in value:
        value = value.split(".")[-1]
    return value[-6:] if value[-6:].isdigit() else value


def _clip(value: float, lower: float, upper: float) -> float:
    """Clip numeric value into a bounded range."""
    return max(lower, min(upper, value))


# ---------------------------------------------------------------------------
# Data fetching
# ---------------------------------------------------------------------------


def _fetch_indices() -> list[IndexSnapshot]:
    """Fetch major index snapshots from East Money / Sina."""
    def _default(code: str, name: str) -> IndexSnapshot:
        return IndexSnapshot(
            code=code, name=name, close=0.0,
            change_pct=0.0, change_val=0.0, volume=0.0, turnover=0.0,
        )

    em_df: pd.DataFrame | None = None
    sina_df: pd.DataFrame | None = None

    try:
        em_df = ak.stock_zh_index_spot_em()
    except Exception as exc:
        logger.warning("indices em fetch failed: {}", exc)

    try:
        sina_df = ak.stock_zh_index_spot_sina()
    except Exception as exc:
        logger.warning("indices sina fetch failed: {}", exc)

    if em_df is None and sina_df is None:
        return [_default(c, n) for c, n in TARGET_INDICES]

    # Build lookup maps
    em_code_map: dict[str, pd.Series] = {}
    em_name_map: dict[str, pd.Series] = {}
    if em_df is not None:
        for _, row in em_df.iterrows():
            em_code_map[_normalize_code(row.get("代码"))] = row
            em_name_map[_safe_str(row.get("名称"))] = row

    sina_code_map: dict[str, pd.Series] = {}
    sina_name_map: dict[str, pd.Series] = {}
    if sina_df is not None:
        for _, row in sina_df.iterrows():
            sina_code_map[_normalize_code(row.get("代码"))] = row
            sina_name_map[_safe_str(row.get("名称"))] = row

    results: list[IndexSnapshot] = []
    for code, name in TARGET_INDICES:
        row = em_code_map.get(code) or em_name_map.get(name)
        if row is not None:
            results.append(IndexSnapshot(
                code=_normalize_code(row.get("代码")) or code,
                name=_safe_str(row.get("名称")) or name,
                close=_safe_float(row.get("最新价")),
                change_pct=_safe_float(row.get("涨跌幅")),
                change_val=_safe_float(row.get("涨跌额")),
                volume=_safe_float(row.get("成交量")),
                turnover=_safe_float(row.get("成交额")),
            ))
            continue

        row = sina_code_map.get(code) or sina_name_map.get(name)
        if row is not None:
            results.append(IndexSnapshot(
                code=_normalize_code(row.get("代码")) or code,
                name=_safe_str(row.get("名称")) or name,
                close=_safe_float(row.get("最新价")),
                change_pct=_safe_float(row.get("涨跌幅")),
                change_val=_safe_float(row.get("涨跌额")),
                volume=_safe_float(row.get("成交量")),
                turnover=_safe_float(row.get("成交额")),
            ))
            continue

        results.append(_default(code, name))

    return results


def _fetch_sectors(
    is_industry: bool,
    top_n: int = 20,
    ascending: bool = False,
) -> list[SectorRecord]:
    """Fetch sector performance (industry or concept board)."""
    try:
        df = (
            ak.stock_board_industry_name_em()
            if is_industry
            else ak.stock_board_concept_name_em()
        )
    except Exception as exc:
        label = "industry" if is_industry else "concept"
        logger.warning("{} sectors fetch failed: {}", label, exc)
        return []

    records: list[dict] = []
    for _, row in df.iterrows():
        records.append({
            "code": _safe_str(row.get("板块代码")),
            "name": _safe_str(row.get("板块名称")),
            "change_pct": _safe_float(row.get("涨跌幅")),
            "up_count": _safe_int(row.get("上涨家数")),
            "down_count": _safe_int(row.get("下跌家数")),
            "leader": _safe_str(row.get("领涨股票")),
        })

    records.sort(key=lambda x: x["change_pct"], reverse=not ascending)
    return [SectorRecord(**r) for r in records[:top_n]]


def _fetch_all_stocks_snapshot() -> pd.DataFrame:
    """Fetch full A-share snapshot."""
    return ak.stock_zh_a_spot_em()


def _parse_stock_changes(
    df: pd.DataFrame,
) -> tuple[list[dict], list[float], int]:
    """Normalize stock snapshot rows into stock list and change series."""
    stocks: list[dict] = []
    changes: list[float] = []
    skipped = 0

    for _, row in df.iterrows():
        change = _safe_float(row.get("涨跌幅"), default=float("nan"))
        if math.isnan(change):
            skipped += 1
            continue

        stocks.append({
            "code": _normalize_code(row.get("代码")),
            "name": _safe_str(row.get("名称")),
            "change_pct": change,
        })
        changes.append(change)

    return stocks, changes, skipped


def _compute_breadth(changes: list[float], skipped: int) -> MarketBreadth:
    """Compute breadth statistics from normalized change list."""
    total = len(changes)
    return MarketBreadth(
        total=total,
        up=sum(1 for c in changes if c > 0),
        down=sum(1 for c in changes if c < 0),
        flat=sum(1 for c in changes if c == 0),
        limit_up=sum(1 for c in changes if c >= 9.9),
        limit_up_20=sum(1 for c in changes if c >= 19.9),
        limit_down=sum(1 for c in changes if c <= -9.9),
        limit_down_20=sum(1 for c in changes if c <= -19.9),
    )


def _sort_records(data: list[dict], n: int, ascending: bool) -> list[StockRecord]:
    """Sort by change_pct and return top n as StockRecord list."""
    sorted_data = sorted(data, key=lambda x: x["change_pct"], reverse=not ascending)
    return [StockRecord(**d) for d in sorted_data[:n]]


# ---------------------------------------------------------------------------
# 5-minute brief helpers
# ---------------------------------------------------------------------------


def _compute_market_regime(
    indices: list[IndexSnapshot],
    breadth: MarketBreadth,
) -> RegimeInfo:
    """Compute market regime score and label."""
    hs300 = next((idx for idx in indices if idx.name == "沪深300"), None)
    cyb = next((idx for idx in indices if idx.name == "创业板指"), None)

    hs300_pct = hs300.change_pct if hs300 else 0.0
    cyb_pct = cyb.change_pct if cyb else 0.0
    index_component = ((hs300_pct + cyb_pct) / 2.0) * 20

    total = breadth.total
    breadth_component = ((breadth.up - breadth.down) / total * 100) if total else 0.0
    limit_component = (
        (breadth.limit_up - breadth.limit_down) / total * 1000
    ) if total else 0.0

    score = _clip(
        index_component * 0.4 + breadth_component * 0.4 + limit_component * 0.2,
        -100.0, 100.0,
    )

    if score >= 20:
        label = "risk_on"
    elif score <= -20:
        label = "risk_off"
    else:
        label = "neutral"

    return RegimeInfo(
        label=label,
        score=round(score, 2),
        drivers={
            "hs300_change_pct": round(hs300_pct, 2),
            "cyb_change_pct": round(cyb_pct, 2),
            "up_down_diff": breadth.up - breadth.down,
            "limit_up_down_diff": breadth.limit_up - breadth.limit_down,
        },
    )


def _find_sector_match(
    concepts: list[SectorRecord], watch_name: str,
) -> SectorRecord | None:
    """Find watch sector by exact or fuzzy name match."""
    watch_lower = watch_name.lower()
    for item in concepts:
        if item.name == watch_name:
            return item
    for item in concepts:
        if watch_lower in item.name.lower() or item.name.lower() in watch_lower:
            return item
    return None


def _extract_watch_sectors(
    concepts: list[SectorRecord], watch_names: list[str],
) -> list[WatchSectorRecord]:
    """Extract watchlist sector records and classify signal strength."""
    results: list[WatchSectorRecord] = []

    for watch_name in watch_names:
        matched = _find_sector_match(concepts, watch_name)
        if matched is None:
            results.append(WatchSectorRecord(
                name=watch_name, matched_name="", change_pct=0.0,
                up_count=0, down_count=0, strength=0.0, status="missing",
            ))
            continue

        total = matched.up_count + matched.down_count
        strength = matched.up_count / total if total > 0 else 0.0

        status = "neutral"
        if matched.change_pct >= 2.0 and strength >= 0.6:
            status = "strong"
        elif matched.change_pct <= -2.0 and strength <= 0.4:
            status = "weak"

        results.append(WatchSectorRecord(
            name=watch_name,
            matched_name=matched.name,
            change_pct=round(matched.change_pct, 2),
            up_count=matched.up_count,
            down_count=matched.down_count,
            strength=round(strength, 2),
            status=status,
        ))

    return results


def _extract_watch_stocks(
    snapshot_df: pd.DataFrame, watch_stocks: list[dict],
) -> list[WatchStockRecord]:
    """Extract watchlist stock records and classify signal status."""
    code_map: dict[str, pd.Series] = {
        _normalize_code(row.get("代码")): row
        for _, row in snapshot_df.iterrows()
    }

    results: list[WatchStockRecord] = []
    for watch in watch_stocks:
        code = _normalize_code(watch.get("code"))
        watch_name = _safe_str(watch.get("name"))
        row = code_map.get(code)

        if row is None:
            results.append(WatchStockRecord(
                code=code, name=watch_name, price=0.0, change_pct=0.0,
                change_val=0.0, turnover_rate=0.0, volume_ratio=0.0,
                status="missing",
            ))
            continue

        change_pct = _safe_float(row.get("涨跌幅"))
        turnover_rate = _safe_float(row.get("换手率"))
        volume_ratio = _safe_float(row.get("量比"))

        status = "watch"
        if change_pct >= 3.0:
            status = "breakout"
        elif change_pct <= -3.0:
            status = "breakdown"
        elif abs(change_pct) >= 1.5 and (turnover_rate >= 5.0 or volume_ratio >= 1.5):
            status = "active"

        results.append(WatchStockRecord(
            code=code,
            name=_safe_str(row.get("名称")) or watch_name,
            price=round(_safe_float(row.get("最新价")), 2),
            change_pct=round(change_pct, 2),
            change_val=round(_safe_float(row.get("涨跌额")), 2),
            turnover_rate=round(turnover_rate, 2),
            volume_ratio=round(volume_ratio, 2),
            status=status,
        ))

    return results


def _build_5m_alerts(
    regime: RegimeInfo,
    sector_watchlist: list[WatchSectorRecord],
    stock_watchlist: list[WatchStockRecord],
) -> list[str]:
    """Build human-readable alerts from 5m regime and watchlist signals."""
    alerts: list[str] = []

    if regime.label == "risk_on":
        alerts.append(f"市场偏强（score={regime.score:+.1f}）")
    elif regime.label == "risk_off":
        alerts.append(f"市场偏弱（score={regime.score:+.1f}）")

    for sector in sector_watchlist:
        name = sector.matched_name or sector.name
        if sector.status == "strong":
            alerts.append(f"板块走强：{name} ({sector.change_pct:+.2f}%)")
        elif sector.status == "weak":
            alerts.append(f"板块走弱：{name} ({sector.change_pct:+.2f}%)")

    for stock in stock_watchlist:
        if stock.status in {"breakout", "breakdown", "active"}:
            alerts.append(
                f"个股{stock.status}：{stock.name} {stock.code} ({stock.change_pct:+.2f}%)"
            )

    return alerts


# ---------------------------------------------------------------------------
# Trading status
# ---------------------------------------------------------------------------


def get_trading_status() -> TradingStatusResponse:
    """Determine current A-share trading session status."""
    now = datetime.now()
    current_time = now.time()
    weekday = now.weekday()  # 0=Mon ... 6=Sun

    # Weekend
    if weekday >= 5:
        days_until_monday = 7 - weekday
        next_open = (now + timedelta(days=days_until_monday)).replace(
            hour=9, minute=15, second=0, microsecond=0,
        )
        return TradingStatusResponse(
            is_trading=False,
            status="closed",
            next_open=next_open.isoformat(),
            message="周末休市",
        )

    # Before morning open
    if current_time < _MORNING_OPEN:
        next_open = now.replace(hour=9, minute=15, second=0, microsecond=0)
        return TradingStatusResponse(
            is_trading=False,
            status="pre_market",
            next_open=next_open.isoformat(),
            message="盘前",
        )

    # Morning session
    if _MORNING_OPEN <= current_time <= _MORNING_CLOSE:
        return TradingStatusResponse(
            is_trading=True,
            status="trading",
            next_open=None,
            message="上午交易中",
        )

    # Lunch break
    if _MORNING_CLOSE < current_time < _AFTERNOON_OPEN:
        next_open = now.replace(hour=13, minute=0, second=0, microsecond=0)
        return TradingStatusResponse(
            is_trading=False,
            status="lunch_break",
            next_open=next_open.isoformat(),
            message="午间休市",
        )

    # Afternoon session
    if _AFTERNOON_OPEN <= current_time <= _AFTERNOON_CLOSE:
        return TradingStatusResponse(
            is_trading=True,
            status="trading",
            next_open=None,
            message="下午交易中",
        )

    # After close
    tomorrow = now + timedelta(days=1)
    if tomorrow.weekday() >= 5:
        days_until_monday = 7 - tomorrow.weekday()
        tomorrow = tomorrow + timedelta(days=days_until_monday)
    next_open = tomorrow.replace(hour=9, minute=15, second=0, microsecond=0)
    return TradingStatusResponse(
        is_trading=False,
        status="closed",
        next_open=next_open.isoformat(),
        message="已收盘",
    )


# ---------------------------------------------------------------------------
# Service class
# ---------------------------------------------------------------------------


class MarketSnapshotService:
    """Aggregated market snapshot service with TTL caching.

    Provides ``get_daily_summary`` and ``get_5m_brief`` methods that
    fetch data from akshare, assemble structured responses, and cache
    results for 60 seconds to avoid upstream rate limits.
    """

    def __init__(self, cache_ttl: int = 60) -> None:
        self._cache = SnapshotCache(ttl_seconds=cache_ttl)

    def get_daily_summary(
        self,
        industry_top_n: int = 10,
        industry_bottom_n: int = 10,
        concept_top_n: int = 15,
        concept_bottom_n: int = 15,
    ) -> DailySummaryResponse:
        """Fetch full daily market summary.

        Args:
            industry_top_n: Number of top gaining industries.
            industry_bottom_n: Number of top losing industries.
            concept_top_n: Number of top gaining concepts.
            concept_bottom_n: Number of top losing concepts.
        """
        now = datetime.now()
        today = now.strftime("%Y-%m-%d")
        cache_key = f"daily:{today}"
        cached = self._cache.get(cache_key)
        if cached is not None:
            logger.debug("daily summary cache hit")
            return cached

        logger.info("fetching daily summary data from akshare")

        with ThreadPoolExecutor(max_workers=8) as pool:
            f_indices = pool.submit(_fetch_indices)
            f_ind_top = pool.submit(_fetch_sectors, True, industry_top_n, False)
            f_ind_bot = pool.submit(_fetch_sectors, True, industry_bottom_n, True)
            f_con_top = pool.submit(_fetch_sectors, False, concept_top_n, False)
            f_con_bot = pool.submit(_fetch_sectors, False, concept_bottom_n, True)
            f_snapshot = pool.submit(_fetch_all_stocks_snapshot)

        indices = _safe_future(f_indices, [])
        ind_top = _safe_future(f_ind_top, [])
        ind_bot = _safe_future(f_ind_bot, [])
        con_top = _safe_future(f_con_top, [])
        con_bot = _safe_future(f_con_bot, [])
        snapshot_df = _safe_future(f_snapshot, pd.DataFrame())

        stocks, changes, skipped = _parse_stock_changes(snapshot_df)
        breadth = _compute_breadth(changes, skipped) if changes else _DEFAULT_BREADTH

        response = DailySummaryResponse(
            date=today,
            timestamp=now.strftime("%Y-%m-%d %H:%M"),
            indices=indices,
            breadth=breadth,
            industry_top=ind_top,
            industry_bottom=ind_bot,
            concept_top=con_top,
            concept_bottom=con_bot,
            stocks_top=_sort_records(stocks, 10, False),
            stocks_bottom=_sort_records(stocks, 10, True),
        )

        self._cache.set(cache_key, response)
        return response

    def get_5m_brief(
        self,
        watch_sectors: list[str],
        watch_stocks: list[dict],
    ) -> FiveMinBriefResponse:
        """Fetch intraday 5-minute brief.

        Args:
            watch_sectors: List of sector names to watch.
            watch_stocks: List of dicts with ``code`` and ``name`` keys.
        """
        now = datetime.now()
        today = now.strftime("%Y-%m-%d")
        minute_bucket = now.strftime("%H:%M")
        cache_key = f"5m:{today}:{minute_bucket}"
        cached = self._cache.get(cache_key)
        if cached is not None:
            logger.debug("5m brief cache hit")
            return cached

        logger.info("fetching 5m brief data from akshare")

        with ThreadPoolExecutor(max_workers=4) as pool:
            f_indices = pool.submit(_fetch_indices)
            f_concepts = pool.submit(_fetch_sectors, False, 1000, False)
            f_snapshot = pool.submit(_fetch_all_stocks_snapshot)

        indices = _safe_future(f_indices, [])
        concepts = _safe_future(f_concepts, [])
        snapshot_df = _safe_future(f_snapshot, pd.DataFrame())

        stocks, changes, skipped = _parse_stock_changes(snapshot_df)
        breadth = _compute_breadth(changes, skipped) if changes else _DEFAULT_BREADTH

        regime = _compute_market_regime(indices, breadth)
        sector_watchlist = _extract_watch_sectors(concepts, watch_sectors)
        stock_watchlist = _extract_watch_stocks(snapshot_df, watch_stocks)
        alerts = _build_5m_alerts(regime, sector_watchlist, stock_watchlist)

        response = FiveMinBriefResponse(
            date=today,
            timestamp=now.strftime("%Y-%m-%d %H:%M"),
            regime=regime,
            sector_watchlist=sector_watchlist,
            stock_watchlist=stock_watchlist,
            alerts=alerts,
        )

        self._cache.set(cache_key, response)
        return response


def _safe_future(future: object, default: object) -> object:
    """Extract future result with fallback on exception."""
    try:
        return future.result()
    except Exception as exc:
        logger.warning("concurrent task failed: {}", exc)
        return default
