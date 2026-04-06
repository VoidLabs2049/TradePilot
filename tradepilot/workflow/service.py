"""Workflow service for pre-market and post-market daily operations."""

from __future__ import annotations

import json
import time
from datetime import date, datetime
from importlib import import_module

import duckdb

from loguru import logger

from tradepilot.db import get_conn
from tradepilot.ingestion.models import NewsSyncRequest, SyncRequest
from tradepilot.ingestion.service import IngestionService
from tradepilot.scanner.daily import DailyScanner, normalize_scan_date
from tradepilot.summary.models import WatchlistConfig
from tradepilot.workflow.models import (
    WorkflowHistoryItem,
    WorkflowPhase,
    WorkflowRunRecord,
    WorkflowStatus,
    WorkflowStepResult,
    WorkflowSummary,
    WorkflowTrigger,
)

_DEFAULT_INDEX_CODES = ["000001", "399001", "399006", "000688"]


class DailyWorkflowService:
    """Coordinate simplified pre-market and post-market workflows."""

    def __init__(self) -> None:
        self._scanner = DailyScanner()
        self._ingestion = IngestionService()
        self._tushare = import_module("tradepilot.data.tushare_client").TushareClient()
        self._summary_api = import_module("tradepilot.api.summary")

    def run_pre_market_workflow(
        self,
        workflow_date: str | None = None,
        triggered_by: WorkflowTrigger = WorkflowTrigger.MANUAL,
    ) -> WorkflowRunRecord:
        """Run the pre-market workflow and persist the resulting snapshot."""
        started_at = datetime.now()
        requested_date, resolved_date, date_resolution = self._resolve_pre_market_date(workflow_date)
        steps: list[WorkflowStepResult] = []
        error_messages: list[str] = []
        previous_post_market = self.get_latest_run(WorkflowPhase.POST_MARKET)
        alerts = self._scanner.list_alerts(unread_only=False)[:10]
        watchlist = self._load_watchlist().model_dump()
        news_items: list[dict] = []

        watch_context = self._build_watch_context(watchlist)

        if not self._should_run_for_trading_day(resolved_date):
            carry_over = self._build_carry_over(previous_post_market)
            summary = WorkflowSummary(
                title="盘前准备已跳过",
                overview="非交易日，未执行盘前工作流。",
                requested_date=requested_date,
                resolved_date=resolved_date,
                date_resolution=date_resolution,
                yesterday_recap=self._build_yesterday_recap(carry_over),
                overnight_news={"summary": "非交易日未执行夜间信息同步", "highlights": []},
                today_watchlist=self._build_today_watchlist(watch_context, alerts, carry_over),
                action_frame=self._build_action_frame(carry_over, alerts),
                watch_context=watch_context,
                alerts=alerts[:5],
                metadata={"data_sources": ["workflow_runs", "alerts", "watchlist"]},
                watchlist=watchlist,
                carry_over=carry_over,
                steps=[
                    WorkflowStepResult(
                        name="trading_day_check",
                        status=WorkflowStatus.SKIPPED.value,
                        details={"reason": "non-trading day"},
                    )
                ],
            )
            return self._persist_run(
                workflow_date=resolved_date,
                phase=WorkflowPhase.PRE_MARKET,
                triggered_by=triggered_by,
                status=WorkflowStatus.SKIPPED,
                started_at=started_at,
                finished_at=datetime.now(),
                summary=summary,
                error_message="non-trading day",
            )

        news_result = self._ingestion.sync_news(NewsSyncRequest())
        news_status = news_result.run.status.value
        news_step_status = WorkflowStatus.SUCCESS.value if news_status == "success" else WorkflowStatus.FAILED.value
        if news_step_status == WorkflowStatus.FAILED.value:
            error_messages.append(news_result.run.error_message or "news sync failed")
        steps.append(
            WorkflowStepResult(
                name="news_sync",
                status=news_step_status,
                records_affected=news_result.run.records_inserted + news_result.run.records_updated,
                error_message=news_result.run.error_message,
            )
        )
        news_items = self._get_latest_news(limit=8)

        status = self._resolve_status(steps)
        carry_over = self._build_carry_over(previous_post_market)
        overview = self._build_pre_market_overview(
            previous_post_market,
            news_items,
            watchlist,
            alerts,
            requested_date,
            resolved_date,
            date_resolution,
        )
        summary = WorkflowSummary(
            title="盘前准备",
            overview=overview,
            requested_date=requested_date,
            resolved_date=resolved_date,
            date_resolution=date_resolution,
            yesterday_recap=self._build_yesterday_recap(carry_over),
            overnight_news=self._build_overnight_news(news_step_status, news_items),
            today_watchlist=self._build_today_watchlist(watch_context, alerts, carry_over),
            action_frame=self._build_action_frame(carry_over, alerts),
            watch_context=watch_context,
            alerts=alerts[:8],
            metadata={
                "data_sources": ["workflow_runs", "news_items", "alerts", "watchlist"],
                "has_previous_post_market": previous_post_market is not None,
            },
            news={
                "status": news_step_status,
                "items": news_items,
            },
            watchlist=watchlist,
            carry_over=carry_over,
            scheduler={
                "has_previous_post_market": previous_post_market is not None,
            },
            steps=steps,
        )
        return self._persist_run(
            workflow_date=resolved_date,
            phase=WorkflowPhase.PRE_MARKET,
            triggered_by=triggered_by,
            status=status,
            started_at=started_at,
            finished_at=datetime.now(),
            summary=summary,
            error_message="；".join(error_messages) or None,
        )

    def run_post_market_workflow(
        self,
        workflow_date: str | None = None,
        triggered_by: WorkflowTrigger = WorkflowTrigger.MANUAL,
    ) -> WorkflowRunRecord:
        """Run the post-market workflow and persist the resulting snapshot."""
        started_at = datetime.now()
        requested_date, resolved_date, date_resolution = self._resolve_post_market_date(workflow_date)
        steps: list[WorkflowStepResult] = []
        error_messages: list[str] = []
        watchlist = self._load_watchlist().model_dump()

        watch_context = self._build_watch_context(watchlist)

        if not self._should_run_for_trading_day(resolved_date):
            summary = WorkflowSummary(
                title="盘后复盘已跳过",
                overview="非交易日，未执行盘后工作流。",
                requested_date=requested_date,
                resolved_date=resolved_date,
                date_resolution=date_resolution,
                market_overview={"summary": "非交易日，未生成市场大势。", "indices": []},
                sector_positioning={"market_leaders": [], "market_laggards": [], "watch_sectors": []},
                position_health={"portfolio_health_summary": "非交易日，未生成持仓健康度。", "tracked_items": []},
                next_day_prep={"market_bias": "observe", "focus_sectors": [], "focus_items": [], "risk_notes": []},
                watch_context=watch_context,
                metadata={"data_sources": ["watchlist"]},
                watchlist=watchlist,
                steps=[
                    WorkflowStepResult(
                        name="trading_day_check",
                        status=WorkflowStatus.SKIPPED.value,
                        details={"reason": "non-trading day"},
                    )
                ],
            )
            return self._persist_run(
                workflow_date=resolved_date,
                phase=WorkflowPhase.POST_MARKET,
                triggered_by=triggered_by,
                status=WorkflowStatus.SKIPPED,
                started_at=started_at,
                finished_at=datetime.now(),
                summary=summary,
                error_message="non-trading day",
            )

        stock_codes, index_codes = self._build_post_market_targets(watchlist)
        try:
            market_request = SyncRequest(
                start_date=resolved_date,
                end_date=resolved_date,
                stock_codes=stock_codes,
                index_codes=index_codes,
                full_refresh=False,
            )
            market_result = self._ingestion.sync_market(market_request)
            market_status = market_result.run.status.value
            market_step_status = WorkflowStatus.SUCCESS.value if market_status == "success" else WorkflowStatus.FAILED.value
            market_records = market_result.run.records_inserted + market_result.run.records_updated
            market_error = market_result.run.error_message
        except Exception as exc:
            logger.exception("post-market workflow market sync failed before run persisted")
            market_step_status = WorkflowStatus.FAILED.value
            market_records = 0
            market_error = str(exc)
        if market_step_status == WorkflowStatus.FAILED.value:
            error_messages.append(market_error or "market sync failed")
        steps.append(
            WorkflowStepResult(
                name="market_sync",
                status=market_step_status,
                records_affected=market_records,
                error_message=market_error,
                details={
                    "stock_codes": stock_codes,
                    "index_codes": index_codes,
                },
            )
        )

        alerts = self._scanner.list_alerts(unread_only=False)[:10]
        try:
            market_overview = self._build_market_overview(resolved_date, steps)
            sector_positioning = self._build_sector_positioning(resolved_date, watch_context)
            position_health = self._build_position_health(resolved_date, watch_context)
            next_day_prep = self._build_next_day_prep(sector_positioning, position_health, market_overview)
            briefing_step_status = WorkflowStatus.SUCCESS.value
            briefing_records = len(position_health.get("tracked_items", [])) + len(sector_positioning.get("watch_sectors", []))
            briefing_error = None
        except Exception as exc:
            logger.exception("post-market workflow briefing build failed")
            market_overview = self._build_market_overview_fallback(resolved_date)
            sector_positioning = {"market_leaders": [], "market_laggards": [], "watch_sectors": [], "observation_focus": []}
            position_health = {"portfolio_health_summary": "盘后复盘生成失败。", "sector_health": [], "tracked_items": []}
            next_day_prep = {"market_bias": "observe", "focus_sectors": [], "focus_items": [], "risk_notes": [], "tomorrow_checkpoints": []}
            briefing_step_status = WorkflowStatus.FAILED.value
            briefing_records = 0
            briefing_error = str(exc)
            error_messages.append(briefing_error)
        steps.append(
            WorkflowStepResult(
                name="post_briefing_build",
                status=briefing_step_status,
                records_affected=briefing_records,
                error_message=briefing_error,
            )
        )

        status = self._resolve_status(steps)
        overview = self._build_post_market_overview(
            market_overview,
            sector_positioning,
            position_health,
            requested_date,
            resolved_date,
            date_resolution,
        )
        summary = WorkflowSummary(
            title="盘后复盘",
            overview=overview,
            requested_date=requested_date,
            resolved_date=resolved_date,
            date_resolution=date_resolution,
            market_overview=market_overview,
            sector_positioning=sector_positioning,
            position_health=position_health,
            next_day_prep=next_day_prep,
            watch_context=watch_context,
            alerts=alerts[:8],
            metadata={
                "data_sources": ["market_sync", "workflow_market_snapshot", "alerts", "watchlist", "portfolio"],
                "steps_completed": len([step for step in steps if step.status == WorkflowStatus.SUCCESS.value]),
                "steps_total": len(steps),
            },
            watchlist=watchlist,
            scheduler={
                "steps_completed": len([step for step in steps if step.status == WorkflowStatus.SUCCESS.value]),
                "steps_total": len(steps),
            },
            steps=steps,
        )
        return self._persist_run(
            workflow_date=resolved_date,
            phase=WorkflowPhase.POST_MARKET,
            triggered_by=triggered_by,
            status=status,
            started_at=started_at,
            finished_at=datetime.now(),
            summary=summary,
            error_message="；".join(error_messages) or None,
        )

    def get_latest_run(self, phase: WorkflowPhase) -> WorkflowRunRecord | None:
        """Return the latest workflow snapshot for one phase."""
        conn = get_conn()
        row = conn.execute(
            """
            SELECT id, workflow_date, phase, triggered_by, status, started_at, finished_at, summary_json, error_message
            FROM workflow_runs
            WHERE phase = ?
            ORDER BY workflow_date DESC, started_at DESC, id DESC
            LIMIT 1
            """,
            [phase.value],
        ).fetchone()
        if row is None:
            return None
        return self._row_to_run(row)

    def list_history(self, limit: int = 20) -> list[WorkflowHistoryItem]:
        """Return recent workflow history rows."""
        conn = get_conn()
        rows = conn.execute(
            """
            SELECT id, workflow_date, phase, triggered_by, status, started_at, finished_at, error_message
            FROM workflow_runs
            ORDER BY started_at DESC, id DESC
            LIMIT ?
            """,
            [limit],
        ).fetchall()
        return [
            WorkflowHistoryItem(
                id=row[0],
                workflow_date=str(row[1]),
                phase=str(row[2]),
                triggered_by=str(row[3]),
                status=str(row[4]),
                started_at=row[5].isoformat() if row[5] else "",
                finished_at=row[6].isoformat() if row[6] else None,
                error_message=row[7],
            )
            for row in rows
        ]

    def get_workflow_status(self) -> dict:
        """Return the latest status for both workflow phases."""
        return {
            "pre_market": self._status_summary(self.get_latest_run(WorkflowPhase.PRE_MARKET)),
            "post_market": self._status_summary(self.get_latest_run(WorkflowPhase.POST_MARKET)),
        }

    def _resolve_requested_date(self, workflow_date: str | None) -> str:
        normalized = normalize_scan_date(workflow_date)
        return normalized or date.today().isoformat()

    def _resolve_pre_market_date(self, workflow_date: str | None) -> tuple[str, str, str]:
        requested_date = self._resolve_requested_date(workflow_date)
        if self._should_run_for_trading_day(requested_date):
            return requested_date, requested_date, "exact"
        next_trading_date = self._next_trading_day(requested_date)
        if next_trading_date is not None:
            return requested_date, next_trading_date, "fallback_next_trading_day"
        return requested_date, requested_date, "exact"

    def _resolve_post_market_date(self, workflow_date: str | None) -> tuple[str, str, str]:
        requested_date = self._resolve_requested_date(workflow_date)
        if self._should_run_for_trading_day(requested_date):
            return requested_date, requested_date, "exact"
        previous_trading_date = self._tushare.previous_trading_day(requested_date)
        if previous_trading_date is None:
            previous_trading_date = self._previous_trading_day(requested_date)
        if previous_trading_date is not None:
            return requested_date, previous_trading_date, "fallback_previous_trading_day"
        return requested_date, requested_date, "exact"

    def _next_trading_day(self, target_date: str) -> str | None:
        cursor = date.fromisoformat(target_date)
        for _ in range(14):
            cursor = cursor.fromordinal(cursor.toordinal() + 1)
            cursor_str = cursor.isoformat()
            if self._should_run_for_trading_day(cursor_str):
                return cursor_str
        return None

    def _previous_trading_day(self, target_date: str) -> str | None:
        cursor = date.fromisoformat(target_date)
        for _ in range(14):
            cursor = cursor.fromordinal(cursor.toordinal() - 1)
            cursor_str = cursor.isoformat()
            if self._should_run_for_trading_day(cursor_str):
                return cursor_str
        return None

    def _should_run_for_trading_day(self, target_date: str) -> bool:
        return self._tushare.is_trading_day(target_date)

    def _build_post_market_targets(self, watchlist: dict) -> tuple[list[str], list[str]]:
        stock_codes: list[str] = []
        for item in watchlist.get("watch_stocks", []):
            code = str(item.get("code", "")).strip()
            if code and code not in stock_codes:
                stock_codes.append(code)
        for position in self._load_positions():
            code = str(position.get("stock_code", "")).strip()
            if code and code not in stock_codes:
                stock_codes.append(code)
        if not stock_codes:
            stock_codes = [item.get("code", "") for item in watchlist.get("watch_stocks", []) if item.get("code")]
        return stock_codes, _DEFAULT_INDEX_CODES.copy()

    def _load_watchlist(self) -> WatchlistConfig:
        return self._summary_api.get_watchlist()

    def _build_watch_context(self, watchlist: dict) -> dict:
        positions = self._load_positions()
        return {
            "watch_sectors": watchlist.get("watch_sectors", []),
            "watch_stocks": watchlist.get("watch_stocks", []),
            "open_positions": [
                {
                    "code": position.get("stock_code"),
                    "name": position.get("stock_name"),
                }
                for position in positions
            ],
        }

    def _load_positions(self) -> list[dict]:
        conn = get_conn()
        rows = conn.execute(
            "SELECT stock_code, stock_name FROM portfolio WHERE status = 'open' ORDER BY buy_date DESC"
        ).fetchdf()
        return rows.to_dict(orient="records")

    def _get_latest_news(self, limit: int = 8) -> list[dict]:
        conn = get_conn()
        rows = conn.execute(
            """
            SELECT source, source_item_id, title, content, category, published_at, collected_at
            FROM news_items
            ORDER BY COALESCE(published_at, collected_at) DESC
            LIMIT ?
            """,
            [limit],
        ).fetchdf()
        return rows.to_dict(orient="records")

    def _resolve_status(self, steps: list[WorkflowStepResult]) -> WorkflowStatus:
        statuses = {step.status for step in steps}
        if statuses == {WorkflowStatus.SUCCESS.value}:
            return WorkflowStatus.SUCCESS
        if WorkflowStatus.FAILED.value in statuses and WorkflowStatus.SUCCESS.value in statuses:
            return WorkflowStatus.PARTIAL
        if WorkflowStatus.FAILED.value in statuses:
            return WorkflowStatus.FAILED
        if WorkflowStatus.SKIPPED.value in statuses and len(statuses) == 1:
            return WorkflowStatus.SKIPPED
        return WorkflowStatus.PARTIAL

    def _build_carry_over(self, previous_post_market: WorkflowRunRecord | None) -> dict:
        if previous_post_market is None:
            return {"available": False}
        return {
            "available": True,
            "workflow_date": previous_post_market.workflow_date,
            "overview": previous_post_market.summary.overview,
            "scan": previous_post_market.summary.scan,
            "market_overview": previous_post_market.summary.market_overview,
            "sector_positioning": previous_post_market.summary.sector_positioning,
            "position_health": previous_post_market.summary.position_health,
            "next_day_prep": previous_post_market.summary.next_day_prep,
        }

    def _build_yesterday_recap(self, carry_over: dict) -> dict:
        if not carry_over.get("available"):
            return {
                "summary": "暂无上一交易日盘后结论。",
                "regime": "unknown",
                "key_metrics": {},
                "carry_over_points": [],
            }
        market_overview = carry_over.get("market_overview", {})
        breadth = market_overview.get("breadth", {})
        limit_stats = market_overview.get("limit_stats", {})
        style = market_overview.get("style", {})
        summary_parts = []
        if breadth.get("up_down_ratio") is not None:
            summary_parts.append(f"涨跌比 {breadth.get('up_down_ratio')}")
        if breadth.get("ratio_5d_avg") is not None:
            summary_parts.append(f"5日均值 {breadth.get('ratio_5d_avg')}")
        if limit_stats.get("limit_up_count") is not None or limit_stats.get("limit_down_count") is not None:
            summary_parts.append(
                f"涨停 {limit_stats.get('limit_up_count') or 0} / 跌停 {limit_stats.get('limit_down_count') or 0}"
            )
        if style.get("style_label"):
            summary_parts.append(f"风格 {style.get('style_label')}")
        summary_text = carry_over.get("overview") or market_overview.get("summary") or "已有上一交易日盘后结论。"
        if summary_parts:
            summary_text = f"{summary_text}；" + "；".join(summary_parts[:4])
        return {
            "summary": summary_text,
            "regime": market_overview.get("regime", "unknown"),
            "key_metrics": {
                "up_down_ratio": breadth.get("up_down_ratio"),
                "ratio_5d_avg": breadth.get("ratio_5d_avg"),
                "limit_up_count": limit_stats.get("limit_up_count"),
                "limit_down_count": limit_stats.get("limit_down_count"),
                "broken_board_count": limit_stats.get("broken_board_count"),
                "max_consecutive_board": limit_stats.get("max_consecutive_board"),
                "style_label": style.get("style_label"),
            },
            "carry_over_points": carry_over.get("next_day_prep", {}).get("tomorrow_checkpoints", []),
        }

    def _build_overnight_news(self, news_status: str, news_items: list[dict]) -> dict:
        highlights = news_items[:5]
        categorized = {
            "macro": [item for item in news_items if item.get("category") == "macro"],
            "industry": [item for item in news_items if item.get("category") == "industry"],
            "company": [item for item in news_items if item.get("category") == "company"],
            "geopolitics": [item for item in news_items if item.get("category") == "geopolitics"],
            "overseas": [item for item in news_items if item.get("category") == "overseas"],
        }
        summary = f"夜间信息同步状态：{news_status}；共整理 {len(news_items)} 条信息。"
        return {
            "summary": summary,
            "status": news_status,
            "highlights": highlights,
            "categorized": categorized,
            "sector_mappings": [],
        }

    def _build_today_watchlist(self, watch_context: dict, alerts: list[dict], carry_over: dict) -> dict:
        watch_sectors = watch_context.get("watch_sectors", [])
        open_positions = watch_context.get("open_positions", [])
        next_day_prep = carry_over.get("next_day_prep", {}) if carry_over else {}
        sector_positioning = carry_over.get("sector_positioning", {}) if carry_over else {}
        position_health = carry_over.get("position_health", {}) if carry_over else {}
        watch_sector_map = {
            item.get("sector_name"): item for item in sector_positioning.get("watch_sectors", []) if item.get("sector_name")
        }
        tracked_item_map = {
            item.get("code"): item for item in position_health.get("tracked_items", []) if item.get("code")
        }
        market_checkpoints = next_day_prep.get("tomorrow_checkpoints") or [
            "开盘前30分钟涨跌比是否明显强于昨日",
            "成交额节奏是否延续上一交易日风险偏好",
            "市场对隔夜信息反应是强化还是钝化",
        ]
        focus_sectors = []
        for sector in watch_sectors[:8]:
            matched = watch_sector_map.get(sector, {})
            focus_sectors.append(
                {
                    "sector_name": sector,
                    "role": matched.get("role", "watch_sector"),
                    "trend_5d": matched.get("trend_5d", "unknown"),
                    "consistency": matched.get("consistency"),
                    "leader_stock": matched.get("leader_stock"),
                    "today_observation": matched.get("observation_note") or f"观察 {sector} 是否进入今日主线。",
                }
            )
        position_watch = []
        for item in open_positions[:8]:
            tracked = tracked_item_map.get(item.get("code"), {})
            position_watch.append(
                {
                    "code": item.get("code"),
                    "name": item.get("name") or item.get("code"),
                    "role": tracked.get("role", "position"),
                    "cost_price": None,
                    "today_observation": tracked.get("observation_note") or f"跟踪 {item.get('name') or item.get('code')} 的盘中强弱与量价配合。",
                    "risk_note": (tracked.get("risk_flags") or [None])[0],
                }
            )
        return {
            "market_checkpoints": market_checkpoints,
            "focus_sectors": focus_sectors,
            "position_watch": position_watch,
            "latest_alerts": alerts[:5],
        }

    def _build_action_frame(self, carry_over: dict, alerts: list[dict]) -> dict:
        next_day_prep = carry_over.get("next_day_prep", {}) if carry_over else {}
        risk_notes = next_day_prep.get("risk_notes") or [item.get("title") for item in alerts[:3] if item.get("title")]
        focus_directions = next_day_prep.get("focus_sectors", [])
        notes = next_day_prep.get("tomorrow_checkpoints", [])
        if not notes and focus_directions:
            notes = [f"重点确认 {item} 是否获得开盘资金响应。" for item in focus_directions[:3]]
        return {
            "posture": next_day_prep.get("market_bias", "observe"),
            "focus_directions": focus_directions,
            "risk_warnings": risk_notes,
            "notes": notes,
        }

    def _build_market_overview(self, workflow_date: str, steps: list[WorkflowStepResult]) -> dict:
        conn = get_conn()
        indices = self._load_index_snapshots(conn, workflow_date)
        breadth = self._load_market_breadth(conn, workflow_date)
        market_stats = conn.execute(
            """
            SELECT market_code, market_name, listed_count, amount, pe, turnover_rate
            FROM market_daily_stats
            WHERE trade_date = ?
            ORDER BY market_code ASC
            """,
            [workflow_date],
        ).fetchdf().to_dict(orient="records")
        limit_stats = self._build_limit_stats(breadth)
        style = self._build_style_snapshot(indices)
        regime, confidence = self._build_regime(indices, breadth)
        completed_steps = len([step for step in steps if step.status == WorkflowStatus.SUCCESS.value])
        summary = self._build_market_summary_text(workflow_date, regime, breadth, limit_stats)
        return {
            "summary": summary,
            "regime": regime,
            "confidence": confidence,
            "indices": indices,
            "breadth": breadth,
            "limit_stats": limit_stats,
            "style": style,
            "risk_proxies": self._build_risk_proxies(indices),
            "market_stats": market_stats,
            "key_takeaways": self._build_market_takeaways(workflow_date, regime, breadth, limit_stats, completed_steps, len(steps)),
        }

    def _build_market_overview_fallback(self, workflow_date: str) -> dict:
        return {
            "summary": f"{workflow_date} 盘后未能完整生成市场大势。",
            "regime": "neutral",
            "confidence": "low",
            "indices": [],
            "breadth": {
                "up_count": None,
                "down_count": None,
                "up_down_ratio": None,
                "ratio_5d_avg": None,
                "up_count_5d_avg": None,
                "down_count_5d_avg": None,
            },
            "limit_stats": {
                "limit_up_count": None,
                "limit_down_count": None,
                "broken_board_count": None,
                "broken_board_rate": None,
                "max_consecutive_board": None,
            },
            "style": {
                "style_label": "unknown",
                "hs300_5d_pct": None,
                "zz1000_5d_pct": None,
                "relative_strength_5d": None,
            },
            "risk_proxies": [],
            "market_stats": [],
            "key_takeaways": ["需要检查市场数据同步或 briefing builder。"],
        }

    def _load_index_snapshots(self, conn: duckdb.DuckDBPyConnection, workflow_date: str) -> list[dict]:
        rows = conn.execute(
            """
            WITH current_day AS (
                SELECT index_code, close, amount
                FROM index_daily
                WHERE date = ?
                  AND index_code IN ('000001', '399001', '399006', '000688', '000016', '000300')
            ),
            previous_day AS (
                SELECT current_day.index_code, previous.close AS prev_close
                FROM current_day
                LEFT JOIN LATERAL (
                    SELECT close
                    FROM index_daily
                    WHERE index_code = current_day.index_code AND date < ?
                    ORDER BY date DESC
                    LIMIT 1
                ) previous ON TRUE
            )
            SELECT current_day.index_code, current_day.close, current_day.amount, previous_day.prev_close
            FROM current_day
            LEFT JOIN previous_day ON current_day.index_code = previous_day.index_code
            ORDER BY current_day.index_code ASC
            """,
            [workflow_date, workflow_date],
        ).fetchdf().to_dict(orient="records")
        snapshots: list[dict] = []
        for row in rows:
            close = row.get("close")
            prev_close = row.get("prev_close")
            pct_change = None
            if close not in (None, 0) and prev_close not in (None, 0):
                pct_change = round((float(close) - float(prev_close)) / float(prev_close) * 100, 2)
            snapshots.append(
                {
                    "code": row.get("index_code"),
                    "name": self._index_name(row.get("index_code")),
                    "close": close,
                    "pct_change": pct_change,
                    "amount": row.get("amount"),
                }
            )
        return snapshots

    def _load_market_breadth(self, conn: duckdb.DuckDBPyConnection, workflow_date: str) -> dict:
        today = self._load_breadth_for_date(conn, workflow_date)
        history = conn.execute(
            """
            SELECT trade_date
            FROM market_daily_stats
            WHERE trade_date <= ?
            GROUP BY trade_date
            ORDER BY trade_date DESC
            LIMIT 5
            """,
            [workflow_date],
        ).fetchall()
        ratio_values: list[float] = []
        up_values: list[int] = []
        down_values: list[int] = []
        for row in history:
            trade_date = str(row[0])
            sample = self._load_breadth_for_date(conn, trade_date)
            if sample["up_down_ratio"] is not None:
                ratio_values.append(sample["up_down_ratio"])
            if sample["up_count"] is not None:
                up_values.append(sample["up_count"])
            if sample["down_count"] is not None:
                down_values.append(sample["down_count"])
        today["ratio_5d_avg"] = round(sum(ratio_values) / len(ratio_values), 2) if ratio_values else None
        today["up_count_5d_avg"] = round(sum(up_values) / len(up_values), 0) if up_values else None
        today["down_count_5d_avg"] = round(sum(down_values) / len(down_values), 0) if down_values else None
        return today

    def _load_breadth_for_date(self, conn: duckdb.DuckDBPyConnection, workflow_date: str) -> dict:
        rows = conn.execute(
            """
            WITH current_day AS (
                SELECT stock_code, close
                FROM stock_daily
                WHERE date = ?
            ),
            previous_day AS (
                SELECT current_day.stock_code, previous.close AS prev_close
                FROM current_day
                LEFT JOIN LATERAL (
                    SELECT close
                    FROM stock_daily
                    WHERE stock_code = current_day.stock_code AND date < ?
                    ORDER BY date DESC
                    LIMIT 1
                ) previous ON TRUE
            )
            SELECT current_day.close, previous_day.prev_close
            FROM current_day
            LEFT JOIN previous_day ON current_day.stock_code = previous_day.stock_code
            """,
            [workflow_date, workflow_date],
        ).fetchall()
        up_count = 0
        down_count = 0
        limit_up_count = 0
        limit_down_count = 0
        valid = 0
        for close, prev_close in rows:
            if close in (None, 0) or prev_close in (None, 0):
                continue
            change_pct = (float(close) - float(prev_close)) / float(prev_close) * 100
            valid += 1
            if change_pct > 0:
                up_count += 1
            elif change_pct < 0:
                down_count += 1
            if change_pct >= 9.9:
                limit_up_count += 1
            if change_pct <= -9.9:
                limit_down_count += 1
        return {
            "up_count": up_count if valid else None,
            "down_count": down_count if valid else None,
            "up_down_ratio": round(up_count / down_count, 2) if down_count else (float(up_count) if valid else None),
            "limit_up_count": limit_up_count if valid else None,
            "limit_down_count": limit_down_count if valid else None,
        }

    def _build_limit_stats(self, breadth: dict) -> dict:
        limit_up_count = breadth.get("limit_up_count")
        limit_down_count = breadth.get("limit_down_count")
        return {
            "limit_up_count": limit_up_count,
            "limit_down_count": limit_down_count,
            "broken_board_count": None,
            "broken_board_rate": None,
            "max_consecutive_board": None,
        }

    def _build_style_snapshot(self, indices: list[dict]) -> dict:
        hs300_pct = next((item.get("pct_change") for item in indices if item.get("code") == "000300"), None)
        cyb_pct = next((item.get("pct_change") for item in indices if item.get("code") == "399006"), None)
        relative_strength = None
        style_label = "unknown"
        if hs300_pct is not None and cyb_pct is not None:
            relative_strength = round(cyb_pct - hs300_pct, 2)
            if relative_strength >= 1:
                style_label = "small_cap"
            elif relative_strength <= -1:
                style_label = "large_cap"
            else:
                style_label = "balanced"
        return {
            "style_label": style_label,
            "hs300_5d_pct": hs300_pct,
            "zz1000_5d_pct": cyb_pct,
            "relative_strength_5d": relative_strength,
        }

    def _build_regime(self, indices: list[dict], breadth: dict) -> tuple[str, str]:
        hs300_pct = next((item.get("pct_change") for item in indices if item.get("code") == "000300"), 0.0) or 0.0
        cyb_pct = next((item.get("pct_change") for item in indices if item.get("code") == "399006"), 0.0) or 0.0
        up_count = breadth.get("up_count") or 0
        down_count = breadth.get("down_count") or 0
        limit_up_count = breadth.get("limit_up_count") or 0
        limit_down_count = breadth.get("limit_down_count") or 0
        total = up_count + down_count
        breadth_component = ((up_count - down_count) / total * 100) if total else 0.0
        index_component = ((hs300_pct + cyb_pct) / 2.0) * 20
        limit_component = ((limit_up_count - limit_down_count) / total * 1000) if total else 0.0
        score = index_component * 0.4 + breadth_component * 0.4 + limit_component * 0.2
        if score >= 20:
            return "risk_on", "medium"
        if score <= -20:
            return "risk_off", "medium"
        return "neutral", "low"

    def _build_market_summary_text(self, workflow_date: str, regime: str, breadth: dict, limit_stats: dict) -> str:
        ratio = breadth.get("up_down_ratio")
        limit_up = limit_stats.get("limit_up_count")
        limit_down = limit_stats.get("limit_down_count")
        return f"{workflow_date} 市场状态 {regime}；涨跌比 {ratio if ratio is not None else '-'}；涨停 {limit_up if limit_up is not None else '-'} 家，跌停 {limit_down if limit_down is not None else '-'} 家。"

    def _build_risk_proxies(self, indices: list[dict]) -> list[dict]:
        return [
            {
                "code": item.get("code"),
                "name": item.get("name"),
                "pct_change": item.get("pct_change"),
                "note": "指数风险偏好代理",
            }
            for item in indices
            if item.get("code") in {"000016", "000300", "399006"}
        ]

    def _build_market_takeaways(
        self,
        workflow_date: str,
        regime: str,
        breadth: dict,
        limit_stats: dict,
        completed_steps: int,
        total_steps: int,
    ) -> list[str]:
        takeaways = [
            f"workflow 日期：{workflow_date}",
            f"市场状态：{regime}",
            f"已完成步骤：{completed_steps}/{total_steps}",
        ]
        if breadth.get("up_down_ratio") is not None:
            takeaways.append(f"涨跌比：{breadth['up_down_ratio']}")
        if limit_stats.get("limit_up_count") is not None:
            takeaways.append(f"涨停/跌停：{limit_stats['limit_up_count']}/{limit_stats.get('limit_down_count')}")
        return takeaways[:5]

    def _build_sector_positioning(self, workflow_date: str, watch_context: dict) -> dict:
        conn = get_conn()
        records = conn.execute(
            """
            SELECT sector, change_1d, change_5d
            FROM sector_data
            WHERE date = ?
            ORDER BY change_1d DESC NULLS LAST
            """,
            [workflow_date],
        ).fetchdf().to_dict(orient="records")

        if not records:
            logger.warning("sector positioning falls back to watchlist-only mode")

        market_leaders = [
            {
                "sector_name": item.get("sector"),
                "pct_change": item.get("change_1d"),
                "net_flow": None,
                "leader_stock": None,
            }
            for item in records[:5]
        ]
        market_laggards = [
            {
                "sector_name": item.get("sector"),
                "pct_change": item.get("change_1d"),
                "net_flow": None,
                "leader_stock": None,
            }
            for item in records[-5:]
        ] if records else []

        watch_sector_records: list[dict] = []
        for sector in watch_context.get("watch_sectors", [])[:8]:
            matched = self._match_watch_sector(records, sector)
            change_5d = matched.get("change_5d") if matched else None
            consistency = self._build_sector_consistency(matched)
            watch_sector_records.append(
                {
                    "sector_name": sector,
                    "role": "watch_sector",
                    "trend_5d": self._trend_from_change(change_5d),
                    "consistency": consistency,
                    "leader_stock": None,
                    "observation_note": self._build_sector_observation_note(sector, matched),
                    "status": self._build_sector_status(matched),
                }
            )
        return {
            "market_leaders": market_leaders,
            "market_laggards": market_laggards,
            "watch_sectors": watch_sector_records,
            "observation_focus": [
                "优先确认当日主线是否与固定观察池重合",
                "若主线不在观察池内，仅记录不立即扩展系统范围",
            ],
        }

    def _match_watch_sector(self, records: list[dict], watch_name: str) -> dict | None:
        watch_lower = watch_name.lower()
        for item in records:
            sector_name = str(item.get("sector") or "")
            if sector_name == watch_name:
                return item
        for item in records:
            sector_name = str(item.get("sector") or "")
            sector_lower = sector_name.lower()
            if watch_lower in sector_lower or sector_lower in watch_lower:
                return item
        return None

    def _trend_from_change(self, change_5d: float | None) -> str:
        if change_5d is None:
            return "unknown"
        if change_5d >= 2:
            return "up"
        if change_5d <= -2:
            return "down"
        return "range"

    def _build_sector_consistency(self, matched: dict | None) -> int | None:
        if matched is None:
            return None
        change_1d = matched.get("change_1d")
        change_5d = matched.get("change_5d")
        if change_1d is None or change_5d is None:
            return None
        score = 20
        if change_1d > 0:
            score += 40
        if change_5d > 0:
            score += 40
        return score

    def _build_sector_status(self, matched: dict | None) -> str:
        if matched is None:
            return "unknown"
        change_1d = matched.get("change_1d")
        change_5d = matched.get("change_5d")
        if change_1d is None or change_5d is None:
            return "unknown"
        if change_1d > 1 and change_5d > 2:
            return "strengthening"
        if change_1d < -1 and change_5d < -2:
            return "weakening"
        return "stable"

    def _build_sector_observation_note(self, sector: str, matched: dict | None) -> str:
        if matched is None:
            return f"{sector} 暂无已落库行业快照，先作为固定观察池保留。"
        change_1d = matched.get("change_1d")
        change_5d = matched.get("change_5d")
        return f"{sector} 当日 {change_1d if change_1d is not None else '-'}%，5日 {change_5d if change_5d is not None else '-'}%，观察是否保持一致性。"

    def _build_position_health(self, workflow_date: str, watch_context: dict) -> dict:
        tracked_items = []
        for item in watch_context.get("open_positions", []):
            tracked_items.append(self._build_tracked_item(workflow_date, item, "position"))
        for item in watch_context.get("watch_stocks", [])[:5]:
            tracked_items.append(self._build_tracked_item(workflow_date, item, "watch_stock"))
        return {
            "portfolio_health_summary": f"当前纳入 {len(tracked_items)} 个持仓/观察对象的健康度跟踪。",
            "sector_health": [],
            "tracked_items": tracked_items,
        }

    def _build_tracked_item(self, workflow_date: str, item: dict, subject_type: str) -> dict:
        code = item.get("code") or item.get("stock_code")
        name = item.get("name") or item.get("stock_name") or code
        snapshot = self._load_stock_snapshot(workflow_date, str(code)) if code else {}
        pct_change = snapshot.get("pct_change")
        turnover_rate = snapshot.get("turnover_rate")
        volume_ratio = snapshot.get("volume_ratio")
        state = self._state_from_snapshot(pct_change, volume_ratio)
        return {
            "subject_type": subject_type,
            "code": code,
            "name": name,
            "role": subject_type,
            "pct_change": pct_change,
            "turnover_rate": turnover_rate,
            "volume_ratio": volume_ratio,
            "state": state,
            "observation_note": self._observation_from_snapshot(name, pct_change, volume_ratio),
            "risk_flags": [self._risk_flag_from_state(state)] if self._risk_flag_from_state(state) else [],
        }

    def _load_stock_snapshot(self, workflow_date: str, stock_code: str) -> dict:
        conn = get_conn()
        row = conn.execute(
            """
            SELECT close, turnover
            FROM stock_daily
            WHERE stock_code = ? AND date = ?
            LIMIT 1
            """,
            [stock_code, workflow_date],
        ).fetchone()
        if row is None:
            return {
                "pct_change": None,
                "turnover_rate": None,
                "volume_ratio": None,
            }
        prev = conn.execute(
            """
            SELECT close
            FROM stock_daily
            WHERE stock_code = ? AND date < ?
            ORDER BY date DESC
            LIMIT 1
            """,
            [stock_code, workflow_date],
        ).fetchone()
        prev_close = prev[0] if prev else None
        pct_change = None
        if prev_close not in (None, 0) and row[0] is not None:
            pct_change = round((float(row[0]) - float(prev_close)) / float(prev_close) * 100, 2)
        return {
            "pct_change": pct_change,
            "turnover_rate": row[1],
            "volume_ratio": None,
        }

    def _state_from_snapshot(self, pct_change: float | None, volume_ratio: float | None) -> str:
        _ = volume_ratio
        if pct_change is not None and pct_change >= 3:
            return "breakout"
        if pct_change is not None and pct_change <= -3:
            return "breakdown"
        if pct_change is not None:
            return "watch"
        return "neutral"

    def _observation_from_snapshot(self, name: str, pct_change: float | None, volume_ratio: float | None) -> str:
        _ = volume_ratio
        if pct_change is None:
            return f"继续观察 {name} 的强弱变化。"
        if pct_change >= 3:
            return f"{name} 当日明显走强，观察次日是否延续。"
        if pct_change <= -3:
            return f"{name} 当日明显走弱，优先关注风险演化。"
        return f"{name} 当日波动有限，继续观察是否出现方向选择。"

    def _risk_flag_from_state(self, state: str) -> str | None:
        if state == "breakdown":
            return "价格走弱"
        if state == "breakout":
            return "趋势强化"
        return None

    def _build_next_day_prep(self, sector_positioning: dict, position_health: dict, market_overview: dict) -> dict:
        watch_sector_names = [item.get("sector_name") for item in sector_positioning.get("watch_sectors", []) if item.get("sector_name")]
        risky_items = [
            item.get("name")
            for item in position_health.get("tracked_items", [])
            if item.get("state") == "breakdown"
        ]
        bias = "observe"
        if market_overview.get("regime") == "risk_on":
            bias = "balanced"
        return {
            "market_bias": bias,
            "focus_sectors": watch_sector_names[:5],
            "focus_items": [item.get("name") for item in position_health.get("tracked_items", [])[:5] if item.get("name")],
            "risk_notes": [f"重点跟踪 {name} 的风险演化。" for name in risky_items[:3]],
            "tomorrow_checkpoints": [
                "确认盘前关注方向是否获得开盘资金响应",
                "确认持仓对象是否延续强弱分化",
            ],
        }

    def _index_name(self, index_code: str | None) -> str:
        mapping = {
            "000001": "上证指数",
            "399001": "深证成指",
            "399006": "创业板指",
            "000688": "科创50",
        }
        return mapping.get(index_code or "", index_code or "未知指数")

    def _build_pre_market_overview(
        self,
        previous_post_market: WorkflowRunRecord | None,
        news_items: list[dict],
        watchlist: dict,
        alerts: list[dict],
        requested_date: str,
        resolved_date: str,
        date_resolution: str,
    ) -> str:
        post_market_text = "已有上一交易日盘后结果" if previous_post_market else "缺少上一交易日盘后结果"
        news_text = f"夜间新闻 {len(news_items)} 条"
        watch_text = f"关注板块 {len(watchlist.get('watch_sectors', []))} 个，关注股票 {len(watchlist.get('watch_stocks', []))} 个"
        alert_text = f"未读/最新预警 {min(len(alerts), 8)} 条"
        date_text = f"日期：{resolved_date}"
        if date_resolution == "fallback_next_trading_day":
            date_text = f"请求日期 {requested_date} 为非交易日，已切换到下一个交易日 {resolved_date}"
        return "；".join([date_text, post_market_text, news_text, watch_text, alert_text])

    def _build_post_market_overview(
        self,
        market_overview: dict,
        sector_positioning: dict,
        position_health: dict,
        requested_date: str,
        resolved_date: str,
        date_resolution: str,
    ) -> str:
        leaders = sector_positioning.get("market_leaders", [])
        tracked_items = position_health.get("tracked_items", [])
        date_text = f"日期：{resolved_date}"
        if date_resolution == "fallback_previous_trading_day":
            date_text = f"请求日期 {requested_date} 为非交易日，已切换到上一个交易日 {resolved_date}"
        regime_text = market_overview.get("summary") or "已生成盘后大势摘要"
        return f"{date_text}；{regime_text}；主线方向 {len(leaders)} 个；纳入健康度跟踪对象 {len(tracked_items)} 个。"

    def _persist_run(
        self,
        workflow_date: str,
        phase: WorkflowPhase,
        triggered_by: WorkflowTrigger,
        status: WorkflowStatus,
        started_at: datetime,
        finished_at: datetime,
        summary: WorkflowSummary,
        error_message: str | None,
    ) -> WorkflowRunRecord:
        run = WorkflowRunRecord(
            id=time.time_ns(),
            workflow_date=workflow_date,
            phase=phase,
            triggered_by=triggered_by,
            status=status,
            started_at=started_at,
            finished_at=finished_at,
            summary=summary,
            error_message=error_message,
        )
        conn = get_conn()
        conn.execute(
            """
            INSERT INTO workflow_runs (
                id, workflow_date, phase, triggered_by, status,
                started_at, finished_at, summary_json, error_message
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                run.id,
                workflow_date,
                phase.value,
                triggered_by.value,
                status.value,
                started_at,
                finished_at,
                json.dumps(summary.model_dump(), ensure_ascii=False),
                error_message,
            ],
        )
        return run

    def _row_to_run(self, row: tuple) -> WorkflowRunRecord:
        return WorkflowRunRecord(
            id=row[0],
            workflow_date=str(row[1]),
            phase=WorkflowPhase(str(row[2])),
            triggered_by=WorkflowTrigger(str(row[3])),
            status=WorkflowStatus(str(row[4])),
            started_at=row[5],
            finished_at=row[6],
            summary=WorkflowSummary(**json.loads(row[7])),
            error_message=row[8],
        )

    def _status_summary(self, run: WorkflowRunRecord | None) -> dict:
        if run is None:
            return {"available": False}
        return {
            "available": True,
            "workflow_date": run.workflow_date,
            "phase": run.phase.value,
            "status": run.status.value,
            "started_at": run.started_at.isoformat(),
            "finished_at": run.finished_at.isoformat() if run.finished_at else None,
        }
