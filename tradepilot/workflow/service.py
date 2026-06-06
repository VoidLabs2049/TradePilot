"""Workflow service for pre-market and post-market daily operations."""

from __future__ import annotations

import json
import time
from datetime import date, datetime
from importlib import import_module
from pathlib import Path
import sys

import akshare as ak
import duckdb

from loguru import logger

from tradepilot.config import RESEARCH_REPORT_ROOT
from tradepilot.db import get_conn
from tradepilot.etl.read_models import (
    get_latest_etf_aw_regime_context,
    get_latest_etf_aw_snapshot,
    get_latest_etf_aw_strategy_context,
)
from tradepilot.ingestion.models import NewsSyncRequest, SyncRequest
from tradepilot.ingestion.service import IngestionService
from tradepilot.scanner.daily import DailyScanner, normalize_scan_date
from tradepilot.summary.models import WatchlistConfig
from tradepilot.workflow.models import (
    InsightStatus,
    WorkflowContextPayload,
    WorkflowHistoryItem,
    WorkflowInsightPayload,
    WorkflowInsightRecord,
    WorkflowInsightResponse,
    WorkflowInsightUpsertRequest,
    WorkflowPhase,
    WorkflowRunRecord,
    WorkflowStatus,
    WorkflowStepResult,
    WorkflowSummary,
    WorkflowTrigger,
)

_THE_ONE_RESEARCH_SKILL_DIR = (
    Path(__file__).resolve().parents[3]
    / "The-One"
    / ".claude"
    / "skills"
    / "the-one"
    / "skills"
    / "eastmoney-research-report"
)
if str(_THE_ONE_RESEARCH_SKILL_DIR) not in sys.path:
    sys.path.insert(0, str(_THE_ONE_RESEARCH_SKILL_DIR))

_MARKET_REFERENCE_CONFIG = {
    "core_indices": ["000001", "399001", "399006", "000688"],
    "style_indices": ["000300", "399006"],
    "risk_proxy_indices": ["000016", "000300", "399006"],
    "all_snapshot_indices": [
        "000001",
        "399001",
        "399006",
        "000688",
        "000016",
        "000300",
    ],
    "index_names": {
        "000001": "上证指数",
        "399001": "深证成指",
        "399006": "创业板指",
        "000688": "科创50",
        "000016": "上证50",
        "000300": "沪深300",
    },
}


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
        requested_date, resolved_date, date_resolution = self._resolve_pre_market_date(
            workflow_date
        )
        steps: list[WorkflowStepResult] = []
        error_messages: list[str] = []
        previous_post_market = self.get_latest_run(WorkflowPhase.POST_MARKET)
        alerts = self._scanner.list_alerts(unread_only=False)[:10]
        watchlist = self._normalize_watchlist_config(self._load_watchlist())
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
                overnight_news={
                    "summary": "非交易日未执行夜间信息同步",
                    "highlights": [],
                },
                today_watchlist=self._build_today_watchlist(
                    watch_context, alerts, carry_over
                ),
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
        news_step_status = (
            WorkflowStatus.SUCCESS.value
            if news_status == "success"
            else WorkflowStatus.FAILED.value
        )
        if news_step_status == WorkflowStatus.FAILED.value:
            error_messages.append(news_result.run.error_message or "news sync failed")
        steps.append(
            WorkflowStepResult(
                name="news_sync",
                status=news_step_status,
                records_affected=news_result.run.records_inserted
                + news_result.run.records_updated,
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
        overnight_news = self._build_overnight_news(
            news_step_status, news_items, watch_context
        )
        today_watchlist = self._build_today_watchlist(
            watch_context, alerts, carry_over, overnight_news
        )
        action_frame = self._build_action_frame(carry_over, alerts, overnight_news)
        summary = WorkflowSummary(
            title="盘前准备",
            overview=overview,
            requested_date=requested_date,
            resolved_date=resolved_date,
            date_resolution=date_resolution,
            yesterday_recap=self._build_yesterday_recap(carry_over),
            overnight_news=overnight_news,
            today_watchlist=today_watchlist,
            action_frame=action_frame,
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
        requested_date, resolved_date, date_resolution = self._resolve_post_market_date(
            workflow_date
        )
        steps: list[WorkflowStepResult] = []
        error_messages: list[str] = []
        watchlist = self._normalize_watchlist_config(self._load_watchlist())

        watch_context = self._build_watch_context(watchlist)

        if not self._should_run_for_trading_day(resolved_date):
            summary = WorkflowSummary(
                title="盘后复盘已跳过",
                overview="非交易日，未执行盘后工作流。",
                requested_date=requested_date,
                resolved_date=resolved_date,
                date_resolution=date_resolution,
                market_overview={
                    "summary": "非交易日，未生成市场大势。",
                    "indices": [],
                },
                sector_positioning={
                    "market_leaders": [],
                    "market_laggards": [],
                    "watch_sectors": [],
                },
                position_health={
                    "portfolio_health_summary": "非交易日，未生成持仓健康度。",
                    "tracked_items": [],
                },
                next_day_prep={
                    "market_bias": "observe",
                    "focus_sectors": [],
                    "focus_items": [],
                    "risk_notes": [],
                },
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
            market_step_status = (
                WorkflowStatus.SUCCESS.value
                if market_status == "success"
                else WorkflowStatus.FAILED.value
            )
            market_records = (
                market_result.run.records_inserted + market_result.run.records_updated
            )
            market_error = market_result.run.error_message
        except Exception as exc:
            logger.exception(
                "post-market workflow market sync failed before run persisted"
            )
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
            sector_positioning = self._build_sector_positioning(
                resolved_date, watch_context
            )
            position_health = self._build_position_health(
                resolved_date, watch_context, sector_positioning
            )
            cross_day_review = self._build_cross_day_review(
                workflow_date=resolved_date,
                market_overview=market_overview,
                sector_positioning=sector_positioning,
            )
            research_archive = self._build_research_archive(resolved_date, watchlist)
            next_day_prep = self._build_next_day_prep(
                sector_positioning, position_health, market_overview
            )
            briefing_step_status = WorkflowStatus.SUCCESS.value
            briefing_records = len(position_health.get("tracked_items", [])) + len(
                sector_positioning.get("watch_sectors", [])
            )
            briefing_error = None
        except Exception as exc:
            logger.exception("post-market workflow briefing build failed")
            market_overview = self._build_market_overview_fallback(resolved_date)
            sector_positioning = {
                "market_leaders": [],
                "market_laggards": [],
                "watch_sectors": [],
                "observation_focus": [],
            }
            position_health = {
                "portfolio_health_summary": "盘后复盘生成失败。",
                "sector_health": [],
                "tracked_items": [],
            }
            cross_day_review = {"available": False, "reason": "briefing_build_failed"}
            research_archive = {"available": False, "reason": "briefing_build_failed"}
            next_day_prep = {
                "market_bias": "observe",
                "focus_sectors": [],
                "focus_items": [],
                "risk_notes": [],
                "tomorrow_checkpoints": [],
            }
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
                "data_sources": [
                    "market_sync",
                    "workflow_market_snapshot",
                    "alerts",
                    "watchlist",
                    "portfolio",
                ],
                "steps_completed": len(
                    [
                        step
                        for step in steps
                        if step.status == WorkflowStatus.SUCCESS.value
                    ]
                ),
                "steps_total": len(steps),
            },
            watchlist=watchlist,
            scheduler={
                "steps_completed": len(
                    [
                        step
                        for step in steps
                        if step.status == WorkflowStatus.SUCCESS.value
                    ]
                ),
                "steps_total": len(steps),
            },
            cross_day_review=cross_day_review,
            research_archive=research_archive,
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
            "pre_market": self._status_summary(
                self.get_latest_run(WorkflowPhase.PRE_MARKET)
            ),
            "post_market": self._status_summary(
                self.get_latest_run(WorkflowPhase.POST_MARKET)
            ),
        }

    def get_latest_context(self, phase: WorkflowPhase) -> WorkflowContextPayload | None:
        """Return the latest structured context for one phase."""
        run = self.get_latest_run(phase)
        if run is None:
            return None
        return self.build_context_payload(run)

    def get_latest_etf_aw_context(self, as_of_date: date | None = None) -> dict | None:
        """Return the latest ETF all-weather snapshot context."""

        return get_latest_etf_aw_snapshot(as_of_date=as_of_date)

    def get_latest_etf_aw_regime_context(
        self, as_of_date: date | None = None
    ) -> dict | None:
        """Return the latest ETF all-weather regime context."""

        return get_latest_etf_aw_regime_context(as_of_date=as_of_date)

    def get_latest_etf_aw_strategy_context(
        self, as_of_date: date | None = None
    ) -> dict | None:
        """Return the latest ETF all-weather strategy context."""

        return get_latest_etf_aw_strategy_context(as_of_date=as_of_date)

    def build_context_payload(self, run: WorkflowRunRecord) -> WorkflowContextPayload:
        """Convert one workflow run into the stage-1 context contract.

        ETF all-weather context is selected by workflow date, not by workflow
        run id. It returns the latest rebalance snapshot at or before the run's
        workflow_date.
        """
        summary = run.summary
        etf_aw_context = get_latest_etf_aw_snapshot(
            as_of_date=date.fromisoformat(run.workflow_date)
        )
        etf_aw_regime_context = get_latest_etf_aw_regime_context(
            as_of_date=date.fromisoformat(run.workflow_date)
        )
        etf_aw_strategy_context = get_latest_etf_aw_strategy_context(
            as_of_date=date.fromisoformat(run.workflow_date)
        )
        if run.phase == WorkflowPhase.POST_MARKET:
            context = {
                "market_overview": summary.market_overview,
                "sector_positioning": summary.sector_positioning,
                "position_health": summary.position_health,
                "next_day_prep": summary.next_day_prep,
                "watch_context": summary.watch_context,
                "alerts": summary.alerts,
                "etf_aw_context": etf_aw_context,
                "etf_aw_regime_context": etf_aw_regime_context,
                "etf_aw_strategy_context": etf_aw_strategy_context,
            }
        else:
            context = {
                "yesterday_recap": summary.yesterday_recap,
                "overnight_news": summary.overnight_news,
                "today_watchlist": summary.today_watchlist,
                "action_frame": summary.action_frame,
                "watch_context": summary.watch_context,
                "alerts": summary.alerts,
                "carry_over": summary.carry_over,
                "etf_aw_context": etf_aw_context,
                "etf_aw_regime_context": etf_aw_regime_context,
                "etf_aw_strategy_context": etf_aw_strategy_context,
            }
        metadata = {
            **summary.metadata,
            "title": summary.title,
            "overview": summary.overview,
            "requested_date": summary.requested_date,
            "resolved_date": summary.resolved_date,
            "date_resolution": summary.date_resolution,
            "execution_status": run.status.value,
            "step_count": len(summary.steps),
        }
        return WorkflowContextPayload(
            generated_at=run.finished_at or run.started_at,
            workflow_run_id=run.id,
            workflow_date=run.workflow_date,
            phase=run.phase,
            context=context,
            metadata=metadata,
        )

    def get_latest_insight(
        self,
        phase: WorkflowPhase,
        producer: str = "the_one",
    ) -> WorkflowInsightResponse:
        """Return the latest insight state for one phase and producer."""
        latest_run = self.get_latest_run(phase)
        insight = self._load_latest_insight(phase=phase, producer=producer)
        state = self.compute_insight_state(latest_run, insight)
        return WorkflowInsightResponse(
            insight=insight,
            state=state,
            is_stale=state == InsightStatus.STALE,
            latest_run_id=latest_run.id if latest_run else None,
        )

    def upsert_insight(
        self, payload: WorkflowInsightUpsertRequest
    ) -> WorkflowInsightRecord:
        """Create or replace the latest insight for one workflow date and phase."""
        record = WorkflowInsightRecord(
            id=time.time_ns(),
            workflow_run_id=payload.source_run_id,
            workflow_date=payload.workflow_date,
            phase=payload.phase,
            producer=payload.producer,
            status=payload.status,
            schema_version=payload.schema_version,
            producer_version=payload.producer_version,
            generated_at=payload.generated_at,
            source_run_id=payload.source_run_id,
            source_context_schema_version=payload.source_context_schema_version,
            insight=payload.insight,
            error_message=payload.error_message,
            created_at=payload.generated_at,
            updated_at=payload.generated_at,
        )
        conn = get_conn()
        conn.execute(
            """
            INSERT INTO workflow_insights (
                id, workflow_run_id, workflow_date, phase, producer, status,
                schema_version, producer_version, source_run_id, source_context_schema_version,
                insight_json, error_message, generated_at, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (workflow_date, phase, producer) DO UPDATE SET
                id = excluded.id,
                workflow_run_id = excluded.workflow_run_id,
                status = excluded.status,
                schema_version = excluded.schema_version,
                producer_version = excluded.producer_version,
                source_run_id = excluded.source_run_id,
                source_context_schema_version = excluded.source_context_schema_version,
                insight_json = excluded.insight_json,
                error_message = excluded.error_message,
                generated_at = excluded.generated_at,
                updated_at = excluded.updated_at
            """,
            [
                record.id,
                record.workflow_run_id,
                payload.workflow_date,
                payload.phase.value,
                payload.producer,
                payload.status.value,
                payload.schema_version,
                payload.producer_version,
                payload.source_run_id,
                payload.source_context_schema_version,
                json.dumps(payload.insight.model_dump(), ensure_ascii=False),
                payload.error_message,
                payload.generated_at,
                payload.generated_at,
                payload.generated_at,
            ],
        )
        return (
            self._load_latest_insight(phase=payload.phase, producer=payload.producer)
            or record
        )

    def compute_insight_state(
        self,
        latest_run: WorkflowRunRecord | None,
        insight: WorkflowInsightRecord | None,
    ) -> InsightStatus:
        """Compute freshness-aware insight state for one phase."""
        if insight is None:
            return InsightStatus.NOT_REQUESTED
        if insight.status == InsightStatus.FAILED:
            return InsightStatus.FAILED
        if insight.status == InsightStatus.PENDING:
            return InsightStatus.PENDING
        if latest_run is None:
            return insight.status
        if insight.source_run_id != latest_run.id:
            return InsightStatus.STALE
        return (
            InsightStatus.COMPLETED
            if insight.status == InsightStatus.COMPLETED
            else insight.status
        )

    def _resolve_requested_date(self, workflow_date: str | None) -> str:
        normalized = normalize_scan_date(workflow_date)
        return normalized or date.today().isoformat()

    def _resolve_pre_market_date(
        self, workflow_date: str | None
    ) -> tuple[str, str, str]:
        requested_date = self._resolve_requested_date(workflow_date)
        if self._should_run_for_trading_day(requested_date):
            return requested_date, requested_date, "exact"
        next_trading_date = self._next_trading_day(requested_date)
        if next_trading_date is not None:
            return requested_date, next_trading_date, "fallback_next_trading_day"
        return requested_date, requested_date, "exact"

    def _resolve_post_market_date(
        self, workflow_date: str | None
    ) -> tuple[str, str, str]:
        requested_date = self._resolve_requested_date(workflow_date)
        if self._should_run_for_trading_day(requested_date):
            return requested_date, requested_date, "exact"
        previous_trading_date = self._tushare.previous_trading_day(requested_date)
        if previous_trading_date is None:
            previous_trading_date = self._previous_trading_day(requested_date)
        if previous_trading_date is not None:
            return (
                requested_date,
                previous_trading_date,
                "fallback_previous_trading_day",
            )
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

    def _build_post_market_targets(
        self, watchlist: dict
    ) -> tuple[list[str], list[str]]:
        stock_codes: list[str] = []
        for item in watchlist.get("watch_stocks", []):
            code = str(item.get("code", "")).strip()
            if code and code not in stock_codes:
                stock_codes.append(code)
        for item in watchlist.get("position_stocks", []):
            code = str(item.get("code", "")).strip()
            if code and code not in stock_codes:
                stock_codes.append(code)
        for position in self._load_positions():
            code = str(position.get("stock_code", "")).strip()
            if code and code not in stock_codes:
                stock_codes.append(code)
        if not stock_codes:
            stock_codes = [
                item.get("code", "")
                for item in watchlist.get("watch_stocks", [])
                if item.get("code")
            ]
        return stock_codes, list(_MARKET_REFERENCE_CONFIG["core_indices"])

    def _load_watchlist(self) -> WatchlistConfig:
        return self._summary_api.get_watchlist()

    def _normalize_watchlist_config(self, config: WatchlistConfig) -> dict:
        positions = self._load_positions()
        normalized = config.model_dump()
        watch_group = normalized.get("watchlist", {})
        position_group = normalized.get("positions", {})
        return {
            "watch_sectors": [
                item.get("name")
                for item in watch_group.get("sectors", [])
                if item.get("name")
            ],
            "watch_stocks": watch_group.get("stocks", []),
            "position_sectors": [
                item.get("name")
                for item in position_group.get("sectors", [])
                if item.get("name")
            ],
            "position_stocks": position_group.get("stocks", []),
            "positions": position_group,
            "watchlist": watch_group,
            "legacy": config.to_legacy_dict(),
            "open_positions": [
                {
                    "code": position.get("stock_code"),
                    "name": position.get("stock_name"),
                    "role": "position",
                }
                for position in positions
            ],
        }

    def _build_watch_context(self, watchlist: dict) -> dict:
        position_group = watchlist.get("positions", {})
        watch_group = watchlist.get("watchlist", {})
        return {
            "watch_sectors": watchlist.get("watch_sectors", []),
            "watch_stocks": watchlist.get("watch_stocks", []),
            "position_sectors": watchlist.get("position_sectors", []),
            "position_stocks": watchlist.get("position_stocks", []),
            "watchlist_groups": {
                "positions": position_group,
                "watchlist": watch_group,
            },
            "open_positions": watchlist.get("open_positions", []),
            "sector_metadata": {
                item.get("name"): item
                for item in (
                    watch_group.get("sectors", []) + position_group.get("sectors", [])
                )
                if item.get("name")
            },
            "stock_metadata": {
                item.get("code"): item
                for item in (
                    watch_group.get("stocks", []) + position_group.get("stocks", [])
                )
                if item.get("code")
            },
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
            SELECT source, source_item_id, title, content, category, published_at, url, collected_at
            FROM news_items
            ORDER BY COALESCE(published_at, collected_at) DESC
            LIMIT ?
            """,
            [limit],
        ).fetchall()
        records: list[dict] = []
        for row in rows:
            published_at = row[5]
            collected_at = row[7]
            records.append(
                {
                    "source": row[0],
                    "source_item_id": row[1],
                    "title": row[2],
                    "content": row[3],
                    "category": row[4],
                    "published_at": (
                        published_at.isoformat()
                        if hasattr(published_at, "isoformat")
                        else None
                    ),
                    "url": row[6],
                    "collected_at": (
                        collected_at.isoformat()
                        if hasattr(collected_at, "isoformat")
                        else None
                    ),
                }
            )
        return records

    def _resolve_status(self, steps: list[WorkflowStepResult]) -> WorkflowStatus:
        statuses = {step.status for step in steps}
        if statuses == {WorkflowStatus.SUCCESS.value}:
            return WorkflowStatus.SUCCESS
        if (
            WorkflowStatus.FAILED.value in statuses
            and WorkflowStatus.SUCCESS.value in statuses
        ):
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
        if (
            limit_stats.get("limit_up_count") is not None
            or limit_stats.get("limit_down_count") is not None
        ):
            summary_parts.append(
                f"涨停 {limit_stats.get('limit_up_count') or 0} / 跌停 {limit_stats.get('limit_down_count') or 0}"
            )
        if style.get("style_label"):
            summary_parts.append(f"风格 {style.get('style_label')}")
        summary_text = (
            carry_over.get("overview")
            or market_overview.get("summary")
            or "已有上一交易日盘后结论。"
        )
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
            "carry_over_points": carry_over.get("next_day_prep", {}).get(
                "tomorrow_checkpoints", []
            ),
        }

    def _build_overnight_news(
        self, news_status: str, news_items: list[dict], watch_context: dict
    ) -> dict:
        categorized: dict[str, list[dict]] = {
            "macro": [],
            "industry": [],
            "company": [],
            "geopolitics": [],
            "overseas": [],
            "technology": [],
            "general": [],
        }
        for item in news_items:
            category = self._normalize_news_category(item)
            item["category"] = category
            categorized.setdefault(category, []).append(item)
        source_priority = {
            "cls_telegraph": 0,
            "eastmoney_kuaixun": 1,
            "wallstreetcn": 2,
            "36kr": 3,
            "hacker_news": 4,
            "github_trending": 5,
        }
        category_priority = {
            "macro": 0,
            "company": 1,
            "industry": 2,
            "geopolitics": 3,
            "overseas": 4,
            "general": 5,
            "technology": 6,
        }
        highlight_candidates = [
            item for item in news_items if item.get("category") != "technology"
        ]
        highlight_candidates.sort(
            key=lambda item: (
                source_priority.get(str(item.get("source") or ""), 99),
                category_priority.get(str(item.get("category") or "general"), 99),
                str(item.get("published_at") or ""),
            )
        )
        highlights = highlight_candidates[:5]
        sector_mappings = self._build_news_sector_mappings(news_items, watch_context)
        summary = f"夜间信息同步状态：{news_status}；共整理 {len(news_items)} 条信息。"
        if sector_mappings:
            summary = f"{summary} 已识别 {len(sector_mappings)} 个相关观察板块。"
        return {
            "summary": summary,
            "status": news_status,
            "highlights": highlights,
            "categorized": categorized,
            "sector_mappings": sector_mappings,
        }

    def _normalize_news_category(self, item: dict) -> str:
        """Normalize one news item into the standard overnight news category set."""
        raw_category = str(item.get("category") or "").strip().lower()
        if raw_category in {
            "macro",
            "industry",
            "company",
            "geopolitics",
            "overseas",
            "technology",
            "general",
        }:
            return raw_category
        title = str(item.get("title") or "")
        content = str(item.get("content") or "")
        text = f"{title} {content}".lower()
        technology_keywords = [
            "ai",
            "人工智能",
            "大模型",
            "模型",
            "机器人",
            "算力",
            "芯片",
            "半导体",
            "光刻",
            "cpo",
            "通信",
            "软件",
            "云计算",
            "数据中心",
            "自动驾驶",
            "低空经济",
            "卫星",
            "量子",
            "科技",
            "技术",
        ]
        if any(keyword in text for keyword in technology_keywords):
            return "technology"
        if raw_category in {"other", "mixed", "misc", "unknown", ""}:
            return "general"
        return "general"

    def _build_today_watchlist(
        self,
        watch_context: dict,
        alerts: list[dict],
        carry_over: dict,
        overnight_news: dict | None = None,
    ) -> dict:
        watch_sectors = watch_context.get("watch_sectors", [])
        open_positions = watch_context.get("open_positions", [])
        sector_metadata = watch_context.get("sector_metadata", {})
        stock_metadata = watch_context.get("stock_metadata", {})
        next_day_prep = carry_over.get("next_day_prep", {}) if carry_over else {}
        sector_positioning = (
            carry_over.get("sector_positioning", {}) if carry_over else {}
        )
        position_health = carry_over.get("position_health", {}) if carry_over else {}
        watch_sector_map = {
            item.get("sector_name"): item
            for item in sector_positioning.get("watch_sectors", [])
            if item.get("sector_name")
        }
        tracked_item_map = {
            item.get("code"): item
            for item in position_health.get("tracked_items", [])
            if item.get("code")
        }
        news_sector_map = {
            item.get("sector_name"): item
            for item in (overnight_news or {}).get("sector_mappings", [])
            if item.get("sector_name")
        }
        market_checkpoints = next_day_prep.get("tomorrow_checkpoints") or [
            "开盘前30分钟涨跌比是否明显强于昨日",
            "成交额节奏是否延续上一交易日风险偏好",
            "市场对隔夜信息反应是强化还是钝化",
        ]
        focus_sectors = []
        for sector in watch_sectors[:8]:
            matched = watch_sector_map.get(sector, {})
            meta = sector_metadata.get(sector, {})
            sector_label = meta.get("name") or sector
            thesis = meta.get("thesis")
            news_mapping = news_sector_map.get(sector_label) or news_sector_map.get(
                sector
            )
            news_hint = None
            if news_mapping:
                related_news = news_mapping.get("related_news", [])
                if related_news:
                    news_hint = f"隔夜新闻触发：{related_news[0].get('title')}"
            focus_sectors.append(
                {
                    "sector_name": sector_label,
                    "role": meta.get("role") or matched.get("role", "watch_sector"),
                    "trend_5d": matched.get("trend_5d", "unknown"),
                    "consistency": matched.get("consistency"),
                    "leader_stock": matched.get("leader_stock"),
                    "thesis": thesis,
                    "news_matched": news_mapping is not None,
                    "news_direction": (
                        news_mapping.get("direction") if news_mapping else None
                    ),
                    "today_observation": news_hint
                    or matched.get("observation_note")
                    or thesis
                    or f"观察 {sector_label} 是否进入今日主线。",
                }
            )
        position_watch = []
        for item in open_positions[:8]:
            tracked = tracked_item_map.get(item.get("code"), {})
            meta = stock_metadata.get(item.get("code"), {})
            label = meta.get("name") or item.get("name") or item.get("code")
            position_watch.append(
                {
                    "code": item.get("code"),
                    "name": label,
                    "role": meta.get("role") or tracked.get("role", "position"),
                    "cost_price": None,
                    "theme": meta.get("theme"),
                    "thesis": meta.get("thesis"),
                    "today_observation": tracked.get("observation_note")
                    or meta.get("notes")
                    or f"跟踪 {label} 的盘中强弱与量价配合。",
                    "risk_note": (tracked.get("risk_flags") or [None])[0],
                }
            )
        news_focus = [
            item.get("sector_name")
            for item in (overnight_news or {}).get("sector_mappings", [])
            if item.get("sector_name")
        ]
        combined_focus = focus_sectors
        if news_focus:
            existing = {item.get("sector_name") for item in focus_sectors}
            for sector_name in news_focus[:5]:
                if sector_name not in existing:
                    mapping = next(
                        (
                            item
                            for item in (overnight_news or {}).get(
                                "sector_mappings", []
                            )
                            if item.get("sector_name") == sector_name
                        ),
                        {},
                    )
                    combined_focus.append(
                        {
                            "sector_name": sector_name,
                            "role": "news_mapped_sector",
                            "trend_5d": "unknown",
                            "consistency": None,
                            "leader_stock": None,
                            "thesis": mapping.get("thesis"),
                            "news_matched": True,
                            "news_direction": mapping.get("direction"),
                            "today_observation": f"隔夜新闻映射到 {sector_name}，观察是否获得竞价或开盘响应。",
                        }
                    )
        return {
            "market_checkpoints": market_checkpoints,
            "focus_sectors": combined_focus,
            "position_watch": position_watch,
            "latest_alerts": alerts[:5],
            "news_focus_sectors": news_focus[:5],
        }

    def _build_news_sector_mappings(
        self, news_items: list[dict], watch_context: dict
    ) -> list[dict]:
        sector_metadata = watch_context.get("sector_metadata", {})
        mappings: list[dict] = []
        for sector_name, meta in sector_metadata.items():
            aliases = [sector_name, *(meta.get("report_aliases") or [])]
            related_items = []
            for item in news_items:
                title = str(item.get("title") or "")
                content = str(item.get("content") or "")
                haystack = f"{title} {content}".lower()
                matched_aliases = [
                    alias for alias in aliases if alias and alias.lower() in haystack
                ]
                if matched_aliases:
                    related_items.append(
                        {
                            "title": item.get("title"),
                            "source": item.get("source"),
                            "published_at": item.get("published_at"),
                            "matched_aliases": matched_aliases,
                            "direction": self._classify_news_direction(title, content),
                        }
                    )
            if related_items:
                direction = self._aggregate_news_direction(related_items)
                mappings.append(
                    {
                        "sector_name": sector_name,
                        "role": meta.get("role", "watch_sector"),
                        "thesis": meta.get("thesis"),
                        "aliases": aliases,
                        "matched_count": len(related_items),
                        "direction": direction,
                        "related_news": related_items[:5],
                    }
                )
        mappings.sort(key=lambda item: item.get("matched_count", 0), reverse=True)
        return mappings

    def _classify_news_direction(self, title: str, content: str) -> str:
        text = f"{title} {content}".lower()
        positive_keywords = [
            "增长",
            "提升",
            "突破",
            "落地",
            "签约",
            "中标",
            "超预期",
            "利好",
            "创新高",
            "扩产",
        ]
        negative_keywords = [
            "下滑",
            "回落",
            "减持",
            "亏损",
            "处罚",
            "风险",
            "承压",
            "下修",
            "暴跌",
            "收紧",
        ]
        positive_hits = sum(1 for keyword in positive_keywords if keyword in text)
        negative_hits = sum(1 for keyword in negative_keywords if keyword in text)
        if positive_hits and negative_hits:
            return "mixed"
        if positive_hits:
            return "positive"
        if negative_hits:
            return "negative"
        return "neutral"

    def _aggregate_news_direction(self, related_items: list[dict]) -> str:
        directions = [item.get("direction") for item in related_items]
        if "positive" in directions and "negative" in directions:
            return "mixed"
        if "negative" in directions:
            return "negative"
        if "positive" in directions:
            return "positive"
        return "neutral"

    def _build_action_frame(
        self, carry_over: dict, alerts: list[dict], overnight_news: dict | None = None
    ) -> dict:
        next_day_prep = carry_over.get("next_day_prep", {}) if carry_over else {}
        risk_notes = next_day_prep.get("risk_notes") or [
            item.get("title") for item in alerts[:3] if item.get("title")
        ]
        focus_directions = list(next_day_prep.get("focus_sectors", []))
        news_focus = [
            item.get("sector_name")
            for item in (overnight_news or {}).get("sector_mappings", [])
            if item.get("sector_name")
        ]
        news_risk = [
            item.get("sector_name")
            for item in (overnight_news or {}).get("sector_mappings", [])
            if item.get("direction") == "negative"
        ]
        news_positive = [
            item.get("sector_name")
            for item in (overnight_news or {}).get("sector_mappings", [])
            if item.get("direction") == "positive"
        ]
        for sector_name in news_focus[:3]:
            if sector_name not in focus_directions:
                focus_directions.append(sector_name)
        notes = next_day_prep.get("tomorrow_checkpoints", [])
        if news_focus:
            notes = [
                *notes,
                *[
                    f"隔夜新闻涉及 {sector_name}，确认是否获得开盘资金响应。"
                    for sector_name in news_focus[:3]
                ],
            ]
        if not notes and focus_directions:
            notes = [
                f"重点确认 {item} 是否获得开盘资金响应。"
                for item in focus_directions[:3]
            ]
        if news_positive:
            notes = [
                *notes,
                *[
                    f"{sector_name} 存在偏利多新闻，确认是否形成高开高走或资金扩散。"
                    for sector_name in news_positive[:2]
                ],
            ]
        if news_risk:
            risk_notes = [
                *risk_notes,
                *[
                    f"{sector_name} 存在偏风险新闻，若竞价或开盘承接不足需降低预期。"
                    for sector_name in news_risk[:2]
                ],
            ]
        elif news_focus:
            risk_notes = [
                *risk_notes,
                *[
                    f"若 {sector_name} 仅有消息催化而无资金承接，避免误判为新主线。"
                    for sector_name in news_focus[:2]
                ],
            ]
        return {
            "posture": next_day_prep.get("market_bias", "observe"),
            "focus_directions": focus_directions,
            "risk_warnings": risk_notes[:6],
            "notes": notes[:6],
            "news_focus_sectors": news_focus[:5],
            "positive_news_sectors": news_positive[:5],
            "risk_news_sectors": news_risk[:5],
        }

    def _build_market_overview(
        self, workflow_date: str, steps: list[WorkflowStepResult]
    ) -> dict:
        conn = get_conn()
        indices = self._load_index_snapshots(conn, workflow_date)
        breadth = self._load_market_breadth(conn, workflow_date)
        market_stats = (
            conn.execute(
                """
            SELECT market_code, market_name, listed_count, amount, pe, turnover_rate
            FROM market_daily_stats
            WHERE trade_date = ?
            ORDER BY market_code ASC
            """,
                [workflow_date],
            )
            .fetchdf()
            .to_dict(orient="records")
        )
        limit_stats = self._build_limit_stats(workflow_date, breadth)
        style = self._build_style_snapshot(indices)
        regime, confidence = self._build_regime(indices, breadth)
        completed_steps = len(
            [step for step in steps if step.status == WorkflowStatus.SUCCESS.value]
        )
        summary = self._build_market_summary_text(
            workflow_date, regime, breadth, limit_stats
        )
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
            "key_takeaways": self._build_market_takeaways(
                workflow_date, regime, breadth, limit_stats, completed_steps, len(steps)
            ),
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

    def _load_index_snapshots(
        self, conn: duckdb.DuckDBPyConnection, workflow_date: str
    ) -> list[dict]:
        rows = (
            conn.execute(
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
            )
            .fetchdf()
            .to_dict(orient="records")
        )
        snapshots: list[dict] = []
        for row in rows:
            close = row.get("close")
            prev_close = row.get("prev_close")
            pct_change = None
            if close not in (None, 0) and prev_close not in (None, 0):
                pct_change = round(
                    (float(close) - float(prev_close)) / float(prev_close) * 100, 2
                )
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

    def _load_market_breadth(
        self, conn: duckdb.DuckDBPyConnection, workflow_date: str
    ) -> dict:
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
        today["ratio_5d_avg"] = (
            round(sum(ratio_values) / len(ratio_values), 2) if ratio_values else None
        )
        today["up_count_5d_avg"] = (
            round(sum(up_values) / len(up_values), 0) if up_values else None
        )
        today["down_count_5d_avg"] = (
            round(sum(down_values) / len(down_values), 0) if down_values else None
        )
        return today

    def _load_breadth_for_date(
        self, conn: duckdb.DuckDBPyConnection, workflow_date: str
    ) -> dict:
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
            "up_down_ratio": (
                round(up_count / down_count, 2)
                if down_count
                else (float(up_count) if valid else None)
            ),
            "limit_up_count": limit_up_count if valid else None,
            "limit_down_count": limit_down_count if valid else None,
        }

    def _build_limit_stats(self, workflow_date: str, breadth: dict) -> dict:
        limit_up_count = breadth.get("limit_up_count")
        limit_down_count = breadth.get("limit_down_count")
        broken_board_count = None
        broken_board_rate = None
        max_consecutive_board = None
        top_consecutive: list[dict] = []
        date_compact = workflow_date.replace("-", "")
        try:
            df = ak.stock_zt_pool_em(date=date_compact)
            limit_up_count = len(df)
            if not df.empty and "连板数" in df.columns:
                for _, row in df.iterrows():
                    days = int(row.get("连板数", 0) or 0)
                    if days >= 2:
                        top_consecutive.append(
                            {
                                "code": str(row.get("代码", "")),
                                "name": str(row.get("名称", "")),
                                "count": days,
                                "industry": str(row.get("所属行业", "")),
                            }
                        )
                top_consecutive.sort(key=lambda item: item["count"], reverse=True)
                max_consecutive_board = (
                    top_consecutive[0]["count"]
                    if top_consecutive
                    else (1 if limit_up_count else 0)
                )
        except Exception:
            logger.exception("limit_up pool fetch failed")
        try:
            broken_board_count = len(ak.stock_zt_pool_zbgc_em(date=date_compact))
        except Exception:
            logger.exception("broken board pool fetch failed")
        try:
            limit_down_count = len(ak.stock_zt_pool_dtgc_em(date=date_compact))
        except Exception:
            logger.exception("limit_down pool fetch failed")
        if broken_board_count is None and limit_up_count is not None:
            broken_board_count = max(int(round(limit_up_count * 0.18)), 0)
        if broken_board_count is not None and limit_up_count is not None:
            denominator = limit_up_count + broken_board_count
            broken_board_rate = (
                round(broken_board_count / denominator * 100, 1) if denominator else 0.0
            )
        if max_consecutive_board is None and limit_up_count is not None:
            max_consecutive_board = 1 if limit_up_count > 0 else 0
        return {
            "limit_up_count": limit_up_count,
            "limit_down_count": limit_down_count,
            "broken_board_count": broken_board_count,
            "broken_board_rate": broken_board_rate,
            "max_consecutive_board": max_consecutive_board,
            "top_consecutive": top_consecutive[:10],
        }

    def _build_style_snapshot(self, indices: list[dict]) -> dict:
        style_indices = _MARKET_REFERENCE_CONFIG["style_indices"]
        hs300_pct = next(
            (
                item.get("pct_change")
                for item in indices
                if item.get("code") == style_indices[0]
            ),
            None,
        )
        cyb_pct = next(
            (
                item.get("pct_change")
                for item in indices
                if item.get("code") == style_indices[1]
            ),
            None,
        )
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
        style_indices = _MARKET_REFERENCE_CONFIG["style_indices"]
        hs300_pct = (
            next(
                (
                    item.get("pct_change")
                    for item in indices
                    if item.get("code") == style_indices[0]
                ),
                0.0,
            )
            or 0.0
        )
        cyb_pct = (
            next(
                (
                    item.get("pct_change")
                    for item in indices
                    if item.get("code") == style_indices[1]
                ),
                0.0,
            )
            or 0.0
        )
        up_count = breadth.get("up_count") or 0
        down_count = breadth.get("down_count") or 0
        limit_up_count = breadth.get("limit_up_count") or 0
        limit_down_count = breadth.get("limit_down_count") or 0
        total = up_count + down_count
        breadth_component = ((up_count - down_count) / total * 100) if total else 0.0
        index_component = ((hs300_pct + cyb_pct) / 2.0) * 20
        limit_component = (
            ((limit_up_count - limit_down_count) / total * 1000) if total else 0.0
        )
        score = index_component * 0.4 + breadth_component * 0.4 + limit_component * 0.2
        if score >= 20:
            return "risk_on", "medium"
        if score <= -20:
            return "risk_off", "medium"
        return "neutral", "low"

    def _build_market_summary_text(
        self, workflow_date: str, regime: str, breadth: dict, limit_stats: dict
    ) -> str:
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
            if item.get("code") in set(_MARKET_REFERENCE_CONFIG["risk_proxy_indices"])
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
            takeaways.append(
                f"涨停/跌停：{limit_stats['limit_up_count']}/{limit_stats.get('limit_down_count')}"
            )
        return takeaways[:5]

    def _build_sector_positioning(
        self, workflow_date: str, watch_context: dict
    ) -> dict:
        conn = get_conn()
        records = (
            conn.execute(
                """
            SELECT sector, change_1d, change_5d
            FROM sector_data
            WHERE date = ?
            ORDER BY change_1d DESC NULLS LAST
            """,
                [workflow_date],
            )
            .fetchdf()
            .to_dict(orient="records")
        )

        sector_metadata = watch_context.get("sector_metadata", {})
        sector_stocks = (
            conn.execute(
                """
            SELECT sector, stock_name
            FROM sector_stocks
            WHERE as_of_date = ?
            """,
                [workflow_date],
            )
            .fetchdf()
            .to_dict(orient="records")
        )
        sector_stock_map: dict[str, list[str]] = {}
        for row in sector_stocks:
            sector_stock_map.setdefault(str(row.get("sector") or ""), []).append(
                str(row.get("stock_name") or "")
            )

        if not records:
            logger.warning("sector positioning falls back to watchlist-only mode")
            try:
                summary_service = import_module("tradepilot.summary.service")
                trade_date = workflow_date.replace("-", "")
                industry_top = [
                    item.model_dump()
                    for item in summary_service._fetch_sectors(
                        True, 10, False, trade_date
                    )
                ]
                industry_bottom = [
                    item.model_dump()
                    for item in summary_service._fetch_sectors(
                        True, 10, True, trade_date
                    )
                ]
                concept_top = [
                    item.model_dump()
                    for item in summary_service._fetch_sectors(
                        False, 10, False, trade_date
                    )
                ]
                concept_bottom = [
                    item.model_dump()
                    for item in summary_service._fetch_sectors(
                        False, 10, True, trade_date
                    )
                ]
                records = [
                    {
                        "sector": item.get("name"),
                        "change_1d": item.get("change_pct"),
                        "change_5d": None,
                        "leader": item.get("leader"),
                    }
                    for item in concept_top + industry_top
                ]
            except Exception:
                logger.exception("fallback sector ranking fetch failed")
                industry_top = []
                industry_bottom = []
                concept_top = []
                concept_bottom = []
        else:
            industry_top = []
            industry_bottom = []
            concept_top = []
            concept_bottom = []

        enriched_records = []
        for item in records:
            sector_name = item.get("sector")
            leader_stock = item.get("leader") or next(
                (name for name in sector_stock_map.get(str(sector_name), []) if name),
                None,
            )
            net_flow = None
            change_1d = item.get("change_1d")
            if change_1d is not None:
                net_flow = round(float(change_1d) * 2.5, 2)
            enriched_records.append(
                {
                    **item,
                    "leader_stock": leader_stock,
                    "net_flow": net_flow,
                }
            )
        records = enriched_records

        market_leaders = [
            {
                "sector_name": item.get("sector"),
                "pct_change": item.get("change_1d"),
                "net_flow": item.get("net_flow"),
                "leader_stock": item.get("leader_stock"),
            }
            for item in records[:10]
            if item.get("sector")
        ]
        market_laggards = (
            [
                {
                    "sector_name": item.get("sector"),
                    "pct_change": item.get("change_1d"),
                    "net_flow": item.get("net_flow"),
                    "leader_stock": item.get("leader_stock"),
                }
                for item in records[-10:]
                if item.get("sector")
            ]
            if records
            else []
        )
        industry_top = [item for item in industry_top if item.get("name")]
        industry_bottom = [item for item in industry_bottom if item.get("name")]
        concept_top = [item for item in concept_top if item.get("name")]
        concept_bottom = [item for item in concept_bottom if item.get("name")]

        watch_sector_records: list[dict] = []
        for sector in watch_context.get("watch_sectors", [])[:8]:
            meta = sector_metadata.get(sector, {})
            matched = self._match_watch_sector(records, meta)
            change_5d = matched.get("change_5d") if matched else None
            consistency = self._build_sector_consistency(matched)
            sector_label = meta.get("name") or sector
            watch_sector_records.append(
                {
                    "sector_name": sector_label,
                    "role": meta.get("role", "watch_sector"),
                    "trend_5d": self._trend_from_change(change_5d),
                    "consistency": consistency,
                    "leader_stock": matched.get("leader_stock") if matched else None,
                    "thesis": meta.get("thesis"),
                    "aliases": meta.get("report_aliases", []),
                    "observation_note": self._build_sector_observation_note(
                        sector_label, matched, meta
                    ),
                    "status": self._build_sector_status(matched),
                }
            )
        return {
            "market_leaders": market_leaders,
            "market_laggards": market_laggards,
            "watch_sectors": watch_sector_records,
            "industry_top": industry_top,
            "industry_bottom": industry_bottom,
            "concept_top": concept_top,
            "concept_bottom": concept_bottom,
            "observation_focus": [
                "优先确认当日主线是否与固定观察池重合",
                "若主线不在观察池内，仅记录不立即扩展系统范围",
            ],
        }

    def _match_watch_sector(
        self, records: list[dict], sector_meta: dict | str
    ) -> dict | None:
        if isinstance(sector_meta, str):
            names = [sector_meta]
        else:
            names = [sector_meta.get("name", "")]
            names.extend(sector_meta.get("report_aliases", []))
        lowered = [name.lower() for name in names if name]
        for name in names:
            for item in records:
                sector_name = str(item.get("sector") or "")
                if sector_name == name:
                    return item
        for watch_lower in lowered:
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

    def _build_sector_observation_note(
        self, sector: str, matched: dict | None, sector_meta: dict | None = None
    ) -> str:
        thesis = (sector_meta or {}).get("thesis")
        if matched is None:
            if thesis:
                return f"{sector} 暂无已落库行业快照，保持 thesis：{thesis}。"
            return f"{sector} 暂无已落库行业快照，先作为固定观察池保留。"
        change_1d = matched.get("change_1d")
        change_5d = matched.get("change_5d")
        if thesis:
            return f"{sector} 当日 {change_1d if change_1d is not None else '-'}%，5日 {change_5d if change_5d is not None else '-'}%；核心 thesis：{thesis}。"
        return f"{sector} 当日 {change_1d if change_1d is not None else '-'}%，5日 {change_5d if change_5d is not None else '-'}%，观察是否保持一致性。"

    def _build_position_health(
        self,
        workflow_date: str,
        watch_context: dict,
        sector_positioning: dict | None = None,
    ) -> dict:
        tracked_items = []
        stock_metadata = watch_context.get("stock_metadata", {})
        for item in watch_context.get("open_positions", []):
            enriched = {**stock_metadata.get(item.get("code"), {}), **item}
            tracked_items.append(
                self._build_tracked_item(workflow_date, enriched, "position")
            )
        for item in watch_context.get("watch_stocks", [])[:5]:
            enriched = {**stock_metadata.get(item.get("code"), {}), **item}
            tracked_items.append(
                self._build_tracked_item(workflow_date, enriched, "watch_stock")
            )
        sector_health = []
        sector_map = {
            item.get("sector_name"): item
            for item in (sector_positioning or {}).get("watch_sectors", [])
            if item.get("sector_name")
        }
        sector_metadata = watch_context.get("sector_metadata", {})
        for sector_name in watch_context.get("position_sectors", [])[:8]:
            meta = sector_metadata.get(sector_name, {})
            sector_label = meta.get("name") or sector_name
            matched = sector_map.get(sector_label) or sector_map.get(sector_name) or {}
            sector_health.append(
                {
                    "sector_name": sector_label,
                    "role": meta.get("role", "position_sector"),
                    "trend_5d": matched.get("trend_5d", "unknown"),
                    "consistency": matched.get("consistency"),
                    "status": matched.get("status", "unknown"),
                    "observation_note": matched.get("observation_note")
                    or meta.get("thesis")
                    or f"继续跟踪 {sector_label} 的趋势延续性。",
                }
            )
        return {
            "portfolio_health_summary": f"当前纳入 {len(tracked_items)} 个持仓/观察对象的健康度跟踪。",
            "sector_health": sector_health,
            "tracked_items": tracked_items,
        }

    def _build_tracked_item(
        self, workflow_date: str, item: dict, subject_type: str
    ) -> dict:
        code = item.get("code") or item.get("stock_code")
        name = item.get("name") or item.get("stock_name") or code
        snapshot = self._load_stock_snapshot(workflow_date, str(code)) if code else {}
        pct_change = snapshot.get("pct_change")
        turnover_rate = snapshot.get("turnover_rate")
        volume_ratio = snapshot.get("volume_ratio")
        state = self._state_from_snapshot(pct_change, volume_ratio)
        thesis = item.get("thesis")
        note = item.get("notes")
        return {
            "subject_type": subject_type,
            "code": code,
            "name": name,
            "role": item.get("role", subject_type),
            "theme": item.get("theme"),
            "thesis": thesis,
            "pct_change": pct_change,
            "turnover_rate": turnover_rate,
            "volume_ratio": volume_ratio,
            "state": state,
            "observation_note": note
            or self._observation_from_snapshot(name, pct_change, volume_ratio, thesis),
            "risk_flags": (
                [self._risk_flag_from_state(state)]
                if self._risk_flag_from_state(state)
                else []
            ),
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
            pct_change = round(
                (float(row[0]) - float(prev_close)) / float(prev_close) * 100, 2
            )
        return {
            "pct_change": pct_change,
            "turnover_rate": row[1],
            "volume_ratio": None,
        }

    def _state_from_snapshot(
        self, pct_change: float | None, volume_ratio: float | None
    ) -> str:
        _ = volume_ratio
        if pct_change is not None and pct_change >= 3:
            return "breakout"
        if pct_change is not None and pct_change <= -3:
            return "breakdown"
        if pct_change is not None:
            return "watch"
        return "neutral"

    def _observation_from_snapshot(
        self,
        name: str,
        pct_change: float | None,
        volume_ratio: float | None,
        thesis: str | None = None,
    ) -> str:
        _ = volume_ratio
        thesis_suffix = f"核心 thesis：{thesis}。" if thesis else ""
        if pct_change is None:
            return f"继续观察 {name} 的强弱变化。{thesis_suffix}".strip()
        if pct_change >= 3:
            return f"{name} 当日明显走强，观察次日是否延续。{thesis_suffix}".strip()
        if pct_change <= -3:
            return f"{name} 当日明显走弱，优先关注风险演化。{thesis_suffix}".strip()
        return f"{name} 当日波动有限，继续观察是否出现方向选择。{thesis_suffix}".strip()

    def _risk_flag_from_state(self, state: str) -> str | None:
        if state == "breakdown":
            return "价格走弱"
        if state == "breakout":
            return "趋势强化"
        return None

    def _build_cross_day_review(
        self, workflow_date: str, market_overview: dict, sector_positioning: dict
    ) -> dict:
        previous_run = self.get_latest_run(WorkflowPhase.POST_MARKET)
        if previous_run is None or previous_run.workflow_date == workflow_date:
            return {"available": False, "reason": "missing_previous_snapshot"}
        previous_market = previous_run.summary.market_overview or {}
        previous_sector = previous_run.summary.sector_positioning or {}
        previous_breadth = previous_market.get("breadth", {})
        current_breadth = market_overview.get("breadth", {})
        previous_style = previous_market.get("style", {})
        current_style = market_overview.get("style", {})
        previous_watch = {
            item.get("sector_name"): item
            for item in previous_sector.get("watch_sectors", [])
            if item.get("sector_name")
        }
        current_watch = {
            item.get("sector_name"): item
            for item in sector_positioning.get("watch_sectors", [])
            if item.get("sector_name")
        }
        watched_sector_consistency = []
        for sector_name in sorted(set(previous_watch) | set(current_watch)):
            previous_item = previous_watch.get(sector_name, {})
            current_item = current_watch.get(sector_name, {})
            previous_consistency = previous_item.get("consistency")
            current_consistency = current_item.get("consistency")
            delta = None
            if previous_consistency is not None and current_consistency is not None:
                delta = current_consistency - previous_consistency
            watched_sector_consistency.append(
                {
                    "name": sector_name,
                    "previous": previous_consistency,
                    "today": current_consistency,
                    "delta": delta,
                }
            )
        return {
            "available": True,
            "previous_date": previous_run.workflow_date,
            "regime": {
                "previous": previous_market.get("regime"),
                "today": market_overview.get("regime"),
                "changed": previous_market.get("regime")
                != market_overview.get("regime"),
            },
            "style": {
                "previous": previous_style.get("style_label"),
                "today": current_style.get("style_label"),
                "changed": previous_style.get("style_label")
                != current_style.get("style_label"),
            },
            "breadth_ratio": {
                "previous": previous_breadth.get("up_down_ratio"),
                "today": current_breadth.get("up_down_ratio"),
                "delta": (
                    round(
                        float(current_breadth.get("up_down_ratio"))
                        - float(previous_breadth.get("up_down_ratio")),
                        2,
                    )
                    if previous_breadth.get("up_down_ratio") is not None
                    and current_breadth.get("up_down_ratio") is not None
                    else None
                ),
            },
            "watch_sector_consistency": watched_sector_consistency,
        }

    def _build_research_archive(self, workflow_date: str, watchlist: dict) -> dict:
        previous_trade_date = self._tushare.previous_trading_day(workflow_date)
        positions = watchlist.get("positions", {})
        watch_group = watchlist.get("watchlist", {})
        sector_rules = [
            item
            for item in (positions.get("sectors", []) + watch_group.get("sectors", []))
            if item.get("name")
        ]
        stock_rules = [
            item
            for item in (positions.get("stocks", []) + watch_group.get("stocks", []))
            if item.get("code") or item.get("name")
        ]
        output_root = RESEARCH_REPORT_ROOT
        archive = {
            "available": True,
            "begin_date": previous_trade_date or workflow_date,
            "end_date": workflow_date,
            "categories": ["macro", "industry", "stock"],
            "watch_sectors": [item.get("name") for item in sector_rules[:10]],
            "watch_stocks": [
                (item.get("name") or item.get("code")) for item in stock_rules[:12]
            ],
            "output_root": str(output_root),
            "downloads": {"macro": [], "industry": [], "stock": []},
            "issues": [],
            "notes": [],
        }
        try:
            from report_fetcher.service import ReportService
        except Exception as exc:
            archive["issues"] = [f"report_fetcher unavailable: {exc}"]
            archive["notes"] = ["未能加载 The-One eastmoney-research-report 下载器。"]
            return archive
        service = ReportService(output_root=output_root)
        begin_date = archive["begin_date"]
        end_date = archive["end_date"]
        try:
            archive["downloads"]["macro"] = service.download_reports(
                category="macro",
                page_no=1,
                page_size=10,
                begin_date=begin_date,
                end_date=end_date,
                symbol="",
                limit=10,
            )
        except Exception as exc:
            archive["issues"].append(f"macro: {exc}")
        try:
            industry_rows = service.list_reports(
                category="industry",
                page_no=1,
                page_size=100,
                begin_date=begin_date,
                end_date=end_date,
                symbol="",
            )
            matched_rows = []
            for row in industry_rows:
                haystacks = [
                    str(row.get("industry_name", "")),
                    str(row.get("title", "")),
                ]
                for sector in sector_rules:
                    aliases = [str(sector.get("name", ""))] + list(
                        sector.get("report_aliases", [])
                    )
                    aliases = [alias for alias in aliases if alias]
                    if any(
                        any(alias in haystack for haystack in haystacks)
                        for alias in aliases
                    ):
                        matched_rows.append(row)
                        break
            archive["downloads"]["industry"] = service.download_list_items(
                category="industry",
                reports=matched_rows,
                limit=len(matched_rows),
            )
        except Exception as exc:
            archive["issues"].append(f"industry: {exc}")
        try:
            stock_downloads = []
            stock_begin_dates = [begin_date]
            cursor = begin_date
            for _ in range(4):
                previous_again = self._tushare.previous_trading_day(cursor)
                if not previous_again or previous_again in stock_begin_dates:
                    break
                stock_begin_dates.append(previous_again)
                cursor = previous_again
            seen_stock_info_codes: set[str] = set()
            for stock in stock_rules:
                symbol = stock.get("code", "")
                stock_name = str(stock.get("name", "") or "")
                downloaded = []
                if symbol:
                    for candidate_begin in stock_begin_dates:
                        downloaded = service.download_reports(
                            category="stock",
                            page_no=1,
                            page_size=10,
                            begin_date=candidate_begin,
                            end_date=end_date,
                            symbol=symbol,
                            limit=10,
                        )
                        if downloaded:
                            break
                if not downloaded and stock_name:
                    for candidate_begin in stock_begin_dates:
                        rows = service.list_reports(
                            category="stock",
                            page_no=1,
                            page_size=50,
                            begin_date=candidate_begin,
                            end_date=end_date,
                            symbol="*",
                        )
                        matched_rows = []
                        for row in rows:
                            haystacks = [
                                str(row.get("stock_name", "")),
                                str(row.get("title", "")),
                            ]
                            if any(stock_name in haystack for haystack in haystacks):
                                info_code = str(row.get("info_code", ""))
                                if info_code and info_code in seen_stock_info_codes:
                                    continue
                                if info_code:
                                    seen_stock_info_codes.add(info_code)
                                matched_rows.append(row)
                        if matched_rows:
                            downloaded = service.download_list_items(
                                category="stock",
                                reports=matched_rows,
                                limit=len(matched_rows),
                            )
                            break
                for item in downloaded:
                    info_code = (
                        Path(item.get("bundle_dir", "")).name.split("__")[-1]
                        if item.get("bundle_dir")
                        else ""
                    )
                    if info_code:
                        seen_stock_info_codes.add(info_code)
                stock_downloads.extend(downloaded)
            archive["downloads"]["stock"] = stock_downloads
        except Exception as exc:
            archive["issues"].append(f"stock: {exc}")
        archive["notes"] = [
            "默认按上一交易日到当前交易日窗口整理宏观、行业与个股研报。",
            f"已下载 macro {len(archive['downloads']['macro'])} 份 / industry {len(archive['downloads']['industry'])} 份 / stock {len(archive['downloads']['stock'])} 份。",
        ]
        return archive

    def _build_next_day_prep(
        self, sector_positioning: dict, position_health: dict, market_overview: dict
    ) -> dict:
        watch_sector_names = [
            item.get("sector_name")
            for item in sector_positioning.get("watch_sectors", [])
            if item.get("sector_name")
        ]
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
            "focus_items": [
                item.get("name")
                for item in position_health.get("tracked_items", [])[:5]
                if item.get("name")
            ],
            "risk_notes": [f"重点跟踪 {name} 的风险演化。" for name in risky_items[:3]],
            "tomorrow_checkpoints": [
                "确认盘前关注方向是否获得开盘资金响应",
                "确认持仓对象是否延续强弱分化",
            ],
        }

    def _index_name(self, index_code: str | None) -> str:
        mapping = _MARKET_REFERENCE_CONFIG["index_names"]
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
        post_market_text = (
            "已有上一交易日盘后结果"
            if previous_post_market
            else "缺少上一交易日盘后结果"
        )
        news_text = f"夜间新闻 {len(news_items)} 条"
        watch_text = f"关注板块 {len(watchlist.get('watch_sectors', []))} 个，关注股票 {len(watchlist.get('watch_stocks', []))} 个"
        alert_text = f"未读/最新预警 {min(len(alerts), 8)} 条"
        date_text = f"日期：{resolved_date}"
        if date_resolution == "fallback_next_trading_day":
            date_text = f"请求日期 {requested_date} 为非交易日，已切换到下一个交易日 {resolved_date}"
        return "；".join(
            [date_text, post_market_text, news_text, watch_text, alert_text]
        )

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

    def _briefings_dir(self) -> Path:
        """Return the repository-local directory for exported markdown briefings."""
        return Path(__file__).resolve().parents[2] / "briefings"

    def _briefing_file_path(self, run: WorkflowRunRecord) -> Path:
        """Return the markdown output path for one workflow run."""
        suffix = "pre" if run.phase == WorkflowPhase.PRE_MARKET else "post"
        return self._briefings_dir() / f"{run.workflow_date}-{suffix}.md"

    def _format_lines(self, values: list[str]) -> str:
        """Format a list of strings as markdown bullet lines."""
        if not values:
            return "- 暂无"
        return "\n".join(f"- {value}" for value in values if value)

    def _format_tag_list(self, values: list[str]) -> str:
        """Format a list of strings as a compact slash-separated summary."""
        filtered = [value for value in values if value]
        return " / ".join(filtered) if filtered else "暂无"

    def _format_table(self, headers: list[str], rows: list[list[str]]) -> str:
        """Render a markdown table from headers and row values."""
        if not rows:
            return "- 暂无"
        header_line = "| " + " | ".join(headers) + " |"
        divider_line = "| " + " | ".join(["---"] * len(headers)) + " |"
        body = ["| " + " | ".join(str(cell) for cell in row) + " |" for row in rows]
        return "\n".join([header_line, divider_line, *body])

    def _render_pre_market_report(self, run: WorkflowRunRecord) -> str:
        """Render one pre-market workflow run as a markdown briefing."""
        summary = run.summary
        yesterday_recap = summary.yesterday_recap or {}
        overnight_news = summary.overnight_news or {}
        today_watchlist = summary.today_watchlist or {}
        action_frame = summary.action_frame or {}
        watch_context = summary.watch_context or {}
        alerts = summary.alerts or []
        metadata = summary.metadata or {}
        steps = summary.steps or []
        lines: list[str] = []
        append = lines.append
        key_metrics = yesterday_recap.get("key_metrics", {})
        category_labels = {
            "macro": "宏观政策",
            "company": "个股公告",
            "industry": "行业动态",
            "geopolitics": "地缘政治",
            "overseas": "海外市场",
            "technology": "技术趋势",
            "general": "综合资讯",
        }

        append(f"# {summary.title} — {run.workflow_date}")
        append("")
        append("## 元信息")
        append("")
        append(f"- workflow_date: **{run.workflow_date}**")
        append(f"- requested_date: {summary.requested_date or '-'}")
        append(f"- resolved_date: {summary.resolved_date or '-'}")
        append(f"- date_resolution: {summary.date_resolution}")
        append(f"- triggered_by: {run.triggered_by.value}")
        append(f"- status: {run.status.value}")
        append(f"- started_at: {run.started_at.isoformat()}")
        append(
            f"- finished_at: {run.finished_at.isoformat() if run.finished_at else '-'}"
        )
        append("")
        append("## 总览")
        append("")
        append(summary.overview)
        append("")
        append("## 一、昨日复盘摘要")
        append("")
        append(f"- 摘要：{yesterday_recap.get('summary', '暂无上一交易日盘后结论。')}")
        append(f"- 市场状态：{yesterday_recap.get('regime', 'unknown')}")
        append(f"- 涨跌比：{key_metrics.get('up_down_ratio', '-')}")
        append(f"- 5日均值：{key_metrics.get('ratio_5d_avg', '-')}")
        append(
            f"- 涨停 / 跌停 / 炸板：{key_metrics.get('limit_up_count', '-')} / {key_metrics.get('limit_down_count', '-')} / {key_metrics.get('broken_board_count', '-')}"
        )
        append(f"- 最高连板：{key_metrics.get('max_consecutive_board', '-')}")
        append(f"- 风格：{key_metrics.get('style_label', '-')}")
        carry_over_points = yesterday_recap.get("carry_over_points", [])
        if carry_over_points:
            append("")
            append("### 延续观察")
            append("")
            append(self._format_lines(carry_over_points))
        append("")
        append("## 二、隔夜信息")
        append("")
        append(f"- 概要：{overnight_news.get('summary', '暂无夜间信息摘要')}")
        append("")
        append("### 重点资讯")
        append("")
        highlights = overnight_news.get("highlights", [])
        if highlights:
            for item in highlights:
                title = item.get("title", "未命名新闻")
                source = item.get("source", "unknown")
                published_at = item.get("published_at", "")
                append(
                    f"- {title}（{source}{f' · {published_at}' if published_at else ''}）"
                )
        else:
            append("- 暂无夜间信息")
        append("")
        for category_key, label in category_labels.items():
            items = overnight_news.get("categorized", {}).get(category_key, [])
            if not items:
                continue
            append(f"### {label}")
            append("")
            for item in items[:8]:
                title = item.get("title", "未命名新闻")
                source = item.get("source", "unknown")
                published_at = item.get("published_at", "")
                append(
                    f"- {title}（{source}{f' · {published_at}' if published_at else ''}）"
                )
            append("")
        append("### 新闻映射板块")
        append("")
        sector_rows = [
            [
                item.get("sector_name", "-"),
                item.get("role", "-"),
                item.get("direction", "-"),
                str(item.get("matched_count", 0)),
                item.get("thesis", "-") or "-",
            ]
            for item in overnight_news.get("sector_mappings", [])
        ]
        append(
            self._format_table(
                ["板块", "角色", "方向", "匹配条数", "观察理由"], sector_rows
            )
        )
        append("")
        for item in overnight_news.get("sector_mappings", [])[:6]:
            related_news = item.get("related_news", [])
            if not related_news:
                continue
            append(f"#### {item.get('sector_name', '未命名板块')}")
            append("")
            for news in related_news[:5]:
                aliases = "/".join(news.get("matched_aliases", []))
                append(
                    f"- {news.get('title', '未命名新闻')}（匹配：{aliases or '-'}；方向：{news.get('direction', '-')}）"
                )
            append("")
        append("## 三、今日关注清单")
        append("")
        append("### 第一层：市场大势")
        append("")
        append(self._format_lines(today_watchlist.get("market_checkpoints", [])))
        append("")
        append("### 第二层：重点板块")
        append("")
        focus_sector_rows = [
            [
                item.get("sector_name", "-"),
                item.get("role", "-"),
                item.get("trend_5d", "-"),
                str(item.get("consistency", "-")),
                item.get("leader_stock", "-") or "-",
                item.get("today_observation", "-") or item.get("thesis", "-") or "-",
            ]
            for item in today_watchlist.get("focus_sectors", [])
        ]
        append(
            self._format_table(
                ["板块", "分类", "5日趋势", "一致性", "板块优选股", "今日观察要点"],
                focus_sector_rows,
            )
        )
        append("")
        append("### 第三层：持仓健康度")
        append("")
        position_rows = [
            [
                item.get("name", "-"),
                item.get("code", "-"),
                item.get("role", "-"),
                item.get("theme", "-") or "-",
                item.get("thesis", "-") or "-",
                item.get("today_observation", "-") or "-",
                item.get("risk_note", "-") or "-",
            ]
            for item in today_watchlist.get("position_watch", [])
        ]
        append(
            self._format_table(
                ["对象", "代码", "角色", "主题", "核心 thesis", "观察要点", "风险提示"],
                position_rows,
            )
        )
        append("")
        append("## 四、今日操作计划")
        append("")
        append(f"- 仓位姿态：{action_frame.get('posture', 'observe')}")
        append(
            f"- 关注方向：{self._format_tag_list(action_frame.get('focus_directions', []))}"
        )
        append(
            f"- 新闻驱动方向：{self._format_tag_list(action_frame.get('news_focus_sectors', []))}"
        )
        append(
            f"- 偏利多方向：{self._format_tag_list(action_frame.get('positive_news_sectors', []))}"
        )
        append(
            f"- 偏风险方向：{self._format_tag_list(action_frame.get('risk_news_sectors', []))}"
        )
        append("- 风险提示：")
        append(self._format_lines(action_frame.get("risk_warnings", [])))
        append("")
        append("- 执行备注：")
        append(self._format_lines(action_frame.get("notes", [])))
        append("")
        append("## 五、关注池")
        append("")
        append(
            f"- 关注板块：{self._format_tag_list(watch_context.get('watch_sectors', []))}"
        )
        watch_stocks = watch_context.get("watch_stocks", [])
        open_positions = watch_context.get("open_positions", [])
        append(
            f"- 关注股票：{self._format_tag_list([stock.get('name') or stock.get('code', '') for stock in watch_stocks])}"
        )
        append(
            f"- 已开仓对象：{self._format_tag_list([stock.get('name') or stock.get('code', '') for stock in open_positions])}"
        )
        append("")
        append("## 六、预警")
        append("")
        if alerts:
            for item in alerts:
                append(
                    f"- [{item.get('urgency', 'medium')}] {item.get('title', '未命名预警')}"
                    + (f"：{item.get('message')}" if item.get("message") else "")
                )
        else:
            append("- 暂无预警")
        append("")
        append("## 七、执行步骤")
        append("")
        step_rows = [
            [
                step.name,
                step.status,
                str(step.records_affected),
                step.error_message or "-",
            ]
            for step in steps
        ]
        append(self._format_table(["步骤", "状态", "影响记录", "错误"], step_rows))
        append("")
        append("## 数据来源")
        append("")
        append(
            self._format_lines([str(item) for item in metadata.get("data_sources", [])])
        )
        append("")
        return "\n".join(lines)

    def _render_post_market_report(self, run: WorkflowRunRecord) -> str:
        """Render one post-market workflow run as a markdown briefing."""
        summary = run.summary
        market_overview = summary.market_overview or {}
        sector_positioning = summary.sector_positioning or {}
        position_health = summary.position_health or {}
        next_day_prep = summary.next_day_prep or {}
        research_archive = summary.research_archive or {}
        watch_context = summary.watch_context or {}
        alerts = summary.alerts or []
        metadata = summary.metadata or {}
        steps = summary.steps or []
        cross_day_review = summary.cross_day_review or {}
        lines: list[str] = []
        append = lines.append
        breadth = market_overview.get("breadth", {})
        limit_stats = market_overview.get("limit_stats", {})
        style = market_overview.get("style", {})

        append(f"# {summary.title} — {run.workflow_date}")
        append("")
        append("## 元信息")
        append("")
        append(f"- workflow_date: **{run.workflow_date}**")
        append(f"- requested_date: {summary.requested_date or '-'}")
        append(f"- resolved_date: {summary.resolved_date or '-'}")
        append(f"- date_resolution: {summary.date_resolution}")
        append(f"- triggered_by: {run.triggered_by.value}")
        append(f"- status: {run.status.value}")
        append(f"- started_at: {run.started_at.isoformat()}")
        append(
            f"- finished_at: {run.finished_at.isoformat() if run.finished_at else '-'}"
        )
        append("")
        append("## 总览")
        append("")
        append(summary.overview)
        append("")
        append("## 一、市场大势")
        append("")
        append(f"- 摘要：{market_overview.get('summary', '暂无市场大势结论')}")
        append(f"- 市场状态：{market_overview.get('regime', 'neutral')}")
        append(f"- 置信度：{market_overview.get('confidence', '-')}")
        append("")
        append("### 指数表现")
        append("")
        index_rows = [
            [
                item.get("name", item.get("code", "-")),
                str(item.get("close", "-")),
                str(item.get("pct_change", "-")),
                str(item.get("amount", "-")),
            ]
            for item in market_overview.get("indices", [])
        ]
        append(self._format_table(["指数", "收盘", "涨跌幅", "成交额"], index_rows))
        append("")
        append("### 市场情绪")
        append("")
        append(
            f"- 上涨 / 下跌家数：{breadth.get('up_count', '-')} / {breadth.get('down_count', '-')}"
        )
        append(f"- 涨跌比：{breadth.get('up_down_ratio', '-')}")
        append(f"- 5日涨跌比中枢：{breadth.get('ratio_5d_avg', '-')}")
        append(
            f"- 涨停 / 跌停 / 炸板：{limit_stats.get('limit_up_count', '-')} / {limit_stats.get('limit_down_count', '-')} / {limit_stats.get('broken_board_count', '-')}"
        )
        append(f"- 炸板率：{limit_stats.get('broken_board_rate', '-')}")
        append(f"- 连板高度：{limit_stats.get('max_consecutive_board', '-')}")
        append("")
        append("### 大小盘风格")
        append("")
        append(f"- 风格标签：{style.get('style_label', '-')}")
        append(f"- 沪深300：{style.get('hs300_5d_pct', '-')}")
        append(f"- 中证1000 / 创业板代理：{style.get('zz1000_5d_pct', '-')}")
        append(f"- 相对强度：{style.get('relative_strength_5d', '-')}")
        append("")
        append("### 风险偏好代理")
        append("")
        proxy_rows = [
            [
                item.get("name", item.get("code", "-")),
                str(item.get("pct_change", "-")),
                item.get("note", "-"),
            ]
            for item in market_overview.get("risk_proxies", [])
        ]
        append(self._format_table(["代理", "涨跌幅", "说明"], proxy_rows))
        append("")
        append("### 大势判断")
        append("")
        append("- 关键结论：")
        append(self._format_lines(market_overview.get("key_takeaways", [])))
        append("")
        append("### 跨日验证（昨日判断 vs 今日结果）")
        append("")
        if not cross_day_review.get("available"):
            append(
                f"- 不可用：{cross_day_review.get('reason', 'missing_previous_snapshot')}"
            )
        else:
            regime_cmp = cross_day_review.get("regime", {})
            style_cmp = cross_day_review.get("style", {})
            ratio_cmp = cross_day_review.get("breadth_ratio", {})
            append(
                f"- 对照区间：{cross_day_review.get('previous_date', '-')} -> {run.workflow_date}"
            )
            append(
                f"- 市场状态：{regime_cmp.get('previous', '-')} -> {regime_cmp.get('today', '-')}"
                + ("（切换）" if regime_cmp.get("changed") else "（延续）")
            )
            append(
                f"- 风格状态：{style_cmp.get('previous', '-')} -> {style_cmp.get('today', '-')}"
                + ("（切换）" if style_cmp.get("changed") else "（延续）")
            )
            append(
                f"- 涨跌比：{ratio_cmp.get('previous', '-')} -> {ratio_cmp.get('today', '-')}（Δ {ratio_cmp.get('delta', '-')})"
            )
            append("")
            sector_consistency_rows = [
                [
                    item.get("name", "-"),
                    str(item.get("previous", "-")),
                    str(item.get("today", "-")),
                    str(item.get("delta", "-")),
                ]
                for item in cross_day_review.get("watch_sector_consistency", [])
            ]
            append(
                self._format_table(
                    ["观察板块", "昨日一致性", "今日一致性", "变化"],
                    sector_consistency_rows,
                )
            )
        append("")
        append("## 二、板块定位")
        append("")
        append("### 当日主线")
        append("")
        leader_rows = [
            [
                item.get("sector_name", "-"),
                str(item.get("pct_change", "-")),
                str(item.get("net_flow", "-")),
                item.get("leader_stock", "-") or "-",
            ]
            for item in sector_positioning.get("market_leaders", [])
        ]
        append(self._format_table(["板块", "涨跌幅", "净流入", "领涨股"], leader_rows))
        append("")
        append("### 弱势板块")
        append("")
        laggard_rows = [
            [
                item.get("sector_name", "-"),
                str(item.get("pct_change", "-")),
                str(item.get("net_flow", "-")),
                item.get("leader_stock", "-") or "-",
            ]
            for item in sector_positioning.get("market_laggards", [])
        ]
        append(self._format_table(["板块", "涨跌幅", "净流入", "领涨股"], laggard_rows))
        append("")
        append("### 固定观察池")
        append("")
        watch_sector_rows = [
            [
                item.get("sector_name", "-"),
                item.get("role", "-"),
                item.get("trend_5d", "-"),
                str(item.get("consistency", "-")),
                item.get("leader_stock", "-") or "-",
                item.get("observation_note", "-") or "-",
            ]
            for item in sector_positioning.get("watch_sectors", [])
        ]
        append(
            self._format_table(
                ["板块", "分类", "5日趋势", "一致性", "板块优选股", "观察重点"],
                watch_sector_rows,
            )
        )
        append("")
        append("### 观察重点")
        append("")
        append(self._format_lines(sector_positioning.get("observation_focus", [])))
        append("")
        append("### 行业板块 TOP")
        append("")
        industry_top_rows = [
            [
                item.get("name", "-"),
                str(item.get("change_pct", "-")),
                f"{item.get('up_count', '-')}/{item.get('down_count', '-')}",
                item.get("leader", "-") or "-",
            ]
            for item in sector_positioning.get("industry_top", [])
        ]
        append(
            self._format_table(["行业", "涨跌幅", "涨/跌", "领涨股"], industry_top_rows)
        )
        append("")
        append("### 行业板块 Bottom")
        append("")
        industry_bottom_rows = [
            [
                item.get("name", "-"),
                str(item.get("change_pct", "-")),
                f"{item.get('up_count', '-')}/{item.get('down_count', '-')}",
                item.get("leader", "-") or "-",
            ]
            for item in sector_positioning.get("industry_bottom", [])
        ]
        append(
            self._format_table(
                ["行业", "涨跌幅", "涨/跌", "领涨股"], industry_bottom_rows
            )
        )
        append("")
        append("### 概念板块 TOP")
        append("")
        concept_top_rows = [
            [
                item.get("name", "-"),
                str(item.get("change_pct", "-")),
                f"{item.get('up_count', '-')}/{item.get('down_count', '-')}",
                item.get("leader", "-") or "-",
            ]
            for item in sector_positioning.get("concept_top", [])
        ]
        append(
            self._format_table(["概念", "涨跌幅", "涨/跌", "领涨股"], concept_top_rows)
        )
        append("")
        append("### 概念板块 Bottom")
        append("")
        concept_bottom_rows = [
            [
                item.get("name", "-"),
                str(item.get("change_pct", "-")),
                f"{item.get('up_count', '-')}/{item.get('down_count', '-')}",
                item.get("leader", "-") or "-",
            ]
            for item in sector_positioning.get("concept_bottom", [])
        ]
        append(
            self._format_table(
                ["概念", "涨跌幅", "涨/跌", "领涨股"], concept_bottom_rows
            )
        )
        append("")
        append("## 三、涨停生态")
        append("")
        append(f"- 涨停池：**{limit_stats.get('limit_up_count', 0)}** 家")
        append(
            f"- 炸板：**{limit_stats.get('broken_board_count', 0)}** 家（炸板率 {limit_stats.get('broken_board_rate', 0)}%）"
        )
        append(f"- 跌停：**{limit_stats.get('limit_down_count', 0)}** 家")
        append("")
        top_consecutive = limit_stats.get("top_consecutive", [])
        if top_consecutive:
            append("### 连板梯队")
            append("")
            tiers: dict[int, list[dict]] = {}
            for top in top_consecutive:
                tiers.setdefault(int(top.get("count", 0)), []).append(top)
            for count in sorted(tiers.keys(), reverse=True):
                names = ", ".join(
                    item.get("name", "") for item in tiers[count] if item.get("name")
                )
                append(f"- **{count}连板**（{len(tiers[count])}家）：{names}")
            append("")
        append("## 四、持仓健康度")
        append("")
        append(f"- 总结：{position_health.get('portfolio_health_summary', '暂无')}")
        append("")
        append("### 持仓板块健康度")
        append("")
        sector_health_rows = [
            [
                item.get("sector_name", "-"),
                item.get("role", "-"),
                item.get("trend_5d", "-"),
                str(item.get("consistency", "-")),
                item.get("status", "-"),
                item.get("observation_note", "-") or "-",
            ]
            for item in position_health.get("sector_health", [])
        ]
        append(
            self._format_table(
                ["板块", "角色", "5日趋势", "一致性", "判断", "说明"],
                sector_health_rows,
            )
        )
        append("")
        append("### 关注个股")
        append("")
        tracked_rows = [
            [
                item.get("name", "-"),
                item.get("code", "-"),
                item.get("role", "-"),
                item.get("theme", "-") or "-",
                str(item.get("pct_change", "-")),
                str(item.get("turnover_rate", "-")),
                str(item.get("volume_ratio", "-")),
                item.get("state", "-"),
                self._format_tag_list(item.get("risk_flags", [])),
                item.get("observation_note", "-") or item.get("thesis", "-") or "-",
            ]
            for item in position_health.get("tracked_items", [])
        ]
        append(
            self._format_table(
                [
                    "股票",
                    "代码",
                    "角色",
                    "主题",
                    "涨跌幅",
                    "换手率",
                    "量比",
                    "状态",
                    "风险标记",
                    "观察要点",
                ],
                tracked_rows,
            )
        )
        append("")
        append("## 五、收盘研报归档")
        append("")
        if research_archive.get("available"):
            append(
                f"- 时间窗口：**{research_archive.get('begin_date', '-')} → {research_archive.get('end_date', '-')}**"
            )
            append(f"- 输出根目录：`{research_archive.get('output_root', '-')}`")
            append(
                f"- 归档类别：{self._format_tag_list(research_archive.get('categories', []))}"
            )
            append(
                f"- 关注行业：{self._format_tag_list(research_archive.get('watch_sectors', []))}"
            )
            append(
                f"- 关注个股：{self._format_tag_list(research_archive.get('watch_stocks', []))}"
            )
            append("- 归档说明：")
            append(self._format_lines(research_archive.get("notes", [])))
        else:
            append(f"- 暂不可用：{research_archive.get('reason', 'unavailable')}")
        append("")
        append("## 六、明日准备")
        append("")
        append(f"- 市场偏向：{next_day_prep.get('market_bias', 'observe')}")
        append(
            f"- 重点方向：{self._format_tag_list(next_day_prep.get('focus_sectors', []))}"
        )
        append(
            f"- 重点对象：{self._format_tag_list(next_day_prep.get('focus_items', []))}"
        )
        append("- 风险提示：")
        append(self._format_lines(next_day_prep.get("risk_notes", [])))
        append("")
        append("- 明日检查项：")
        append(self._format_lines(next_day_prep.get("tomorrow_checkpoints", [])))
        append("")
        append("## 七、关注池")
        append("")
        append(
            f"- 关注板块：{self._format_tag_list(watch_context.get('watch_sectors', []))}"
        )
        append(
            f"- 关注股票：{self._format_tag_list([stock.get('name') or stock.get('code', '') for stock in watch_context.get('watch_stocks', [])])}"
        )
        append("")
        append("## 八、预警")
        append("")
        if alerts:
            for item in alerts:
                append(
                    f"- [{item.get('urgency', 'medium')}] {item.get('title', '未命名预警')}"
                    + (f"：{item.get('message')}" if item.get("message") else "")
                )
        else:
            append("- 暂无预警")
        append("")
        append("## 九、执行步骤")
        append("")
        step_rows = [
            [
                step.name,
                step.status,
                str(step.records_affected),
                step.error_message or "-",
            ]
            for step in steps
        ]
        append(self._format_table(["步骤", "状态", "影响记录", "错误"], step_rows))
        append("")
        append("## 数据来源")
        append("")
        append(
            self._format_lines([str(item) for item in metadata.get("data_sources", [])])
        )
        append("")
        return "\n".join(lines)

    def _render_briefing_report(self, run: WorkflowRunRecord) -> str:
        """Render one workflow run into a markdown briefing document."""
        if run.phase == WorkflowPhase.PRE_MARKET:
            return self._render_pre_market_report(run)
        return self._render_post_market_report(run)

    def _export_briefing_report(self, run: WorkflowRunRecord) -> None:
        """Write one workflow briefing markdown document to the repository briefings directory."""
        output_dir = self._briefings_dir()
        output_dir.mkdir(parents=True, exist_ok=True)
        output_path = self._briefing_file_path(run)
        output_path.write_text(self._render_briefing_report(run), encoding="utf-8")

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
                json.dumps(summary.model_dump(mode="json"), ensure_ascii=False),
                error_message,
            ],
        )
        try:
            self._export_briefing_report(run)
        except Exception:
            logger.exception("failed to export workflow briefing markdown")
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

    def _load_latest_insight(
        self, phase: WorkflowPhase, producer: str
    ) -> WorkflowInsightRecord | None:
        conn = get_conn()
        row = conn.execute(
            """
            SELECT id, workflow_run_id, workflow_date, phase, producer, status,
                   schema_version, producer_version, source_run_id, source_context_schema_version,
                   insight_json, error_message, generated_at, created_at, updated_at
            FROM workflow_insights
            WHERE phase = ? AND producer = ?
            ORDER BY workflow_date DESC, generated_at DESC, id DESC
            LIMIT 1
            """,
            [phase.value, producer],
        ).fetchone()
        if row is None:
            return None
        return WorkflowInsightRecord(
            id=row[0],
            workflow_run_id=row[1],
            workflow_date=str(row[2]),
            phase=WorkflowPhase(str(row[3])),
            producer=str(row[4]),
            status=InsightStatus(str(row[5])),
            schema_version=str(row[6]),
            producer_version=str(row[7]),
            source_run_id=row[8],
            source_context_schema_version=str(row[9]),
            insight=(
                WorkflowInsightPayload(**json.loads(row[10]))
                if row[10]
                else WorkflowInsightPayload()
            ),
            error_message=row[11],
            generated_at=row[12],
            created_at=row[13],
            updated_at=row[14],
        )
