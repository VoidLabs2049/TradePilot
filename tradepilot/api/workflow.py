"""Workflow API routes for the simplified daily operating loop."""

from __future__ import annotations

from datetime import date
from functools import lru_cache
import math

from fastapi import APIRouter, Query
import pandas as pd

from tradepilot.config import LAKEHOUSE_ROOT
from tradepilot.etf_aw.cli import (
    _backtest_robustness_report,
    update_local_shadow_artifacts,
)
from tradepilot.etf_aw.research import fixed_weight_segment_backtests
from tradepilot.etf_aw.shadow_run import (
    PAPER_FILL_DATASET,
    SHADOW_ACCOUNT_SEED_DATASET,
    SHADOW_OBSERVATION_DATASET,
    ShadowRunError,
    build_performance_report,
    read_shadow_dataset,
)
from tradepilot.etf_aw.rebalance_plan import REBALANCE_PLAN_DATASET
from tradepilot.etl.etf_aw_universe import ETF_AW_SLEEVE_CODES
from tradepilot.etl.models import StorageZone
from tradepilot.etl.read_models import (
    get_latest_etf_aw_risk_budget,
    get_latest_etf_aw_snapshot,
)
from tradepilot.etl.storage import build_zone_path
from tradepilot.workflow.models import (
    EtfAwRiskBudgetResponse,
    WorkflowContextPayload,
    WorkflowInsightResponse,
    WorkflowInsightUpsertRequest,
    WorkflowPhase,
    WorkflowRunResponse,
    WorkflowTrigger,
)
from tradepilot.workflow.service import DailyWorkflowService

router = APIRouter()
_service = DailyWorkflowService()

_ETF_AW_CALENDAR = "etf_aw_v2_monthly_post_20"
_ETF_AW_STRATEGY = "etf_aw_v2"
_ETF_AW_TARGET_VERSION = "target_weight_inverse_vol_v2"
_ETF_AW_RISK_BUDGET_VERSION = "risk_budget_v2"
_ETF_AW_BASELINE = "static_inverse_vol"
_ETF_AW_BASELINE_VERSION = "static_inverse_vol_v2"


@router.get("/latest", response_model=WorkflowRunResponse | None)
def get_latest_workflow(
    phase: WorkflowPhase = Query(..., description="Workflow phase to fetch"),
) -> WorkflowRunResponse | None:
    """Return the latest workflow snapshot for one phase."""
    run = _service.get_latest_run(phase)
    if run is None:
        return None
    return WorkflowRunResponse(run=run)


@router.get("/history")
def get_workflow_history(limit: int = 20) -> list[dict]:
    """Return recent workflow history rows."""
    return [item.model_dump() for item in _service.list_history(limit=limit)]


@router.get("/status")
def get_workflow_status() -> dict:
    """Return the latest status for both workflow phases."""
    return _service.get_workflow_status()


@router.post("/pre/run", response_model=WorkflowRunResponse)
def run_pre_market_workflow(workflow_date: str | None = None) -> WorkflowRunResponse:
    """Trigger a manual pre-market workflow run."""
    run = _service.run_pre_market_workflow(
        workflow_date=workflow_date,
        triggered_by=WorkflowTrigger.MANUAL,
    )
    return WorkflowRunResponse(run=run)


@router.post("/post/run", response_model=WorkflowRunResponse)
def run_post_market_workflow(workflow_date: str | None = None) -> WorkflowRunResponse:
    """Trigger a manual post-market workflow run."""
    run = _service.run_post_market_workflow(
        workflow_date=workflow_date,
        triggered_by=WorkflowTrigger.MANUAL,
    )
    return WorkflowRunResponse(run=run)


@router.get("/context/latest", response_model=WorkflowContextPayload | None)
def get_latest_workflow_context(
    phase: WorkflowPhase = Query(
        ..., description="Workflow phase to fetch context for"
    ),
) -> WorkflowContextPayload | None:
    """Return the latest structured context for one workflow phase."""
    return _service.get_latest_context(phase)


@router.get("/etf-aw/latest")
def get_latest_etf_aw_context(
    as_of_date: date | None = Query(
        None, description="Latest rebalance snapshot date upper bound"
    ),
) -> dict | None:
    """Return the latest ETF all-weather snapshot context."""
    return get_latest_etf_aw_snapshot(
        as_of_date=as_of_date,
        calendar_name=_ETF_AW_CALENDAR,
    )


@router.get(
    "/etf-aw/risk-budget/latest",
    response_model=EtfAwRiskBudgetResponse | None,
)
def get_latest_etf_aw_risk_budget_context(
    as_of_date: date | None = Query(
        None, description="Latest risk budget rebalance date upper bound"
    ),
) -> EtfAwRiskBudgetResponse | None:
    """Return the latest frozen ETF all-weather risk budget."""
    return get_latest_etf_aw_risk_budget(
        as_of_date=as_of_date,
        calendar_name=_ETF_AW_CALENDAR,
        strategy_name=_ETF_AW_STRATEGY,
        strategy_version=_ETF_AW_RISK_BUDGET_VERSION,
    )


@router.get("/etf-aw/research-summary")
def get_etf_aw_research_summary() -> dict:
    """Return current target weights, latest plan, and cost-aware backtest result."""
    return _cached_etf_aw_research_summary(_research_artifact_signature())


_RESEARCH_DATASETS = (
    "derived.etf_aw_target_weight",
    "derived.etf_aw_backtest_kernel",
    "derived.etf_aw_baseline_weight",
    "derived.etf_aw_sleeve_daily",
    "derived.etf_aw_risk_budget",
    "derived.etf_aw_strategy_context",
    REBALANCE_PLAN_DATASET,
)


def _research_artifact_signature() -> tuple[tuple[str, int, int], ...]:
    """Return a cache key that changes when a research parquet artifact changes."""
    files: list[tuple[str, int, int]] = []
    for dataset_name in _RESEARCH_DATASETS:
        root = build_zone_path(dataset_name, StorageZone.DERIVED, LAKEHOUSE_ROOT)
        for path in sorted(root.rglob("*.parquet")) if root.exists() else []:
            stat = path.stat()
            files.append((str(path), stat.st_mtime_ns, stat.st_size))
    return tuple(files)


@lru_cache(maxsize=2)
def _cached_etf_aw_research_summary(
    artifact_signature: tuple[tuple[str, int, int], ...],
) -> dict:
    """Build and cache the expensive research summary for frozen artifacts."""
    del artifact_signature
    target_weight = read_shadow_dataset(LAKEHOUSE_ROOT, "derived.etf_aw_target_weight")
    kernel = read_shadow_dataset(LAKEHOUSE_ROOT, "derived.etf_aw_backtest_kernel")
    baseline_weight = read_shadow_dataset(
        LAKEHOUSE_ROOT, "derived.etf_aw_baseline_weight"
    )
    sleeve_daily = read_shadow_dataset(LAKEHOUSE_ROOT, "derived.etf_aw_sleeve_daily")
    target_weight = _current_target_weight_rows(target_weight)
    kernel = _current_kernel_rows(kernel)
    baseline_weight = _current_baseline_weight_rows(baseline_weight)
    risk_budget = _current_risk_budget_rows(
        read_shadow_dataset(LAKEHOUSE_ROOT, "derived.etf_aw_risk_budget")
    )
    strategy_context = _current_strategy_context_rows(
        read_shadow_dataset(LAKEHOUSE_ROOT, "derived.etf_aw_strategy_context")
    )
    latest_target = _latest_complete_target_weight_rows(target_weight)
    robustness = _local_robustness_summary(
        kernel=kernel,
        target_weight=target_weight,
        baseline_weight=baseline_weight,
        risk_budget=risk_budget,
        strategy_context=strategy_context,
        sleeve_daily=sleeve_daily,
    )
    return _json_safe(
        {
            "target_weight": latest_target,
            "latest_plan": _latest_rebalance_plan_rows(
                read_shadow_dataset(LAKEHOUSE_ROOT, REBALANCE_PLAN_DATASET)
            ),
            "robustness": robustness,
            "fixed_weight_backtest": fixed_weight_segment_backtests(
                target=latest_target, sleeve_daily=sleeve_daily
            ),
        }
    )


@router.get("/etf-aw/shadow-report")
def get_etf_aw_shadow_report(account_id: str | None = None) -> dict:
    """Return the full available Stage O report for one shadow account."""
    seed = read_shadow_dataset(LAKEHOUSE_ROOT, SHADOW_ACCOUNT_SEED_DATASET)
    observations = read_shadow_dataset(LAKEHOUSE_ROOT, SHADOW_OBSERVATION_DATASET)
    accounts = (
        []
        if seed.empty or "account_id" not in seed.columns
        else sorted(seed["account_id"].astype(str).unique().tolist())
    )
    selected = account_id or (accounts[0] if accounts else None)
    if selected is None:
        return {"state": "not_initialized", "accounts": [], "report": None}
    account_observations = (
        observations[observations["account_id"].astype(str).eq(selected)].copy()
        if not observations.empty and "account_id" in observations.columns
        else observations
    )
    if account_observations.empty:
        return {"state": "awaiting_observation", "accounts": accounts, "report": None}
    dates = account_observations["observation_date"].astype(str)
    try:
        report = build_performance_report(
            account_id=selected,
            start=date.fromisoformat(dates.min()[:10]),
            end=date.fromisoformat(dates.max()[:10]),
            seed=seed,
            observations=observations,
            fills=read_shadow_dataset(LAKEHOUSE_ROOT, PAPER_FILL_DATASET),
            plans=read_shadow_dataset(LAKEHOUSE_ROOT, REBALANCE_PLAN_DATASET),
        )
    except ShadowRunError as exc:
        return {
            "state": "invalid",
            "accounts": accounts,
            "report": None,
            "blocking_reasons": exc.reasons,
        }
    return {"state": "ready", "accounts": accounts, "report": report}


@router.post("/etf-aw/shadow/update-local")
def update_etf_aw_local_shadow(
    account_id: str = "etf-aw-v2-paper",
    initial_asset: float = 1_000_000.0,
    weight_source: str = "target-weight",
    seed_date: date | None = None,
    end_date: date | None = None,
) -> dict:
    """Update the research shadow account from local lakehouse artifacts."""
    if initial_asset <= 0 or not math.isfinite(initial_asset):
        return {
            "state": "invalid",
            "blocking_reasons": ["invalid_initial_asset"],
            "diagnostics": {"initial_asset": str(initial_asset)},
        }
    try:
        result = update_local_shadow_artifacts(
            account_id=account_id,
            initial_asset=initial_asset,
            weight_source_type=weight_source,
            seed_date=seed_date,
            end_date=end_date,
            lakehouse_root=LAKEHOUSE_ROOT,
        )
    except ShadowRunError as exc:
        return {
            "state": "invalid",
            "blocking_reasons": exc.reasons,
            "diagnostics": exc.diagnostics,
        }
    return {"state": "updated", **result}


@router.get("/etf-aw/shadow/status")
def get_etf_aw_shadow_status(account_id: str = "etf-aw-v2-paper") -> dict:
    """Return local lakehouse freshness and shadow observation coverage."""
    sleeve_daily = read_shadow_dataset(LAKEHOUSE_ROOT, "derived.etf_aw_sleeve_daily")
    target_weight = read_shadow_dataset(LAKEHOUSE_ROOT, "derived.etf_aw_target_weight")
    target_weight = _current_target_weight_rows(target_weight)
    observations = read_shadow_dataset(LAKEHOUSE_ROOT, SHADOW_OBSERVATION_DATASET)

    price_dates = _complete_etf_aw_price_dates(sleeve_daily)
    latest_sleeve_daily_date = price_dates[-1] if price_dates else None
    latest_target_weight_date = _latest_complete_target_weight_date(target_weight)
    account_observation_dates = _account_observation_dates(observations, account_id)
    latest_observation_date = (
        account_observation_dates[-1] if account_observation_dates else None
    )
    start_after = latest_observation_date or latest_target_weight_date
    missing = [
        value
        for value in price_dates
        if start_after is not None and start_after < value
    ]
    if latest_sleeve_daily_date is None:
        next_action = "本地 ETF 行情尚未写入 lakehouse"
    elif latest_target_weight_date is None:
        next_action = "本地目标权重尚未写入 lakehouse"
    elif not account_observation_dates:
        next_action = "点击更新本地观察以初始化模拟盘观察"
    elif missing:
        next_action = f"点击更新本地观察可补 {len(missing)} 个交易日"
    else:
        next_action = "本地模拟盘观察已更新到最新可用行情日"
    return {
        "account_id": account_id,
        "latest_sleeve_daily_date": (
            None
            if latest_sleeve_daily_date is None
            else latest_sleeve_daily_date.isoformat()
        ),
        "latest_target_weight_date": (
            None
            if latest_target_weight_date is None
            else latest_target_weight_date.isoformat()
        ),
        "latest_shadow_observation_date": (
            None
            if latest_observation_date is None
            else latest_observation_date.isoformat()
        ),
        "missing_observation_dates": [value.isoformat() for value in missing],
        "latest_prices": _latest_etf_aw_prices(sleeve_daily, latest_sleeve_daily_date),
        "is_stale": bool(missing),
        "next_action": next_action,
    }


@router.get("/etf-aw/performance")
def get_etf_aw_local_performance() -> dict | None:
    """Return strategy and baseline performance from the local backtest artifact."""
    frame = read_shadow_dataset(LAKEHOUSE_ROOT, "derived.etf_aw_backtest_kernel")
    frame = _current_kernel_rows(frame)
    if frame.empty:
        return None
    daily = frame[frame["observation_type"].astype(str).eq("daily_nav")].copy()
    daily = daily.sort_values(["strategy_name", "observation_date"])
    if daily.empty:
        return None
    start_values = daily.groupby("strategy_name")["net_value"].transform("first")
    daily["period_return"] = daily["net_value"].astype(float) / start_values - 1.0
    series = [
        {
            "date": str(row["observation_date"])[:10],
            "strategy": str(row["strategy_name"]),
            "strategy_version": str(row["strategy_version"]),
            "net_value": float(row["net_value"]),
            "period_return": float(row["period_return"]),
            "daily_return": float(row["portfolio_return"]),
        }
        for _, row in daily.iterrows()
    ]
    metrics = frame[frame["observation_type"].astype(str).eq("metric")]
    metrics = metrics.sort_values("ingested_at").drop_duplicates(
        ["strategy_name", "metric_name"], keep="last"
    )
    return {
        "source_dataset": "derived.etf_aw_backtest_kernel",
        "start_date": min(item["date"] for item in series),
        "end_date": max(item["date"] for item in series),
        "observation_count": int(daily["observation_date"].nunique()),
        "series": series,
        "metrics": [
            {
                "strategy": str(row["strategy_name"]),
                "metric": str(row["metric_name"]),
                "value": float(row["metric_value"]),
            }
            for _, row in metrics.iterrows()
        ],
    }


def _complete_etf_aw_price_dates(frame: pd.DataFrame) -> list[date]:
    """Return local price dates with all frozen ETF sleeves present."""
    if frame.empty or "trade_date" not in frame.columns:
        return []
    rows = frame.copy()
    rows["trade_date"] = pd.to_datetime(rows["trade_date"], errors="coerce").dt.date
    rows = rows[
        rows["trade_date"].notna()
        & rows["sleeve_code"].astype(str).isin(ETF_AW_SLEEVE_CODES)
        & pd.to_numeric(rows["close"], errors="coerce").gt(0)
    ]
    return [
        value
        for value in sorted(rows["trade_date"].dropna().unique())
        if set(rows.loc[rows["trade_date"].eq(value), "sleeve_code"].astype(str))
        == set(ETF_AW_SLEEVE_CODES)
    ]


def _latest_complete_target_weight_date(frame: pd.DataFrame) -> date | None:
    """Return the latest complete target-weight date from local lakehouse."""
    if frame.empty or "rebalance_date" not in frame.columns:
        return None
    rows = frame.copy()
    rows["rebalance_date"] = pd.to_datetime(
        rows["rebalance_date"], errors="coerce"
    ).dt.date
    rows = rows[
        rows["rebalance_date"].notna()
        & rows["sleeve_code"].astype(str).isin(ETF_AW_SLEEVE_CODES)
        & rows["target_weight_status"].astype(str).eq("complete")
    ]
    complete_dates = [
        value
        for value in sorted(rows["rebalance_date"].dropna().unique())
        if set(rows.loc[rows["rebalance_date"].eq(value), "sleeve_code"].astype(str))
        == set(ETF_AW_SLEEVE_CODES)
    ]
    return complete_dates[-1] if complete_dates else None


def _latest_etf_aw_prices(frame: pd.DataFrame, trade_date: date | None) -> list[dict]:
    """Return latest local close prices for every frozen ETF sleeve."""
    if frame.empty or trade_date is None:
        return []
    rows = frame.copy()
    rows["trade_date"] = pd.to_datetime(rows["trade_date"], errors="coerce").dt.date
    rows = rows[rows["trade_date"].eq(trade_date)].copy()
    rows = rows[rows["sleeve_code"].astype(str).isin(ETF_AW_SLEEVE_CODES)]
    return [
        {
            "sleeve_code": str(row["sleeve_code"]),
            "sleeve_role": str(row["sleeve_role"]),
            "close": float(row["close"]),
            "trade_date": trade_date.isoformat(),
        }
        for _, row in rows.sort_values("sleeve_code").iterrows()
    ]


def _account_observation_dates(frame: pd.DataFrame, account_id: str) -> list[date]:
    """Return sorted shadow observation dates for one account."""
    if frame.empty or "account_id" not in frame.columns:
        return []
    rows = frame[frame["account_id"].astype(str).eq(account_id)].copy()
    if rows.empty:
        return []
    return sorted(
        pd.to_datetime(rows["observation_date"], errors="coerce")
        .dt.date.dropna()
        .unique()
    )


def _current_target_weight_rows(frame: pd.DataFrame) -> pd.DataFrame:
    """Return current V2 target-weight rows."""

    return _current_strategy_rows(
        frame,
        strategy_name=_ETF_AW_STRATEGY,
        strategy_version=_ETF_AW_TARGET_VERSION,
    )


def _current_risk_budget_rows(frame: pd.DataFrame) -> pd.DataFrame:
    """Return current V2 risk-budget rows."""

    return _current_strategy_rows(
        frame,
        strategy_name=_ETF_AW_STRATEGY,
        strategy_version=_ETF_AW_RISK_BUDGET_VERSION,
    )


def _current_strategy_context_rows(frame: pd.DataFrame) -> pd.DataFrame:
    """Return current V2 strategy-context rows."""

    return _current_strategy_rows(
        frame,
        strategy_name=_ETF_AW_STRATEGY,
        strategy_version="stage_g_v2",
    )


def _current_kernel_rows(frame: pd.DataFrame) -> pd.DataFrame:
    """Return current V2 target and baseline kernel rows."""

    if frame.empty:
        return frame
    required = {"calendar_name", "strategy_name", "strategy_version"}
    if not required.issubset(frame.columns):
        return frame.iloc[0:0].copy()
    current_identity = (
        frame["strategy_name"].astype(str).eq(_ETF_AW_STRATEGY)
        & frame["strategy_version"].astype(str).eq(_ETF_AW_TARGET_VERSION)
    ) | (
        frame["strategy_name"].astype(str).eq(_ETF_AW_BASELINE)
        & frame["strategy_version"].astype(str).eq(_ETF_AW_BASELINE_VERSION)
    )
    return frame[
        frame["calendar_name"].astype(str).eq(_ETF_AW_CALENDAR) & current_identity
    ].copy()


def _current_baseline_weight_rows(frame: pd.DataFrame) -> pd.DataFrame:
    """Return current V2 baseline-weight rows."""

    if frame.empty:
        return frame
    required = {"calendar_name", "baseline_name", "baseline_version"}
    if not required.issubset(frame.columns):
        return frame.iloc[0:0].copy()
    return frame[
        frame["calendar_name"].astype(str).eq(_ETF_AW_CALENDAR)
        & frame["baseline_name"].astype(str).eq(_ETF_AW_BASELINE)
        & frame["baseline_version"].astype(str).eq(_ETF_AW_BASELINE_VERSION)
    ].copy()


def _current_strategy_rows(
    frame: pd.DataFrame,
    *,
    strategy_name: str,
    strategy_version: str,
) -> pd.DataFrame:
    """Return current-calendar rows for one strategy identity."""

    if frame.empty:
        return frame
    required = {"calendar_name", "strategy_name", "strategy_version"}
    if not required.issubset(frame.columns):
        return frame.iloc[0:0].copy()
    return frame[
        frame["calendar_name"].astype(str).eq(_ETF_AW_CALENDAR)
        & frame["strategy_name"].astype(str).eq(strategy_name)
        & frame["strategy_version"].astype(str).eq(strategy_version)
    ].copy()


def _latest_complete_target_weight_rows(frame: pd.DataFrame) -> dict:
    """Return the latest complete current-universe target-weight vector."""
    if frame.empty or "rebalance_date" not in frame.columns:
        return {"rebalance_date": None, "rows": [], "status_counts": {}}
    rows = frame.copy()
    rows["rebalance_date"] = pd.to_datetime(
        rows["rebalance_date"], errors="coerce"
    ).dt.date
    status_counts = (
        rows["target_weight_status"].astype(str).value_counts().sort_index().to_dict()
        if "target_weight_status" in rows.columns
        else {}
    )
    complete = rows[
        rows["rebalance_date"].notna()
        & rows["sleeve_code"].astype(str).isin(ETF_AW_SLEEVE_CODES)
        & rows["target_weight_status"].astype(str).eq("complete")
    ].copy()
    dates = [
        value
        for value in sorted(complete["rebalance_date"].dropna().unique())
        if set(
            complete.loc[complete["rebalance_date"].eq(value), "sleeve_code"].astype(
                str
            )
        )
        == set(ETF_AW_SLEEVE_CODES)
    ]
    if not dates:
        return {
            "rebalance_date": None,
            "rows": [],
            "status_counts": {
                str(key): int(value) for key, value in status_counts.items()
            },
        }
    latest_date = dates[-1]
    latest = complete[complete["rebalance_date"].eq(latest_date)].copy()
    latest["_role_order"] = latest["sleeve_role"].map(
        {
            "equity_large": 0,
            "equity_small": 1,
            "equity_overseas": 2,
            "bond": 3,
            "gold": 4,
            "cash": 5,
        }
    )
    latest = latest.sort_values(["_role_order", "sleeve_code"])
    return {
        "rebalance_date": latest_date.isoformat(),
        "status_counts": {str(key): int(value) for key, value in status_counts.items()},
        "rows": [
            {
                "sleeve_code": str(row["sleeve_code"]),
                "sleeve_role": str(row["sleeve_role"]),
                "target_weight": _finite_or_none(row.get("target_weight")),
                "target_weight_status": str(row.get("target_weight_status", "")),
                "turnover_estimate": _finite_or_none(row.get("turnover_estimate")),
            }
            for _, row in latest.iterrows()
        ],
    }


def _latest_rebalance_plan_rows(frame: pd.DataFrame) -> dict | None:
    """Return the latest locally generated ETF AW rebalance plan."""
    if frame.empty or "plan_id" not in frame.columns:
        return None
    rows = frame.copy()
    for column in ("plan_date", "generated_at"):
        if column in rows.columns:
            rows[column] = pd.to_datetime(rows[column], errors="coerce")
    sort_columns = [
        column for column in ("plan_date", "generated_at") if column in rows.columns
    ]
    rows = rows.sort_values(sort_columns or ["plan_id"])
    plan_id = str(rows.iloc[-1]["plan_id"])
    latest = rows[rows["plan_id"].astype(str).eq(plan_id)].copy()
    return {
        "plan_id": plan_id,
        "plan_date": str(latest.iloc[0].get("plan_date", ""))[:10],
        "plan_status": str(latest.iloc[0].get("plan_status", "")),
        "account_id": str(latest.iloc[0].get("account_id", "")),
        "estimated_buy_notional": _finite_or_none(
            latest.loc[latest["order_side"].astype(str).eq("BUY"), "estimated_notional"]
            .astype(float)
            .sum()
        ),
        "estimated_sell_notional": _finite_or_none(
            latest.loc[
                latest["order_side"].astype(str).eq("SELL"), "estimated_notional"
            ]
            .astype(float)
            .sum()
        ),
        "rows": [
            {
                "sleeve_code": str(row["symbol"]),
                "sleeve_role": str(row["sleeve_role"]),
                "target_weight": _finite_or_none(row.get("target_weight")),
                "latest_price": _finite_or_none(row.get("latest_price")),
                "order_side": str(row.get("order_side", "")),
                "order_quantity": int(row.get("order_quantity") or 0),
                "estimated_notional": _finite_or_none(row.get("estimated_notional")),
                "target_notional": _finite_or_none(row.get("target_notional")),
            }
            for _, row in latest.sort_values("sleeve_role").iterrows()
        ],
    }


def _local_robustness_summary(
    *,
    kernel: pd.DataFrame,
    target_weight: pd.DataFrame,
    baseline_weight: pd.DataFrame,
    risk_budget: pd.DataFrame,
    strategy_context: pd.DataFrame,
    sleeve_daily: pd.DataFrame,
) -> dict | None:
    """Return a compact pass/fail summary from local backtest artifacts."""
    if kernel.empty:
        return None
    dates = _daily_nav_dates(kernel)
    if not dates:
        return None
    report = _backtest_robustness_report(
        inputs={
            "kernel": kernel,
            "target_weight": target_weight,
            "baseline_weight": baseline_weight,
            "risk_budget": risk_budget,
            "strategy_context": strategy_context,
            "sleeve_daily": sleeve_daily,
        },
        strategy_name="etf_aw_v2",
        strategy_version="target_weight_inverse_vol_v2",
        baseline_name="static_inverse_vol",
        baseline_version="static_inverse_vol_v2",
        start=dates[0],
        end=dates[-1],
    )
    cost_10bps = next(
        (
            item
            for item in report.get("comparisons", [])
            if item.get("cost_scenario") == "cost_10bps"
        ),
        {},
    )
    diff = _finite_or_none(cost_10bps.get("net_total_return_diff"))
    verdict = "blocked"
    if report.get("report_status") == "complete":
        verdict = "pass" if diff is not None and diff > 0 else "fail"
    return {
        "verdict": verdict,
        "decision_rule": "cost_10bps net_total_return_diff > 0",
        "report_status": report.get("report_status"),
        "comparable_range": report.get("comparable_range"),
        "coverage": report.get("coverage"),
        "strategies": report.get("strategies"),
        "comparisons": report.get("comparisons"),
        "diagnostics": report.get("diagnostics"),
    }


def _daily_nav_dates(frame: pd.DataFrame) -> list[date]:
    """Return sorted dates that have daily NAV observations in local kernel."""
    if frame.empty or "observation_date" not in frame.columns:
        return []
    rows = frame[frame["observation_type"].astype(str).eq("daily_nav")].copy()
    return sorted(
        pd.to_datetime(rows["observation_date"], errors="coerce")
        .dt.date.dropna()
        .unique()
    )


def _finite_or_none(value: object) -> float | None:
    """Return a finite float or None for API JSON safety."""
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _json_safe(value: object) -> object:
    """Convert pandas/numpy/date values into strict JSON-compatible values."""
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    if isinstance(value, tuple):
        return [_json_safe(item) for item in value]
    if isinstance(value, date):
        return value.isoformat()
    if pd.isna(value):
        return None
    if hasattr(value, "item"):
        return _json_safe(value.item())
    return value


@router.get("/insight/latest", response_model=WorkflowInsightResponse)
def get_latest_workflow_insight(
    phase: WorkflowPhase = Query(
        ..., description="Workflow phase to fetch insight for"
    ),
    producer: str = Query("the_one", description="Insight producer identifier"),
) -> WorkflowInsightResponse:
    """Return the latest insight state for one workflow phase."""
    return _service.get_latest_insight(phase=phase, producer=producer)


@router.put("/insight", response_model=WorkflowInsightResponse)
def upsert_workflow_insight(
    payload: WorkflowInsightUpsertRequest,
) -> WorkflowInsightResponse:
    """Create or replace the latest workflow insight for one phase."""
    _service.upsert_insight(payload)
    return _service.get_latest_insight(phase=payload.phase, producer=payload.producer)
