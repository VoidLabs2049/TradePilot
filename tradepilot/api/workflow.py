"""Workflow API routes for the simplified daily operating loop."""

from __future__ import annotations

from datetime import date
import math

from fastapi import APIRouter, Query
import pandas as pd

from tradepilot.config import LAKEHOUSE_ROOT
from tradepilot.etf_aw.cli import (
    _backtest_robustness_report,
    update_local_shadow_artifacts,
)
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
from tradepilot.etl.read_models import get_latest_etf_aw_risk_budget
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
    return _service.get_latest_etf_aw_context(as_of_date=as_of_date)


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
    return get_latest_etf_aw_risk_budget(as_of_date=as_of_date)


@router.get("/etf-aw/research-summary")
def get_etf_aw_research_summary() -> dict:
    """Return current target weights, latest plan, and cost-aware backtest result."""
    target_weight = read_shadow_dataset(LAKEHOUSE_ROOT, "derived.etf_aw_target_weight")
    kernel = read_shadow_dataset(LAKEHOUSE_ROOT, "derived.etf_aw_backtest_kernel")
    baseline_weight = read_shadow_dataset(
        LAKEHOUSE_ROOT, "derived.etf_aw_baseline_weight"
    )
    latest_target = _latest_complete_target_weight_rows(target_weight)
    robustness = _local_robustness_summary(
        kernel=kernel,
        target_weight=target_weight,
        baseline_weight=baseline_weight,
        risk_budget=read_shadow_dataset(LAKEHOUSE_ROOT, "derived.etf_aw_risk_budget"),
        strategy_context=read_shadow_dataset(
            LAKEHOUSE_ROOT, "derived.etf_aw_strategy_context"
        ),
        sleeve_daily=read_shadow_dataset(LAKEHOUSE_ROOT, "derived.etf_aw_sleeve_daily"),
    )
    return _json_safe(
        {
            "target_weight": latest_target,
            "latest_plan": _latest_rebalance_plan_rows(
                read_shadow_dataset(LAKEHOUSE_ROOT, REBALANCE_PLAN_DATASET)
            ),
            "robustness": robustness,
            "fixed_weight_backtest": _fixed_weight_segment_backtests(
                target=latest_target,
                sleeve_daily=read_shadow_dataset(
                    LAKEHOUSE_ROOT, "derived.etf_aw_sleeve_daily"
                ),
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
    account_id: str = "etf-aw-paper",
    initial_asset: float = 1_000_000.0,
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
def get_etf_aw_shadow_status(account_id: str = "etf-aw-paper") -> dict:
    """Return local lakehouse freshness and shadow observation coverage."""
    sleeve_daily = read_shadow_dataset(LAKEHOUSE_ROOT, "derived.etf_aw_sleeve_daily")
    target_weight = read_shadow_dataset(LAKEHOUSE_ROOT, "derived.etf_aw_target_weight")
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


def _latest_complete_target_weight_rows(frame: pd.DataFrame) -> dict:
    """Return the latest complete five-sleeve target-weight vector."""
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
            "bond": 2,
            "gold": 3,
            "cash": 4,
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
        strategy_name="etf_aw_v1",
        strategy_version="target_weight_inverse_vol_v1",
        baseline_name="static_inverse_vol",
        baseline_version="static_inverse_vol_v1",
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


def _fixed_weight_segment_backtests(
    *, target: dict, sleeve_daily: pd.DataFrame
) -> dict | None:
    """Backtest the latest fixed target-weight vector over multiple periods."""
    rows = target.get("rows", [])
    if not rows or sleeve_daily.empty:
        return None
    weights = {
        str(row["sleeve_code"]): float(row["target_weight"])
        for row in rows
        if row.get("target_weight") is not None
    }
    if set(weights) != set(ETF_AW_SLEEVE_CODES):
        return None
    panel = sleeve_daily.copy()
    panel["trade_date"] = pd.to_datetime(panel["trade_date"], errors="coerce").dt.date
    panel = panel[
        panel["trade_date"].notna()
        & panel["sleeve_code"].astype(str).isin(ETF_AW_SLEEVE_CODES)
    ].copy()
    panel["daily_return"] = (
        pd.to_numeric(panel["adj_pct_chg"], errors="coerce").fillna(0.0) / 100.0
    )
    returns = (
        panel.pivot_table(
            index="trade_date",
            columns="sleeve_code",
            values="daily_return",
            aggfunc="last",
        )
        .sort_index()
        .dropna()
    )
    if returns.empty:
        return None
    equal_weight = returns.apply(
        lambda row: sum(float(row[code]) for code in ETF_AW_SLEEVE_CODES)
        / len(ETF_AW_SLEEVE_CODES),
        axis=1,
    )
    segments = _backtest_segments(list(returns.index))
    results = _fixed_weight_results(returns, equal_weight, segments, weights)
    summary = _fixed_weight_summary(results)
    return {
        "weight_rebalance_date": target.get("rebalance_date"),
        "weight_basis": "latest complete target weights applied as fixed weights",
        "baseline": "equal_weight_fixed_20pct_each",
        "segments": results,
        "summary": summary,
        "optimization": _weight_shrinkage_optimization(
            returns=returns,
            equal_weight_returns=equal_weight,
            segments=segments,
            current_weights=weights,
        ),
    }


def _fixed_weight_results(
    returns: pd.DataFrame,
    equal_weight: pd.Series,
    segments: list[dict],
    weights: dict[str, float],
) -> list[dict]:
    """Run the same segment tests for one fixed weight vector."""
    strategy = pd.Series(
        returns.loc[:, ETF_AW_SLEEVE_CODES].to_numpy()
        @ [weights[code] for code in ETF_AW_SLEEVE_CODES],
        index=returns.index,
    )
    results = []
    for segment in segments:
        segment_strategy = strategy.loc[segment["start_date"] : segment["end_date"]]
        segment_equal = equal_weight.loc[segment["start_date"] : segment["end_date"]]
        if len(segment_strategy) < 20:
            continue
        strategy_metrics = _return_metrics(segment_strategy)
        equal_metrics = _return_metrics(segment_equal)
        results.append(
            {
                **segment,
                "observation_count": int(len(segment_strategy)),
                "strategy": strategy_metrics,
                "equal_weight_baseline": equal_metrics,
                "comparison": {
                    "total_return_diff": _value_diff(
                        strategy_metrics["total_return"],
                        equal_metrics["total_return"],
                    ),
                    "annualized_return_diff": _value_diff(
                        strategy_metrics["annualized_return"],
                        equal_metrics["annualized_return"],
                    ),
                    "sharpe_ratio_diff": _value_diff(
                        strategy_metrics["sharpe_ratio"],
                        equal_metrics["sharpe_ratio"],
                    ),
                    "max_drawdown_diff": _value_diff(
                        strategy_metrics["max_drawdown"],
                        equal_metrics["max_drawdown"],
                    ),
                },
                "profitable": (
                    strategy_metrics["total_return"] is not None
                    and strategy_metrics["total_return"] > 0
                ),
                "beats_equal_weight": (
                    strategy_metrics["total_return"] is not None
                    and equal_metrics["total_return"] is not None
                    and strategy_metrics["total_return"]
                    > equal_metrics["total_return"] + 1e-12
                ),
            }
        )
    return results


def _fixed_weight_summary(results: list[dict]) -> dict:
    """Summarize segmented fixed-weight backtest results."""
    positive_segments = sum(1 for item in results if item["profitable"])
    beat_segments = sum(1 for item in results if item["beats_equal_weight"])
    diffs = [
        float(item["comparison"]["total_return_diff"])
        for item in results
        if item["comparison"]["total_return_diff"] is not None
    ]
    drawdowns = [
        float(item["strategy"]["max_drawdown"])
        for item in results
        if item["strategy"]["max_drawdown"] is not None
    ]
    return {
        "segment_count": len(results),
        "profitable_segments": positive_segments,
        "beat_equal_weight_segments": beat_segments,
        "profitable_ratio": positive_segments / len(results) if results else None,
        "beat_equal_weight_ratio": beat_segments / len(results) if results else None,
        "average_total_return_diff": sum(diffs) / len(diffs) if diffs else None,
        "worst_max_drawdown": min(drawdowns) if drawdowns else None,
    }


def _weight_shrinkage_optimization(
    *,
    returns: pd.DataFrame,
    equal_weight_returns: pd.Series,
    segments: list[dict],
    current_weights: dict[str, float],
) -> dict:
    """Evaluate simple explainable candidate weight vectors."""
    equal = {code: 1.0 / len(ETF_AW_SLEEVE_CODES) for code in ETF_AW_SLEEVE_CODES}
    candidates = []
    for shrinkage in (0.0, 0.25, 0.5, 0.75, 1.0):
        weights = {
            code: (1.0 - shrinkage) * current_weights[code] + shrinkage * equal[code]
            for code in ETF_AW_SLEEVE_CODES
        }
        results = _fixed_weight_results(
            returns=returns,
            equal_weight=equal_weight_returns,
            segments=segments,
            weights=weights,
        )
        summary = _fixed_weight_summary(results)
        candidates.append(
            {
                "candidate_name": (
                    "当前权重"
                    if shrinkage == 0
                    else (
                        "等权"
                        if shrinkage == 1
                        else f"向等权收缩{int(shrinkage * 100)}%"
                    )
                ),
                "shrinkage_to_equal_weight": shrinkage,
                "weights": weights,
                "summary": summary,
                "score": _candidate_score(summary),
            }
        )
    candidates.extend(
        _grid_weight_candidates(
            returns=returns,
            equal_weight_returns=equal_weight_returns,
            segments=segments,
        )
    )
    best = max(
        candidates,
        key=lambda item: (
            item["summary"]["beat_equal_weight_segments"],
            item["summary"]["profitable_segments"],
            item["summary"]["average_total_return_diff"] or -999.0,
            item["summary"]["worst_max_drawdown"] or -999.0,
        ),
    )
    return {
        "method": "shrinkage_plus_focused_5pct_grid_search",
        "objective": "优先提高跑赢等权的分段数，其次盈利分段数、平均相对收益和最差回撤",
        "best_candidate_name": best["candidate_name"],
        "candidates": candidates,
    }


def _grid_weight_candidates(
    *,
    returns: pd.DataFrame,
    equal_weight_returns: pd.Series,
    segments: list[dict],
) -> list[dict]:
    """Return the best focused-grid candidate under simple long-only caps."""
    best: dict | None = None
    for first in (0.10, 0.15, 0.20, 0.25):
        for second in (0.20, 0.25, 0.30, 0.35, 0.40, 0.45):
            for third in (0.20, 0.25, 0.30, 0.35, 0.40):
                for fourth in (0.15, 0.20, 0.25):
                    fifth = 1.0 - first - second - third - fourth
                    if fifth < 0 or fifth > 0.35:
                        continue
                    weights_list = [first, second, third, fourth, fifth]
                    if max(weights_list) > 0.45 or weights_list[4] > 0.35:
                        continue
                    weights = dict(zip(ETF_AW_SLEEVE_CODES, weights_list, strict=True))
                    results = _fixed_weight_results(
                        returns=returns,
                        equal_weight=equal_weight_returns,
                        segments=segments,
                        weights=weights,
                    )
                    summary = _fixed_weight_summary(results)
                    candidate = {
                        "candidate_name": "候选优化",
                        "search_method": "focused_5pct_long_only_grid_caps",
                        "weights": weights,
                        "summary": summary,
                        "score": _candidate_score(summary),
                    }
                    if best is None or _candidate_rank(candidate) > _candidate_rank(
                        best
                    ):
                        best = candidate
    return [] if best is None else [best]


def _candidate_rank(candidate: dict) -> tuple:
    """Return the deterministic optimization rank for one candidate."""
    summary = candidate["summary"]
    return (
        summary["beat_equal_weight_segments"],
        summary["profitable_segments"],
        summary["average_total_return_diff"] or -999.0,
        summary["worst_max_drawdown"] or -999.0,
    )


def _candidate_score(summary: dict) -> float:
    """Return a simple display score for ranking candidate weight vectors."""
    return (
        float(summary.get("beat_equal_weight_segments") or 0) * 100.0
        + float(summary.get("profitable_segments") or 0) * 10.0
        + float(summary.get("average_total_return_diff") or 0.0) * 100.0
        + float(summary.get("worst_max_drawdown") or 0.0) * 10.0
    )


def _backtest_segments(dates: list[date]) -> list[dict]:
    """Build full, recency, yearly, and split segments for fixed-weight tests."""
    start = min(dates)
    end = max(dates)
    split = dates[int(len(dates) * 0.7)]
    segments = [
        {
            "segment_name": "全可比区间",
            "segment_type": "full",
            "start_date": start,
            "end_date": end,
        },
        {
            "segment_name": "样本内前70%",
            "segment_type": "in_sample",
            "start_date": start,
            "end_date": split,
        },
        {
            "segment_name": "样本外后30%",
            "segment_type": "out_of_sample",
            "start_date": split,
            "end_date": end,
        },
    ]
    if len(dates) >= 126:
        segments.append(
            {
                "segment_name": "近6个月",
                "segment_type": "recent_6m",
                "start_date": dates[-126],
                "end_date": end,
            }
        )
    if len(dates) >= 252:
        segments.append(
            {
                "segment_name": "近12个月",
                "segment_type": "recent_12m",
                "start_date": dates[-252],
                "end_date": end,
            }
        )
    for year in sorted({value.year for value in dates}):
        year_dates = [value for value in dates if value.year == year]
        if len(year_dates) >= 60:
            segments.append(
                {
                    "segment_name": f"{year}年",
                    "segment_type": "calendar_year",
                    "start_date": min(year_dates),
                    "end_date": max(year_dates),
                }
            )
    return segments


def _return_metrics(returns: pd.Series) -> dict[str, float | None]:
    """Calculate simple long-only portfolio metrics from daily returns."""
    if returns.empty:
        return {
            "total_return": None,
            "annualized_return": None,
            "annualized_volatility": None,
            "sharpe_ratio": None,
            "max_drawdown": None,
        }
    series = returns.astype(float)
    nav = (1.0 + series).cumprod()
    final_nav = float(nav.iloc[-1])
    annualized_return = final_nav ** (252.0 / len(series)) - 1.0
    annualized_volatility = float(series.std(ddof=1) * math.sqrt(252))
    sharpe_ratio = (
        float(annualized_return / annualized_volatility)
        if annualized_volatility > 0
        else None
    )
    drawdown = nav / nav.cummax() - 1.0
    return {
        "total_return": float(final_nav - 1.0),
        "annualized_return": float(annualized_return),
        "annualized_volatility": annualized_volatility,
        "sharpe_ratio": sharpe_ratio,
        "max_drawdown": float(drawdown.min()),
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


def _value_diff(left: object, right: object) -> float | None:
    """Return numeric left-minus-right when both values are finite."""
    left_value = _finite_or_none(left)
    right_value = _finite_or_none(right)
    if left_value is None or right_value is None:
        return None
    return left_value - right_value


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
