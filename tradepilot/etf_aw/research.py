"""Research helpers for ETF all-weather dashboard summaries."""

from __future__ import annotations

from datetime import date
import math

import pandas as pd

from tradepilot.etl.etf_aw_universe import ETF_AW_SLEEVE_CODES
from tradepilot.etl.service import _backtest_metric_values


def fixed_weight_segment_backtests(
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
        "baseline": "equal_weight_fixed",
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
    best = max(candidates, key=_candidate_rank)
    return {
        "method": "shrinkage_plus_focused_6_sleeve_grid_search",
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
    for equity_large in (0.10, 0.15, 0.20):
        for equity_small in (0.10, 0.15, 0.20):
            for equity_overseas in (0.05, 0.10, 0.15, 0.20):
                for bond in (0.20, 0.25, 0.30):
                    for gold in (0.10, 0.15, 0.20):
                        cash = round(
                            1.0
                            - equity_large
                            - equity_small
                            - equity_overseas
                            - bond
                            - gold,
                            10,
                        )
                        weights_list = [
                            equity_large,
                            equity_small,
                            equity_overseas,
                            bond,
                            gold,
                            cash,
                        ]
                        if cash < 0 or cash > 0.35 or max(weights_list) > 0.45:
                            continue
                        weights = dict(
                            zip(ETF_AW_SLEEVE_CODES, weights_list, strict=True)
                        )
                        results = _fixed_weight_results(
                            returns=returns,
                            equal_weight=equal_weight_returns,
                            segments=segments,
                            weights=weights,
                        )
                        summary = _fixed_weight_summary(results)
                        candidate = {
                            "candidate_name": "候选优化",
                            "search_method": "focused_6_sleeve_long_only_grid_caps",
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
    values = returns.astype(float).tolist()
    if not values:
        return {
            "total_return": None,
            "annualized_return": None,
            "annualized_volatility": None,
            "sharpe_ratio": None,
            "max_drawdown": None,
        }
    final_nav = float((1.0 + pd.Series(values, dtype=float)).prod())
    metrics = _backtest_metric_values(values, [], final_nav)
    return {
        "total_return": metrics["total_return"],
        "annualized_return": metrics["annualized_return"],
        "annualized_volatility": metrics["annualized_volatility"],
        "sharpe_ratio": metrics["sharpe_ratio"],
        "max_drawdown": metrics["max_drawdown"],
    }


def _finite_or_none(value: object) -> float | None:
    """Return a finite float or None for research comparisons."""
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
