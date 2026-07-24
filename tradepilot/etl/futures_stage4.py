"""Commodity futures stage 4 basket rule freeze and risk report."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime
from enum import StrEnum
import hashlib
import json
import math
import subprocess
from pathlib import Path

import click
import pandas as pd

from tradepilot.config import LAKEHOUSE_ROOT
from tradepilot.etl.futures_stage23_batch import NON_GOLD_FUTURES_ROOT_CODES
from tradepilot.etl.models import StorageZone
from tradepilot.etl.storage import (
    ParquetWriteResult,
    build_zone_path,
    write_dataset_parquet,
)

_CONTINUOUS_DATASET = "derived.futures_continuous_contract"
_OUTPUT_DATASET = "derived.futures_commodity_basket"
_DEFAULT_DOCS_OUTPUT = Path(
    "docs/futures-v2-design/reports/stage-4/"
    "commodity-futures-stage-4-basket-rule-freeze-report.md"
)
_DEFAULT_CONTROL_ROOT = "AU.SHF"
_DEFAULT_REBALANCE_FREQUENCY = "month_end"
_DEFAULT_VOL_WINDOW = 252
_DEFAULT_MIN_VOL_OBSERVATIONS = 126
_DEFAULT_WEIGHT_CAP = 0.25
_TRADING_DAYS_PER_YEAR = 252
_SECTORS = {
    "AL.SHF": "metals",
    "CU.SHF": "metals",
    "RB.SHF": "ferrous",
    "I.DCE": "ferrous",
    "M.DCE": "agri",
    "P.DCE": "agri",
    "SC.INE": "energy",
    "TA.ZCE": "energy",
    "AU.SHF": "gold_control",
}


class BasketRule(StrEnum):
    """Stage 4 basket rule identifiers."""

    EQUAL_WEIGHT = "equal_weight"
    EQUAL_RISK = "equal_risk"


@dataclass(frozen=True)
class Stage4BasketReport:
    """Structured data for the Stage 4 basket rule-freeze report."""

    generated_at: datetime
    code_version: str | None
    snapshot_id: str
    lakehouse_root: Path
    output_path: Path
    root_codes: list[str]
    control_root_code: str
    start_date: date
    end_date: date
    row_count: int
    rebalance_frequency: str
    volatility_window: int
    min_vol_observations: int
    weight_cap: float
    missing_data_rule: str
    equal_risk_initial_equal_weight_days: int
    first_equal_risk_rebalance_date: date | None
    quality_decisions: dict[str, str]
    basket_metrics: list[dict[str, object]]
    latest_weights: list[dict[str, object]]
    latest_risk_contributions: list[dict[str, object]]
    sector_risk_contributions: list[dict[str, object]]
    pair_correlations: list[dict[str, object]]
    leave_one_out_metrics: list[dict[str, object]]
    leave_sector_out_metrics: list[dict[str, object]]
    au_sensitivity_metrics: list[dict[str, object]]


@click.command()
@click.option(
    "--lakehouse-root",
    type=click.Path(path_type=Path, file_okay=False),
    default=LAKEHOUSE_ROOT,
    show_default=True,
)
@click.option(
    "--root-codes",
    default=",".join(NON_GOLD_FUTURES_ROOT_CODES),
    show_default=True,
    help="Comma-separated non-gold futures root codes.",
)
@click.option("--control-root-code", default=_DEFAULT_CONTROL_ROOT, show_default=True)
@click.option(
    "--volatility-window", type=int, default=_DEFAULT_VOL_WINDOW, show_default=True
)
@click.option(
    "--min-vol-observations",
    type=int,
    default=_DEFAULT_MIN_VOL_OBSERVATIONS,
    show_default=True,
)
@click.option(
    "--weight-cap", type=float, default=_DEFAULT_WEIGHT_CAP, show_default=True
)
@click.option(
    "--output",
    type=click.Path(path_type=Path, dir_okay=False),
    default=_DEFAULT_DOCS_OUTPUT,
    show_default=True,
)
def main(
    lakehouse_root: Path,
    root_codes: str,
    control_root_code: str,
    volatility_window: int,
    min_vol_observations: int,
    weight_cap: float,
    output: Path,
) -> None:
    """Build Stage 4 basket artifacts and write the rule-freeze report."""

    roots = _parse_root_codes(root_codes)
    frame = build_basket_frame(
        lakehouse_root=lakehouse_root,
        root_codes=roots,
        volatility_window=volatility_window,
        min_vol_observations=min_vol_observations,
        weight_cap=weight_cap,
    )
    write_result = write_basket_frame(frame=frame, lakehouse_root=lakehouse_root)
    report = build_stage4_report(
        frame=frame,
        lakehouse_root=lakehouse_root,
        root_codes=roots,
        control_root_code=control_root_code.strip().upper(),
        output_path=write_result.path,
        volatility_window=volatility_window,
        min_vol_observations=min_vol_observations,
        weight_cap=weight_cap,
    )
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(render_stage4_report(report), encoding="utf-8")
    click.echo(f"wrote {output}")
    click.echo(f"wrote {write_result.relative_path}")
    click.echo(f"snapshot_id={report.snapshot_id}")
    click.echo(f"rows={report.row_count}")


def build_basket_frame(
    *,
    lakehouse_root: Path,
    root_codes: list[str] | None = None,
    volatility_window: int = _DEFAULT_VOL_WINDOW,
    min_vol_observations: int = _DEFAULT_MIN_VOL_OBSERVATIONS,
    weight_cap: float = _DEFAULT_WEIGHT_CAP,
) -> pd.DataFrame:
    """Build equal-weight and equal-risk commodity basket daily returns."""

    roots = list(root_codes or NON_GOLD_FUTURES_ROOT_CODES)
    _validate_stage4_parameters(
        root_codes=roots,
        volatility_window=volatility_window,
        min_vol_observations=min_vol_observations,
        weight_cap=weight_cap,
    )
    _validate_stage3_preconditions(root_codes=roots)
    returns = _load_return_matrix(lakehouse_root=lakehouse_root, root_codes=roots)
    returns = returns.dropna(how="any").sort_index()
    if len(returns) <= min_vol_observations:
        raise ValueError("not enough complete-case return rows for Stage 4 baskets")

    equal_weight = pd.DataFrame(
        1.0 / len(roots), index=returns.index, columns=roots, dtype=float
    )
    equal_risk, equal_risk_weight_source = _equal_risk_weights(
        returns=returns,
        volatility_window=volatility_window,
        min_vol_observations=min_vol_observations,
        weight_cap=weight_cap,
    )
    frames = [
        _basket_rows(
            returns=returns,
            weights=equal_weight,
            basket_rule=BasketRule.EQUAL_WEIGHT,
            volatility_window=volatility_window,
            weight_cap=weight_cap,
        ),
        _basket_rows(
            returns=returns,
            weights=equal_risk,
            basket_rule=BasketRule.EQUAL_RISK,
            volatility_window=volatility_window,
            weight_cap=weight_cap,
            weight_source=equal_risk_weight_source,
        ),
    ]
    return pd.concat(frames, ignore_index=True)


def write_basket_frame(
    *, frame: pd.DataFrame, lakehouse_root: Path
) -> ParquetWriteResult:
    """Write the Stage 4 basket frame to the derived lakehouse zone."""

    return write_dataset_parquet(
        frame=frame,
        dataset_name=_OUTPUT_DATASET,
        zone=StorageZone.DERIVED,
        partition_parts=[],
        lakehouse_root=lakehouse_root,
    )


def build_stage4_report(
    *,
    frame: pd.DataFrame,
    lakehouse_root: Path,
    root_codes: list[str],
    control_root_code: str,
    output_path: Path,
    volatility_window: int = _DEFAULT_VOL_WINDOW,
    min_vol_observations: int = _DEFAULT_MIN_VOL_OBSERVATIONS,
    weight_cap: float = _DEFAULT_WEIGHT_CAP,
) -> Stage4BasketReport:
    """Build Stage 4 report metrics from basket and continuous-contract inputs."""

    _validate_basket_report_frame(frame)
    returns = _load_return_matrix(lakehouse_root=lakehouse_root, root_codes=root_codes)
    returns = returns.dropna(how="any").sort_index()
    control_returns = _load_return_matrix(
        lakehouse_root=lakehouse_root, root_codes=[control_root_code]
    ).dropna(how="any")
    basket_returns = _basket_return_matrix(frame=frame)
    equal_risk_rows = frame[frame["basket_rule"].eq(BasketRule.EQUAL_RISK.value)]
    latest_date = equal_risk_rows["trade_date"].max()
    latest_rows = equal_risk_rows[equal_risk_rows["trade_date"].eq(latest_date)]
    latest_weights = _latest_weights(latest_rows)
    latest_risk = _risk_contributions(
        returns=returns.loc[:latest_date],
        weights={row["root_code"]: float(row["weight"]) for row in latest_weights},
        volatility_window=volatility_window,
    )
    code_version = _git_commit()
    return Stage4BasketReport(
        generated_at=datetime.now(tz=UTC),
        code_version=code_version,
        snapshot_id=_snapshot_id(
            code_version=code_version,
            lakehouse_root=lakehouse_root,
            root_codes=[*root_codes, control_root_code],
        ),
        lakehouse_root=lakehouse_root,
        output_path=output_path,
        root_codes=root_codes,
        control_root_code=control_root_code,
        start_date=frame["trade_date"].min(),
        end_date=frame["trade_date"].max(),
        row_count=len(frame),
        rebalance_frequency=_DEFAULT_REBALANCE_FREQUENCY,
        volatility_window=volatility_window,
        min_vol_observations=min_vol_observations,
        weight_cap=weight_cap,
        missing_data_rule="complete_case_across_stage4_roots",
        equal_risk_initial_equal_weight_days=int(
            equal_risk_rows.drop_duplicates("trade_date")["weight_source"]
            .eq("initial_equal_weight_until_vol_ready")
            .sum()
        ),
        first_equal_risk_rebalance_date=_first_rebalance_date(equal_risk_rows),
        quality_decisions=_load_quality_decisions(root_codes=root_codes),
        basket_metrics=[
            _series_metrics(rule, series) for rule, series in basket_returns.items()
        ],
        latest_weights=latest_weights,
        latest_risk_contributions=latest_risk,
        sector_risk_contributions=_sector_risk_contributions(latest_risk),
        pair_correlations=_pair_correlations(returns),
        leave_one_out_metrics=_leave_one_out_metrics(returns),
        leave_sector_out_metrics=_leave_sector_out_metrics(returns),
        au_sensitivity_metrics=_au_sensitivity_metrics(
            returns=returns,
            control_returns=control_returns,
            control_root_code=control_root_code,
        ),
    )


def render_stage4_report(report: Stage4BasketReport) -> str:
    """Render the Stage 4 basket rule-freeze report."""

    lines: list[str] = []
    lines.append("# TradePilot 商品期货阶段 4：商品篮子规则冻结报告")
    lines.append("")
    lines.append(f"Generated at: `{report.generated_at.isoformat()}`")
    lines.append(f"Code version: `{report.code_version or 'unknown'}`")
    lines.append(f"Snapshot id: `{report.snapshot_id}`")
    lines.append(f"Lakehouse root: `{report.lakehouse_root}`")
    lines.append(f"Output path: `{report.output_path}`")
    lines.append("")
    lines.append("## Scope")
    lines.append("")
    lines.append(
        "本报告只覆盖 Stage 4 商品篮子定义、权重规则冻结和风险贡献检查；"
        "不运行 ETF 基线增量回测，不形成商品 sleeve 接受结论。"
    )
    lines.append("")
    lines.append("## Frozen Rules")
    lines.append("")
    lines.extend(
        _markdown_table(
            headers=["Item", "Value"],
            rows=[
                ["Candidates", ", ".join(report.root_codes)],
                ["Control", report.control_root_code],
                ["Rebalance frequency", report.rebalance_frequency],
                ["Volatility window", str(report.volatility_window)],
                ["Minimum volatility observations", str(report.min_vol_observations)],
                ["Weight cap", _percent_text(report.weight_cap)],
                ["Missing data rule", report.missing_data_rule],
                ["Performance field", "continuous_return from Stage 2 adjusted_close"],
                [
                    "Equal-risk initial equal-weight days",
                    str(report.equal_risk_initial_equal_weight_days),
                ],
                [
                    "First equal-risk rebalance date",
                    (
                        "-"
                        if report.first_equal_risk_rebalance_date is None
                        else report.first_equal_risk_rebalance_date.isoformat()
                    ),
                ],
            ],
        )
    )
    lines.append("")
    lines.append("## Candidate Decisions")
    lines.append("")
    lines.extend(
        _markdown_table(
            headers=["Root", "Stage 3 decision"],
            rows=[[root, report.quality_decisions[root]] for root in report.root_codes],
        )
    )
    lines.append("")
    lines.append("## Basket Metrics")
    lines.append("")
    lines.extend(
        _markdown_table(
            headers=[
                "Rule",
                "Rows",
                "Window",
                "Ann return",
                "Ann volatility",
                "Max drawdown",
            ],
            rows=[
                [
                    str(row["rule"]),
                    str(row["rows"]),
                    str(row["window"]),
                    _percent_text(float(row["annualized_return"])),
                    _percent_text(float(row["annualized_volatility"])),
                    _percent_text(float(row["max_drawdown"])),
                ]
                for row in report.basket_metrics
            ],
        )
    )
    lines.append("")
    lines.append("## Latest Equal-Risk Weights")
    lines.append("")
    lines.extend(
        _markdown_table(
            headers=["Root", "Sector", "Weight"],
            rows=[
                [
                    str(row["root_code"]),
                    str(row["sector"]),
                    _percent_text(float(row["weight"])),
                ]
                for row in report.latest_weights
            ],
        )
    )
    lines.append("")
    lines.append("## Latest Equal-Risk Contribution")
    lines.append("")
    lines.extend(
        _markdown_table(
            headers=[
                "Root",
                "Sector",
                "Weight",
                "Vol contribution",
                "Risk contribution",
            ],
            rows=[
                [
                    str(row["root_code"]),
                    str(row["sector"]),
                    _percent_text(float(row["weight"])),
                    _percent_text(float(row["vol_contribution"])),
                    _percent_text(float(row["risk_contribution"])),
                ]
                for row in report.latest_risk_contributions
            ],
        )
    )
    lines.append("")
    lines.append("## Sector Risk Contribution")
    lines.append("")
    lines.extend(
        _markdown_table(
            headers=["Sector", "Risk contribution"],
            rows=[
                [str(row["sector"]), _percent_text(float(row["risk_contribution"]))]
                for row in report.sector_risk_contributions
            ],
        )
    )
    lines.append("")
    lines.append("## Required Pair Correlations")
    lines.append("")
    lines.extend(
        _markdown_table(
            headers=["Pair", "Correlation"],
            rows=[
                [str(row["pair"]), f"{float(row['correlation']):.4f}"]
                for row in report.pair_correlations
            ],
        )
    )
    lines.append("")
    lines.append("## Sensitivity Checks")
    lines.append("")
    lines.append("### Leave One Out")
    lines.append("")
    lines.extend(_sensitivity_table(report.leave_one_out_metrics))
    lines.append("")
    lines.append("### Leave Sector Out")
    lines.append("")
    lines.extend(_sensitivity_table(report.leave_sector_out_metrics))
    lines.append("")
    lines.append("### AU Control")
    lines.append("")
    lines.extend(_sensitivity_table(report.au_sensitivity_metrics))
    lines.append("")
    lines.append("## Stage 4 Decision")
    lines.append("")
    lines.append(
        "结论：`stage4_rule_frozen`。等权与等风险商品篮子定义、参数和缺失数据"
        "规则已冻结，可进入 Stage 5 的 ETF 基线增量回测。"
    )
    lines.append("")
    lines.append(
        "限制：Stage 4 只证明篮子构造可复算；商品 sleeve 是否保留仍取决于 Stage 5/6 "
        "的基线增量回测、成本和稳健性评估。"
    )
    lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def _equal_risk_weights(
    *,
    returns: pd.DataFrame,
    volatility_window: int,
    min_vol_observations: int,
    weight_cap: float,
) -> tuple[pd.DataFrame, pd.Series]:
    """Return trailing-vol inverse weights and source labels."""

    trailing_vol = returns.rolling(
        window=volatility_window, min_periods=min_vol_observations
    ).std()
    rebalance_dates = set(_month_end_dates(returns.index))
    weights = pd.DataFrame(index=returns.index, columns=returns.columns, dtype=float)
    sources = pd.Series(
        "initial_equal_weight_until_vol_ready", index=returns.index, dtype=object
    )
    current = pd.Series(1.0 / len(returns.columns), index=returns.columns, dtype=float)
    for trade_date, row in trailing_vol.shift(1).iterrows():
        if trade_date in rebalance_dates and row.notna().all() and row.gt(0).all():
            current = _cap_and_normalize((1.0 / row), cap=weight_cap)
            sources.loc[trade_date:] = "inverse_vol_month_end"
        weights.loc[trade_date] = current
    return weights, sources


def _validate_basket_report_frame(frame: pd.DataFrame) -> None:
    """Validate the long-form basket frame before report metrics."""

    required = {"trade_date", "basket_rule", "basket_return"}
    missing = required.difference(frame.columns)
    if missing:
        raise ValueError(
            "Stage 4 basket frame missing columns: " + ", ".join(sorted(missing))
        )
    equal_risk_rows = frame[frame["basket_rule"].eq(BasketRule.EQUAL_RISK.value)]
    if equal_risk_rows.empty:
        raise ValueError("Stage 4 basket frame has no equal_risk rows")
    if equal_risk_rows["trade_date"].nunique() < 2:
        raise ValueError("Stage 4 basket frame needs at least two equal_risk dates")


def _basket_rows(
    *,
    returns: pd.DataFrame,
    weights: pd.DataFrame,
    basket_rule: BasketRule,
    volatility_window: int,
    weight_cap: float,
    weight_source: pd.Series | None = None,
) -> pd.DataFrame:
    """Return long-form basket rows with component weights and contributions."""

    basket_return = (returns * weights).sum(axis=1)
    basket_nav = (1 + basket_return).cumprod()
    rows: list[dict[str, object]] = []
    for trade_date in returns.index:
        for root_code in returns.columns:
            weight = float(weights.loc[trade_date, root_code])
            component_return = float(returns.loc[trade_date, root_code])
            rows.append(
                {
                    "trade_date": trade_date,
                    "basket_rule": basket_rule.value,
                    "root_code": root_code,
                    "sector": _SECTORS[root_code],
                    "target_weight": weight,
                    "component_return": component_return,
                    "weighted_return": weight * component_return,
                    "basket_return": float(basket_return.loc[trade_date]),
                    "basket_nav": float(basket_nav.loc[trade_date]),
                    "rebalance_frequency": _DEFAULT_REBALANCE_FREQUENCY,
                    "volatility_window": volatility_window,
                    "weight_cap": weight_cap,
                    "weight_source": (
                        basket_rule.value
                        if weight_source is None
                        else str(weight_source.loc[trade_date])
                    ),
                    "missing_data_rule": "complete_case_across_stage4_roots",
                }
            )
    return pd.DataFrame(rows)


def _load_return_matrix(*, lakehouse_root: Path, root_codes: list[str]) -> pd.DataFrame:
    """Load Stage 2 continuous returns into a wide return matrix."""

    series: list[pd.Series] = []
    for root_code in root_codes:
        path = (
            build_zone_path(_CONTINUOUS_DATASET, StorageZone.DERIVED, lakehouse_root)
            / root_code
            / "part-00000.parquet"
        )
        if not path.exists():
            raise ValueError(f"missing Stage 2 continuous contract for {root_code}")
        frame = pd.read_parquet(path)
        required = {"trade_date", "root_symbol", "continuous_return"}
        missing = required.difference(frame.columns)
        if missing:
            raise ValueError(
                f"continuous contract {root_code} missing columns: "
                + ", ".join(sorted(missing))
            )
        if not frame["root_symbol"].eq(root_code).all():
            raise ValueError(
                f"continuous contract {root_code} has mixed root_symbol rows"
            )
        dates = pd.to_datetime(frame["trade_date"], errors="coerce").dt.date
        item = pd.Series(
            frame["continuous_return"].to_numpy(), index=dates, name=root_code
        )
        if item.index.duplicated().any():
            raise ValueError(
                f"continuous contract {root_code} has duplicate trade_date rows"
            )
        series.append(item)
    return pd.concat(series, axis=1)


def _validate_stage3_preconditions(*, root_codes: list[str]) -> None:
    """Validate Stage 3 cards exist and do not reject any Stage 4 candidate."""

    decisions = _load_quality_decisions(root_codes=root_codes)
    rejected = [root for root, decision in decisions.items() if decision == "reject"]
    if rejected:
        raise ValueError("Stage 3 rejected candidates: " + ", ".join(rejected))


def _load_quality_decisions(*, root_codes: list[str]) -> dict[str, str]:
    """Load Stage 3 decisions from structured quality-card JSON sidecars."""

    decisions: dict[str, str] = {}
    for root_code in root_codes:
        path = _quality_card_json_path(root_code)
        if not path.exists():
            raise ValueError(f"missing Stage 3 quality-card JSON for {root_code}")
        payload = json.loads(path.read_text(encoding="utf-8"))
        decision = str(payload.get("decision", ""))
        if decision not in {"accept", "observe", "reject"}:
            raise ValueError(f"unknown Stage 3 decision for {root_code}: {decision}")
        decisions[root_code] = decision
    return decisions


def _quality_card_json_path(root_code: str) -> Path:
    """Return the canonical Stage 3 quality-card JSON path for one root."""

    base = Path("docs/futures-v2-design/reports/stage-3/quality-cards")
    if root_code == "M.DCE":
        return base / "commodity-futures-stage-3-m-quality-card.json"
    return base / f"commodity-futures-stage-3-{_root_slug(root_code)}-quality-card.json"


def _basket_return_matrix(*, frame: pd.DataFrame) -> dict[str, pd.Series]:
    """Return one basket-return series per rule from long-form rows."""

    result: dict[str, pd.Series] = {}
    for rule, group in frame.groupby("basket_rule"):
        daily = group.drop_duplicates("trade_date").sort_values("trade_date")
        result[str(rule)] = pd.Series(
            daily["basket_return"].to_numpy(), index=daily["trade_date"], name=str(rule)
        )
    return result


def _series_metrics(rule: str, series: pd.Series) -> dict[str, object]:
    """Return annualized return, volatility and drawdown for one return series."""

    valid = series.dropna()
    wealth = (1 + valid).cumprod()
    drawdown = wealth / wealth.cummax() - 1
    return {
        "rule": rule,
        "rows": len(valid),
        "window": f"{valid.index.min()} .. {valid.index.max()}",
        "annualized_return": _annualized_return(valid),
        "annualized_volatility": float(valid.std() * (_TRADING_DAYS_PER_YEAR**0.5)),
        "max_drawdown": float(drawdown.min()),
    }


def _latest_weights(rows: pd.DataFrame) -> list[dict[str, object]]:
    """Return latest equal-risk target weights."""

    output: list[dict[str, object]] = []
    for row in rows.sort_values("root_code").to_dict("records"):
        output.append(
            {
                "root_code": row["root_code"],
                "sector": row["sector"],
                "weight": float(row["target_weight"]),
            }
        )
    return output


def _risk_contributions(
    *, returns: pd.DataFrame, weights: dict[str, float], volatility_window: int
) -> list[dict[str, object]]:
    """Return latest volatility and risk contribution by root."""

    returns = returns.sort_index()
    weight_series = pd.Series(weights)
    covariance = returns[list(weights)].tail(volatility_window).cov()
    portfolio_variance = float(weight_series.T @ covariance @ weight_series)
    if portfolio_variance <= 0 or math.isnan(portfolio_variance):
        raise ValueError("cannot compute risk contribution from covariance matrix")
    marginal = covariance @ weight_series
    contributions = weight_series * marginal / portfolio_variance
    vol_contribution = (
        weight_series * returns[list(weights)].tail(volatility_window).std()
    )
    total_vol_contribution = float(vol_contribution.sum())
    rows: list[dict[str, object]] = []
    for root_code in sorted(weights):
        rows.append(
            {
                "root_code": root_code,
                "sector": _SECTORS[root_code],
                "weight": float(weight_series[root_code]),
                "vol_contribution": float(
                    vol_contribution[root_code] / total_vol_contribution
                ),
                "risk_contribution": float(contributions[root_code]),
            }
        )
    return rows


def _sector_risk_contributions(
    root_contributions: list[dict[str, object]],
) -> list[dict[str, object]]:
    """Aggregate root risk contributions by sector."""

    sector_totals: dict[str, float] = {}
    for row in root_contributions:
        sector = str(row["sector"])
        sector_totals[sector] = sector_totals.get(sector, 0.0) + float(
            row["risk_contribution"]
        )
    return [
        {"sector": sector, "risk_contribution": sector_totals[sector]}
        for sector in sorted(sector_totals)
    ]


def _pair_correlations(returns: pd.DataFrame) -> list[dict[str, object]]:
    """Return required pair correlations from the latest common sample."""

    rows: list[dict[str, object]] = []
    for left, right in [
        ("AL.SHF", "CU.SHF"),
        ("RB.SHF", "I.DCE"),
        ("SC.INE", "TA.ZCE"),
    ]:
        if left in returns and right in returns:
            rows.append(
                {
                    "pair": f"{left}/{right}",
                    "correlation": float(returns[left].corr(returns[right])),
                }
            )
    return rows


def _leave_one_out_metrics(returns: pd.DataFrame) -> list[dict[str, object]]:
    """Return equal-weight basket metrics after excluding each root."""

    rows: list[dict[str, object]] = []
    for root_code in returns.columns:
        subset = returns.drop(columns=[root_code])
        rows.append(_sensitivity_metrics(f"exclude {root_code}", subset.mean(axis=1)))
    return rows


def _leave_sector_out_metrics(returns: pd.DataFrame) -> list[dict[str, object]]:
    """Return equal-weight basket metrics after excluding each sector."""

    rows: list[dict[str, object]] = []
    sectors = sorted({_SECTORS[root] for root in returns.columns})
    for sector in sectors:
        kept = [root for root in returns.columns if _SECTORS[root] != sector]
        rows.append(
            _sensitivity_metrics(f"exclude {sector}", returns[kept].mean(axis=1))
        )
    return rows


def _au_sensitivity_metrics(
    *, returns: pd.DataFrame, control_returns: pd.DataFrame, control_root_code: str
) -> list[dict[str, object]]:
    """Return equal-weight metrics with and without the AU control root."""

    common = returns.join(control_returns, how="inner").dropna(how="any")
    with_control = common.mean(axis=1)
    without_control = common[returns.columns].mean(axis=1)
    return [
        _sensitivity_metrics("exclude AU control", without_control),
        _sensitivity_metrics(f"include {control_root_code}", with_control),
    ]


def _sensitivity_metrics(name: str, series: pd.Series) -> dict[str, object]:
    """Return compact sensitivity metrics for one synthetic series."""

    metrics = _series_metrics(name, series)
    return {
        "scenario": name,
        "rows": metrics["rows"],
        "annualized_return": metrics["annualized_return"],
        "annualized_volatility": metrics["annualized_volatility"],
        "max_drawdown": metrics["max_drawdown"],
    }


def _cap_and_normalize(values: pd.Series, *, cap: float) -> pd.Series:
    """Apply a long-only cap and normalize weights to one."""

    if values.empty or values.isna().any() or values.lt(0).any():
        raise ValueError("weight inputs must be non-missing and non-negative")
    if float(values.sum()) <= 0:
        raise ValueError("weight inputs must have positive total")
    weights = values / values.sum()
    capped = weights.clip(upper=cap)
    while capped.lt(cap).any() and capped.sum() < 0.999999:
        room = capped.lt(cap)
        remaining = 1.0 - float(capped.sum())
        room_sum = float(weights[room].sum())
        if room_sum <= 0:
            raise ValueError("uncapped weight inputs must have positive total")
        add = weights[room] / room_sum * remaining
        capped.loc[room] = (capped.loc[room] + add).clip(upper=cap)
    return capped / capped.sum()


def _first_rebalance_date(equal_risk_rows: pd.DataFrame) -> date | None:
    """Return the first date that used inverse-vol equal-risk weights."""

    rows = equal_risk_rows[equal_risk_rows["weight_source"].eq("inverse_vol_month_end")]
    if rows.empty:
        return None
    return rows["trade_date"].min()


def _month_end_dates(index: pd.Index) -> list[date]:
    """Return the last available trading date of each calendar month."""

    frame = pd.DataFrame({"trade_date": list(index)})
    month_key = pd.to_datetime(frame["trade_date"]).dt.to_period("M")
    return list(frame.groupby(month_key)["trade_date"].max())


def _annualized_return(valid_returns: pd.Series) -> float:
    """Return geometric annualized return for a daily return series."""

    if valid_returns.empty:
        return 0.0
    total_return = float((1 + valid_returns).prod())
    if total_return <= 0:
        return math.nan
    return total_return ** (_TRADING_DAYS_PER_YEAR / len(valid_returns)) - 1


def _snapshot_id(
    *, code_version: str | None, lakehouse_root: Path, root_codes: list[str]
) -> str:
    """Build one deterministic Stage 4 snapshot identifier."""

    digest = hashlib.sha256()
    for item in [code_version or "unknown", lakehouse_root.as_posix(), *root_codes]:
        digest.update(item.encode("utf-8"))
    dataset_root = build_zone_path(
        _CONTINUOUS_DATASET, StorageZone.DERIVED, lakehouse_root
    )
    for root_code in root_codes:
        path = dataset_root / root_code / "part-00000.parquet"
        if path.exists():
            digest.update(path.relative_to(lakehouse_root).as_posix().encode("utf-8"))
            digest.update(_sha256_file(path).encode("utf-8"))
    return digest.hexdigest()[:16]


def _sha256_file(path: Path) -> str:
    """Return the SHA256 digest for one file."""

    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _git_commit() -> str | None:
    """Return the current git commit hash and dirty marker if available."""

    try:
        completed = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
        )
    except (OSError, subprocess.CalledProcessError):
        return None
    commit = completed.stdout.strip()
    if not commit:
        return None
    status = subprocess.run(
        ["git", "status", "--porcelain"],
        check=False,
        capture_output=True,
        text=True,
    )
    if status.stdout.strip():
        return f"{commit}-dirty"
    return commit


def _validate_stage4_parameters(
    *,
    root_codes: list[str],
    volatility_window: int,
    min_vol_observations: int,
    weight_cap: float,
) -> None:
    """Validate Stage 4 rule parameters."""

    if not root_codes:
        raise ValueError("root_codes must not be empty")
    if len(set(root_codes)) != len(root_codes):
        raise ValueError("root_codes must be unique")
    unknown = [root for root in root_codes if root not in _SECTORS or root == "AU.SHF"]
    if unknown:
        raise ValueError("unsupported Stage 4 candidate roots: " + ", ".join(unknown))
    if volatility_window <= 1:
        raise ValueError("volatility_window must be greater than 1")
    if min_vol_observations <= 1 or min_vol_observations > volatility_window:
        raise ValueError("min_vol_observations must be in (1, volatility_window]")
    if weight_cap <= 0 or weight_cap > 1:
        raise ValueError("weight_cap must be in (0, 1]")
    if weight_cap * len(root_codes) < 1:
        raise ValueError("weight_cap is too low for the number of roots")


def _parse_root_codes(root_codes: str) -> list[str]:
    """Parse a comma-separated root-code list."""

    roots = [root.strip().upper() for root in root_codes.split(",") if root.strip()]
    if not roots:
        raise click.BadParameter("root-codes must contain at least one root")
    return roots


def _root_slug(root_code: str) -> str:
    """Return a stable lowercase file slug for one root code."""

    return root_code.lower().replace(".", "-")


def _sensitivity_table(rows: list[dict[str, object]]) -> list[str]:
    """Render one sensitivity table."""

    return _markdown_table(
        headers=["Scenario", "Rows", "Ann return", "Ann volatility", "Max drawdown"],
        rows=[
            [
                str(row["scenario"]),
                str(row["rows"]),
                _percent_text(float(row["annualized_return"])),
                _percent_text(float(row["annualized_volatility"])),
                _percent_text(float(row["max_drawdown"])),
            ]
            for row in rows
        ],
    )


def _percent_text(value: float) -> str:
    """Format a decimal number as a percentage."""

    return f"{value:.4%}"


def _markdown_table(headers: list[str], rows: list[list[str]]) -> list[str]:
    """Render a markdown table."""

    if not rows:
        return ["_No rows_"]
    output = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join("---" for _ in headers) + " |",
    ]
    for row in rows:
        output.append("| " + " | ".join(row) + " |")
    return output


if __name__ == "__main__":
    main()
