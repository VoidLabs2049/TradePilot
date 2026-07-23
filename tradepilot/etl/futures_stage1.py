"""Commodity futures stage 1 single-contract and roll audit report."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime
import hashlib
import subprocess
from pathlib import Path
from typing import Any

import click
import pandas as pd

from tradepilot.config import LAKEHOUSE_ROOT
from tradepilot.etl.models import StorageZone
from tradepilot.etl.storage import build_zone_path

_DEFAULT_ROOT_CODE = "M.DCE"
_DEFAULT_ROLL_DATE = date(2025, 4, 7)
_DEFAULT_WINDOW_DAYS = 5
_REQUIRED_DAILY_FIELDS = ("close", "settle", "volume", "oi")
_SOURCE_DATASETS = (
    "reference.futures_instruments",
    "market.futures_mapping",
    "market.futures_contract_daily",
)


@dataclass(frozen=True)
class ContractCalculation:
    """Single-contract sizing calculation for the roll target contract."""

    contract_code: str
    trade_date: date
    close: float
    multiplier: float
    trade_unit: str
    quote_unit: str
    one_lot_notional: float
    pnl_for_one_percent_move: float


@dataclass(frozen=True)
class RollGap:
    """Naive roll gap on the selected switch date."""

    trade_date: date
    roll_from: str
    roll_to: str
    old_close: float
    new_close: float
    close_gap: float
    close_gap_pct: float
    old_settle: float
    new_settle: float
    settle_gap: float
    settle_gap_pct: float


@dataclass(frozen=True)
class RollWindowRow:
    """One audited contract row in the selected roll window."""

    trade_date: date
    mapped_active: str
    contract_code: str
    close: float
    settle: float
    volume: float
    oi: float
    is_mapped_active: bool


@dataclass(frozen=True)
class Stage1Report:
    """Structured data backing the stage 1 M roll audit report."""

    generated_at: datetime
    code_version: str | None
    snapshot_id: str
    lakehouse_root: Path
    root_code: str
    roll_date: date
    roll_from: str
    roll_to: str
    window_days: int
    contract_calculation: ContractCalculation
    roll_gap: RollGap
    window_rows: list[RollWindowRow]


@click.command()
@click.option(
    "--lakehouse-root",
    type=click.Path(path_type=Path, file_okay=False),
    default=LAKEHOUSE_ROOT,
    show_default=True,
)
@click.option("--root-code", default=_DEFAULT_ROOT_CODE, show_default=True)
@click.option(
    "--roll-date",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    default=_DEFAULT_ROLL_DATE.isoformat(),
    show_default=True,
)
@click.option(
    "--window-days", type=int, default=_DEFAULT_WINDOW_DAYS, show_default=True
)
@click.option(
    "--output",
    type=click.Path(path_type=Path, dir_okay=False),
    default=Path("docs/futures-v2-design/commodity-futures-stage-1-m-roll-audit.md"),
    show_default=True,
)
def main(
    lakehouse_root: Path,
    root_code: str,
    roll_date: datetime,
    window_days: int,
    output: Path,
) -> None:
    """Build and write the commodity futures stage 1 roll audit report."""

    report = build_stage1_report(
        lakehouse_root=lakehouse_root,
        root_code=root_code,
        roll_date=roll_date.date(),
        window_days=window_days,
    )
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(render_stage1_report(report), encoding="utf-8")
    click.echo(f"wrote {output}")
    click.echo(f"snapshot_id={report.snapshot_id}")
    click.echo(f"roll={report.roll_from}->{report.roll_to}")


def build_stage1_report(
    *,
    lakehouse_root: Path,
    root_code: str = _DEFAULT_ROOT_CODE,
    roll_date: date = _DEFAULT_ROLL_DATE,
    window_days: int = _DEFAULT_WINDOW_DAYS,
) -> Stage1Report:
    """Build a stage 1 single-contract and one-roll audit from lakehouse data."""

    if window_days < 1:
        raise ValueError("window_days must be positive")
    root_code = root_code.strip().upper()
    mapping = _load_normalized_dataset("market.futures_mapping", lakehouse_root)
    daily = _load_normalized_dataset("market.futures_contract_daily", lakehouse_root)
    instruments = _load_normalized_dataset(
        "reference.futures_instruments", lakehouse_root
    )
    root_mapping = (
        mapping[mapping["root_code"].eq(root_code)]
        .sort_values("trade_date")
        .reset_index(drop=True)
    )
    if root_mapping.empty:
        raise ValueError(f"no mapping rows for {root_code}")

    switch_index = _roll_switch_index(root_mapping, roll_date)
    roll_from = str(root_mapping.loc[switch_index - 1, "active_contract"])
    roll_to = str(root_mapping.loc[switch_index, "active_contract"])
    window_mapping = root_mapping.iloc[
        switch_index - window_days : switch_index + window_days + 1
    ].copy()
    expected_rows = window_days * 2 + 1
    if len(window_mapping) != expected_rows:
        raise ValueError(
            f"roll window requires {expected_rows} mapping rows, found {len(window_mapping)}"
        )

    roll_contracts = [roll_from, roll_to]
    window_rows = _build_window_rows(
        window_mapping=window_mapping,
        daily=daily,
        roll_contracts=roll_contracts,
    )
    roll_gap = _build_roll_gap(
        daily=daily,
        roll_date=roll_date,
        roll_from=roll_from,
        roll_to=roll_to,
    )
    contract_calculation = _build_contract_calculation(
        instruments=instruments,
        roll_to=roll_to,
        roll_date=roll_date,
        close=roll_gap.new_close,
    )
    code_version = _git_commit()
    return Stage1Report(
        generated_at=datetime.now(tz=UTC),
        code_version=code_version,
        snapshot_id=_snapshot_id(
            code_version=code_version,
            lakehouse_root=lakehouse_root,
            root_code=root_code,
            roll_date=roll_date,
            window_days=window_days,
            roll_from=roll_from,
            roll_to=roll_to,
        ),
        lakehouse_root=lakehouse_root,
        root_code=root_code,
        roll_date=roll_date,
        roll_from=roll_from,
        roll_to=roll_to,
        window_days=window_days,
        contract_calculation=contract_calculation,
        roll_gap=roll_gap,
        window_rows=window_rows,
    )


def render_stage1_report(report: Stage1Report) -> str:
    """Render one stage 1 roll audit report as markdown."""

    calc = report.contract_calculation
    gap = report.roll_gap
    lines: list[str] = []
    lines.append("# TradePilot 商品期货阶段 1：M 单合约与一次换月审计")
    lines.append("")
    lines.append(f"Generated at: `{report.generated_at.isoformat()}`")
    lines.append(f"Code version: `{report.code_version or 'unknown'}`")
    lines.append(f"Snapshot id: `{report.snapshot_id}`")
    lines.append(f"Lakehouse root: `{report.lakehouse_root}`")
    lines.append("")
    lines.append("## Scope")
    lines.append("")
    lines.append(
        "本报告只覆盖 Stage 1 的最小样本：豆粕 `M.DCE` 的单合约计算样例，"
        "以及一次主力切换窗口审计；不构建连续合约，不进入篮子研究。"
    )
    lines.append("")
    lines.append("## Roll Selection")
    lines.append("")
    lines.extend(
        _markdown_table(
            headers=["Root", "Roll date", "Roll from", "Roll to", "Window"],
            rows=[
                [
                    report.root_code,
                    report.roll_date.isoformat(),
                    report.roll_from,
                    report.roll_to,
                    f"{report.window_days} trading days before/after",
                ]
            ],
        )
    )
    lines.append("")
    lines.append("## Single Contract Calculation")
    lines.append("")
    lines.extend(
        _markdown_table(
            headers=[
                "Contract",
                "Trade date",
                "Close",
                "Multiplier",
                "Trade unit",
                "Quote unit",
                "One-lot notional",
                "P/L for 1% move",
            ],
            rows=[
                [
                    calc.contract_code,
                    calc.trade_date.isoformat(),
                    _number_text(calc.close),
                    _number_text(calc.multiplier),
                    calc.trade_unit,
                    calc.quote_unit,
                    _number_text(calc.one_lot_notional),
                    _number_text(calc.pnl_for_one_percent_move),
                ]
            ],
        )
    )
    lines.append("")
    lines.append("## Roll Window Audit")
    lines.append("")
    lines.extend(
        _markdown_table(
            headers=[
                "Trade date",
                "Mapped active",
                "Contract",
                "Close",
                "Settle",
                "Volume",
                "OI",
                "Mapped?",
            ],
            rows=[
                [
                    row.trade_date.isoformat(),
                    row.mapped_active,
                    row.contract_code,
                    _number_text(row.close),
                    _number_text(row.settle),
                    _number_text(row.volume),
                    _number_text(row.oi),
                    "yes" if row.is_mapped_active else "no",
                ]
                for row in report.window_rows
            ],
        )
    )
    lines.append("")
    lines.append("## Naive Roll Gap")
    lines.append("")
    lines.extend(
        _markdown_table(
            headers=[
                "Date",
                "Old close",
                "New close",
                "Close gap",
                "Close gap %",
                "Old settle",
                "New settle",
                "Settle gap",
                "Settle gap %",
            ],
            rows=[
                [
                    gap.trade_date.isoformat(),
                    _number_text(gap.old_close),
                    _number_text(gap.new_close),
                    _number_text(gap.close_gap),
                    f"{gap.close_gap_pct:.4%}",
                    _number_text(gap.old_settle),
                    _number_text(gap.new_settle),
                    _number_text(gap.settle_gap),
                    f"{gap.settle_gap_pct:.4%}",
                ]
            ],
        )
    )
    lines.append("")
    lines.append("## Stage 1 Decision")
    lines.append("")
    lines.append(
        "结论：`pass`。本窗口所有新旧合约审计行均包含 `close/settle/volume/oi`，"
        "单合约名义价值和 1% 价格变动盈亏可由原始 `close` 与 `multiplier` 复算。"
    )
    lines.append("")
    lines.append(
        f"天真拼接会在 {gap.trade_date.isoformat()} 从 `{gap.roll_from}` 的 close "
        f"`{_number_text(gap.old_close)}` 跳到 `{gap.roll_to}` 的 close "
        f"`{_number_text(gap.new_close)}`，形成 `{_number_text(gap.close_gap)}` "
        f"点、`{gap.close_gap_pct:.4%}` 的假跳空；该跳空来自合约切换价差，"
        "不能解释为可交易的单日市场收益，也不是实际移仓成本。"
    )
    lines.append("")
    lines.append(
        "后续 Stage 2 若构建连续合约，应继续保留新旧合约价格、换月调整量和来源批次，"
        "并禁止用天真拼接序列计算绩效。"
    )
    lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def _roll_switch_index(mapping: pd.DataFrame, roll_date: date) -> int:
    """Return the row index where active contract switches on roll_date."""

    matches = mapping.index[mapping["trade_date"].eq(roll_date)].tolist()
    if len(matches) != 1:
        raise ValueError(f"roll_date {roll_date.isoformat()} is not unique in mapping")
    index = int(matches[0])
    if index == 0:
        raise ValueError("roll_date has no previous mapping row")
    previous = str(mapping.loc[index - 1, "active_contract"])
    current = str(mapping.loc[index, "active_contract"])
    if previous == current:
        raise ValueError(f"roll_date {roll_date.isoformat()} is not a switch date")
    return index


def _build_window_rows(
    *,
    window_mapping: pd.DataFrame,
    daily: pd.DataFrame,
    roll_contracts: list[str],
) -> list[RollWindowRow]:
    """Build the two-contract audit rows for a selected mapping window."""

    dates = set(window_mapping["trade_date"])
    subset = daily[
        daily["trade_date"].isin(dates) & daily["contract_code"].isin(roll_contracts)
    ].copy()
    expected_rows = len(window_mapping) * len(roll_contracts)
    if len(subset) != expected_rows:
        raise ValueError(
            f"roll window daily rows expected {expected_rows}, found {len(subset)}"
        )
    if subset[list(_REQUIRED_DAILY_FIELDS)].isna().any(axis=None):
        raise ValueError("roll window has missing close/settle/volume/oi values")

    rows: list[RollWindowRow] = []
    for _, mapping_row in window_mapping.iterrows():
        trade_date = mapping_row["trade_date"]
        mapped_active = str(mapping_row["active_contract"])
        for contract_code in roll_contracts:
            matched = subset[
                subset["trade_date"].eq(trade_date)
                & subset["contract_code"].eq(contract_code)
            ]
            if len(matched) != 1:
                raise ValueError(
                    f"daily row is not unique for {contract_code} {trade_date}"
                )
            daily_row = matched.iloc[0]
            rows.append(
                RollWindowRow(
                    trade_date=trade_date,
                    mapped_active=mapped_active,
                    contract_code=contract_code,
                    close=float(daily_row["close"]),
                    settle=float(daily_row["settle"]),
                    volume=float(daily_row["volume"]),
                    oi=float(daily_row["oi"]),
                    is_mapped_active=contract_code == mapped_active,
                )
            )
    return rows


def _build_roll_gap(
    *, daily: pd.DataFrame, roll_date: date, roll_from: str, roll_to: str
) -> RollGap:
    """Calculate the naive same-day price gap between old and new contracts."""

    rows = daily[
        daily["trade_date"].eq(roll_date)
        & daily["contract_code"].isin([roll_from, roll_to])
    ].copy()
    if len(rows) != 2:
        raise ValueError("roll gap requires exactly two same-day contract rows")
    if rows[list(_REQUIRED_DAILY_FIELDS)].isna().any(axis=None):
        raise ValueError("roll gap rows have missing close/settle/volume/oi values")
    old = rows[rows["contract_code"].eq(roll_from)].iloc[0]
    new = rows[rows["contract_code"].eq(roll_to)].iloc[0]
    old_close = float(old["close"])
    new_close = float(new["close"])
    old_settle = float(old["settle"])
    new_settle = float(new["settle"])
    return RollGap(
        trade_date=roll_date,
        roll_from=roll_from,
        roll_to=roll_to,
        old_close=old_close,
        new_close=new_close,
        close_gap=new_close - old_close,
        close_gap_pct=(new_close / old_close) - 1,
        old_settle=old_settle,
        new_settle=new_settle,
        settle_gap=new_settle - old_settle,
        settle_gap_pct=(new_settle / old_settle) - 1,
    )


def _build_contract_calculation(
    *, instruments: pd.DataFrame, roll_to: str, roll_date: date, close: float
) -> ContractCalculation:
    """Build the single-contract sizing calculation for the new active contract."""

    rows = instruments[instruments["contract_code"].eq(roll_to)]
    if rows.empty:
        raise ValueError(f"missing instrument metadata for {roll_to}")
    row = rows.iloc[0]
    multiplier = float(row["multiplier"])
    one_lot_notional = close * multiplier
    return ContractCalculation(
        contract_code=roll_to,
        trade_date=roll_date,
        close=close,
        multiplier=multiplier,
        trade_unit=str(row.get("trade_unit", "")),
        quote_unit=str(row.get("quote_unit", "")),
        one_lot_notional=one_lot_notional,
        pnl_for_one_percent_move=one_lot_notional * 0.01,
    )


def _load_normalized_dataset(dataset_name: str, lakehouse_root: Path) -> pd.DataFrame:
    """Load one normalized parquet dataset from lakehouse partitions."""

    paths = sorted(
        build_zone_path(dataset_name, StorageZone.NORMALIZED, lakehouse_root).rglob(
            "*.parquet"
        )
    )
    if not paths:
        return pd.DataFrame()
    frame = pd.concat([pd.read_parquet(path) for path in paths], ignore_index=True)
    for column in ("trade_date", "list_date", "delist_date"):
        if column in frame.columns:
            frame[column] = pd.to_datetime(frame[column], errors="coerce").dt.date
    return frame


def _snapshot_id(
    *,
    code_version: str | None,
    lakehouse_root: Path,
    root_code: str,
    roll_date: date,
    window_days: int,
    roll_from: str,
    roll_to: str,
) -> str:
    """Build one deterministic snapshot identifier from current inputs."""

    digest = hashlib.sha256()
    for item in (
        code_version or "unknown",
        lakehouse_root.as_posix(),
        root_code,
        roll_date.isoformat(),
        str(window_days),
        roll_from,
        roll_to,
    ):
        digest.update(item.encode("utf-8"))
    for dataset_name in _SOURCE_DATASETS:
        digest.update(dataset_name.encode("utf-8"))
        for path in sorted(
            build_zone_path(dataset_name, StorageZone.NORMALIZED, lakehouse_root).rglob(
                "*.parquet"
            )
        ):
            stat = path.stat()
            digest.update(
                f"{path.relative_to(lakehouse_root).as_posix()}:{stat.st_size}:{stat.st_mtime_ns}".encode(
                    "utf-8"
                )
            )
    return digest.hexdigest()[:16]


def _markdown_table(headers: list[str], rows: list[list[str]]) -> list[str]:
    """Render a small markdown table."""

    if not rows:
        return ["_No rows_"]
    output = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join("---" for _ in headers) + " |",
    ]
    for row in rows:
        output.append("| " + " | ".join(row) + " |")
    return output


def _number_text(value: float) -> str:
    """Format a numeric value without hiding material precision."""

    if float(value).is_integer():
        return str(int(value))
    return f"{value:.6g}"


def _git_commit() -> str | None:
    """Return the current git commit hash if the repository is available."""

    try:
        completed = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
        )
    except (OSError, subprocess.CalledProcessError):
        return None
    return completed.stdout.strip() or None


if __name__ == "__main__":
    main()
