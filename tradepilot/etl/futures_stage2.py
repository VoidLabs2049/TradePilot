"""Commodity futures stage 2 continuous-contract construction."""

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
from tradepilot.etl.storage import (
    ParquetWriteResult,
    build_zone_path,
    write_dataset_parquet,
)

_DEFAULT_ROOT_CODE = "M.DCE"
_OUTPUT_DATASET = "derived.futures_continuous_contract"
_REQUIRED_DAILY_FIELDS = ("close", "settle", "volume", "oi")
_SOURCE_DATASETS = (
    "market.futures_mapping",
    "market.futures_contract_daily",
)


@dataclass(frozen=True)
class Stage2Report:
    """Structured data backing the stage 2 continuous-contract report."""

    generated_at: datetime
    code_version: str | None
    snapshot_id: str
    lakehouse_root: Path
    root_code: str
    output_path: Path
    row_count: int
    start_date: date
    end_date: date
    roll_count: int
    max_roll_close_gap_abs: float
    max_roll_settle_gap_abs: float
    min_adjusted_close: float
    min_adjusted_settle: float
    max_roll_return_abs: float
    first_roll_date: date | None
    last_roll_date: date | None
    start_date_override: date | None = None


@click.command()
@click.option(
    "--lakehouse-root",
    type=click.Path(path_type=Path, file_okay=False),
    default=LAKEHOUSE_ROOT,
    show_default=True,
)
@click.option("--root-code", default=_DEFAULT_ROOT_CODE, show_default=True)
@click.option(
    "--start-date",
    "start_date_text",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    default=None,
    help="Optional inclusive trade-date start for an explicitly truncated sample.",
)
@click.option(
    "--output",
    type=click.Path(path_type=Path, dir_okay=False),
    default=Path(
        "docs/futures-v2-design/commodity-futures-stage-2-m-continuous-contract-report.md"
    ),
    show_default=True,
)
def main(
    lakehouse_root: Path,
    root_code: str,
    start_date_text: datetime | None,
    output: Path,
) -> None:
    """Build the stage 2 M continuous contract and write its audit report."""

    start_date = None if start_date_text is None else start_date_text.date()
    frame = build_continuous_contract(
        lakehouse_root=lakehouse_root,
        root_code=root_code,
        start_date=start_date,
    )
    write_result = write_continuous_contract(
        frame=frame,
        lakehouse_root=lakehouse_root,
        root_code=root_code,
    )
    report = build_stage2_report(
        frame=frame,
        lakehouse_root=lakehouse_root,
        root_code=root_code,
        output_path=write_result.path,
        start_date=start_date,
    )
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(render_stage2_report(report, frame), encoding="utf-8")
    click.echo(f"wrote {output}")
    click.echo(f"wrote {write_result.relative_path}")
    click.echo(f"snapshot_id={report.snapshot_id}")
    click.echo(f"rows={report.row_count} rolls={report.roll_count}")


def build_continuous_contract(
    *,
    lakehouse_root: Path,
    root_code: str = _DEFAULT_ROOT_CODE,
    start_date: date | None = None,
) -> pd.DataFrame:
    """Build one ratio back-adjusted continuous contract for a root symbol."""

    root_code = root_code.strip().upper()
    mapping = _load_normalized_dataset(
        "market.futures_mapping",
        lakehouse_root,
        required_columns=["root_code", "trade_date", "active_contract", "raw_batch_id"],
    )
    daily = _load_normalized_dataset(
        "market.futures_contract_daily",
        lakehouse_root,
        required_columns=[
            "contract_code",
            "trade_date",
            "close",
            "settle",
            "volume",
            "oi",
            "raw_batch_id",
        ],
    )
    root_mapping = (
        mapping[mapping["root_code"].eq(root_code)]
        .sort_values("trade_date")
        .reset_index(drop=True)
    )
    if start_date is not None:
        root_mapping = root_mapping[root_mapping["trade_date"].ge(start_date)]
        root_mapping = root_mapping.reset_index(drop=True)
    if root_mapping.empty:
        raise ValueError(f"no mapping rows for {root_code}")
    if root_mapping.duplicated(["root_code", "trade_date"]).any():
        raise ValueError(f"duplicate mapping rows for {root_code}")
    if daily.duplicated(["contract_code", "trade_date"]).any():
        raise ValueError("duplicate futures daily rows by contract_code/trade_date")

    active = root_mapping.merge(
        daily,
        left_on=["active_contract", "trade_date"],
        right_on=["contract_code", "trade_date"],
        how="left",
        suffixes=("_mapping", "_daily"),
    )
    if active["contract_code"].isna().any():
        raise ValueError(f"{root_code} mapping rows without matching daily bars")
    if active[list(_REQUIRED_DAILY_FIELDS)].isna().any(axis=None):
        raise ValueError(f"{root_code} active rows have missing close/settle/volume/oi")
    _require_positive_columns(active, ["close", "settle"], f"{root_code} active rows")

    roll_rows = _build_roll_rows(root_mapping=root_mapping, daily=daily)
    frame = active[
        [
            "trade_date",
            "root_code",
            "active_contract",
            "close",
            "settle",
            "volume",
            "oi",
            "raw_batch_id_mapping",
            "raw_batch_id_daily",
        ]
    ].rename(
        columns={
            "root_code": "root_symbol",
            "close": "raw_close",
            "settle": "raw_settle",
            "raw_batch_id_daily": "source_run_id",
        }
    )
    frame["is_roll_day"] = frame["trade_date"].isin(
        {row["trade_date"] for row in roll_rows}
    )
    frame["roll_from"] = pd.NA
    frame["roll_to"] = pd.NA
    frame["roll_gap"] = 0.0
    frame["settle_roll_gap"] = 0.0
    frame["roll_ratio"] = 1.0
    frame["settle_roll_ratio"] = 1.0
    for row in roll_rows:
        mask = frame["trade_date"].eq(row["trade_date"])
        frame.loc[mask, "roll_from"] = row["roll_from"]
        frame.loc[mask, "roll_to"] = row["roll_to"]
        frame.loc[mask, "roll_gap"] = row["close_gap"]
        frame.loc[mask, "settle_roll_gap"] = row["settle_gap"]
        frame.loc[mask, "roll_ratio"] = row["close_ratio"]
        frame.loc[mask, "settle_roll_ratio"] = row["settle_ratio"]

    frame["cumulative_roll_adjustment"] = _future_ratio_products(
        frame["trade_date"], roll_rows, "close_ratio"
    )
    frame["cumulative_settle_roll_adjustment"] = _future_ratio_products(
        frame["trade_date"], roll_rows, "settle_ratio"
    )
    frame["adjusted_close"] = frame["raw_close"] * frame["cumulative_roll_adjustment"]
    frame["adjusted_settle"] = (
        frame["raw_settle"] * frame["cumulative_settle_roll_adjustment"]
    )
    frame["continuous_return"] = frame["adjusted_close"].pct_change()
    frame["settle_return_audit"] = frame["adjusted_settle"].pct_change()
    frame["naive_return"] = frame["raw_close"].pct_change()
    frame["adjustment_method"] = "ratio_back_adjustment"
    frame["performance_price_field"] = "adjusted_close"
    frame["audit_settle_field"] = "adjusted_settle"
    frame["root_symbol"] = root_code
    return frame[
        [
            "trade_date",
            "root_symbol",
            "active_contract",
            "raw_close",
            "raw_settle",
            "adjusted_close",
            "adjusted_settle",
            "continuous_return",
            "settle_return_audit",
            "naive_return",
            "volume",
            "oi",
            "is_roll_day",
            "roll_from",
            "roll_to",
            "roll_gap",
            "settle_roll_gap",
            "roll_ratio",
            "settle_roll_ratio",
            "cumulative_roll_adjustment",
            "cumulative_settle_roll_adjustment",
            "source_run_id",
            "raw_batch_id_mapping",
            "adjustment_method",
            "performance_price_field",
            "audit_settle_field",
        ]
    ].reset_index(drop=True)


def write_continuous_contract(
    *, frame: pd.DataFrame, lakehouse_root: Path, root_code: str
) -> ParquetWriteResult:
    """Write the continuous-contract frame to the derived lakehouse zone."""

    return write_dataset_parquet(
        frame=frame,
        dataset_name=_OUTPUT_DATASET,
        zone=StorageZone.DERIVED,
        partition_parts=[("root_symbol", root_code.strip().upper())],
        lakehouse_root=lakehouse_root,
    )


def build_stage2_report(
    *,
    frame: pd.DataFrame,
    lakehouse_root: Path,
    root_code: str,
    output_path: Path,
    start_date: date | None = None,
) -> Stage2Report:
    """Build summary metrics for the continuous-contract artifact."""

    if frame.empty:
        raise ValueError("continuous contract frame is empty")
    roll_frame = frame[frame["is_roll_day"].eq(True)]
    code_version = _git_commit()
    return Stage2Report(
        generated_at=datetime.now(tz=UTC),
        code_version=code_version,
        snapshot_id=_snapshot_id(
            code_version=code_version,
            lakehouse_root=lakehouse_root,
            root_code=root_code,
        ),
        lakehouse_root=lakehouse_root,
        root_code=root_code.strip().upper(),
        output_path=output_path,
        row_count=len(frame),
        start_date=frame["trade_date"].min(),
        end_date=frame["trade_date"].max(),
        roll_count=len(roll_frame),
        max_roll_close_gap_abs=_max_abs_or_zero(roll_frame, "roll_gap"),
        max_roll_settle_gap_abs=_max_abs_or_zero(roll_frame, "settle_roll_gap"),
        min_adjusted_close=float(frame["adjusted_close"].min()),
        min_adjusted_settle=float(frame["adjusted_settle"].min()),
        max_roll_return_abs=_max_abs_or_zero(roll_frame, "continuous_return"),
        first_roll_date=None if roll_frame.empty else roll_frame["trade_date"].min(),
        last_roll_date=None if roll_frame.empty else roll_frame["trade_date"].max(),
        start_date_override=start_date,
    )


def render_stage2_report(report: Stage2Report, frame: pd.DataFrame) -> str:
    """Render the stage 2 report as markdown."""

    roll_rows = frame[frame["is_roll_day"].eq(True)].copy()
    sample_roll_rows = roll_rows.head(10)
    lines: list[str] = []
    lines.append(f"# TradePilot 商品期货阶段 2：{report.root_code} 连续合约构建报告")
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
        f"本报告只覆盖 Stage 2 的单品种主力连续合约：`{report.root_code}`。"
        "不扩展其他品种，不构建商品篮子，不做 ETF 基线增量回测。"
    )
    lines.append("")
    lines.append("## Frozen Method")
    lines.append("")
    lines.append("- 主力选择：逐日遵循冻结的 `market.futures_mapping`。")
    lines.append("- 复权公式：比值法后向复权。")
    if report.start_date_override is not None:
        lines.append(
            "- 样本截断：因截断日前存在无法同日定位新旧合约价格的换月，"
            f"本报告只使用 `{report.start_date_override.isoformat()}` 起的映射行。"
        )
    lines.append(
        "- `adjusted_close`：每个换月日用 `new_close / old_close` 调整所有更早历史段。"
    )
    lines.append(
        "- `adjusted_settle`：每个换月日用 `new_settle / old_settle` 调整所有更早历史段。"
    )
    lines.append("- 绝对 `roll_gap` / `settle_roll_gap` 仍保留为换月价差审计字段。")
    lines.append("- 绩效主口径：`continuous_return = adjusted_close.pct_change()`。")
    lines.append(
        "- 审计对照口径：`settle_return_audit = adjusted_settle.pct_change()`。"
    )
    lines.append("")
    lines.append("## Summary")
    lines.append("")
    lines.extend(
        _markdown_table(
            headers=[
                "Root",
                "Rows",
                "Window",
                "Rolls",
                "Max roll close gap",
                "Max roll settle gap",
                "Min adjusted close",
                "Min adjusted settle",
                "Max abs roll return",
            ],
            rows=[
                [
                    report.root_code,
                    str(report.row_count),
                    f"{report.start_date.isoformat()} .. {report.end_date.isoformat()}",
                    str(report.roll_count),
                    _number_text(report.max_roll_close_gap_abs),
                    _number_text(report.max_roll_settle_gap_abs),
                    _number_text(report.min_adjusted_close),
                    _number_text(report.min_adjusted_settle),
                    f"{report.max_roll_return_abs:.4%}",
                ]
            ],
        )
    )
    lines.append("")
    lines.append("## Roll Sample")
    lines.append("")
    lines.extend(
        _markdown_table(
            headers=[
                "Trade date",
                "From",
                "To",
                "Roll gap",
                "Settle roll gap",
                "Roll ratio",
                "Naive return",
                "Continuous return",
            ],
            rows=[
                [
                    row["trade_date"].isoformat(),
                    str(row["roll_from"]),
                    str(row["roll_to"]),
                    _number_text(float(row["roll_gap"])),
                    _number_text(float(row["settle_roll_gap"])),
                    _number_text(float(row["roll_ratio"])),
                    f"{float(row['naive_return']):.4%}",
                    f"{float(row['continuous_return']):.4%}",
                ]
                for _, row in sample_roll_rows.iterrows()
            ],
        )
    )
    lines.append("")
    lines.append("## Stage 2 Decision")
    lines.append("")
    lines.append(
        f"结论：`pass`。`{report.root_code}` 连续序列已按冻结的比值法后向复权生成，"
        "换月日绩效收益使用 `adjusted_close` 复算，天真拼接收益只作为诊断列保留。"
    )
    lines.append("")
    lines.append(
        "该产物仍是单品种 Stage 2 样本，不代表其他品种已通过连续合约验收，"
        "也不代表商品篮子或 ETF 基线增量回测可以直接开始。"
    )
    lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def _build_roll_rows(
    *, root_mapping: pd.DataFrame, daily: pd.DataFrame
) -> list[dict[str, Any]]:
    """Return roll metadata rows with same-day old/new close and settle gaps."""

    rolls: list[dict[str, Any]] = []
    for index in range(1, len(root_mapping)):
        previous_contract = str(root_mapping.loc[index - 1, "active_contract"])
        current_contract = str(root_mapping.loc[index, "active_contract"])
        if previous_contract == current_contract:
            continue
        trade_date = root_mapping.loc[index, "trade_date"]
        rows = daily[
            daily["trade_date"].eq(trade_date)
            & daily["contract_code"].isin([previous_contract, current_contract])
        ]
        if len(rows) != 2:
            raise ValueError(
                f"roll {trade_date} requires old/new daily rows for "
                f"{previous_contract}->{current_contract}"
            )
        if rows[list(_REQUIRED_DAILY_FIELDS)].isna().any(axis=None):
            raise ValueError(f"roll {trade_date} has missing core daily fields")
        old_rows = rows[rows["contract_code"].eq(previous_contract)]
        new_rows = rows[rows["contract_code"].eq(current_contract)]
        if len(old_rows) != 1 or len(new_rows) != 1:
            raise ValueError(
                f"roll {trade_date} rows are not unique for "
                f"{previous_contract}->{current_contract}"
            )
        old = old_rows.iloc[0]
        new = new_rows.iloc[0]
        old_close = _positive_float(
            old["close"], f"{previous_contract} close on {trade_date}"
        )
        new_close = _positive_float(
            new["close"], f"{current_contract} close on {trade_date}"
        )
        old_settle = _positive_float(
            old["settle"], f"{previous_contract} settle on {trade_date}"
        )
        new_settle = _positive_float(
            new["settle"], f"{current_contract} settle on {trade_date}"
        )
        rolls.append(
            {
                "trade_date": trade_date,
                "roll_from": previous_contract,
                "roll_to": current_contract,
                "close_gap": new_close - old_close,
                "settle_gap": new_settle - old_settle,
                "close_ratio": new_close / old_close,
                "settle_ratio": new_settle / old_settle,
            }
        )
    return rolls


def _future_ratio_products(
    trade_dates: pd.Series, roll_rows: list[dict[str, Any]], key: str
) -> pd.Series:
    """Return future roll-ratio products for each trade date."""

    if not roll_rows:
        return pd.Series([1.0] * len(trade_dates), index=trade_dates.index)

    rolls = pd.DataFrame(roll_rows).sort_values("trade_date").reset_index(drop=True)
    future_products = rolls[key].astype(float).iloc[::-1].cumprod().iloc[
        ::-1
    ].tolist() + [1.0]
    roll_dates = pd.to_datetime(rolls["trade_date"]).to_numpy()
    positions = roll_dates.searchsorted(
        pd.to_datetime(trade_dates).to_numpy(), side="right"
    )
    return pd.Series(
        [future_products[int(position)] for position in positions],
        index=trade_dates.index,
    )


def _load_normalized_dataset(
    dataset_name: str, lakehouse_root: Path, *, required_columns: list[str]
) -> pd.DataFrame:
    """Load one normalized parquet dataset from lakehouse partitions."""

    paths = sorted(
        build_zone_path(dataset_name, StorageZone.NORMALIZED, lakehouse_root).rglob(
            "*.parquet"
        )
    )
    if not paths:
        raise ValueError(f"missing normalized dataset {dataset_name}")
    frame = pd.concat([pd.read_parquet(path) for path in paths], ignore_index=True)
    if frame.empty:
        raise ValueError(f"empty normalized dataset {dataset_name}")
    missing_columns = [column for column in required_columns if column not in frame]
    if missing_columns:
        raise ValueError(
            f"normalized dataset {dataset_name} missing columns: "
            + ", ".join(missing_columns)
        )
    for column in ("trade_date", "list_date", "delist_date"):
        if column in frame.columns:
            frame[column] = pd.to_datetime(frame[column], errors="coerce").dt.date
    return frame


def _snapshot_id(
    *, code_version: str | None, lakehouse_root: Path, root_code: str
) -> str:
    """Build one deterministic snapshot identifier from current inputs."""

    digest = hashlib.sha256()
    for item in (code_version or "unknown", lakehouse_root.as_posix(), root_code):
        digest.update(item.encode("utf-8"))
    for dataset_name in _SOURCE_DATASETS:
        digest.update(dataset_name.encode("utf-8"))
        for path in sorted(
            build_zone_path(dataset_name, StorageZone.NORMALIZED, lakehouse_root).rglob(
                "*.parquet"
            )
        ):
            digest.update(path.relative_to(lakehouse_root).as_posix().encode("utf-8"))
            digest.update(_sha256_file(path).encode("utf-8"))
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


def _max_abs_or_zero(frame: pd.DataFrame, column: str) -> float:
    """Return max absolute numeric value for a column, or zero for no data."""

    if frame.empty:
        return 0.0
    value = frame[column].dropna().abs().max()
    return 0.0 if pd.isna(value) else float(value)


def _require_positive_columns(
    frame: pd.DataFrame, columns: list[str], context: str
) -> None:
    """Require all values in selected columns to be positive."""

    for column in columns:
        if not frame[column].gt(0).all():
            raise ValueError(f"{context} {column} must be positive")


def _positive_float(value: object, field_name: str) -> float:
    """Return value as a positive float or raise a diagnostic error."""

    if pd.isna(value):
        raise ValueError(f"{field_name} must be positive")
    number = float(value)
    if number <= 0:
        raise ValueError(f"{field_name} must be positive")
    return number


def _sha256_file(path: Path) -> str:
    """Return the SHA-256 hash of one file."""

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


if __name__ == "__main__":
    main()
