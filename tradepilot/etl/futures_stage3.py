"""Commodity futures stage 3 single-root quality card."""

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
from tradepilot.etl.models import StorageZone
from tradepilot.etl.storage import build_zone_path

_DEFAULT_ROOT_CODE = "M.DCE"
_CONTINUOUS_DATASET = "derived.futures_continuous_contract"
_INSTRUMENTS_DATASET = "reference.futures_instruments"
_REQUIRED_CONTINUOUS_COLUMNS = [
    "trade_date",
    "root_symbol",
    "active_contract",
    "raw_close",
    "adjusted_close",
    "continuous_return",
    "volume",
    "oi",
    "is_roll_day",
]
_REQUIRED_INSTRUMENT_COLUMNS = [
    "contract_code",
    "multiplier",
    "trade_unit",
    "quote_unit",
]
_TRADING_DAYS_PER_YEAR = 252
_HISTORY_TABLE_HEADERS = [
    "Root",
    "Rows",
    "Window",
    "Return rows",
    "Missing returns",
    "Missing rate",
    "Duplicate dates",
]
_ROLL_LIQUIDITY_TABLE_HEADERS = [
    "Rolls",
    "Abnormal roll returns",
    "Avg holding days",
    "Median volume",
    "Median OI",
    "Zero volume days",
    "Zero OI days",
]
_RETURN_TABLE_HEADERS = [
    "Ann return",
    "Ann volatility",
    "Max drawdown",
    "Max daily gain",
    "Max daily loss",
    "Extreme days",
]
_INTEGER_LOT_TABLE_HEADERS = [
    "Latest contract",
    "Latest close",
    "Multiplier",
    "Trade unit",
    "Quote unit",
    "One-lot notional",
    "Target notional",
    "Nearest lots",
    "Lot error",
    "Lot error %",
]


class QualityDecision(StrEnum):
    """Stage 3 quality-card decision values."""

    ACCEPT = "accept"
    OBSERVE = "observe"
    REJECT = "reject"


@dataclass(frozen=True)
class FuturesQualityCard:
    """Structured metrics for one commodity futures Stage 3 quality card."""

    generated_at: datetime
    code_version: str | None
    snapshot_id: str
    lakehouse_root: Path
    root_code: str
    row_count: int
    start_date: date
    end_date: date
    return_count: int
    return_missing_count: int
    return_missing_rate: float
    duplicate_trade_date_count: int
    roll_count: int
    abnormal_roll_count: int
    average_holding_days: float
    median_volume: float
    median_oi: float
    zero_volume_days: int
    zero_oi_days: int
    annualized_return: float
    annualized_volatility: float
    max_drawdown: float
    max_daily_gain: float
    max_daily_loss: float
    extreme_day_count: int
    latest_contract: str
    latest_close: float
    multiplier: float
    trade_unit: str
    quote_unit: str
    one_lot_notional: float
    portfolio_notional: float
    target_weight: float
    target_notional: float
    nearest_lots: int
    integer_lot_error: float
    integer_lot_error_pct: float
    peer_correlations: list[tuple[str, float]]
    decision: QualityDecision
    decision_reasons: list[str]


@click.command()
@click.option(
    "--lakehouse-root",
    type=click.Path(path_type=Path, file_okay=False),
    default=LAKEHOUSE_ROOT,
    show_default=True,
)
@click.option("--root-code", default=_DEFAULT_ROOT_CODE, show_default=True)
@click.option(
    "--portfolio-notional", type=float, default=1_000_000.0, show_default=True
)
@click.option("--target-weight", type=float, default=0.05, show_default=True)
@click.option(
    "--output",
    type=click.Path(path_type=Path, dir_okay=False),
    default=Path(
        "docs/futures-v2-design/reports/stage-3/quality-cards/"
        "commodity-futures-stage-3-m-quality-card.md"
    ),
    show_default=True,
)
@click.option(
    "--json-output",
    type=click.Path(path_type=Path, dir_okay=False),
    default=None,
    help="Optional structured JSON sidecar path for downstream stages.",
)
def main(
    lakehouse_root: Path,
    root_code: str,
    portfolio_notional: float,
    target_weight: float,
    output: Path,
    json_output: Path | None,
) -> None:
    """Build and write one Stage 3 commodity futures quality-card report."""

    card = build_quality_card(
        lakehouse_root=lakehouse_root,
        root_code=root_code,
        portfolio_notional=portfolio_notional,
        target_weight=target_weight,
    )
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(render_quality_card(card), encoding="utf-8")
    if json_output is not None:
        json_output.parent.mkdir(parents=True, exist_ok=True)
        json_output.write_text(render_quality_card_json(card), encoding="utf-8")
        click.echo(f"wrote {json_output}")
    click.echo(f"wrote {output}")
    click.echo(f"snapshot_id={card.snapshot_id}")
    click.echo(f"decision={card.decision.value}")


def build_quality_card(
    *,
    lakehouse_root: Path,
    root_code: str = _DEFAULT_ROOT_CODE,
    portfolio_notional: float = 1_000_000.0,
    target_weight: float = 0.05,
    min_return_rows: int = _TRADING_DAYS_PER_YEAR * 5,
    extreme_return_threshold: float = 0.05,
    abnormal_roll_return_threshold: float = 0.05,
) -> FuturesQualityCard:
    """Build one Stage 3 quality card from a Stage 2 continuous contract."""

    if portfolio_notional <= 0:
        raise ValueError("portfolio_notional must be positive")
    if target_weight <= 0:
        raise ValueError("target_weight must be positive")

    root_code = root_code.strip().upper()
    frame = _load_continuous_contract(
        lakehouse_root=lakehouse_root, root_code=root_code
    )
    instruments = _load_normalized_dataset(
        _INSTRUMENTS_DATASET,
        lakehouse_root,
        required_columns=_REQUIRED_INSTRUMENT_COLUMNS,
    )
    frame = frame.sort_values("trade_date").reset_index(drop=True)
    _validate_continuous_frame(frame=frame, root_code=root_code)

    returns = frame["continuous_return"]
    valid_returns = returns.dropna()
    roll_returns = frame.loc[
        frame["is_roll_day"].eq(True), "continuous_return"
    ].dropna()
    wealth = (1 + returns.fillna(0.0)).cumprod()
    drawdown = wealth / wealth.cummax() - 1
    latest = frame.iloc[-1]
    latest_contract = str(latest["active_contract"])
    instrument = _instrument_row(instruments=instruments, contract_code=latest_contract)
    latest_close = _positive_float(
        latest["raw_close"], f"{latest_contract} latest close"
    )
    multiplier = _positive_float(
        instrument["multiplier"], f"{latest_contract} multiplier"
    )
    one_lot_notional = latest_close * multiplier
    target_notional = portfolio_notional * target_weight
    nearest_lots = max(1, round(target_notional / one_lot_notional))
    rounded_notional = nearest_lots * one_lot_notional
    integer_lot_error = rounded_notional - target_notional
    return_count = len(valid_returns)
    return_missing_count = int(returns.isna().sum())
    roll_count = int(frame["is_roll_day"].sum())
    abnormal_roll_count = int(
        roll_returns.abs().ge(abnormal_roll_return_threshold).sum()
    )
    zero_volume_days = int(frame["volume"].le(0).sum())
    zero_oi_days = int(frame["oi"].le(0).sum())
    duplicate_trade_date_count = int(frame["trade_date"].duplicated().sum())
    reject_reasons = _reject_reasons(
        return_count=return_count,
        min_return_rows=min_return_rows,
        return_missing_count=return_missing_count,
        duplicate_trade_date_count=duplicate_trade_date_count,
        non_positive_adjusted_count=int(frame["adjusted_close"].le(0).sum()),
        all_zero_volume=zero_volume_days == len(frame),
        all_zero_oi=zero_oi_days == len(frame),
    )
    observe_reasons = _observe_reasons(
        abnormal_roll_count=abnormal_roll_count,
        zero_volume_days=zero_volume_days,
        zero_oi_days=zero_oi_days,
        max_drawdown=float(drawdown.min()),
    )
    decision = QualityDecision.ACCEPT
    decision_reasons = ["meets fixed Stage 3 quality-card thresholds"]
    if reject_reasons:
        decision = QualityDecision.REJECT
        decision_reasons = reject_reasons
    elif observe_reasons:
        decision = QualityDecision.OBSERVE
        decision_reasons = observe_reasons

    code_version = _git_commit()
    return FuturesQualityCard(
        generated_at=datetime.now(tz=UTC),
        code_version=code_version,
        snapshot_id=_snapshot_id(
            code_version=code_version,
            lakehouse_root=lakehouse_root,
            root_code=root_code,
        ),
        lakehouse_root=lakehouse_root,
        root_code=root_code,
        row_count=len(frame),
        start_date=frame["trade_date"].min(),
        end_date=frame["trade_date"].max(),
        return_count=return_count,
        return_missing_count=return_missing_count,
        return_missing_rate=return_missing_count / len(frame),
        duplicate_trade_date_count=duplicate_trade_date_count,
        roll_count=roll_count,
        abnormal_roll_count=abnormal_roll_count,
        average_holding_days=0.0 if roll_count == 0 else len(frame) / (roll_count + 1),
        median_volume=float(frame["volume"].median()),
        median_oi=float(frame["oi"].median()),
        zero_volume_days=zero_volume_days,
        zero_oi_days=zero_oi_days,
        annualized_return=_annualized_return(valid_returns),
        annualized_volatility=float(
            valid_returns.std() * (_TRADING_DAYS_PER_YEAR**0.5)
        ),
        max_drawdown=float(drawdown.min()),
        max_daily_gain=float(valid_returns.max()),
        max_daily_loss=float(valid_returns.min()),
        extreme_day_count=int(valid_returns.abs().ge(extreme_return_threshold).sum()),
        latest_contract=latest_contract,
        latest_close=latest_close,
        multiplier=multiplier,
        trade_unit=str(instrument.get("trade_unit", "")),
        quote_unit=str(instrument.get("quote_unit", "")),
        one_lot_notional=one_lot_notional,
        portfolio_notional=portfolio_notional,
        target_weight=target_weight,
        target_notional=target_notional,
        nearest_lots=nearest_lots,
        integer_lot_error=integer_lot_error,
        integer_lot_error_pct=abs(integer_lot_error) / target_notional,
        peer_correlations=_peer_correlations(
            lakehouse_root=lakehouse_root,
            root_code=root_code,
            frame=frame,
        ),
        decision=decision,
        decision_reasons=decision_reasons,
    )


def render_quality_card(
    card: FuturesQualityCard, *, caveats: list[str] | None = None
) -> str:
    """Render one Stage 3 quality card as markdown."""

    lines: list[str] = []
    lines.append(f"# TradePilot 商品期货阶段 3：{card.root_code} 单品种质量卡")
    lines.append("")
    lines.append(f"Generated at: `{card.generated_at.isoformat()}`")
    lines.append(f"Code version: `{card.code_version or 'unknown'}`")
    lines.append(f"Snapshot id: `{card.snapshot_id}`")
    lines.append(f"Lakehouse root: `{card.lakehouse_root}`")
    lines.append("")
    lines.append("## Scope")
    lines.append("")
    lines.append(
        "本报告只覆盖 Stage 3 的单品种质量与可研究性筛选；不构建商品篮子，"
        "不运行 ETF 基线增量回测。绩效口径沿用 Stage 2 冻结的 `continuous_return`。"
    )
    if caveats:
        lines.append("")
        lines.append("## Input Caveats")
        lines.append("")
        for caveat in caveats:
            lines.append(f"- {caveat}")
    lines.append("")
    lines.append("## History And Continuity")
    lines.append("")
    lines.extend(
        _markdown_table(
            headers=_HISTORY_TABLE_HEADERS,
            rows=[_history_table_row(card)],
        )
    )
    lines.append("")
    lines.append("## Roll And Liquidity")
    lines.append("")
    lines.extend(
        _markdown_table(
            headers=_ROLL_LIQUIDITY_TABLE_HEADERS,
            rows=[_roll_liquidity_table_row(card)],
        )
    )
    lines.append("")
    lines.append("## Return And Drawdown")
    lines.append("")
    lines.extend(
        _markdown_table(
            headers=_RETURN_TABLE_HEADERS,
            rows=[_return_table_row(card)],
        )
    )
    lines.append("")
    lines.append("## Integer-Lot Sizing Hint")
    lines.append("")
    lines.extend(
        _markdown_table(
            headers=_INTEGER_LOT_TABLE_HEADERS,
            rows=[_integer_lot_table_row(card)],
        )
    )
    lines.append("")
    lines.append("## Peer Correlation")
    lines.append("")
    lines.extend(
        _markdown_table(
            headers=["Peer", "Correlation"],
            rows=[
                [peer, f"{correlation:.4f}"]
                for peer, correlation in card.peer_correlations
            ],
        )
    )
    lines.append("")
    lines.append("## Stage 3 Decision")
    lines.append("")
    lines.append(f"结论：`{card.decision.value}`。")
    for reason in card.decision_reasons:
        lines.append(f"- {reason}")
    lines.append("")
    lines.append(
        "该结论只说明单品种是否可进入后续候选池讨论；正式商品篮子仍需在所有候选"
        "逐一质量卡完成后冻结权重规则。"
    )
    lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def render_quality_card_json(
    card: FuturesQualityCard, *, caveats: list[str] | None = None
) -> str:
    """Render one Stage 3 quality card as structured JSON."""

    payload = {
        "schema_version": 1,
        "stage": 3,
        "generated_at": card.generated_at.isoformat(),
        "code_version": card.code_version,
        "snapshot_id": card.snapshot_id,
        "lakehouse_root": str(card.lakehouse_root),
        "root_code": card.root_code,
        "decision": card.decision.value,
        "decision_reasons": card.decision_reasons,
        "caveats": caveats or [],
        "row_count": card.row_count,
        "start_date": card.start_date.isoformat(),
        "end_date": card.end_date.isoformat(),
        "return_count": card.return_count,
        "return_missing_count": card.return_missing_count,
        "roll_count": card.roll_count,
        "abnormal_roll_count": card.abnormal_roll_count,
        "annualized_return": card.annualized_return,
        "annualized_volatility": card.annualized_volatility,
        "max_drawdown": card.max_drawdown,
    }
    return json.dumps(payload, ensure_ascii=False, indent=2, allow_nan=True) + "\n"


def _load_continuous_contract(*, lakehouse_root: Path, root_code: str) -> pd.DataFrame:
    """Load one Stage 2 derived continuous-contract partition."""

    path = (
        build_zone_path(_CONTINUOUS_DATASET, StorageZone.DERIVED, lakehouse_root)
        / root_code
        / "part-00000.parquet"
    )
    if not path.exists():
        raise ValueError(f"missing Stage 2 continuous contract for {root_code}")
    frame = pd.read_parquet(path)
    missing_columns = [
        column for column in _REQUIRED_CONTINUOUS_COLUMNS if column not in frame
    ]
    if missing_columns:
        raise ValueError(
            f"continuous contract {root_code} missing columns: "
            + ", ".join(missing_columns)
        )
    frame["trade_date"] = pd.to_datetime(frame["trade_date"], errors="coerce").dt.date
    return frame


def _validate_continuous_frame(*, frame: pd.DataFrame, root_code: str) -> None:
    """Validate a continuous-contract frame before metric calculation."""

    if frame.empty:
        raise ValueError(f"continuous contract {root_code} is empty")
    if not frame["root_symbol"].eq(root_code).all():
        raise ValueError(f"continuous contract {root_code} has mixed root_symbol rows")
    required_full_columns = [
        "trade_date",
        "raw_close",
        "adjusted_close",
        "volume",
        "oi",
    ]
    if frame[required_full_columns].isna().any(axis=None):
        raise ValueError(f"continuous contract {root_code} has missing core fields")
    if frame["continuous_return"].iloc[1:].isna().any():
        raise ValueError(f"continuous contract {root_code} has missing core fields")
    if frame[["raw_close", "adjusted_close"]].le(0).any(axis=None):
        raise ValueError(f"continuous contract {root_code} has non-positive prices")
    if frame[["volume", "oi"]].lt(0).any(axis=None):
        raise ValueError(f"continuous contract {root_code} has negative volume/oi")


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
    return frame


def _instrument_row(*, instruments: pd.DataFrame, contract_code: str) -> pd.Series:
    """Return unique instrument metadata for one concrete contract."""

    rows = instruments[instruments["contract_code"].eq(contract_code)]
    if rows.empty:
        raise ValueError(f"missing instrument metadata for {contract_code}")
    unique_rows = rows.drop_duplicates(
        ["contract_code", "multiplier", "trade_unit", "quote_unit"]
    )
    if len(unique_rows) != 1:
        raise ValueError(f"instrument metadata is not unique for {contract_code}")
    return unique_rows.iloc[0]


def _peer_correlations(
    *, lakehouse_root: Path, root_code: str, frame: pd.DataFrame
) -> list[tuple[str, float]]:
    """Return same-lakehouse futures peer return correlations when available."""

    dataset_root = build_zone_path(
        _CONTINUOUS_DATASET, StorageZone.DERIVED, lakehouse_root
    )
    if not dataset_root.exists():
        return []
    correlations: list[tuple[str, float]] = []
    own_returns = frame[["trade_date", "continuous_return"]].rename(
        columns={"continuous_return": "own_return"}
    )
    for path in sorted(dataset_root.glob("*/part-00000.parquet")):
        peer_root = path.parent.name
        if peer_root == root_code:
            continue
        peer = pd.read_parquet(path)
        if "trade_date" not in peer or "continuous_return" not in peer:
            continue
        peer["trade_date"] = pd.to_datetime(peer["trade_date"], errors="coerce").dt.date
        merged = own_returns.merge(
            peer[["trade_date", "continuous_return"]],
            on="trade_date",
            how="inner",
        ).dropna()
        if len(merged) < _TRADING_DAYS_PER_YEAR:
            continue
        correlations.append(
            (peer_root, float(merged["own_return"].corr(merged["continuous_return"])))
        )
    return correlations


def _reject_reasons(
    *,
    return_count: int,
    min_return_rows: int,
    return_missing_count: int,
    duplicate_trade_date_count: int,
    non_positive_adjusted_count: int,
    all_zero_volume: bool,
    all_zero_oi: bool,
) -> list[str]:
    """Return blocking Stage 3 quality-card reasons."""

    reasons: list[str] = []
    if return_count < min_return_rows:
        reasons.append(
            f"return history has {return_count} rows, below minimum {min_return_rows}"
        )
    if return_missing_count > 1:
        reasons.append(f"continuous_return has {return_missing_count} missing values")
    if duplicate_trade_date_count > 0:
        reasons.append(f"duplicate trade_date rows: {duplicate_trade_date_count}")
    if non_positive_adjusted_count > 0:
        reasons.append(
            f"non-positive adjusted_close rows: {non_positive_adjusted_count}"
        )
    if all_zero_volume:
        reasons.append("volume is zero for the full sample")
    if all_zero_oi:
        reasons.append("OI is zero for the full sample")
    return reasons


def _observe_reasons(
    *,
    abnormal_roll_count: int,
    zero_volume_days: int,
    zero_oi_days: int,
    max_drawdown: float,
) -> list[str]:
    """Return non-blocking Stage 3 caution reasons."""

    reasons: list[str] = []
    if abnormal_roll_count > 0:
        reasons.append(f"abnormal roll return days: {abnormal_roll_count}")
    if zero_volume_days > 0:
        reasons.append(f"zero volume days: {zero_volume_days}")
    if zero_oi_days > 0:
        reasons.append(f"zero OI days: {zero_oi_days}")
    if max_drawdown < -0.80:
        reasons.append(f"max drawdown is {max_drawdown:.4%}")
    return reasons


def _annualized_return(valid_returns: pd.Series) -> float:
    """Return geometric annualized return for a daily return series."""

    if valid_returns.empty:
        return 0.0
    total_return = float((1 + valid_returns).prod())
    if total_return <= 0:
        return math.nan
    return total_return ** (_TRADING_DAYS_PER_YEAR / len(valid_returns)) - 1


def _positive_float(value: object, field_name: str) -> float:
    """Return value as a positive float or raise a diagnostic error."""

    if pd.isna(value):
        raise ValueError(f"{field_name} must be positive")
    number = float(value)
    if number <= 0:
        raise ValueError(f"{field_name} must be positive")
    return number


def _snapshot_id(
    *, code_version: str | None, lakehouse_root: Path, root_code: str
) -> str:
    """Build one deterministic snapshot identifier from Stage 3 inputs."""

    digest = hashlib.sha256()
    for item in (code_version or "unknown", lakehouse_root.as_posix(), root_code):
        digest.update(item.encode("utf-8"))
    continuous_path = (
        build_zone_path(_CONTINUOUS_DATASET, StorageZone.DERIVED, lakehouse_root)
        / root_code
        / "part-00000.parquet"
    )
    if continuous_path.exists():
        digest.update(continuous_path.relative_to(lakehouse_root).as_posix().encode())
        digest.update(_sha256_file(continuous_path).encode("utf-8"))
    instruments_root = build_zone_path(
        _INSTRUMENTS_DATASET, StorageZone.NORMALIZED, lakehouse_root
    )
    for path in sorted(instruments_root.rglob("*.parquet")):
        digest.update(path.relative_to(lakehouse_root).as_posix().encode("utf-8"))
        digest.update(_sha256_file(path).encode("utf-8"))
    return digest.hexdigest()[:16]


def _sha256_file(path: Path) -> str:
    """Return the SHA-256 hash of one file."""

    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _history_table_row(card: FuturesQualityCard) -> list[str]:
    """Return the history table row for one quality card."""

    return [
        card.root_code,
        str(card.row_count),
        f"{card.start_date.isoformat()} .. {card.end_date.isoformat()}",
        str(card.return_count),
        str(card.return_missing_count),
        f"{card.return_missing_rate:.4%}",
        str(card.duplicate_trade_date_count),
    ]


def _roll_liquidity_table_row(card: FuturesQualityCard) -> list[str]:
    """Return the roll and liquidity table row for one quality card."""

    return [
        str(card.roll_count),
        str(card.abnormal_roll_count),
        _number_text(card.average_holding_days),
        _number_text(card.median_volume),
        _number_text(card.median_oi),
        str(card.zero_volume_days),
        str(card.zero_oi_days),
    ]


def _return_table_row(card: FuturesQualityCard) -> list[str]:
    """Return the return and drawdown table row for one quality card."""

    return [
        _percent_text(card.annualized_return),
        _percent_text(card.annualized_volatility),
        _percent_text(card.max_drawdown),
        _percent_text(card.max_daily_gain),
        _percent_text(card.max_daily_loss),
        str(card.extreme_day_count),
    ]


def _integer_lot_table_row(card: FuturesQualityCard) -> list[str]:
    """Return the integer-lot sizing table row for one quality card."""

    return [
        card.latest_contract,
        _number_text(card.latest_close),
        _number_text(card.multiplier),
        card.trade_unit,
        card.quote_unit,
        _number_text(card.one_lot_notional),
        _number_text(card.target_notional),
        str(card.nearest_lots),
        _number_text(card.integer_lot_error),
        _percent_text(card.integer_lot_error_pct),
    ]


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


def _percent_text(value: float) -> str:
    """Format a decimal number as percentage text."""

    if math.isnan(value):
        return "NaN"
    return f"{value:.4%}"


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
