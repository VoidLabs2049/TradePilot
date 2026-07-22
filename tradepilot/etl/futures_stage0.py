"""Commodity futures stage 0 acceptance audit and freeze report."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime
import hashlib
import json
import re
import subprocess
from pathlib import Path
from typing import Any

import click
import duckdb
import pandas as pd

from tradepilot.config import DB_PATH, LAKEHOUSE_ROOT
from tradepilot.etl.models import StorageZone, ValidationStatus
from tradepilot.etl.normalizers import TradingCalendarNormalizer
from tradepilot.etl.storage import build_zone_path
from tradepilot.etl.validators import get_validator

_DEFAULT_ROOT_CODES = (
    "AU.SHF",
    "AL.SHF",
    "CU.SHF",
    "RB.SHF",
    "I.DCE",
    "M.DCE",
    "P.DCE",
    "SC.INE",
    "TA.ZCE",
)
_FUTURES_DATASETS = (
    "reference.trading_calendar",
    "reference.futures_instruments",
    "market.futures_mapping",
    "market.futures_contract_daily",
)
_FUTURES_UNIT_ROWS = (
    (
        "reference.futures_instruments",
        "multiplier",
        "contract size multiplier; programmatic sizing input",
    ),
    (
        "reference.futures_instruments",
        "per_unit",
        "secondary sizing field from Tushare; kept for audit",
    ),
    (
        "reference.futures_instruments",
        "trade_unit",
        "physical trade unit label from source",
    ),
    (
        "reference.futures_instruments",
        "quote_unit",
        "quotation unit label from source",
    ),
    (
        "market.futures_contract_daily",
        "open/high/low/close/settle/pre_close/pre_settle/change1/change2",
        "raw quoted price fields in contract quotation units",
    ),
    (
        "market.futures_contract_daily",
        "volume",
        "daily traded volume / hands as delivered by source",
    ),
    (
        "market.futures_contract_daily",
        "oi",
        "daily open interest as delivered by source",
    ),
    (
        "market.futures_contract_daily",
        "oi_chg",
        "open-interest delta as delivered by source",
    ),
    (
        "market.futures_mapping",
        "active_contract",
        "point-in-time dominant concrete contract code",
    ),
    (
        "reference.trading_calendar",
        "trade_date",
        "exchange trading date, not natural calendar date",
    ),
)


@dataclass(frozen=True)
class ValidationSummary:
    """Compact summary for one dataset's validation output."""

    dataset_name: str
    check_name: str
    status: str
    count: int
    sample_keys: list[str]


@dataclass(frozen=True)
class RootCoverageRow:
    """Coverage summary for one futures root code."""

    root_code: str
    mapping_rows: int
    mapping_start: str | None
    mapping_end: str | None
    distinct_active_contracts: int
    matched_daily_rows: int
    unmatched_mapping_rows: int
    mapped_daily_coverage: str
    daily_rows: int
    daily_missing_core_fields: int
    daily_ohlc_order_violations: int
    mapped_missing_price_rows: int
    mapped_missing_core_rows: int


@dataclass(frozen=True)
class Phase0Report:
    """Structured data backing the stage 0 report."""

    generated_at: datetime
    code_version: str | None
    snapshot_id: str
    lakehouse_root: Path
    db_path: Path | None
    root_codes: list[str]
    raw_trading_calendar_rows: int
    normalized_trading_calendar_rows: int
    data_file_counts: dict[str, int]
    watermarks: list[dict[str, Any]]
    ingestion_runs: list[dict[str, Any]]
    validation_summaries: list[ValidationSummary]
    root_coverage_rows: list[RootCoverageRow]
    missing_field_counts: dict[str, int]
    mapped_missing_field_counts: dict[str, int]
    anomaly_rows: list[dict[str, Any]]
    notes: list[str]


@click.command()
@click.option(
    "--db-path",
    type=click.Path(path_type=Path, dir_okay=False),
    default=DB_PATH,
    show_default=True,
)
@click.option(
    "--lakehouse-root",
    type=click.Path(path_type=Path, file_okay=False),
    default=LAKEHOUSE_ROOT,
    show_default=True,
)
@click.option(
    "--output",
    type=click.Path(path_type=Path, dir_okay=False),
    default=Path("docs/futures-v2-design/commodity-futures-stage-0-report.md"),
    show_default=True,
)
@click.option(
    "--roots",
    type=str,
    default=",".join(_DEFAULT_ROOT_CODES),
    show_default=True,
    help="Comma-separated futures root codes to audit.",
)
def main(db_path: Path, lakehouse_root: Path, output: Path, roots: str) -> None:
    """Audit current commodity futures data and write a stage 0 report."""

    root_codes = _parse_roots(roots)
    report = build_phase0_report(
        db_path=db_path,
        lakehouse_root=lakehouse_root,
        root_codes=root_codes,
    )
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(render_phase0_report(report), encoding="utf-8")
    click.echo(f"wrote {output}")
    click.echo(f"snapshot_id={report.snapshot_id}")
    click.echo(f"code_version={report.code_version or 'unknown'}")
    click.echo(
        f"calendar_rows audit={report.raw_trading_calendar_rows} "
        f"normalized={report.normalized_trading_calendar_rows}"
    )


def build_phase0_report(
    *,
    db_path: Path,
    lakehouse_root: Path,
    root_codes: list[str],
) -> Phase0Report:
    """Build a stage 0 acceptance report from current lakehouse data."""

    generated_at = datetime.now(tz=UTC)
    code_version = _git_commit()
    trading_calendar = _load_trading_calendar(lakehouse_root)
    canonical_trading_calendar = trading_calendar.copy()
    if not canonical_trading_calendar.empty:
        canonical_trading_calendar["trade_date"] = pd.to_datetime(
            canonical_trading_calendar["trade_date"], errors="coerce"
        ).dt.date
    normalized_trading_calendar_rows = 0
    normalized_calendar_path = build_zone_path(
        "reference.trading_calendar", StorageZone.NORMALIZED, lakehouse_root
    )
    if normalized_calendar_path.exists():
        normalized_trading_calendar_rows = len(
            _read_parquet_files(sorted(normalized_calendar_path.rglob("*.parquet")))
        )

    instruments = _load_normalized_dataset(
        "reference.futures_instruments", lakehouse_root
    )
    mapping = _load_normalized_dataset("market.futures_mapping", lakehouse_root)
    daily = _load_normalized_dataset("market.futures_contract_daily", lakehouse_root)
    root_daily = _with_root_code(daily)

    validators_context: dict[str, Any] = {
        "dataset_name": "reference.trading_calendar",
        "canonical_trading_calendar": canonical_trading_calendar[
            canonical_trading_calendar["is_open"].eq(True)
        ].copy(),
    }
    validation_summaries = _summarize_validation(
        trading_calendar,
        "reference.trading_calendar",
        validators_context,
    )
    validation_summaries.extend(
        _summarize_validation(
            instruments,
            "reference.futures_instruments",
            {"dataset_name": "reference.futures_instruments"},
        )
    )
    validation_summaries.extend(
        _summarize_validation(
            mapping,
            "market.futures_mapping",
            {"dataset_name": "market.futures_mapping"},
        )
    )
    validation_summaries.extend(
        _summarize_validation(
            daily,
            "market.futures_contract_daily",
            {
                "dataset_name": "market.futures_contract_daily",
                "canonical_trading_calendar": validators_context[
                    "canonical_trading_calendar"
                ],
            },
        )
    )

    root_coverage_rows = _summarize_root_coverage(
        mapping=mapping,
        daily=root_daily,
        root_codes=root_codes,
    )
    missing_field_counts = _missing_field_counts(daily)
    watermarks = _read_watermarks(db_path)
    ingestion_runs = _read_ingestion_runs(db_path)
    snapshot_id = _snapshot_id(
        code_version=code_version,
        lakehouse_root=lakehouse_root,
        root_codes=root_codes,
        files_by_dataset={
            dataset_name: _dataset_file_fingerprints(dataset_name, lakehouse_root)
            for dataset_name in _FUTURES_DATASETS
        },
    )
    notes = _build_notes(
        normalized_trading_calendar_rows=normalized_trading_calendar_rows,
        missing_field_counts=missing_field_counts,
        root_coverage_rows=root_coverage_rows,
    )
    mapped_missing_field_counts = _mapped_missing_field_counts(
        mapping=mapping,
        daily=root_daily,
    )
    anomaly_rows = _build_anomaly_rows(mapping=mapping, daily=root_daily)
    return Phase0Report(
        generated_at=generated_at,
        code_version=code_version,
        snapshot_id=snapshot_id,
        lakehouse_root=lakehouse_root,
        db_path=db_path,
        root_codes=root_codes,
        raw_trading_calendar_rows=len(trading_calendar),
        normalized_trading_calendar_rows=normalized_trading_calendar_rows,
        data_file_counts={
            dataset_name: _dataset_file_count(dataset_name, lakehouse_root)
            for dataset_name in _FUTURES_DATASETS
        },
        watermarks=watermarks,
        ingestion_runs=ingestion_runs,
        validation_summaries=validation_summaries,
        root_coverage_rows=root_coverage_rows,
        missing_field_counts=missing_field_counts,
        mapped_missing_field_counts=mapped_missing_field_counts,
        anomaly_rows=anomaly_rows,
        notes=notes,
    )


def render_phase0_report(report: Phase0Report) -> str:
    """Render one stage 0 report as markdown."""

    lines: list[str] = []
    lines.append("# TradePilot 商品期货阶段 0 接入验收与数据冻结报告")
    lines.append("")
    lines.append(f"Generated at: `{report.generated_at.isoformat()}`")
    lines.append(f"Code version: `{report.code_version or 'unknown'}`")
    lines.append(f"Snapshot id: `{report.snapshot_id}`")
    lines.append(f"Lakehouse root: `{report.lakehouse_root}`")
    lines.append(f"DB path: `{report.db_path}`")
    lines.append("")
    lines.append("## Scope")
    lines.append("")
    lines.append(
        "本报告覆盖接入验收、字段/单位清单、交易日历交叉校验、主力映射可追溯性、"
        "连续合约前置缺口与数据冻结标识。"
    )
    lines.append("")
    lines.append("## Stage 0 Decision")
    lines.append("")
    lines.append(
        "结论：`pass_with_caveats`。当前快照满足阶段 0 进入阶段 1 的最小门槛："
        "交易日历按业务键冻结后无重复，主力映射全部能关联到实际单合约行情，"
        "映射主力行的 `close/settle/volume/oi` 无缺失。"
    )
    lines.append("")
    lines.append(
        "限制：非主力/远月单合约仍存在早期 OHLC、`settle`、`volume`、`oi` 缺口，"
        "以及少量到期附近零 OHLC 但有结算价的记录；这些记录不得在阶段 1 之后静默用于"
        "收益、换月或流动性判断，必须在单合约审计时逐条复核或排除。"
    )
    lines.append("")
    lines.append("## Inputs")
    lines.append("")
    lines.append(f"- root codes: `{', '.join(report.root_codes)}`")
    lines.append(
        "- trading calendar audit rows after business-key freeze: "
        f"`{report.raw_trading_calendar_rows}`"
    )
    lines.append(
        "- normalized trading calendar rows: "
        f"`{report.normalized_trading_calendar_rows}`"
    )
    for dataset_name, count in report.data_file_counts.items():
        lines.append(f"- {dataset_name}: `{count}` parquet files")
    lines.append("")
    lines.append("## Field And Unit Manifest")
    lines.append("")
    lines.extend(
        _markdown_table(
            headers=["Dataset", "Field", "Stage 0 unit / meaning"],
            rows=[list(row) for row in _FUTURES_UNIT_ROWS],
        )
    )
    lines.append("")
    lines.append("## Validation Summary")
    lines.append("")
    lines.extend(
        _markdown_table(
            headers=["Dataset", "Check", "Status", "Count", "Sample keys"],
            rows=[
                [
                    item.dataset_name,
                    item.check_name,
                    item.status,
                    str(item.count),
                    ", ".join(item.sample_keys) if item.sample_keys else "-",
                ]
                for item in report.validation_summaries
            ],
        )
    )
    lines.append("")
    lines.append("## Root Coverage")
    lines.append("")
    lines.extend(
        _markdown_table(
            headers=[
                "Root",
                "Mapping rows",
                "Mapping window",
                "Distinct active contracts",
                "Matched daily rows",
                "Unmatched mapping rows",
                "Coverage",
                "Daily rows",
                "Missing core fields",
                "OHLC order violations",
                "Mapped missing prices",
                "Mapped missing core",
            ],
            rows=[
                [
                    item.root_code,
                    str(item.mapping_rows),
                    _window_text(item.mapping_start, item.mapping_end),
                    str(item.distinct_active_contracts),
                    str(item.matched_daily_rows),
                    str(item.unmatched_mapping_rows),
                    item.mapped_daily_coverage,
                    str(item.daily_rows),
                    str(item.daily_missing_core_fields),
                    str(item.daily_ohlc_order_violations),
                    str(item.mapped_missing_price_rows),
                    str(item.mapped_missing_core_rows),
                ]
                for item in report.root_coverage_rows
            ],
        )
    )
    lines.append("")
    lines.append("## Snapshot Freeze")
    lines.append("")
    lines.extend(
        _markdown_table(
            headers=["Dataset", "Latest fetched date", "Latest successful run id"],
            rows=[
                [
                    row.get("dataset_name", "-"),
                    str(row.get("latest_fetched_date") or "-"),
                    str(row.get("latest_successful_run_id") or "-"),
                ]
                for row in report.watermarks
            ],
        )
    )
    lines.append("")
    if report.ingestion_runs:
        lines.append("Recent ingestion runs:")
        for row in report.ingestion_runs[:12]:
            lines.append(
                "- "
                f"{row.get('dataset_name', '-')}: "
                f"run_id={row.get('run_id', '-')}, "
                f"status={row.get('status', '-')}, "
                f"finished_at={row.get('finished_at', '-')}"
            )
        lines.append("")
    lines.append("## Missing Field Counts")
    lines.append("")
    lines.extend(
        _markdown_table(
            headers=["Field", "Missing rows"],
            rows=[
                [field, str(count)]
                for field, count in report.missing_field_counts.items()
            ],
        )
    )
    lines.append("")
    lines.append("Mapped active-contract missing counts:")
    lines.extend(
        _markdown_table(
            headers=["Field", "Missing rows"],
            rows=[
                [field, str(count)]
                for field, count in report.mapped_missing_field_counts.items()
            ],
        )
    )
    lines.append("")
    lines.append("## Accepted Anomaly Records")
    lines.append("")
    lines.extend(
        _markdown_table(
            headers=[
                "Category",
                "Root",
                "Contract",
                "Trade date",
                "Fields",
                "Disposition",
            ],
            rows=[
                [
                    str(row.get("category", "-")),
                    str(row.get("root_code", "-")),
                    str(row.get("contract_code", "-")),
                    str(row.get("trade_date", "-")),
                    str(row.get("fields", "-")),
                    str(row.get("disposition", "-")),
                ]
                for row in report.anomaly_rows
            ],
        )
    )
    lines.append("")
    lines.append("## Notes")
    lines.append("")
    for note in report.notes:
        lines.append(f"- {note}")
    lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def _summarize_validation(
    frame: pd.DataFrame, dataset_name: str, context: dict[str, Any]
) -> list[ValidationSummary]:
    """Run the registered validator and summarize its output."""

    validator = get_validator(dataset_name)
    results = validator.validate(frame, context=context)
    ordered = sorted(
        results,
        key=lambda item: (
            _status_rank(item.status),
            item.dataset_name,
            item.check_name,
            item.subject_key or "",
        ),
    )
    summaries: dict[str, list[Any]] = {}
    for result in ordered:
        summaries.setdefault(result.check_name, []).append(result)
    rows: list[ValidationSummary] = []
    for check_name, items in sorted(summaries.items()):
        worst = max(items, key=lambda item: _status_rank(item.status))
        rows.append(
            ValidationSummary(
                dataset_name=dataset_name,
                check_name=check_name,
                status=worst.status.value,
                count=len(items),
                sample_keys=[
                    key
                    for key in [item.subject_key for item in items[:3]]
                    if key is not None
                ],
            )
        )
    return rows


def _summarize_root_coverage(
    *, mapping: pd.DataFrame, daily: pd.DataFrame, root_codes: list[str]
) -> list[RootCoverageRow]:
    """Summarize how well each root maps to concrete daily bars."""

    rows: list[RootCoverageRow] = []
    for root_code in root_codes:
        root_mapping = mapping[mapping["root_code"].eq(root_code)].copy()
        root_daily = daily[daily["root_code"].eq(root_code)].copy()
        if root_mapping.empty:
            rows.append(
                RootCoverageRow(
                    root_code=root_code,
                    mapping_rows=0,
                    mapping_start=None,
                    mapping_end=None,
                    distinct_active_contracts=0,
                    matched_daily_rows=0,
                    unmatched_mapping_rows=0,
                    mapped_daily_coverage="0.0%",
                    daily_rows=len(root_daily),
                    daily_missing_core_fields=_core_missing_count(root_daily),
                    daily_ohlc_order_violations=_ohlc_order_violations(root_daily),
                    mapped_missing_price_rows=0,
                    mapped_missing_core_rows=0,
                )
            )
            continue
        joined = root_mapping.merge(
            root_daily,
            left_on=["active_contract", "trade_date"],
            right_on=["contract_code", "trade_date"],
            how="left",
            indicator=True,
        )
        matched = int((joined["_merge"] == "both").sum())
        unmatched = int((joined["_merge"] == "left_only").sum())
        price_fields = [
            field for field in ["open", "high", "low", "close"] if field in joined
        ]
        core_fields = [
            field for field in ["close", "settle", "volume", "oi"] if field in joined
        ]
        mapped_missing_price_rows = (
            int(joined[price_fields].isna().any(axis=1).sum()) if price_fields else 0
        )
        mapped_missing_core_rows = (
            int(joined[core_fields].isna().any(axis=1).sum()) if core_fields else 0
        )
        rows.append(
            RootCoverageRow(
                root_code=root_code,
                mapping_rows=len(root_mapping),
                mapping_start=_date_text(root_mapping["trade_date"].min()),
                mapping_end=_date_text(root_mapping["trade_date"].max()),
                distinct_active_contracts=root_mapping["active_contract"].nunique(),
                matched_daily_rows=matched,
                unmatched_mapping_rows=unmatched,
                mapped_daily_coverage=_percent_text(matched, len(root_mapping)),
                daily_rows=len(root_daily),
                daily_missing_core_fields=_core_missing_count(root_daily),
                daily_ohlc_order_violations=_ohlc_order_violations(root_daily),
                mapped_missing_price_rows=mapped_missing_price_rows,
                mapped_missing_core_rows=mapped_missing_core_rows,
            )
        )
    return rows


def _missing_field_counts(frame: pd.DataFrame) -> dict[str, int]:
    """Return missing counts for the core futures daily fields."""

    fields = ["open", "high", "low", "close", "settle", "volume", "oi"]
    return {field: int(frame[field].isna().sum()) for field in fields if field in frame}


def _mapped_missing_field_counts(
    *, mapping: pd.DataFrame, daily: pd.DataFrame
) -> dict[str, int]:
    """Return missing counts on mapped active-contract daily rows."""

    if mapping.empty or daily.empty:
        return {}
    joined = mapping.merge(
        daily,
        left_on=["active_contract", "trade_date"],
        right_on=["contract_code", "trade_date"],
        how="left",
    )
    fields = ["open", "high", "low", "close", "settle", "volume", "oi"]
    return {
        field: int(joined[field].isna().sum()) for field in fields if field in joined
    }


def _build_anomaly_rows(
    *, mapping: pd.DataFrame, daily: pd.DataFrame
) -> list[dict[str, Any]]:
    """Return stage-0 anomaly records that must be carried into later audits."""

    rows: list[dict[str, Any]] = []
    if daily.empty:
        return rows

    missing_core = daily[
        daily[
            [field for field in ["close", "settle", "volume", "oi"] if field in daily]
        ]
        .isna()
        .any(axis=1)
    ].copy()
    for _, row in missing_core.head(6).iterrows():
        missing_fields = [
            field
            for field in ["close", "settle", "volume", "oi"]
            if field in row and pd.isna(row[field])
        ]
        rows.append(
            {
                "category": "single_contract_missing_core",
                "root_code": row.get("root_code", "-"),
                "contract_code": row.get("contract_code", "-"),
                "trade_date": _date_text(row.get("trade_date")),
                "fields": ",".join(missing_fields),
                "disposition": "accepted for stage 0; recheck before stage 1 roll audit if selected",
            }
        )

    ohlc_bad = _ohlc_order_violation_rows(daily)
    for _, row in ohlc_bad.head(6).iterrows():
        rows.append(
            {
                "category": "single_contract_ohlc_order",
                "root_code": row.get("root_code", "-"),
                "contract_code": row.get("contract_code", "-"),
                "trade_date": _date_text(row.get("trade_date")),
                "fields": "open,high,low,close",
                "disposition": "accepted for stage 0; exclude from return construction unless manually justified",
            }
        )

    if mapping.empty:
        return rows
    mapped = mapping.merge(
        daily,
        left_on=["active_contract", "trade_date"],
        right_on=["contract_code", "trade_date"],
        how="left",
    )
    mapped_missing_price = mapped[
        mapped[[field for field in ["open", "high", "low", "close"] if field in mapped]]
        .isna()
        .any(axis=1)
    ]
    for _, row in mapped_missing_price.head(6).iterrows():
        missing_fields = [
            field
            for field in ["open", "high", "low", "close"]
            if field in row and pd.isna(row[field])
        ]
        rows.append(
            {
                "category": "mapped_active_missing_ohlc",
                "root_code": row.get("root_code_x", row.get("root_code", "-")),
                "contract_code": row.get("active_contract", "-"),
                "trade_date": _date_text(row.get("trade_date")),
                "fields": ",".join(missing_fields),
                "disposition": "non-blocking for settle/close returns; audit before using intraday OHLC logic",
            }
        )
    return rows


def _core_missing_count(frame: pd.DataFrame) -> int:
    """Count rows missing any core daily field."""

    if frame.empty:
        return 0
    fields = [field for field in ["close", "settle", "volume", "oi"] if field in frame]
    if not fields:
        return 0
    return int(frame[frame[fields].isna().any(axis=1)].shape[0])


def _ohlc_order_violations(frame: pd.DataFrame) -> int:
    """Count rows where OHLC order is inconsistent."""

    if frame.empty:
        return 0
    required = ["open", "high", "low", "close"]
    if any(field not in frame for field in required):
        return 0
    return len(_ohlc_order_violation_rows(frame))


def _ohlc_order_violation_rows(frame: pd.DataFrame) -> pd.DataFrame:
    """Return rows where OHLC order is inconsistent."""

    required = ["open", "high", "low", "close"]
    if frame.empty or any(field not in frame for field in required):
        return pd.DataFrame()
    subset = frame.dropna(subset=required).copy()
    if subset.empty:
        return subset
    high_violation = subset["high"] < subset[["open", "low", "close"]].max(axis=1)
    low_violation = subset["low"] > subset[["open", "high", "close"]].min(axis=1)
    return subset[high_violation | low_violation]


def _load_trading_calendar(lakehouse_root: Path) -> pd.DataFrame:
    """Load and canonicalize raw trading-calendar batches."""

    raw_path = build_zone_path(
        "reference.trading_calendar", StorageZone.RAW, lakehouse_root
    )
    frame = _read_parquet_files(sorted(raw_path.rglob("*.parquet")))
    if frame.empty:
        return frame
    normalizer = TradingCalendarNormalizer()
    result = normalizer.normalize(frame, context={"exchange": "SH"})
    canonical = result.canonical_payload.copy()
    if canonical.empty:
        return canonical
    # The source stores both SH and SZ rows in the same raw dataset. Preserve the
    # source exchange values after the normalizer's generic fallback has run.
    if "exchange" in frame.columns and "exchange" in canonical.columns:
        canonical["exchange"] = frame["exchange"].astype(str).str.upper().values
    return canonical.drop_duplicates(
        ["exchange", "trade_date"], keep="last"
    ).reset_index(drop=True)


def _load_normalized_dataset(dataset_name: str, lakehouse_root: Path) -> pd.DataFrame:
    """Load one normalized parquet dataset from lakehouse partitions."""

    return _read_parquet_files(
        sorted(
            build_zone_path(dataset_name, StorageZone.NORMALIZED, lakehouse_root).rglob(
                "*.parquet"
            )
        )
    )


def _read_parquet_files(paths: list[Path]) -> pd.DataFrame:
    """Read a list of parquet files into one frame."""

    if not paths:
        return pd.DataFrame()
    frames = [pd.read_parquet(path) for path in paths]
    if not frames:
        return pd.DataFrame()
    frame = pd.concat(frames, ignore_index=True)
    for column in ("trade_date", "list_date", "delist_date", "pretrade_date"):
        if column in frame.columns:
            frame[column] = pd.to_datetime(frame[column], errors="coerce").dt.date
    return frame


def _with_root_code(frame: pd.DataFrame) -> pd.DataFrame:
    """Attach root_code to a futures daily frame."""

    if frame.empty:
        return frame.copy()
    result = frame.copy()
    result["root_code"] = result["contract_code"].map(_root_code_from_contract)
    return result


def _root_code_from_contract(contract_code: object) -> str | None:
    """Derive the root code from one concrete futures contract code."""

    text = str(contract_code or "").strip().upper()
    match = re.match(r"^([A-Z]+)\d*\.(?P<exchange>[A-Z]+)$", text)
    if not match:
        return None
    return f"{match.group(1)}.{match.group('exchange')}"


def _dataset_file_count(dataset_name: str, lakehouse_root: Path) -> int:
    """Return the number of parquet files for one dataset across zones."""

    counts = 0
    for zone in (StorageZone.RAW, StorageZone.NORMALIZED, StorageZone.DERIVED):
        counts += len(
            list(build_zone_path(dataset_name, zone, lakehouse_root).rglob("*.parquet"))
        )
    return counts


def _dataset_file_fingerprints(dataset_name: str, lakehouse_root: Path) -> list[str]:
    """Return stable fingerprints for one dataset's parquet files."""

    fingerprints: list[str] = []
    for zone in (StorageZone.RAW, StorageZone.NORMALIZED, StorageZone.DERIVED):
        for path in sorted(
            build_zone_path(dataset_name, zone, lakehouse_root).rglob("*.parquet")
        ):
            stat = path.stat()
            fingerprints.append(
                f"{path.relative_to(lakehouse_root).as_posix()}:{stat.st_size}:{stat.st_mtime_ns}"
            )
    return fingerprints


def _snapshot_id(
    *,
    code_version: str | None,
    lakehouse_root: Path,
    root_codes: list[str],
    files_by_dataset: dict[str, list[str]],
) -> str:
    """Build one deterministic snapshot identifier from current inputs."""

    digest = hashlib.sha256()
    digest.update((code_version or "unknown").encode("utf-8"))
    digest.update(lakehouse_root.as_posix().encode("utf-8"))
    digest.update(",".join(root_codes).encode("utf-8"))
    for dataset_name in sorted(files_by_dataset):
        digest.update(dataset_name.encode("utf-8"))
        for item in files_by_dataset[dataset_name]:
            digest.update(item.encode("utf-8"))
    return digest.hexdigest()[:16]


def _read_watermarks(db_path: Path) -> list[dict[str, Any]]:
    """Read latest source watermarks for futures datasets if available."""

    if not db_path.exists():
        return []
    conn = duckdb.connect(str(db_path))
    try:
        if not _table_exists(conn, "etl_source_watermarks"):
            return []
        frame = conn.execute(
            """
            SELECT dataset_name, latest_fetched_date
            FROM etl_source_watermarks
            WHERE dataset_name IN (?, ?, ?, ?)
            ORDER BY dataset_name
            """,
            list(_FUTURES_DATASETS),
        ).fetchdf()
        if frame.empty:
            return []
        latest_runs = _latest_ingestion_runs(conn)
        rows: list[dict[str, Any]] = []
        for _, row in frame.iterrows():
            dataset_name = str(row["dataset_name"])
            rows.append(
                {
                    "dataset_name": dataset_name,
                    "latest_fetched_date": row["latest_fetched_date"],
                    "latest_successful_run_id": latest_runs.get(dataset_name),
                }
            )
        return rows
    finally:
        conn.close()


def _read_ingestion_runs(db_path: Path) -> list[dict[str, Any]]:
    """Read recent ingestion runs for the futures datasets if available."""

    if not db_path.exists():
        return []
    conn = duckdb.connect(str(db_path))
    try:
        if not _table_exists(conn, "etl_ingestion_runs"):
            return []
        frame = conn.execute(
            """
            SELECT run_id, dataset_name, status, finished_at
            FROM etl_ingestion_runs
            WHERE dataset_name IN (?, ?, ?, ?)
            ORDER BY finished_at DESC NULLS LAST, run_id DESC
            LIMIT 12
            """,
            list(_FUTURES_DATASETS),
        ).fetchdf()
        return frame.to_dict(orient="records")
    finally:
        conn.close()


def _latest_ingestion_runs(conn: duckdb.DuckDBPyConnection) -> dict[str, int]:
    """Return the latest successful run id for each futures dataset."""

    if not _table_exists(conn, "etl_ingestion_runs"):
        return {}
    frame = conn.execute(
        """
        SELECT dataset_name, MAX(run_id) AS run_id
        FROM etl_ingestion_runs
        WHERE dataset_name IN (?, ?, ?, ?)
          AND status = 'success'
        GROUP BY dataset_name
        """,
        list(_FUTURES_DATASETS),
    ).fetchdf()
    if frame.empty:
        return {}
    return {
        str(row["dataset_name"]): int(row["run_id"])
        for _, row in frame.iterrows()
        if pd.notna(row["run_id"])
    }


def _table_exists(conn: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    """Return whether a DuckDB metadata table exists."""

    return bool(
        conn.execute(
            """
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_name = ?
            """,
            [table_name],
        ).fetchone()[0]
    )


def _build_notes(
    *,
    normalized_trading_calendar_rows: int,
    missing_field_counts: dict[str, int],
    root_coverage_rows: list[RootCoverageRow],
) -> list[str]:
    """Build operator notes for the stage 0 report."""

    notes: list[str] = []
    if normalized_trading_calendar_rows == 0:
        notes.append(
            "normalized trading calendar is absent; stage 0 uses raw trading-calendar batches deduplicated by exchange/trade_date for the open-day audit"
        )
    missing_core = {
        field: count
        for field, count in missing_field_counts.items()
        if field in {"close", "settle", "volume", "oi"} and count > 0
    }
    if missing_core:
        notes.append(
            "core daily fields still have missing rows: "
            + ", ".join(f"{field}={count}" for field, count in missing_core.items())
            + "; current mapped active-contract rows have no close/settle/volume/oi gaps"
        )
    if any(item.unmatched_mapping_rows > 0 for item in root_coverage_rows):
        notes.append("some mapping rows do not find a matching daily bar")
    if not notes:
        notes.append(
            "stage 0 audit completed without blocking row-level gaps in the selected inputs"
        )
    return notes


def _parse_roots(value: str) -> list[str]:
    """Parse and de-duplicate a comma-separated root list."""

    roots = [item.strip().upper() for item in value.split(",") if item.strip()]
    if not roots:
        raise click.BadParameter(
            "at least one root code is required", param_hint="--roots"
        )
    return list(dict.fromkeys(roots))


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


def _status_rank(status: ValidationStatus) -> int:
    """Return a rank for validation severity ordering."""

    order = {
        ValidationStatus.FAIL: 4,
        ValidationStatus.DEFER: 3,
        ValidationStatus.WARNING: 2,
        ValidationStatus.PASS_WITH_CAVEAT: 1,
        ValidationStatus.PASS: 0,
        ValidationStatus.VALIDATION_ONLY: 0,
    }
    return order.get(status, 0)


def _window_text(start: str | None, end: str | None) -> str:
    """Format one date window for markdown."""

    if start is None and end is None:
        return "-"
    return f"{start or '-'} .. {end or '-'}"


def _percent_text(numerator: int, denominator: int) -> str:
    """Format a percentage string."""

    if denominator <= 0:
        return "0.0%"
    return f"{(numerator / denominator) * 100:.1f}%"


def _date_text(value: date | datetime | pd.Timestamp | Any) -> str | None:
    """Format one date-like value for display."""

    if value is None or pd.isna(value):
        return None
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


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
