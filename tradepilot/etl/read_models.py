"""Read services for derived ETL read models."""

from __future__ import annotations

from datetime import date
import json
from pathlib import Path
from typing import Any

import pandas as pd

from tradepilot.etl.models import StorageZone
from tradepilot.etl.storage import build_dataset_file_path

_ETF_AW_SNAPSHOT_DATASET = "derived.etf_aw_rebalance_snapshot"
_ETF_AW_SNAPSHOT_SCHEMA_VERSION = "etf_aw_snapshot_v1"
_ETF_AW_SNAPSHOT_CONTRACT_VERSION = "etf_aw_snapshot_contract_v1"
_ETF_AW_SNAPSHOT_REQUIRED_COLUMNS = {
    "calendar_name",
    "calendar_month",
    "rebalance_date",
    "effective_date",
    "sleeve_code",
    "sleeve_role",
    "data_status",
}
_ETF_AW_SNAPSHOT_STATUS_ORDER = ["stale", "missing", "partial", "complete"]
_ETF_AW_SNAPSHOT_STATUSES = set(_ETF_AW_SNAPSHOT_STATUS_ORDER)


def get_latest_etf_aw_snapshot(
    as_of_date: date | None = None,
    *,
    lakehouse_root: Path | None = None,
) -> dict[str, Any] | None:
    """Return the latest ETF all-weather snapshot at or before a date."""

    frame = _read_latest_etf_aw_snapshot_partition(
        as_of_date=as_of_date,
        lakehouse_root=lakehouse_root,
    )
    if frame.empty:
        return None
    dates = frame["rebalance_date"].dropna().tolist()
    if not dates:
        return None
    latest_date = max(dates)
    latest = frame[frame["rebalance_date"] == latest_date].copy()
    latest = latest.sort_values("calendar_name")
    for _, group in latest.groupby("calendar_name", sort=True):
        return _snapshot_contract(group)
    return None


def list_etf_aw_snapshots(
    start: date,
    end: date,
    *,
    lakehouse_root: Path | None = None,
) -> list[dict[str, Any]]:
    """Return ETF all-weather snapshots in an inclusive rebalance-date window."""

    if start > end:
        start, end = end, start
    frame = _read_etf_aw_snapshot_partitions(
        start=start,
        end=end,
        lakehouse_root=lakehouse_root,
    )
    if frame.empty:
        return []
    frame = _normalize_snapshot_frame(frame)
    if frame.empty:
        return []
    frame = frame[frame["rebalance_date"].between(start, end, inclusive="both")]
    return [
        _snapshot_contract(group)
        for _, group in frame.groupby(["calendar_name", "rebalance_date"], sort=True)
    ]


def _read_etf_aw_snapshot_partitions(
    start: date | None = None,
    end: date | None = None,
    *,
    lakehouse_root: Path | None = None,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for year, month in _snapshot_months(start, end, lakehouse_root=lakehouse_root):
        path = build_dataset_file_path(
            _ETF_AW_SNAPSHOT_DATASET,
            StorageZone.DERIVED,
            [("year", year), ("month", f"{month:02d}")],
            lakehouse_root=lakehouse_root,
        )
        if path.exists():
            frames.append(pd.read_parquet(path))
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def _read_latest_etf_aw_snapshot_partition(
    *,
    as_of_date: date | None,
    lakehouse_root: Path | None,
) -> pd.DataFrame:
    months = _snapshot_months(None, as_of_date, lakehouse_root=lakehouse_root)
    for year, month in reversed(months):
        path = build_dataset_file_path(
            _ETF_AW_SNAPSHOT_DATASET,
            StorageZone.DERIVED,
            [("year", year), ("month", f"{month:02d}")],
            lakehouse_root=lakehouse_root,
        )
        if not path.exists():
            continue
        frame = _normalize_snapshot_frame(pd.read_parquet(path))
        if as_of_date is not None and not frame.empty:
            frame = frame[frame["rebalance_date"] <= as_of_date].copy()
        if not frame.empty:
            return frame
    return pd.DataFrame()


def _normalize_snapshot_frame(frame: pd.DataFrame) -> pd.DataFrame:
    if not _ETF_AW_SNAPSHOT_REQUIRED_COLUMNS.issubset(frame.columns):
        return pd.DataFrame()
    normalized = frame.copy()
    normalized["rebalance_date"] = pd.to_datetime(
        normalized["rebalance_date"], errors="coerce"
    ).dt.date
    normalized = normalized.dropna(subset=["rebalance_date"])
    normalized["data_status"] = normalized["data_status"].astype(str)
    return normalized[normalized["data_status"].isin(_ETF_AW_SNAPSHOT_STATUSES)]


def _snapshot_months(
    start: date | None,
    end: date | None,
    *,
    lakehouse_root: Path | None,
) -> list[tuple[int, int]]:
    if start is not None and end is not None:
        if start > end:
            start, end = end, start
        months: list[tuple[int, int]] = []
        cursor = date(start.year, start.month, 1)
        while cursor <= end:
            months.append((cursor.year, cursor.month))
            if cursor.month == 12:
                cursor = date(cursor.year + 1, 1, 1)
            else:
                cursor = date(cursor.year, cursor.month + 1, 1)
        return months

    upper_month = (end.year, end.month) if end is not None else None
    dataset_root = (
        (lakehouse_root / StorageZone.DERIVED.value)
        if lakehouse_root is not None
        else None
    )
    if dataset_root is None:
        from tradepilot.config import LAKEHOUSE_DERIVED_ROOT

        dataset_root = LAKEHOUSE_DERIVED_ROOT
    root = dataset_root / _ETF_AW_SNAPSHOT_DATASET
    if not root.exists():
        return []
    months = []
    for path in root.glob("*/*/part-00000.parquet"):
        try:
            month = (int(path.parent.parent.name), int(path.parent.name))
        except ValueError:
            continue
        if upper_month is None or month <= upper_month:
            months.append(month)
    return sorted(set(months))


def _snapshot_contract(frame: pd.DataFrame) -> dict[str, Any]:
    ordered = frame.sort_values(["sleeve_role", "sleeve_code"]).copy()
    statuses = set(ordered["data_status"].dropna().astype(str).tolist())
    if "stale" in statuses:
        status = "stale"
    elif "missing" in statuses:
        status = "missing"
    elif "partial" in statuses:
        status = "partial"
    else:
        status = "complete"
    first = ordered.iloc[0]
    return {
        "schema_version": _ETF_AW_SNAPSHOT_SCHEMA_VERSION,
        "contract_version": _ETF_AW_SNAPSHOT_CONTRACT_VERSION,
        "calendar_name": str(first["calendar_name"]),
        "calendar_month": str(first["calendar_month"]),
        "rebalance_date": _date_text(first["rebalance_date"]),
        "effective_date": _date_text(first["effective_date"]),
        "data_status": status,
        "sleeves": [_sleeve_contract(row) for _, row in ordered.iterrows()],
    }


def _sleeve_contract(row: pd.Series) -> dict[str, Any]:
    return {
        "sleeve_code": str(row["sleeve_code"]),
        "sleeve_role": str(row["sleeve_role"]),
        "close": _optional_float(row.get("close")),
        "adj_factor": _optional_float(row.get("adj_factor")),
        "adj_close": _optional_float(row.get("adj_close")),
        "return_1m": _optional_float(row.get("return_1m")),
        "return_3m": _optional_float(row.get("return_3m")),
        "return_6m": _optional_float(row.get("return_6m")),
        "volatility_3m": _optional_float(row.get("volatility_3m")),
        "max_drawdown_6m": _optional_float(row.get("max_drawdown_6m")),
        "data_status": str(row["data_status"]),
        "quality_notes": _quality_notes(row.get("quality_notes")),
        "source_max_trade_date": _date_text(row.get("source_max_trade_date")),
    }


def _quality_notes(value: object) -> dict[str, Any]:
    if not isinstance(value, str) or not value.strip():
        return {}
    try:
        loaded = json.loads(value)
    except json.JSONDecodeError:
        return {"raw": value}
    return loaded if isinstance(loaded, dict) else {"value": loaded}


def _date_text(value: object) -> str | None:
    if value is None or pd.isna(value):
        return None
    if isinstance(value, date):
        return value.isoformat()
    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        return None
    return parsed.date().isoformat()


def _optional_float(value: object) -> float | None:
    if value is None or pd.isna(value):
        return None
    return float(value)
