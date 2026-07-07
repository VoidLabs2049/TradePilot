"""Read services for derived ETL read models."""

from __future__ import annotations

from datetime import date
import json
from pathlib import Path
from typing import Any

import pandas as pd

from tradepilot.etl.constants import ETF_AW_ROLE_RANK
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
_ETF_AW_REGIME_SCORE_DATASET = "derived.etf_aw_regime_score"
_ETF_AW_REGIME_SCORE_SCHEMA_VERSION = "etf_aw_regime_score_v1"
_ETF_AW_MARKET_FEATURES_DATASET = "derived.etf_aw_market_features"
_ETF_AW_MARKET_FEATURES_SCHEMA_VERSION = "etf_aw_market_features_v1"
_ETF_AW_STRATEGY_CONTEXT_DATASET = "derived.etf_aw_strategy_context"
_ETF_AW_STRATEGY_CONTEXT_SCHEMA_VERSION = "etf_aw_strategy_context_v1"
_ETF_AW_STRATEGY_CONTEXT_CONTRACT_VERSION = "etf_aw_strategy_context_contract_v1"
_ETF_AW_RISK_BUDGET_DATASET = "derived.etf_aw_risk_budget"
_ETF_AW_RISK_BUDGET_SCHEMA_VERSION = "etf_aw_risk_budget_v1"
_ETF_AW_RISK_BUDGET_CONTRACT_VERSION = "etf_aw_risk_budget_contract_v1"
_ETF_AW_TARGET_WEIGHT_DATASET = "derived.etf_aw_target_weight"
_ETF_AW_TARGET_WEIGHT_SCHEMA_VERSION = "etf_aw_target_weight_v1"
_ETF_AW_TARGET_WEIGHT_CONTRACT_VERSION = "etf_aw_target_weight_contract_v1"
_ETF_AW_MACRO_RATES_CONTEXT_SCHEMA_VERSION = "etf_aw_macro_rates_context_v1"
_MACRO_SLOW_FIELDS_DATASET = "macro.slow_fields"
_RATES_DAILY_RATES_DATASET = "rates.daily_rates"
_RATES_LPR_DATASET = "rates.lpr"
_RATES_GOV_CURVE_POINTS_DATASET = "rates.gov_curve_points"
_ETF_AW_PRIMARY_FIELDS = {
    "official_pmi",
    "shibor_1w",
    "lpr_1y",
    "cn_gov_10y_yield",
}
_ETF_AW_CONFIRMATORY_FIELDS = {
    "shibor_overnight",
    "lpr_5y",
    "cn_gov_1y_yield",
    "cn_yield_curve_slope_10y_1y",
}
_ETF_AW_FIELD_MAX_AGE_DAYS = {
    "official_pmi": 65,
    "shibor_1w": 10,
    "shibor_overnight": 10,
    "lpr_1y": 65,
    "lpr_5y": 65,
    "cn_gov_1y_yield": 10,
    "cn_gov_10y_yield": 10,
    "cn_yield_curve_slope_10y_1y": 10,
}


def _etf_aw_role_sort_key(series: pd.Series) -> pd.Series:
    return series.astype(str).map(ETF_AW_ROLE_RANK).fillna(len(ETF_AW_ROLE_RANK))


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


def get_latest_etf_aw_regime_context(
    as_of_date: date | None = None,
    *,
    lakehouse_root: Path | None = None,
) -> dict[str, Any] | None:
    """Return the latest ETF all-weather regime context at or before a date."""

    frame = _read_etf_aw_regime_score_partitions(lakehouse_root=lakehouse_root)
    if frame.empty:
        return None
    frame["rebalance_date"] = _normalize_date_series(frame["rebalance_date"])
    frame = frame.dropna(subset=["rebalance_date"])
    if as_of_date is not None:
        frame = frame[frame["rebalance_date"] <= as_of_date].copy()
    if frame.empty:
        return None
    latest_date = max(frame["rebalance_date"].dropna().tolist())
    latest = frame[frame["rebalance_date"] == latest_date].copy()
    latest = latest.sort_values("ingested_at")
    return _regime_contract(latest.iloc[-1])


def list_etf_aw_regime_contexts(
    start: date,
    end: date,
    *,
    lakehouse_root: Path | None = None,
) -> list[dict[str, Any]]:
    """Return ETF all-weather regime contexts in a rebalance-date window."""

    if start > end:
        start, end = end, start
    frame = _read_etf_aw_regime_score_partitions(
        start=start,
        end=end,
        lakehouse_root=lakehouse_root,
    )
    if frame.empty:
        return []
    frame["rebalance_date"] = _normalize_date_series(frame["rebalance_date"])
    frame = frame.dropna(subset=["rebalance_date"])
    frame = frame[frame["rebalance_date"].between(start, end, inclusive="both")]
    frame = frame.sort_values(["rebalance_date", "scorer_name", "scorer_version"])
    return [_regime_contract(row) for _, row in frame.iterrows()]


def get_latest_etf_aw_strategy_context(
    as_of_date: date | None = None,
    *,
    lakehouse_root: Path | None = None,
) -> dict[str, Any] | None:
    """Return the latest ETF all-weather strategy context at or before a date."""

    frame = _read_etf_aw_strategy_context_partitions(lakehouse_root=lakehouse_root)
    if frame.empty:
        return None
    frame = _normalize_strategy_context_frame(frame)
    if as_of_date is not None and not frame.empty:
        frame = frame[frame["rebalance_date"] <= as_of_date].copy()
    if frame.empty:
        return None
    latest_date = max(frame["rebalance_date"].dropna().tolist())
    latest = frame[frame["rebalance_date"] == latest_date].copy()
    latest = _sort_latest_rows(latest)
    return _strategy_context_contract(latest.iloc[-1])


def list_etf_aw_strategy_contexts(
    start: date,
    end: date,
    *,
    lakehouse_root: Path | None = None,
) -> list[dict[str, Any]]:
    """Return ETF all-weather strategy contexts in a rebalance-date window."""

    if start > end:
        start, end = end, start
    frame = _read_etf_aw_strategy_context_partitions(
        start=start,
        end=end,
        lakehouse_root=lakehouse_root,
    )
    if frame.empty:
        return []
    frame = _normalize_strategy_context_frame(frame)
    frame = frame[frame["rebalance_date"].between(start, end, inclusive="both")]
    frame = _sort_latest_rows(frame)
    latest = frame.drop_duplicates(
        ["calendar_name", "rebalance_date", "strategy_name", "strategy_version"],
        keep="last",
    )
    return [_strategy_context_contract(row) for _, row in latest.iterrows()]


def get_latest_etf_aw_risk_budget(
    as_of_date: date | None = None,
    *,
    lakehouse_root: Path | None = None,
) -> dict[str, Any] | None:
    """Return the latest ETF all-weather risk budget at or before a date."""

    frame = _read_etf_aw_risk_budget_partitions(lakehouse_root=lakehouse_root)
    if frame.empty:
        return None
    frame = _normalize_risk_budget_frame(frame)
    if as_of_date is not None and not frame.empty:
        frame = frame[frame["rebalance_date"] <= as_of_date].copy()
    if frame.empty:
        return None
    latest_date = max(frame["rebalance_date"].dropna().tolist())
    latest = frame[frame["rebalance_date"] == latest_date].copy()
    latest = _sort_latest_rows(latest)
    latest = latest.drop_duplicates(
        [
            "calendar_name",
            "rebalance_date",
            "strategy_name",
            "strategy_version",
            "sleeve_role",
        ],
        keep="last",
    )
    for _, group in latest.groupby(
        ["calendar_name", "rebalance_date", "strategy_name", "strategy_version"],
        sort=True,
    ):
        return _risk_budget_contract(group)
    return None


def list_etf_aw_risk_budgets(
    start: date,
    end: date,
    *,
    lakehouse_root: Path | None = None,
) -> list[dict[str, Any]]:
    """Return ETF all-weather risk budgets in a rebalance-date window."""

    if start > end:
        start, end = end, start
    frame = _read_etf_aw_risk_budget_partitions(
        start=start,
        end=end,
        lakehouse_root=lakehouse_root,
    )
    if frame.empty:
        return []
    frame = _normalize_risk_budget_frame(frame)
    frame = frame[frame["rebalance_date"].between(start, end, inclusive="both")]
    frame = _sort_latest_rows(frame)
    latest = frame.drop_duplicates(
        [
            "calendar_name",
            "rebalance_date",
            "strategy_name",
            "strategy_version",
            "sleeve_role",
        ],
        keep="last",
    )
    return [
        _risk_budget_contract(group)
        for _, group in latest.groupby(
            ["calendar_name", "rebalance_date", "strategy_name", "strategy_version"],
            sort=True,
        )
    ]


def get_latest_etf_aw_target_weight(
    as_of_date: date | None = None,
    *,
    calendar_name: str | None = None,
    strategy_name: str | None = None,
    strategy_version: str | None = None,
    lakehouse_root: Path | None = None,
) -> dict[str, Any] | None:
    """Return the latest ETF all-weather target weight at or before a date."""

    frame = _read_etf_aw_target_weight_partitions(lakehouse_root=lakehouse_root)
    if frame.empty:
        return None
    frame = _normalize_target_weight_frame(frame)
    if as_of_date is not None and not frame.empty:
        frame = frame[frame["rebalance_date"] <= as_of_date].copy()
    frame = _filter_target_weight_strategy(
        frame,
        calendar_name=calendar_name,
        strategy_name=strategy_name,
        strategy_version=strategy_version,
    )
    if frame.empty:
        return None
    dates = frame["rebalance_date"].dropna().tolist()
    if not dates:
        return None
    latest_date = max(dates)
    latest = frame[frame["rebalance_date"] == latest_date].copy()
    latest = _sort_target_weight_rows(latest)
    latest = latest.drop_duplicates(
        [
            "calendar_name",
            "rebalance_date",
            "strategy_name",
            "strategy_version",
            "sleeve_code",
        ],
        keep="last",
    )
    groups = [
        group
        for _, group in latest.groupby(
            ["calendar_name", "rebalance_date", "strategy_name", "strategy_version"],
            sort=True,
        )
    ]
    if len(groups) != 1:
        return None
    return _target_weight_contract(groups[0])


def list_etf_aw_target_weights(
    start: date,
    end: date,
    *,
    calendar_name: str | None = None,
    strategy_name: str | None = None,
    strategy_version: str | None = None,
    lakehouse_root: Path | None = None,
) -> list[dict[str, Any]]:
    """Return ETF all-weather target weights in a rebalance-date window."""

    if start > end:
        start, end = end, start
    frame = _read_etf_aw_target_weight_partitions(
        start=start,
        end=end,
        lakehouse_root=lakehouse_root,
    )
    if frame.empty:
        return []
    frame = _normalize_target_weight_frame(frame)
    frame = frame[frame["rebalance_date"].between(start, end, inclusive="both")]
    frame = _filter_target_weight_strategy(
        frame,
        calendar_name=calendar_name,
        strategy_name=strategy_name,
        strategy_version=strategy_version,
    )
    frame = _sort_target_weight_rows(frame)
    latest = frame.drop_duplicates(
        [
            "calendar_name",
            "rebalance_date",
            "strategy_name",
            "strategy_version",
            "sleeve_code",
        ],
        keep="last",
    )
    return [
        _target_weight_contract(group)
        for _, group in latest.groupby(
            ["calendar_name", "rebalance_date", "strategy_name", "strategy_version"],
            sort=True,
        )
    ]


def get_latest_etf_aw_market_features(
    as_of_date: date | None = None,
    *,
    lakehouse_root: Path | None = None,
) -> dict[str, Any] | None:
    """Return latest ETF all-weather market features at or before a date."""

    frame = _read_etf_aw_market_features_partitions(lakehouse_root=lakehouse_root)
    if frame.empty:
        return None
    frame = _normalize_market_features_frame(frame)
    if as_of_date is not None and not frame.empty:
        frame = frame[frame["rebalance_date"] <= as_of_date].copy()
    if frame.empty:
        return None
    latest_date = max(frame["rebalance_date"].dropna().tolist())
    latest = frame[frame["rebalance_date"] == latest_date].copy()
    latest = _sort_latest_rows(latest)
    latest = latest.drop_duplicates(
        [
            "calendar_name",
            "rebalance_date",
            "feature_name",
            "feature_scope",
            "feature_subject",
        ],
        keep="last",
    )
    return _market_features_contract(latest)


def get_latest_etf_aw_macro_rates_context(
    as_of_date: date | None = None,
    *,
    lakehouse_root: Path | None = None,
) -> dict[str, Any] | None:
    """Return timing-safe macro/rates context at or before a rebalance date."""

    if as_of_date is None:
        as_of_date = _latest_rates_effective_date(lakehouse_root=lakehouse_root)
    if as_of_date is None:
        return None
    macro = _read_macro_slow_fields_partitions(lakehouse_root=lakehouse_root)
    daily_rates = _read_daily_rates_partitions(lakehouse_root=lakehouse_root)
    lpr = _read_lpr_partitions(lakehouse_root=lakehouse_root)
    curve = _read_gov_curve_points_partitions(lakehouse_root=lakehouse_root)
    return _macro_rates_context_contract(
        rebalance_date=as_of_date,
        macro=macro,
        daily_rates=daily_rates,
        lpr=lpr,
        curve=curve,
    )


def list_etf_aw_macro_rates_contexts(
    start: date,
    end: date,
    *,
    lakehouse_root: Path | None = None,
) -> list[dict[str, Any]]:
    """Return macro/rates contexts for available dates in a window."""

    if start > end:
        start, end = end, start
    macro = _read_macro_slow_fields_partitions(lakehouse_root=lakehouse_root)
    daily_rates = _read_daily_rates_partitions(lakehouse_root=lakehouse_root)
    lpr = _read_lpr_partitions(lakehouse_root=lakehouse_root)
    curve = _read_gov_curve_points_partitions(lakehouse_root=lakehouse_root)
    dates = _rates_context_dates(macro, daily_rates, lpr, curve, start, end)
    if not dates and start == end:
        dates = [start]
    return [
        _macro_rates_context_contract(
            rebalance_date=rebalance_date,
            macro=macro,
            daily_rates=daily_rates,
            lpr=lpr,
            curve=curve,
        )
        for rebalance_date in dates
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


def _read_etf_aw_regime_score_partitions(
    start: date | None = None,
    end: date | None = None,
    *,
    lakehouse_root: Path | None = None,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for year, month in _dataset_months(
        _ETF_AW_REGIME_SCORE_DATASET,
        start,
        end,
        lakehouse_root=lakehouse_root,
    ):
        path = build_dataset_file_path(
            _ETF_AW_REGIME_SCORE_DATASET,
            StorageZone.DERIVED,
            [("year", year), ("month", f"{month:02d}")],
            lakehouse_root=lakehouse_root,
        )
        if path.exists():
            frames.append(pd.read_parquet(path))
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def _read_etf_aw_market_features_partitions(
    start: date | None = None,
    end: date | None = None,
    *,
    lakehouse_root: Path | None = None,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for year, month in _dataset_months(
        _ETF_AW_MARKET_FEATURES_DATASET,
        start,
        end,
        lakehouse_root=lakehouse_root,
    ):
        path = build_dataset_file_path(
            _ETF_AW_MARKET_FEATURES_DATASET,
            StorageZone.DERIVED,
            [("year", year), ("month", f"{month:02d}")],
            lakehouse_root=lakehouse_root,
        )
        if path.exists():
            frames.append(pd.read_parquet(path))
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def _read_etf_aw_strategy_context_partitions(
    start: date | None = None,
    end: date | None = None,
    *,
    lakehouse_root: Path | None = None,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for year, month in _dataset_months(
        _ETF_AW_STRATEGY_CONTEXT_DATASET,
        start,
        end,
        lakehouse_root=lakehouse_root,
    ):
        path = build_dataset_file_path(
            _ETF_AW_STRATEGY_CONTEXT_DATASET,
            StorageZone.DERIVED,
            [("year", year), ("month", f"{month:02d}")],
            lakehouse_root=lakehouse_root,
        )
        if path.exists():
            frames.append(pd.read_parquet(path))
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def _read_etf_aw_risk_budget_partitions(
    start: date | None = None,
    end: date | None = None,
    *,
    lakehouse_root: Path | None = None,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for year, month in _dataset_months(
        _ETF_AW_RISK_BUDGET_DATASET,
        start,
        end,
        lakehouse_root=lakehouse_root,
    ):
        path = build_dataset_file_path(
            _ETF_AW_RISK_BUDGET_DATASET,
            StorageZone.DERIVED,
            [("year", year), ("month", f"{month:02d}")],
            lakehouse_root=lakehouse_root,
        )
        if path.exists():
            frames.append(pd.read_parquet(path))
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def _read_etf_aw_target_weight_partitions(
    start: date | None = None,
    end: date | None = None,
    *,
    lakehouse_root: Path | None = None,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for year, month in _dataset_months(
        _ETF_AW_TARGET_WEIGHT_DATASET,
        start,
        end,
        lakehouse_root=lakehouse_root,
    ):
        path = build_dataset_file_path(
            _ETF_AW_TARGET_WEIGHT_DATASET,
            StorageZone.DERIVED,
            [("year", year), ("month", f"{month:02d}")],
            lakehouse_root=lakehouse_root,
        )
        if path.exists():
            frames.append(pd.read_parquet(path))
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def _read_daily_rates_partitions(
    start: date | None = None,
    end: date | None = None,
    *,
    lakehouse_root: Path | None = None,
) -> pd.DataFrame:
    frame = _read_partitioned_lakehouse_dataset(
        _RATES_DAILY_RATES_DATASET,
        StorageZone.NORMALIZED,
        start,
        end,
        lakehouse_root=lakehouse_root,
    )
    if frame.empty:
        return frame
    return _normalize_rate_fact_frame(frame, observation_date_column="trade_date")


def _read_macro_slow_fields_partitions(
    start: date | None = None,
    end: date | None = None,
    *,
    lakehouse_root: Path | None = None,
) -> pd.DataFrame:
    frame = _read_partitioned_lakehouse_dataset(
        _MACRO_SLOW_FIELDS_DATASET,
        StorageZone.NORMALIZED,
        start,
        end,
        lakehouse_root=lakehouse_root,
    )
    if frame.empty:
        return frame
    return _normalize_macro_fact_frame(frame)


def _read_lpr_partitions(
    start: date | None = None,
    end: date | None = None,
    *,
    lakehouse_root: Path | None = None,
) -> pd.DataFrame:
    frame = _read_partitioned_lakehouse_dataset(
        _RATES_LPR_DATASET,
        StorageZone.NORMALIZED,
        start,
        end,
        lakehouse_root=lakehouse_root,
    )
    if frame.empty:
        return frame
    return _normalize_rate_fact_frame(frame, observation_date_column="quote_date")


def _read_gov_curve_points_partitions(
    start: date | None = None,
    end: date | None = None,
    *,
    lakehouse_root: Path | None = None,
) -> pd.DataFrame:
    frame = _read_partitioned_lakehouse_dataset(
        _RATES_GOV_CURVE_POINTS_DATASET,
        StorageZone.NORMALIZED,
        start,
        end,
        lakehouse_root=lakehouse_root,
    )
    if frame.empty:
        return frame
    return _normalize_rate_fact_frame(frame, observation_date_column="curve_date")


def _read_partitioned_lakehouse_dataset(
    dataset_name: str,
    zone: StorageZone,
    start: date | None,
    end: date | None,
    *,
    lakehouse_root: Path | None,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for year, month in _dataset_months_in_zone(
        dataset_name,
        zone,
        start,
        end,
        lakehouse_root=lakehouse_root,
    ):
        path = build_dataset_file_path(
            dataset_name,
            zone,
            [("year", year), ("month", f"{month:02d}")],
            lakehouse_root=lakehouse_root,
        )
        if path.exists():
            frames.append(pd.read_parquet(path))
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def _normalize_rate_fact_frame(
    frame: pd.DataFrame, *, observation_date_column: str
) -> pd.DataFrame:
    required = {
        "field_name",
        observation_date_column,
        "value",
        "unit",
        "field_role",
        "release_date",
        "effective_date",
        "revision_note",
        "source_caveat",
        "quality_status",
    }
    if not required.issubset(frame.columns):
        return pd.DataFrame()
    normalized = frame.copy()
    for column in (observation_date_column, "release_date", "effective_date"):
        normalized[column] = _normalize_date_series(normalized[column])
    if "ingested_at" in normalized.columns:
        normalized["ingested_at"] = pd.to_datetime(
            normalized["ingested_at"], errors="coerce"
        )
    normalized = normalized.dropna(subset=["field_name", "effective_date"])
    return normalized


def _normalize_macro_fact_frame(frame: pd.DataFrame) -> pd.DataFrame:
    required = {
        "field_name",
        "period_label",
        "period_type",
        "value",
        "unit",
        "field_role",
        "release_date",
        "effective_date",
        "revision_note",
        "source_caveat",
        "quality_status",
    }
    if not required.issubset(frame.columns):
        return pd.DataFrame()
    normalized = frame.copy()
    for column in ("release_date", "effective_date"):
        normalized[column] = _normalize_date_series(normalized[column])
    if "ingested_at" in normalized.columns:
        normalized["ingested_at"] = pd.to_datetime(
            normalized["ingested_at"], errors="coerce"
        )
    normalized = normalized.dropna(
        subset=["field_name", "period_label", "effective_date"]
    )
    return normalized


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


def _normalize_market_features_frame(frame: pd.DataFrame) -> pd.DataFrame:
    required = {
        "schema_version",
        "calendar_name",
        "calendar_month",
        "rebalance_date",
        "feature_name",
        "feature_scope",
        "feature_subject",
        "feature_value",
        "feature_status",
    }
    if not required.issubset(frame.columns):
        return pd.DataFrame()
    normalized = frame.copy()
    normalized["rebalance_date"] = _normalize_date_series(normalized["rebalance_date"])
    normalized = normalized.dropna(subset=["rebalance_date"])
    normalized = normalized[
        normalized["schema_version"].astype(str)
        == _ETF_AW_MARKET_FEATURES_SCHEMA_VERSION
    ].copy()
    return normalized


def _normalize_strategy_context_frame(frame: pd.DataFrame) -> pd.DataFrame:
    required = {
        "schema_version",
        "contract_version",
        "calendar_name",
        "calendar_month",
        "rebalance_date",
        "strategy_name",
        "strategy_version",
        "context_status",
    }
    if not required.issubset(frame.columns):
        return pd.DataFrame()
    normalized = frame.copy()
    normalized["rebalance_date"] = _normalize_date_series(normalized["rebalance_date"])
    if "effective_date" in normalized.columns:
        normalized["effective_date"] = _normalize_date_series(
            normalized["effective_date"]
        )
    normalized = normalized.dropna(subset=["rebalance_date"])
    normalized = normalized[
        (
            normalized["schema_version"].astype(str)
            == _ETF_AW_STRATEGY_CONTEXT_SCHEMA_VERSION
        )
        & (
            normalized["contract_version"].astype(str)
            == _ETF_AW_STRATEGY_CONTEXT_CONTRACT_VERSION
        )
    ].copy()
    return normalized


def _normalize_risk_budget_frame(frame: pd.DataFrame) -> pd.DataFrame:
    required = {
        "schema_version",
        "contract_version",
        "calendar_name",
        "rebalance_date",
        "strategy_name",
        "strategy_version",
        "sleeve_role",
        "base_budget",
        "tilted_budget",
        "budget_status",
    }
    if not required.issubset(frame.columns):
        return pd.DataFrame()
    normalized = frame.copy()
    normalized["rebalance_date"] = _normalize_date_series(normalized["rebalance_date"])
    for column in (
        "source_strategy_context_rebalance_date",
        "source_regime_rebalance_date",
    ):
        if column in normalized.columns:
            normalized[column] = _normalize_date_series(normalized[column])
    normalized = normalized.dropna(subset=["rebalance_date"])
    normalized = normalized[
        (normalized["schema_version"].astype(str) == _ETF_AW_RISK_BUDGET_SCHEMA_VERSION)
        & (
            normalized["contract_version"].astype(str)
            == _ETF_AW_RISK_BUDGET_CONTRACT_VERSION
        )
    ].copy()
    return normalized


def _normalize_target_weight_frame(frame: pd.DataFrame) -> pd.DataFrame:
    required = {
        "schema_version",
        "contract_version",
        "calendar_name",
        "rebalance_date",
        "strategy_name",
        "strategy_version",
        "sleeve_code",
        "sleeve_role",
        "risk_budget",
        "volatility_estimate",
        "volatility_floor",
        "raw_target_weight",
        "constrained_target_weight",
        "target_weight",
        "target_weight_status",
        "optimizer_name",
        "optimizer_basis",
        "quality_notes_json",
    }
    if not required.issubset(frame.columns):
        return pd.DataFrame()
    normalized = frame.copy()
    normalized["rebalance_date"] = _normalize_date_series(normalized["rebalance_date"])
    for column in (
        "effective_date",
        "source_risk_budget_rebalance_date",
        "source_sleeve_daily_max_trade_date",
    ):
        if column in normalized.columns:
            normalized[column] = _normalize_date_series(normalized[column])
    normalized = normalized.dropna(subset=["rebalance_date"])
    normalized = normalized[
        (
            normalized["schema_version"].astype(str)
            == _ETF_AW_TARGET_WEIGHT_SCHEMA_VERSION
        )
        & (
            normalized["contract_version"].astype(str)
            == _ETF_AW_TARGET_WEIGHT_CONTRACT_VERSION
        )
    ].copy()
    return normalized


def _filter_target_weight_strategy(
    frame: pd.DataFrame,
    *,
    calendar_name: str | None,
    strategy_name: str | None,
    strategy_version: str | None,
) -> pd.DataFrame:
    if frame.empty:
        return frame
    filtered = frame
    if calendar_name is not None:
        filtered = filtered[filtered["calendar_name"].astype(str) == calendar_name]
    if strategy_name is not None:
        filtered = filtered[filtered["strategy_name"].astype(str) == strategy_name]
    if strategy_version is not None:
        filtered = filtered[
            filtered["strategy_version"].astype(str) == strategy_version
        ]
    return filtered.copy()


def _sort_target_weight_rows(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame
    sorted_frame = frame.copy()
    if "ingested_at" in sorted_frame.columns:
        sorted_frame["ingested_at"] = pd.to_datetime(
            sorted_frame["ingested_at"], errors="coerce"
        )
    else:
        sorted_frame["ingested_at"] = pd.NaT
    return sorted_frame.sort_values(
        [
            "rebalance_date",
            "calendar_name",
            "strategy_name",
            "strategy_version",
            "sleeve_code",
            "ingested_at",
        ]
    )


def _dataset_months(
    dataset_name: str,
    start: date | None,
    end: date | None,
    *,
    lakehouse_root: Path | None,
) -> list[tuple[int, int]]:
    return _dataset_months_in_zone(
        dataset_name,
        StorageZone.DERIVED,
        start,
        end,
        lakehouse_root=lakehouse_root,
    )


def _dataset_months_in_zone(
    dataset_name: str,
    zone: StorageZone,
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
    dataset_root = (lakehouse_root / zone.value) if lakehouse_root is not None else None
    if dataset_root is None:
        from tradepilot.config import (
            LAKEHOUSE_DERIVED_ROOT,
            LAKEHOUSE_NORMALIZED_ROOT,
            LAKEHOUSE_RAW_ROOT,
        )

        dataset_root = {
            StorageZone.RAW: LAKEHOUSE_RAW_ROOT,
            StorageZone.NORMALIZED: LAKEHOUSE_NORMALIZED_ROOT,
            StorageZone.DERIVED: LAKEHOUSE_DERIVED_ROOT,
        }[zone]
    root = dataset_root / dataset_name
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

    return _dataset_months(
        _ETF_AW_SNAPSHOT_DATASET,
        start,
        end,
        lakehouse_root=lakehouse_root,
    )


def _latest_rates_effective_date(*, lakehouse_root: Path | None) -> date | None:
    macro = _read_macro_slow_fields_partitions(lakehouse_root=lakehouse_root)
    daily_rates = _read_daily_rates_partitions(lakehouse_root=lakehouse_root)
    lpr = _read_lpr_partitions(lakehouse_root=lakehouse_root)
    curve = _read_gov_curve_points_partitions(lakehouse_root=lakehouse_root)
    dates = []
    for frame in (macro, daily_rates, lpr, curve):
        if frame.empty or "effective_date" not in frame.columns:
            continue
        dates.extend(frame["effective_date"].dropna().tolist())
    return max(dates) if dates else None


def _rates_context_dates(
    macro: pd.DataFrame,
    daily_rates: pd.DataFrame,
    lpr: pd.DataFrame,
    curve: pd.DataFrame,
    start: date,
    end: date,
) -> list[date]:
    dates: set[date] = set()
    for frame in (macro, daily_rates, lpr, curve):
        if frame.empty or "effective_date" not in frame.columns:
            continue
        for value in frame["effective_date"].dropna().tolist():
            if start <= value <= end:
                dates.add(value)
    return sorted(dates)


def _macro_rates_context_contract(
    *,
    rebalance_date: date,
    macro: pd.DataFrame,
    daily_rates: pd.DataFrame,
    lpr: pd.DataFrame,
    curve: pd.DataFrame,
) -> dict[str, Any]:
    macro_fields = _latest_eligible_macro_rows(macro, rebalance_date)
    daily_selected = _latest_eligible_rate_rows(
        daily_rates,
        rebalance_date,
        source_dataset=_RATES_DAILY_RATES_DATASET,
        observation_date_column="trade_date",
    )
    lpr_selected = _latest_eligible_rate_rows(
        lpr,
        rebalance_date,
        source_dataset=_RATES_LPR_DATASET,
        observation_date_column="quote_date",
    )
    curve_fields = _latest_eligible_curve_rows(curve, rebalance_date)
    rates_fields = daily_selected + lpr_selected
    available_fields = macro_fields + rates_fields + curve_fields
    available_names = {str(field["field_name"]) for field in available_fields}
    missing_primary = sorted(_ETF_AW_PRIMARY_FIELDS - available_names)
    missing_confirmatory = sorted(_ETF_AW_CONFIRMATORY_FIELDS - available_names)
    stale_fields = _stale_macro_rate_fields(available_fields, rebalance_date)
    excluded_future = _future_effective_rows(
        macro,
        daily_rates,
        lpr,
        curve,
        rebalance_date,
    )
    if missing_primary:
        context_status = "unavailable"
    elif stale_fields:
        context_status = "stale"
    elif missing_confirmatory:
        context_status = "partial"
    else:
        context_status = "complete"
    source_caveats = _rate_caveats(available_fields, "source_caveat")
    revision_caveats = _rate_caveats(available_fields, "revision_note")
    return {
        "schema_version": _ETF_AW_MACRO_RATES_CONTEXT_SCHEMA_VERSION,
        "rebalance_date": rebalance_date.isoformat(),
        "context_status": context_status,
        "macro_fields": macro_fields,
        "rates_fields": rates_fields,
        "curve_fields": curve_fields,
        "available_fields": available_fields,
        "missing_primary_fields": missing_primary,
        "missing_confirmatory_fields": missing_confirmatory,
        "source_caveats": source_caveats,
        "revision_caveats": revision_caveats,
        "quality_notes": {
            "macro_fields_deferred": "official_pmi" not in available_names,
            "curve_fields_deferred": "cn_gov_10y_yield" not in available_names,
            "rates_primary_fields_available": {"shibor_1w", "lpr_1y"}.issubset(
                available_names
            ),
            "missing_primary_fields": missing_primary,
            "missing_confirmatory_fields": missing_confirmatory,
            "stale_fields": stale_fields,
            "field_freshness_max_age_days": _ETF_AW_FIELD_MAX_AGE_DAYS,
            "excluded_future_effective_fields": excluded_future,
            "point_in_time_filter": "effective_date <= rebalance_date",
            "latest_history_macro": bool(macro_fields),
        },
    }


def _latest_eligible_rate_rows(
    frame: pd.DataFrame,
    rebalance_date: date,
    *,
    source_dataset: str,
    observation_date_column: str,
) -> list[dict[str, Any]]:
    if frame.empty:
        return []
    eligible = frame[frame["effective_date"] <= rebalance_date].copy()
    if eligible.empty:
        return []
    sort_columns = ["field_name", "effective_date"]
    if "ingested_at" in eligible.columns:
        sort_columns.append("ingested_at")
    eligible = eligible.sort_values(sort_columns)
    latest = eligible.drop_duplicates(["field_name"], keep="last")
    return [
        _rate_field_contract(
            row,
            source_dataset=source_dataset,
            observation_date_column=observation_date_column,
        )
        for _, row in latest.sort_values("field_name").iterrows()
    ]


def _latest_eligible_macro_rows(
    frame: pd.DataFrame, rebalance_date: date
) -> list[dict[str, Any]]:
    if frame.empty:
        return []
    eligible = frame[frame["effective_date"] <= rebalance_date].copy()
    if eligible.empty:
        return []
    sort_columns = ["field_name", "effective_date"]
    if "ingested_at" in eligible.columns:
        sort_columns.append("ingested_at")
    eligible = eligible.sort_values(sort_columns)
    latest = eligible.drop_duplicates(["field_name"], keep="last")
    return [
        _macro_field_contract(row)
        for _, row in latest.sort_values("field_name").iterrows()
    ]


def _latest_eligible_curve_rows(
    frame: pd.DataFrame, rebalance_date: date
) -> list[dict[str, Any]]:
    if frame.empty:
        return []
    eligible = frame[frame["effective_date"] <= rebalance_date].copy()
    if eligible.empty:
        return []
    sort_columns = ["field_name", "effective_date"]
    if "ingested_at" in eligible.columns:
        sort_columns.append("ingested_at")
    eligible = eligible.sort_values(sort_columns)
    latest = eligible.drop_duplicates(["field_name"], keep="last")
    rows = [
        _rate_field_contract(
            row,
            source_dataset=_RATES_GOV_CURVE_POINTS_DATASET,
            observation_date_column="curve_date",
        )
        for _, row in latest.sort_values("field_name").iterrows()
    ]
    slope = _curve_slope_field(rows)
    if slope is not None:
        rows.append(slope)
    return rows


def _macro_field_contract(row: pd.Series) -> dict[str, Any]:
    return {
        "source_dataset": _MACRO_SLOW_FIELDS_DATASET,
        "field_name": _optional_text(row.get("field_name")),
        "field_role": _optional_text(row.get("field_role")),
        "period_label": _optional_text(row.get("period_label")),
        "period_type": _optional_text(row.get("period_type")),
        "value": _optional_float(row.get("value")),
        "unit": _optional_text(row.get("unit")),
        "release_date": _date_text(row.get("release_date")),
        "effective_date": _date_text(row.get("effective_date")),
        "revision_note": _optional_text(row.get("revision_note")),
        "source_caveat": _optional_text(row.get("source_caveat")),
        "quality_status": _optional_text(row.get("quality_status")),
    }


def _rate_field_contract(
    row: pd.Series,
    *,
    source_dataset: str,
    observation_date_column: str,
) -> dict[str, Any]:
    field = {
        "source_dataset": source_dataset,
        "field_name": _optional_text(row.get("field_name")),
        "field_role": _optional_text(row.get("field_role")),
        "value": _optional_float(row.get("value")),
        "unit": _optional_text(row.get("unit")),
        "release_date": _date_text(row.get("release_date")),
        "effective_date": _date_text(row.get("effective_date")),
        "revision_note": _optional_text(row.get("revision_note")),
        "source_caveat": _optional_text(row.get("source_caveat")),
        "quality_status": _optional_text(row.get("quality_status")),
    }
    field[observation_date_column] = _date_text(row.get(observation_date_column))
    return field


def _curve_slope_field(fields: list[dict[str, Any]]) -> dict[str, Any] | None:
    by_name = {field.get("field_name"): field for field in fields}
    one_year = by_name.get("cn_gov_1y_yield")
    ten_year = by_name.get("cn_gov_10y_yield")
    if one_year is None or ten_year is None:
        return None
    one_value = one_year.get("value")
    ten_value = ten_year.get("value")
    if one_value is None or ten_value is None:
        return None
    effective_dates = [
        value
        for value in (one_year.get("effective_date"), ten_year.get("effective_date"))
        if value is not None
    ]
    release_dates = [
        value
        for value in (one_year.get("release_date"), ten_year.get("release_date"))
        if value is not None
    ]
    return {
        "source_dataset": "derived.etf_aw_macro_rates_context",
        "field_name": "cn_yield_curve_slope_10y_1y",
        "field_role": "confirmatory",
        "value": float(ten_value) - float(one_value),
        "unit": "percentage_point",
        "release_date": max(release_dates) if release_dates else None,
        "effective_date": max(effective_dates) if effective_dates else None,
        "revision_note": "derived_from_latest_eligible_curve_points",
        "source_caveat": "derived_from_eligible_1y_10y_curve_points",
        "quality_status": "pass_with_caveat",
    }


def _future_effective_rows(
    macro: pd.DataFrame,
    daily_rates: pd.DataFrame,
    lpr: pd.DataFrame,
    curve: pd.DataFrame,
    rebalance_date: date,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for source_dataset, frame in (
        (_MACRO_SLOW_FIELDS_DATASET, macro),
        (_RATES_DAILY_RATES_DATASET, daily_rates),
        (_RATES_LPR_DATASET, lpr),
        (_RATES_GOV_CURVE_POINTS_DATASET, curve),
    ):
        if frame.empty:
            continue
        future = frame[frame["effective_date"] > rebalance_date]
        for _, row in future.sort_values(["field_name", "effective_date"]).iterrows():
            rows.append(
                {
                    "source_dataset": source_dataset,
                    "field_name": _optional_text(row.get("field_name")),
                    "effective_date": _date_text(row.get("effective_date")),
                    "rebalance_date": rebalance_date.isoformat(),
                }
            )
    return rows


def _rate_caveats(
    fields: list[dict[str, Any]], caveat_key: str
) -> list[dict[str, str | None]]:
    caveats: list[dict[str, str | None]] = []
    seen: set[tuple[str | None, str | None, str | None]] = set()
    for field in fields:
        key = (
            field.get("source_dataset"),
            field.get("field_name"),
            field.get(caveat_key),
        )
        if key in seen:
            continue
        seen.add(key)
        caveats.append(
            {
                "source_dataset": field.get("source_dataset"),
                "field_name": field.get("field_name"),
                caveat_key: field.get(caveat_key),
            }
        )
    return caveats


def _stale_macro_rate_fields(
    fields: list[dict[str, Any]], rebalance_date: date
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for field in fields:
        field_name = str(field.get("field_name") or "")
        max_age_days = _ETF_AW_FIELD_MAX_AGE_DAYS.get(field_name)
        if max_age_days is None:
            continue
        effective_date = _contract_date(field.get("effective_date"))
        if effective_date is None:
            continue
        age_days = (rebalance_date - effective_date).days
        if age_days <= max_age_days:
            continue
        rows.append(
            {
                "source_dataset": field.get("source_dataset"),
                "field_name": field_name,
                "effective_date": effective_date.isoformat(),
                "age_days": age_days,
                "max_age_days": max_age_days,
            }
        )
    return sorted(rows, key=lambda row: str(row["field_name"]))


def _contract_date(value: object) -> date | None:
    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        return None
    return parsed.date()


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


def _regime_contract(row: pd.Series) -> dict[str, Any]:
    return {
        "schema_version": _ETF_AW_REGIME_SCORE_SCHEMA_VERSION,
        "calendar_name": _optional_text(row.get("calendar_name")),
        "calendar_month": _optional_text(row.get("calendar_month")),
        "rebalance_date": _date_text(row["rebalance_date"]),
        "scorer_name": _optional_text(row.get("scorer_name")),
        "scorer_version": _optional_text(row.get("scorer_version")),
        "input_snapshot_status": _optional_text(row.get("input_snapshot_status")),
        "scoring_status": _optional_text(row.get("scoring_status")),
        "market_regime_label": _optional_text(row.get("market_regime_label")),
        "market_score": _optional_float(row.get("market_score")),
        "confidence_score": _optional_float(row.get("confidence_score")),
        "confidence_level": _optional_text(row.get("confidence_level")),
        "confidence_cap": _optional_float(row.get("confidence_cap")),
        "signal_summary": _optional_text(row.get("signal_summary")),
        "signals": _json_list(row.get("signals_json")),
        "quality_notes": _quality_notes(row.get("quality_notes")),
        "source_snapshot_rebalance_date": _date_text(
            row.get("source_snapshot_rebalance_date")
        ),
    }


def _strategy_context_contract(row: pd.Series) -> dict[str, Any]:
    missing_primary = _json_list(row.get("missing_primary_fields_json"))
    missing_confirmatory = _json_list(row.get("missing_confirmatory_fields_json"))
    available_fields = _json_list(row.get("available_fields_json"))
    source_caveats = _json_list(row.get("source_caveats_json"))
    revision_caveats = _json_list(row.get("revision_caveats_json"))
    point_in_time_notes = _quality_notes(row.get("point_in_time_notes_json"))
    market_features = _quality_notes(row.get("market_features_json"))
    return {
        "schema_version": _ETF_AW_STRATEGY_CONTEXT_SCHEMA_VERSION,
        "contract_version": _ETF_AW_STRATEGY_CONTEXT_CONTRACT_VERSION,
        "calendar_name": _optional_text(row.get("calendar_name")),
        "calendar_month": _optional_text(row.get("calendar_month")),
        "rebalance_date": _date_text(row.get("rebalance_date")),
        "effective_date": _date_text(row.get("effective_date")),
        "strategy_name": _optional_text(row.get("strategy_name")),
        "strategy_version": _optional_text(row.get("strategy_version")),
        "context_status": _optional_text(row.get("context_status")),
        "readiness_level": _optional_text(row.get("readiness_level")),
        "context_basis": _optional_text(row.get("context_basis")),
        "market_context_status": _optional_text(row.get("market_context_status")),
        "market": {
            "label": _optional_text(row.get("market_regime_label")),
            "score": _optional_float(row.get("market_score")),
            "confidence_score": _optional_float(row.get("market_confidence_score")),
            "confidence_cap": _optional_float(row.get("market_confidence_cap")),
        },
        "macro_rates": {
            "status": _optional_text(row.get("macro_rates_context_status")),
            "missing_primary_fields": missing_primary,
            "missing_confirmatory_fields": missing_confirmatory,
            "available_fields": available_fields,
            "source_caveats": source_caveats,
            "revision_caveats": revision_caveats,
        },
        "quality_notes": point_in_time_notes,
        "market_features": market_features,
        "source_snapshot_rebalance_date": _date_text(
            row.get("source_snapshot_rebalance_date")
        ),
        "source_regime_rebalance_date": _date_text(
            row.get("source_regime_rebalance_date")
        ),
        "source_macro_rates_rebalance_date": _date_text(
            row.get("source_macro_rates_rebalance_date")
        ),
    }


def _risk_budget_contract(frame: pd.DataFrame) -> dict[str, Any]:
    ordered = frame.sort_values("sleeve_role", key=_etf_aw_role_sort_key)
    first = ordered.iloc[0]
    base_sum = float(ordered["base_budget"].astype(float).sum())
    tilted_sum = float(ordered["tilted_budget"].astype(float).sum())
    return {
        "schema_version": _ETF_AW_RISK_BUDGET_SCHEMA_VERSION,
        "contract_version": _ETF_AW_RISK_BUDGET_CONTRACT_VERSION,
        "calendar_name": _optional_text(first.get("calendar_name")),
        "rebalance_date": _date_text(first.get("rebalance_date")),
        "strategy_name": _optional_text(first.get("strategy_name")),
        "strategy_version": _optional_text(first.get("strategy_version")),
        "market_regime_label": _optional_text(first.get("market_regime_label")),
        "budget_status": _optional_text(first.get("budget_status")),
        "budget_basis": _optional_text(first.get("budget_basis")),
        "confidence_score": _optional_float(first.get("confidence_score")),
        "effective_confidence_score": _optional_float(
            first.get("effective_confidence_score")
        ),
        "base_budget_sum": round(base_sum, 6),
        "tilted_budget_sum": round(tilted_sum, 6),
        "budgets": [_risk_budget_sleeve_contract(row) for _, row in ordered.iterrows()],
        "quality_notes": _quality_notes(first.get("quality_notes_json")),
        "source_strategy_context_rebalance_date": _date_text(
            first.get("source_strategy_context_rebalance_date")
        ),
        "source_regime_rebalance_date": _date_text(
            first.get("source_regime_rebalance_date")
        ),
    }


def _risk_budget_sleeve_contract(row: pd.Series) -> dict[str, Any]:
    return {
        "sleeve_role": _optional_text(row.get("sleeve_role")),
        "base_budget": _optional_float(row.get("base_budget")),
        "delta_budget": _optional_float(row.get("delta_budget")),
        "tilted_budget": _optional_float(row.get("tilted_budget")),
        "budget_status": _optional_text(row.get("budget_status")),
        "quality_notes": _quality_notes(row.get("quality_notes_json")),
    }


def _target_weight_contract(frame: pd.DataFrame) -> dict[str, Any]:
    ordered = frame.sort_values("sleeve_role", key=_etf_aw_role_sort_key)
    first = ordered.iloc[0]
    return {
        "schema_version": _ETF_AW_TARGET_WEIGHT_SCHEMA_VERSION,
        "contract_version": _ETF_AW_TARGET_WEIGHT_CONTRACT_VERSION,
        "calendar_name": _optional_text(first.get("calendar_name")),
        "rebalance_date": _date_text(first.get("rebalance_date")),
        "effective_date": _date_text(first.get("effective_date")),
        "strategy_name": _optional_text(first.get("strategy_name")),
        "strategy_version": _optional_text(first.get("strategy_version")),
        "target_weight_status": _target_weight_group_status(ordered),
        "optimizer_name": _single_or_mixed_text(ordered["optimizer_name"]),
        "optimizer_basis": _single_or_mixed_text(ordered["optimizer_basis"]),
        "risk_budget_sum": round(float(ordered["risk_budget"].astype(float).sum()), 6),
        "raw_target_weight_sum": round(
            float(ordered["raw_target_weight"].astype(float).sum()), 6
        ),
        "constrained_target_weight_sum": round(
            float(ordered["constrained_target_weight"].astype(float).sum()), 6
        ),
        "target_weight_sum": round(
            float(ordered["target_weight"].astype(float).sum()), 6
        ),
        "weights": [
            _target_weight_sleeve_contract(row) for _, row in ordered.iterrows()
        ],
        "quality_notes": _target_weight_group_quality_notes(ordered),
        "source_risk_budget_rebalance_date": _date_text(
            first.get("source_risk_budget_rebalance_date")
        ),
    }


def _target_weight_group_status(frame: pd.DataFrame) -> str | None:
    statuses = set(frame["target_weight_status"].dropna().astype(str).tolist())
    if not statuses:
        return None
    if statuses == {"complete"}:
        return "complete"
    if "unavailable" in statuses:
        return "unavailable"
    if "missing" in statuses:
        return "missing"
    if "stale" in statuses:
        return "stale"
    return "partial"


def _single_or_mixed_text(series: pd.Series) -> str | None:
    values = sorted(set(series.dropna().astype(str).tolist()))
    if not values:
        return None
    return values[0] if len(values) == 1 else "mixed"


def _target_weight_group_quality_notes(frame: pd.DataFrame) -> dict[str, Any]:
    reasons: set[str] = set()
    sleeves: dict[str, Any] = {}
    for _, row in frame.iterrows():
        notes = _quality_notes(row.get("quality_notes_json"))
        for reason in notes.get("reasons", []):
            reasons.add(str(reason))
        sleeve_role = _optional_text(row.get("sleeve_role"))
        if sleeve_role is not None:
            sleeves[sleeve_role] = notes
    return {
        "reasons": sorted(reasons),
        "sleeves": sleeves,
    }


def _target_weight_sleeve_contract(row: pd.Series) -> dict[str, Any]:
    return {
        "sleeve_code": _optional_text(row.get("sleeve_code")),
        "sleeve_role": _optional_text(row.get("sleeve_role")),
        "risk_budget": _optional_float(row.get("risk_budget")),
        "volatility_estimate": _optional_float(row.get("volatility_estimate")),
        "volatility_floor": _optional_float(row.get("volatility_floor")),
        "raw_target_weight": _optional_float(row.get("raw_target_weight")),
        "constrained_target_weight": _optional_float(
            row.get("constrained_target_weight")
        ),
        "target_weight": _optional_float(row.get("target_weight")),
        "target_weight_status": _optional_text(row.get("target_weight_status")),
        "turnover_estimate": _optional_float(row.get("turnover_estimate")),
        "quality_notes": _quality_notes(row.get("quality_notes_json")),
        "source_sleeve_daily_max_trade_date": _date_text(
            row.get("source_sleeve_daily_max_trade_date")
        ),
    }


def _market_features_contract(frame: pd.DataFrame) -> dict[str, Any]:
    ordered = frame.sort_values(["feature_scope", "feature_subject", "feature_name"])
    first = ordered.iloc[0]
    return {
        "schema_version": _ETF_AW_MARKET_FEATURES_SCHEMA_VERSION,
        "calendar_name": _optional_text(first.get("calendar_name")),
        "calendar_month": _optional_text(first.get("calendar_month")),
        "rebalance_date": _date_text(first.get("rebalance_date")),
        "features": [_market_feature_contract(row) for _, row in ordered.iterrows()],
    }


def _market_feature_contract(row: pd.Series) -> dict[str, Any]:
    return {
        "feature_name": _optional_text(row.get("feature_name")),
        "feature_scope": _optional_text(row.get("feature_scope")),
        "feature_subject": _optional_text(row.get("feature_subject")),
        "feature_value": _optional_float(row.get("feature_value")),
        "unit": _optional_text(row.get("unit")),
        "source_dataset": _optional_text(row.get("source_dataset")),
        "source_status": _optional_text(row.get("source_status")),
        "feature_status": _optional_text(row.get("feature_status")),
        "quality_notes": _quality_notes(row.get("quality_notes")),
        "source_rebalance_date": _date_text(row.get("source_rebalance_date")),
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


def _sort_latest_rows(frame: pd.DataFrame) -> pd.DataFrame:
    if "ingested_at" not in frame.columns:
        return frame.sort_values(["rebalance_date"])
    sorted_frame = frame.copy()
    sorted_frame["ingested_at"] = pd.to_datetime(
        sorted_frame["ingested_at"], errors="coerce"
    )
    return sorted_frame.sort_values(["rebalance_date", "ingested_at"])


def _quality_notes(value: object) -> dict[str, Any]:
    if not isinstance(value, str) or not value.strip():
        return {}
    try:
        loaded = json.loads(value)
    except json.JSONDecodeError:
        return {"raw": value}
    return loaded if isinstance(loaded, dict) else {"value": loaded}


def _json_list(value: object) -> list[Any]:
    if not isinstance(value, str) or not value.strip():
        return []
    try:
        loaded = json.loads(value)
    except json.JSONDecodeError:
        return [{"raw": value}]
    return loaded if isinstance(loaded, list) else [loaded]


def _normalize_date_series(series: pd.Series) -> pd.Series:
    parsed = pd.to_datetime(series, errors="coerce")
    return pd.Series(
        [value.date() if not pd.isna(value) else None for value in parsed],
        index=series.index,
        dtype=object,
    )


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


def _optional_text(value: object) -> str | None:
    if value is None or pd.isna(value):
        return None
    return str(value)
