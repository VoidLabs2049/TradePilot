"""CLI checks for ETF all-weather lakehouse data consistency."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
import json
import math

import click
import duckdb
import pandas as pd

from tradepilot.config import DB_PATH, LAKEHOUSE_ROOT
from tradepilot.etl.read_models import (
    get_latest_etf_aw_macro_rates_context,
    get_latest_etf_aw_market_features,
    get_latest_etf_aw_regime_context,
    get_latest_etf_aw_snapshot,
    get_latest_etf_aw_strategy_context,
)

_V1_CODES = {"510300.SH", "159845.SZ", "511010.SH", "518850.SH", "159001.SZ"}
_V1_ROLES = {"equity_large", "equity_small", "bond", "gold", "cash"}
_SNAPSHOT_STATUSES = {"complete", "partial", "missing", "stale"}
_FEATURE_STATUSES = {"complete", "partial", "missing", "stale"}
_STRATEGY_STATUSES = {"complete", "partial", "stale", "unavailable"}


@dataclass(frozen=True)
class CheckResult:
    """One data check result."""

    name: str
    passed: bool
    detail: str = ""


@dataclass(frozen=True)
class LoadedData:
    """ETF all-weather tables loaded from DuckDB and lakehouse parquet."""

    sleeves: pd.DataFrame
    calendar: pd.DataFrame
    rebalance_calendar: pd.DataFrame
    watermarks: pd.DataFrame
    manifest_ids: set[int]
    market: pd.DataFrame
    adj: pd.DataFrame
    sleeve_daily: pd.DataFrame
    snapshot: pd.DataFrame
    regime: pd.DataFrame
    features: pd.DataFrame
    strategy: pd.DataFrame
    macro: pd.DataFrame
    daily_rates: pd.DataFrame
    lpr: pd.DataFrame
    curve: pd.DataFrame


def run_checks(
    db_path: Path, lakehouse_root: Path
) -> tuple[list[CheckResult], list[str]]:
    """Run ETF all-weather local data checks."""

    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        data = _load_data(conn, lakehouse_root)
        results: list[CheckResult] = []
        _check_reference_data(results, data)
        _check_dataset_keys(results, data)
        _check_market_data(results, data)
        _check_derived_data(results, data)
        _check_macro_rates_data(results, data)
        _check_lineage(results, data)
        _check_read_models(results, lakehouse_root)
        warnings = _orphan_raw_file_warnings(conn, lakehouse_root)
        return results, warnings
    finally:
        conn.close()


@click.command()
@click.option(
    "--db-path",
    type=click.Path(path_type=Path, dir_okay=False),
    default=DB_PATH,
    show_default=True,
    help="DuckDB metadata database path.",
)
@click.option(
    "--lakehouse-root",
    type=click.Path(path_type=Path, file_okay=False),
    default=LAKEHOUSE_ROOT,
    show_default=True,
    help="Lakehouse root containing raw/normalized/derived zones.",
)
def main(db_path: Path, lakehouse_root: Path) -> None:
    """Check ETF all-weather data consistency and exit non-zero on failure."""

    results, warnings = run_checks(db_path=db_path, lakehouse_root=lakehouse_root)
    failures = [result for result in results if not result.passed]
    for result in results:
        status = "PASS" if result.passed else "FAIL"
        suffix = f" - {result.detail}" if result.detail else ""
        click.echo(f"{status} {result.name}{suffix}")
    for warning in warnings:
        click.echo(f"WARN {warning}")
    click.echo(
        f"checks_total={len(results)} failures={len(failures)} warnings={len(warnings)}"
    )
    if failures:
        raise click.ClickException("ETF all-weather data checks failed")


def _load_data(conn: duckdb.DuckDBPyConnection, lakehouse_root: Path) -> LoadedData:
    sleeves = conn.execute(
        """
        SELECT sleeve_code, sleeve_role, listing_exchange, exposure_note, is_active
        FROM canonical_sleeves
        WHERE is_active = TRUE
        """
    ).fetchdf()
    calendar = conn.execute(
        "SELECT exchange, trade_date, is_open FROM canonical_trading_calendar"
    ).fetchdf()
    calendar["trade_date"] = _date_series(calendar, "trade_date")
    rebalance_calendar = conn.execute(
        """
        SELECT calendar_name, calendar_month, rebalance_date, effective_date
        FROM canonical_rebalance_calendar
        """
    ).fetchdf()
    watermarks = conn.execute(
        "SELECT dataset_name, latest_fetched_date FROM etl_source_watermarks"
    ).fetchdf()
    manifest_ids = {
        int(value)
        for (value,) in conn.execute(
            "SELECT raw_batch_id FROM etl_raw_batches"
        ).fetchall()
    }
    return LoadedData(
        sleeves=sleeves,
        calendar=calendar,
        rebalance_calendar=rebalance_calendar,
        watermarks=watermarks,
        manifest_ids=manifest_ids,
        market=_read_dataset(lakehouse_root, "normalized", "market.etf_daily"),
        adj=_read_dataset(lakehouse_root, "normalized", "market.etf_adj_factor"),
        sleeve_daily=_read_dataset(
            lakehouse_root, "derived", "derived.etf_aw_sleeve_daily"
        ),
        snapshot=_read_dataset(
            lakehouse_root, "derived", "derived.etf_aw_rebalance_snapshot"
        ),
        regime=_read_dataset(lakehouse_root, "derived", "derived.etf_aw_regime_score"),
        features=_read_dataset(
            lakehouse_root, "derived", "derived.etf_aw_market_features"
        ),
        strategy=_read_dataset(
            lakehouse_root, "derived", "derived.etf_aw_strategy_context"
        ),
        macro=_read_dataset(lakehouse_root, "normalized", "macro.slow_fields"),
        daily_rates=_read_dataset(lakehouse_root, "normalized", "rates.daily_rates"),
        lpr=_read_dataset(lakehouse_root, "normalized", "rates.lpr"),
        curve=_read_dataset(lakehouse_root, "normalized", "rates.gov_curve_points"),
    )


def _check_reference_data(results: list[CheckResult], data: LoadedData) -> None:
    _add(
        results,
        "canonical_sleeves_exact_v1_codes",
        set(data.sleeves["sleeve_code"]) == _V1_CODES,
        str(sorted(data.sleeves["sleeve_code"].astype(str).tolist())),
    )
    _add(
        results,
        "canonical_sleeves_exact_v1_roles",
        set(data.sleeves["sleeve_role"]) == _V1_ROLES,
        str(sorted(data.sleeves["sleeve_role"].astype(str).tolist())),
    )
    _add(
        results,
        "canonical_sleeves_no_511020",
        "511020.SH" not in set(data.sleeves["sleeve_code"].astype(str)),
    )
    _add(
        results,
        "canonical_sleeves_exposure_notes",
        bool(data.sleeves["exposure_note"].astype(str).str.strip().all()),
    )
    _add(
        results,
        "canonical_sleeves_exchange_suffix",
        bool(
            (
                data.sleeves["sleeve_code"].astype(str).str.rsplit(".", n=1).str[-1]
                == data.sleeves["listing_exchange"].astype(str)
            ).all()
        ),
    )
    may_20 = date(2026, 5, 20)
    may_20_open = data.calendar[
        (data.calendar["trade_date"] == may_20) & (data.calendar["is_open"] == True)
    ]
    _add(
        results,
        "calendar_2026_05_20_open_both_exchanges",
        set(may_20_open["exchange"]) == {"SH", "SZ"},
        str(may_20_open[["exchange", "trade_date", "is_open"]].to_dict("records")),
    )
    _add(
        results,
        "rebalance_calendar_no_duplicate_month",
        int(
            data.rebalance_calendar.duplicated(
                ["calendar_name", "calendar_month"]
            ).sum()
        )
        == 0,
    )


def _check_dataset_keys(results: list[CheckResult], data: LoadedData) -> None:
    datasets = [
        ("market_etf_daily", data.market, ["instrument_id", "trade_date"]),
        ("market_etf_adj_factor", data.adj, ["instrument_id", "trade_date"]),
        ("derived_sleeve_daily", data.sleeve_daily, ["sleeve_code", "trade_date"]),
        (
            "rebalance_snapshot",
            data.snapshot,
            ["calendar_name", "rebalance_date", "sleeve_code"],
        ),
        (
            "regime_score",
            data.regime,
            ["calendar_name", "rebalance_date", "scorer_name", "scorer_version"],
        ),
        (
            "market_features",
            data.features,
            [
                "calendar_name",
                "rebalance_date",
                "feature_name",
                "feature_scope",
                "feature_subject",
            ],
        ),
        (
            "strategy_context",
            data.strategy,
            ["calendar_name", "rebalance_date", "strategy_name", "strategy_version"],
        ),
        ("macro_slow_fields", data.macro, ["field_name", "period_label"]),
        ("daily_rates", data.daily_rates, ["field_name", "trade_date"]),
        ("lpr", data.lpr, ["field_name", "quote_date"]),
        ("gov_curve_points", data.curve, ["curve_code", "curve_date", "tenor_years"]),
    ]
    for name, frame, keys in datasets:
        _add(results, f"{name}_non_empty", not frame.empty, f"rows={len(frame)}")
        if frame.empty:
            continue
        duplicate_count = int(frame.duplicated(keys).sum())
        _add(
            results,
            f"{name}_no_duplicate_business_keys",
            duplicate_count == 0,
            f"duplicates={duplicate_count}",
        )


def _check_market_data(results: list[CheckResult], data: LoadedData) -> None:
    _normalize_date_columns(data.market, ["trade_date"])
    _normalize_date_columns(data.adj, ["trade_date"])
    _normalize_date_columns(data.sleeve_daily, ["trade_date"])
    _add(
        results,
        "market_etf_daily_v1_only",
        set(data.market["instrument_id"].astype(str)) == _V1_CODES,
    )
    _add(
        results,
        "market_etf_adj_factor_v1_only",
        set(data.adj["instrument_id"].astype(str)) == _V1_CODES,
    )
    _add(
        results,
        "sleeve_daily_v1_only",
        set(data.sleeve_daily["sleeve_code"].astype(str)) == _V1_CODES,
    )
    open_days = data.calendar[data.calendar["is_open"] == True].loc[
        :, ["exchange", "trade_date"]
    ]
    market = data.market.assign(
        exchange=data.market["instrument_id"].astype(str).str.rsplit(".", n=1).str[-1]
    )
    joined = market.merge(
        open_days.assign(_open=True), on=["exchange", "trade_date"], how="left"
    )
    missing_open_days = int(joined["_open"].isna().sum())
    _add(
        results,
        "market_trade_dates_are_open_days",
        missing_open_days == 0,
        f"missing={missing_open_days}",
    )
    watermark_map = _watermark_map(data.watermarks)
    _add(
        results,
        "market_etf_daily_watermark_covers_data",
        max(data.market["trade_date"]) <= watermark_map.get("market.etf_daily"),
    )
    _add(
        results,
        "market_etf_adj_factor_watermark_covers_data",
        max(data.adj["trade_date"]) <= watermark_map.get("market.etf_adj_factor"),
    )
    _add(
        results,
        "calendar_watermark_covers_data",
        max(data.calendar["trade_date"])
        <= watermark_map.get("reference.trading_calendar"),
    )
    for column in ("open", "high", "low", "close", "pre_close", "volume", "amount"):
        values = pd.to_numeric(data.market[column], errors="coerce").dropna()
        _add(
            results, f"market_{column}_non_negative_or_null", bool((values >= 0).all())
        )
    _add(
        results,
        "market_ohlc_order",
        bool(
            (
                data.market["high"] >= data.market[["open", "low", "close"]].max(axis=1)
            ).all()
            and (
                data.market["low"] <= data.market[["open", "high", "close"]].min(axis=1)
            ).all()
        ),
    )
    joined_adj = data.sleeve_daily.merge(
        data.adj.rename(columns={"instrument_id": "sleeve_code"}).loc[
            :, ["sleeve_code", "trade_date", "adj_factor"]
        ],
        on=["sleeve_code", "trade_date"],
        how="left",
        suffixes=("", "_source"),
    )
    _add(
        results,
        "sleeve_daily_adj_factor_matches_source",
        bool(
            (
                joined_adj["adj_factor"].round(10)
                == joined_adj["adj_factor_source"].round(10)
            ).all()
        ),
    )
    _add(
        results,
        "sleeve_daily_adj_close_formula",
        bool(
            (
                data.sleeve_daily["adj_close"]
                - data.sleeve_daily["close"] * data.sleeve_daily["adj_factor"]
            )
            .abs()
            .lt(1e-9)
            .all()
        ),
    )
    calc = data.sleeve_daily.sort_values(["sleeve_code", "trade_date"]).copy()
    calc["expected_adj_pct_chg"] = (
        calc.groupby("sleeve_code")["adj_close"].pct_change() * 100
    )
    both_null = calc["adj_pct_chg"].isna() & calc["expected_adj_pct_chg"].isna()
    within_tolerance = (calc["adj_pct_chg"] - calc["expected_adj_pct_chg"]).abs() < 1e-8
    _add(
        results,
        "sleeve_daily_adj_pct_chg_formula",
        bool((both_null | within_tolerance).all()),
        f"mismatches={int((~(both_null | within_tolerance)).sum())}",
    )


def _check_derived_data(results: list[CheckResult], data: LoadedData) -> None:
    _normalize_date_columns(data.snapshot, ["rebalance_date", "effective_date"])
    _normalize_date_columns(data.regime, ["rebalance_date"])
    _normalize_date_columns(data.features, ["rebalance_date"])
    _normalize_date_columns(data.strategy, ["rebalance_date", "effective_date"])
    _add(
        results,
        "snapshot_v1_only",
        set(data.snapshot["sleeve_code"].astype(str)) == _V1_CODES,
    )
    rows_per_snapshot = data.snapshot.groupby(["calendar_name", "rebalance_date"])[
        "sleeve_code"
    ].nunique()
    _add(
        results,
        "snapshot_five_rows_per_rebalance",
        bool((rows_per_snapshot == 5).all()),
    )
    _add(
        results,
        "snapshot_status_allowed",
        set(data.snapshot["data_status"].astype(str)).issubset(_SNAPSHOT_STATUSES),
    )
    complete = data.snapshot[data.snapshot["data_status"] == "complete"]
    feature_columns = [
        "return_1m",
        "return_3m",
        "return_6m",
        "volatility_3m",
        "max_drawdown_6m",
    ]
    _add(
        results,
        "snapshot_complete_rows_have_features",
        bool(complete.loc[:, feature_columns].notna().all().all()),
    )
    _add(
        results,
        "snapshot_quality_notes_json",
        all(_is_json_object(value) for value in data.snapshot["quality_notes"]),
    )
    _add(
        results,
        "regime_schema_version",
        set(data.regime["schema_version"].astype(str)) == {"etf_aw_regime_score_v1"},
    )
    _add(
        results,
        "regime_scores_finite",
        _finite_or_null(data.regime["market_score"])
        and _finite_or_null(data.regime["confidence_score"]),
    )
    _add(
        results,
        "regime_confidence_capped",
        bool((data.regime["confidence_score"] <= data.regime["confidence_cap"]).all()),
    )
    _add(
        results,
        "features_schema_version",
        set(data.features["schema_version"].astype(str))
        == {"etf_aw_market_features_v1"},
    )
    _add(
        results,
        "features_status_allowed",
        set(data.features["feature_status"].astype(str)).issubset(_FEATURE_STATUSES),
    )
    complete_features = data.features[data.features["feature_status"] == "complete"]
    _add(
        results,
        "features_complete_values_finite",
        _finite_or_null(complete_features["feature_value"]),
    )
    _add(
        results,
        "features_no_macro_rates_names",
        not any(
            any(
                token in str(name)
                for token in ("macro", "rate", "lpr", "curve", "yield")
            )
            for name in data.features["feature_name"]
        ),
    )
    _add(
        results,
        "strategy_schema_version",
        set(data.strategy["schema_version"].astype(str))
        == {"etf_aw_strategy_context_v1"},
    )
    _add(
        results,
        "strategy_contract_version",
        set(data.strategy["contract_version"].astype(str))
        == {"etf_aw_strategy_context_contract_v1"},
    )
    _add(
        results,
        "strategy_status_allowed",
        set(data.strategy["context_status"].astype(str)).issubset(_STRATEGY_STATUSES),
    )
    json_columns = [
        "missing_primary_fields_json",
        "missing_confirmatory_fields_json",
        "available_fields_json",
        "source_caveats_json",
        "revision_caveats_json",
        "point_in_time_notes_json",
        "market_features_json",
    ]
    _add(
        results,
        "strategy_json_fields_valid",
        all(
            _is_json(value)
            for column in json_columns
            for value in data.strategy[column]
        ),
    )


def _check_macro_rates_data(results: list[CheckResult], data: LoadedData) -> None:
    datasets = [
        ("macro", data.macro),
        ("daily_rates", data.daily_rates),
        ("lpr", data.lpr),
        ("curve", data.curve),
    ]
    for name, frame in datasets:
        if frame.empty:
            continue
        _normalize_date_columns(frame, ["release_date", "effective_date"])
        _add(
            results,
            f"{name}_release_date_present",
            bool(frame["release_date"].notna().all()),
        )
        _add(
            results,
            f"{name}_effective_date_present",
            bool(frame["effective_date"].notna().all()),
        )
        _add(
            results,
            f"{name}_effective_not_before_release",
            bool((frame["effective_date"] >= frame["release_date"]).all()),
        )
        _add(
            results,
            f"{name}_quality_status_present",
            bool(frame["quality_status"].astype(str).str.strip().ne("").all()),
        )


def _check_lineage(results: list[CheckResult], data: LoadedData) -> None:
    datasets = [
        ("market", data.market),
        ("adj", data.adj),
        ("macro", data.macro),
        ("daily_rates", data.daily_rates),
        ("lpr", data.lpr),
        ("curve", data.curve),
    ]
    for name, frame in datasets:
        if "raw_batch_id" not in frame.columns:
            continue
        ids = {int(value) for value in frame["raw_batch_id"].dropna().tolist()}
        missing = sorted(ids - data.manifest_ids)
        _add(
            results,
            f"{name}_raw_batch_ids_have_manifest",
            not missing,
            str(missing),
        )


def _check_read_models(results: list[CheckResult], lakehouse_root: Path) -> None:
    snapshot = get_latest_etf_aw_snapshot(lakehouse_root=lakehouse_root)
    regime = get_latest_etf_aw_regime_context(lakehouse_root=lakehouse_root)
    features = get_latest_etf_aw_market_features(lakehouse_root=lakehouse_root)
    strategy = get_latest_etf_aw_strategy_context(lakehouse_root=lakehouse_root)
    macro_rates = get_latest_etf_aw_macro_rates_context(lakehouse_root=lakehouse_root)
    _add(results, "read_model_snapshot_available", snapshot is not None)
    _add(
        results,
        "read_model_snapshot_has_five_sleeves",
        snapshot is not None and len(snapshot.get("sleeves", [])) == 5,
    )
    _add(results, "read_model_regime_available", regime is not None)
    _add(results, "read_model_market_features_available", features is not None)
    _add(results, "read_model_strategy_available", strategy is not None)
    _add(results, "read_model_macro_rates_available", macro_rates is not None)


def _read_dataset(lakehouse_root: Path, zone: str, dataset_name: str) -> pd.DataFrame:
    files = sorted(
        (lakehouse_root / zone / dataset_name).glob("*/*/part-00000.parquet")
    )
    if not files:
        return pd.DataFrame()
    return pd.concat([pd.read_parquet(path) for path in files], ignore_index=True)


def _date_series(frame: pd.DataFrame, column: str) -> pd.Series:
    return pd.to_datetime(frame[column], errors="coerce").dt.date


def _normalize_date_columns(frame: pd.DataFrame, columns: list[str]) -> None:
    for column in columns:
        if column in frame.columns:
            frame[column] = _date_series(frame, column)


def _watermark_map(watermarks: pd.DataFrame) -> dict[str, date]:
    mapping: dict[str, date] = {}
    for row in watermarks.itertuples(index=False):
        if pd.isna(row.latest_fetched_date):
            continue
        mapping[str(row.dataset_name)] = pd.to_datetime(row.latest_fetched_date).date()
    return mapping


def _finite_or_null(series: pd.Series) -> bool:
    values = pd.to_numeric(series.dropna(), errors="coerce")
    return bool(values.map(math.isfinite).all())


def _is_json(value: object) -> bool:
    if not isinstance(value, str):
        return False
    try:
        json.loads(value)
    except json.JSONDecodeError:
        return False
    return True


def _is_json_object(value: object) -> bool:
    if not isinstance(value, str):
        return False
    try:
        return isinstance(json.loads(value), dict)
    except json.JSONDecodeError:
        return False


def _orphan_raw_file_warnings(
    conn: duckdb.DuckDBPyConnection, lakehouse_root: Path
) -> list[str]:
    manifest_paths = {
        str(path)
        for (path,) in conn.execute(
            "SELECT storage_path FROM etl_raw_batches"
        ).fetchall()
    }
    raw_root = lakehouse_root / "raw"
    raw_files = sorted(
        set(raw_root.glob("*/*/*/*.parquet")) | set(raw_root.glob("*/*/*/*/*.parquet"))
    )
    warnings: list[str] = []
    for path in raw_files:
        rel_path = path.relative_to(lakehouse_root).as_posix()
        if rel_path not in manifest_paths:
            warnings.append(
                f"raw file is not referenced by etl_raw_batches: {rel_path}"
            )
    return warnings


def _add(results: list[CheckResult], name: str, passed: bool, detail: str = "") -> None:
    results.append(CheckResult(name=name, passed=passed, detail=detail))


if __name__ == "__main__":
    main()
