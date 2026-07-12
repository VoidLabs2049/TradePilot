"""CLI workflow for updating ETF all-weather source and derived data."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import click
import duckdb
import pandas as pd

from tradepilot import db as tradepilot_db
from tradepilot.config import DB_PATH, LAKEHOUSE_ROOT
from tradepilot.etl.models import IngestionRequest, StorageZone, TriggerMode
from tradepilot.etl.read_models import (
    get_latest_etf_aw_macro_rates_context,
    get_latest_etf_aw_snapshot,
)
from tradepilot.etl.service import ETLService

_V1_CODES = ("510300.SH", "159845.SZ", "511010.SH", "518850.SH", "159001.SZ")
_ETF_AW_HISTORY_START = date(2016, 1, 1)
_MACRO_RATES_HISTORY_START = date(2025, 1, 1)
_BOOTSTRAP_STARTS = {
    "reference.trading_calendar": _ETF_AW_HISTORY_START,
    "market.etf_daily": _ETF_AW_HISTORY_START,
    "market.etf_adj_factor": _ETF_AW_HISTORY_START,
    "macro.slow_fields": _MACRO_RATES_HISTORY_START,
    "rates.daily_rates": _MACRO_RATES_HISTORY_START,
    "rates.lpr": _MACRO_RATES_HISTORY_START,
    "rates.gov_curve_points": _MACRO_RATES_HISTORY_START,
}
_SOURCE_DATASETS = (
    "market.etf_daily",
    "market.etf_adj_factor",
    "macro.slow_fields",
    "rates.daily_rates",
    "rates.lpr",
    "rates.gov_curve_points",
)
_SOURCE_DATASET_COVERAGE = {
    "market.etf_daily": (StorageZone.NORMALIZED, "trade_date"),
    "market.etf_adj_factor": (StorageZone.NORMALIZED, "trade_date"),
    "macro.slow_fields": (StorageZone.NORMALIZED, "effective_date"),
    "rates.daily_rates": (StorageZone.NORMALIZED, "trade_date"),
    "rates.lpr": (StorageZone.NORMALIZED, "quote_date"),
    "rates.gov_curve_points": (StorageZone.NORMALIZED, "curve_date"),
}
_DERIVED_DATASET_COVERAGE = {
    "derived.etf_aw_sleeve_daily": (StorageZone.DERIVED, "trade_date"),
    "derived.etf_aw_rebalance_snapshot": (StorageZone.DERIVED, "rebalance_date"),
    "derived.etf_aw_regime_score": (StorageZone.DERIVED, "rebalance_date"),
    "derived.etf_aw_market_features": (StorageZone.DERIVED, "rebalance_date"),
    "derived.etf_aw_strategy_context": (StorageZone.DERIVED, "rebalance_date"),
    "derived.etf_aw_risk_budget": (StorageZone.DERIVED, "rebalance_date"),
    "derived.etf_aw_target_weight": (StorageZone.DERIVED, "rebalance_date"),
    "derived.etf_aw_baseline_weight": (StorageZone.DERIVED, "rebalance_date"),
    "derived.etf_aw_backtest_kernel": (StorageZone.DERIVED, "observation_date"),
    "derived.etf_aw_monthly_explainability": (StorageZone.DERIVED, "rebalance_date"),
}
_DERIVED_PROFILES = (
    "reference.rebalance_calendar.monthly_post_20",
    "derived.etf_aw_sleeve_daily.build",
    "derived.etf_aw_rebalance_snapshot.build",
    "derived.etf_aw_regime_score.build",
    "derived.etf_aw_market_features.build",
    "derived.etf_aw_strategy_context.build",
    "derived.etf_aw_risk_budget.build",
    "derived.etf_aw_target_weight.build",
    "derived.etf_aw_baseline_weight.build",
    "derived.etf_aw_backtest_kernel.build",
    "derived.etf_aw_monthly_explainability.build",
)
_SLEEVE_PROFILE = "reference.etf_aw_sleeves.frozen_v1"
_CALENDAR_DATASET = "reference.trading_calendar"


@dataclass(frozen=True)
class UpdatePlanItem:
    """One executable ETF all-weather update step."""

    kind: str
    name: str
    start: date | None = None
    end: date | None = None
    context: dict[str, Any] | None = None


@click.command()
@click.option(
    "--start",
    "start_text",
    type=str,
    default=None,
    help="Optional explicit start date. Defaults to watermark minus repair days.",
)
@click.option(
    "--end",
    "end_text",
    type=str,
    default=None,
    help="Optional end date. Defaults to today.",
)
@click.option(
    "--repair-days",
    type=int,
    default=45,
    show_default=True,
    help="Rolling lookback used when start is omitted.",
)
@click.option(
    "--codes",
    type=str,
    default=",".join(_V1_CODES),
    show_default=True,
    help="Comma-separated ETF codes to download.",
)
@click.option(
    "--dry-run",
    is_flag=True,
    help="Print the planned update steps without downloading or writing data.",
)
@click.option(
    "--full-refresh",
    is_flag=True,
    help="Ignore existing watermarks and local parquet coverage; rebuild from history starts.",
)
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
    help="Lakehouse root path.",
)
def main(
    start_text: str | None,
    end_text: str | None,
    repair_days: int,
    codes: str,
    dry_run: bool,
    full_refresh: bool,
    db_path: Path,
    lakehouse_root: Path,
) -> None:
    """Download, repair, and rebuild ETF all-weather data."""

    if repair_days < 0:
        raise click.BadParameter(
            "repair-days must not be negative", param_hint="--repair-days"
        )
    end = _parse_date(end_text, "end") if end_text else date.today()
    start = _parse_date(start_text, "start") if start_text else None
    etf_codes = _parse_codes(codes)
    conn = _connect_update_db(db_path, dry_run=dry_run)
    try:
        if not dry_run:
            tradepilot_db.initialize_schema(conn)
        plan = build_update_plan(
            conn=conn,
            end=end,
            start=start,
            repair_days=repair_days,
            codes=etf_codes,
            lakehouse_root=lakehouse_root,
            full_refresh=full_refresh,
        )
        _print_plan(plan)
        if dry_run:
            return
        service = ETLService(conn=conn, lakehouse_root=lakehouse_root)
        results = execute_update_plan(service, plan)
        _print_results(results)
        _print_freshness(conn, lakehouse_root)
    finally:
        conn.close()


def build_update_plan(
    *,
    conn: duckdb.DuckDBPyConnection,
    end: date,
    start: date | None,
    repair_days: int,
    codes: list[str],
    lakehouse_root: Path | None = None,
    full_refresh: bool = False,
) -> list[UpdatePlanItem]:
    """Build the source download and derived rebuild plan."""

    source_starts = {
        dataset_name: _dataset_start(
            conn,
            dataset_name,
            end,
            start,
            repair_days,
            lakehouse_root=lakehouse_root,
            full_refresh=full_refresh,
        )
        for dataset_name in _SOURCE_DATASETS
    }
    calendar_start = min(
        [
            _calendar_start(
                conn,
                end,
                start,
                repair_days,
                full_refresh=full_refresh,
            )
        ]
        + list(source_starts.values())
    )
    rebuild_start = min(
        min(source_starts.values()),
        _derived_rebuild_start(
            lakehouse_root=lakehouse_root,
            end=end,
            start=start,
            repair_days=repair_days,
            full_refresh=full_refresh,
        ),
    )
    plan = [
        UpdatePlanItem(kind="profile", name=_SLEEVE_PROFILE),
        UpdatePlanItem(
            kind="dataset",
            name=_CALENDAR_DATASET,
            start=calendar_start,
            end=end,
            context={"exchanges": ["SH", "SZ"]},
        ),
        UpdatePlanItem(
            kind="dataset",
            name="market.etf_daily",
            start=source_starts["market.etf_daily"],
            end=end,
            context={"instrument_ids": codes},
        ),
        UpdatePlanItem(
            kind="dataset",
            name="market.etf_adj_factor",
            start=source_starts["market.etf_adj_factor"],
            end=end,
            context={"instrument_ids": codes},
        ),
    ]
    for dataset_name in _SOURCE_DATASETS[2:]:
        plan.append(
            UpdatePlanItem(
                kind="dataset",
                name=dataset_name,
                start=source_starts[dataset_name],
                end=end,
            )
        )
    for profile_name in _DERIVED_PROFILES:
        plan.append(
            UpdatePlanItem(
                kind="profile",
                name=profile_name,
                start=rebuild_start,
                end=end,
            )
        )
    return plan


def execute_update_plan(
    service: ETLService, plan: list[UpdatePlanItem]
) -> list[dict[str, Any]]:
    """Execute an ETF all-weather update plan with the existing ETL service."""

    results: list[dict[str, Any]] = []
    for item in plan:
        if item.kind == "dataset":
            result = service.run_dataset_sync(
                item.name,
                IngestionRequest(
                    request_start=item.start,
                    request_end=item.end,
                    trigger_mode=TriggerMode.SCHEDULED,
                    context=item.context or {},
                ),
            )
            results.append(
                {
                    "kind": item.kind,
                    "name": item.name,
                    "status": result.status.value,
                    "records_written": result.records_written,
                    "error_message": result.error_message,
                }
            )
            continue
        if item.name == _SLEEVE_PROFILE:
            result = service.run_bootstrap(item.name)
        else:
            if item.start is None or item.end is None:
                raise ValueError(f"profile requires date window: {item.name}")
            result = service.run_bootstrap(item.name, start=item.start, end=item.end)
        results.append(
            {
                "kind": item.kind,
                "name": item.name,
                "status": str(result.get("status", "")),
                "records_written": int(result.get("records_written", 0) or 0),
                "error_message": result.get("error_message"),
            }
        )
    return results


def _connect_update_db(db_path: Path, *, dry_run: bool) -> duckdb.DuckDBPyConnection:
    """Open the update database without creating files during a missing-DB dry run."""

    if dry_run and not db_path.exists():
        return duckdb.connect(":memory:")
    if not dry_run:
        db_path.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(db_path), read_only=dry_run)


def _dataset_start(
    conn: duckdb.DuckDBPyConnection,
    dataset_name: str,
    end: date,
    start: date | None,
    repair_days: int,
    *,
    lakehouse_root: Path | None,
    full_refresh: bool,
) -> date:
    """Return the sync start for one dataset."""

    if start is not None:
        return start
    bootstrap_start = _bootstrap_start(dataset_name, end, repair_days)
    if full_refresh:
        return min(bootstrap_start, end)
    candidates = [
        _watermark_repair_start(conn, dataset_name, bootstrap_start, repair_days),
    ]
    if lakehouse_root is not None:
        zone, date_column = _SOURCE_DATASET_COVERAGE[dataset_name]
        candidates.append(
            _lakehouse_repair_start(
                lakehouse_root=lakehouse_root,
                dataset_name=dataset_name,
                zone=zone,
                date_column=date_column,
                bootstrap_start=bootstrap_start,
                end=end,
                repair_days=repair_days,
            )
        )
    return min(candidates)


def _calendar_start(
    conn: duckdb.DuckDBPyConnection,
    end: date,
    start: date | None,
    repair_days: int,
    *,
    full_refresh: bool,
) -> date:
    """Return the trading-calendar sync start."""

    if start is not None:
        return start
    bootstrap_start = _bootstrap_start(_CALENDAR_DATASET, end, repair_days)
    if full_refresh:
        return min(bootstrap_start, end)
    return min(
        _watermark_repair_start(conn, _CALENDAR_DATASET, bootstrap_start, repair_days),
        _calendar_repair_start(conn, bootstrap_start, end, repair_days),
    )


def _derived_rebuild_start(
    *,
    lakehouse_root: Path | None,
    end: date,
    start: date | None,
    repair_days: int,
    full_refresh: bool,
) -> date:
    """Return the earliest rebuild date required by local derived coverage."""

    if start is not None:
        return start
    if full_refresh or lakehouse_root is None:
        return _ETF_AW_HISTORY_START if full_refresh else end
    starts = []
    for dataset_name, (zone, date_column) in _DERIVED_DATASET_COVERAGE.items():
        starts.append(
            _lakehouse_repair_start(
                lakehouse_root=lakehouse_root,
                dataset_name=dataset_name,
                zone=zone,
                date_column=date_column,
                bootstrap_start=_ETF_AW_HISTORY_START,
                end=end,
                repair_days=repair_days,
            )
        )
    return min(starts)


def _bootstrap_start(dataset_name: str, end: date, repair_days: int) -> date:
    """Return the project-defined bootstrap start for one dataset."""

    return min(
        _BOOTSTRAP_STARTS.get(dataset_name, end - timedelta(days=repair_days)),
        end,
    )


def _watermark_repair_start(
    conn: duckdb.DuckDBPyConnection,
    dataset_name: str,
    bootstrap_start: date,
    repair_days: int,
) -> date:
    """Return the rolling repair start from the source watermark."""

    latest = _latest_fetched_date(conn, dataset_name)
    if latest is None:
        return bootstrap_start
    return max(latest - timedelta(days=repair_days), bootstrap_start)


def _latest_fetched_date(
    conn: duckdb.DuckDBPyConnection, dataset_name: str
) -> date | None:
    """Return the latest fetched date for one dataset, if metadata exists."""

    if not _table_exists(conn, "etl_source_watermarks"):
        return None
    row = conn.execute(
        """
        SELECT latest_fetched_date
        FROM etl_source_watermarks
        WHERE dataset_name = ?
        ORDER BY updated_at DESC
        LIMIT 1
        """,
        [dataset_name],
    ).fetchone()
    if row is None:
        return None
    return _coerce_date(row[0])


def _calendar_repair_start(
    conn: duckdb.DuckDBPyConnection,
    bootstrap_start: date,
    end: date,
    repair_days: int,
) -> date:
    """Return a conservative repair start from canonical calendar coverage."""

    if not _table_exists(conn, "canonical_trading_calendar"):
        return bootstrap_start
    row = conn.execute(
        """
        SELECT MIN(trade_date), MAX(trade_date), COUNT(DISTINCT exchange)
        FROM canonical_trading_calendar
        WHERE exchange IN ('SH', 'SZ')
        """,
    ).fetchone()
    if row is None or int(row[2] or 0) < 2:
        return bootstrap_start
    first = _coerce_date(row[0])
    latest = _coerce_date(row[1])
    if first is None or latest is None or first > bootstrap_start:
        return bootstrap_start
    if latest < end:
        return max(latest - timedelta(days=repair_days), bootstrap_start)
    return end


def _lakehouse_repair_start(
    *,
    lakehouse_root: Path,
    dataset_name: str,
    zone: StorageZone,
    date_column: str,
    bootstrap_start: date,
    end: date,
    repair_days: int,
) -> date:
    """Return a conservative repair start from local parquet coverage."""

    bounds = _lakehouse_date_bounds(lakehouse_root, dataset_name, zone, date_column)
    if bounds is None:
        return bootstrap_start
    first, latest = bounds
    if _starts_after_bootstrap_month(first, bootstrap_start):
        return bootstrap_start
    missing_month = _first_missing_partition_month(
        lakehouse_root, dataset_name, zone, first, latest
    )
    if missing_month is not None:
        return max(missing_month - timedelta(days=repair_days), bootstrap_start)
    if latest < end:
        return max(latest - timedelta(days=repair_days), bootstrap_start)
    return end


def _lakehouse_date_bounds(
    lakehouse_root: Path,
    dataset_name: str,
    zone: StorageZone,
    date_column: str,
) -> tuple[date, date] | None:
    """Return min/max date coverage for a partitioned lakehouse dataset."""

    dataset_root = lakehouse_root / zone.value / dataset_name
    files = sorted(dataset_root.glob("*/*/part-00000.parquet"))
    if not files:
        return None
    dates: list[date] = []
    for file_path in files:
        try:
            frame = pd.read_parquet(file_path, columns=[date_column])
        except Exception:
            return None
        if date_column not in frame.columns:
            return None
        series = pd.to_datetime(frame[date_column], errors="coerce").dropna()
        dates.extend(value.date() for value in series.tolist())
    if not dates:
        return None
    return min(dates), max(dates)


def _table_exists(conn: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    """Return whether a DuckDB table exists in the current connection."""

    count = conn.execute(
        "SELECT COUNT(*) FROM duckdb_tables() WHERE table_name = ?",
        [table_name],
    ).fetchone()[0]
    return int(count) > 0


def _coerce_date(value: object) -> date | None:
    """Convert a DuckDB/pandas date-like value into a date."""

    if value is None or pd.isna(value):
        return None
    if isinstance(value, date):
        return value
    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        return None
    return parsed.date()


def _starts_after_bootstrap_month(first: date, bootstrap_start: date) -> bool:
    """Return whether coverage starts after the bootstrap month."""

    first_month = date(first.year, first.month, 1)
    bootstrap_month = date(bootstrap_start.year, bootstrap_start.month, 1)
    return first_month > bootstrap_month


def _first_missing_partition_month(
    lakehouse_root: Path,
    dataset_name: str,
    zone: StorageZone,
    first: date,
    latest: date,
) -> date | None:
    """Return the first missing month partition between local date bounds."""

    dataset_root = lakehouse_root / zone.value / dataset_name
    current = date(first.year, first.month, 1)
    final = date(latest.year, latest.month, 1)
    while current <= final:
        path = dataset_root / f"{current.year:04d}" / f"{current.month:02d}"
        if not (path / "part-00000.parquet").exists():
            return current
        current = _next_month(current)
    return None


def _next_month(value: date) -> date:
    """Return the first day of the next calendar month."""

    if value.month == 12:
        return date(value.year + 1, 1, 1)
    return date(value.year, value.month + 1, 1)


def _print_plan(plan: list[UpdatePlanItem]) -> None:
    click.echo("ETF all-weather update plan:")
    for item in plan:
        window = (
            ""
            if item.start is None or item.end is None
            else f" {item.start}..{item.end}"
        )
        click.echo(f"- {item.kind}: {item.name}{window}")


def _print_results(results: list[dict[str, Any]]) -> None:
    click.echo("\nETF all-weather update results:")
    failures = []
    for result in results:
        error = result.get("error_message")
        suffix = f" error={error}" if error else ""
        click.echo(
            f"- {result['kind']}: {result['name']} status={result['status']} "
            f"records_written={result['records_written']}{suffix}"
        )
        if result["status"] not in {"success", "partial_success"}:
            failures.append(result)
    if failures:
        raise click.ClickException("ETF all-weather update finished with failures")


def _print_freshness(conn: duckdb.DuckDBPyConnection, lakehouse_root: Path) -> None:
    """Print post-update freshness markers for operator review."""

    click.echo("\nETF all-weather freshness:")
    rows = conn.execute("""
        SELECT dataset_name, latest_fetched_date
        FROM etl_source_watermarks
        WHERE dataset_name IN (
            'reference.trading_calendar',
            'market.etf_daily',
            'market.etf_adj_factor',
            'macro.slow_fields',
            'rates.daily_rates',
            'rates.lpr',
            'rates.gov_curve_points'
        )
        ORDER BY dataset_name
        """).fetchall()
    for dataset_name, latest_fetched_date in rows:
        click.echo(f"- {dataset_name}: latest_fetched_date={latest_fetched_date}")
    snapshot = get_latest_etf_aw_snapshot(lakehouse_root=lakehouse_root)
    if snapshot is not None:
        click.echo(f"- latest_snapshot_rebalance_date={snapshot['rebalance_date']}")
    macro_rates = get_latest_etf_aw_macro_rates_context(lakehouse_root=lakehouse_root)
    if macro_rates is None:
        click.echo("- macro_rates_context_status=unavailable")
        return
    status = str(macro_rates.get("context_status"))
    click.echo(
        f"- macro_rates_context_date={macro_rates.get('rebalance_date')} "
        f"status={status}"
    )
    if status != "complete":
        stale = macro_rates.get("quality_notes", {}).get("stale_fields", [])
        click.echo(f"WARN macro/rates context is {status}; stale_fields={stale}")


def _parse_date(value: str, label: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise click.BadParameter(
            "date must use YYYY-MM-DD format", param_hint=label
        ) from exc


def _parse_codes(value: str) -> list[str]:
    codes = [_normalize_etf_code(part) for part in value.split(",") if part.strip()]
    if not codes:
        raise click.BadParameter(
            "at least one ETF code is required", param_hint="codes"
        )
    return list(dict.fromkeys(codes))


def _normalize_etf_code(value: str) -> str:
    code = value.strip().upper()
    if "." in code:
        return code
    exchange = "SH" if code.startswith(("5", "6")) else "SZ"
    return f"{code}.{exchange}"


if __name__ == "__main__":
    main()
