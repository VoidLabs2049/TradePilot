"""CLI workflow for syncing commodity futures source data."""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
from typing import Any

import click
import duckdb

from tradepilot import db as tradepilot_db
from tradepilot.config import DB_PATH, LAKEHOUSE_ROOT
from tradepilot.etl.models import IngestionRequest, RunStatus, StorageZone, TriggerMode
from tradepilot.etl.service import ETLService
from tradepilot.etl.storage import build_zone_path

FUTURES_ROOT_CODES = (
    "AU.SHF",
    "AL.SHF",
    "CU.SHF",
    "RB.SHF",
    "I.DCE",
    "M.DCE",
    "P.DCE",
    "SC.INE",
    # CZCE/ZCE contract codes can use 3-digit year-month text. This raw-layer
    # sync keeps Tushare ts_code unchanged; continuous-series builders must
    # disambiguate contract years with trade_date before de-duplication.
    "TA.ZCE",
)
_HISTORY_START = date(2005, 1, 1)
_CALENDAR_DATASET = "reference.trading_calendar"
_REFERENCE_DATASET = "reference.futures_instruments"
_MAPPING_DATASET = "market.futures_mapping"
_DAILY_DATASET = "market.futures_contract_daily"
_ROOT_EXCHANGE_MAP = {
    "SHF": "SHFE",
    "DCE": "DCE",
    "ZCE": "CZCE",
    "INE": "INE",
    "CFX": "CFFEX",
}


@click.command()
@click.option(
    "--start",
    "start_text",
    type=str,
    default=None,
    help="Inclusive mapping and contract daily start date. Defaults to watermark + 1 day.",
)
@click.option(
    "--end",
    "end_text",
    type=str,
    default=None,
    help="Inclusive end date. Defaults to today.",
)
@click.option(
    "--roots",
    type=str,
    default=",".join(FUTURES_ROOT_CODES),
    show_default=True,
    help="Comma-separated Tushare futures root codes.",
)
@click.option("--dry-run", is_flag=True, help="Print the plan without writing data.")
@click.option(
    "--full-refresh",
    is_flag=True,
    help="Ignore watermarks and rebuild from the historical start date.",
)
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
def main(
    start_text: str,
    end_text: str | None,
    roots: str,
    dry_run: bool,
    full_refresh: bool,
    db_path: Path,
    lakehouse_root: Path,
) -> None:
    """Sync dominant mappings and concrete daily bars for commodity futures."""

    end = _parse_date(end_text, "end") if end_text else date.today()
    start = _parse_date(start_text, "start") if start_text else _HISTORY_START
    if start > end:
        raise click.BadParameter("start must not be after end", param_hint="--start")
    root_codes = _parse_roots(roots)
    if dry_run:
        _print_plan(root_codes, start, end, full_refresh=full_refresh)
        return

    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(db_path))
    try:
        tradepilot_db.initialize_schema(conn)
        if not start_text and not full_refresh:
            start = _incremental_start(conn, fallback=_HISTORY_START)
        if start > end:
            click.echo("watermarks are already current; nothing to sync")
            return
        _print_plan(root_codes, start, end, full_refresh=full_refresh)
        service = ETLService(conn=conn, lakehouse_root=lakehouse_root)
        results = sync_futures_data(
            service=service,
            root_codes=root_codes,
            start=start,
            end=end,
            lakehouse_root=lakehouse_root,
            full_refresh=full_refresh,
        )
    finally:
        conn.close()
    _print_results(results)
    failures = [result for result in results if result["status"] != "success"]
    if failures:
        raise click.ClickException(f"{len(failures)} futures sync steps failed")


def sync_futures_data(
    *,
    service: ETLService,
    root_codes: list[str],
    start: date,
    end: date,
    lakehouse_root: Path,
    full_refresh: bool = False,
) -> list[dict[str, Any]]:
    """Sync mappings and their concrete contracts one root at a time."""

    results: list[dict[str, Any]] = []
    exchanges = _exchanges_from_roots(root_codes)
    calendar = service.run_dataset_sync(
        _CALENDAR_DATASET,
        IngestionRequest(
            request_start=start,
            request_end=end,
            full_refresh=full_refresh,
            trigger_mode=TriggerMode.BACKFILL,
            context={"exchanges": exchanges},
        ),
    )
    results.append(_sync_result("calendar", calendar))
    if calendar.status != RunStatus.SUCCESS:
        return results
    instruments = service.run_dataset_sync(
        _REFERENCE_DATASET,
        IngestionRequest(
            full_refresh=full_refresh,
            trigger_mode=TriggerMode.BACKFILL,
            context={"exchanges": exchanges},
        ),
    )
    results.append(_sync_result("reference", instruments))
    if instruments.status != RunStatus.SUCCESS:
        return results
    for root_code in root_codes:
        mapping = service.run_dataset_sync(
            _MAPPING_DATASET,
            IngestionRequest(
                request_start=start,
                request_end=end,
                full_refresh=full_refresh,
                trigger_mode=TriggerMode.BACKFILL,
                context={"root_codes": [root_code]},
            ),
        )
        results.append(_sync_result(root_code, mapping))
        if mapping.status != RunStatus.SUCCESS:
            continue
        contract_codes = active_contract_codes(
            lakehouse_root=lakehouse_root,
            root_code=root_code,
            start=start,
            end=end,
        )
        if not contract_codes:
            results.append(
                {
                    "root_code": root_code,
                    "dataset_name": _DAILY_DATASET,
                    "status": "failed",
                    "records_written": 0,
                    "contract_count": 0,
                    "error_message": "no active contracts found in mapping",
                }
            )
            continue
        daily = service.run_dataset_sync(
            _DAILY_DATASET,
            IngestionRequest(
                request_start=start,
                request_end=end,
                full_refresh=full_refresh,
                trigger_mode=TriggerMode.BACKFILL,
                context={"contract_codes": contract_codes},
            ),
        )
        result = _sync_result(root_code, daily)
        result["contract_count"] = len(contract_codes)
        results.append(result)
    return results


def active_contract_codes(
    *, lakehouse_root: Path, root_code: str, start: date, end: date
) -> list[str]:
    """Return concrete contracts from persisted point-in-time mappings."""

    root = build_zone_path(_MAPPING_DATASET, StorageZone.NORMALIZED, lakehouse_root)
    paths = sorted(root.rglob("*.parquet")) if root.exists() else []
    if not paths:
        return []
    conn = duckdb.connect()
    try:
        frame = conn.execute(
            """
            SELECT DISTINCT active_contract
            FROM read_parquet(?)
            WHERE root_code = ?
              AND trade_date BETWEEN ? AND ?
              AND active_contract IS NOT NULL
            ORDER BY active_contract
            """,
            [[str(path) for path in paths], root_code, start, end],
        ).fetchdf()
    finally:
        conn.close()
    return frame["active_contract"].astype(str).tolist()


def _sync_result(root_code: str, result: Any) -> dict[str, Any]:
    return {
        "root_code": root_code,
        "dataset_name": result.dataset_name,
        "status": result.status.value,
        "records_written": result.records_written,
        "error_message": result.error_message,
    }


def _parse_roots(value: str) -> list[str]:
    roots = list(
        dict.fromkeys(item.strip().upper() for item in value.split(",") if item.strip())
    )
    unsupported = [root for root in roots if root not in FUTURES_ROOT_CODES]
    if not roots:
        raise click.BadParameter("at least one root is required", param_hint="--roots")
    if unsupported:
        raise click.BadParameter(
            f"unsupported roots: {','.join(unsupported)}", param_hint="--roots"
        )
    return roots


def _parse_date(value: str, label: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise click.BadParameter(
            f"{label} must use YYYY-MM-DD", param_hint=f"--{label}"
        ) from exc


def _exchanges_from_roots(root_codes: list[str]) -> list[str]:
    """Return Tushare exchange codes needed by the selected root codes."""

    exchanges = [
        _ROOT_EXCHANGE_MAP[root.rsplit(".", maxsplit=1)[-1]] for root in root_codes
    ]
    return list(dict.fromkeys(exchanges))


def _incremental_start(conn: duckdb.DuckDBPyConnection, fallback: date) -> date:
    """Return the next futures sync start date from existing watermarks."""

    table_exists = conn.execute("""
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_name = 'etl_source_watermarks'
        """).fetchone()[0]
    if not table_exists:
        return fallback
    rows = conn.execute(
        """
        SELECT latest_fetched_date
        FROM etl_source_watermarks
        WHERE dataset_name IN (?, ?)
          AND latest_fetched_date IS NOT NULL
        """,
        [_MAPPING_DATASET, _DAILY_DATASET],
    ).fetchall()
    dates = [row[0] for row in rows if row[0] is not None]
    if not dates:
        return fallback
    return min(dates) + timedelta(days=1)


def _print_plan(
    root_codes: list[str], start: date, end: date, *, full_refresh: bool
) -> None:
    mode = "full-refresh" if full_refresh else "incremental"
    click.echo(f"mode={mode}")
    click.echo(f"window={start.isoformat()}..{end.isoformat()}")
    click.echo(f"roots={','.join(root_codes)}")
    click.echo(
        "steps=trade_cal -> fut_basic -> fut_mapping -> persisted active contracts -> fut_daily"
    )


def _print_results(results: list[dict[str, Any]]) -> None:
    for result in results:
        contracts = result.get("contract_count")
        contract_text = f" contracts={contracts}" if contracts is not None else ""
        click.echo(
            f"{result['root_code']} {result['dataset_name']} "
            f"status={result['status']} records={result['records_written']}"
            f"{contract_text}"
        )
        if result.get("error_message"):
            click.echo(f"  error={result['error_message']}")


if __name__ == "__main__":
    main()
