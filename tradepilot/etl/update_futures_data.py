"""CLI workflow for syncing commodity futures source data."""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any

import click
import duckdb
import pandas as pd

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
    "TA.ZCE",
)
_HISTORY_START = date(2005, 1, 1)
_MAPPING_DATASET = "market.futures_mapping"
_DAILY_DATASET = "market.futures_contract_daily"


@click.command()
@click.option(
    "--start",
    "start_text",
    type=str,
    default=_HISTORY_START.isoformat(),
    show_default=True,
    help="Inclusive mapping and contract daily start date.",
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
    db_path: Path,
    lakehouse_root: Path,
) -> None:
    """Sync dominant mappings and concrete daily bars for commodity futures."""

    start = _parse_date(start_text, "start")
    end = _parse_date(end_text, "end") if end_text else date.today()
    if start > end:
        raise click.BadParameter("start must not be after end", param_hint="--start")
    root_codes = _parse_roots(roots)
    _print_plan(root_codes, start, end)
    if dry_run:
        return

    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(db_path))
    try:
        tradepilot_db.initialize_schema(conn)
        service = ETLService(conn=conn, lakehouse_root=lakehouse_root)
        results = sync_futures_data(
            service=service,
            root_codes=root_codes,
            start=start,
            end=end,
            lakehouse_root=lakehouse_root,
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
) -> list[dict[str, Any]]:
    """Sync mappings and their concrete contracts one root at a time."""

    results: list[dict[str, Any]] = []
    for root_code in root_codes:
        mapping = service.run_dataset_sync(
            _MAPPING_DATASET,
            IngestionRequest(
                request_start=start,
                request_end=end,
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
    frames = [
        pd.read_parquet(path, columns=["root_code", "trade_date", "active_contract"])
        for path in paths
    ]
    frame = pd.concat(frames, ignore_index=True)
    trade_dates = pd.to_datetime(frame["trade_date"], errors="coerce").dt.date
    selected = frame[
        frame["root_code"].astype(str).eq(root_code) & trade_dates.between(start, end)
    ]
    return sorted(selected["active_contract"].dropna().astype(str).unique().tolist())


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


def _print_plan(root_codes: list[str], start: date, end: date) -> None:
    click.echo(f"window={start.isoformat()}..{end.isoformat()}")
    click.echo(f"roots={','.join(root_codes)}")
    click.echo("steps=fut_mapping -> persisted active contracts -> fut_daily")


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
