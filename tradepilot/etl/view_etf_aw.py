"""CLI helper for inspecting ETF all-weather data by ETF code and date range."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import click
import pandas as pd

from tradepilot.config import LAKEHOUSE_ROOT

_DATASETS = {
    "daily": (
        "derived/derived.etf_aw_sleeve_daily",
        "trade_date",
        [
            "trade_date",
            "sleeve_code",
            "sleeve_role",
            "close",
            "adj_factor",
            "adj_close",
            "pct_chg",
            "adj_pct_chg",
            "volume",
            "amount",
            "quality_status",
        ],
    ),
    "snapshot": (
        "derived/derived.etf_aw_rebalance_snapshot",
        "rebalance_date",
        [
            "rebalance_date",
            "sleeve_code",
            "sleeve_role",
            "close",
            "adj_factor",
            "adj_close",
            "return_1m",
            "return_3m",
            "return_6m",
            "volatility_3m",
            "max_drawdown_6m",
            "data_status",
        ],
    ),
}


@click.command()
@click.argument("etf_code")
@click.argument("start")
@click.argument("end")
@click.option(
    "--dataset",
    type=click.Choice(sorted(_DATASETS)),
    default="daily",
    show_default=True,
    help="Dataset view to display.",
)
@click.option(
    "--limit",
    type=int,
    default=200,
    show_default=True,
    help="Maximum rows printed to terminal.",
)
@click.option(
    "--csv",
    "csv_path",
    type=click.Path(path_type=Path, dir_okay=False),
    default=None,
    help="Optional CSV path for all matching rows.",
)
@click.option(
    "--lakehouse-root",
    type=click.Path(path_type=Path, file_okay=False),
    default=LAKEHOUSE_ROOT,
    show_default=True,
    help="Lakehouse root path.",
)
def main(
    etf_code: str,
    start: str,
    end: str,
    dataset: str,
    limit: int,
    csv_path: Path | None,
    lakehouse_root: Path,
) -> None:
    """Show one ETF all-weather dataset for an ETF code and date range."""

    if limit <= 0:
        raise click.BadParameter("limit must be positive", param_hint="--limit")
    start_date = _parse_date(start, "start")
    end_date = _parse_date(end, "end")
    if start_date > end_date:
        start_date, end_date = end_date, start_date

    code = _normalize_etf_code(etf_code)
    dataset_path, date_column, columns = _DATASETS[dataset]
    frame = _read_dataset(lakehouse_root / dataset_path)
    frame[date_column] = pd.to_datetime(frame[date_column], errors="coerce").dt.date
    filtered = frame[
        (frame["sleeve_code"].astype(str) == code)
        & (frame[date_column] >= start_date)
        & (frame[date_column] <= end_date)
    ].copy()
    filtered = filtered.sort_values([date_column, "sleeve_code"]).reset_index(drop=True)
    view = filtered.loc[:, columns]

    click.echo(
        f"dataset={dataset} etf={code} start={start_date} end={end_date} "
        f"rows={len(view)} displayed={min(len(view), limit)}"
    )
    if csv_path is not None:
        csv_path.parent.mkdir(parents=True, exist_ok=True)
        view.to_csv(csv_path, index=False)
        click.echo(f"csv={csv_path}")
    if view.empty:
        click.echo("No rows matched.")
        return
    with pd.option_context(
        "display.max_columns",
        None,
        "display.width",
        240,
        "display.max_colwidth",
        80,
    ):
        click.echo(view.head(limit).to_string(index=False))


def _read_dataset(dataset_root: Path) -> pd.DataFrame:
    """Read all partitioned parquet files under a dataset root."""

    files = sorted(dataset_root.glob("*/*/part-00000.parquet"))
    if not files:
        raise click.ClickException(f"no parquet files found under: {dataset_root}")
    return pd.concat(
        [pd.read_parquet(file_path) for file_path in files],
        ignore_index=True,
    )


def _parse_date(value: str, label: str) -> date:
    """Parse an ISO date argument."""

    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise click.BadParameter(
            "date must use YYYY-MM-DD format", param_hint=label
        ) from exc


def _normalize_etf_code(value: str) -> str:
    """Normalize bare ETF codes to the canonical SH/SZ suffix convention."""

    code = value.strip().upper()
    if "." in code:
        return code
    exchange = "SH" if code.startswith(("5", "6")) else "SZ"
    return f"{code}.{exchange}"


if __name__ == "__main__":
    main()
