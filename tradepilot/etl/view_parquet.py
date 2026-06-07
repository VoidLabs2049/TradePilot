"""CLI helper for inspecting Parquet files from the lakehouse."""

from __future__ import annotations

from pathlib import Path

import click
import pandas as pd


@click.command()
@click.argument(
    "path",
    type=click.Path(path_type=Path, exists=True),
)
@click.option(
    "--limit",
    type=int,
    default=20,
    show_default=True,
    help="Number of rows to display.",
)
@click.option(
    "--columns",
    type=str,
    default=None,
    help="Comma-separated columns to display.",
)
@click.option(
    "--tail",
    is_flag=True,
    help="Show the last rows instead of the first rows.",
)
@click.option(
    "--transpose",
    is_flag=True,
    help="Transpose the displayed rows for wide single-row tables.",
)
@click.option(
    "--schema",
    is_flag=True,
    help="Print column dtypes before row data.",
)
@click.option(
    "--csv",
    "csv_path",
    type=click.Path(path_type=Path, dir_okay=False),
    default=None,
    help="Optional CSV path for the displayed rows.",
)
def main(
    path: Path,
    limit: int,
    columns: str | None,
    tail: bool,
    transpose: bool,
    schema: bool,
    csv_path: Path | None,
) -> None:
    """Inspect one Parquet file or all Parquet files under a directory."""

    if limit <= 0:
        raise click.BadParameter("limit must be positive", param_hint="--limit")
    frame = _read_parquet_path(path)
    selected = _select_columns(frame, columns)
    view = selected.tail(limit) if tail else selected.head(limit)

    click.echo(f"path={path}")
    click.echo(f"rows={len(frame)} columns={len(frame.columns)} displayed={len(view)}")
    if schema:
        click.echo("\nSchema:")
        click.echo(_schema_text(selected))
    if csv_path is not None:
        csv_path.parent.mkdir(parents=True, exist_ok=True)
        view.to_csv(csv_path, index=False)
        click.echo(f"\nCSV written: {csv_path}")

    click.echo("\nData:")
    with pd.option_context(
        "display.max_columns",
        None,
        "display.width",
        240,
        "display.max_colwidth",
        120,
    ):
        if transpose:
            click.echo(view.T.to_string())
        else:
            click.echo(view.to_string(index=False))


def _read_parquet_path(path: Path) -> pd.DataFrame:
    """Read one Parquet file or concatenate Parquet files under a directory."""

    if path.is_file():
        return pd.read_parquet(path)
    files = sorted(
        file_path for file_path in path.rglob("*.parquet") if file_path.is_file()
    )
    if not files:
        raise click.ClickException(f"no parquet files found under: {path}")
    return pd.concat(
        [pd.read_parquet(file_path) for file_path in files],
        ignore_index=True,
    )


def _select_columns(frame: pd.DataFrame, columns: str | None) -> pd.DataFrame:
    """Return a frame with optional comma-separated column selection."""

    if columns is None:
        return frame
    selected = [column.strip() for column in columns.split(",") if column.strip()]
    if not selected:
        raise click.BadParameter("columns must not be blank", param_hint="--columns")
    missing = [column for column in selected if column not in frame.columns]
    if missing:
        raise click.ClickException(f"missing columns: {', '.join(missing)}")
    return frame.loc[:, selected]


def _schema_text(frame: pd.DataFrame) -> str:
    """Return a compact schema summary."""

    schema = pd.DataFrame(
        {
            "column": list(frame.columns),
            "dtype": [str(dtype) for dtype in frame.dtypes],
        }
    )
    return schema.to_string(index=False)


if __name__ == "__main__":
    main()
