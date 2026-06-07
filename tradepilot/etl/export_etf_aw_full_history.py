"""Export and compare full-history external ETF data for all-weather sleeves."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import click
import pandas as pd

from tradepilot.etl.export_etf_aw_sources import (
    FetchContext,
    _COMPARE_FIELDS,
    _compare_tolerance,
    _normalize_etf_code,
    _parse_sources,
    export_sources,
)

_V1_CODES = ("510300.SH", "159845.SZ", "511010.SH", "518850.SH", "159001.SZ")
_HISTORY_START = date(2016, 1, 1)
_FULL_HISTORY_DEFAULT_SOURCES = "local,tencent,sina"


@dataclass(frozen=True)
class FullHistoryResult:
    """Paths and counts produced by one full-history export run."""

    output_dir: Path
    successful_codes: list[str]
    failed_codes: list[str]


@click.command()
@click.option(
    "--codes",
    type=str,
    default=",".join(_V1_CODES),
    show_default=True,
    help="Comma-separated ETF codes. Bare codes are normalized to SH/SZ suffixes.",
)
@click.option(
    "--start",
    "start_text",
    type=str,
    default=_HISTORY_START.isoformat(),
    show_default=True,
    help="Full-history start date.",
)
@click.option(
    "--end",
    "end_text",
    type=str,
    default=None,
    help="Full-history end date. Defaults to today.",
)
@click.option(
    "--sources",
    type=str,
    default=_FULL_HISTORY_DEFAULT_SOURCES,
    show_default=True,
    help="Comma-separated sources: local,eastmoney,tencent,sina,xueqiu,investing,all.",
)
@click.option(
    "--out-dir",
    type=click.Path(path_type=Path, file_okay=False),
    default=Path("data/source-review/full-history"),
    show_default=True,
    help="Output directory for full-history CSV files.",
)
@click.option(
    "--xueqiu-cookie",
    type=str,
    default=None,
    help="Optional Cookie header copied from a logged-in Xueqiu browser session.",
)
@click.option(
    "--investing-url",
    type=str,
    default=None,
    help="Optional Investing.com historical-data page URL, used when one code is exported.",
)
@click.option(
    "--timeout",
    type=int,
    default=30,
    show_default=True,
    help="HTTP request timeout in seconds.",
)
@click.option(
    "--fail-fast",
    is_flag=True,
    help="Stop on the first ETF/source failure.",
)
def main(
    codes: str,
    start_text: str,
    end_text: str | None,
    sources: str,
    out_dir: Path,
    xueqiu_cookie: str | None,
    investing_url: str | None,
    timeout: int,
    fail_fast: bool,
) -> None:
    """Export website CSV data and full-history comparisons for ETF sleeves."""

    if timeout <= 0:
        raise click.BadParameter("timeout must be positive", param_hint="--timeout")
    start = _parse_date(start_text, "start")
    end = _parse_date(end_text, "end") if end_text else date.today()
    if start > end:
        start, end = end, start
    code_list = _parse_codes(codes)
    source_list = _parse_sources(sources)
    result = export_full_history(
        codes=code_list,
        start=start,
        end=end,
        sources=source_list,
        out_dir=out_dir,
        timeout=timeout,
        xueqiu_cookie=xueqiu_cookie,
        investing_url=investing_url,
        fail_fast=fail_fast,
    )
    click.echo(f"output={result.output_dir}")
    click.echo(f"successful_codes={','.join(result.successful_codes)}")
    if result.failed_codes:
        click.echo(f"failed_codes={','.join(result.failed_codes)}")
    click.echo(f"combined={result.output_dir / 'all_codes_combined.csv'}")
    click.echo(f"comparison={result.output_dir / 'all_codes_comparison.csv'}")
    click.echo(f"summary={result.output_dir / 'all_codes_summary.csv'}")
    click.echo(f"mismatch_rows={result.output_dir / 'all_codes_mismatch_rows.csv'}")
    click.echo(f"manifest={result.output_dir / 'run_manifest.csv'}")


def export_full_history(
    *,
    codes: list[str],
    start: date,
    end: date,
    sources: list[str],
    out_dir: Path,
    timeout: int,
    xueqiu_cookie: str | None = None,
    investing_url: str | None = None,
    fail_fast: bool = False,
) -> FullHistoryResult:
    """Export website CSVs and aggregate comparison files for multiple ETFs."""

    output_dir = out_dir / f"{start}_{end}"
    by_code_dir = output_dir / "by-code"
    output_dir.mkdir(parents=True, exist_ok=True)
    _remove_generated_outputs(output_dir)

    combined_frames: list[pd.DataFrame] = []
    comparison_frames: list[pd.DataFrame] = []
    summary_frames: list[pd.DataFrame] = []
    mismatch_frames: list[pd.DataFrame] = []
    manifest_rows: list[dict[str, Any]] = []
    error_rows: list[dict[str, str]] = []
    successful_codes: list[str] = []
    failed_codes: list[str] = []

    for code in codes:
        context = FetchContext(
            etf_code=code,
            start=start,
            end=end,
            timeout=timeout,
            xueqiu_cookie=xueqiu_cookie,
            investing_url=investing_url,
        )
        try:
            export_dir, frames, errors = export_sources(
                context=context,
                sources=sources,
                out_dir=by_code_dir,
                fail_fast=fail_fast,
            )
        except Exception as exc:
            if fail_fast:
                raise
            failed_codes.append(code)
            error_rows.append({"etf_code": code, "source": "", "error": str(exc)})
            manifest_rows.append(_manifest_row(code, "", 0, "failed", str(exc)))
            continue

        successful_codes.append(code)
        errors_by_source = {error["source"]: error["error"] for error in errors}
        for source in sources:
            frame = frames.get(source)
            status = "success" if frame is not None else "failed"
            manifest_rows.append(
                _manifest_row(
                    code,
                    source,
                    0 if frame is None else len(frame),
                    status,
                    errors_by_source.get(source, ""),
                )
            )
        error_rows.extend(
            {
                "etf_code": code,
                "source": error["source"],
                "error": error["error"],
            }
            for error in errors
        )
        combined_frames.append(pd.read_csv(export_dir / "combined.csv"))
        comparison = pd.read_csv(export_dir / "comparison.csv")
        summary = pd.read_csv(export_dir / "summary.csv")
        comparison_frames.append(comparison)
        summary_frames.append(summary.assign(etf_code=code))
        mismatch = _mismatch_rows(comparison)
        if not mismatch.empty:
            mismatch_frames.append(mismatch)

    _write_concat(combined_frames, output_dir / "all_codes_combined.csv")
    _write_concat(comparison_frames, output_dir / "all_codes_comparison.csv")
    _write_concat(summary_frames, output_dir / "all_codes_summary.csv")
    _write_concat(mismatch_frames, output_dir / "all_codes_mismatch_rows.csv")
    pd.DataFrame(manifest_rows).to_csv(output_dir / "run_manifest.csv", index=False)
    if error_rows:
        pd.DataFrame(error_rows).to_csv(
            output_dir / "all_codes_errors.csv", index=False
        )
    if not successful_codes:
        raise click.ClickException(
            f"full-history export failed for every ETF; see {output_dir / 'all_codes_errors.csv'}"
        )
    return FullHistoryResult(
        output_dir=output_dir,
        successful_codes=successful_codes,
        failed_codes=failed_codes,
    )


def _parse_codes(value: str) -> list[str]:
    codes = [_normalize_etf_code(part) for part in value.split(",") if part.strip()]
    if not codes:
        raise click.BadParameter(
            "at least one ETF code is required", param_hint="codes"
        )
    return list(dict.fromkeys(codes))


def _parse_date(value: str, label: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise click.BadParameter(
            "date must use YYYY-MM-DD format", param_hint=label
        ) from exc


def _manifest_row(
    etf_code: str,
    source: str,
    rows: int,
    status: str,
    error: str,
) -> dict[str, Any]:
    return {
        "etf_code": etf_code,
        "source": source,
        "rows": rows,
        "status": status,
        "error": error,
    }


def _mismatch_rows(comparison: pd.DataFrame) -> pd.DataFrame:
    if comparison.empty:
        return comparison
    mismatch = comparison["sources_missing"].fillna("").astype(str) != ""
    for field in _COMPARE_FIELDS:
        range_column = f"{field}_range"
        if range_column in comparison:
            values = pd.to_numeric(comparison[range_column], errors="coerce").fillna(0)
            mismatch = mismatch | (values > _compare_tolerance(field))
    return comparison[mismatch].copy()


def _write_concat(frames: list[pd.DataFrame], path: Path) -> None:
    if not frames:
        pd.DataFrame().to_csv(path, index=False)
        return
    pd.concat(frames, ignore_index=True).to_csv(path, index=False)


def _remove_generated_outputs(output_dir: Path) -> None:
    for file_name in (
        "all_codes_combined.csv",
        "all_codes_comparison.csv",
        "all_codes_summary.csv",
        "all_codes_mismatch_rows.csv",
        "all_codes_errors.csv",
        "run_manifest.csv",
    ):
        file_path = output_dir / file_name
        if file_path.exists():
            file_path.unlink()


if __name__ == "__main__":
    main()
