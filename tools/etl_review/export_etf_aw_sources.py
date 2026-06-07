"""Export ETF all-weather source CSVs for manual comparison."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Callable

import click
import pandas as pd
import requests

from tradepilot.config import LAKEHOUSE_ROOT

_V1_CODES = ("510300.SH", "159845.SZ", "511010.SH", "518850.SH", "159001.SZ")
_HISTORY_START = date(2016, 1, 1)
_DEFAULT_SOURCES = "local,tencent,sina"
_FULL_HISTORY_DEFAULT_SOURCES = "local,tencent,sina"
_EXPORT_COLUMNS = [
    "source",
    "etf_code",
    "trade_date",
    "open",
    "close",
    "high",
    "low",
    "volume",
    "volume_unit",
    "amount",
    "amount_unit",
    "pct_chg",
    "source_url",
    "source_note",
]
_COMPARE_FIELDS = (
    "open",
    "close",
    "high",
    "low",
    "volume_hand",
    "amount_cny",
    "pct_chg",
)
_PRICE_TOLERANCE = 1e-6
_FIELD_TOLERANCES = {
    "volume_hand": 1.0,
}
_INVESTING_URLS = {
    "511010.SH": "https://cn.investing.com/etfs/guotai-sse-deliverable-5-tb-historical-data",
}


@dataclass(frozen=True)
class FetchContext:
    """Parameters shared by all external source fetchers."""

    etf_code: str
    start: date
    end: date
    timeout: int
    xueqiu_cookie: str | None = None
    investing_url: str | None = None


@dataclass(frozen=True)
class FullHistoryResult:
    """Paths and counts produced by one full-history export run."""

    output_dir: Path
    successful_codes: list[str]
    failed_codes: list[str]


@click.command()
@click.argument("etf_code", required=False)
@click.argument("start", required=False)
@click.argument("end", required=False)
@click.option(
    "--full-history",
    is_flag=True,
    help="Compare the full v1 ETF all-weather sleeve history instead of one ETF.",
)
@click.option(
    "--codes",
    type=str,
    default=",".join(_V1_CODES),
    show_default=True,
    help="Comma-separated ETF codes for --full-history.",
)
@click.option(
    "--start",
    "history_start",
    type=str,
    default=None,
    help="Full-history start date. Defaults to 2016-01-01.",
)
@click.option(
    "--end",
    "history_end",
    type=str,
    default=None,
    help="Full-history end date. Defaults to today.",
)
@click.option(
    "--sources",
    type=str,
    default=None,
    help=(
        "Comma-separated sources: local,eastmoney,tencent,sina,xueqiu,"
        "investing,all. Defaults to local,tencent,sina."
    ),
)
@click.option(
    "--out-dir",
    type=click.Path(path_type=Path, file_okay=False),
    default=None,
    help="Directory for exported CSV files.",
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
    help="Optional Investing.com historical-data page URL for this ETF.",
)
@click.option(
    "--timeout",
    type=int,
    default=None,
    help="HTTP request timeout in seconds. Defaults to 15, or 30 for --full-history.",
)
@click.option(
    "--fail-fast",
    is_flag=True,
    help="Stop on the first ETF/source failure instead of writing errors.csv.",
)
def main(
    etf_code: str | None,
    start: str | None,
    end: str | None,
    full_history: bool,
    codes: str,
    history_start: str | None,
    history_end: str | None,
    sources: str | None,
    out_dir: Path | None,
    xueqiu_cookie: str | None,
    investing_url: str | None,
    timeout: int | None,
    fail_fast: bool,
) -> None:
    """Download ETF comparison data and export CSV files."""

    timeout_value = _resolve_timeout(timeout, full_history)
    if timeout_value <= 0:
        raise click.BadParameter("timeout must be positive", param_hint="--timeout")
    if full_history:
        _run_full_history_export(
            etf_code=etf_code,
            start_arg=start,
            end_arg=end,
            codes=codes,
            history_start=history_start,
            history_end=history_end,
            sources=sources,
            out_dir=out_dir,
            xueqiu_cookie=xueqiu_cookie,
            investing_url=investing_url,
            timeout=timeout_value,
            fail_fast=fail_fast,
        )
        return
    _run_single_export(
        etf_code=etf_code,
        start_arg=start,
        end_arg=end,
        history_start=history_start,
        history_end=history_end,
        sources=sources,
        out_dir=out_dir,
        xueqiu_cookie=xueqiu_cookie,
        investing_url=investing_url,
        timeout=timeout_value,
        fail_fast=fail_fast,
    )


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
    """Export source CSVs and aggregate comparison files for multiple ETFs."""

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
            "full-history export failed for every ETF; "
            f"see {output_dir / 'all_codes_errors.csv'}"
        )
    return FullHistoryResult(
        output_dir=output_dir,
        successful_codes=successful_codes,
        failed_codes=failed_codes,
    )


def _run_single_export(
    *,
    etf_code: str | None,
    start_arg: str | None,
    end_arg: str | None,
    history_start: str | None,
    history_end: str | None,
    sources: str | None,
    out_dir: Path | None,
    xueqiu_cookie: str | None,
    investing_url: str | None,
    timeout: int,
    fail_fast: bool,
) -> None:
    if history_start is not None or history_end is not None:
        raise click.UsageError(
            "--start/--end are only valid with --full-history; "
            "use positional START END for one ETF"
        )
    if etf_code is None or start_arg is None or end_arg is None:
        raise click.UsageError(
            "ETF_CODE START END are required unless --full-history is set"
        )
    start_date = _parse_date(start_arg, "start")
    end_date = _parse_date(end_arg, "end")
    if start_date > end_date:
        start_date, end_date = end_date, start_date
    code = _normalize_etf_code(etf_code)
    source_names = _parse_sources(sources or _DEFAULT_SOURCES)
    context = FetchContext(
        etf_code=code,
        start=start_date,
        end=end_date,
        timeout=timeout,
        xueqiu_cookie=xueqiu_cookie,
        investing_url=investing_url,
    )
    export_dir, frames, errors = export_sources(
        context=context,
        sources=source_names,
        out_dir=out_dir or Path("data/source-review"),
        fail_fast=fail_fast,
    )
    click.echo(f"etf={code} start={start_date} end={end_date} output={export_dir}")
    for source_name in source_names:
        rows = len(frames.get(source_name, pd.DataFrame()))
        click.echo(f"- {source_name}: rows={rows}")
    if errors:
        click.echo(f"errors={export_dir / 'errors.csv'}")
    combined = export_dir / "combined.csv"
    if combined.exists():
        click.echo(f"combined={combined}")
    comparison = export_dir / "comparison.csv"
    if comparison.exists():
        click.echo(f"comparison={comparison}")
    summary = export_dir / "summary.csv"
    if summary.exists():
        click.echo(f"summary={summary}")


def _run_full_history_export(
    *,
    etf_code: str | None,
    start_arg: str | None,
    end_arg: str | None,
    codes: str,
    history_start: str | None,
    history_end: str | None,
    sources: str | None,
    out_dir: Path | None,
    xueqiu_cookie: str | None,
    investing_url: str | None,
    timeout: int,
    fail_fast: bool,
) -> None:
    if etf_code is not None or start_arg is not None or end_arg is not None:
        raise click.UsageError(
            "--full-history does not accept ETF_CODE START END positional arguments"
        )
    start = _parse_date(history_start, "start") if history_start else _HISTORY_START
    end = _parse_date(history_end, "end") if history_end else date.today()
    if start > end:
        start, end = end, start
    source_list = _parse_sources(sources or _FULL_HISTORY_DEFAULT_SOURCES)
    result = export_full_history(
        codes=_parse_codes(codes),
        start=start,
        end=end,
        sources=source_list,
        out_dir=out_dir or Path("data/source-review/full-history"),
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


def export_sources(
    *,
    context: FetchContext,
    sources: list[str],
    out_dir: Path,
    fail_fast: bool = False,
) -> tuple[Path, dict[str, pd.DataFrame], list[dict[str, str]]]:
    """Fetch and export source CSV files for one ETF/date range."""

    export_dir = (
        out_dir / f"{context.etf_code.replace('.', '_')}_{context.start}_{context.end}"
    )
    export_dir.mkdir(parents=True, exist_ok=True)
    for generated_name in (
        "combined.csv",
        "comparison.csv",
        "summary.csv",
        "errors.csv",
    ):
        generated_path = export_dir / generated_name
        if generated_path.exists():
            generated_path.unlink()
    frames: dict[str, pd.DataFrame] = {}
    errors: list[dict[str, str]] = []
    for source_name in sources:
        fetcher = _SOURCE_FETCHERS[source_name]
        source_path = _source_csv_path(export_dir, source_name, context)
        if source_path.exists():
            source_path.unlink()
        try:
            frame = fetcher(context)
        except Exception as exc:
            if fail_fast:
                raise
            errors.append({"source": source_name, "error": str(exc)})
            continue
        frame = _finish_source_frame(frame, source_name, context)
        frames[source_name] = frame
        frame.to_csv(source_path, index=False)
    if frames:
        combined = pd.concat(
            [frame.astype(object) for frame in frames.values()],
            ignore_index=True,
        )
        combined.to_csv(export_dir / "combined.csv", index=False)
        comparison, summary = compare_source_frames(combined, expected_sources=sources)
        comparison.to_csv(export_dir / "comparison.csv", index=False)
        summary.to_csv(export_dir / "summary.csv", index=False)
    if errors:
        pd.DataFrame(errors).to_csv(export_dir / "errors.csv", index=False)
    if not frames:
        raise click.ClickException(
            f"all sources failed; see {export_dir / 'errors.csv'}"
        )
    return export_dir, frames, errors


def compare_source_frames(
    combined: pd.DataFrame, expected_sources: list[str] | None = None
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Build detailed and summary comparisons across exported source rows."""

    if combined.empty:
        return pd.DataFrame(), pd.DataFrame()
    working = combined.copy()
    working["trade_date"] = pd.to_datetime(
        working["trade_date"], errors="coerce"
    ).dt.date
    if expected_sources is None:
        source_order = sorted(working["source"].dropna().astype(str).unique())
    else:
        source_order = sorted(dict.fromkeys(expected_sources))
    detail_rows: list[dict[str, Any]] = []
    for (etf_code, trade_date), group in working.groupby(
        ["etf_code", "trade_date"], dropna=False
    ):
        group_by_source = group.set_index("source")
        present_sources = sorted(group_by_source.index.astype(str).unique())
        row: dict[str, Any] = {
            "etf_code": etf_code,
            "trade_date": trade_date.isoformat(),
            "sources_present": ",".join(present_sources),
            "sources_missing": ",".join(
                source for source in source_order if source not in present_sources
            ),
        }
        for field in _COMPARE_FIELDS:
            values = pd.Series(
                {
                    str(source): _normalized_compare_value(row, field)
                    for source, row in group_by_source.iterrows()
                }
            )
            field_values = values.dropna()
            for source in source_order:
                column = f"{field}_{source}"
                row[column] = values.get(source) if source in values.index else None
            if field_values.empty:
                row[f"{field}_min"] = None
                row[f"{field}_max"] = None
                row[f"{field}_range"] = None
                continue
            min_value = float(field_values.min())
            max_value = float(field_values.max())
            row[f"{field}_min"] = min_value
            row[f"{field}_max"] = max_value
            row[f"{field}_range"] = max_value - min_value
        detail_rows.append(row)
    comparison = (
        pd.DataFrame(detail_rows)
        .sort_values(["trade_date", "etf_code"])
        .reset_index(drop=True)
    )
    summary = _make_comparison_summary(comparison, source_order)
    return comparison, summary


def _make_comparison_summary(
    comparison: pd.DataFrame, source_order: list[str]
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    total_days = len(comparison)
    for field in _COMPARE_FIELDS:
        range_column = f"{field}_range"
        ranges = pd.to_numeric(comparison.get(range_column), errors="coerce")
        max_diff = ranges.max(skipna=True)
        max_diff_value = None if pd.isna(max_diff) else float(max_diff)
        row: dict[str, Any] = {
            "field": field,
            "days_compared": int(total_days),
            "max_abs_diff": max_diff_value,
            "mismatch_days": int((ranges.fillna(0) > _compare_tolerance(field)).sum()),
        }
        if max_diff_value is None or max_diff_value <= _compare_tolerance(field):
            row["max_abs_diff_date"] = None
        else:
            max_index = ranges.idxmax()
            row["max_abs_diff_date"] = comparison.loc[max_index, "trade_date"]
        for source in source_order:
            column = f"{field}_{source}"
            if column not in comparison:
                row[f"missing_{source}"] = total_days
                continue
            row[f"missing_{source}"] = int(comparison[column].isna().sum())
        rows.append(row)
    return pd.DataFrame(rows)


def _normalized_compare_value(row: pd.Series, field: str) -> float | None:
    if field == "volume_hand":
        value = _to_float(row.get("volume"))
        if value is None:
            return None
        unit = str(row.get("volume_unit") or "").lower()
        if unit == "share":
            return value / 100
        return value
    if field == "amount_cny":
        value = _to_float(row.get("amount"))
        if value is None:
            return None
        unit = str(row.get("amount_unit") or "").lower()
        if unit in {"thousand_cny", "thousand cny"}:
            return value * 1000
        return value
    return _to_float(row.get(field))


def _compare_tolerance(field: str) -> float:
    return _FIELD_TOLERANCES.get(field, _PRICE_TOLERANCE)


def fetch_eastmoney(context: FetchContext) -> pd.DataFrame:
    """Fetch unadjusted daily ETF K-line data from Eastmoney."""

    url = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
    response = requests.get(
        url,
        params={
            "secid": _eastmoney_secid(context.etf_code),
            "fields1": "f1,f2,f3,f4,f5,f6",
            "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
            "klt": "101",
            "fqt": "0",
            "beg": context.start.strftime("%Y%m%d"),
            "end": context.end.strftime("%Y%m%d"),
        },
        headers=_headers("https://quote.eastmoney.com/"),
        timeout=context.timeout,
    )
    response.raise_for_status()
    payload = response.json()
    if payload.get("rc") != 0:
        raise RuntimeError(f"eastmoney rc={payload.get('rc')}")
    rows = payload.get("data", {}).get("klines") or []
    records = []
    for raw in rows:
        parts = str(raw).split(",")
        if len(parts) < 11:
            continue
        records.append(
            {
                "source": "eastmoney",
                "etf_code": context.etf_code,
                "trade_date": parts[0],
                "open": _to_float(parts[1]),
                "close": _to_float(parts[2]),
                "high": _to_float(parts[3]),
                "low": _to_float(parts[4]),
                "volume": _to_float(parts[5]),
                "volume_unit": "hand",
                "amount": _to_float(parts[6]),
                "amount_unit": "CNY",
                "pct_chg": _to_float(parts[8]),
                "source_url": response.url,
                "source_note": "eastmoney push2his kline fqt=0",
            }
        )
    return pd.DataFrame(records)


def fetch_tencent(context: FetchContext) -> pd.DataFrame:
    """Fetch unadjusted daily ETF K-line data from Tencent."""

    symbol = _market_symbol(context.etf_code)
    url = "https://web.ifzq.gtimg.cn/appstock/app/fqkline/get"
    records = []
    for window_start, window_end in _date_windows(context.start, context.end, 700):
        response = requests.get(
            url,
            params={
                "param": f"{symbol},day,{window_start},{window_end},640,bfq",
            },
            headers=_headers("https://gu.qq.com/"),
            timeout=context.timeout,
        )
        response.raise_for_status()
        payload = response.json()
        if payload.get("code") != 0:
            raise RuntimeError(
                f"tencent code={payload.get('code')} msg={payload.get('msg')}"
            )
        rows = payload.get("data", {}).get(symbol, {}).get("day") or []
        for row in rows:
            if len(row) < 6:
                continue
            records.append(
                {
                    "source": "tencent",
                    "etf_code": context.etf_code,
                    "trade_date": row[0],
                    "open": _to_float(row[1]),
                    "close": _to_float(row[2]),
                    "high": _to_float(row[3]),
                    "low": _to_float(row[4]),
                    "volume": _to_float(row[5]),
                    "volume_unit": "hand",
                    "amount": None,
                    "amount_unit": None,
                    "pct_chg": None,
                    "source_url": response.url,
                    "source_note": "tencent fqkline bfq",
                }
            )
    if not records:
        return pd.DataFrame(records)
    return (
        pd.DataFrame(records)
        .drop_duplicates(subset=["source", "etf_code", "trade_date"])
        .sort_values("trade_date")
        .reset_index(drop=True)
    )


def fetch_sina(context: FetchContext) -> pd.DataFrame:
    """Fetch daily ETF K-line data from Sina and filter by date."""

    symbol = _market_symbol(context.etf_code)
    datalen = max(100, min(10000, int((context.end - context.start).days * 2.5) + 30))
    url = "https://money.finance.sina.com.cn/quotes_service/api/json_v2.php/CN_MarketData.getKLineData"
    response = requests.get(
        url,
        params={"symbol": symbol, "scale": "240", "ma": "no", "datalen": str(datalen)},
        headers=_headers("https://finance.sina.com.cn/"),
        timeout=context.timeout,
    )
    response.raise_for_status()
    rows = response.json()
    records = []
    for row in rows:
        trade_date = date.fromisoformat(str(row["day"]))
        if trade_date < context.start or trade_date > context.end:
            continue
        records.append(
            {
                "source": "sina",
                "etf_code": context.etf_code,
                "trade_date": trade_date.isoformat(),
                "open": _to_float(row.get("open")),
                "close": _to_float(row.get("close")),
                "high": _to_float(row.get("high")),
                "low": _to_float(row.get("low")),
                "volume": _to_float(row.get("volume")),
                "volume_unit": "share",
                "amount": None,
                "amount_unit": None,
                "pct_chg": None,
                "source_url": response.url,
                "source_note": "sina CN_MarketData.getKLineData",
            }
        )
    return pd.DataFrame(records)


def fetch_xueqiu(context: FetchContext) -> pd.DataFrame:
    """Fetch daily ETF K-line data from Xueqiu when a valid cookie is available."""

    symbol = _xueqiu_symbol(context.etf_code)
    session = requests.Session()
    headers = _headers(f"https://xueqiu.com/S/{symbol}")
    if context.xueqiu_cookie:
        headers["Cookie"] = context.xueqiu_cookie
    else:
        session.get("https://xueqiu.com/", headers=headers, timeout=context.timeout)
    days = max(10, int((context.end - context.start).days * 2.5) + 30)
    response = session.get(
        "https://stock.xueqiu.com/v5/stock/chart/kline.json",
        params={
            "symbol": symbol,
            "begin": int(pd.Timestamp(context.end).timestamp() * 1000),
            "period": "day",
            "type": "before",
            "count": f"-{days}",
            "indicator": "kline,pe,pb,ps,pcf,market_capital,agt,ggt,balance",
        },
        headers=headers,
        timeout=context.timeout,
    )
    if response.status_code >= 400:
        raise RuntimeError(f"xueqiu HTTP {response.status_code}: {response.text[:300]}")
    payload = response.json()
    data = payload.get("data") or {}
    columns = data.get("column") or []
    records = []
    for item in data.get("item") or []:
        row = dict(zip(columns, item))
        trade_date = _date_from_xueqiu_timestamp(row.get("timestamp"))
        if trade_date is None or trade_date < context.start or trade_date > context.end:
            continue
        records.append(
            {
                "source": "xueqiu",
                "etf_code": context.etf_code,
                "trade_date": trade_date.isoformat(),
                "open": _to_float(row.get("open")),
                "close": _to_float(row.get("close")),
                "high": _to_float(row.get("high")),
                "low": _to_float(row.get("low")),
                "volume": _to_float(row.get("volume")),
                "volume_unit": "share",
                "amount": _to_float(row.get("amount")),
                "amount_unit": "CNY",
                "pct_chg": _to_float(row.get("percent")),
                "source_url": response.url,
                "source_note": "xueqiu chart kline; may require browser cookie",
            }
        )
    return pd.DataFrame(records)


def fetch_investing(context: FetchContext) -> pd.DataFrame:
    """Fetch visible Investing.com historical table data when the page is accessible."""

    url = context.investing_url or _INVESTING_URLS.get(context.etf_code)
    if url is None:
        raise RuntimeError("investing URL is required for this ETF")
    response = requests.get(
        url,
        headers=_headers("https://cn.investing.com/"),
        timeout=context.timeout,
    )
    if response.status_code >= 400:
        raise RuntimeError(
            f"investing HTTP {response.status_code}; Investing.com has no public API"
        )
    tables = pd.read_html(response.text)
    table = _pick_investing_table(tables)
    records = []
    for _, row in table.iterrows():
        trade_date = _parse_investing_date(_first_present(row, ["日期", "Date"]))
        if trade_date is None or trade_date < context.start or trade_date > context.end:
            continue
        records.append(
            {
                "source": "investing",
                "etf_code": context.etf_code,
                "trade_date": trade_date.isoformat(),
                "open": _to_float(_first_present(row, ["开盘", "Open"])),
                "close": _to_float(_first_present(row, ["收盘", "Price"])),
                "high": _to_float(_first_present(row, ["高", "High"])),
                "low": _to_float(_first_present(row, ["低", "Low"])),
                "volume": _parse_volume_text(_first_present(row, ["交易量", "Vol."])),
                "volume_unit": "display_scaled",
                "amount": None,
                "amount_unit": None,
                "pct_chg": _parse_percent(_first_present(row, ["涨跌幅", "Change %"])),
                "source_url": response.url,
                "source_note": "investing visible historical table; no public API",
            }
        )
    return pd.DataFrame(records)


def fetch_local_lakehouse(context: FetchContext) -> pd.DataFrame:
    """Read local ETF all-weather sleeve daily data for comparison."""

    dataset_root = LAKEHOUSE_ROOT / "derived" / "derived.etf_aw_sleeve_daily"
    files = sorted(dataset_root.glob("*/*/part-00000.parquet"))
    if not files:
        raise RuntimeError(f"local lakehouse dataset not found: {dataset_root}")
    frame = pd.concat(
        [pd.read_parquet(file_path) for file_path in files],
        ignore_index=True,
    )
    frame["trade_date"] = pd.to_datetime(frame["trade_date"], errors="coerce").dt.date
    filtered = frame[
        (frame["sleeve_code"].astype(str) == context.etf_code)
        & (frame["trade_date"] >= context.start)
        & (frame["trade_date"] <= context.end)
    ].copy()
    if filtered.empty:
        return pd.DataFrame(columns=_EXPORT_COLUMNS)
    return pd.DataFrame(
        {
            "source": "local",
            "etf_code": filtered["sleeve_code"].astype(str),
            "trade_date": filtered["trade_date"].map(lambda value: value.isoformat()),
            "open": filtered["open"],
            "close": filtered["close"],
            "high": filtered["high"],
            "low": filtered["low"],
            "volume": filtered["volume"],
            "volume_unit": "hand",
            "amount": filtered["amount"],
            "amount_unit": "thousand_CNY",
            "pct_chg": filtered["pct_chg"],
            "source_url": str(dataset_root),
            "source_note": "local derived.etf_aw_sleeve_daily; amount is thousand CNY",
        }
    )


def _finish_source_frame(
    frame: pd.DataFrame, source_name: str, context: FetchContext
) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame(columns=_EXPORT_COLUMNS)
    missing = [column for column in _EXPORT_COLUMNS if column not in frame.columns]
    if missing:
        raise RuntimeError(f"{source_name} missing columns: {missing}")
    result = frame.loc[:, _EXPORT_COLUMNS].copy()
    result["trade_date"] = pd.to_datetime(result["trade_date"], errors="coerce").dt.date
    result = result[
        (result["trade_date"] >= context.start) & (result["trade_date"] <= context.end)
    ].copy()
    result["trade_date"] = result["trade_date"].map(lambda value: value.isoformat())
    return result.sort_values(["trade_date", "source"]).reset_index(drop=True)


def _parse_sources(value: str) -> list[str]:
    raw_sources = [part.strip().lower() for part in value.split(",") if part.strip()]
    if raw_sources == ["all"]:
        raw_sources = list(_SOURCE_FETCHERS)
    if not raw_sources:
        raise click.BadParameter(
            "at least one source is required", param_hint="sources"
        )
    unknown = [source for source in raw_sources if source not in _SOURCE_FETCHERS]
    if unknown:
        raise click.BadParameter(
            f"unknown sources: {', '.join(unknown)}", param_hint="sources"
        )
    return list(dict.fromkeys(raw_sources))


def _parse_codes(value: str) -> list[str]:
    codes = [_normalize_etf_code(part) for part in value.split(",") if part.strip()]
    if not codes:
        raise click.BadParameter(
            "at least one ETF code is required", param_hint="codes"
        )
    return list(dict.fromkeys(codes))


def _resolve_timeout(value: int | None, full_history: bool) -> int:
    if value is not None:
        return value
    return 30 if full_history else 15


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


def _date_windows(start: date, end: date, max_days: int) -> list[tuple[date, date]]:
    windows = []
    current = start
    while current <= end:
        window_end = min(current + timedelta(days=max_days - 1), end)
        windows.append((current, window_end))
        current = window_end + timedelta(days=1)
    return windows


def _source_csv_path(export_dir: Path, source_name: str, context: FetchContext) -> Path:
    return (
        export_dir
        / f"{source_name}_{context.etf_code.replace('.', '_')}_{context.start}_{context.end}.csv"
    )


def _parse_date(value: str, label: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise click.BadParameter(
            "date must use YYYY-MM-DD format", param_hint=label
        ) from exc


def _normalize_etf_code(value: str) -> str:
    code = value.strip().upper()
    if "." in code:
        return code
    exchange = "SH" if code.startswith(("5", "6")) else "SZ"
    return f"{code}.{exchange}"


def _market_symbol(etf_code: str) -> str:
    code, exchange = etf_code.split(".", 1)
    return f"{exchange.lower()}{code}"


def _xueqiu_symbol(etf_code: str) -> str:
    code, exchange = etf_code.split(".", 1)
    return f"{exchange.upper()}{code}"


def _eastmoney_secid(etf_code: str) -> str:
    code, exchange = etf_code.split(".", 1)
    market = "1" if exchange.upper() == "SH" else "0"
    return f"{market}.{code}"


def _headers(referer: str) -> dict[str, str]:
    return {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/126.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Referer": referer,
    }


def _to_float(value: object) -> float | None:
    if value is None:
        return None
    text = str(value).strip().replace(",", "")
    if text in {"", "-", "--", "nan", "None"}:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _parse_percent(value: object) -> float | None:
    if value is None:
        return None
    return _to_float(str(value).replace("%", ""))


def _parse_volume_text(value: object) -> float | None:
    if value is None:
        return None
    text = str(value).strip().replace(",", "")
    if not text:
        return None
    suffix = text[-1].upper()
    multiplier = {"K": 1_000, "M": 1_000_000, "B": 1_000_000_000}.get(suffix, 1)
    number = text[:-1] if suffix in {"K", "M", "B"} else text
    parsed = _to_float(number)
    return None if parsed is None else parsed * multiplier


def _date_from_xueqiu_timestamp(value: object) -> date | None:
    parsed = _to_float(value)
    if parsed is None:
        return None
    return (
        pd.to_datetime(parsed, unit="ms", utc=True).tz_convert("Asia/Shanghai").date()
    )


def _pick_investing_table(tables: list[pd.DataFrame]) -> pd.DataFrame:
    for table in tables:
        columns = {str(column) for column in table.columns}
        if {"日期", "收盘"}.issubset(columns) or {"Date", "Price"}.issubset(columns):
            return table
    raise RuntimeError("investing historical table not found")


def _first_present(row: pd.Series, names: list[str]) -> Any:
    for name in names:
        if name in row:
            return row[name]
    return None


def _parse_investing_date(value: object) -> date | None:
    if value is None:
        return None
    text = str(value).strip()
    for fmt in ("%Y年%m月%d日", "%Y-%m-%d", "%b %d, %Y", "%m/%d/%Y"):
        parsed = pd.to_datetime(text, format=fmt, errors="coerce")
        if not pd.isna(parsed):
            return parsed.date()
    parsed = pd.to_datetime(text, errors="coerce")
    return None if pd.isna(parsed) else parsed.date()


_SOURCE_FETCHERS: dict[str, Callable[[FetchContext], pd.DataFrame]] = {
    "local": fetch_local_lakehouse,
    "eastmoney": fetch_eastmoney,
    "tencent": fetch_tencent,
    "sina": fetch_sina,
    "xueqiu": fetch_xueqiu,
    "investing": fetch_investing,
}


if __name__ == "__main__":
    main()
