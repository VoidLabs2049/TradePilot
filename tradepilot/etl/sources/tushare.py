"""Tushare source adapter for Stage B ETL datasets."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from typing import Any

import pandas as pd

from tradepilot.data.tushare_client import TushareClient
from tradepilot.etl.models import (
    IngestionRequest,
    SourceFetchResult,
    normalize_request_window,
)
from tradepilot.etl.sources.base import BaseSourceAdapter, SourceRole

_FUTURES_EXCHANGES = ("SHFE", "DCE", "CZCE", "INE", "CFFEX")


class TushareSourceAdapter(BaseSourceAdapter):
    """Dataset-aware Tushare adapter returning typed DataFrame payloads."""

    source_name = "tushare"
    source_role = SourceRole.PRIMARY

    _SUPPORTED = {
        "reference.trading_calendar",
        "reference.instruments",
        "reference.futures_instruments",
        "market.etf_adj_factor",
        "market.etf_daily",
        "market.futures_contract_daily",
        "market.futures_mapping",
        "market.index_daily",
        "macro.slow_fields",
        "rates.daily_rates",
        "rates.lpr",
        "rates.gov_curve_points",
    }

    def __init__(
        self,
        client: TushareClient | Any | None = None,
        akshare_module: Any | None = None,
    ) -> None:
        self._client = client or TushareClient()
        self._akshare = akshare_module

    def supports_dataset(self, dataset_name: str) -> bool:
        """Return whether this adapter can fetch one dataset."""

        return dataset_name in self._SUPPORTED

    def fetch(self, dataset_name: str, request: IngestionRequest) -> SourceFetchResult:
        """Fetch one Stage B dataset from Tushare."""

        if not self.supports_dataset(dataset_name):
            raise KeyError(f"tushare source does not support dataset: {dataset_name}")
        window_start, window_end = _result_window(dataset_name, request)
        if dataset_name == "reference.trading_calendar":
            payload = self._fetch_trading_calendar(request)
            endpoint = "trade_cal"
        elif dataset_name == "reference.instruments":
            payload = self._fetch_instruments(request)
            endpoint = _instrument_endpoint(request)
        elif dataset_name == "reference.futures_instruments":
            payload = self._fetch_futures_instruments(request)
            endpoint = "fut_basic"
        elif dataset_name == "market.etf_adj_factor":
            payload = self._fetch_etf_adj_factor(request)
            endpoint = "fund_adj"
        elif dataset_name == "market.etf_daily":
            payload = self._fetch_market_daily(request, instrument_type="etf")
            endpoint = "fund_daily"
        elif dataset_name == "market.index_daily":
            payload = self._fetch_market_daily(request, instrument_type="index")
            endpoint = "index_daily"
        elif dataset_name == "market.futures_mapping":
            payload = self._fetch_futures_mapping(request)
            endpoint = "fut_mapping"
        elif dataset_name == "market.futures_contract_daily":
            payload = self._fetch_futures_contract_daily(request)
            endpoint = "fut_daily"
        elif dataset_name == "macro.slow_fields":
            payload = self._fetch_macro_slow_fields(request)
            endpoint = "macro_slow_fields"
        elif dataset_name == "rates.daily_rates":
            payload = self._fetch_daily_rates(request)
            endpoint = "shibor"
        elif dataset_name == "rates.gov_curve_points":
            payload = self._fetch_gov_curve_points(request)
            endpoint = "gov_curve_points"
        else:
            payload = self._fetch_lpr(request)
            endpoint = "shibor_lpr"
        return SourceFetchResult(
            dataset_name=dataset_name,
            source_name=self.source_name,
            source_endpoint=endpoint,
            payload=payload,
            row_count=len(payload),
            window_start=window_start,
            window_end=window_end,
            partition_hints=_partition_hints(dataset_name, request),
            fetched_at=_utc_now(),
            schema_version="stage_b_v1",
            is_fallback_source=False,
        )

    def _fetch_trading_calendar(self, request: IngestionRequest) -> pd.DataFrame:
        start_date, end_date = _date_window(request)
        exchanges = _context_list(request, "exchanges")
        if not exchanges:
            exchange = request.context.get("exchange")
            exchanges = [str(exchange)] if exchange else ["SH", "SZ"]
        frames: list[pd.DataFrame] = []
        for tushare_exchange in _unique_list(
            [_tushare_exchange(exchange) for exchange in exchanges]
        ):
            frame = self._client.get_trade_calendar(
                start_date.isoformat(), end_date.isoformat(), exchange=tushare_exchange
            )
            if not frame.empty:
                frame = frame.copy()
                frame["exchange"] = _canonical_exchange(tushare_exchange)
            frames.append(frame)
        return _concat_or_empty(
            frames, ["exchange", "trade_date", "is_open", "pretrade_date"]
        )

    def _fetch_instruments(self, request: IngestionRequest) -> pd.DataFrame:
        instrument_type = request.context.get("instrument_type")
        frames: list[pd.DataFrame] = []
        if instrument_type in (None, "etf"):
            etfs = self._client.get_etf_catalog()
            if not etfs.empty:
                etfs = etfs.copy()
                etfs["instrument_type"] = "etf"
                frames.append(etfs)
        if instrument_type in (None, "index"):
            indices = self._client.get_index_catalog()
            if not indices.empty:
                indices = indices.copy()
                indices["instrument_type"] = "index"
                frames.append(indices)
        return _concat_or_empty(
            frames,
            ["code", "name", "list_date", "delist_date", "instrument_type"],
        )

    def _fetch_futures_instruments(self, request: IngestionRequest) -> pd.DataFrame:
        exchanges = _context_list(request, "exchanges") or list(_FUTURES_EXCHANGES)
        frames = [
            self._client.get_futures_basic(exchange)
            for exchange in _unique_list(exchanges)
        ]
        return _concat_or_empty(frames, list(_empty_futures_instruments().columns))

    def _fetch_market_daily(
        self, request: IngestionRequest, instrument_type: str
    ) -> pd.DataFrame:
        start_date, end_date = _date_window(request)
        instrument_ids = _context_list(request, "instrument_ids")
        if not instrument_ids:
            return _empty_market_daily(instrument_type)
        frames: list[pd.DataFrame] = []
        for instrument_id in _unique_list(instrument_ids):
            if instrument_type == "etf":
                frame = self._client.get_etf_daily(
                    str(instrument_id), start_date.isoformat(), end_date.isoformat()
                )
            else:
                frame = self._client.get_index_daily(
                    str(instrument_id), start_date.isoformat(), end_date.isoformat()
                )
            if not frame.empty:
                frames.append(frame)
        return _concat_or_empty(
            frames, list(_empty_market_daily(instrument_type).columns)
        )

    def _fetch_etf_adj_factor(self, request: IngestionRequest) -> pd.DataFrame:
        start_date, end_date = _date_window(request)
        instrument_ids = _context_list(request, "instrument_ids")
        if not instrument_ids:
            return _empty_etf_adj_factor()
        frames: list[pd.DataFrame] = []
        for instrument_id in _unique_list(instrument_ids):
            frame = self._client.get_etf_adj_factor(
                str(instrument_id), start_date.isoformat(), end_date.isoformat()
            )
            if not frame.empty:
                frames.append(frame)
        return _concat_or_empty(frames, list(_empty_etf_adj_factor().columns))

    def _fetch_futures_mapping(self, request: IngestionRequest) -> pd.DataFrame:
        start_date, end_date = _date_window(request)
        root_codes = _context_list(request, "root_codes")
        frames = [
            self._client.get_futures_mapping(
                root_code, start_date.isoformat(), end_date.isoformat()
            )
            for root_code in _unique_list(root_codes)
        ]
        return _concat_or_empty(frames, list(_empty_futures_mapping().columns))

    def _fetch_futures_contract_daily(self, request: IngestionRequest) -> pd.DataFrame:
        start_date, end_date = _date_window(request)
        contract_codes = _context_list(request, "contract_codes")
        frames = [
            self._client.get_futures_daily(
                contract_code, start_date.isoformat(), end_date.isoformat()
            )
            for contract_code in _unique_list(contract_codes)
        ]
        return _concat_or_empty(frames, list(_empty_futures_daily().columns))

    def _fetch_daily_rates(self, request: IngestionRequest) -> pd.DataFrame:
        start_date, end_date = _date_window(request)
        return self._client.get_shibor(start_date.isoformat(), end_date.isoformat())

    def _fetch_macro_slow_fields(self, request: IngestionRequest) -> pd.DataFrame:
        start_date, end_date = _date_window(request)
        return self._client.get_macro_slow_fields(
            start_date.isoformat(), end_date.isoformat()
        )

    def _fetch_lpr(self, request: IngestionRequest) -> pd.DataFrame:
        start_date, end_date = _date_window(request)
        return self._client.get_lpr(start_date.isoformat(), end_date.isoformat())

    def _fetch_gov_curve_points(self, request: IngestionRequest) -> pd.DataFrame:
        start_date, end_date = _date_window(request)
        primary_error: Exception | None = None
        try:
            frame = self._client.get_gov_curve_points(
                start_date.isoformat(), end_date.isoformat()
            )
            if not frame.empty:
                return frame
        except Exception as exc:
            primary_error = exc
        fallback = self._fetch_akshare_gov_curve_points(start_date, end_date)
        if not fallback.empty:
            return fallback
        if primary_error is not None:
            raise primary_error
        return fallback

    def _fetch_akshare_gov_curve_points(
        self, start_date: date, end_date: date
    ) -> pd.DataFrame:
        """Return ChinaBond government curve points from AKShare fallback."""

        akshare = self._akshare or _import_akshare()
        if akshare is None or not hasattr(akshare, "bond_china_yield"):
            return _empty_gov_curve_points()
        frames: list[pd.DataFrame] = []
        for window_start, window_end in _akshare_curve_windows(start_date, end_date):
            frame = akshare.bond_china_yield(
                start_date=window_start.strftime("%Y%m%d"),
                end_date=window_end.strftime("%Y%m%d"),
            )
            if frame is None or frame.empty:
                continue
            normalized = frame.copy()
            if "曲线名称" in normalized.columns:
                normalized = normalized[
                    normalized["曲线名称"].astype(str).eq("中债国债收益率曲线")
                ]
            if normalized.empty:
                continue
            normalized = normalized.rename(
                columns={"日期": "curve_date", "1年": "1y", "10年": "10y"}
            )
            normalized["curve_code"] = "cn_gov_bond"
            frames.append(normalized)
        return _concat_or_empty(frames, list(_empty_gov_curve_points().columns))


def _date_window(request: IngestionRequest) -> tuple[date, date]:
    """Return a concrete request window."""

    return normalize_request_window(request)


def _result_window(
    dataset_name: str, request: IngestionRequest
) -> tuple[date | None, date | None]:
    """Return normalized lineage windows for windowed datasets."""

    if dataset_name == "reference.instruments":
        return request.request_start, request.request_end
    return normalize_request_window(request)


def _utc_now() -> datetime:
    """Return a naive UTC timestamp for DuckDB compatibility."""

    return datetime.now(UTC).replace(tzinfo=None)


def _context_list(request: IngestionRequest, key: str) -> list[str]:
    """Read a string list from request context."""

    value = request.context.get(key)
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    if isinstance(value, list):
        return [str(item) for item in value]
    return []


def _unique_list(values: list[str]) -> list[str]:
    """Return values deduplicated in first-seen order."""

    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        result.append(value)
    return result


def _tushare_exchange(exchange: str) -> str:
    """Return the Tushare exchange code for one canonical or provider code."""

    text = str(exchange).upper()
    return {"SH": "SSE", "SZ": "SZSE", "SSE": "SSE", "SZSE": "SZSE"}.get(text, text)


def _canonical_exchange(exchange: str) -> str:
    """Return the canonical Stage B exchange suffix for one provider code."""

    text = str(exchange).upper()
    return {"SSE": "SH", "SZSE": "SZ", "SH": "SH", "SZ": "SZ"}.get(text, text)


def _instrument_endpoint(request: IngestionRequest) -> str:
    """Return the Tushare endpoint lineage matching the requested instrument type."""

    instrument_type = request.context.get("instrument_type")
    if instrument_type == "etf":
        return "fund_basic"
    if instrument_type == "index":
        return "index_basic"
    return "fund_basic,index_basic"


def _partition_hints(
    dataset_name: str, request: IngestionRequest
) -> dict[str, str | int]:
    """Return raw partition hints for one fetch."""

    if dataset_name == "reference.instruments":
        snapshot = request.context.get("snapshot_date") or date.today().isoformat()
        return {"snapshot_date": str(snapshot)}
    start, _ = _date_window(request)
    return {"year": start.year, "month": f"{start.month:02d}"}


def _concat_or_empty(frames: list[pd.DataFrame], columns: list[str]) -> pd.DataFrame:
    """Concatenate non-empty frames or return an empty typed frame."""

    non_empty = [frame for frame in frames if not frame.empty]
    if not non_empty:
        return pd.DataFrame({column: pd.Series(dtype="object") for column in columns})
    return pd.concat(non_empty, ignore_index=True)


def _import_akshare() -> Any | None:
    """Import AKShare lazily for the government curve fallback."""

    try:
        import akshare as ak
    except ImportError:
        return None
    return ak


def _akshare_curve_windows(start_date: date, end_date: date) -> list[tuple[date, date]]:
    """Return windows shorter than one year for AKShare curve requests."""

    windows: list[tuple[date, date]] = []
    current = start_date
    while current <= end_date:
        window_end = min(current + timedelta(days=330), end_date)
        windows.append((current, window_end))
        current = window_end + timedelta(days=1)
    return windows


def _empty_gov_curve_points() -> pd.DataFrame:
    """Return an empty government curve payload frame."""

    return pd.DataFrame(
        {
            "curve_date": pd.Series(dtype="datetime64[ns]"),
            "curve_code": pd.Series(dtype="object"),
            "1y": pd.Series(dtype="float64"),
            "10y": pd.Series(dtype="float64"),
        }
    )


def _empty_market_daily(instrument_type: str) -> pd.DataFrame:
    """Return an empty market daily payload frame."""

    code_column = "etf_code" if instrument_type == "etf" else "index_code"
    return pd.DataFrame(
        {
            "date": pd.Series(dtype="datetime64[ns]"),
            code_column: pd.Series(dtype="object"),
            "open": pd.Series(dtype="float64"),
            "high": pd.Series(dtype="float64"),
            "low": pd.Series(dtype="float64"),
            "close": pd.Series(dtype="float64"),
            "pre_close": pd.Series(dtype="float64"),
            "change": pd.Series(dtype="float64"),
            "pct_chg": pd.Series(dtype="float64"),
            "volume": pd.Series(dtype="float64"),
            "amount": pd.Series(dtype="float64"),
        }
    )


def _empty_etf_adj_factor() -> pd.DataFrame:
    """Return an empty ETF adjustment factor payload frame."""

    return pd.DataFrame(
        {
            "date": pd.Series(dtype="datetime64[ns]"),
            "etf_code": pd.Series(dtype="object"),
            "adj_factor": pd.Series(dtype="float64"),
        }
    )


def _empty_futures_mapping() -> pd.DataFrame:
    """Return an empty futures dominant-contract mapping frame."""

    return pd.DataFrame(
        {
            "root_code": pd.Series(dtype="object"),
            "trade_date": pd.Series(dtype="datetime64[ns]"),
            "active_contract": pd.Series(dtype="object"),
        }
    )


def _empty_futures_instruments() -> pd.DataFrame:
    """Return an empty futures instrument metadata frame."""

    return pd.DataFrame(
        {
            "contract_code": pd.Series(dtype="object"),
            "symbol": pd.Series(dtype="object"),
            "exchange": pd.Series(dtype="object"),
            "name": pd.Series(dtype="object"),
            "futures_code": pd.Series(dtype="object"),
            "multiplier": pd.Series(dtype="float64"),
            "trade_unit": pd.Series(dtype="object"),
            "per_unit": pd.Series(dtype="float64"),
            "quote_unit": pd.Series(dtype="object"),
            "list_date": pd.Series(dtype="datetime64[ns]"),
            "delist_date": pd.Series(dtype="datetime64[ns]"),
        }
    )


def _empty_futures_daily() -> pd.DataFrame:
    """Return an empty concrete futures contract daily frame."""

    columns = {
        "contract_code": pd.Series(dtype="object"),
        "trade_date": pd.Series(dtype="datetime64[ns]"),
    }
    for column in (
        "pre_close",
        "pre_settle",
        "open",
        "high",
        "low",
        "close",
        "settle",
        "change1",
        "change2",
        "volume",
        "amount",
        "oi",
        "oi_chg",
    ):
        columns[column] = pd.Series(dtype="float64")
    return pd.DataFrame(columns)
