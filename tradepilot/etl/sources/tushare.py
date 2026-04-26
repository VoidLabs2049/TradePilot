"""Tushare source adapter for Stage B ETL datasets."""

from __future__ import annotations

from datetime import UTC, date, datetime
from typing import Any

import pandas as pd

from tradepilot.data.tushare_client import TushareClient
from tradepilot.etl.models import IngestionRequest, SourceFetchResult
from tradepilot.etl.sources.base import BaseSourceAdapter, SourceRole


class TushareSourceAdapter(BaseSourceAdapter):
    """Dataset-aware Tushare adapter returning typed DataFrame payloads."""

    source_name = "tushare"
    source_role = SourceRole.PRIMARY

    _SUPPORTED = {
        "reference.trading_calendar",
        "reference.instruments",
        "market.etf_daily",
        "market.index_daily",
    }

    def __init__(self, client: TushareClient | Any | None = None) -> None:
        self._client = client or TushareClient()

    def supports_dataset(self, dataset_name: str) -> bool:
        """Return whether this adapter can fetch one dataset."""

        return dataset_name in self._SUPPORTED

    def fetch(self, dataset_name: str, request: IngestionRequest) -> SourceFetchResult:
        """Fetch one Stage B dataset from Tushare."""

        if not self.supports_dataset(dataset_name):
            raise KeyError(f"tushare source does not support dataset: {dataset_name}")
        if dataset_name == "reference.trading_calendar":
            payload = self._fetch_trading_calendar(request)
            endpoint = "trade_cal"
        elif dataset_name == "reference.instruments":
            payload = self._fetch_instruments(request)
            endpoint = "fund_basic,index_basic"
        elif dataset_name == "market.etf_daily":
            payload = self._fetch_market_daily(request, instrument_type="etf")
            endpoint = "fund_daily"
        else:
            payload = self._fetch_market_daily(request, instrument_type="index")
            endpoint = "index_daily"
        return SourceFetchResult(
            dataset_name=dataset_name,
            source_name=self.source_name,
            source_endpoint=endpoint,
            payload=payload,
            row_count=len(payload),
            window_start=request.request_start,
            window_end=request.request_end,
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
        for exchange in exchanges:
            tushare_exchange = {
                "SH": "SSE",
                "SZ": "SZSE",
                "SSE": "SSE",
                "SZSE": "SZSE",
            }.get(str(exchange).upper(), str(exchange).upper())
            frame = self._client.get_trade_calendar(
                start_date.isoformat(), end_date.isoformat(), exchange=tushare_exchange
            )
            if not frame.empty:
                frame = frame.copy()
                frame["exchange"] = {"SSE": "SH", "SZSE": "SZ"}.get(
                    tushare_exchange, str(exchange).upper()
                )
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

    def _fetch_market_daily(
        self, request: IngestionRequest, instrument_type: str
    ) -> pd.DataFrame:
        start_date, end_date = _date_window(request)
        instrument_ids = _context_list(request, "instrument_ids")
        if not instrument_ids:
            return _empty_market_daily(instrument_type)
        frames: list[pd.DataFrame] = []
        for instrument_id in instrument_ids:
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


def _date_window(request: IngestionRequest) -> tuple[date, date]:
    """Return a concrete request window."""

    today = date.today()
    start = request.request_start or request.request_end or today
    end = request.request_end or request.request_start or today
    return start, end


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
