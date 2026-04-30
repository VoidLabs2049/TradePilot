"""Dataset-specific normalizers for the ETL foundation."""

from __future__ import annotations

from abc import ABC, abstractmethod
import re
from typing import Any

import pandas as pd
from pydantic import BaseModel, ConfigDict, Field


class NormalizationResult(BaseModel):
    """Canonical rows and lineage metadata produced by one normalizer."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    canonical_payload: pd.DataFrame = Field(
        description="Normalized records that conform to the dataset canonical schema."
    )
    canonical_rows: list[dict[str, Any]] = Field(
        default_factory=list,
        description="JSON-friendly copy of normalized records.",
    )
    lineage_metadata: dict[str, Any] = Field(
        default_factory=dict,
        description="Source, transformation, and provenance details for the normalized rows.",
    )


class BaseNormalizer(ABC):
    """Base interface for dataset-specific normalizers."""

    @abstractmethod
    def normalize(
        self,
        raw_payload: pd.DataFrame,
        context: dict[str, Any] | None = None,
    ) -> NormalizationResult:
        """Transform raw payloads into canonical rows and lineage metadata."""

        raise NotImplementedError


class TradingCalendarNormalizer(BaseNormalizer):
    """Normalize source trading-calendar payloads."""

    def normalize(
        self,
        raw_payload: pd.DataFrame,
        context: dict[str, Any] | None = None,
    ) -> NormalizationResult:
        """Normalize a trading calendar frame."""

        frame = raw_payload.copy()
        if "cal_date" in frame.columns and "trade_date" not in frame.columns:
            frame = frame.rename(columns={"cal_date": "trade_date"})
        if "exchange" not in frame.columns:
            frame["exchange"] = (context or {}).get("exchange", "SH")
        frame["exchange"] = frame["exchange"].map(_normalize_exchange)
        frame["trade_date"] = _to_date_series(frame.get("trade_date"))
        frame["pretrade_date"] = _to_date_series(frame.get("pretrade_date"))
        frame["is_open"] = frame.get("is_open", False).map(_to_bool)
        canonical = frame.loc[
            :, ["exchange", "trade_date", "is_open", "pretrade_date"]
        ].copy()
        return _result(canonical, context=context)


class InstrumentNormalizer(BaseNormalizer):
    """Normalize ETF and index instrument metadata."""

    _SUPPORTED_CODE_RE = re.compile(r"^\d{6}(\.(SH|SZ))?$")

    def normalize(
        self,
        raw_payload: pd.DataFrame,
        context: dict[str, Any] | None = None,
    ) -> NormalizationResult:
        """Normalize instrument metadata."""

        ctx = context or {}
        frame = raw_payload.copy()
        source_name = str(ctx.get("source_name", ""))
        if "ts_code" in frame.columns and "source_instrument_id" not in frame.columns:
            frame["source_instrument_id"] = frame["ts_code"]
        elif "code" in frame.columns and "source_instrument_id" not in frame.columns:
            frame["source_instrument_id"] = frame["code"]
        if "name" in frame.columns and "instrument_name" not in frame.columns:
            frame["instrument_name"] = frame["name"]
        if "instrument_type" not in frame.columns:
            frame["instrument_type"] = ctx.get("instrument_type")
        frame["instrument_type"] = frame["instrument_type"].astype("string").str.lower()
        supported_code = (
            frame["source_instrument_id"].map(_is_supported_stage_b_code).astype(bool)
        )
        frame = frame[supported_code].copy()
        frame["instrument_id"] = frame.apply(
            lambda row: normalize_instrument_id(
                row.get("source_instrument_id") or row.get("instrument_id"),
                row.get("instrument_type"),
            ),
            axis=1,
        )
        frame["exchange"] = frame.apply(
            lambda row: _normalize_exchange(row.get("exchange"))
            or _suffix_exchange(row.get("instrument_id")),
            axis=1,
        )
        frame["list_date"] = _to_date_series(frame.get("list_date"))
        frame["delist_date"] = _to_date_series(frame.get("delist_date"))
        if "is_active" not in frame.columns:
            frame["is_active"] = True
        frame["is_active"] = frame["is_active"].map(_to_bool)
        frame["source_name"] = source_name
        canonical = frame.loc[
            :,
            [
                "instrument_id",
                "source_instrument_id",
                "instrument_name",
                "instrument_type",
                "exchange",
                "list_date",
                "delist_date",
                "is_active",
                "source_name",
            ],
        ].copy()
        return _result(canonical, context=ctx)


class MarketDailyNormalizer(BaseNormalizer):
    """Normalize ETF and index daily market data."""

    def normalize(
        self,
        raw_payload: pd.DataFrame,
        context: dict[str, Any] | None = None,
    ) -> NormalizationResult:
        """Normalize market daily payloads."""

        ctx = context or {}
        frame = raw_payload.copy()
        source_name = str(ctx.get("source_name", ""))
        raw_batch_id = ctx.get("raw_batch_id")
        if "date" in frame.columns and "trade_date" not in frame.columns:
            frame = frame.rename(columns={"date": "trade_date"})
        if "vol" in frame.columns and "volume" not in frame.columns:
            frame = frame.rename(columns={"vol": "volume"})
        code_column = _first_existing(
            frame, ["instrument_id", "ts_code", "etf_code", "index_code", "stock_code"]
        )
        if code_column is None:
            frame["instrument_id"] = None
        else:
            frame["instrument_id"] = frame[code_column].map(
                lambda value: normalize_instrument_id(value, ctx.get("instrument_type"))
            )
        frame["trade_date"] = _to_date_series(frame.get("trade_date"))
        for column in [
            "open",
            "high",
            "low",
            "close",
            "pre_close",
            "change",
            "pct_chg",
            "volume",
            "amount",
        ]:
            if column not in frame.columns:
                frame[column] = pd.NA
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
        frame["source_name"] = source_name
        frame["raw_batch_id"] = raw_batch_id
        frame["ingested_at"] = pd.Timestamp.utcnow().tz_localize(None)
        frame["quality_status"] = str(ctx.get("quality_status", "pass"))
        canonical = frame.loc[
            :,
            [
                "instrument_id",
                "trade_date",
                "open",
                "high",
                "low",
                "close",
                "pre_close",
                "change",
                "pct_chg",
                "volume",
                "amount",
                "source_name",
                "raw_batch_id",
                "ingested_at",
                "quality_status",
            ],
        ].copy()
        return _result(canonical, context=ctx)


class EtfAdjFactorNormalizer(BaseNormalizer):
    """Normalize ETF adjustment factor rows."""

    def normalize(
        self,
        raw_payload: pd.DataFrame,
        context: dict[str, Any] | None = None,
    ) -> NormalizationResult:
        """Normalize ETF adjustment factor payloads."""

        ctx = context or {}
        frame = raw_payload.copy()
        source_name = str(ctx.get("source_name", ""))
        raw_batch_id = ctx.get("raw_batch_id")
        if "date" in frame.columns and "trade_date" not in frame.columns:
            frame = frame.rename(columns={"date": "trade_date"})
        code_column = _first_existing(frame, ["instrument_id", "ts_code", "etf_code"])
        if code_column is None:
            frame["instrument_id"] = None
        else:
            frame["instrument_id"] = frame[code_column].map(
                lambda value: normalize_instrument_id(value, "etf")
            )
        frame["trade_date"] = _to_date_series(frame.get("trade_date"))
        frame["adj_factor"] = pd.to_numeric(frame.get("adj_factor"), errors="coerce")
        frame["source_name"] = source_name
        frame["raw_batch_id"] = raw_batch_id
        frame["ingested_at"] = pd.Timestamp.utcnow().tz_localize(None)
        frame["quality_status"] = str(ctx.get("quality_status", "pass"))
        canonical = frame.loc[
            :,
            [
                "instrument_id",
                "trade_date",
                "adj_factor",
                "source_name",
                "raw_batch_id",
                "ingested_at",
                "quality_status",
            ],
        ].copy()
        return _result(canonical, context=ctx)


def get_normalizer(dataset_name: str) -> BaseNormalizer:
    """Return the normalizer for one Stage B dataset."""

    if dataset_name == "reference.trading_calendar":
        return TradingCalendarNormalizer()
    if dataset_name == "reference.instruments":
        return InstrumentNormalizer()
    if dataset_name == "market.etf_adj_factor":
        return EtfAdjFactorNormalizer()
    if dataset_name in {"market.etf_daily", "market.index_daily"}:
        return MarketDailyNormalizer()
    raise KeyError(f"no normalizer registered for dataset: {dataset_name}")


def normalize_instrument_id(
    value: object, instrument_type: object = None
) -> str | None:
    """Normalize source codes to the canonical six-digit exchange-suffixed form."""

    if value is None or pd.isna(value):
        return None
    text = str(value).strip().upper()
    if not text:
        return None
    if "." in text:
        code, suffix = text.split(".", 1)
        return f"{code.zfill(6)}.{_normalize_exchange(suffix) or suffix}"
    code = text.zfill(6)
    kind = str(instrument_type or "").lower()
    if kind == "index":
        exchange = "SZ" if code.startswith("399") else "SH"
    else:
        exchange = "SH" if code.startswith(("5", "6")) else "SZ"
    return f"{code}.{exchange}"


def _is_supported_stage_b_code(value: object) -> bool:
    """Return whether a source code fits the Stage B six-digit SH/SZ scope."""

    if value is None or pd.isna(value):
        return False
    return bool(
        InstrumentNormalizer._SUPPORTED_CODE_RE.match(str(value).strip().upper())
    )


def _result(
    canonical: pd.DataFrame, context: dict[str, Any] | None = None
) -> NormalizationResult:
    """Build a normalization result with both DataFrame and record views."""

    return NormalizationResult(
        canonical_payload=canonical,
        canonical_rows=canonical.where(pd.notna(canonical), None).to_dict("records"),
        lineage_metadata=dict(context or {}),
    )


def _first_existing(frame: pd.DataFrame, columns: list[str]) -> str | None:
    """Return the first existing column name from a list."""

    for column in columns:
        if column in frame.columns:
            return column
    return None


def _to_date_series(values: Any) -> pd.Series:
    """Convert a column-like value to Python date objects."""

    if values is None:
        return pd.Series(dtype="object")
    series = pd.to_datetime(values, errors="coerce")
    return series.dt.date


def _to_bool(value: object) -> bool | None:
    """Normalize common source boolean encodings."""

    if value is None or pd.isna(value):
        return None
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"1", "true", "t", "yes", "y", "open"}:
        return True
    if text in {"0", "false", "f", "no", "n", "closed"}:
        return False
    return None


def _normalize_exchange(value: object) -> str | None:
    """Normalize common exchange names to Stage B suffixes."""

    if value is None or pd.isna(value):
        return None
    text = str(value).strip().upper()
    mapping = {"SSE": "SH", "SHSE": "SH", "SH": "SH", "SZSE": "SZ", "SZ": "SZ"}
    return mapping.get(text, text)


def _suffix_exchange(instrument_id: object) -> str | None:
    """Return the suffix exchange from a canonical instrument id."""

    if instrument_id is None or pd.isna(instrument_id):
        return None
    text = str(instrument_id)
    if "." not in text:
        return None
    return _normalize_exchange(text.rsplit(".", 1)[-1])
