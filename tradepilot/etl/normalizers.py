"""Dataset-specific normalizers for the ETL foundation."""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import date
import re
from typing import Any

import pandas as pd
from pydantic import BaseModel, ConfigDict, Field

from tradepilot.etl.timing import (
    monthly_day,
    next_common_open_date,
    next_common_open_date_from_conn,
)


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
        if "adj_factor" not in frame.columns:
            frame["adj_factor"] = pd.NA
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


class FuturesMappingNormalizer(BaseNormalizer):
    """Normalize point-in-time futures dominant-contract mappings."""

    def normalize(
        self,
        raw_payload: pd.DataFrame,
        context: dict[str, Any] | None = None,
    ) -> NormalizationResult:
        """Normalize futures mapping rows without changing provider codes."""

        ctx = context or {}
        frame = raw_payload.copy()
        frame["root_code"] = (
            frame.get("root_code", pd.Series(dtype="object"))
            .astype("string")
            .str.upper()
        )
        frame["active_contract"] = (
            frame.get("active_contract", pd.Series(dtype="object"))
            .astype("string")
            .str.upper()
        )
        frame["trade_date"] = _to_date_series(frame.get("trade_date"))
        frame["source_name"] = str(ctx.get("source_name", ""))
        frame["raw_batch_id"] = ctx.get("raw_batch_id")
        frame["ingested_at"] = pd.Timestamp.utcnow().tz_localize(None)
        frame["quality_status"] = str(ctx.get("quality_status", "pass"))
        canonical = frame.loc[
            :,
            [
                "root_code",
                "trade_date",
                "active_contract",
                "source_name",
                "raw_batch_id",
                "ingested_at",
                "quality_status",
            ],
        ].copy()
        return _result(canonical, context=ctx)


class FuturesInstrumentsNormalizer(BaseNormalizer):
    """Normalize futures contract metadata."""

    _NUMERIC_COLUMNS = ["multiplier", "per_unit"]

    def normalize(
        self,
        raw_payload: pd.DataFrame,
        context: dict[str, Any] | None = None,
    ) -> NormalizationResult:
        """Normalize futures instrument rows with contract sizing fields."""

        ctx = context or {}
        frame = raw_payload.copy()
        for column in (
            "contract_code",
            "symbol",
            "exchange",
            "futures_code",
        ):
            frame[column] = (
                frame.get(column, pd.Series(dtype="object"))
                .astype("string")
                .str.upper()
            )
        for column in (
            "name",
            "trade_unit",
            "quote_unit",
        ):
            frame[column] = frame.get(column, pd.Series(dtype="object")).astype(
                "string"
            )
        for column in self._NUMERIC_COLUMNS:
            frame[column] = pd.to_numeric(frame.get(column), errors="coerce")
        frame["multiplier"] = frame["multiplier"].fillna(frame["per_unit"])
        frame["list_date"] = _to_date_series(frame.get("list_date"))
        frame["delist_date"] = _to_date_series(frame.get("delist_date"))
        frame["source_name"] = str(ctx.get("source_name", ""))
        frame["raw_batch_id"] = ctx.get("raw_batch_id")
        frame["ingested_at"] = pd.Timestamp.utcnow().tz_localize(None)
        frame["quality_status"] = str(ctx.get("quality_status", "pass"))
        canonical = frame.loc[
            :,
            [
                "contract_code",
                "symbol",
                "exchange",
                "name",
                "futures_code",
                "multiplier",
                "trade_unit",
                "per_unit",
                "quote_unit",
                "list_date",
                "delist_date",
                "source_name",
                "raw_batch_id",
                "ingested_at",
                "quality_status",
            ],
        ].copy()
        return _result(canonical, context=ctx)


class FuturesContractDailyNormalizer(BaseNormalizer):
    """Normalize unadjusted concrete futures contract daily bars."""

    _NUMERIC_COLUMNS = [
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
    ]

    def normalize(
        self,
        raw_payload: pd.DataFrame,
        context: dict[str, Any] | None = None,
    ) -> NormalizationResult:
        """Normalize futures daily rows while retaining settlement and OI fields."""

        ctx = context or {}
        frame = raw_payload.copy()
        frame["contract_code"] = (
            frame.get("contract_code", pd.Series(dtype="object"))
            .astype("string")
            .str.upper()
        )
        frame["trade_date"] = _to_date_series(frame.get("trade_date"))
        for column in self._NUMERIC_COLUMNS:
            if column not in frame.columns:
                frame[column] = pd.NA
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
        frame["source_name"] = str(ctx.get("source_name", ""))
        frame["raw_batch_id"] = ctx.get("raw_batch_id")
        frame["ingested_at"] = pd.Timestamp.utcnow().tz_localize(None)
        frame["quality_status"] = str(ctx.get("quality_status", "pass"))
        canonical = frame.loc[
            :,
            [
                "contract_code",
                "trade_date",
                *self._NUMERIC_COLUMNS,
                "source_name",
                "raw_batch_id",
                "ingested_at",
                "quality_status",
            ],
        ].copy()
        return _result(canonical, context=ctx)


class MacroSlowFieldsNormalizer(BaseNormalizer):
    """Normalize slow macro observations into long canonical facts."""

    _FIELD_COLUMNS = {
        "official_pmi": ("official_pmi", "pmi", "manufacturing_pmi"),
    }
    _FIELD_ROLES = {
        "official_pmi": "primary",
    }
    _FIELD_UNITS = {
        "official_pmi": "index_point",
    }

    def normalize(
        self,
        raw_payload: pd.DataFrame,
        context: dict[str, Any] | None = None,
    ) -> NormalizationResult:
        """Normalize supported slow macro payloads."""

        ctx = context or {}
        rows: list[dict[str, Any]] = []
        for _, raw_row in raw_payload.iterrows():
            if "field_name" in raw_payload.columns and "value" in raw_payload.columns:
                field_name = _normalize_macro_field_name(raw_row.get("field_name"))
                if field_name in self._FIELD_ROLES:
                    rows.append(
                        self._row(ctx, raw_row, field_name, raw_row.get("value"))
                    )
                continue
            for field_name, candidates in self._FIELD_COLUMNS.items():
                column = _first_existing(raw_payload, list(candidates))
                if column is None:
                    continue
                rows.append(self._row(ctx, raw_row, field_name, raw_row.get(column)))
        canonical = pd.DataFrame(rows, columns=_MACRO_SLOW_FIELDS_COLUMNS)
        return _result(canonical, context=ctx)

    def _row(
        self,
        ctx: dict[str, Any],
        raw_row: pd.Series,
        field_name: str,
        value: object,
    ) -> dict[str, Any]:
        period_label = _period_label(raw_row) or _period_label_from_date(raw_row)
        release_date = _coerce_date(raw_row.get("release_date"))
        if release_date is None and period_label is not None:
            # Conservative PMI timing: use the first day after the period month;
            # actual official PMI is usually released near month end.
            release_date = _monthly_release_date(period_label, 1)
        effective_date = _coerce_date(raw_row.get("effective_date"))
        if effective_date is None:
            effective_date = _next_common_open_from_context(ctx, release_date)
        return {
            "field_name": field_name,
            "period_label": period_label,
            "period_type": "monthly",
            "value": _coerce_number(value),
            "unit": self._FIELD_UNITS[field_name],
            "field_role": self._FIELD_ROLES[field_name],
            "release_date": release_date,
            "effective_date": effective_date,
            "definition_regime": str(raw_row.get("definition_regime") or ""),
            "regime_note": str(raw_row.get("regime_note") or ""),
            "source_name": str(ctx.get("source_name", "")),
            "raw_batch_id": ctx.get("raw_batch_id"),
            "ingested_at": pd.Timestamp.utcnow().tz_localize(None),
            "revision_note": str(
                ctx.get(
                    "revision_note",
                    "latest_history_only_unless_vintage_captured",
                )
            ),
            "source_caveat": str(
                ctx.get("source_caveat", "tushare_wrapper_latest_history_caveat")
            ),
            "quality_status": str(ctx.get("quality_status", "pass")),
        }


class DailyRatesNormalizer(BaseNormalizer):
    """Normalize daily rates into long canonical facts."""

    _FIELD_COLUMNS = {
        "shibor_1w": ("shibor_1w", "1w", "1_week"),
        "shibor_overnight": ("shibor_overnight", "overnight", "on"),
    }
    _FIELD_ROLES = {
        "shibor_1w": "primary",
        "shibor_overnight": "confirmatory",
    }

    def normalize(
        self,
        raw_payload: pd.DataFrame,
        context: dict[str, Any] | None = None,
    ) -> NormalizationResult:
        """Normalize Shibor daily rate payloads."""

        ctx = context or {}
        rows: list[dict[str, Any]] = []
        for _, raw_row in raw_payload.iterrows():
            if "field_name" in raw_payload.columns and "value" in raw_payload.columns:
                field_name = _normalize_rate_field_name(raw_row.get("field_name"))
                if field_name in self._FIELD_ROLES:
                    rows.append(
                        self._row(ctx, raw_row, field_name, raw_row.get("value"))
                    )
                continue
            for field_name, candidates in self._FIELD_COLUMNS.items():
                column = _first_existing(raw_payload, list(candidates))
                if column is None:
                    continue
                rows.append(self._row(ctx, raw_row, field_name, raw_row.get(column)))
        canonical = pd.DataFrame(rows, columns=_DAILY_RATES_COLUMNS)
        return _result(canonical, context=ctx)

    def _row(
        self,
        ctx: dict[str, Any],
        raw_row: pd.Series,
        field_name: str,
        value: object,
    ) -> dict[str, Any]:
        trade_date = _coerce_date(
            _first_row_value(raw_row, ["trade_date", "date", "quote_date"])
        )
        return {
            "field_name": field_name,
            "trade_date": trade_date,
            "value": _coerce_number(value),
            "unit": "percent",
            "field_role": self._FIELD_ROLES[field_name],
            "release_date": trade_date,
            "effective_date": trade_date,
            "source_name": str(ctx.get("source_name", "")),
            "raw_batch_id": ctx.get("raw_batch_id"),
            "ingested_at": pd.Timestamp.utcnow().tz_localize(None),
            "revision_note": str(ctx.get("revision_note", "low_revision_risk")),
            "source_caveat": str(
                ctx.get(
                    "source_caveat",
                    "tushare_wrapper_same_day_availability_caveat",
                )
            ),
            "quality_status": str(ctx.get("quality_status", "pass")),
        }


class LprNormalizer(BaseNormalizer):
    """Normalize loan prime rate rows into long canonical facts."""

    _FIELD_COLUMNS = {
        "lpr_1y": ("lpr_1y", "1y", "one_year"),
        "lpr_5y": ("lpr_5y", "5y", "five_year"),
    }
    _FIELD_ROLES = {
        "lpr_1y": "primary",
        "lpr_5y": "confirmatory",
    }

    def normalize(
        self,
        raw_payload: pd.DataFrame,
        context: dict[str, Any] | None = None,
    ) -> NormalizationResult:
        """Normalize LPR payloads with conservative effective dates."""

        ctx = context or {}
        rows: list[dict[str, Any]] = []
        for _, raw_row in raw_payload.iterrows():
            if "field_name" in raw_payload.columns and "value" in raw_payload.columns:
                field_name = _normalize_lpr_field_name(raw_row.get("field_name"))
                if field_name in self._FIELD_ROLES:
                    rows.append(
                        self._row(ctx, raw_row, field_name, raw_row.get("value"))
                    )
                continue
            for field_name, candidates in self._FIELD_COLUMNS.items():
                column = _first_existing(raw_payload, list(candidates))
                if column is None:
                    continue
                rows.append(self._row(ctx, raw_row, field_name, raw_row.get(column)))
        canonical = pd.DataFrame(rows, columns=_LPR_COLUMNS)
        return _result(canonical, context=ctx)

    def _row(
        self,
        ctx: dict[str, Any],
        raw_row: pd.Series,
        field_name: str,
        value: object,
    ) -> dict[str, Any]:
        quote_date = _coerce_date(
            _first_row_value(raw_row, ["quote_date", "date", "trade_date"])
        )
        inferred_date = False
        if quote_date is None:
            period_label = _period_label(raw_row)
            quote_date = monthly_day(period_label, 20) if period_label else None
            inferred_date = quote_date is not None
        effective_date = _next_common_open_from_context(ctx, quote_date)
        source_caveat = (
            "tushare_wrapper_source_date_inferred_month_20"
            if inferred_date
            else "tushare_wrapper_source_date_used"
        )
        return {
            "field_name": field_name,
            "quote_date": quote_date,
            "value": _coerce_number(value),
            "unit": "percent",
            "field_role": self._FIELD_ROLES[field_name],
            "release_date": quote_date,
            "effective_date": effective_date,
            "source_name": str(ctx.get("source_name", "")),
            "raw_batch_id": ctx.get("raw_batch_id"),
            "ingested_at": pd.Timestamp.utcnow().tz_localize(None),
            "revision_note": str(
                ctx.get(
                    "revision_note",
                    "low_revision_risk_relative_to_other_slow_fields",
                )
            ),
            "source_caveat": str(ctx.get("source_caveat", source_caveat)),
            "quality_status": str(ctx.get("quality_status", "pass")),
        }


class GovCurvePointsNormalizer(BaseNormalizer):
    """Normalize government curve points into long canonical facts."""

    _TENOR_FIELDS = {
        1.0: "cn_gov_1y_yield",
        10.0: "cn_gov_10y_yield",
    }
    _FIELD_ROLES = {
        "cn_gov_1y_yield": "confirmatory",
        "cn_gov_10y_yield": "primary",
    }

    def normalize(
        self,
        raw_payload: pd.DataFrame,
        context: dict[str, Any] | None = None,
    ) -> NormalizationResult:
        """Normalize source curve point payloads."""

        ctx = context or {}
        rows: list[dict[str, Any]] = []
        for _, raw_row in raw_payload.iterrows():
            if "field_name" in raw_payload.columns and "value" in raw_payload.columns:
                field_name = _normalize_curve_field_name(raw_row.get("field_name"))
                if field_name in self._FIELD_ROLES:
                    rows.append(
                        self._row(
                            ctx,
                            raw_row,
                            field_name,
                            raw_row.get("value"),
                            _curve_tenor_from_row(raw_row, field_name),
                        )
                    )
                continue
            for tenor_years, field_name in self._TENOR_FIELDS.items():
                column = _first_existing(
                    raw_payload,
                    [
                        f"{int(tenor_years)}y",
                        f"yield_{int(tenor_years)}y",
                        field_name,
                    ],
                )
                if column is None:
                    continue
                rows.append(
                    self._row(
                        ctx, raw_row, field_name, raw_row.get(column), tenor_years
                    )
                )
        canonical = pd.DataFrame(rows, columns=_GOV_CURVE_POINTS_COLUMNS)
        return _result(canonical, context=ctx)

    def _row(
        self,
        ctx: dict[str, Any],
        raw_row: pd.Series,
        field_name: str,
        value: object,
        tenor_years: float | None,
    ) -> dict[str, Any]:
        curve_date = _coerce_date(
            _first_row_value(raw_row, ["curve_date", "date", "trade_date"])
        )
        release_date = _coerce_date(raw_row.get("release_date")) or curve_date
        effective_date = _coerce_date(raw_row.get("effective_date")) or curve_date
        return {
            "curve_code": str(raw_row.get("curve_code") or "cn_gov_bond"),
            "curve_date": curve_date,
            "tenor_years": tenor_years,
            "field_name": field_name,
            "value": _coerce_number(value),
            "unit": "percent",
            "field_role": self._FIELD_ROLES[field_name],
            "release_date": release_date,
            "effective_date": effective_date,
            "source_name": str(ctx.get("source_name", "")),
            "raw_batch_id": ctx.get("raw_batch_id"),
            "ingested_at": pd.Timestamp.utcnow().tz_localize(None),
            "revision_note": str(
                ctx.get("revision_note", "extraction_method_risk_present")
            ),
            "source_caveat": str(
                ctx.get("source_caveat", "tushare_curve_extraction_caveat")
            ),
            "quality_status": str(ctx.get("quality_status", "pass")),
        }


def get_normalizer(dataset_name: str) -> BaseNormalizer:
    """Return the normalizer for one Stage B dataset."""

    if dataset_name == "reference.trading_calendar":
        return TradingCalendarNormalizer()
    if dataset_name == "reference.instruments":
        return InstrumentNormalizer()
    if dataset_name == "reference.futures_instruments":
        return FuturesInstrumentsNormalizer()
    if dataset_name == "market.etf_adj_factor":
        return EtfAdjFactorNormalizer()
    if dataset_name == "market.futures_mapping":
        return FuturesMappingNormalizer()
    if dataset_name == "market.futures_contract_daily":
        return FuturesContractDailyNormalizer()
    if dataset_name in {"market.etf_daily", "market.index_daily"}:
        return MarketDailyNormalizer()
    if dataset_name == "macro.slow_fields":
        return MacroSlowFieldsNormalizer()
    if dataset_name == "rates.daily_rates":
        return DailyRatesNormalizer()
    if dataset_name == "rates.lpr":
        return LprNormalizer()
    if dataset_name == "rates.gov_curve_points":
        return GovCurvePointsNormalizer()
    raise KeyError(f"no normalizer registered for dataset: {dataset_name}")


_DAILY_RATES_COLUMNS = [
    "field_name",
    "trade_date",
    "value",
    "unit",
    "field_role",
    "release_date",
    "effective_date",
    "source_name",
    "raw_batch_id",
    "ingested_at",
    "revision_note",
    "source_caveat",
    "quality_status",
]
_MACRO_SLOW_FIELDS_COLUMNS = [
    "field_name",
    "period_label",
    "period_type",
    "value",
    "unit",
    "field_role",
    "release_date",
    "effective_date",
    "definition_regime",
    "regime_note",
    "source_name",
    "raw_batch_id",
    "ingested_at",
    "revision_note",
    "source_caveat",
    "quality_status",
]
_LPR_COLUMNS = [
    "field_name",
    "quote_date",
    "value",
    "unit",
    "field_role",
    "release_date",
    "effective_date",
    "source_name",
    "raw_batch_id",
    "ingested_at",
    "revision_note",
    "source_caveat",
    "quality_status",
]
_GOV_CURVE_POINTS_COLUMNS = [
    "curve_code",
    "curve_date",
    "tenor_years",
    "field_name",
    "value",
    "unit",
    "field_role",
    "release_date",
    "effective_date",
    "source_name",
    "raw_batch_id",
    "ingested_at",
    "revision_note",
    "source_caveat",
    "quality_status",
]


def _first_row_value(row: pd.Series, columns: list[str]) -> object:
    """Return the first non-null value from a row."""

    for column in columns:
        if column not in row.index:
            continue
        value = row.get(column)
        if value is not None and not pd.isna(value):
            return value
    return None


def _coerce_date(value: object) -> date | None:
    """Convert a scalar source date to a Python date."""

    if value is None or pd.isna(value):
        return None
    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        return None
    return parsed.date()


def _coerce_number(value: object) -> float | None:
    """Convert source numeric values to finite floats."""

    if value is None or pd.isna(value):
        return None
    number = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(number):
        return None
    return float(number)


def _normalize_rate_field_name(value: object) -> str:
    """Normalize source daily-rate field names to canonical keys."""

    text = str(value or "").strip().lower()
    mapping = {
        "1w": "shibor_1w",
        "1_week": "shibor_1w",
        "shibor_1w": "shibor_1w",
        "on": "shibor_overnight",
        "overnight": "shibor_overnight",
        "shibor_overnight": "shibor_overnight",
    }
    return mapping.get(text, text)


def _normalize_macro_field_name(value: object) -> str:
    """Normalize source macro field names to canonical keys."""

    text = str(value or "").strip().lower()
    mapping = {
        "pmi": "official_pmi",
        "official_pmi": "official_pmi",
        "manufacturing_pmi": "official_pmi",
    }
    return mapping.get(text, text)


def _normalize_lpr_field_name(value: object) -> str:
    """Normalize source LPR field names to canonical keys."""

    text = str(value or "").strip().lower()
    mapping = {
        "1y": "lpr_1y",
        "one_year": "lpr_1y",
        "lpr_1y": "lpr_1y",
        "5y": "lpr_5y",
        "five_year": "lpr_5y",
        "lpr_5y": "lpr_5y",
    }
    return mapping.get(text, text)


def _normalize_curve_field_name(value: object) -> str:
    """Normalize source curve field names to canonical keys."""

    text = str(value or "").strip().lower()
    mapping = {
        "1y": "cn_gov_1y_yield",
        "cn_gov_1y_yield": "cn_gov_1y_yield",
        "yield_1y": "cn_gov_1y_yield",
        "10y": "cn_gov_10y_yield",
        "cn_gov_10y_yield": "cn_gov_10y_yield",
        "yield_10y": "cn_gov_10y_yield",
    }
    return mapping.get(text, text)


def _curve_tenor_from_row(row: pd.Series, field_name: str) -> float | None:
    """Return tenor years from a curve row or canonical field name."""

    value = row.get("tenor_years")
    number = _coerce_number(value)
    if number is not None:
        return number
    return {
        "cn_gov_1y_yield": 1.0,
        "cn_gov_10y_yield": 10.0,
    }.get(field_name)


def _period_label(row: pd.Series) -> str | None:
    """Return a YYYY-MM period label from common source columns."""

    value = _first_row_value(row, ["period_label", "period", "month"])
    if value is None:
        return None
    text = str(value).strip()
    if re.fullmatch(r"\d{6}", text):
        return f"{text[:4]}-{text[4:]}"
    if re.fullmatch(r"\d{4}-\d{2}", text):
        return text
    return None


def _period_label_from_date(row: pd.Series) -> str | None:
    """Return YYYY-MM from date-like source columns."""

    value = _first_row_value(row, ["date", "release_date", "effective_date"])
    parsed = _coerce_date(value)
    if parsed is None:
        return None
    return f"{parsed.year:04d}-{parsed.month:02d}"


def _monthly_release_date(period_label: str, day: int) -> date:
    """Return a conservative release date in the month after a period."""

    year_text, month_text = period_label.split("-", 1)
    year = int(year_text)
    month = int(month_text) + 1
    if month == 13:
        year += 1
        month = 1
    return date(year, month, day)


def _next_common_open_from_context(
    context: dict[str, Any], target_date: date | None
) -> date | None:
    """Return the next SH/SZ common open date using normalizer context."""

    if target_date is None:
        return None
    calendar = context.get("canonical_trading_calendar")
    if isinstance(calendar, pd.DataFrame):
        return next_common_open_date(calendar, target_date)
    conn = context.get("conn")
    if conn is not None:
        return next_common_open_date_from_conn(conn, target_date)
    return target_date


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
