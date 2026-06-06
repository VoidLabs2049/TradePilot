"""Dataset-specific validators for the ETL foundation."""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import UTC, date, datetime, timedelta
import json
import math
import re
from typing import Any

import pandas as pd
from pydantic import BaseModel

from tradepilot.etl.models import ValidationResultRecord, ValidationStatus


class ValidationRuleDefinition(BaseModel):
    """Lightweight metadata for one validation rule."""

    rule_name: str
    level: str
    description: str | None = None


class BaseValidator(ABC):
    """Base interface for dataset validators."""

    @abstractmethod
    def validate(
        self,
        payload: pd.DataFrame,
        context: dict[str, Any] | None = None,
    ) -> list[ValidationResultRecord]:
        """Validate a payload and return structured validation results."""

        raise NotImplementedError


class TradingCalendarValidator(BaseValidator):
    """Validate canonical trading calendar rows."""

    def validate(
        self,
        payload: pd.DataFrame,
        context: dict[str, Any] | None = None,
    ) -> list[ValidationResultRecord]:
        """Validate a trading calendar DataFrame."""

        ctx = context or {}
        results: list[ValidationResultRecord] = []
        _required_columns(payload, ["exchange", "trade_date", "is_open"], ctx, results)
        if results:
            return results

        duplicate_count = int(payload.duplicated(["exchange", "trade_date"]).sum())
        results.append(
            _record(
                ctx,
                "calendar.duplicate_key",
                "dataset",
                ValidationStatus.FAIL if duplicate_count else ValidationStatus.PASS,
                metric_value=duplicate_count,
                threshold_value=0,
                details={
                    "sample": _sample_keys(
                        payload[
                            payload.duplicated(["exchange", "trade_date"], keep=False)
                        ],
                        ["exchange", "trade_date"],
                    )
                },
            )
        )
        missing_dates = payload[payload["trade_date"].isna()]
        _row_records(
            results,
            ctx,
            missing_dates,
            "calendar.trade_date_required",
            ["exchange", "trade_date"],
            "trade_date is required",
        )
        unsupported = payload[~payload["exchange"].isin(["SH", "SZ"])]
        _row_records(
            results,
            ctx,
            unsupported,
            "calendar.exchange_supported",
            ["exchange", "trade_date"],
            "exchange must be SH or SZ",
        )
        bad_bool = payload[
            payload["is_open"].map(lambda value: not isinstance(value, bool))
        ]
        _row_records(
            results,
            ctx,
            bad_bool,
            "calendar.is_open_boolean",
            ["exchange", "trade_date"],
            "is_open must be boolean",
        )
        bad_pretrade = payload[
            payload["pretrade_date"].notna()
            & payload["trade_date"].notna()
            & (payload["pretrade_date"] >= payload["trade_date"])
        ]
        _row_records(
            results,
            ctx,
            bad_pretrade,
            "calendar.pretrade_before_trade_date",
            ["exchange", "trade_date"],
            "pretrade_date must be earlier than trade_date",
        )
        _open_day_pretrade_sequence(results, ctx, payload)
        _calendar_date_continuity(results, ctx, payload)
        return results


class InstrumentValidator(BaseValidator):
    """Validate canonical instrument rows."""

    _ID_RE = re.compile(r"^\d{6}\.(SH|SZ)$")

    def validate(
        self,
        payload: pd.DataFrame,
        context: dict[str, Any] | None = None,
    ) -> list[ValidationResultRecord]:
        """Validate an instrument metadata DataFrame."""

        ctx = context or {}
        results: list[ValidationResultRecord] = []
        _required_columns(
            payload,
            ["instrument_id", "instrument_name", "instrument_type", "exchange"],
            ctx,
            results,
        )
        if results:
            return results

        _row_records(
            results,
            ctx,
            payload[
                payload["instrument_id"].isna()
                | (payload["instrument_id"].astype(str).str.strip() == "")
            ],
            "instruments.instrument_id_required",
            ["instrument_id"],
            "instrument_id is required",
        )
        bad_format = payload[
            payload["instrument_id"].notna()
            & ~payload["instrument_id"].astype(str).str.match(self._ID_RE)
        ]
        _row_records(
            results,
            ctx,
            bad_format,
            "instruments.instrument_id_format",
            ["instrument_id"],
            "instrument_id must match NNNNNN.SH or NNNNNN.SZ",
        )
        duplicate_count = int(payload.duplicated(["instrument_id"]).sum())
        results.append(
            _record(
                ctx,
                "instruments.duplicate_instrument_id",
                "dataset",
                ValidationStatus.FAIL if duplicate_count else ValidationStatus.PASS,
                metric_value=duplicate_count,
                threshold_value=0,
                details={
                    "sample": _sample_keys(
                        payload[payload.duplicated(["instrument_id"], keep=False)],
                        ["instrument_id"],
                    )
                },
            )
        )
        mismatch = payload[
            payload["instrument_id"].notna()
            & payload["exchange"].notna()
            & (
                payload["instrument_id"].astype(str).str.rsplit(".", n=1).str[-1]
                != payload["exchange"].astype(str)
            )
        ]
        _row_records(
            results,
            ctx,
            mismatch,
            "instruments.exchange_suffix_match",
            ["instrument_id"],
            "exchange must match instrument_id suffix",
        )
        _row_records(
            results,
            ctx,
            payload[
                payload["instrument_name"].isna()
                | (payload["instrument_name"].astype(str).str.strip() == "")
            ],
            "instruments.name_required",
            ["instrument_id"],
            "instrument_name is required",
        )
        unsupported_type = payload[~payload["instrument_type"].isin(["etf", "index"])]
        _row_records(
            results,
            ctx,
            unsupported_type,
            "instruments.type_supported",
            ["instrument_id"],
            "instrument_type must be etf or index",
        )
        bad_order = payload[
            payload["list_date"].notna()
            & payload["delist_date"].notna()
            & (payload["list_date"] > payload["delist_date"])
        ]
        _row_records(
            results,
            ctx,
            bad_order,
            "instruments.list_delist_order",
            ["instrument_id"],
            "list_date must not be after delist_date",
        )
        bad_active = payload[
            payload["is_active"].map(lambda value: not isinstance(value, bool))
        ]
        _row_records(
            results,
            ctx,
            bad_active,
            "instruments.active_boolean",
            ["instrument_id"],
            "is_active must be boolean",
        )
        return results


class MarketDailyValidator(BaseValidator):
    """Validate canonical market daily rows."""

    def validate(
        self,
        payload: pd.DataFrame,
        context: dict[str, Any] | None = None,
    ) -> list[ValidationResultRecord]:
        """Validate ETF or index daily market rows."""

        ctx = context or {}
        results: list[ValidationResultRecord] = []
        _required_columns(
            payload, ["instrument_id", "trade_date", "close"], ctx, results
        )
        if results:
            return results

        duplicate_count = int(payload.duplicated(["instrument_id", "trade_date"]).sum())
        results.append(
            _record(
                ctx,
                "market_daily.duplicate_business_key",
                "dataset",
                ValidationStatus.FAIL if duplicate_count else ValidationStatus.PASS,
                metric_value=duplicate_count,
                threshold_value=0,
                details={
                    "sample": _sample_keys(
                        payload[
                            payload.duplicated(
                                ["instrument_id", "trade_date"], keep=False
                            )
                        ],
                        ["instrument_id", "trade_date"],
                    )
                },
            )
        )
        instruments = _instrument_lookup(ctx)
        if instruments is not None:
            missing = payload[
                ~payload["instrument_id"].isin(instruments["instrument_id"])
            ]
            _row_records(
                results,
                ctx,
                missing,
                "market_daily.instrument_exists",
                ["instrument_id", "trade_date"],
                "instrument must exist in canonical_instruments",
            )
            expected_type = ctx.get("instrument_type")
            if expected_type:
                typed = instruments.loc[
                    instruments["instrument_type"].eq(expected_type),
                    "instrument_id",
                ]
                wrong_type = payload[~payload["instrument_id"].isin(typed)]
                _row_records(
                    results,
                    ctx,
                    wrong_type,
                    "market_daily.instrument_type_matches_dataset",
                    ["instrument_id", "trade_date"],
                    f"instrument_type must be {expected_type}",
                )
        calendars = _open_day_lookup(ctx)
        if calendars is not None and not payload.empty:
            calendar_payload = payload
            if (
                "exchange" not in calendar_payload.columns
                and instruments is not None
                and "exchange" in instruments.columns
            ):
                calendar_payload = calendar_payload.merge(
                    instruments.loc[:, ["instrument_id", "exchange"]].drop_duplicates(),
                    on="instrument_id",
                    how="left",
                )
            joined = calendar_payload.merge(
                calendars.assign(_is_open_day=True),
                on=(
                    ["exchange", "trade_date"]
                    if "exchange" in calendar_payload.columns
                    else ["trade_date"]
                ),
                how="left",
            )
            non_open = joined[joined["_is_open_day"].isna()]
            _row_records(
                results,
                ctx,
                non_open,
                "market_daily.trade_date_open",
                ["instrument_id", "trade_date"],
                "trade_date must be an open trading day",
            )
        _row_records(
            results,
            ctx,
            payload[payload["close"].isna()],
            "market_daily.close_required",
            ["instrument_id", "trade_date"],
            "close is required",
        )
        negative_price = payload[
            payload[["open", "high", "low", "close", "pre_close"]].lt(0).any(axis=1)
        ]
        _row_records(
            results,
            ctx,
            negative_price,
            "market_daily.ohlc_non_negative",
            ["instrument_id", "trade_date"],
            "OHLC fields must be non-negative",
        )
        full_ohlc = payload.dropna(subset=["open", "high", "low", "close"])
        bad_order = full_ohlc[
            (full_ohlc["high"] < full_ohlc[["open", "low", "close"]].max(axis=1))
            | (full_ohlc["low"] > full_ohlc[["open", "high", "close"]].min(axis=1))
        ]
        _row_records(
            results,
            ctx,
            bad_order,
            "market_daily.ohlc_order",
            ["instrument_id", "trade_date"],
            "high/low must contain open and close",
        )
        _row_records(
            results,
            ctx,
            payload[payload["volume"].notna() & (payload["volume"] < 0)],
            "market_daily.volume_non_negative",
            ["instrument_id", "trade_date"],
            "volume must be non-negative",
        )
        _row_records(
            results,
            ctx,
            payload[payload["amount"].notna() & (payload["amount"] < 0)],
            "market_daily.amount_non_negative",
            ["instrument_id", "trade_date"],
            "amount must be non-negative",
        )
        extreme = payload[
            payload["pct_chg"].abs() >= float(ctx.get("extreme_return_threshold", 20))
        ]
        _row_records(
            results,
            ctx,
            extreme,
            "market_daily.extreme_return",
            ["instrument_id", "trade_date"],
            "absolute pct_chg exceeds warning threshold",
            status=ValidationStatus.WARNING,
            threshold_value=float(ctx.get("extreme_return_threshold", 20)),
        )
        _market_daily_change_consistency(results, ctx, payload)
        _market_daily_pct_chg_consistency(results, ctx, payload)
        return results


class EtfAdjFactorValidator(BaseValidator):
    """Validate canonical ETF adjustment factor rows."""

    def validate(
        self,
        payload: pd.DataFrame,
        context: dict[str, Any] | None = None,
    ) -> list[ValidationResultRecord]:
        """Validate ETF adjustment factors."""

        ctx = context or {}
        results: list[ValidationResultRecord] = []
        _required_columns(
            payload, ["instrument_id", "trade_date", "adj_factor"], ctx, results
        )
        if results:
            return results

        duplicate_count = int(payload.duplicated(["instrument_id", "trade_date"]).sum())
        results.append(
            _record(
                ctx,
                "etf_adj_factor.duplicate_business_key",
                "dataset",
                ValidationStatus.FAIL if duplicate_count else ValidationStatus.PASS,
                metric_value=duplicate_count,
                threshold_value=0,
                details={
                    "sample": _sample_keys(
                        payload[
                            payload.duplicated(
                                ["instrument_id", "trade_date"], keep=False
                            )
                        ],
                        ["instrument_id", "trade_date"],
                    )
                },
            )
        )
        _row_records(
            results,
            ctx,
            payload[
                payload["instrument_id"].isna()
                | (payload["instrument_id"].astype(str).str.strip() == "")
            ],
            "etf_adj_factor.instrument_id_required",
            ["instrument_id", "trade_date"],
            "instrument_id is required",
        )
        _row_records(
            results,
            ctx,
            payload[payload["trade_date"].isna()],
            "etf_adj_factor.trade_date_required",
            ["instrument_id", "trade_date"],
            "trade_date is required",
        )
        _row_records(
            results,
            ctx,
            payload[payload["adj_factor"].isna()],
            "etf_adj_factor.adj_factor_required",
            ["instrument_id", "trade_date"],
            "adj_factor is required",
        )
        _row_records(
            results,
            ctx,
            payload[payload["adj_factor"].notna() & (payload["adj_factor"] <= 0)],
            "etf_adj_factor.adj_factor_positive",
            ["instrument_id", "trade_date"],
            "adj_factor must be positive",
        )
        instruments = _instrument_lookup(ctx)
        if instruments is not None:
            etfs = instruments.loc[
                instruments["instrument_type"].eq("etf"), "instrument_id"
            ]
            missing = payload[~payload["instrument_id"].isin(etfs)]
            _row_records(
                results,
                ctx,
                missing,
                "etf_adj_factor.instrument_exists",
                ["instrument_id", "trade_date"],
                "instrument must exist as an ETF in canonical_instruments",
            )
        return results


class DailyRatesValidator(BaseValidator):
    """Validate canonical daily rates rows."""

    _FIELD_ROLES = {
        "shibor_1w": "primary",
        "shibor_overnight": "confirmatory",
    }

    def validate(
        self,
        payload: pd.DataFrame,
        context: dict[str, Any] | None = None,
    ) -> list[ValidationResultRecord]:
        """Validate daily rate facts."""

        ctx = context or {}
        return _validate_rate_dataset(
            payload,
            ctx,
            required_columns=[
                "field_name",
                "trade_date",
                "value",
                "unit",
                "field_role",
                "release_date",
                "effective_date",
                "revision_note",
                "source_caveat",
            ],
            key_columns=["field_name", "trade_date"],
            required_date_columns=["trade_date", "release_date", "effective_date"],
            required_text_columns=["revision_note"],
            field_roles=self._FIELD_ROLES,
            prefix="daily_rates",
        )


class MacroSlowFieldsValidator(BaseValidator):
    """Validate canonical slow macro field rows."""

    _FIELD_ROLES = {
        "official_pmi": "primary",
    }
    _FIELD_VALUE_RANGES = {
        "official_pmi": (0.0, 100.0),
    }

    def validate(
        self,
        payload: pd.DataFrame,
        context: dict[str, Any] | None = None,
    ) -> list[ValidationResultRecord]:
        """Validate slow macro facts."""

        ctx = context or {}
        results: list[ValidationResultRecord] = []
        required_columns = [
            "field_name",
            "period_label",
            "period_type",
            "value",
            "unit",
            "field_role",
            "release_date",
            "effective_date",
            "revision_note",
            "source_caveat",
        ]
        _required_columns(payload, required_columns, ctx, results)
        if results:
            return results
        _duplicate_key_result(
            results, ctx, payload, ["field_name", "period_label"], "slow_fields"
        )
        _allowed_field_result(results, ctx, payload, self._FIELD_ROLES, "slow_fields")
        for field_name, (minimum, maximum) in self._FIELD_VALUE_RANGES.items():
            _value_plausibility_result(
                results,
                ctx,
                payload[payload["field_name"].eq(field_name)],
                "slow_fields",
                minimum,
                maximum,
            )
        bad_unit = payload[~payload["unit"].isin({"index_point", "percent"})]
        _row_records(
            results,
            ctx,
            bad_unit,
            "slow_fields.unit_allowed",
            ["field_name", "period_label"],
            "unit must be index_point or percent",
        )
        for column in ("release_date", "effective_date"):
            invalid_dates = _missing_or_unparseable_dates(payload, column)
            _row_records(
                results,
                ctx,
                invalid_dates,
                f"slow_fields.{column}_required",
                ["field_name", "period_label"],
                f"{column} is required and must be date-like",
            )
        _effective_after_release_result(
            results,
            ctx,
            payload,
            "slow_fields",
            ["field_name", "period_label"],
        )
        _field_role_result(results, ctx, payload, self._FIELD_ROLES, "slow_fields")
        _source_caveat_result(results, ctx, payload, "slow_fields")
        return results


class LprValidator(BaseValidator):
    """Validate canonical loan prime rate rows."""

    _FIELD_ROLES = {
        "lpr_1y": "primary",
        "lpr_5y": "confirmatory",
    }

    def validate(
        self,
        payload: pd.DataFrame,
        context: dict[str, Any] | None = None,
    ) -> list[ValidationResultRecord]:
        """Validate LPR facts."""

        ctx = context or {}
        results = _validate_rate_dataset(
            payload,
            ctx,
            required_columns=[
                "field_name",
                "quote_date",
                "value",
                "unit",
                "field_role",
                "release_date",
                "effective_date",
                "revision_note",
                "source_caveat",
            ],
            key_columns=["field_name", "quote_date"],
            required_date_columns=["quote_date", "release_date", "effective_date"],
            required_text_columns=["revision_note"],
            field_roles=self._FIELD_ROLES,
            prefix="lpr",
        )
        if not _has_required_column_failure(results):
            _lpr_effective_date_order_result(results, ctx, payload)
        return results


class GovCurvePointsValidator(BaseValidator):
    """Validate canonical government curve point rows."""

    _FIELD_ROLES = {
        "cn_gov_1y_yield": "confirmatory",
        "cn_gov_10y_yield": "primary",
    }

    def validate(
        self,
        payload: pd.DataFrame,
        context: dict[str, Any] | None = None,
    ) -> list[ValidationResultRecord]:
        """Validate government curve points."""

        ctx = context or {}
        results = _validate_rate_dataset(
            payload,
            ctx,
            required_columns=[
                "curve_code",
                "curve_date",
                "tenor_years",
                "field_name",
                "value",
                "unit",
                "field_role",
                "release_date",
                "effective_date",
                "revision_note",
                "source_caveat",
            ],
            key_columns=["curve_code", "curve_date", "tenor_years"],
            required_date_columns=["curve_date", "release_date", "effective_date"],
            required_text_columns=["revision_note"],
            field_roles=self._FIELD_ROLES,
            prefix="gov_curve_points",
        )
        if not _has_required_column_failure(results):
            bad_tenor = payload[
                payload["tenor_years"].isna()
                | (pd.to_numeric(payload["tenor_years"], errors="coerce") <= 0)
            ]
            _row_records(
                results,
                ctx,
                bad_tenor,
                "gov_curve_points.tenor_positive",
                ["curve_code", "curve_date", "tenor_years"],
                "tenor_years must be positive",
            )
        return results


def get_validator(dataset_name: str) -> BaseValidator:
    """Return the validator for one Stage B dataset."""

    if dataset_name == "reference.trading_calendar":
        return TradingCalendarValidator()
    if dataset_name == "reference.instruments":
        return InstrumentValidator()
    if dataset_name == "market.etf_adj_factor":
        return EtfAdjFactorValidator()
    if dataset_name in {"market.etf_daily", "market.index_daily"}:
        return MarketDailyValidator()
    if dataset_name == "macro.slow_fields":
        return MacroSlowFieldsValidator()
    if dataset_name == "rates.daily_rates":
        return DailyRatesValidator()
    if dataset_name == "rates.lpr":
        return LprValidator()
    if dataset_name == "rates.gov_curve_points":
        return GovCurvePointsValidator()
    raise KeyError(f"no validator registered for dataset: {dataset_name}")


def has_blocking_failures(results: list[ValidationResultRecord]) -> bool:
    """Return whether validation results contain a blocking status."""

    return any(
        result.status in {ValidationStatus.FAIL, ValidationStatus.DEFER}
        for result in results
    )


def validation_counts(results: list[ValidationResultRecord]) -> dict[str, int]:
    """Count validation results by status."""

    counts: dict[str, int] = {}
    for result in results:
        key = result.status.value
        counts[key] = counts.get(key, 0) + 1
    return counts


def _required_columns(
    payload: pd.DataFrame,
    columns: list[str],
    context: dict[str, Any],
    results: list[ValidationResultRecord],
) -> None:
    """Append a contract failure if required columns are missing."""

    missing = [column for column in columns if column not in payload.columns]
    if not missing:
        return
    results.append(
        _record(
            context,
            "normalization_contract.required_columns",
            "contract",
            ValidationStatus.FAIL,
            details={"missing_columns": missing},
        )
    )


def _row_records(
    results: list[ValidationResultRecord],
    context: dict[str, Any],
    frame: pd.DataFrame,
    check_name: str,
    key_columns: list[str],
    message: str,
    status: ValidationStatus = ValidationStatus.FAIL,
    threshold_value: float | None = None,
) -> None:
    """Append row-level records or one pass record for a check."""

    if frame.empty:
        results.append(_record(context, check_name, "row", ValidationStatus.PASS))
        return
    for _, row in frame.head(50).iterrows():
        results.append(
            _record(
                context,
                check_name,
                "row",
                status,
                subject_key=_subject_key(row, key_columns),
                metric_value=None,
                threshold_value=threshold_value,
                details={"message": message},
            )
        )
    if len(frame) > 50:
        results.append(
            _record(
                context,
                check_name,
                "row",
                status,
                metric_value=float(len(frame)),
                threshold_value=threshold_value,
                details={"message": message, "truncated": True},
            )
        )


def _validate_rate_dataset(
    payload: pd.DataFrame,
    context: dict[str, Any],
    required_columns: list[str],
    key_columns: list[str],
    required_date_columns: list[str],
    required_text_columns: list[str],
    field_roles: dict[str, str],
    prefix: str,
) -> list[ValidationResultRecord]:
    """Validate shared canonical rate fact rules."""

    results: list[ValidationResultRecord] = []
    _required_columns(payload, required_columns, context, results)
    if results:
        return results

    _duplicate_key_result(results, context, payload, key_columns, prefix)
    _allowed_field_result(results, context, payload, field_roles, prefix)
    _value_plausibility_result(results, context, payload, prefix, 0.0, 20.0)
    _unit_allowed_result(results, context, payload, prefix)
    for column in required_date_columns:
        invalid_dates = _missing_or_unparseable_dates(payload, column)
        _row_records(
            results,
            context,
            invalid_dates,
            f"{prefix}.{column}_required",
            key_columns,
            f"{column} is required and must be date-like",
        )
    for column in required_text_columns:
        missing_text = payload[
            payload[column].isna() | (payload[column].astype(str).str.strip() == "")
        ]
        _row_records(
            results,
            context,
            missing_text,
            f"{prefix}.{column}_required",
            key_columns,
            f"{column} is required",
        )
    _field_role_result(results, context, payload, field_roles, prefix)
    _source_caveat_result(results, context, payload, prefix)
    return results


def _has_required_column_failure(results: list[ValidationResultRecord]) -> bool:
    """Return whether required-column contract validation failed."""

    return any(
        result.check_name == "normalization_contract.required_columns"
        and result.status == ValidationStatus.FAIL
        for result in results
    )


def _lpr_effective_date_order_result(
    results: list[ValidationResultRecord],
    context: dict[str, Any],
    payload: pd.DataFrame,
) -> None:
    """Validate LPR effective dates after parsing date-like values."""

    release_dates = pd.to_datetime(payload["release_date"], errors="coerce")
    effective_dates = pd.to_datetime(payload["effective_date"], errors="coerce")
    bad_order = payload[
        payload["release_date"].notna()
        & payload["effective_date"].notna()
        & release_dates.notna()
        & effective_dates.notna()
        & (effective_dates < release_dates)
    ]
    _row_records(
        results,
        context,
        bad_order,
        "lpr.effective_date_after_release",
        ["field_name", "quote_date"],
        "effective_date must not precede release_date and dates must be parseable",
    )


def _effective_after_release_result(
    results: list[ValidationResultRecord],
    context: dict[str, Any],
    payload: pd.DataFrame,
    prefix: str,
    key_columns: list[str],
) -> None:
    """Validate effective dates after parsing date-like values."""

    release_dates = pd.to_datetime(payload["release_date"], errors="coerce")
    effective_dates = pd.to_datetime(payload["effective_date"], errors="coerce")
    bad_order = payload[
        payload["release_date"].notna()
        & payload["effective_date"].notna()
        & release_dates.notna()
        & effective_dates.notna()
        & (effective_dates < release_dates)
    ]
    _row_records(
        results,
        context,
        bad_order,
        f"{prefix}.effective_date_after_release",
        key_columns,
        "effective_date must not precede release_date and dates must be parseable",
    )


def _missing_or_unparseable_dates(payload: pd.DataFrame, column: str) -> pd.DataFrame:
    """Return rows where a required date column is missing or unparseable."""

    parsed = pd.to_datetime(payload[column], errors="coerce")
    return payload[payload[column].isna() | parsed.isna()]


def _duplicate_key_result(
    results: list[ValidationResultRecord],
    context: dict[str, Any],
    payload: pd.DataFrame,
    key_columns: list[str],
    prefix: str,
) -> None:
    """Append a duplicate business-key validation result."""

    duplicate_count = int(payload.duplicated(key_columns).sum())
    results.append(
        _record(
            context,
            f"{prefix}.duplicate_business_key",
            "dataset",
            ValidationStatus.FAIL if duplicate_count else ValidationStatus.PASS,
            metric_value=duplicate_count,
            threshold_value=0,
            details={
                "sample": _sample_keys(
                    payload[payload.duplicated(key_columns, keep=False)], key_columns
                )
            },
        )
    )


def _allowed_field_result(
    results: list[ValidationResultRecord],
    context: dict[str, Any],
    payload: pd.DataFrame,
    field_roles: dict[str, str],
    prefix: str,
) -> None:
    """Validate field_name membership."""

    invalid = payload[~payload["field_name"].isin(field_roles.keys())]
    _row_records(
        results,
        context,
        invalid,
        f"{prefix}.field_allowed",
        ["field_name"],
        "field_name is not allowed for this dataset",
    )


def _value_plausibility_result(
    results: list[ValidationResultRecord],
    context: dict[str, Any],
    payload: pd.DataFrame,
    prefix: str,
    minimum: float,
    maximum: float,
) -> None:
    """Validate numeric values are finite and within a broad plausible range."""

    def invalid(value: object) -> bool:
        if value is None or pd.isna(value):
            return True
        try:
            number = float(value)
        except (TypeError, ValueError):
            return True
        return not math.isfinite(number) or number < minimum or number > maximum

    bad = payload[payload["value"].map(invalid)]
    _row_records(
        results,
        context,
        bad,
        f"{prefix}.value_plausible",
        ["field_name"],
        "value must be finite and within the broad plausible rate range",
        threshold_value=maximum,
    )


def _unit_allowed_result(
    results: list[ValidationResultRecord],
    context: dict[str, Any],
    payload: pd.DataFrame,
    prefix: str,
) -> None:
    """Validate rate unit conventions."""

    allowed = {"percent", "bp", "basis_point"}
    bad = payload[~payload["unit"].isin(allowed)]
    _row_records(
        results,
        context,
        bad,
        f"{prefix}.unit_allowed",
        ["field_name"],
        "unit must be percent or basis-point convention",
    )


def _field_role_result(
    results: list[ValidationResultRecord],
    context: dict[str, Any],
    payload: pd.DataFrame,
    field_roles: dict[str, str],
    prefix: str,
) -> None:
    """Validate canonical field_role values."""

    expected = payload["field_name"].map(field_roles)
    bad = payload[expected.notna() & (payload["field_role"] != expected)]
    _row_records(
        results,
        context,
        bad,
        f"{prefix}.field_role_matches",
        ["field_name"],
        "field_role must match the v1 role map",
    )


def _source_caveat_result(
    results: list[ValidationResultRecord],
    context: dict[str, Any],
    payload: pd.DataFrame,
    prefix: str,
) -> None:
    """Require source caveats and mark wrapper-backed rows with caveat status."""

    missing = payload[
        payload["source_caveat"].isna()
        | (payload["source_caveat"].astype(str).str.strip() == "")
    ]
    _row_records(
        results,
        context,
        missing,
        f"{prefix}.source_caveat_present",
        ["field_name"],
        "source_caveat is required",
    )
    if missing.empty:
        results.append(
            _record(
                context,
                f"{prefix}.source_caveat_present",
                "dataset",
                ValidationStatus.PASS_WITH_CAVEAT,
                metric_value=float(len(payload)),
                details={"message": "wrapper source caveat recorded"},
            )
        )


def _record(
    context: dict[str, Any],
    check_name: str,
    level: str,
    status: ValidationStatus,
    subject_key: str | None = None,
    metric_value: float | int | None = None,
    threshold_value: float | int | None = None,
    details: dict[str, Any] | None = None,
) -> ValidationResultRecord:
    """Build one validation result with service-filled validation id."""

    return ValidationResultRecord(
        validation_id=int(context.get("validation_id", 0) or 0),
        run_id=int(context.get("run_id", 0) or 0),
        raw_batch_id=context.get("raw_batch_id"),
        dataset_name=str(context.get("dataset_name", "")),
        check_name=check_name,
        check_level=level,
        status=status,
        subject_key=subject_key,
        metric_value=float(metric_value) if metric_value is not None else None,
        threshold_value=float(threshold_value) if threshold_value is not None else None,
        details_json=json.dumps(details or {}, default=str, ensure_ascii=False),
        created_at=_utc_now(),
    )


def _subject_key(row: pd.Series, columns: list[str]) -> str:
    """Build a stable subject key from row values."""

    values = []
    for column in columns:
        value = row.get(column)
        if isinstance(value, (datetime, date)):
            values.append(value.isoformat())
        else:
            values.append("" if pd.isna(value) else str(value))
    return "|".join(values)


def _sample_keys(frame: pd.DataFrame, columns: list[str]) -> list[str]:
    """Return a small list of subject keys for diagnostics."""

    if frame.empty:
        return []
    return [_subject_key(row, columns) for _, row in frame.head(10).iterrows()]


def _instrument_lookup(context: dict[str, Any]) -> pd.DataFrame | None:
    """Return canonical instruments from context or DuckDB."""

    if isinstance(context.get("canonical_instruments"), pd.DataFrame):
        return context["canonical_instruments"]
    conn = context.get("conn")
    if conn is None:
        return None
    return conn.execute(
        "SELECT instrument_id, instrument_type, exchange FROM canonical_instruments"
    ).fetchdf()


def _open_day_lookup(context: dict[str, Any]) -> pd.DataFrame | None:
    """Return open trading days from context or DuckDB."""

    if isinstance(context.get("canonical_trading_calendar"), pd.DataFrame):
        frame = context["canonical_trading_calendar"]
    else:
        conn = context.get("conn")
        if conn is None:
            return None
        frame = conn.execute(
            "SELECT exchange, trade_date FROM canonical_trading_calendar WHERE is_open = TRUE"
        ).fetchdf()
    if frame.empty:
        return frame
    frame = frame.copy()
    frame["trade_date"] = pd.to_datetime(frame["trade_date"], errors="coerce").dt.date
    return frame


def _open_day_pretrade_sequence(
    results: list[ValidationResultRecord],
    context: dict[str, Any],
    payload: pd.DataFrame,
) -> None:
    """Validate that open-day pretrade_date points to the prior open day."""

    open_days = payload[payload["is_open"].eq(True)].copy()
    if open_days.empty:
        results.append(
            _record(
                context,
                "calendar.open_day_pretrade_sequence",
                "dataset",
                ValidationStatus.PASS,
            )
        )
        return
    open_days["trade_date"] = pd.to_datetime(
        open_days["trade_date"], errors="coerce"
    ).dt.date
    open_days["pretrade_date"] = pd.to_datetime(
        open_days["pretrade_date"], errors="coerce"
    ).dt.date
    open_days = open_days.sort_values(["exchange", "trade_date"])
    open_days["_expected_pretrade_date"] = open_days.groupby("exchange")[
        "trade_date"
    ].shift(1)
    expected_pretrade = open_days["_expected_pretrade_date"]
    bad_sequence = open_days[
        expected_pretrade.notna() & (open_days["pretrade_date"] != expected_pretrade)
    ]
    _row_records(
        results,
        context,
        bad_sequence,
        "calendar.open_day_pretrade_sequence",
        ["exchange", "trade_date"],
        "pretrade_date must match the previous open trade_date",
    )


def _calendar_date_continuity(
    results: list[ValidationResultRecord],
    context: dict[str, Any],
    payload: pd.DataFrame,
) -> None:
    """Validate that each exchange has a continuous daily date spine."""

    frame = payload.dropna(subset=["exchange", "trade_date"]).copy()
    frame["trade_date"] = pd.to_datetime(frame["trade_date"], errors="coerce").dt.date
    missing: list[str] = []
    for exchange, exchange_frame in frame.groupby("exchange"):
        dates = set(exchange_frame["trade_date"].dropna().tolist())
        if not dates:
            continue
        current = min(dates)
        end = max(dates)
        while current <= end:
            if current not in dates:
                missing.append(f"{exchange}|{current.isoformat()}")
            current = current + timedelta(days=1)
    results.append(
        _record(
            context,
            "calendar.date_continuity",
            "dataset",
            ValidationStatus.FAIL if missing else ValidationStatus.PASS,
            metric_value=len(missing),
            threshold_value=0,
            details={"missing_dates": missing[:50]},
        )
    )


def _market_daily_change_consistency(
    results: list[ValidationResultRecord],
    context: dict[str, Any],
    payload: pd.DataFrame,
) -> None:
    """Validate change against close - pre_close."""

    required = {"close", "pre_close", "change"}
    if not required.issubset(payload.columns):
        results.append(
            _record(
                context,
                "market_daily.change_consistency",
                "contract",
                ValidationStatus.FAIL,
                details={"missing_columns": sorted(required - set(payload.columns))},
            )
        )
        return
    tolerance = float(context.get("change_consistency_tolerance", 1e-6))
    comparable = payload.dropna(subset=["close", "pre_close", "change"]).copy()
    expected = comparable["close"] - comparable["pre_close"]
    bad = comparable[(comparable["change"] - expected).abs() > tolerance]
    _row_records(
        results,
        context,
        bad,
        "market_daily.change_consistency",
        ["instrument_id", "trade_date"],
        "change must equal close - pre_close",
        status=ValidationStatus.WARNING,
        threshold_value=tolerance,
    )


def _market_daily_pct_chg_consistency(
    results: list[ValidationResultRecord],
    context: dict[str, Any],
    payload: pd.DataFrame,
) -> None:
    """Validate pct_chg against price movement."""

    required = {"close", "pre_close", "pct_chg"}
    if not required.issubset(payload.columns):
        results.append(
            _record(
                context,
                "market_daily.pct_chg_consistency",
                "contract",
                ValidationStatus.FAIL,
                details={"missing_columns": sorted(required - set(payload.columns))},
            )
        )
        return
    tolerance = float(context.get("pct_chg_consistency_tolerance", 0.01))
    comparable = payload.dropna(subset=["close", "pre_close", "pct_chg"]).copy()
    comparable = comparable[comparable["pre_close"] != 0]
    expected = (
        (comparable["close"] - comparable["pre_close"]) / comparable["pre_close"] * 100
    )
    bad = comparable[(comparable["pct_chg"] - expected).abs() > tolerance]
    _row_records(
        results,
        context,
        bad,
        "market_daily.pct_chg_consistency",
        ["instrument_id", "trade_date"],
        "pct_chg must match percentage movement from pre_close to close",
        status=ValidationStatus.WARNING,
        threshold_value=tolerance,
    )


def _utc_now() -> datetime:
    """Return a naive UTC timestamp for DuckDB compatibility."""

    return datetime.now(UTC).replace(tzinfo=None)
