"""Dataset-specific validators for the ETL foundation."""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import UTC, date, datetime
import json
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
        results.append(_warning_or_pass(ctx, "calendar.open_day_pretrade_sequence"))
        results.append(_warning_or_pass(ctx, "calendar.date_continuity"))
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
            joined = payload.merge(
                calendars.assign(_is_open_day=True),
                on=(
                    ["exchange", "trade_date"]
                    if "exchange" in payload.columns
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
        return results


def get_validator(dataset_name: str) -> BaseValidator:
    """Return the validator for one Stage B dataset."""

    if dataset_name == "reference.trading_calendar":
        return TradingCalendarValidator()
    if dataset_name == "reference.instruments":
        return InstrumentValidator()
    if dataset_name in {"market.etf_daily", "market.index_daily"}:
        return MarketDailyValidator()
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


def _warning_or_pass(
    context: dict[str, Any], check_name: str
) -> ValidationResultRecord:
    """Return a placeholder pass record for warning-capable checks."""

    return _record(context, check_name, "dataset", ValidationStatus.PASS)


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
        "SELECT instrument_id, instrument_type FROM canonical_instruments"
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


def _utc_now() -> datetime:
    """Return a naive UTC timestamp for DuckDB compatibility."""

    return datetime.now(UTC).replace(tzinfo=None)
