"""Build ETF all-weather Stage N simulated rebalance plan drafts."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from enum import StrEnum
from hashlib import sha256
import json
import math
from pathlib import Path
from typing import Any

import pandas as pd
from pydantic import (
    AliasChoices,
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)

from tradepilot.etl.etf_aw_universe import (
    ETF_AW_SLEEVE_CODE_BY_ROLE,
    ETF_AW_SLEEVE_CODES,
    ETF_AW_SLEEVE_ROLE_ORDER,
    etf_aw_role_sort_key,
)

REBALANCE_PLAN_DATASET = "derived.etf_aw_rebalance_plan"
REBALANCE_PLAN_SCHEMA_VERSION = "etf_aw_rebalance_plan_v1"
REBALANCE_PLAN_CONTRACT_VERSION = "etf_aw_rebalance_plan_contract_v1"
REBALANCE_PLAN_STATUS = "DRAFT"
DEFAULT_LOT_SIZE = 100
DEFAULT_CASH_BUFFER_RATIO = 0.01
WEIGHT_SUM_TOLERANCE = 1e-6
MARKET_VALUE_MISMATCH_RATIO = 0.01
MARKET_VALUE_MISMATCH_ABS = 1.0


class OrderSide(StrEnum):
    """Allowed simulated order draft sides."""

    BUY = "BUY"
    SELL = "SELL"
    HOLD = "HOLD"


@dataclass(frozen=True)
class PlanDiagnostics:
    """Structured diagnostics for blocked plans and non-blocking warnings."""

    blocking_reasons: list[str]
    warnings: list[str]
    line_diagnostics: list[dict[str, Any]]

    @property
    def blocked(self) -> bool:
        """Return whether any blocking reason exists."""

        return bool(self.blocking_reasons)


class AccountPosition(BaseModel):
    """Single frozen ETF position in an account snapshot."""

    model_config = ConfigDict(extra="forbid")

    symbol: str
    quantity: int
    available_quantity: int
    market_value: float
    cost_basis: Any | None = None

    @field_validator("symbol")
    @classmethod
    def _symbol_not_blank(cls, value: str) -> str:
        """Reject blank ETF symbols."""

        stripped = value.strip()
        if not stripped:
            raise ValueError("symbol must not be blank")
        return stripped

    @model_validator(mode="after")
    def _validate_position(self) -> AccountPosition:
        """Validate non-negative integer quantities and market value."""

        if self.quantity < 0 or self.available_quantity < 0:
            raise ValueError("position quantities must be non-negative")
        if self.available_quantity > self.quantity:
            raise ValueError("available_quantity must not exceed quantity")
        if not math.isfinite(self.market_value) or self.market_value < 0:
            raise ValueError("market_value must be finite and non-negative")
        return self


class AccountSnapshot(BaseModel):
    """Account snapshot used by the simulated rebalance plan."""

    model_config = ConfigDict(extra="forbid")

    account_id: str
    snapshot_at: str
    cash: float
    total_asset: float
    positions: list[AccountPosition]

    @field_validator("account_id", "snapshot_at")
    @classmethod
    def _text_not_blank(cls, value: str) -> str:
        """Reject blank account metadata."""

        stripped = value.strip()
        if not stripped:
            raise ValueError("value must not be blank")
        return stripped

    @model_validator(mode="after")
    def _validate_snapshot(self) -> AccountSnapshot:
        """Validate account cash and total assets."""

        if not math.isfinite(self.cash) or self.cash < 0:
            raise ValueError("cash must be finite and non-negative")
        if not math.isfinite(self.total_asset) or self.total_asset <= 0:
            raise ValueError("total_asset must be finite and positive")
        return self


class PriceSnapshotItem(BaseModel):
    """Latest available price for one frozen ETF."""

    model_config = ConfigDict(extra="forbid")

    symbol: str
    latest_price: float
    source: str

    @field_validator("symbol", "source")
    @classmethod
    def _text_not_blank(cls, value: str) -> str:
        """Reject blank price metadata."""

        stripped = value.strip()
        if not stripped:
            raise ValueError("value must not be blank")
        return stripped

    @field_validator("latest_price")
    @classmethod
    def _price_positive(cls, value: float) -> float:
        """Require a positive finite latest price."""

        if not math.isfinite(value) or value <= 0:
            raise ValueError("latest_price must be finite and positive")
        return value


class PriceSnapshot(BaseModel):
    """Price snapshot used by the simulated rebalance plan."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    price_as_of: str
    prices: list[PriceSnapshotItem] = Field(
        validation_alias=AliasChoices("prices", "items")
    )

    @field_validator("price_as_of")
    @classmethod
    def _price_as_of_not_blank(cls, value: str) -> str:
        """Reject blank price timestamp."""

        stripped = value.strip()
        if not stripped:
            raise ValueError("price_as_of must not be blank")
        return stripped


def load_account_snapshot(path: Path) -> AccountSnapshot:
    """Load an account snapshot JSON file."""

    return AccountSnapshot.model_validate(json.loads(path.read_text(encoding="utf-8")))


def load_price_snapshot(path: Path) -> PriceSnapshot:
    """Load a price snapshot JSON file."""

    return PriceSnapshot.model_validate(json.loads(path.read_text(encoding="utf-8")))


def build_rebalance_plan(
    *,
    target_weight: pd.DataFrame,
    account: AccountSnapshot,
    prices: PriceSnapshot,
    plan_date: date,
    generated_at: datetime | None = None,
    lot_size: int = DEFAULT_LOT_SIZE,
    cash_buffer_ratio: float = DEFAULT_CASH_BUFFER_RATIO,
) -> tuple[pd.DataFrame, dict[str, Any], PlanDiagnostics]:
    """Return a complete Stage N plan draft or blocking diagnostics."""

    generated = generated_at or datetime.now(timezone.utc)
    blocking_reasons: list[str] = []
    warnings = [
        "same_batch_sell_proceeds_assumed",
        "research_only_strategy",
    ]
    selected_weight, weight_reasons = select_latest_target_weight(target_weight)
    blocking_reasons.extend(weight_reasons)

    position_by_symbol, position_reasons = _positions_by_symbol(account)
    price_by_symbol, price_reasons = _prices_by_symbol(prices)
    blocking_reasons.extend(position_reasons)
    blocking_reasons.extend(price_reasons)

    strategy_version = _first_text(selected_weight, "strategy_version", "unknown")
    target_rebalance_date = _first_date(selected_weight, "rebalance_date")
    plan_id = stable_plan_id(
        account_id=account.account_id,
        plan_date=plan_date,
        strategy_version=strategy_version,
        target_weight_rebalance_date=target_rebalance_date,
    )

    summary: dict[str, Any] = {
        "plan_id": plan_id,
        "plan_date": plan_date.isoformat(),
        "account_id": account.account_id,
        "required_cash_buffer": account.total_asset * cash_buffer_ratio,
        "tradable_equity": account.total_asset
        - (account.total_asset * cash_buffer_ratio),
        "estimated_sell_proceeds": 0.0,
        "estimated_buy_notional": 0.0,
        "cash_after_plan": account.cash,
        "blocking_reasons": _dedupe(blocking_reasons),
        "warnings": _dedupe(warnings),
    }
    line_diagnostics: list[dict[str, Any]] = []
    if blocking_reasons:
        return (
            pd.DataFrame(),
            summary,
            PlanDiagnostics(_dedupe(blocking_reasons), _dedupe(warnings), []),
        )

    rows: list[dict[str, Any]] = []
    estimated_sell_proceeds = 0.0
    estimated_buy_notional = 0.0
    for _, weight_row in selected_weight.sort_values(
        "sleeve_role", key=etf_aw_role_sort_key
    ).iterrows():
        sleeve_role = str(weight_row["sleeve_role"])
        symbol = str(weight_row["sleeve_code"])
        position = position_by_symbol[symbol]
        price = price_by_symbol[symbol]
        target_weight_value = float(weight_row["target_weight"])
        target_notional = summary["tradable_equity"] * target_weight_value
        raw_delta_quantity = (
            target_notional - position.market_value
        ) / price.latest_price
        rounded_quantity = math.floor(abs(raw_delta_quantity) / lot_size) * lot_size
        side = OrderSide.HOLD
        line_warnings: list[str] = []
        line_blocking: list[str] = []
        if rounded_quantity == 0:
            line_warnings.append("below_lot_size")
        elif raw_delta_quantity > 0:
            side = OrderSide.BUY
        elif raw_delta_quantity < 0:
            side = OrderSide.SELL
            if rounded_quantity > position.available_quantity:
                line_blocking.append("insufficient_available_quantity")
        estimated_notional = rounded_quantity * price.latest_price
        if side == OrderSide.BUY:
            estimated_buy_notional += estimated_notional
        if side == OrderSide.SELL and not line_blocking:
            estimated_sell_proceeds += estimated_notional
        repriced_market_value = position.quantity * price.latest_price
        if _market_value_mismatch(position.market_value, repriced_market_value):
            line_warnings.append("market_value_price_mismatch")
        blocking_reasons.extend(line_blocking)
        warnings.extend(line_warnings)
        for reason in line_blocking:
            line_diagnostics.append(
                {
                    "reason": reason,
                    "sleeve_role": sleeve_role,
                    "symbol": symbol,
                    "order_side": side.value,
                    "order_quantity": rounded_quantity,
                    "available_quantity": position.available_quantity,
                    "shortfall_quantity": max(
                        rounded_quantity - position.available_quantity, 0
                    ),
                }
            )
        rows.append(
            {
                "schema_version": REBALANCE_PLAN_SCHEMA_VERSION,
                "contract_version": REBALANCE_PLAN_CONTRACT_VERSION,
                "plan_id": plan_id,
                "plan_date": plan_date,
                "generated_at": generated,
                "account_id": account.account_id,
                "account_snapshot_at": account.snapshot_at,
                "price_as_of": prices.price_as_of,
                "target_weight_rebalance_date": target_rebalance_date,
                "strategy_name": str(weight_row["strategy_name"]),
                "strategy_version": str(weight_row["strategy_version"]),
                "sleeve_role": sleeve_role,
                "symbol": symbol,
                "target_weight": target_weight_value,
                "current_quantity": position.quantity,
                "available_quantity": position.available_quantity,
                "current_market_value": position.market_value,
                "latest_price": price.latest_price,
                "target_notional": target_notional,
                "raw_delta_quantity": raw_delta_quantity,
                "lot_size": lot_size,
                "order_side": side.value,
                "order_quantity": rounded_quantity,
                "estimated_notional": estimated_notional,
                "cash_buffer_ratio": cash_buffer_ratio,
                "plan_status": REBALANCE_PLAN_STATUS,
                "blocking_reasons_json": json.dumps(line_blocking, ensure_ascii=False),
                "warnings_json": json.dumps(line_warnings, ensure_ascii=False),
            }
        )

    required_cash_buffer = account.total_asset * cash_buffer_ratio
    cash_after_plan = account.cash + estimated_sell_proceeds - estimated_buy_notional
    if not blocking_reasons and cash_after_plan < required_cash_buffer:
        blocking_reasons.append("insufficient_cash_buffer")
    blocking_reasons = _dedupe(blocking_reasons)
    warnings = _dedupe(warnings)
    summary.update(
        {
            "estimated_sell_proceeds": estimated_sell_proceeds,
            "estimated_buy_notional": estimated_buy_notional,
            "cash_after_plan": cash_after_plan,
            "blocking_reasons": blocking_reasons,
            "warnings": warnings,
            "line_diagnostics": line_diagnostics,
        }
    )
    if blocking_reasons:
        return (
            pd.DataFrame(),
            summary,
            PlanDiagnostics(blocking_reasons, warnings, line_diagnostics),
        )
    frame = pd.DataFrame(rows)
    frame["blocking_reasons_json"] = json.dumps([], ensure_ascii=False)
    return frame, summary, PlanDiagnostics([], warnings, [])


def select_latest_target_weight(frame: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    """Select and validate the latest complete target weight vector."""

    required = {
        "calendar_name",
        "rebalance_date",
        "strategy_name",
        "strategy_version",
        "sleeve_code",
        "sleeve_role",
        "target_weight",
    }
    if frame.empty or not required.issubset(frame.columns):
        return pd.DataFrame(), ["incomplete_target_weight"]
    normalized = frame.copy()
    normalized["rebalance_date"] = pd.to_datetime(
        normalized["rebalance_date"], errors="coerce"
    ).dt.date
    normalized = normalized.dropna(
        subset=["calendar_name", "rebalance_date", "strategy_name", "strategy_version"]
    )
    if normalized.empty:
        return pd.DataFrame(), ["incomplete_target_weight"]
    key_columns = [
        "calendar_name",
        "rebalance_date",
        "strategy_name",
        "strategy_version",
    ]
    latest_key = (
        normalized[key_columns]
        .drop_duplicates()
        .sort_values(
            ["rebalance_date", "calendar_name", "strategy_name", "strategy_version"]
        )
        .iloc[-1]
    )
    selected = normalized
    for column in key_columns:
        selected = selected[selected[column] == latest_key[column]]
    reasons: list[str] = []
    if len(selected) != len(ETF_AW_SLEEVE_CODES) or set(
        selected["sleeve_role"].astype(str)
    ) != set(ETF_AW_SLEEVE_ROLE_ORDER):
        reasons.append("incomplete_target_weight")
    if (
        selected.duplicated(["sleeve_role"]).sum() > 0
        or selected.duplicated(["sleeve_code"]).sum() > 0
    ):
        reasons.append("invalid_target_weight")
    for role, symbol in ETF_AW_SLEEVE_CODE_BY_ROLE.items():
        rows = selected[selected["sleeve_role"].astype(str) == role]
        if len(rows) != 1 or str(rows.iloc[0]["sleeve_code"]) != symbol:
            reasons.append("missing_symbol_mapping")
            break
    weights = pd.to_numeric(selected["target_weight"], errors="coerce")
    if weights.isna().any() or not all(
        math.isfinite(float(value)) for value in weights
    ):
        reasons.append("invalid_target_weight")
    elif bool((weights < 0).any()):
        reasons.append("invalid_target_weight")
    if (
        not weights.isna().any()
        and abs(float(weights.sum()) - 1.0) > WEIGHT_SUM_TOLERANCE
    ):
        reasons.append("invalid_target_weight_sum")
    return selected, _dedupe(reasons)


def stable_plan_id(
    *,
    account_id: str,
    plan_date: date,
    strategy_version: str,
    target_weight_rebalance_date: date | None,
) -> str:
    """Return the stable Stage N plan id for one business idempotency key."""

    target_date = (
        target_weight_rebalance_date.isoformat()
        if target_weight_rebalance_date
        else "unknown"
    )
    key = "|".join([account_id, plan_date.isoformat(), strategy_version, target_date])
    return "etf_aw_rp_" + sha256(key.encode("utf-8")).hexdigest()[:16]


def plan_to_json_payload(
    *,
    frame: pd.DataFrame,
    summary: dict[str, Any],
    account_snapshot_path: Path,
    price_snapshot_path: Path,
    target_weight_artifact: str,
    diagnostics: PlanDiagnostics,
) -> dict[str, Any]:
    """Return the human-review JSON payload for a successful plan."""

    return {
        "schema_version": REBALANCE_PLAN_SCHEMA_VERSION,
        "contract_version": REBALANCE_PLAN_CONTRACT_VERSION,
        "review_notice": "模拟盘草案，需人工判断，未提交订单",
        "input_paths": {
            "account_snapshot": str(account_snapshot_path),
            "price_snapshot": str(price_snapshot_path),
            "target_weight_artifact": target_weight_artifact,
        },
        "summary": summary,
        "diagnostics": {
            "blocking_reasons": diagnostics.blocking_reasons,
            "warnings": diagnostics.warnings,
            "line_diagnostics": diagnostics.line_diagnostics,
        },
        "rows": _records_for_json(frame),
    }


def plan_to_markdown(payload: dict[str, Any]) -> str:
    """Return a concise Markdown review file for a successful plan."""

    summary = payload["summary"]
    rows = payload["rows"]
    lines = [
        "# ETF 全天候模拟盘调仓计划草案",
        "",
        "模拟盘草案，需人工判断，未提交订单。",
        "",
        "## Summary",
        "",
        f"- plan_id: `{summary['plan_id']}`",
        f"- plan_date: `{summary['plan_date']}`",
        f"- account_id: `{summary['account_id']}`",
        f"- required_cash_buffer: `{summary['required_cash_buffer']:.2f}`",
        f"- estimated_sell_proceeds: `{summary['estimated_sell_proceeds']:.2f}`",
        f"- estimated_buy_notional: `{summary['estimated_buy_notional']:.2f}`",
        f"- cash_after_plan: `{summary['cash_after_plan']:.2f}`",
        f"- warnings: `{', '.join(payload['diagnostics']['warnings'])}`",
    ]
    if payload["diagnostics"]["blocking_reasons"]:
        lines.append(
            f"- blocking_reasons: `{', '.join(payload['diagnostics']['blocking_reasons'])}`"
        )
        lines.extend(["", "## Blocking Reasons", ""])
        for reason in payload["diagnostics"]["blocking_reasons"]:
            lines.append(f"- {reason}")
        if payload["diagnostics"]["line_diagnostics"]:
            lines.extend(["", "## Line Diagnostics", ""])
            for item in payload["diagnostics"]["line_diagnostics"]:
                lines.append(
                    f"- {json.dumps(item, sort_keys=True, ensure_ascii=False)}"
                )
        return "\n".join(lines) + "\n"

    lines.extend(
        [
            "",
            "## Inputs",
            "",
            f"- account_snapshot: `{payload['input_paths']['account_snapshot']}`",
            f"- price_snapshot: `{payload['input_paths']['price_snapshot']}`",
            f"- target_weight_artifact: `{payload['input_paths']['target_weight_artifact']}`",
            "",
            "## Orders",
            "",
            "| sleeve_role | symbol | side | quantity | estimated_notional | warnings |",
            "| --- | --- | --- | ---: | ---: | --- |",
        ]
    )
    for row in rows:
        warnings = ", ".join(row["warnings_json"])
        lines.append(
            "| {sleeve_role} | {symbol} | {order_side} | {order_quantity} | "
            "{estimated_notional:.2f} | {warnings} |".format(**row, warnings=warnings)
        )
    return "\n".join(lines) + "\n"


def _positions_by_symbol(
    account: AccountSnapshot,
) -> tuple[dict[str, AccountPosition], list[str]]:
    reasons: list[str] = []
    positions: dict[str, AccountPosition] = {}
    symbols = [position.symbol for position in account.positions]
    if len(symbols) != len(set(symbols)):
        reasons.append("invalid_account_snapshot")
    for position in account.positions:
        positions[position.symbol] = position
    for symbol in ETF_AW_SLEEVE_CODES:
        if symbol not in positions:
            reasons.append("missing_position")
    return positions, _dedupe(reasons)


def _prices_by_symbol(
    prices: PriceSnapshot,
) -> tuple[dict[str, PriceSnapshotItem], list[str]]:
    reasons: list[str] = []
    by_symbol: dict[str, PriceSnapshotItem] = {}
    symbols = [item.symbol for item in prices.prices]
    if len(symbols) != len(set(symbols)):
        reasons.append("missing_or_invalid_price")
    for item in prices.prices:
        by_symbol[item.symbol] = item
    for symbol in ETF_AW_SLEEVE_CODES:
        if symbol not in by_symbol:
            reasons.append("missing_or_invalid_price")
    return by_symbol, _dedupe(reasons)


def _market_value_mismatch(current: float, repriced: float) -> bool:
    diff = abs(current - repriced)
    return (
        diff > MARKET_VALUE_MISMATCH_ABS
        and diff / max(current, repriced, 1.0) > MARKET_VALUE_MISMATCH_RATIO
    )


def _first_text(frame: pd.DataFrame, column: str, default: str) -> str:
    if frame.empty or column not in frame.columns:
        return default
    value = frame.iloc[0][column]
    return default if pd.isna(value) else str(value)


def _first_date(frame: pd.DataFrame, column: str) -> date | None:
    if frame.empty or column not in frame.columns:
        return None
    value = pd.to_datetime(frame.iloc[0][column], errors="coerce")
    if pd.isna(value):
        return None
    return value.date()


def _records_for_json(frame: pd.DataFrame) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for record in frame.to_dict(orient="records"):
        clean: dict[str, Any] = {}
        for key, value in record.items():
            if isinstance(value, (date, datetime, pd.Timestamp)):
                clean[key] = value.isoformat()
            elif key.endswith("_json") and isinstance(value, str):
                clean[key] = json.loads(value)
            else:
                clean[key] = value
        records.append(clean)
    return records


def _dedupe(values: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        if value not in seen:
            seen.add(value)
            result.append(value)
    return result
