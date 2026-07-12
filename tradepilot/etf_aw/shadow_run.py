"""Stage O shadow portfolio observation helpers for ETF all-weather."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from enum import StrEnum
import html
import json
import math
from pathlib import Path
from typing import Any

import pandas as pd
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from tradepilot.etf_aw.rebalance_plan import AccountSnapshot, OrderSide
from tradepilot.etl.etf_aw_universe import (
    ETF_AW_SLEEVE_CODE_BY_ROLE,
    ETF_AW_SLEEVE_CODES,
    ETF_AW_SLEEVE_ROLE_ORDER,
    etf_aw_role_sort_key,
)
from tradepilot.etl.models import StorageZone
from tradepilot.etl.storage import build_zone_path, write_dataset_parquet

SHADOW_CONTRACT_VERSION = "etf_aw_shadow_run_contract_v1"
SHADOW_ACCOUNT_SEED_DATASET = "derived.etf_aw_shadow_account_seed"
PAPER_DECISION_DATASET = "derived.etf_aw_paper_decision"
PAPER_FILL_DATASET = "derived.etf_aw_paper_fill"
SHADOW_OBSERVATION_DATASET = "derived.etf_aw_shadow_observation"
SHADOW_ACCOUNT_SEED_SCHEMA_VERSION = "etf_aw_shadow_account_seed_v1"
PAPER_DECISION_SCHEMA_VERSION = "etf_aw_paper_decision_v1"
PAPER_FILL_SCHEMA_VERSION = "etf_aw_paper_fill_v1"
SHADOW_OBSERVATION_SCHEMA_VERSION = "etf_aw_shadow_observation_v1"
SHADOW_PERFORMANCE_REPORT_VERSION = "etf_aw_shadow_performance_report_v1"
POST_MORTEM_VERSION = "etf_aw_shadow_post_mortem_v1"
WEIGHT_TOLERANCE = 1e-6


class PaperDecision(StrEnum):
    """Allowed manual Stage O plan decisions."""

    CONFIRMED = "CONFIRMED"
    CANCELLED = "CANCELLED"


class DerivedPlanStatus(StrEnum):
    """Derived read-only Stage O plan status."""

    DRAFT = "DRAFT"
    CANCELLED = "CANCELLED"
    CONFIRMED = "CONFIRMED"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"


class ShadowRunError(Exception):
    """Structured Stage O blocking error."""

    def __init__(self, reasons: list[str], diagnostics: dict[str, Any] | None = None):
        super().__init__("; ".join(reasons))
        self.reasons = _dedupe(reasons)
        self.diagnostics = diagnostics or {}


class DecisionInput(BaseModel):
    """Manual decision input JSON for a Stage N plan."""

    model_config = ConfigDict(extra="forbid")

    plan_id: str
    decision: PaperDecision
    decided_at: datetime
    operator: str
    note: str = ""

    @field_validator("plan_id", "operator")
    @classmethod
    def _text_not_blank(cls, value: str) -> str:
        stripped = value.strip()
        if not stripped:
            raise ValueError("value must not be blank")
        return stripped


class FillInput(BaseModel):
    """Manual paper fill input JSON."""

    model_config = ConfigDict(extra="forbid")

    fill_id: str
    plan_id: str
    symbol: str
    order_side: OrderSide
    fill_at: datetime
    fill_quantity: int
    fill_price: float
    source: str
    note: str = ""

    @field_validator("fill_id", "plan_id", "symbol", "source")
    @classmethod
    def _text_not_blank(cls, value: str) -> str:
        stripped = value.strip()
        if not stripped:
            raise ValueError("value must not be blank")
        return stripped

    @model_validator(mode="after")
    def _validate_fill(self) -> FillInput:
        if self.order_side == OrderSide.HOLD:
            raise ValueError("fill order_side must be BUY or SELL")
        if self.fill_quantity <= 0:
            raise ValueError("fill_quantity must be positive")
        if not math.isfinite(self.fill_price) or self.fill_price <= 0:
            raise ValueError("fill_price must be finite and positive")
        return self


class ClosePriceItem(BaseModel):
    """One close price item for a shadow observation."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    symbol: str
    close_price: float = Field(validation_alias="close_price")
    price_trade_date: date | None = None

    @field_validator("symbol")
    @classmethod
    def _symbol_not_blank(cls, value: str) -> str:
        stripped = value.strip()
        if not stripped:
            raise ValueError("symbol must not be blank")
        return stripped

    @field_validator("close_price")
    @classmethod
    def _close_price_positive(cls, value: float) -> float:
        if not math.isfinite(value) or value <= 0:
            raise ValueError("close_price must be finite and positive")
        return value


class PriceSnapshotInput(BaseModel):
    """Close price snapshot input for one observation date."""

    model_config = ConfigDict(extra="forbid")

    price_as_of: datetime
    prices: list[ClosePriceItem]


class BaselineObservationInput(BaseModel):
    """Optional baseline observation for one shadow observation date."""

    model_config = ConfigDict(extra="forbid")

    observation_date: date
    strategy_name: str
    strategy_version: str
    baseline_daily_return: float | None = None
    baseline_net_value: float | None = None
    source_artifact: str = ""


@dataclass(frozen=True)
class AppliedFillResult:
    """Result of applying paper fills to a shadow account state."""

    quantities: dict[str, int]
    cash: float
    applied_fill_ids: list[str]


def load_decision_input(path: Path) -> DecisionInput:
    """Load a manual decision JSON file."""

    return DecisionInput.model_validate(json.loads(path.read_text(encoding="utf-8")))


def load_fill_input(path: Path) -> FillInput:
    """Load a paper fill JSON file."""

    return FillInput.model_validate(json.loads(path.read_text(encoding="utf-8")))


def load_price_snapshot_input(path: Path) -> PriceSnapshotInput:
    """Load a close price snapshot JSON file."""

    payload = json.loads(path.read_text(encoding="utf-8"))
    items = payload.get("prices", payload.get("items"))
    if items is None:
        payload["prices"] = []
    elif "prices" not in payload:
        payload["prices"] = items
        payload.pop("items", None)
    for item in payload["prices"]:
        if "close_price" not in item and "latest_price" in item:
            item["close_price"] = item.pop("latest_price")
    return PriceSnapshotInput.model_validate(payload)


def load_baseline_observation_input(path: Path) -> BaselineObservationInput:
    """Load an optional baseline observation JSON file."""

    return BaselineObservationInput.model_validate(
        json.loads(path.read_text(encoding="utf-8"))
    )


def read_shadow_dataset(lakehouse_root: Path, dataset_name: str) -> pd.DataFrame:
    """Read every parquet file for one Stage O dataset."""

    root = build_zone_path(dataset_name, StorageZone.DERIVED, lakehouse_root)
    if not root.exists():
        return pd.DataFrame()
    frames = [pd.read_parquet(path) for path in sorted(root.rglob("*.parquet"))]
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def append_dataset(
    *,
    lakehouse_root: Path,
    dataset_name: str,
    frame: pd.DataFrame,
    partition_parts: list[tuple[str, str | int]],
) -> str:
    """Append rows to a canonical partition by rewriting that partition."""

    existing = _read_partition(lakehouse_root, dataset_name, partition_parts)
    if not existing.empty and set(existing.columns) != set(frame.columns):
        raise ShadowRunError(
            [f"{dataset_name}.schema_mismatch"],
            {
                "existing_columns": sorted(existing.columns),
                "new_columns": sorted(frame.columns),
            },
        )
    output = (
        frame if existing.empty else pd.concat([existing, frame], ignore_index=True)
    )
    result = write_dataset_parquet(
        output,
        dataset_name,
        StorageZone.DERIVED,
        partition_parts,
        lakehouse_root=lakehouse_root,
    )
    return result.relative_path


def build_shadow_seed_rows(
    *,
    plan: pd.DataFrame,
    account: AccountSnapshot,
    account_snapshot_path: Path,
    seed_date: date,
    recorded_at: datetime | None = None,
) -> pd.DataFrame:
    """Build the immutable five-row shadow account seed artifact."""

    if plan.empty:
        raise ShadowRunError(["missing_plan"])
    if account.account_id != str(plan.iloc[0]["account_id"]):
        raise ShadowRunError(
            ["invalid_account_snapshot"], {"reason": "account_id_mismatch"}
        )
    position_by_symbol = {item.symbol: item for item in account.positions}
    missing = [
        symbol for symbol in ETF_AW_SLEEVE_CODES if symbol not in position_by_symbol
    ]
    if missing:
        raise ShadowRunError(
            ["missing_or_duplicate_seed"], {"missing_symbols": missing}
        )
    recorded = recorded_at or datetime.now(timezone.utc)
    total_market_value = sum(
        position_by_symbol[symbol].market_value for symbol in ETF_AW_SLEEVE_CODES
    )
    if abs(account.cash + total_market_value - account.total_asset) > 0.01:
        raise ShadowRunError(
            ["missing_or_duplicate_seed"], {"reason": "asset_sum_mismatch"}
        )
    rows = []
    for role in ETF_AW_SLEEVE_ROLE_ORDER:
        symbol = ETF_AW_SLEEVE_CODE_BY_ROLE[role]
        position = position_by_symbol[symbol]
        rows.append(
            {
                "schema_version": SHADOW_ACCOUNT_SEED_SCHEMA_VERSION,
                "contract_version": SHADOW_CONTRACT_VERSION,
                "account_id": account.account_id,
                "seed_at": pd.Timestamp(account.snapshot_at),
                "seed_date": seed_date,
                "source_plan_id": str(plan.iloc[0]["plan_id"]),
                "cash": float(account.cash),
                "total_asset": float(account.total_asset),
                "sleeve_role": role,
                "symbol": symbol,
                "quantity": int(position.quantity),
                "market_value": float(position.market_value),
                "source_snapshot_path": str(account_snapshot_path),
                "recorded_at": recorded,
            }
        )
    return pd.DataFrame(rows)


def build_decision_row(
    *, plan: pd.DataFrame, decision: DecisionInput, recorded_at: datetime | None = None
) -> pd.DataFrame:
    """Build one paper decision artifact row."""

    if plan.empty:
        raise ShadowRunError(["missing_plan"])
    first = plan.iloc[0]
    return pd.DataFrame(
        [
            {
                "schema_version": PAPER_DECISION_SCHEMA_VERSION,
                "contract_version": SHADOW_CONTRACT_VERSION,
                "plan_id": decision.plan_id,
                "account_id": str(first["account_id"]),
                "strategy_name": str(first["strategy_name"]),
                "strategy_version": str(first["strategy_version"]),
                "decision": decision.decision.value,
                "decided_at": decision.decided_at,
                "operator": decision.operator,
                "note": decision.note,
                "recorded_at": recorded_at or datetime.now(timezone.utc),
            }
        ]
    )


def build_fill_row(
    *,
    plan: pd.DataFrame,
    decisions: pd.DataFrame,
    existing_fills: pd.DataFrame,
    fill: FillInput,
    recorded_at: datetime | None = None,
) -> pd.DataFrame:
    """Build and validate one paper fill artifact row."""

    if plan.empty:
        raise ShadowRunError(["missing_plan"])
    if _contains(existing_fills, "fill_id", fill.fill_id):
        raise ShadowRunError(["duplicate_fill"])
    decision = latest_decision_for_plan(decisions, fill.plan_id)
    if decision is None or decision.get("decision") != PaperDecision.CONFIRMED.value:
        raise ShadowRunError(["fill_before_confirmation"])
    candidates = plan[
        plan["symbol"].astype(str).eq(fill.symbol)
        & plan["order_side"].astype(str).eq(fill.order_side.value)
    ]
    if len(candidates) != 1:
        raise ShadowRunError(["invalid_fill"])
    plan_row = candidates.iloc[0]
    planned_quantity = int(plan_row["order_quantity"])
    prior_quantity = _filled_quantity(
        existing_fills, fill.plan_id, fill.symbol, fill.order_side.value
    )
    if prior_quantity + fill.fill_quantity > planned_quantity:
        raise ShadowRunError(["fill_exceeds_plan"])
    return pd.DataFrame(
        [
            {
                "schema_version": PAPER_FILL_SCHEMA_VERSION,
                "contract_version": SHADOW_CONTRACT_VERSION,
                "fill_id": fill.fill_id,
                "plan_id": fill.plan_id,
                "account_id": str(plan_row["account_id"]),
                "strategy_name": str(plan_row["strategy_name"]),
                "strategy_version": str(plan_row["strategy_version"]),
                "sleeve_role": str(plan_row["sleeve_role"]),
                "symbol": fill.symbol,
                "order_side": fill.order_side.value,
                "planned_quantity": planned_quantity,
                "fill_at": fill.fill_at,
                "fill_quantity": int(fill.fill_quantity),
                "fill_price": float(fill.fill_price),
                "fill_notional": float(fill.fill_quantity * fill.fill_price),
                "source": fill.source,
                "note": fill.note,
                "recorded_at": recorded_at or datetime.now(timezone.utc),
            }
        ]
    )


def build_shadow_observation_rows(
    *,
    account_id: str,
    observation_date: date,
    price_snapshot: PriceSnapshotInput,
    baseline: BaselineObservationInput | None,
    note: str,
    seed: pd.DataFrame,
    observations: pd.DataFrame,
    decisions: pd.DataFrame,
    fills: pd.DataFrame,
    plans: pd.DataFrame,
    generated_at: datetime | None = None,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Build one five-row shadow observation artifact and review payload."""

    generated = generated_at or datetime.now(timezone.utc)
    seed = _seed_for_account(seed, account_id)
    prior = _latest_observation_before(observations, account_id, observation_date)
    if _has_observation(observations, account_id, observation_date):
        raise ShadowRunError(["duplicate_observation"])
    if prior is None and observation_date <= _date_value(seed.iloc[0]["seed_date"]):
        raise ShadowRunError(["observation_date_regression"])
    if prior is not None and observation_date <= _date_value(
        prior.iloc[0]["observation_date"]
    ):
        raise ShadowRunError(["observation_date_regression"])
    price_by_symbol = validate_close_prices(price_snapshot, observation_date)
    previous_generated_at = (
        pd.Timestamp(prior.iloc[0]["generated_at"]).to_pydatetime()
        if prior is not None
        else pd.Timestamp(seed.iloc[0]["seed_at"]).to_pydatetime()
    )
    quantities = _state_quantities(seed if prior is None else prior)
    cash = float((seed if prior is None else prior).iloc[0]["cash"])
    applicable_fills = _applicable_fills(
        fills,
        account_id=account_id,
        after=previous_generated_at,
        at_or_before=price_snapshot.price_as_of,
    )
    applied = apply_fills(
        quantities=quantities,
        cash=cash,
        fills=applicable_fills,
    )
    active_plan = active_confirmed_plan(decisions, plans, account_id, generated)
    target_by_symbol = _target_weights(active_plan)
    market_values = {
        symbol: applied.quantities[symbol] * price_by_symbol[symbol]
        for symbol in ETF_AW_SLEEVE_CODES
    }
    total_asset = applied.cash + sum(market_values.values())
    if total_asset <= 0:
        raise ShadowRunError(["invalid_observation"])
    previous_total_asset = (
        float(prior.iloc[0]["total_asset"])
        if prior is not None
        else float(seed.iloc[0]["total_asset"])
    )
    seed_total_asset = float(seed.iloc[0]["total_asset"])
    daily_return = total_asset / previous_total_asset - 1.0
    cumulative_return = total_asset / seed_total_asset - 1.0
    baseline_daily_return = (
        baseline.baseline_daily_return if baseline is not None else None
    )
    baseline_cumulative_return = _next_baseline_cumulative(
        observations,
        account_id=account_id,
        observation_date=observation_date,
        baseline=baseline,
    )
    relative_cumulative_return = (
        None
        if baseline_cumulative_return is None
        else cumulative_return - baseline_cumulative_return
    )
    warnings = ["zero_fee_assumption", "research_only_strategy"]
    if baseline is None:
        warnings.append("missing_baseline_observation")
    if not applicable_fills.empty:
        warnings.append("intraday_fill_attribution_unavailable")
    rows = []
    for role in ETF_AW_SLEEVE_ROLE_ORDER:
        symbol = ETF_AW_SLEEVE_CODE_BY_ROLE[role]
        actual_weight = market_values[symbol] / total_asset
        target_weight = target_by_symbol.get(symbol)
        weight_drift = None if target_weight is None else actual_weight - target_weight
        if weight_drift is not None and abs(weight_drift) > 0.02:
            warnings.append("large_weight_drift")
        rows.append(
            {
                "schema_version": SHADOW_OBSERVATION_SCHEMA_VERSION,
                "contract_version": SHADOW_CONTRACT_VERSION,
                "account_id": account_id,
                "observation_date": observation_date,
                "generated_at": generated,
                "target_plan_id": (
                    None if active_plan.empty else str(active_plan.iloc[0]["plan_id"])
                ),
                "strategy_name": _active_strategy_name(active_plan, seed),
                "strategy_version": _active_strategy_version(active_plan, seed),
                "sleeve_role": role,
                "symbol": symbol,
                "close_price": price_by_symbol[symbol],
                "quantity": int(applied.quantities[symbol]),
                "market_value": market_values[symbol],
                "actual_weight": actual_weight,
                "target_weight": target_weight,
                "weight_drift": weight_drift,
                "cash": applied.cash,
                "total_asset": total_asset,
                "daily_return": daily_return,
                "cumulative_return": cumulative_return,
                "baseline_daily_return": baseline_daily_return,
                "baseline_cumulative_return": baseline_cumulative_return,
                "relative_cumulative_return": relative_cumulative_return,
                "derived_plan_status": derive_plan_status(active_plan, fills),
                "warnings_json": json.dumps(_dedupe(warnings), ensure_ascii=False),
                "note": note,
                "review_metadata_json": json.dumps(
                    {
                        "price_as_of": price_snapshot.price_as_of.isoformat(),
                        "previous_observation_date": (
                            None
                            if prior is None
                            else _date_value(
                                prior.iloc[0]["observation_date"]
                            ).isoformat()
                        ),
                        "applied_fill_ids": applied.applied_fill_ids,
                        "baseline_source_artifact": (
                            None if baseline is None else baseline.source_artifact
                        ),
                    },
                    sort_keys=True,
                    ensure_ascii=False,
                ),
            }
        )
    frame = pd.DataFrame(rows)
    _validate_weight_sum(frame)
    review = {
        "schema_version": SHADOW_OBSERVATION_SCHEMA_VERSION,
        "account_id": account_id,
        "observation_date": observation_date.isoformat(),
        "generated_at": generated.isoformat(),
        "target_plan_id": rows[0]["target_plan_id"],
        "total_asset": total_asset,
        "daily_return": daily_return,
        "cumulative_return": cumulative_return,
        "applied_fill_ids": applied.applied_fill_ids,
        "warnings": _dedupe(warnings),
    }
    return frame, review


def validate_close_prices(
    price_snapshot: PriceSnapshotInput, observation_date: date
) -> dict[str, float]:
    """Return validated close prices keyed by frozen ETF symbol."""

    symbols = [item.symbol for item in price_snapshot.prices]
    if len(symbols) != len(set(symbols)):
        raise ShadowRunError(
            ["missing_or_invalid_close_price"], {"reason": "duplicate_symbol"}
        )
    by_symbol = {item.symbol: item for item in price_snapshot.prices}
    missing = [symbol for symbol in ETF_AW_SLEEVE_CODES if symbol not in by_symbol]
    if missing:
        raise ShadowRunError(
            ["missing_or_invalid_close_price"], {"missing_symbols": missing}
        )
    result = {}
    for symbol in ETF_AW_SLEEVE_CODES:
        item = by_symbol[symbol]
        if (
            item.price_trade_date is not None
            and item.price_trade_date != observation_date
        ):
            raise ShadowRunError(["missing_or_invalid_close_price"], {"symbol": symbol})
        result[symbol] = float(item.close_price)
    return result


def apply_fills(
    *, quantities: dict[str, int], cash: float, fills: pd.DataFrame
) -> AppliedFillResult:
    """Apply sorted paper fills to quantities and cash."""

    result_quantities = dict(quantities)
    result_cash = float(cash)
    applied_ids: list[str] = []
    if fills.empty:
        return AppliedFillResult(result_quantities, result_cash, applied_ids)
    rows = fills.copy()
    rows["fill_at"] = pd.to_datetime(rows["fill_at"], errors="coerce")
    rows = rows.sort_values(["fill_at", "fill_id"])
    for _, row in rows.iterrows():
        symbol = str(row["symbol"])
        quantity = int(row["fill_quantity"])
        notional = float(row["fill_notional"])
        if str(row["order_side"]) == OrderSide.BUY.value:
            result_cash -= notional
            if result_cash < -1e-8:
                raise ShadowRunError(
                    ["insufficient_shadow_cash"], {"fill_id": str(row["fill_id"])}
                )
            result_quantities[symbol] = result_quantities.get(symbol, 0) + quantity
        elif str(row["order_side"]) == OrderSide.SELL.value:
            result_quantities[symbol] = result_quantities.get(symbol, 0) - quantity
            if result_quantities[symbol] < 0:
                raise ShadowRunError(
                    ["insufficient_shadow_position"], {"fill_id": str(row["fill_id"])}
                )
            result_cash += notional
        else:
            raise ShadowRunError(["invalid_fill"])
        applied_ids.append(str(row["fill_id"]))
    return AppliedFillResult(result_quantities, result_cash, applied_ids)


def derive_plan_status(plan: pd.DataFrame, fills: pd.DataFrame) -> str | None:
    """Derive the read-only Stage O status for one plan."""

    if plan.empty:
        return None
    plan_id = str(plan.iloc[0]["plan_id"])
    relevant = (
        fills[fills["plan_id"].astype(str).eq(plan_id)]
        if not fills.empty
        else pd.DataFrame()
    )
    actionable = plan[
        plan["order_side"].astype(str).isin([OrderSide.BUY.value, OrderSide.SELL.value])
    ]
    if relevant.empty:
        return DerivedPlanStatus.CONFIRMED.value
    for _, row in actionable.iterrows():
        filled = _filled_quantity(
            relevant, plan_id, str(row["symbol"]), str(row["order_side"])
        )
        if filled < int(row["order_quantity"]):
            return DerivedPlanStatus.PARTIALLY_FILLED.value
    return DerivedPlanStatus.FILLED.value


def latest_decision_for_plan(
    decisions: pd.DataFrame, plan_id: str
) -> dict[str, Any] | None:
    """Return the single decision row for a plan if present."""

    if decisions.empty or "plan_id" not in decisions.columns:
        return None
    rows = decisions[decisions["plan_id"].astype(str).eq(plan_id)]
    if rows.empty:
        return None
    rows = rows.sort_values("decided_at")
    return rows.iloc[-1].to_dict()


def active_confirmed_plan(
    decisions: pd.DataFrame,
    plans: pd.DataFrame,
    account_id: str,
    as_of: datetime,
) -> pd.DataFrame:
    """Return the latest confirmed plan active at an observation time."""

    if decisions.empty or plans.empty:
        return pd.DataFrame()
    rows = decisions[
        decisions["account_id"].astype(str).eq(account_id)
        & decisions["decision"].astype(str).eq(PaperDecision.CONFIRMED.value)
    ].copy()
    if rows.empty:
        return pd.DataFrame()
    rows["decided_at"] = pd.to_datetime(rows["decided_at"], errors="coerce")
    rows = rows[
        rows["decided_at"].notna() & (rows["decided_at"] <= pd.Timestamp(as_of))
    ]
    if rows.empty:
        return pd.DataFrame()
    plan_id = str(rows.sort_values(["decided_at", "plan_id"]).iloc[-1]["plan_id"])
    return plans[plans["plan_id"].astype(str).eq(plan_id)].copy()


def build_post_mortem(
    plan_id: str,
    plans: pd.DataFrame,
    decisions: pd.DataFrame,
    fills: pd.DataFrame,
    observations: pd.DataFrame,
) -> dict[str, Any]:
    """Build a read-only post-mortem payload for one plan."""

    plan = (
        plans[plans["plan_id"].astype(str).eq(plan_id)]
        if not plans.empty
        else pd.DataFrame()
    )
    if plan.empty:
        raise ShadowRunError(["missing_plan"])
    plan_fills = (
        fills[fills["plan_id"].astype(str).eq(plan_id)]
        if not fills.empty
        else pd.DataFrame()
    )
    rows = []
    for _, row in plan.sort_values("sleeve_role", key=etf_aw_role_sort_key).iterrows():
        if str(row["order_side"]) == OrderSide.HOLD.value:
            continue
        symbol = str(row["symbol"])
        side = str(row["order_side"])
        line_fills = plan_fills[
            plan_fills["symbol"].astype(str).eq(symbol)
            & plan_fills["order_side"].astype(str).eq(side)
        ]
        quantity = int(line_fills["fill_quantity"].sum()) if not line_fills.empty else 0
        notional = (
            float(line_fills["fill_notional"].sum()) if not line_fills.empty else 0.0
        )
        vwap = None if quantity == 0 else notional / quantity
        latest_price = float(row["latest_price"])
        side_sign = 1.0 if side == OrderSide.BUY.value else -1.0
        rows.append(
            {
                "sleeve_role": str(row["sleeve_role"]),
                "symbol": symbol,
                "order_side": side,
                "planned_quantity": int(row["order_quantity"]),
                "filled_quantity": quantity,
                "fill_ratio": (
                    None
                    if int(row["order_quantity"]) == 0
                    else quantity / int(row["order_quantity"])
                ),
                "volume_weighted_fill_price": vwap,
                "planned_latest_price": latest_price,
                "price_deviation": (
                    None if vwap is None else (vwap / latest_price - 1.0) * side_sign
                ),
            }
        )
    account_id = str(plan.iloc[0]["account_id"])
    obs = (
        observations[observations["account_id"].astype(str).eq(account_id)]
        if not observations.empty
        else pd.DataFrame()
    )
    dates = (
        sorted({_date_value(value) for value in obs["observation_date"].tolist()})
        if not obs.empty
        else []
    )
    return {
        "report_version": POST_MORTEM_VERSION,
        "plan_id": plan_id,
        "account_id": account_id,
        "decision": _jsonable_record(latest_decision_for_plan(decisions, plan_id)),
        "derived_status": derive_plan_status(plan, fills),
        "fill_quality": rows,
        "observation_range": {
            "start_date": None if not dates else dates[0].isoformat(),
            "end_date": None if not dates else dates[-1].isoformat(),
            "observation_dates": [value.isoformat() for value in dates],
        },
    }


def post_mortem_markdown(payload: dict[str, Any]) -> str:
    """Render a concise Markdown post-mortem."""

    lines = [
        "# ETF 全天候模拟盘复盘",
        "",
        f"- plan_id: `{payload['plan_id']}`",
        f"- account_id: `{payload['account_id']}`",
        f"- derived_status: `{payload['derived_status']}`",
        "",
        "## Fill Quality",
        "",
        "| sleeve_role | symbol | side | planned | filled | fill_ratio | vwap | price_deviation |",
        "| --- | --- | --- | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in payload["fill_quality"]:
        lines.append(
            "| {sleeve_role} | {symbol} | {order_side} | {planned_quantity} | "
            "{filled_quantity} | {fill_ratio} | {volume_weighted_fill_price} | "
            "{price_deviation} |".format(**row)
        )
    return "\n".join(lines) + "\n"


def build_performance_report(
    *,
    account_id: str,
    start: date,
    end: date,
    seed: pd.DataFrame,
    observations: pd.DataFrame,
    fills: pd.DataFrame,
    plans: pd.DataFrame,
) -> dict[str, Any]:
    """Build a read-only shadow performance report payload."""

    account_seed = _seed_for_account(seed, account_id)
    obs = observations[observations["account_id"].astype(str).eq(account_id)].copy()
    if obs.empty:
        raise ShadowRunError(["missing_observation"])
    obs["observation_date"] = pd.to_datetime(
        obs["observation_date"], errors="coerce"
    ).dt.date
    obs = obs[
        (obs["observation_date"] >= start) & (obs["observation_date"] <= end)
    ].copy()
    if obs.empty:
        raise ShadowRunError(["missing_observation"])
    daily = _daily_observation_frame(obs)
    pre_start_asset = _pre_start_asset(account_seed, observations, account_id, start)
    returns = daily["total_asset"].astype(float).pct_change().tolist()
    if daily.iloc[0]["observation_date"] == _first_observation_date(
        observations, account_id
    ):
        returns[0] = (
            daily.iloc[0]["total_asset"] / float(account_seed.iloc[0]["total_asset"])
            - 1.0
        )
    else:
        returns[0] = daily.iloc[0]["total_asset"] / pre_start_asset - 1.0
    metrics = _performance_metrics(
        returns=returns,
        assets=daily["total_asset"].astype(float).tolist(),
        pre_start_asset=pre_start_asset,
        ending_asset=float(daily.iloc[-1]["total_asset"]),
    )
    warnings = _warning_counts(obs)
    if len(returns) < 60:
        warnings["short_observation_window"] = (
            warnings.get("short_observation_window", 0) + 1
        )
    contribution = _sleeve_contribution(account_seed, obs, fills)
    payload = {
        "report_version": SHADOW_PERFORMANCE_REPORT_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "account_id": account_id,
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "header": {
            "mode": "模拟盘",
            "zero_fee_assumption": True,
            "research_only": True,
            "broker_connected": False,
        },
        "metrics": metrics,
        "daily_series": [
            {
                "observation_date": row["observation_date"].isoformat(),
                "total_asset": float(row["total_asset"]),
                "daily_return": float(ret),
                "cumulative_return": float(row["total_asset"]) / pre_start_asset - 1.0,
                "baseline_daily_return": _json_float(row.get("baseline_daily_return")),
                "baseline_cumulative_return": _json_float(
                    row.get("baseline_cumulative_return")
                ),
                "relative_cumulative_return": _json_float(
                    row.get("relative_cumulative_return")
                ),
            }
            for (_, row), ret in zip(daily.iterrows(), returns, strict=True)
        ],
        "sleeve_contribution": contribution,
        "weight_drift": _weight_drift_report(obs),
        "fill_quality": _fill_quality_report(plans, fills, account_id),
        "integrity": {
            "observation_count": int(len(daily)),
            "missing_baseline_dates": _missing_baseline_dates(daily),
            "unattributable_fill_dates": contribution["unattributable_dates"],
            "warnings": warnings,
        },
        "monthly_returns": _monthly_returns(daily),
    }
    return payload


def performance_report_html(payload: dict[str, Any]) -> str:
    """Render a self-contained HTML performance report."""

    data = json.dumps(payload, ensure_ascii=False, sort_keys=True)
    metrics = payload["metrics"]
    rows = "\n".join(
        f"<tr><td>{html.escape(key)}</td><td>{html.escape(str(value))}</td></tr>"
        for key, value in metrics.items()
    )
    return f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <title>shadow-performance-report {html.escape(payload["account_id"])}</title>
  <style>
    body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; margin: 32px; color: #17233d; }}
    h1, h2 {{ color: #102a43; }}
    table {{ border-collapse: collapse; width: 100%; margin: 12px 0 28px; }}
    th, td {{ border: 1px solid #d8e2ef; padding: 8px; text-align: left; }}
    th {{ background: #e8f1fb; }}
    .notice {{ padding: 12px; background: #fff7e6; border: 1px solid #ffd591; }}
    pre {{ white-space: pre-wrap; background: #f6f8fa; padding: 12px; }}
  </style>
</head>
<body>
  <h1>shadow-performance-report</h1>
  <p class="notice">模拟盘；零费用假设；research-only；未连接券商。</p>
  <p>账户：{html.escape(payload["account_id"])}；区间：{payload["start_date"]} 至 {payload["end_date"]}</p>
  <h2>Performance Metrics</h2>
  <table><tbody>{rows}</tbody></table>
  <h2>Daily Series</h2>
  <pre id="daily"></pre>
  <h2>Sleeve 收益贡献</h2>
  <pre id="contribution"></pre>
  <h2>权重偏离 / 成交质量 / 完整性 / 月度收益</h2>
  <pre id="extra"></pre>
  <script type="application/json" id="report-data">{html.escape(data)}</script>
  <script>
    const report = JSON.parse(document.getElementById('report-data').textContent);
    document.getElementById('daily').textContent = JSON.stringify(report.daily_series, null, 2);
    document.getElementById('contribution').textContent = JSON.stringify(report.sleeve_contribution, null, 2);
    document.getElementById('extra').textContent = JSON.stringify({{
      weight_drift: report.weight_drift,
      fill_quality: report.fill_quality,
      integrity: report.integrity,
      monthly_returns: report.monthly_returns
    }}, null, 2);
  </script>
</body>
</html>
"""


def _read_partition(
    lakehouse_root: Path,
    dataset_name: str,
    partition_parts: list[tuple[str, str | int]],
) -> pd.DataFrame:
    path = build_zone_path(dataset_name, StorageZone.DERIVED, lakehouse_root)
    for _, value in partition_parts:
        path = path / str(value)
    path = path / "part-00000.parquet"
    if not path.exists():
        return pd.DataFrame()
    return pd.read_parquet(path)


def _seed_for_account(seed: pd.DataFrame, account_id: str) -> pd.DataFrame:
    if seed.empty or "account_id" not in seed.columns:
        raise ShadowRunError(["missing_or_duplicate_seed"])
    rows = seed[seed["account_id"].astype(str).eq(account_id)].copy()
    if len(rows) != len(ETF_AW_SLEEVE_ROLE_ORDER):
        raise ShadowRunError(["missing_or_duplicate_seed"])
    if set(rows["sleeve_role"].astype(str)) != set(ETF_AW_SLEEVE_ROLE_ORDER):
        raise ShadowRunError(["missing_or_duplicate_seed"])
    return rows.sort_values("sleeve_role", key=etf_aw_role_sort_key).reset_index(
        drop=True
    )


def _latest_observation_before(
    observations: pd.DataFrame, account_id: str, observation_date: date
) -> pd.DataFrame | None:
    if observations.empty:
        return None
    rows = observations[observations["account_id"].astype(str).eq(account_id)].copy()
    if rows.empty:
        return None
    rows["observation_date"] = pd.to_datetime(
        rows["observation_date"], errors="coerce"
    ).dt.date
    dates = sorted(
        value
        for value in rows["observation_date"].dropna().unique()
        if value < observation_date
    )
    if not dates:
        return None
    latest = dates[-1]
    return rows[rows["observation_date"].eq(latest)].sort_values(
        "sleeve_role", key=etf_aw_role_sort_key
    )


def _has_observation(
    observations: pd.DataFrame, account_id: str, observation_date: date
) -> bool:
    if observations.empty:
        return False
    rows = observations[observations["account_id"].astype(str).eq(account_id)].copy()
    if rows.empty:
        return False
    dates = pd.to_datetime(rows["observation_date"], errors="coerce").dt.date
    return bool(dates.eq(observation_date).any())


def _state_quantities(frame: pd.DataFrame) -> dict[str, int]:
    return {str(row["symbol"]): int(row["quantity"]) for _, row in frame.iterrows()}


def _applicable_fills(
    fills: pd.DataFrame, *, account_id: str, after: datetime, at_or_before: datetime
) -> pd.DataFrame:
    if fills.empty:
        return pd.DataFrame()
    rows = fills[fills["account_id"].astype(str).eq(account_id)].copy()
    if rows.empty:
        return pd.DataFrame()
    rows["fill_at"] = pd.to_datetime(rows["fill_at"], errors="coerce")
    return rows[
        (rows["fill_at"] > pd.Timestamp(after))
        & (rows["fill_at"] <= pd.Timestamp(at_or_before))
    ].copy()


def _target_weights(plan: pd.DataFrame) -> dict[str, float]:
    if plan.empty:
        return {}
    return {
        str(row["symbol"]): float(row["target_weight"]) for _, row in plan.iterrows()
    }


def _active_strategy_name(active_plan: pd.DataFrame, seed: pd.DataFrame) -> str | None:
    if not active_plan.empty:
        return str(active_plan.iloc[0]["strategy_name"])
    return None


def _active_strategy_version(
    active_plan: pd.DataFrame, seed: pd.DataFrame
) -> str | None:
    if not active_plan.empty:
        return str(active_plan.iloc[0]["strategy_version"])
    return None


def _next_baseline_cumulative(
    observations: pd.DataFrame,
    *,
    account_id: str,
    observation_date: date,
    baseline: BaselineObservationInput | None,
) -> float | None:
    if baseline is None or baseline.baseline_daily_return is None:
        return None
    previous = _latest_observation_before(observations, account_id, observation_date)
    previous_cumulative = 0.0
    if previous is not None:
        value = previous.iloc[0].get("baseline_cumulative_return")
        if _json_float(value) is None:
            return None
        previous_cumulative = float(value)
    return (1.0 + previous_cumulative) * (1.0 + baseline.baseline_daily_return) - 1.0


def _validate_weight_sum(frame: pd.DataFrame) -> None:
    weight_sum = float(frame["actual_weight"].sum())
    cash_weight = float(frame.iloc[0]["cash"]) / float(frame.iloc[0]["total_asset"])
    if abs(weight_sum + cash_weight - 1.0) > WEIGHT_TOLERANCE:
        raise ShadowRunError(["invalid_observation"], {"reason": "weight_sum_mismatch"})


def _contains(frame: pd.DataFrame, column: str, value: str) -> bool:
    return (
        not frame.empty
        and column in frame.columns
        and bool(frame[column].astype(str).eq(value).any())
    )


def _filled_quantity(fills: pd.DataFrame, plan_id: str, symbol: str, side: str) -> int:
    if fills.empty:
        return 0
    rows = fills[
        fills["plan_id"].astype(str).eq(plan_id)
        & fills["symbol"].astype(str).eq(symbol)
        & fills["order_side"].astype(str).eq(side)
    ]
    return int(rows["fill_quantity"].sum()) if not rows.empty else 0


def _date_value(value: Any) -> date:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    return pd.Timestamp(value).date()


def _dedupe(values: list[str]) -> list[str]:
    return list(dict.fromkeys(values))


def _daily_observation_frame(observations: pd.DataFrame) -> pd.DataFrame:
    rows = observations.sort_values(
        ["observation_date", "sleeve_role"],
        key=lambda series: (
            etf_aw_role_sort_key(series) if series.name == "sleeve_role" else series
        ),
    )
    return (
        rows.drop_duplicates(["account_id", "observation_date"])
        .sort_values("observation_date")
        .reset_index(drop=True)
    )


def _pre_start_asset(
    seed: pd.DataFrame, observations: pd.DataFrame, account_id: str, start: date
) -> float:
    prior = _latest_observation_before(observations, account_id, start)
    if prior is None:
        return float(seed.iloc[0]["total_asset"])
    return float(prior.iloc[0]["total_asset"])


def _first_observation_date(observations: pd.DataFrame, account_id: str) -> date | None:
    rows = observations[observations["account_id"].astype(str).eq(account_id)].copy()
    if rows.empty:
        return None
    return min(
        pd.to_datetime(rows["observation_date"], errors="coerce").dt.date.dropna()
    )


def _performance_metrics(
    *,
    returns: list[float],
    assets: list[float],
    pre_start_asset: float,
    ending_asset: float,
) -> dict[str, float | None]:
    series = pd.Series(returns, dtype=float)
    interval_count = len(series)
    period_return = ending_asset / pre_start_asset - 1.0
    vol = None if interval_count < 2 else float(series.std(ddof=1) * math.sqrt(252))
    annualized_return = (
        None
        if interval_count == 0
        else (1.0 + period_return) ** (252 / interval_count) - 1.0
    )
    cumulative_max = pd.Series(assets, dtype=float).cummax()
    drawdowns = pd.Series(assets, dtype=float) / cumulative_max - 1.0
    max_drawdown = float(drawdowns.min())
    sharpe = None
    calmar = None
    if interval_count >= 20 and vol not in (None, 0.0):
        sharpe = float(series.mean() / series.std(ddof=1) * math.sqrt(252))
    if interval_count >= 20 and max_drawdown < 0 and annualized_return is not None:
        calmar = annualized_return / abs(max_drawdown)
    gains = series[series > 0]
    losses = series[series < 0]
    return {
        "initial_asset": pre_start_asset,
        "ending_asset": ending_asset,
        "period_return": period_return,
        "annualized_return": annualized_return,
        "annualized_volatility": vol,
        "max_drawdown": max_drawdown,
        "sharpe": sharpe,
        "calmar": calmar,
        "positive_day_ratio": (
            None if interval_count == 0 else float((series > 0).sum() / interval_count)
        ),
        "daily_profit_loss_ratio": (
            None
            if gains.empty or losses.empty
            else float(gains.mean() / abs(losses.mean()))
        ),
    }


def _warning_counts(observations: pd.DataFrame) -> dict[str, int]:
    counts: dict[str, int] = {}
    for value in observations["warnings_json"].dropna().tolist():
        for warning in json.loads(value):
            counts[warning] = counts.get(warning, 0) + 1
    return counts


def _sleeve_contribution(
    seed: pd.DataFrame, observations: pd.DataFrame, fills: pd.DataFrame
) -> dict[str, Any]:
    obs = observations.copy()
    obs["observation_date"] = pd.to_datetime(
        obs["observation_date"], errors="coerce"
    ).dt.date
    dates = sorted(obs["observation_date"].dropna().unique())
    cumulative = {role: 0.0 for role in ETF_AW_SLEEVE_ROLE_ORDER}
    rows = []
    unattributable = []
    previous_prices = {
        str(row["symbol"]): (
            float(row["market_value"]) / int(row["quantity"])
            if int(row["quantity"]) > 0
            else None
        )
        for _, row in seed.iterrows()
    }
    previous_weights = {
        str(row["symbol"]): float(row["market_value"]) / float(row["total_asset"])
        for _, row in seed.iterrows()
    }
    fill_dates = set()
    if not fills.empty:
        fill_dates = set(
            pd.to_datetime(fills["fill_at"], errors="coerce").dt.date.dropna()
        )
    for current_date in dates:
        day = obs[obs["observation_date"].eq(current_date)].sort_values(
            "sleeve_role", key=etf_aw_role_sort_key
        )
        if current_date in fill_dates:
            unattributable.append(current_date.isoformat())
        else:
            for _, row in day.iterrows():
                symbol = str(row["symbol"])
                role = str(row["sleeve_role"])
                if previous_prices.get(symbol) not in (None, 0):
                    sleeve_return = (
                        float(row["close_price"]) / float(previous_prices[symbol]) - 1.0
                    )
                    cumulative[role] += (
                        previous_weights.get(symbol, 0.0) * sleeve_return
                    )
        rows.append({"observation_date": current_date.isoformat(), **cumulative})
        previous_prices = {
            str(row["symbol"]): float(row["close_price"]) for _, row in day.iterrows()
        }
        previous_weights = {
            str(row["symbol"]): float(row["actual_weight"]) for _, row in day.iterrows()
        }
    return {
        "basis": "lagged_actual_weight",
        "cumulative": rows,
        "unattributable_dates": unattributable,
    }


def _weight_drift_report(observations: pd.DataFrame) -> dict[str, Any]:
    values = pd.to_numeric(observations["weight_drift"], errors="coerce").dropna()
    breaches = observations[
        pd.to_numeric(observations["weight_drift"], errors="coerce").abs() > 0.02
    ]
    return {
        "max_abs_weight_drift": None if values.empty else float(values.abs().max()),
        "breach_dates": sorted(
            {
                _date_value(value).isoformat()
                for value in breaches["observation_date"].tolist()
            }
        ),
    }


def _fill_quality_report(
    plans: pd.DataFrame, fills: pd.DataFrame, account_id: str
) -> list[dict[str, Any]]:
    account_plans = (
        plans[plans["account_id"].astype(str).eq(account_id)]
        if not plans.empty
        else pd.DataFrame()
    )
    results = []
    for plan_id in (
        sorted(account_plans["plan_id"].astype(str).unique())
        if not account_plans.empty
        else []
    ):
        results.extend(
            build_post_mortem(
                plan_id, account_plans, pd.DataFrame(), fills, pd.DataFrame()
            )["fill_quality"]
        )
    return results


def _missing_baseline_dates(daily: pd.DataFrame) -> list[str]:
    rows = daily[pd.to_numeric(daily["baseline_daily_return"], errors="coerce").isna()]
    return [value.isoformat() for value in rows["observation_date"].tolist()]


def _monthly_returns(daily: pd.DataFrame) -> list[dict[str, Any]]:
    rows = []
    for month, group in daily.groupby(
        daily["observation_date"].apply(lambda value: value.strftime("%Y-%m"))
    ):
        returns = pd.to_numeric(group["daily_return"], errors="coerce").dropna()
        rows.append(
            {
                "month": month,
                "shadow_return": (
                    None if returns.empty else float((1 + returns).prod() - 1)
                ),
                "coverage": "partial",
            }
        )
    return rows


def _json_float(value: Any) -> float | None:
    if value is None or pd.isna(value):
        return None
    number = float(value)
    return number if math.isfinite(number) else None


def _jsonable_record(record: dict[str, Any] | None) -> dict[str, Any] | None:
    if record is None:
        return None
    result = {}
    for key, value in record.items():
        if isinstance(value, pd.Timestamp):
            result[key] = value.isoformat()
        elif isinstance(value, datetime):
            result[key] = value.isoformat()
        elif isinstance(value, date):
            result[key] = value.isoformat()
        elif pd.isna(value):
            result[key] = None
        else:
            result[key] = value
    return result
