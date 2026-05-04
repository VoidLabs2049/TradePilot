"""Dataset definition models for the generic ETL registry."""

from __future__ import annotations

from pydantic import BaseModel, Field, field_validator

from tradepilot.etl.models import DatasetCategory, DependencyType, StorageZone
from tradepilot.etl.path_safety import validate_safe_path_component


class DatasetDefinition(BaseModel):
    """Metadata contract for one dataset in the ETL registry."""

    dataset_name: str = Field(
        description="Stable registry key and safe path component for the dataset."
    )
    category: DatasetCategory = Field(
        description="Business family used to group related datasets."
    )
    grain: str = Field(
        description="Smallest logical observation level, such as daily stock bars."
    )
    primary_source: str = Field(
        description="Default source adapter name used to fetch the dataset."
    )
    storage_zone: StorageZone = Field(
        description="Lakehouse zone where the dataset is primarily stored."
    )
    fallback_sources: list[str] = Field(
        default_factory=list,
        description="Ordered fallback source adapter names for degraded fetching.",
    )
    validation_sources: list[str] = Field(
        default_factory=list,
        description="Independent source names used to cross-check dataset quality.",
    )
    partition_strategy: str | None = Field(
        default=None,
        description="Storage partitioning rule applied when writing dataset files.",
    )
    canonical_schema_name: str | None = Field(
        default=None,
        description="Canonical schema identifier expected after normalization.",
    )
    validation_rule_names: list[str] = Field(
        default_factory=list,
        description="Validation rule names that should run for this dataset.",
    )
    supports_incremental: bool = Field(
        default=False,
        description="Whether the dataset supports watermark-based incremental sync.",
    )
    watermark_key: str | None = Field(
        default=None,
        description="Field name used as the incremental sync watermark.",
    )
    timing_semantics: str | None = Field(
        default=None,
        description="Timing convention for interpreting record dates and availability.",
    )
    dependencies: list[str] = Field(
        default_factory=list,
        description="Dataset names that must be available before this dataset runs.",
    )
    dependency_types: dict[str, DependencyType] = Field(
        default_factory=dict,
        description="Preflight semantics for each dependency dataset.",
    )

    @field_validator("dataset_name", "grain", "primary_source")
    @classmethod
    def _validate_required_text(cls, value: str) -> str:
        """Reject blank required text fields."""

        stripped = value.strip()
        if not stripped:
            raise ValueError("value must not be blank")
        return stripped

    @field_validator("dataset_name")
    @classmethod
    def _validate_dataset_name(cls, value: str) -> str:
        """Reject dataset names that are not safe path components."""

        return validate_safe_path_component(value, "dataset_name")


def build_reference_trading_calendar_dataset() -> DatasetDefinition:
    """Return the Stage B trading calendar dataset definition."""

    return DatasetDefinition(
        dataset_name="reference.trading_calendar",
        category=DatasetCategory.REFERENCE,
        grain="exchange_trade_date",
        primary_source="tushare",
        storage_zone=StorageZone.RAW,
        partition_strategy="year_month",
        canonical_schema_name="canonical_trading_calendar",
        validation_rule_names=[
            "calendar.duplicate_key",
            "calendar.trade_date_required",
            "calendar.exchange_supported",
            "calendar.is_open_boolean",
            "calendar.pretrade_before_trade_date",
            "calendar.open_day_pretrade_sequence",
            "calendar.date_continuity",
        ],
    )


def build_reference_instruments_dataset() -> DatasetDefinition:
    """Return the Stage B instruments dataset definition."""

    return DatasetDefinition(
        dataset_name="reference.instruments",
        category=DatasetCategory.REFERENCE,
        grain="instrument",
        primary_source="tushare",
        storage_zone=StorageZone.RAW,
        partition_strategy="snapshot_date",
        canonical_schema_name="canonical_instruments",
        validation_rule_names=[
            "instruments.instrument_id_required",
            "instruments.instrument_id_format",
            "instruments.duplicate_instrument_id",
            "instruments.exchange_suffix_match",
            "instruments.name_required",
            "instruments.type_supported",
            "instruments.list_delist_order",
            "instruments.active_boolean",
        ],
    )


def build_reference_rebalance_calendar_dataset() -> DatasetDefinition:
    """Return the ETF all-weather monthly rebalance calendar definition."""

    return DatasetDefinition(
        dataset_name="reference.rebalance_calendar",
        category=DatasetCategory.REFERENCE,
        grain="calendar_month",
        primary_source="derived",
        storage_zone=StorageZone.NORMALIZED,
        canonical_schema_name="canonical_rebalance_calendar",
        validation_rule_names=[
            "rebalance_calendar.monthly_post_20_common_open_day",
            "rebalance_calendar.effective_date_required",
            "rebalance_calendar.duplicate_calendar_month",
        ],
        dependencies=["reference.trading_calendar"],
        dependency_types={
            "reference.trading_calendar": DependencyType.WINDOW,
        },
    )


def build_reference_etf_aw_sleeves_dataset() -> DatasetDefinition:
    """Return the frozen ETF all-weather v1 sleeve registry definition."""

    return DatasetDefinition(
        dataset_name="reference.etf_aw_sleeves",
        category=DatasetCategory.REFERENCE,
        grain="sleeve",
        primary_source="static",
        storage_zone=StorageZone.NORMALIZED,
        canonical_schema_name="canonical_sleeves",
        validation_rule_names=[
            "sleeves.frozen_v1_exact_codes",
            "sleeves.role_supported",
            "sleeves.exposure_note_present",
        ],
    )


def build_market_etf_daily_dataset() -> DatasetDefinition:
    """Return the Stage B ETF daily dataset definition."""

    dependencies = ["reference.instruments", "reference.trading_calendar"]
    return DatasetDefinition(
        dataset_name="market.etf_daily",
        category=DatasetCategory.MARKET,
        grain="instrument_trade_date",
        primary_source="tushare",
        storage_zone=StorageZone.NORMALIZED,
        partition_strategy="year_month",
        canonical_schema_name="market_daily_v1",
        validation_rule_names=[
            "market_daily.duplicate_business_key",
            "market_daily.instrument_exists",
            "market_daily.instrument_type_matches_dataset",
            "market_daily.trade_date_open",
            "market_daily.close_required",
            "market_daily.ohlc_non_negative",
            "market_daily.ohlc_order",
            "market_daily.volume_non_negative",
            "market_daily.amount_non_negative",
            "market_daily.extreme_return",
            "market_daily.change_consistency",
            "market_daily.pct_chg_consistency",
        ],
        supports_incremental=True,
        watermark_key="trade_date",
        dependencies=dependencies,
        dependency_types={
            "reference.instruments": DependencyType.SNAPSHOT,
            "reference.trading_calendar": DependencyType.WINDOW,
        },
    )


def build_market_etf_adj_factor_dataset() -> DatasetDefinition:
    """Return the ETF adjustment factor dataset definition."""

    dependencies = ["reference.instruments", "reference.trading_calendar"]
    return DatasetDefinition(
        dataset_name="market.etf_adj_factor",
        category=DatasetCategory.MARKET,
        grain="instrument_trade_date",
        primary_source="tushare",
        storage_zone=StorageZone.NORMALIZED,
        partition_strategy="year_month",
        canonical_schema_name="etf_adj_factor_v1",
        validation_rule_names=[
            "etf_adj_factor.duplicate_business_key",
            "etf_adj_factor.instrument_id_required",
            "etf_adj_factor.trade_date_required",
            "etf_adj_factor.adj_factor_required",
            "etf_adj_factor.adj_factor_positive",
            "etf_adj_factor.instrument_exists",
        ],
        supports_incremental=True,
        watermark_key="trade_date",
        dependencies=dependencies,
        dependency_types={
            "reference.instruments": DependencyType.SNAPSHOT,
            "reference.trading_calendar": DependencyType.WINDOW,
        },
    )


def build_derived_etf_aw_sleeve_daily_dataset() -> DatasetDefinition:
    """Return the adjustment-aware ETF all-weather sleeve daily panel definition."""

    return DatasetDefinition(
        dataset_name="derived.etf_aw_sleeve_daily",
        category=DatasetCategory.DERIVED,
        grain="sleeve_trade_date",
        primary_source="derived",
        storage_zone=StorageZone.DERIVED,
        partition_strategy="year_month",
        canonical_schema_name="etf_aw_sleeve_daily_v1",
        timing_semantics=(
            "adj_pct_chg is the return between adjacent available observations "
            "after joining ETF daily bars with adjustment factors; it is not a "
            "strict calendar-day continuity assertion."
        ),
        validation_rule_names=[
            "sleeve_daily.duplicate_business_key",
            "sleeve_daily.adjustment_factor_present",
            "sleeve_daily.adjusted_close_positive",
        ],
        dependencies=[
            "reference.etf_aw_sleeves",
            "market.etf_daily",
            "market.etf_adj_factor",
        ],
    )


def build_market_index_daily_dataset() -> DatasetDefinition:
    """Return the Stage B index daily dataset definition."""

    dependencies = ["reference.instruments", "reference.trading_calendar"]
    return DatasetDefinition(
        dataset_name="market.index_daily",
        category=DatasetCategory.MARKET,
        grain="instrument_trade_date",
        primary_source="tushare",
        storage_zone=StorageZone.NORMALIZED,
        partition_strategy="year_month",
        canonical_schema_name="market_daily_v1",
        validation_rule_names=[
            "market_daily.duplicate_business_key",
            "market_daily.instrument_exists",
            "market_daily.instrument_type_matches_dataset",
            "market_daily.trade_date_open",
            "market_daily.close_required",
            "market_daily.ohlc_non_negative",
            "market_daily.ohlc_order",
            "market_daily.volume_non_negative",
            "market_daily.amount_non_negative",
            "market_daily.extreme_return",
            "market_daily.change_consistency",
            "market_daily.pct_chg_consistency",
        ],
        supports_incremental=True,
        watermark_key="trade_date",
        dependencies=dependencies,
        dependency_types={
            "reference.instruments": DependencyType.SNAPSHOT,
            "reference.trading_calendar": DependencyType.WINDOW,
        },
    )


def build_stage_b_datasets() -> list[DatasetDefinition]:
    """Return all built-in Stage B dataset definitions."""

    return [
        build_reference_trading_calendar_dataset(),
        build_reference_instruments_dataset(),
        build_reference_rebalance_calendar_dataset(),
        build_reference_etf_aw_sleeves_dataset(),
        build_market_etf_adj_factor_dataset(),
        build_market_etf_daily_dataset(),
        build_market_index_daily_dataset(),
        build_derived_etf_aw_sleeve_daily_dataset(),
    ]
