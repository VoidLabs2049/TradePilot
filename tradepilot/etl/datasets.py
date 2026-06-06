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


def build_derived_etf_aw_rebalance_snapshot_dataset() -> DatasetDefinition:
    """Return the ETF all-weather monthly rebalance snapshot definition."""

    return DatasetDefinition(
        dataset_name="derived.etf_aw_rebalance_snapshot",
        category=DatasetCategory.DERIVED,
        grain="calendar_rebalance_sleeve",
        primary_source="derived",
        storage_zone=StorageZone.DERIVED,
        partition_strategy="year_month",
        canonical_schema_name="etf_aw_rebalance_snapshot_v1",
        timing_semantics=(
            "Monthly post-20 rebalance read model built from adjustment-aware "
            "ETF all-weather sleeve daily observations."
        ),
        validation_rule_names=[
            "rebalance_snapshot.duplicate_business_key",
            "rebalance_snapshot.five_frozen_sleeves_per_rebalance_date",
            "rebalance_snapshot.rebalance_dates_from_calendar",
            "rebalance_snapshot.data_status_allowed",
            "rebalance_snapshot.quality_notes_json",
            "rebalance_snapshot.complete_rows_have_features",
        ],
        dependencies=[
            "reference.rebalance_calendar",
            "reference.etf_aw_sleeves",
            "derived.etf_aw_sleeve_daily",
        ],
    )


def build_derived_etf_aw_regime_score_dataset() -> DatasetDefinition:
    """Return the ETF all-weather market-only regime score definition."""

    return DatasetDefinition(
        dataset_name="derived.etf_aw_regime_score",
        category=DatasetCategory.DERIVED,
        grain="calendar_rebalance_scorer",
        primary_source="derived",
        storage_zone=StorageZone.DERIVED,
        partition_strategy="year_month",
        canonical_schema_name="etf_aw_regime_score_v1",
        timing_semantics=(
            "Market-only regime score built from the ETF all-weather rebalance "
            "snapshot; it is not a macro regime classifier or allocation model."
        ),
        validation_rule_names=[
            "regime_score.duplicate_business_key",
            "regime_score.status_allowed",
            "regime_score.confidence_capped",
        ],
        dependencies=["derived.etf_aw_rebalance_snapshot"],
    )


def build_derived_etf_aw_market_features_dataset() -> DatasetDefinition:
    """Return the ETF all-weather strategy-facing market features definition."""

    return DatasetDefinition(
        dataset_name="derived.etf_aw_market_features",
        category=DatasetCategory.DERIVED,
        grain="calendar_rebalance_feature",
        primary_source="derived",
        storage_zone=StorageZone.DERIVED,
        partition_strategy="year_month",
        canonical_schema_name="etf_aw_market_features_v1",
        timing_semantics=(
            "Strategy-facing market feature rows built only from ETF all-weather "
            "rebalance snapshot and market-only regime score derived datasets."
        ),
        validation_rule_names=[
            "market_features.duplicate_business_key",
            "market_features.required_columns",
            "market_features.scope_allowed",
            "market_features.name_matches_scope",
            "market_features.status_allowed",
            "market_features.complete_values_finite",
            "market_features.quality_notes_json",
            "market_features.no_macro_rates_features",
        ],
        dependencies=[
            "derived.etf_aw_rebalance_snapshot",
            "derived.etf_aw_regime_score",
        ],
    )


def build_derived_etf_aw_strategy_context_dataset() -> DatasetDefinition:
    """Return the ETF all-weather strategy context boundary definition."""

    return DatasetDefinition(
        dataset_name="derived.etf_aw_strategy_context",
        category=DatasetCategory.DERIVED,
        grain="calendar_rebalance_strategy_context",
        primary_source="derived",
        storage_zone=StorageZone.DERIVED,
        partition_strategy="year_month",
        canonical_schema_name="etf_aw_strategy_context_v1",
        timing_semantics=(
            "Strategy-facing context assembly boundary; this dataset carries "
            "market, macro, rates, source, revision, and point-in-time caveats "
            "but no allocation or trade instruction fields."
        ),
        validation_rule_names=[
            "strategy_context.duplicate_business_key",
            "strategy_context.status_allowed",
            "strategy_context.complete_requires_primary_fields",
            "strategy_context.research_ready_requires_complete",
            "strategy_context.no_allocation_trade_fields",
            "strategy_context.json_fields_valid",
            "strategy_context.deferred_requires_point_in_time_notes",
        ],
        dependencies=[
            "derived.etf_aw_market_features",
            "derived.etf_aw_regime_score",
        ],
    )


def build_macro_slow_fields_dataset() -> DatasetDefinition:
    """Return the Stage F slow macro fields dataset definition."""

    return DatasetDefinition(
        dataset_name="macro.slow_fields",
        category=DatasetCategory.MACRO,
        grain="field_period",
        primary_source="tushare",
        storage_zone=StorageZone.NORMALIZED,
        partition_strategy="year_month",
        canonical_schema_name="slow_fields_v1",
        timing_semantics=(
            "Slow macro observations use conservative release-date rules and "
            "become eligible only after effective_date."
        ),
        validation_rule_names=[
            "slow_fields.duplicate_business_key",
            "slow_fields.field_allowed",
            "slow_fields.value_plausible",
            "slow_fields.unit_allowed",
            "slow_fields.release_date_required",
            "slow_fields.effective_date_required",
            "slow_fields.effective_date_after_release",
            "slow_fields.field_role_matches",
            "slow_fields.source_caveat_present",
        ],
        supports_incremental=True,
        watermark_key="effective_date",
        dependencies=["reference.trading_calendar"],
        dependency_types={
            "reference.trading_calendar": DependencyType.WINDOW,
        },
    )


def build_rates_daily_rates_dataset() -> DatasetDefinition:
    """Return the Stage F daily interbank rates dataset definition."""

    return DatasetDefinition(
        dataset_name="rates.daily_rates",
        category=DatasetCategory.RATES,
        grain="field_trade_date",
        primary_source="tushare",
        storage_zone=StorageZone.NORMALIZED,
        partition_strategy="year_month",
        canonical_schema_name="daily_rates_v1",
        validation_rule_names=[
            "daily_rates.duplicate_business_key",
            "daily_rates.field_allowed",
            "daily_rates.value_plausible",
            "daily_rates.unit_allowed",
            "daily_rates.trade_date_required",
            "daily_rates.release_date_required",
            "daily_rates.effective_date_required",
            "daily_rates.revision_note_required",
            "daily_rates.field_role_matches",
            "daily_rates.source_caveat_present",
        ],
        supports_incremental=True,
        watermark_key="trade_date",
        dependencies=["reference.trading_calendar"],
        dependency_types={
            "reference.trading_calendar": DependencyType.WINDOW,
        },
    )


def build_rates_lpr_dataset() -> DatasetDefinition:
    """Return the Stage F loan prime rate dataset definition."""

    return DatasetDefinition(
        dataset_name="rates.lpr",
        category=DatasetCategory.RATES,
        grain="field_quote_date",
        primary_source="tushare",
        storage_zone=StorageZone.NORMALIZED,
        partition_strategy="year_month",
        canonical_schema_name="lpr_v1",
        validation_rule_names=[
            "lpr.duplicate_business_key",
            "lpr.field_allowed",
            "lpr.value_plausible",
            "lpr.unit_allowed",
            "lpr.quote_date_required",
            "lpr.release_date_required",
            "lpr.effective_date_required",
            "lpr.effective_date_after_release",
            "lpr.revision_note_required",
            "lpr.field_role_matches",
            "lpr.source_caveat_present",
        ],
        supports_incremental=True,
        watermark_key="quote_date",
        dependencies=["reference.trading_calendar"],
        dependency_types={
            "reference.trading_calendar": DependencyType.WINDOW,
        },
    )


def build_rates_gov_curve_points_dataset() -> DatasetDefinition:
    """Return the Stage F government curve point dataset definition."""

    return DatasetDefinition(
        dataset_name="rates.gov_curve_points",
        category=DatasetCategory.RATES,
        grain="curve_tenor_date",
        primary_source="tushare",
        storage_zone=StorageZone.NORMALIZED,
        partition_strategy="year_month",
        canonical_schema_name="gov_curve_points_v1",
        timing_semantics=(
            "Government curve points are state observations; effective_date is "
            "the curve observation date and consumers must preserve extraction caveats."
        ),
        validation_rule_names=[
            "gov_curve_points.duplicate_business_key",
            "gov_curve_points.field_allowed",
            "gov_curve_points.value_plausible",
            "gov_curve_points.unit_allowed",
            "gov_curve_points.curve_date_required",
            "gov_curve_points.release_date_required",
            "gov_curve_points.effective_date_required",
            "gov_curve_points.tenor_positive",
            "gov_curve_points.field_role_matches",
            "gov_curve_points.source_caveat_present",
        ],
        supports_incremental=True,
        watermark_key="curve_date",
        dependencies=["reference.trading_calendar"],
        dependency_types={
            "reference.trading_calendar": DependencyType.WINDOW,
        },
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
        build_derived_etf_aw_rebalance_snapshot_dataset(),
        build_derived_etf_aw_regime_score_dataset(),
        build_derived_etf_aw_market_features_dataset(),
        build_derived_etf_aw_strategy_context_dataset(),
        build_macro_slow_fields_dataset(),
        build_rates_daily_rates_dataset(),
        build_rates_lpr_dataset(),
        build_rates_gov_curve_points_dataset(),
    ]
