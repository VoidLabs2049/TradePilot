"""Pydantic models for daily workflow snapshots."""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum

from pydantic import BaseModel, Field

WORKFLOW_CONTEXT_SCHEMA_VERSION = "workflow-context.v1"
WORKFLOW_INSIGHT_SCHEMA_VERSION = "workflow-insight.v1"
WORKFLOW_CONTEXT_PRODUCER = "tradepilot"
WORKFLOW_INSIGHT_PRODUCER = "the_one"
WORKFLOW_PRODUCER_VERSION = "tradepilot-briefing-first-refactor-stage1"


class WorkflowPhase(StrEnum):
    """Supported workflow phases for the daily operating loop."""

    PRE_MARKET = "pre_market"
    POST_MARKET = "post_market"


class WorkflowStatus(StrEnum):
    """Execution status for one workflow run."""

    SUCCESS = "success"
    PARTIAL = "partial"
    FAILED = "failed"
    SKIPPED = "skipped"


class WorkflowTrigger(StrEnum):
    """How a workflow run was triggered."""

    MANUAL = "manual"
    SCHEDULER = "scheduler"


class InsightStatus(StrEnum):
    """Lifecycle status for one workflow insight."""

    NOT_REQUESTED = "not_requested"
    PENDING = "pending"
    COMPLETED = "completed"
    FAILED = "failed"
    STALE = "stale"


class WorkflowStepResult(BaseModel):
    """Status summary for one workflow step."""

    name: str
    status: str
    records_affected: int = 0
    error_message: str | None = None
    details: dict = Field(default_factory=dict)


class WorkflowSummary(BaseModel):
    """Top-level payload rendered by the workflow UI."""

    title: str
    overview: str
    requested_date: str | None = None
    resolved_date: str | None = None
    date_resolution: str = "exact"
    market_overview: dict = Field(default_factory=dict)
    sector_positioning: dict = Field(default_factory=dict)
    position_health: dict = Field(default_factory=dict)
    next_day_prep: dict = Field(default_factory=dict)
    yesterday_recap: dict = Field(default_factory=dict)
    overnight_news: dict = Field(default_factory=dict)
    today_watchlist: dict = Field(default_factory=dict)
    action_frame: dict = Field(default_factory=dict)
    watch_context: dict = Field(default_factory=dict)
    alerts: list[dict] = Field(default_factory=list)
    metadata: dict = Field(default_factory=dict)
    steps: list[WorkflowStepResult] = Field(default_factory=list)
    watchlist: dict = Field(default_factory=dict)
    scan: dict = Field(default_factory=dict)
    news: dict = Field(default_factory=dict)
    scheduler: dict = Field(default_factory=dict)
    carry_over: dict = Field(default_factory=dict)
    cross_day_review: dict = Field(default_factory=dict)
    research_archive: dict = Field(default_factory=dict)


class WorkflowRunRecord(BaseModel):
    """Persisted workflow run record."""

    id: int
    workflow_date: str
    phase: WorkflowPhase
    triggered_by: WorkflowTrigger
    status: WorkflowStatus
    started_at: datetime
    finished_at: datetime | None = None
    summary: WorkflowSummary
    error_message: str | None = None


class WorkflowRunResponse(BaseModel):
    """API payload for one workflow run."""

    run: WorkflowRunRecord


class WorkflowHistoryItem(BaseModel):
    """Compact workflow history row for tables."""

    id: int
    workflow_date: str
    phase: str
    triggered_by: str
    status: str
    started_at: str
    finished_at: str | None = None
    error_message: str | None = None


class WorkflowContextPayload(BaseModel):
    """Structured workflow context exposed to external consumers."""

    schema_version: str = WORKFLOW_CONTEXT_SCHEMA_VERSION
    producer: str = WORKFLOW_CONTEXT_PRODUCER
    producer_version: str = WORKFLOW_PRODUCER_VERSION
    generated_at: datetime
    workflow_run_id: int
    workflow_date: str
    phase: WorkflowPhase
    context: dict = Field(default_factory=dict)
    metadata: dict = Field(default_factory=dict)


class EtfAwRiskBudgetSleeveResponse(BaseModel):
    """One sleeve row in the ETF all-weather risk budget response."""

    sleeve_role: str
    base_budget: float | None = None
    delta_budget: float | None = None
    tilted_budget: float | None = None
    budget_status: str | None = None
    quality_notes: dict = Field(default_factory=dict)


class EtfAwRiskBudgetResponse(BaseModel):
    """Latest frozen ETF all-weather risk budget response."""

    schema_version: str
    contract_version: str
    calendar_name: str | None = None
    rebalance_date: str | None = None
    strategy_name: str | None = None
    strategy_version: str | None = None
    market_regime_label: str | None = None
    budget_status: str | None = None
    budget_basis: str | None = None
    confidence_score: float | None = None
    effective_confidence_score: float | None = None
    base_budget_sum: float | None = None
    tilted_budget_sum: float | None = None
    budgets: list[EtfAwRiskBudgetSleeveResponse] = Field(default_factory=list)
    quality_notes: dict = Field(default_factory=dict)
    source_strategy_context_rebalance_date: str | None = None
    source_regime_rebalance_date: str | None = None


class InsightSectionKey(StrEnum):
    """Standard section keys for The-One workflow insights."""

    MARKET_VIEW = "market_view"
    THEME_VIEW = "theme_view"
    POSITION_VIEW = "position_view"
    TOMORROW_VIEW = "tomorrow_view"
    ACTION_FRAME = "action_frame"
    RISK_NOTES = "risk_notes"
    EXECUTION_NOTES = "execution_notes"
    CUSTOM = "custom"


class InsightMetric(BaseModel):
    """Labeled metric rendered inside one insight section."""

    label: str
    value: str | int | float | None = None


class InsightListItem(BaseModel):
    """Structured list item rendered inside one insight section."""

    title: str | None = None
    description: str | None = None
    status: str | None = None
    tags: list[str] = Field(default_factory=list)


class WorkflowInsightSection(BaseModel):
    """One standardized section inside a workflow insight."""

    key: InsightSectionKey | str
    title: str
    summary: str | None = None
    bullets: list[str] = Field(default_factory=list)
    tags: list[str] = Field(default_factory=list)
    metrics: list[InsightMetric] = Field(default_factory=list)
    items: list[InsightListItem] = Field(default_factory=list)


class WorkflowInsightPayload(BaseModel):
    """Structured insight payload produced by The-One."""

    summary: str | None = None
    sections: list[WorkflowInsightSection] = Field(default_factory=list)


class WorkflowInsightRecord(BaseModel):
    """Persisted insight produced from one workflow context."""

    id: int
    workflow_run_id: int
    workflow_date: str
    phase: WorkflowPhase
    producer: str = WORKFLOW_INSIGHT_PRODUCER
    status: InsightStatus
    schema_version: str = WORKFLOW_INSIGHT_SCHEMA_VERSION
    producer_version: str
    generated_at: datetime
    source_run_id: int
    source_context_schema_version: str = WORKFLOW_CONTEXT_SCHEMA_VERSION
    insight: WorkflowInsightPayload = Field(default_factory=WorkflowInsightPayload)
    error_message: str | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None


class WorkflowInsightUpsertRequest(BaseModel):
    """Write-back payload for one workflow insight."""

    workflow_date: str
    phase: WorkflowPhase
    producer: str = WORKFLOW_INSIGHT_PRODUCER
    status: InsightStatus = InsightStatus.COMPLETED
    schema_version: str = WORKFLOW_INSIGHT_SCHEMA_VERSION
    producer_version: str
    generated_at: datetime
    source_run_id: int
    source_context_schema_version: str = WORKFLOW_CONTEXT_SCHEMA_VERSION
    insight: WorkflowInsightPayload = Field(default_factory=WorkflowInsightPayload)
    error_message: str | None = None


class WorkflowInsightResponse(BaseModel):
    """API payload for one workflow insight lookup."""

    insight: WorkflowInsightRecord | None = None
    state: InsightStatus = InsightStatus.NOT_REQUESTED
    is_stale: bool = False
    latest_run_id: int | None = None
