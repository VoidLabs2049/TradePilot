"""Workflow API routes for the simplified daily operating loop."""

from __future__ import annotations

from datetime import date

from fastapi import APIRouter, Query

from tradepilot.etl.read_models import get_latest_etf_aw_risk_budget
from tradepilot.workflow.models import (
    WorkflowContextPayload,
    WorkflowInsightResponse,
    WorkflowInsightUpsertRequest,
    WorkflowPhase,
    WorkflowRunResponse,
    WorkflowTrigger,
)
from tradepilot.workflow.service import DailyWorkflowService

router = APIRouter()
_service = DailyWorkflowService()


@router.get("/latest", response_model=WorkflowRunResponse | None)
def get_latest_workflow(
    phase: WorkflowPhase = Query(..., description="Workflow phase to fetch"),
) -> WorkflowRunResponse | None:
    """Return the latest workflow snapshot for one phase."""
    run = _service.get_latest_run(phase)
    if run is None:
        return None
    return WorkflowRunResponse(run=run)


@router.get("/history")
def get_workflow_history(limit: int = 20) -> list[dict]:
    """Return recent workflow history rows."""
    return [item.model_dump() for item in _service.list_history(limit=limit)]


@router.get("/status")
def get_workflow_status() -> dict:
    """Return the latest status for both workflow phases."""
    return _service.get_workflow_status()


@router.post("/pre/run", response_model=WorkflowRunResponse)
def run_pre_market_workflow(workflow_date: str | None = None) -> WorkflowRunResponse:
    """Trigger a manual pre-market workflow run."""
    run = _service.run_pre_market_workflow(
        workflow_date=workflow_date,
        triggered_by=WorkflowTrigger.MANUAL,
    )
    return WorkflowRunResponse(run=run)


@router.post("/post/run", response_model=WorkflowRunResponse)
def run_post_market_workflow(workflow_date: str | None = None) -> WorkflowRunResponse:
    """Trigger a manual post-market workflow run."""
    run = _service.run_post_market_workflow(
        workflow_date=workflow_date,
        triggered_by=WorkflowTrigger.MANUAL,
    )
    return WorkflowRunResponse(run=run)


@router.get("/context/latest", response_model=WorkflowContextPayload | None)
def get_latest_workflow_context(
    phase: WorkflowPhase = Query(
        ..., description="Workflow phase to fetch context for"
    ),
) -> WorkflowContextPayload | None:
    """Return the latest structured context for one workflow phase."""
    return _service.get_latest_context(phase)


@router.get("/etf-aw/latest")
def get_latest_etf_aw_context(
    as_of_date: date | None = Query(
        None, description="Latest rebalance snapshot date upper bound"
    ),
) -> dict | None:
    """Return the latest ETF all-weather snapshot context."""
    return _service.get_latest_etf_aw_context(as_of_date=as_of_date)


@router.get("/etf-aw/risk-budget/latest")
def get_latest_etf_aw_risk_budget_context(
    as_of_date: date | None = Query(
        None, description="Latest risk budget rebalance date upper bound"
    ),
) -> dict | None:
    """Return the latest frozen ETF all-weather risk budget."""
    return get_latest_etf_aw_risk_budget(as_of_date=as_of_date)


@router.get("/insight/latest", response_model=WorkflowInsightResponse)
def get_latest_workflow_insight(
    phase: WorkflowPhase = Query(
        ..., description="Workflow phase to fetch insight for"
    ),
    producer: str = Query("the_one", description="Insight producer identifier"),
) -> WorkflowInsightResponse:
    """Return the latest insight state for one workflow phase."""
    return _service.get_latest_insight(phase=phase, producer=producer)


@router.put("/insight", response_model=WorkflowInsightResponse)
def upsert_workflow_insight(
    payload: WorkflowInsightUpsertRequest,
) -> WorkflowInsightResponse:
    """Create or replace the latest workflow insight for one phase."""
    _service.upsert_insight(payload)
    return _service.get_latest_insight(phase=payload.phase, producer=payload.producer)
