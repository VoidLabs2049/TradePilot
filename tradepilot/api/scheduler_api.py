from fastapi import APIRouter

from tradepilot.scheduler.engine import scheduler_status
from tradepilot.scheduler.jobs import get_scheduler_history

router = APIRouter()


@router.get("/status")
def get_status() -> dict:
    return scheduler_status()


@router.get("/history")
def get_history(limit: int = 20) -> list[dict]:
    return get_scheduler_history(limit=limit)
