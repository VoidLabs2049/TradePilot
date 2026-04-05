from fastapi import APIRouter

from tradepilot.scanner import DailyScanner
from tradepilot.scanner.daily import normalize_scan_date

router = APIRouter()
_scanner = DailyScanner()


@router.post("/scan/run")
def run_daily_scan(scan_date: str | None = None) -> dict:
    result = _scanner.run(scan_date=normalize_scan_date(scan_date))
    return result.model_dump()


@router.get("/scan/latest")
def get_latest_scan() -> dict:
    latest = _scanner.get_latest_scan()
    return latest or {"scan_date": None, "advice": []}


@router.get("/alerts")
def list_alerts(unread_only: bool = False) -> list[dict]:
    return _scanner.list_alerts(unread_only=unread_only)


@router.post("/alerts/{alert_id}/read")
def mark_alert_read(alert_id: int) -> dict:
    _scanner.mark_alert_read(alert_id)
    return {"status": "ok"}
