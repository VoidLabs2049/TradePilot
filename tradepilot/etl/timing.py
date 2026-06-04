"""Timing helpers for ETL release and effective-date rules."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date, timedelta
from typing import Any

import pandas as pd


def next_common_open_date(
    calendar: pd.DataFrame,
    target_date: date,
    exchanges: Sequence[str] = ("SH", "SZ"),
) -> date | None:
    """Return the first date on or after target_date open for every exchange."""

    required = {str(exchange).upper() for exchange in exchanges}
    if calendar.empty or not required:
        return None
    frame = calendar.copy()
    if "trade_date" not in frame.columns or "exchange" not in frame.columns:
        return None
    if "is_open" not in frame.columns:
        frame["is_open"] = True
    frame["exchange"] = frame["exchange"].astype(str).str.upper()
    frame["trade_date"] = pd.to_datetime(frame["trade_date"], errors="coerce").dt.date
    frame = frame[
        frame["trade_date"].notna()
        & frame["trade_date"].ge(target_date)
        & frame["exchange"].isin(required)
        & frame["is_open"].eq(True)
    ].copy()
    if frame.empty:
        return None
    for trade_date, date_frame in frame.sort_values("trade_date").groupby("trade_date"):
        if set(date_frame["exchange"].dropna().tolist()) >= required:
            return trade_date
    return None


def next_common_open_date_from_conn(
    conn: Any,
    target_date: date,
    exchanges: Sequence[str] = ("SH", "SZ"),
    search_days: int = 31,
) -> date | None:
    """Load canonical calendar rows and return the next common open date."""

    end_date = target_date + timedelta(days=search_days)
    frame = conn.execute(
        """
        SELECT exchange, trade_date, is_open
        FROM canonical_trading_calendar
        WHERE trade_date BETWEEN ? AND ?
          AND exchange IN (?, ?)
        ORDER BY trade_date, exchange
        """,
        [target_date, end_date, exchanges[0], exchanges[1]],
    ).fetchdf()
    return next_common_open_date(frame, target_date, exchanges=exchanges)


def monthly_day(period_label: str, day: int) -> date:
    """Return a calendar date for a YYYY-MM period label and day number."""

    year_text, month_text = period_label.split("-", 1)
    return date(int(year_text), int(month_text), day)
