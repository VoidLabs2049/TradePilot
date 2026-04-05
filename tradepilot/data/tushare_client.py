from __future__ import annotations

import time
from datetime import date, datetime, timedelta
from typing import cast

import pandas as pd
import tushare as ts
from loguru import logger

from tradepilot.config import TUSHARE_TOKEN


_TRADE_CALENDAR_COLUMNS = ("exchange", "trade_date", "is_open", "pretrade_date")
_MARKET_DAILY_STATS_COLUMNS = (
    "trade_date",
    "market_code",
    "market_name",
    "listed_count",
    "total_share",
    "float_share",
    "total_mv",
    "float_mv",
    "amount",
    "vol",
    "trans_count",
    "pe",
    "turnover_rate",
)


def _empty_frame(columns: tuple[str, ...]) -> pd.DataFrame:
    return pd.DataFrame({column: pd.Series(dtype="object") for column in columns})


def _to_tushare_date(value: str) -> str:
    return value.replace("-", "")


def _to_date_str(value: object) -> str | None:
    if value in (None, ""):
        return None
    text = str(value).strip()
    for parser in ("%Y%m%d", "%Y-%m-%d"):
        try:
            return datetime.strptime(text, parser).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


class TushareClient:
    def __init__(self) -> None:
        self._pro = ts.pro_api(TUSHARE_TOKEN) if TUSHARE_TOKEN else None

    @property
    def enabled(self) -> bool:
        return self._pro is not None

    def get_trade_calendar(
        self,
        start_date: str,
        end_date: str,
        exchange: str = "SSE",
    ) -> pd.DataFrame:
        pro = self._pro
        if pro is None:
            return _empty_frame(_TRADE_CALENDAR_COLUMNS)
        logger.debug("tushare: fetch trade_cal {} {} {}", exchange, start_date, end_date)
        df = pro.trade_cal(
            exchange=exchange,
            start_date=_to_tushare_date(start_date),
            end_date=_to_tushare_date(end_date),
            fields="exchange,cal_date,is_open,pretrade_date",
        )
        if df.empty:
            return _empty_frame(_TRADE_CALENDAR_COLUMNS)
        normalized = df.rename(columns={"cal_date": "trade_date"}).copy()
        normalized["trade_date"] = pd.to_datetime(normalized["trade_date"], format="%Y%m%d", errors="coerce")
        normalized["pretrade_date"] = pd.to_datetime(
            normalized["pretrade_date"], format="%Y%m%d", errors="coerce"
        )
        normalized["is_open"] = normalized["is_open"].astype(int).astype(bool)
        return cast(pd.DataFrame, normalized.loc[:, list(_TRADE_CALENDAR_COLUMNS)].copy())

    def get_market_daily_stats(self, start_date: str, end_date: str) -> pd.DataFrame:
        pro = self._pro
        if pro is None:
            return _empty_frame(_MARKET_DAILY_STATS_COLUMNS)
        rows: list[pd.DataFrame] = []
        current = date.fromisoformat(start_date)
        end = date.fromisoformat(end_date)
        while current <= end:
            tushare_date = current.strftime("%Y%m%d")
            try:
                time.sleep(0.2)
                daily = pro.daily_info(
                    trade_date=tushare_date,
                    fields=(
                        "trade_date,ts_code,ts_name,com_count,total_share,float_share,"
                        "total_mv,float_mv,amount,vol,trans_count,pe,tr"
                    ),
                )
            except Exception as exc:
                logger.warning("tushare: daily_info failed for {}: {}", tushare_date, exc)
                current += timedelta(days=1)
                continue
            if not daily.empty:
                normalized = daily.rename(
                    columns={
                        "ts_code": "market_code",
                        "ts_name": "market_name",
                        "com_count": "listed_count",
                        "tr": "turnover_rate",
                    }
                ).copy()
                normalized["trade_date"] = pd.to_datetime(
                    normalized["trade_date"], format="%Y%m%d", errors="coerce"
                )
                rows.append(
                    cast(pd.DataFrame, normalized.loc[:, list(_MARKET_DAILY_STATS_COLUMNS)].copy())
                )
            current += timedelta(days=1)
        if not rows:
            return _empty_frame(_MARKET_DAILY_STATS_COLUMNS)
        return pd.concat(rows, ignore_index=True)

    def is_trading_day(self, target_date: str, exchange: str = "SSE") -> bool:
        frame = self.get_trade_calendar(target_date, target_date, exchange=exchange)
        if frame.empty:
            return date.fromisoformat(target_date).weekday() < 5
        return bool(frame.iloc[-1]["is_open"])

    def previous_trading_day(self, target_date: str, exchange: str = "SSE") -> str | None:
        frame = self.get_trade_calendar(target_date, target_date, exchange=exchange)
        if frame.empty:
            return None
        return _to_date_str(frame.iloc[-1].get("pretrade_date"))
