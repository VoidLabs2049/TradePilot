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


def _empty_with_columns(*columns: str) -> pd.DataFrame:
    return pd.DataFrame({column: pd.Series(dtype="object") for column in columns})


def _to_tushare_date(value: str) -> str:
    return value.replace("-", "")


def _with_exchange_suffix(code: str, *, kind: str) -> str:
    normalized = code.strip().upper()
    if "." in normalized:
        return normalized
    if kind == "index":
        exchange = "SZ" if normalized.startswith("399") else "SH"
    elif kind == "fund":
        exchange = "SH" if normalized.startswith(("5", "6")) else "SZ"
    else:
        if normalized.startswith(("6", "9")):
            exchange = "SH"
        elif normalized.startswith(("4", "8")):
            exchange = "BJ"
        else:
            exchange = "SZ"
    return f"{normalized}.{exchange}"


def _normalize_quotes(
    df: pd.DataFrame, code_column: str, code_value: str
) -> pd.DataFrame:
    normalized = df.rename(
        columns={
            "trade_date": "date",
            "ts_code": code_column,
            "vol": "volume",
        }
    ).copy()
    normalized[code_column] = code_value
    normalized["date"] = pd.to_datetime(
        normalized["date"], format="%Y%m%d", errors="coerce"
    )
    return normalized.sort_values("date").reset_index(drop=True)


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

    def get_stock_catalog(self) -> pd.DataFrame:
        pro = self._pro
        if pro is None:
            return _empty_with_columns("code", "name")
        stocks = pro.stock_basic(exchange="", list_status="L", fields="symbol,name")
        if stocks.empty:
            return _empty_with_columns("code", "name")
        normalized = stocks.rename(columns={"symbol": "code", "name": "name"}).copy()
        return cast(
            pd.DataFrame,
            normalized.loc[:, ["code", "name"]]
            .drop_duplicates()
            .reset_index(drop=True),
        )

    def get_index_catalog(self) -> pd.DataFrame:
        pro = self._pro
        if pro is None:
            return _empty_with_columns("code", "name", "list_date", "delist_date")
        frames: list[pd.DataFrame] = []
        for market in ("SSE", "SZSE"):
            frame = pro.index_basic(
                market=market, fields="ts_code,name,list_date,delist_date"
            )
            if frame.empty:
                continue
            normalized = frame.rename(
                columns={"ts_code": "code", "name": "name"}
            ).copy()
            normalized["code"] = normalized["code"].str.split(".").str[0]
            if "delist_date" not in normalized.columns:
                normalized["delist_date"] = None
            frames.append(
                cast(
                    pd.DataFrame,
                    normalized.loc[:, ["code", "name", "list_date", "delist_date"]],
                )
            )
        if not frames:
            return _empty_with_columns("code", "name", "list_date", "delist_date")
        return cast(
            pd.DataFrame,
            pd.concat(frames, ignore_index=True)
            .drop_duplicates()
            .reset_index(drop=True),
        )

    def get_etf_catalog(self) -> pd.DataFrame:
        pro = self._pro
        if pro is None:
            return _empty_with_columns("code", "name", "list_date", "delist_date")
        frame = pro.fund_basic(
            market="E",
            fields="ts_code,name,list_date,delist_date",
        )
        if frame.empty:
            return _empty_with_columns("code", "name", "list_date", "delist_date")
        normalized = frame.rename(columns={"ts_code": "code"}).copy()
        return cast(
            pd.DataFrame,
            normalized.loc[:, ["code", "name", "list_date", "delist_date"]]
            .drop_duplicates()
            .reset_index(drop=True),
        )

    def get_trade_calendar(
        self,
        start_date: str,
        end_date: str,
        exchange: str = "SSE",
    ) -> pd.DataFrame:
        pro = self._pro
        if pro is None:
            return _empty_frame(_TRADE_CALENDAR_COLUMNS)
        logger.debug(
            "tushare: fetch trade_cal {} {} {}", exchange, start_date, end_date
        )
        df = pro.trade_cal(
            exchange=exchange,
            start_date=_to_tushare_date(start_date),
            end_date=_to_tushare_date(end_date),
            fields="exchange,cal_date,is_open,pretrade_date",
        )
        if df.empty:
            return _empty_frame(_TRADE_CALENDAR_COLUMNS)
        normalized = df.rename(columns={"cal_date": "trade_date"}).copy()
        normalized["trade_date"] = pd.to_datetime(
            normalized["trade_date"], format="%Y%m%d", errors="coerce"
        )
        normalized["pretrade_date"] = pd.to_datetime(
            normalized["pretrade_date"], format="%Y%m%d", errors="coerce"
        )
        normalized["is_open"] = normalized["is_open"].astype(int).astype(bool)
        return cast(
            pd.DataFrame, normalized.loc[:, list(_TRADE_CALENDAR_COLUMNS)].copy()
        )

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
                logger.warning(
                    "tushare: daily_info failed for {}: {}", tushare_date, exc
                )
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
                    cast(
                        pd.DataFrame,
                        normalized.loc[:, list(_MARKET_DAILY_STATS_COLUMNS)].copy(),
                    )
                )
            current += timedelta(days=1)
        if not rows:
            return _empty_frame(_MARKET_DAILY_STATS_COLUMNS)
        return pd.concat(rows, ignore_index=True)

    def get_stock_daily(
        self, stock_code: str, start_date: str, end_date: str
    ) -> pd.DataFrame:
        pro = self._pro
        if pro is None:
            return _empty_with_columns(
                "date",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "amount",
                "stock_code",
                "turnover",
            )
        symbol = _with_exchange_suffix(stock_code, kind="stock")
        daily = pro.daily(
            ts_code=symbol,
            start_date=_to_tushare_date(start_date),
            end_date=_to_tushare_date(end_date),
            fields="ts_code,trade_date,open,high,low,close,vol,amount",
        )
        if daily.empty and stock_code.startswith(("1", "5")):
            daily = pro.fund_daily(
                ts_code=_with_exchange_suffix(stock_code, kind="fund"),
                start_date=_to_tushare_date(start_date),
                end_date=_to_tushare_date(end_date),
                fields="ts_code,trade_date,open,high,low,close,vol,amount",
            )
        if daily.empty:
            return _empty_with_columns(
                "date",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "amount",
                "stock_code",
                "turnover",
            )
        basic = pro.daily_basic(
            ts_code=symbol,
            start_date=_to_tushare_date(start_date),
            end_date=_to_tushare_date(end_date),
            fields="trade_date,turnover_rate",
        )
        normalized = _normalize_quotes(daily, "stock_code", stock_code)
        if basic.empty:
            normalized["turnover"] = None
            return normalized.loc[
                :,
                [
                    "date",
                    "stock_code",
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume",
                    "amount",
                    "turnover",
                ],
            ]
        turnover = basic.rename(
            columns={"trade_date": "date", "turnover_rate": "turnover"}
        ).copy()
        turnover["date"] = pd.to_datetime(
            turnover["date"], format="%Y%m%d", errors="coerce"
        )
        merged = normalized.merge(turnover, on="date", how="left")
        return merged.loc[
            :,
            [
                "date",
                "stock_code",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "amount",
                "turnover",
            ],
        ]

    def get_stock_weekly(
        self, stock_code: str, start_date: str, end_date: str
    ) -> pd.DataFrame:
        daily = self.get_stock_daily(stock_code, start_date, end_date)
        if daily.empty:
            return daily
        weekly = cast(
            pd.DataFrame,
            daily.set_index("date")
            .resample("W-FRI")
            .agg(
                {
                    "stock_code": "last",
                    "open": "first",
                    "high": "max",
                    "low": "min",
                    "close": "last",
                    "volume": "sum",
                    "amount": "sum",
                    "turnover": "mean",
                }
            )
            .reset_index(),
        )
        weekly = weekly.dropna(subset=["stock_code", "open", "high", "low", "close"])
        return weekly

    def get_stock_monthly(
        self, stock_code: str, start_date: str, end_date: str
    ) -> pd.DataFrame:
        daily = self.get_stock_daily(stock_code, start_date, end_date)
        if daily.empty:
            return daily
        monthly = cast(
            pd.DataFrame,
            daily.set_index("date")
            .resample("ME")
            .agg(
                {
                    "stock_code": "last",
                    "open": "first",
                    "high": "max",
                    "low": "min",
                    "close": "last",
                    "volume": "sum",
                    "amount": "sum",
                    "turnover": "mean",
                }
            )
            .reset_index(),
        )
        monthly = monthly.dropna(subset=["stock_code", "open", "high", "low", "close"])
        return monthly

    def get_index_daily(
        self, index_code: str, start_date: str, end_date: str
    ) -> pd.DataFrame:
        pro = self._pro
        if pro is None:
            return _empty_with_columns(
                "date", "open", "high", "low", "close", "volume", "amount", "index_code"
            )
        daily = pro.index_daily(
            ts_code=_with_exchange_suffix(index_code, kind="index"),
            start_date=_to_tushare_date(start_date),
            end_date=_to_tushare_date(end_date),
            fields="ts_code,trade_date,open,high,low,close,vol,amount",
        )
        if daily.empty:
            return _empty_with_columns(
                "date", "open", "high", "low", "close", "volume", "amount", "index_code"
            )
        normalized = _normalize_quotes(daily, "index_code", index_code)
        return normalized.loc[
            :,
            ["date", "index_code", "open", "high", "low", "close", "volume", "amount"],
        ]

    def get_etf_daily(
        self, etf_code: str, start_date: str, end_date: str
    ) -> pd.DataFrame:
        pro = self._pro
        if pro is None:
            return _empty_with_columns(
                "date", "open", "high", "low", "close", "volume", "amount", "etf_code"
            )
        daily = pro.fund_daily(
            ts_code=_with_exchange_suffix(etf_code, kind="fund"),
            start_date=_to_tushare_date(start_date),
            end_date=_to_tushare_date(end_date),
            fields="ts_code,trade_date,open,high,low,close,pre_close,change,pct_chg,vol,amount",
        )
        if daily.empty:
            return _empty_with_columns(
                "date",
                "open",
                "high",
                "low",
                "close",
                "pre_close",
                "change",
                "pct_chg",
                "volume",
                "amount",
                "etf_code",
            )
        normalized = _normalize_quotes(daily, "etf_code", etf_code)
        return normalized.loc[
            :,
            [
                "date",
                "etf_code",
                "open",
                "high",
                "low",
                "close",
                "pre_close",
                "change",
                "pct_chg",
                "volume",
                "amount",
            ],
        ]

    def get_margin_data(self, start_date: str, end_date: str) -> pd.DataFrame:
        pro = self._pro
        if pro is None:
            return _empty_with_columns(
                "date", "stock_code", "margin_balance", "margin_buy"
            )
        rows: list[pd.DataFrame] = []
        current = date.fromisoformat(start_date)
        end = date.fromisoformat(end_date)
        while current <= end:
            trade_date = current.strftime("%Y%m%d")
            for exchange_id in ("SSE", "SZSE"):
                time.sleep(0.2)
                daily = pro.margin(trade_date=trade_date, exchange_id=exchange_id)
                if daily.empty:
                    continue
                normalized = daily.rename(
                    columns={
                        "trade_date": "date",
                        "rzye": "margin_balance",
                        "rzmre": "margin_buy",
                    }
                ).copy()
                normalized["stock_code"] = exchange_id
                normalized["date"] = pd.to_datetime(
                    normalized["date"], format="%Y%m%d", errors="coerce"
                )
                rows.append(
                    normalized.loc[
                        :, ["date", "stock_code", "margin_balance", "margin_buy"]
                    ]
                )
            current += timedelta(days=1)
        if not rows:
            return _empty_with_columns(
                "date", "stock_code", "margin_balance", "margin_buy"
            )
        return pd.concat(rows, ignore_index=True)

    def get_northbound_flow(self, start_date: str, end_date: str) -> pd.DataFrame:
        pro = self._pro
        if pro is None:
            return _empty_with_columns("date", "net_buy", "buy_amount", "sell_amount")
        daily = pro.moneyflow_hsgt(
            start_date=_to_tushare_date(start_date),
            end_date=_to_tushare_date(end_date),
            fields="trade_date,hgt,sgt,north_money",
        )
        if daily.empty:
            return _empty_with_columns("date", "net_buy", "buy_amount", "sell_amount")
        normalized = daily.rename(
            columns={
                "trade_date": "date",
                "north_money": "net_buy",
                "hgt": "buy_amount",
                "sgt": "sell_amount",
            }
        ).copy()
        normalized["date"] = pd.to_datetime(
            normalized["date"], format="%Y%m%d", errors="coerce"
        )
        return (
            normalized.loc[:, ["date", "net_buy", "buy_amount", "sell_amount"]]
            .sort_values("date")
            .reset_index(drop=True)
        )

    def get_stock_valuation(
        self, stock_code: str, start_date: str, end_date: str
    ) -> pd.DataFrame:
        pro = self._pro
        if pro is None:
            return _empty_with_columns(
                "stock_code", "date", "pe_ttm", "pb", "ps", "market_cap"
            )
        daily = pro.daily_basic(
            ts_code=_with_exchange_suffix(stock_code, kind="stock"),
            start_date=_to_tushare_date(start_date),
            end_date=_to_tushare_date(end_date),
            fields="ts_code,trade_date,pe_ttm,pb,ps,ps_ttm,total_mv",
        )
        if daily.empty:
            return _empty_with_columns(
                "stock_code", "date", "pe_ttm", "pb", "ps", "market_cap"
            )
        normalized = daily.rename(
            columns={
                "trade_date": "date",
                "pe_ttm": "pe_ttm",
                "pb": "pb",
                "ps": "ps",
                "ps_ttm": "ps_ttm",
                "total_mv": "market_cap",
            }
        ).copy()
        normalized["date"] = pd.to_datetime(
            normalized["date"], format="%Y%m%d", errors="coerce"
        )
        normalized["stock_code"] = stock_code
        if "ps" not in normalized.columns and "ps_ttm" in normalized.columns:
            normalized["ps"] = normalized["ps_ttm"]
        normalized["ps"] = normalized["ps"].where(
            normalized["ps"].notna(), normalized.get("ps_ttm")
        )
        return (
            normalized.loc[
                :, ["stock_code", "date", "pe_ttm", "pb", "ps", "market_cap"]
            ]
            .sort_values("date")
            .reset_index(drop=True)
        )

    def is_trading_day(self, target_date: str, exchange: str = "SSE") -> bool:
        frame = self.get_trade_calendar(target_date, target_date, exchange=exchange)
        if frame.empty:
            return date.fromisoformat(target_date).weekday() < 5
        return bool(frame.iloc[-1]["is_open"])

    def previous_trading_day(
        self, target_date: str, exchange: str = "SSE"
    ) -> str | None:
        frame = self.get_trade_calendar(target_date, target_date, exchange=exchange)
        if frame.empty:
            return None
        return _to_date_str(frame.iloc[-1].get("pretrade_date"))
