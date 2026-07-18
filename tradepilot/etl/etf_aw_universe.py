"""Frozen ETF all-weather sleeve universe helpers."""

from __future__ import annotations

from datetime import date
from typing import Any

import pandas as pd

ETF_AW_SLEEVES: list[dict[str, Any]] = [
    {
        "sleeve_code": "510300.SH",
        "sleeve_role": "equity_large",
        "sleeve_name": "沪深300ETF华泰柏瑞",
        "listing_exchange": "SH",
        "benchmark_name": "沪深300指数",
        "list_date": date(2012, 5, 28),
        "exposure_note": "Large-cap China equity beta proxy.",
    },
    {
        "sleeve_code": "159845.SZ",
        "sleeve_role": "equity_small",
        "sleeve_name": "中证1000ETF华夏",
        "listing_exchange": "SZ",
        "benchmark_name": "中证1000指数收益率",
        "list_date": date(2021, 3, 31),
        "exposure_note": "Small-cap and higher-beta China equity proxy.",
    },
    {
        "sleeve_code": "513100.SH",
        "sleeve_role": "equity_overseas",
        "sleeve_name": "纳指ETF国泰",
        "listing_exchange": "SH",
        "benchmark_name": "纳斯达克100指数（人民币计价）",
        "list_date": date(2013, 5, 15),
        "exposure_note": (
            "US large-cap growth equity proxy traded in CNY; QDII quota, "
            "subscription suspension, market premium, FX, and overseas "
            "calendar risks can cause tracking divergence."
        ),
    },
    {
        "sleeve_code": "511010.SH",
        "sleeve_role": "bond",
        "sleeve_name": "国债ETF国泰",
        "listing_exchange": "SH",
        "benchmark_name": "上证5年期国债指数收益率",
        "list_date": date(2013, 3, 25),
        "exposure_note": (
            "Duration-bearing sovereign bond defense sleeve; not a universal "
            "bond factor or maximally convex crisis hedge."
        ),
    },
    {
        "sleeve_code": "518850.SH",
        "sleeve_role": "gold",
        "sleeve_name": "黄金ETF华夏",
        "listing_exchange": "SH",
        "benchmark_name": "上海黄金交易所黄金现货实盘合约Au99.99价格收益率",
        "list_date": date(2020, 6, 5),
        "exposure_note": "Gold hedge sleeve for inflation and stress diversification.",
    },
    {
        "sleeve_code": "159001.SZ",
        "sleeve_role": "cash",
        "sleeve_name": "货币ETF易方达",
        "listing_exchange": "SZ",
        "benchmark_name": "活期存款基准利率*(1-利息税税率)",
        "list_date": date(2014, 10, 20),
        "exposure_note": "Cash-like neutral buffer sleeve with low-volatility behavior.",
    },
]

ETF_AW_SLEEVE_ROLE_ORDER = [
    "equity_large",
    "equity_small",
    "equity_overseas",
    "bond",
    "gold",
    "cash",
]
ETF_AW_SLEEVE_ROLE_RANK = {
    role: rank for rank, role in enumerate(ETF_AW_SLEEVE_ROLE_ORDER)
}
ETF_AW_SLEEVE_CODES = [str(row["sleeve_code"]) for row in ETF_AW_SLEEVES]
ETF_AW_SLEEVE_ROLES = {str(row["sleeve_role"]) for row in ETF_AW_SLEEVES}
ETF_AW_SLEEVE_CODE_BY_ROLE = {
    str(row["sleeve_role"]): str(row["sleeve_code"]) for row in ETF_AW_SLEEVES
}


def etf_aw_sleeves_frame() -> pd.DataFrame:
    """Return frozen ETF all-weather sleeves in canonical role order."""

    return pd.DataFrame(ETF_AW_SLEEVES).sort_values(
        "sleeve_role", key=lambda series: series.map(ETF_AW_SLEEVE_ROLE_RANK)
    )


def etf_aw_sleeve_codes_frame() -> pd.DataFrame:
    """Return frozen ETF all-weather sleeve codes in canonical role order."""

    return pd.DataFrame({"sleeve_code": ETF_AW_SLEEVE_CODES})


def etf_aw_role_sort_key(series: pd.Series) -> pd.Series:
    """Return a stable sort key for frozen ETF all-weather sleeve roles."""

    return series.astype(str).map(ETF_AW_SLEEVE_ROLE_RANK).fillna(len(ETF_AW_SLEEVES))
