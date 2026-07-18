"""Shared ETL constants."""

from __future__ import annotations

ETF_AW_ROLE_ORDER = (
    "equity_large",
    "equity_small",
    "equity_overseas",
    "bond",
    "gold",
    "cash",
)
ETF_AW_REQUIRED_ROLES = frozenset(ETF_AW_ROLE_ORDER)
ETF_AW_ROLE_RANK = {role: index for index, role in enumerate(ETF_AW_ROLE_ORDER)}
