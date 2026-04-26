"""Source adapter contracts for the generic ETL foundation."""

from tradepilot.etl.sources.base import BaseSourceAdapter, SourceRole
from tradepilot.etl.sources.tushare import TushareSourceAdapter

__all__ = ["BaseSourceAdapter", "SourceRole", "TushareSourceAdapter"]
