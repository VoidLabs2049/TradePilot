"""Structured data provider factory and exports."""

from tradepilot.config import DATA_PROVIDER, DataProviderType
from tradepilot.data.mock_provider import MOCK_ETFS, MOCK_INDICES, MOCK_STOCKS, MOCK_SECTORS, MockProvider
from tradepilot.data.provider import DataProvider

_provider: DataProvider | None = None


def get_provider() -> DataProvider:
    """Return the configured structured data provider (singleton)."""
    global _provider
    if _provider is not None:
        return _provider
    if DATA_PROVIDER == DataProviderType.AKSHARE:
        from tradepilot.data.akshare_provider import AKShareProvider

        _provider = AKShareProvider()
    else:
        _provider = MockProvider()
    return _provider


__all__ = [
    "DataProvider",
    "MOCK_ETFS",
    "MOCK_INDICES",
    "MOCK_SECTORS",
    "MOCK_STOCKS",
    "MockProvider",
    "get_provider",
]
