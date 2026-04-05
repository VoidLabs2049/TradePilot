import os
from enum import StrEnum
from pathlib import Path


def _load_dotenv() -> dict[str, str]:
    env_path = Path(__file__).parent.parent / ".env"
    if not env_path.exists():
        return {}
    values: dict[str, str] = {}
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            values[key] = value
    return values


_DOTENV_VALUES = _load_dotenv()


def _env(name: str, default: str | None = None) -> str | None:
    return os.environ.get(name, _DOTENV_VALUES.get(name, default))


class DataProviderType(StrEnum):
    """Supported structured market data providers."""

    MOCK = "mock"
    AKSHARE = "akshare"


DB_PATH = Path(__file__).parent.parent / "data" / "tradepilot.duckdb"
DATA_PROVIDER = DataProviderType.MOCK
DATA_ROOT = Path(__file__).parent.parent / "data"
BILIBILI_STORAGE_PATH = DATA_ROOT / "bilibili"
TUSHARE_TOKEN: str | None = _env("TUSHARE_TOKEN")
TUSHARE_ENABLED: bool = bool(TUSHARE_TOKEN)

# When True, AKShareProvider silently falls back to MockProvider on API errors.
# When False, API errors propagate as exceptions.
AKSHARE_FALLBACK_ENABLED = True
