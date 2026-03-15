"""Simple TTL in-memory cache for market snapshot data."""

from __future__ import annotations

import time
from typing import Any


class SnapshotCache:
    """Thread-safe TTL cache for avoiding repeated akshare API calls.

    Each entry expires after ``ttl_seconds``. Expired entries are lazily
    evicted on the next ``get`` call for that key.
    """

    def __init__(self, ttl_seconds: int = 60) -> None:
        self._ttl = ttl_seconds
        self._store: dict[str, tuple[float, Any]] = {}

    def get(self, key: str) -> Any | None:
        """Return cached value or ``None`` if missing/expired."""
        entry = self._store.get(key)
        if entry is None:
            return None
        ts, value = entry
        if time.time() - ts > self._ttl:
            del self._store[key]
            return None
        return value

    def set(self, key: str, value: Any) -> None:
        """Store a value with current timestamp."""
        self._store[key] = (time.time(), value)
