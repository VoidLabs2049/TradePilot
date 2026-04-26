"""Base source adapter contract for the generic ETL foundation."""

from __future__ import annotations

from abc import ABC, abstractmethod
from enum import StrEnum
from tradepilot.etl.models import IngestionRequest, SourceFetchResult


class SourceRole(StrEnum):
    """Supported source roles in the ETL framework."""

    PRIMARY = "primary"
    FALLBACK = "fallback"
    VALIDATION = "validation"


class BaseSourceAdapter(ABC):
    """Base interface for dataset-aware source adapters."""

    source_name: str
    source_role: SourceRole

    @abstractmethod
    def supports_dataset(self, dataset_name: str) -> bool:
        """Return whether this adapter can fetch one dataset."""

        raise NotImplementedError

    @abstractmethod
    def fetch(self, dataset_name: str, request: IngestionRequest) -> SourceFetchResult:
        """Fetch raw data for one dataset."""

        raise NotImplementedError
