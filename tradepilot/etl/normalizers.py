"""Normalization contracts for the generic ETL foundation."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from pydantic import BaseModel, Field


class NormalizationResult(BaseModel):
    """Canonical rows and lineage metadata produced by one normalizer."""

    canonical_rows: list[dict[str, Any]] = Field(
        default_factory=list,
        description="Normalized records that conform to the dataset canonical schema.",
    )
    lineage_metadata: dict[str, Any] = Field(
        default_factory=dict,
        description="Source, transformation, and provenance details for the normalized rows.",
    )


class BaseNormalizer(ABC):
    """Base interface for dataset-specific normalizers."""

    @abstractmethod
    def normalize(
        self,
        raw_payload: Any,
        context: dict[str, Any] | None = None,
    ) -> NormalizationResult:
        """Transform raw payloads into canonical rows and lineage metadata."""

        raise NotImplementedError
