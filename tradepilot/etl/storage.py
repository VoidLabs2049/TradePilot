"""Lakehouse path planning helpers for Stage A."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from pathlib import Path

from tradepilot.config import (
    LAKEHOUSE_DERIVED_ROOT,
    LAKEHOUSE_NORMALIZED_ROOT,
    LAKEHOUSE_RAW_ROOT,
)
from tradepilot.etl.models import StorageZone
from tradepilot.etl.path_safety import validate_safe_path_component

PartitionValue = str | int
PartitionParts = Mapping[str, PartitionValue] | Sequence[tuple[str, PartitionValue]]


def ensure_zone_roots(lakehouse_root: Path | None = None) -> dict[StorageZone, Path]:
    """Create and return the top-level lakehouse zone directories."""

    roots = _zone_roots(lakehouse_root=lakehouse_root)
    for path in roots.values():
        path.mkdir(parents=True, exist_ok=True)
    return roots


def build_zone_path(
    dataset_name: str,
    zone: StorageZone,
    lakehouse_root: Path | None = None,
) -> Path:
    """Return the root directory for one dataset in one zone."""

    return _zone_roots(lakehouse_root=lakehouse_root)[zone] / _validate_dataset_name(
        dataset_name
    )


def build_partition_path(
    dataset_name: str,
    zone: StorageZone,
    partition_parts: PartitionParts,
    lakehouse_root: Path | None = None,
) -> Path:
    """Return one partition directory path for a dataset."""

    path = build_zone_path(
        dataset_name=dataset_name, zone=zone, lakehouse_root=lakehouse_root
    )
    for key, value in _normalize_partition_parts(partition_parts):
        partition_key = _validate_partition_key(key)
        partition_value = _validate_partition_value(value)
        path = path / f"{partition_key}={partition_value}"
    return path


def _zone_roots(lakehouse_root: Path | None = None) -> dict[StorageZone, Path]:
    """Return zone roots from either the configured or overridden root."""

    if lakehouse_root is None:
        return {
            StorageZone.RAW: LAKEHOUSE_RAW_ROOT,
            StorageZone.NORMALIZED: LAKEHOUSE_NORMALIZED_ROOT,
            StorageZone.DERIVED: LAKEHOUSE_DERIVED_ROOT,
        }
    return {
        StorageZone.RAW: lakehouse_root / StorageZone.RAW.value,
        StorageZone.NORMALIZED: lakehouse_root / StorageZone.NORMALIZED.value,
        StorageZone.DERIVED: lakehouse_root / StorageZone.DERIVED.value,
    }


def _normalize_partition_parts(
    partition_parts: PartitionParts,
) -> list[tuple[str, PartitionValue]]:
    """Return partition parts in deterministic order."""

    if isinstance(partition_parts, Mapping):
        return sorted(partition_parts.items())
    return list(partition_parts)


def _validate_dataset_name(dataset_name: str) -> str:
    """Reject dataset names that are not safe path components."""

    return validate_safe_path_component(dataset_name, "dataset_name")


def _validate_partition_key(partition_key: str) -> str:
    """Reject partition keys that are not safe path components."""

    return validate_safe_path_component(partition_key, "partition key")


def _validate_partition_value(partition_value: PartitionValue) -> str:
    """Reject partition values that are not safe path components."""

    return validate_safe_path_component(str(partition_value), "partition value")
