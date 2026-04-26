"""Lakehouse path planning and write helpers for the ETL foundation."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
import hashlib
import os
from pathlib import Path

import pandas as pd

from tradepilot.config import (
    LAKEHOUSE_ROOT,
    LAKEHOUSE_DERIVED_ROOT,
    LAKEHOUSE_NORMALIZED_ROOT,
    LAKEHOUSE_RAW_ROOT,
)
from tradepilot.etl.models import StorageZone
from tradepilot.etl.path_safety import validate_safe_path_component

PartitionValue = str | int
PartitionParts = Mapping[str, PartitionValue] | Sequence[tuple[str, PartitionValue]]


@dataclass(frozen=True)
class ParquetWriteResult:
    """Result of one parquet write."""

    path: Path
    relative_path: str
    row_count: int
    content_hash: str


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
        _validate_partition_key(key)
        partition_value = _validate_partition_value(value)
        path = path / partition_value
    return path


def build_raw_batch_path(
    dataset_name: str,
    partition_parts: PartitionParts,
    raw_batch_id: int,
    lakehouse_root: Path | None = None,
) -> Path:
    """Return the final raw batch parquet path."""

    if raw_batch_id <= 0:
        raise ValueError("raw_batch_id must be positive")
    return (
        build_partition_path(
            dataset_name=dataset_name,
            zone=StorageZone.RAW,
            partition_parts=partition_parts,
            lakehouse_root=lakehouse_root,
        )
        / f"batch-{raw_batch_id}.parquet"
    )


def build_normalized_file_path(
    dataset_name: str,
    partition_parts: PartitionParts,
    lakehouse_root: Path | None = None,
) -> Path:
    """Return the single canonical normalized parquet file for a partition."""

    return (
        build_partition_path(
            dataset_name=dataset_name,
            zone=StorageZone.NORMALIZED,
            partition_parts=partition_parts,
            lakehouse_root=lakehouse_root,
        )
        / "part-00000.parquet"
    )


def write_raw_parquet(
    frame: pd.DataFrame,
    dataset_name: str,
    partition_parts: PartitionParts,
    raw_batch_id: int,
    lakehouse_root: Path | None = None,
) -> ParquetWriteResult:
    """Write one immutable raw parquet batch and return its manifest details."""

    final_path = build_raw_batch_path(
        dataset_name=dataset_name,
        partition_parts=partition_parts,
        raw_batch_id=raw_batch_id,
        lakehouse_root=lakehouse_root,
    )
    return _write_parquet_atomic(frame, final_path, lakehouse_root=lakehouse_root)


def write_normalized_parquet(
    frame: pd.DataFrame,
    dataset_name: str,
    partition_parts: PartitionParts,
    lakehouse_root: Path | None = None,
) -> ParquetWriteResult:
    """Write one normalized partition with tmp-write then atomic replace."""

    final_path = build_normalized_file_path(
        dataset_name=dataset_name,
        partition_parts=partition_parts,
        lakehouse_root=lakehouse_root,
    )
    return _write_parquet_atomic(frame, final_path, lakehouse_root=lakehouse_root)


def cleanup_temp_files(dataset_name: str, lakehouse_root: Path | None = None) -> None:
    """Remove leftover Stage B temporary parquet files for one dataset."""

    for zone in (StorageZone.RAW, StorageZone.NORMALIZED):
        root = build_zone_path(
            dataset_name=dataset_name, zone=zone, lakehouse_root=lakehouse_root
        )
        if not root.exists():
            continue
        for path in root.rglob("*.tmp"):
            if path.is_file():
                path.unlink()


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


def _write_parquet_atomic(
    frame: pd.DataFrame,
    final_path: Path,
    lakehouse_root: Path | None,
) -> ParquetWriteResult:
    """Write a parquet file through a same-directory temporary file."""

    final_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = final_path.with_name(f".{final_path.name}.{os.getpid()}.tmp")
    try:
        frame.to_parquet(tmp_path, index=False)
        os.replace(tmp_path, final_path)
    finally:
        if tmp_path.exists():
            tmp_path.unlink()
    content_hash = _sha256_file(final_path)
    return ParquetWriteResult(
        path=final_path,
        relative_path=_relative_to_lakehouse(final_path, lakehouse_root),
        row_count=len(frame),
        content_hash=content_hash,
    )


def _relative_to_lakehouse(path: Path, lakehouse_root: Path | None) -> str:
    """Return a POSIX path relative to the lakehouse root."""

    root = lakehouse_root or LAKEHOUSE_ROOT
    return path.relative_to(root).as_posix()


def _sha256_file(path: Path) -> str:
    """Return the SHA-256 hash of one file."""

    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _normalize_partition_parts(
    partition_parts: PartitionParts,
) -> list[tuple[str, PartitionValue]]:
    """Return partition parts in declared order."""

    if isinstance(partition_parts, Mapping):
        return list(partition_parts.items())
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
