"""Path component safety helpers for ETL lakehouse storage."""

from __future__ import annotations

from pathlib import PurePath


def validate_safe_path_component(value: str, field_name: str) -> str:
    """Reject values that are not safe single path components."""

    stripped = value.strip()
    path = PurePath(stripped)
    if (
        not stripped
        or stripped != value
        or path.is_absolute()
        or any(part in {"", ".", ".."} for part in path.parts)
        or len(path.parts) != 1
    ):
        raise ValueError(f"{field_name} must be a single safe path component")
    return stripped
