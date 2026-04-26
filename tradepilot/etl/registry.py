"""In-memory dataset registry for Stage A."""

from __future__ import annotations

from tradepilot.etl.datasets import DatasetDefinition, build_stage_b_datasets


class DatasetRegistry:
    """Process-local dataset registry."""

    def __init__(self) -> None:
        self._definitions: dict[str, DatasetDefinition] = {}

    def register_dataset(self, definition: DatasetDefinition) -> None:
        """Register one dataset definition by unique dataset name."""

        if definition.dataset_name in self._definitions:
            raise ValueError(f"dataset already registered: {definition.dataset_name}")
        self._definitions[definition.dataset_name] = definition

    def register_dataset_if_missing(self, definition: DatasetDefinition) -> None:
        """Register one dataset unless it is already present."""

        if definition.dataset_name not in self._definitions:
            self._definitions[definition.dataset_name] = definition

    def get_dataset(self, dataset_name: str) -> DatasetDefinition:
        """Return one dataset definition by name."""

        try:
            return self._definitions[dataset_name]
        except KeyError as exc:
            raise KeyError(f"unknown dataset: {dataset_name}") from exc

    def list_datasets(self) -> list[DatasetDefinition]:
        """Return all registered dataset definitions."""

        return list(self._definitions.values())

    def has_dataset(self, dataset_name: str) -> bool:
        """Return whether one dataset is registered."""

        return dataset_name in self._definitions


_REGISTRY = DatasetRegistry()


def register_dataset(definition: DatasetDefinition) -> None:
    """Register one dataset in the module-level registry."""

    _REGISTRY.register_dataset(definition)


def register_stage_b_datasets(registry: DatasetRegistry | None = None) -> None:
    """Register the built-in Stage B dataset definitions."""

    target = registry or _REGISTRY
    for definition in build_stage_b_datasets():
        target.register_dataset_if_missing(definition)


def get_dataset(dataset_name: str) -> DatasetDefinition:
    """Return one dataset from the module-level registry."""

    return _REGISTRY.get_dataset(dataset_name)


def list_datasets() -> list[DatasetDefinition]:
    """Return all datasets from the module-level registry."""

    return _REGISTRY.list_datasets()


def has_dataset(dataset_name: str) -> bool:
    """Return whether one dataset is in the module-level registry."""

    return _REGISTRY.has_dataset(dataset_name)
