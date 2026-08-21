"""Pure work-item-family processor flag helpers.

This module must remain safe to import from read-only API projection builders;
it deliberately has no planner, provider, or worker runtime dependencies.
"""

from __future__ import annotations

from collections.abc import Mapping

WORK_ITEM_DATASETS: tuple[str, ...] = (
    "work-items",
    "work-item-labels",
    "work-item-projects",
    "work-item-history",
    "work-item-comments",
)

# The one family member the alias-collapse machinery emits as a persisted
# unit; every other member of WORK_ITEM_DATASETS is a planner-collapsed
# alias, never a direct claim.
FAMILY_CANONICAL_DATASET_KEY = WORK_ITEM_DATASETS[0]

FAMILY_DATASET_FLAG_PREFIX = "family_dataset_"


def family_dataset_flag(dataset: str) -> str:
    return FAMILY_DATASET_FLAG_PREFIX + dataset.replace("-", "_")


def family_dataset_keys_from_flags(
    processor_flags: Mapping[str, object] | None,
) -> list[str]:
    """Return enabled work-item-family datasets in canonical order."""

    flags = processor_flags or {}
    return [
        dataset
        for dataset in WORK_ITEM_DATASETS
        if bool(flags.get(family_dataset_flag(dataset)))
    ]
