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

# CHAOS-4078: the PR-social and TestOps alias families. Unlike the work-item
# family (ATOMIC_CANONICAL -- every unit claims all five aliases
# unconditionally), these are non-atomic folds: only the datasets the org
# actually enabled contribute a window and receive a completion flag. See
# ``dev_health_ops.workers.provider_family_contract`` for the execution-mode
# distinction and ``sync.planner._build_fold_family_units``.
PR_SOCIAL_DATASETS: tuple[str, ...] = ("prs", "pr-reviews", "pr-comments")
PR_SOCIAL_CANONICAL_DATASET_KEY = PR_SOCIAL_DATASETS[0]

TESTOPS_DATASETS: tuple[str, ...] = ("cicd", "tests")
TESTOPS_CANONICAL_DATASET_KEY = TESTOPS_DATASETS[0]

# (canonical_dataset_key, member_datasets) for every collapsible family, in
# the order the planner should evaluate them. Used by sync-coverage scope
# matching and by any caller that needs to expand a canonical claim back to
# its member datasets without hardcoding the work-item family alone.
FOLD_FAMILIES: tuple[tuple[str, tuple[str, ...]], ...] = (
    (FAMILY_CANONICAL_DATASET_KEY, WORK_ITEM_DATASETS),
    (PR_SOCIAL_CANONICAL_DATASET_KEY, PR_SOCIAL_DATASETS),
    (TESTOPS_CANONICAL_DATASET_KEY, TESTOPS_DATASETS),
)

FAMILY_DATASET_FLAG_PREFIX = "family_dataset_"


def family_dataset_flag(dataset: str) -> str:
    return FAMILY_DATASET_FLAG_PREFIX + dataset.replace("-", "_")


def dataset_keys_from_flags(
    datasets: tuple[str, ...],
    processor_flags: Mapping[str, object] | None,
) -> list[str]:
    """Return the members of ``datasets`` whose ``family_dataset_*`` flag is
    true, in the given order. Generalizes ``family_dataset_keys_from_flags``
    to any collapsible family (CHAOS-4078)."""

    flags = processor_flags or {}
    return [
        dataset for dataset in datasets if bool(flags.get(family_dataset_flag(dataset)))
    ]


def family_dataset_keys_from_flags(
    processor_flags: Mapping[str, object] | None,
) -> list[str]:
    """Return enabled work-item-family datasets in canonical order.

    Kept scoped to ``WORK_ITEM_DATASETS`` only, for existing callers
    (``workers/sync_units.py``) that assume the work-item family
    specifically. New callers that need PR-social/TestOps expansion should
    use :func:`dataset_keys_from_flags` with the relevant family tuple.
    """

    return dataset_keys_from_flags(WORK_ITEM_DATASETS, processor_flags)
