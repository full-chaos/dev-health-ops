"""Unit coverage for ``dev_health_ops.sync.family_flags`` (CHAOS-4078).

Dedicated file because this module is a pure, dependency-free helper with
no prior test home -- see the module docstring for why it must stay that
way.
"""

from __future__ import annotations

from dev_health_ops.sync.family_flags import (
    PR_SOCIAL_DATASETS,
    dataset_keys_from_flags,
    family_dataset_flag,
)


def test_dataset_keys_from_flags_requires_the_literal_boolean_true() -> None:
    """Codex round 3 finding #3: a truthy-but-not-``True`` value (e.g. a
    malformed ``"family_dataset_pr_comments": "false"`` string, itself
    truthy in Python) must never be read as "this dataset ran" -- that
    would report requested or covered work for a unit that never touched
    it. Only the literal boolean ``True`` counts as enabled.
    """

    processor_flags = {
        family_dataset_flag("pr-reviews"): True,
        # A malformed non-boolean truthy value must NOT expand.
        family_dataset_flag("pr-comments"): "false",
        # An explicit False must NOT expand either.
        family_dataset_flag("prs"): False,
    }

    assert dataset_keys_from_flags(PR_SOCIAL_DATASETS, processor_flags) == [
        "pr-reviews",
    ]


def test_dataset_keys_from_flags_treats_missing_flags_as_disabled() -> None:
    assert dataset_keys_from_flags(PR_SOCIAL_DATASETS, None) == []
    assert dataset_keys_from_flags(PR_SOCIAL_DATASETS, {}) == []
