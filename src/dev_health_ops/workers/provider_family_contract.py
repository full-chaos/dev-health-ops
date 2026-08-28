"""Provider-neutral execution-family admission contracts.

This module describes relationships between persisted provider datasets without
changing their D16 execution boundary. Work-item providers use one atomic,
canonical claim. PagerDuty's incident quartet remains four independent claims;
cataloguing it here must never collapse or reject those units.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum

from dev_health_ops.sync.family_flags import (
    FAMILY_DATASET_FLAG_PREFIX,
    PR_SOCIAL_DATASETS,
    TESTOPS_DATASETS,
    WORK_ITEM_DATASETS,
    family_dataset_flag,
)


class FamilyExecutionMode(StrEnum):
    ATOMIC_CANONICAL = "atomic_canonical"
    INDEPENDENT = "independent"
    # CHAOS-4078: an alias-only selection folds onto its canonical writer, but
    # -- unlike ATOMIC_CANONICAL -- membership is not all-or-nothing. Only the
    # datasets the org actually enabled contribute a window and a completion
    # flag; a caught-up or never-enabled sibling is never forced along.
    FOLD_CONTRIBUTING = "fold_contributing"


@dataclass(frozen=True, slots=True)
class ProviderFamilyPolicy:
    providers: frozenset[str]
    canonical_dataset: str
    datasets: tuple[str, ...]
    mode: FamilyExecutionMode

    def applies_to(self, provider: str, dataset: str) -> bool:
        return provider in self.providers and dataset in self.datasets


PAGERDUTY_INCIDENT_DATASETS = (
    "incidents",
    "incident-alerts",
    "incident-log-entries",
    "incident-notes",
)

_POLICIES = (
    ProviderFamilyPolicy(
        providers=frozenset({"github", "gitlab", "jira", "linear"}),
        canonical_dataset="work-items",
        datasets=WORK_ITEM_DATASETS,
        mode=FamilyExecutionMode.ATOMIC_CANONICAL,
    ),
    ProviderFamilyPolicy(
        providers=frozenset({"pagerduty"}),
        canonical_dataset="incidents",
        datasets=PAGERDUTY_INCIDENT_DATASETS,
        mode=FamilyExecutionMode.INDEPENDENT,
    ),
    ProviderFamilyPolicy(
        providers=frozenset({"github", "gitlab"}),
        canonical_dataset="prs",
        datasets=PR_SOCIAL_DATASETS,
        mode=FamilyExecutionMode.FOLD_CONTRIBUTING,
    ),
    ProviderFamilyPolicy(
        providers=frozenset({"github", "gitlab"}),
        canonical_dataset="cicd",
        datasets=TESTOPS_DATASETS,
        mode=FamilyExecutionMode.FOLD_CONTRIBUTING,
    ),
)


def provider_family_policy(provider: str, dataset: str) -> ProviderFamilyPolicy | None:
    normalized_provider = provider.strip().lower()
    normalized_dataset = dataset.strip().lower()
    for policy in _POLICIES:
        if policy.applies_to(normalized_provider, normalized_dataset):
            return policy
    return None


def validate_provider_family_claim(
    provider: str,
    dataset: str,
    processor_flags: Mapping[str, object] | None,
    *,
    strict_atomic: bool,
) -> bool:
    """Validate one claim against its family's execution mode.

    Only the canonical claim carrying every declared family flag as the literal
    boolean ``True`` is admissible for an ATOMIC_CANONICAL family. Unknown
    ``family_dataset_*`` flags fail closed. INDEPENDENT families preserve their
    existing per-dataset claim shape under D16, and FOLD_CONTRIBUTING families
    (CHAOS-4078: PR-social, TestOps) admit a canonical claim carrying any
    non-empty subset of its declared flags -- unlike ATOMIC_CANONICAL, a
    partial fold is exactly the intended shape, not a malformed claim.

    ``strict_atomic`` remains a parameter so a caller can validate a claim
    without asserting atomicity (the capability contract still describes both
    shapes). Every production caller passes ``True``: CHAOS-4054 removed the
    route switches that used to make strictness a deployment property.
    """

    policy = provider_family_policy(provider, dataset)
    if (
        policy is None
        or policy.mode is not FamilyExecutionMode.ATOMIC_CANONICAL
        or not strict_atomic
    ):
        return True
    normalized_dataset = dataset.strip().lower()
    if normalized_dataset != policy.canonical_dataset:
        return False
    expected = frozenset(family_dataset_flag(item) for item in policy.datasets)
    flags = processor_flags or {}
    if any(
        name.startswith(FAMILY_DATASET_FLAG_PREFIX) and name not in expected
        for name in flags
    ):
        return False
    return all(flags.get(name) is True for name in expected)
