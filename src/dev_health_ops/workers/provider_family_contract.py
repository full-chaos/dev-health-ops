"""Provider-neutral execution-family admission contracts.

This module describes relationships between persisted provider datasets without
changing their D16 execution boundary. Work-item providers use one atomic,
canonical claim once their Go family switch is enabled. PagerDuty's incident
quartet remains four independent claims; cataloguing it here must never collapse
or reject those units.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum

from dev_health_ops.sync.family_flags import (
    FAMILY_DATASET_FLAG_PREFIX,
    WORK_ITEM_DATASETS,
    family_dataset_flag,
)


class FamilyExecutionMode(StrEnum):
    ATOMIC_CANONICAL = "atomic_canonical"
    INDEPENDENT = "independent"


@dataclass(frozen=True, slots=True)
class ProviderFamilyPolicy:
    providers: frozenset[str]
    canonical_dataset: str
    datasets: tuple[str, ...]
    mode: FamilyExecutionMode
    switch_environment_names: tuple[tuple[str, str], ...] = ()

    def applies_to(self, provider: str, dataset: str) -> bool:
        return provider in self.providers and dataset in self.datasets

    def switch_environment_name(self, provider: str) -> str | None:
        normalized_provider = provider.strip().lower()
        return next(
            (
                name
                for policy_provider, name in self.switch_environment_names
                if policy_provider == normalized_provider
            ),
            None,
        )


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
        switch_environment_names=(
            ("github", "WORKER_GITHUB_WORK_ITEMS_ENABLED"),
            ("jira", "WORKER_JIRA_WORK_ITEMS_ENABLED"),
            ("linear", "WORKER_LINEAR_WORK_ITEMS_ENABLED"),
        ),
    ),
    ProviderFamilyPolicy(
        providers=frozenset({"pagerduty"}),
        canonical_dataset="incidents",
        datasets=PAGERDUTY_INCIDENT_DATASETS,
        mode=FamilyExecutionMode.INDEPENDENT,
    ),
)

_TRUE = frozenset({"1", "true", "yes", "on"})


def provider_family_policy(provider: str, dataset: str) -> ProviderFamilyPolicy | None:
    normalized_provider = provider.strip().lower()
    normalized_dataset = dataset.strip().lower()
    for policy in _POLICIES:
        if policy.applies_to(normalized_provider, normalized_dataset):
            return policy
    return None


def atomic_provider_family_route_enabled(
    provider: str,
    dataset: str,
    environment: Mapping[str, str] | None = None,
) -> bool:
    """Whether one atomic family has native ownership in this process.

    Explicit per-provider switches override the local ``all`` preset. Keeping
    this decision beside the family policy lets the planner and dispatcher
    agree on whether processor flags describe contributing datasets or the
    complete indivisible Go writer family.
    """

    normalized_provider = provider.strip().lower()
    policy = provider_family_policy(normalized_provider, dataset.strip().lower())
    if policy is None or policy.mode is not FamilyExecutionMode.ATOMIC_CANONICAL:
        return False
    source = os.environ if environment is None else environment
    switch_name = (
        policy.switch_environment_name(normalized_provider)
        or f"WORKER_{normalized_provider.upper()}_WORK_ITEMS_ENABLED"
    )
    explicit = source.get(switch_name)
    if explicit is not None:
        return explicit.strip().lower() in _TRUE
    return (
        source.get("DEV_HEALTH_ENV", "").strip().lower() == "local"
        and source.get("GO_PROVIDER_ROUTES", "").strip().lower() == "all"
    )


def validate_provider_family_claim(
    provider: str,
    dataset: str,
    processor_flags: Mapping[str, object] | None,
    *,
    strict_atomic: bool,
) -> bool:
    """Validate one claim without changing default-off legacy ownership.

    Atomic validation is switch-gated. Once enabled, only the canonical claim
    carrying every declared family flag as the literal boolean ``True`` is
    admissible. Unknown ``family_dataset_*`` flags fail closed. Independent
    families preserve their existing per-dataset claim shape under D16.
    """

    policy = provider_family_policy(provider, dataset)
    if (
        policy is None
        or policy.mode is FamilyExecutionMode.INDEPENDENT
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
