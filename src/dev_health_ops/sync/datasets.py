from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class DatasetKey(str, Enum):
    REPO_METADATA = "repo-metadata"
    COMMITS = "commits"
    COMMIT_STATS = "commit-stats"
    FILES = "files"
    BLAME = "blame"
    PRS = "prs"
    PR_REVIEWS = "pr-reviews"
    PR_COMMENTS = "pr-comments"
    CICD = "cicd"
    TESTS = "tests"
    DEPLOYMENTS = "deployments"
    INCIDENTS = "incidents"
    SECURITY = "security"
    WORK_ITEMS = "work-items"
    WORK_ITEM_LABELS = "work-item-labels"
    WORK_ITEM_PROJECTS = "work-item-projects"
    WORK_ITEM_HISTORY = "work-item-history"
    WORK_ITEM_COMMENTS = "work-item-comments"
    FEATURE_FLAGS = "feature-flags"
    SERVICES = "services"
    BUSINESS_SERVICES = "business-services"
    ESCALATION_POLICIES = "escalation-policies"
    SCHEDULES = "schedules"
    ON_CALLS = "on-calls"
    USERS = "users"
    TEAMS = "teams"
    INCIDENT_ALERTS = "incident-alerts"
    INCIDENT_LOG_ENTRIES = "incident-log-entries"
    INCIDENT_NOTES = "incident-notes"


class CostClass(str, Enum):
    LIGHT = "light"
    MEDIUM = "medium"
    HEAVY = "heavy"


class WatermarkBehavior(str, Enum):
    INCREMENTAL = "incremental"
    NONE = "none"


@dataclass(frozen=True)
class DatasetSpec:
    provider: str
    dataset_key: str
    legacy_targets: frozenset[str]
    processor_flags: dict[str, bool]
    default_cost_class: CostClass
    watermark_behavior: WatermarkBehavior
    supported: bool


_LIGHT_DATASETS = frozenset(
    {
        DatasetKey.REPO_METADATA.value,
        DatasetKey.INCIDENTS.value,
        DatasetKey.WORK_ITEM_LABELS.value,
        DatasetKey.WORK_ITEM_PROJECTS.value,
        DatasetKey.SERVICES.value,
        DatasetKey.BUSINESS_SERVICES.value,
        DatasetKey.ESCALATION_POLICIES.value,
        DatasetKey.SCHEDULES.value,
        DatasetKey.USERS.value,
        DatasetKey.TEAMS.value,
    }
)
_MEDIUM_DATASETS = frozenset(
    {
        DatasetKey.COMMITS.value,
        DatasetKey.PRS.value,
        DatasetKey.PR_REVIEWS.value,
        DatasetKey.PR_COMMENTS.value,
        DatasetKey.CICD.value,
        DatasetKey.DEPLOYMENTS.value,
        DatasetKey.SECURITY.value,
        DatasetKey.WORK_ITEMS.value,
        DatasetKey.WORK_ITEM_HISTORY.value,
        DatasetKey.WORK_ITEM_COMMENTS.value,
        DatasetKey.FEATURE_FLAGS.value,
        DatasetKey.ON_CALLS.value,
        DatasetKey.INCIDENT_ALERTS.value,
        DatasetKey.INCIDENT_LOG_ENTRIES.value,
        DatasetKey.INCIDENT_NOTES.value,
    }
)
_HEAVY_DATASETS = frozenset(
    {
        DatasetKey.COMMIT_STATS.value,
        DatasetKey.FILES.value,
        DatasetKey.BLAME.value,
        DatasetKey.TESTS.value,
    }
)

_PROCESSOR_FLAGS_BY_DATASET: dict[str, dict[str, bool]] = {
    DatasetKey.COMMITS.value: {"sync_git": True, "sync_commits": True},
    DatasetKey.COMMIT_STATS.value: {"sync_git": True, "sync_commit_stats": True},
    DatasetKey.FILES.value: {"sync_git": True, "sync_files": True},
    DatasetKey.BLAME.value: {"blame_only": True, "sync_blame": True},
    DatasetKey.PRS.value: {"sync_prs": True},
    DatasetKey.PR_REVIEWS.value: {"sync_prs": True},
    DatasetKey.PR_COMMENTS.value: {"sync_prs": True},
    DatasetKey.CICD.value: {"sync_cicd": True},
    DatasetKey.TESTS.value: {"sync_tests": True},
    DatasetKey.DEPLOYMENTS.value: {"sync_deployments": True},
    DatasetKey.INCIDENTS.value: {"sync_incidents": True},
    DatasetKey.SECURITY.value: {"sync_security": True},
}

_LEGACY_TARGETS_BY_DATASET: dict[str, frozenset[str]] = {
    DatasetKey.REPO_METADATA.value: frozenset({"git"}),
    DatasetKey.COMMITS.value: frozenset({"git"}),
    DatasetKey.COMMIT_STATS.value: frozenset({"git"}),
    DatasetKey.FILES.value: frozenset({"git"}),
    DatasetKey.BLAME.value: frozenset({"blame"}),
    DatasetKey.PRS.value: frozenset({"prs"}),
    DatasetKey.PR_REVIEWS.value: frozenset({"prs"}),
    DatasetKey.PR_COMMENTS.value: frozenset({"prs"}),
    DatasetKey.CICD.value: frozenset({"cicd"}),
    DatasetKey.TESTS.value: frozenset({"tests"}),
    DatasetKey.DEPLOYMENTS.value: frozenset({"deployments"}),
    DatasetKey.INCIDENTS.value: frozenset({"incidents"}),
    DatasetKey.SECURITY.value: frozenset({"security"}),
    DatasetKey.WORK_ITEMS.value: frozenset({"work-items"}),
    DatasetKey.WORK_ITEM_LABELS.value: frozenset({"work-items"}),
    DatasetKey.WORK_ITEM_PROJECTS.value: frozenset({"work-items"}),
    DatasetKey.WORK_ITEM_HISTORY.value: frozenset({"work-items"}),
    DatasetKey.WORK_ITEM_COMMENTS.value: frozenset({"work-items"}),
    DatasetKey.FEATURE_FLAGS.value: frozenset({"feature-flags"}),
    DatasetKey.SERVICES.value: frozenset({"operational"}),
    DatasetKey.BUSINESS_SERVICES.value: frozenset({"operational"}),
    DatasetKey.ESCALATION_POLICIES.value: frozenset({"operational"}),
    DatasetKey.SCHEDULES.value: frozenset({"operational"}),
    DatasetKey.ON_CALLS.value: frozenset({"operational"}),
    DatasetKey.USERS.value: frozenset({"operational"}),
    DatasetKey.TEAMS.value: frozenset({"operational"}),
    DatasetKey.INCIDENT_ALERTS.value: frozenset({"operational"}),
    DatasetKey.INCIDENT_LOG_ENTRIES.value: frozenset({"operational"}),
    DatasetKey.INCIDENT_NOTES.value: frozenset({"operational"}),
}

_NO_WATERMARK_DATASETS = frozenset(
    {
        DatasetKey.REPO_METADATA.value,
        DatasetKey.SERVICES.value,
        DatasetKey.BUSINESS_SERVICES.value,
        DatasetKey.ESCALATION_POLICIES.value,
        DatasetKey.SCHEDULES.value,
        DatasetKey.ON_CALLS.value,
        DatasetKey.USERS.value,
        DatasetKey.TEAMS.value,
    }
)

_PAGERDUTY_LEGACY_TARGET_OVERRIDES: dict[str, frozenset[str]] = {
    DatasetKey.INCIDENTS.value: frozenset({"operational"}),
}

_PROVIDER_DATASET_COST_CLASSES: dict[tuple[str, str], CostClass] = {
    ("jira", DatasetKey.INCIDENTS.value): CostClass.MEDIUM,
}

_PROVIDER_DATASET_LEGACY_TARGETS: dict[tuple[str, str], frozenset[str]] = {
    ("jira", DatasetKey.INCIDENTS.value): frozenset({"operational"}),
}

_PROVIDER_DATASET_PROCESSOR_FLAGS: dict[tuple[str, str], dict[str, bool]] = {
    ("jira", DatasetKey.INCIDENTS.value): {},
}

_LEGACY_TARGET_ORDER = (
    "git",
    "prs",
    "blame",
    "cicd",
    "deployments",
    "incidents",
    "security",
    "tests",
    "work-items",
    "feature-flags",
    "operational",
)

_PROVIDER_SUPPORTED_DATASETS: dict[str, frozenset[str]] = {
    "github": frozenset(
        {
            DatasetKey.REPO_METADATA.value,
            DatasetKey.COMMITS.value,
            DatasetKey.COMMIT_STATS.value,
            DatasetKey.FILES.value,
            DatasetKey.BLAME.value,
            DatasetKey.PRS.value,
            DatasetKey.PR_REVIEWS.value,
            DatasetKey.PR_COMMENTS.value,
            DatasetKey.CICD.value,
            DatasetKey.TESTS.value,
            DatasetKey.DEPLOYMENTS.value,
            DatasetKey.INCIDENTS.value,
            DatasetKey.SECURITY.value,
            DatasetKey.WORK_ITEMS.value,
            DatasetKey.WORK_ITEM_LABELS.value,
            DatasetKey.WORK_ITEM_PROJECTS.value,
            DatasetKey.WORK_ITEM_HISTORY.value,
            DatasetKey.WORK_ITEM_COMMENTS.value,
        }
    ),
    "gitlab": frozenset(
        {
            DatasetKey.REPO_METADATA.value,
            DatasetKey.COMMITS.value,
            DatasetKey.COMMIT_STATS.value,
            DatasetKey.FILES.value,
            DatasetKey.BLAME.value,
            DatasetKey.PRS.value,
            DatasetKey.PR_REVIEWS.value,
            DatasetKey.PR_COMMENTS.value,
            DatasetKey.CICD.value,
            DatasetKey.TESTS.value,
            DatasetKey.DEPLOYMENTS.value,
            DatasetKey.INCIDENTS.value,
            DatasetKey.SECURITY.value,
            DatasetKey.WORK_ITEMS.value,
            DatasetKey.WORK_ITEM_LABELS.value,
            DatasetKey.WORK_ITEM_PROJECTS.value,
            DatasetKey.WORK_ITEM_HISTORY.value,
            DatasetKey.WORK_ITEM_COMMENTS.value,
            DatasetKey.FEATURE_FLAGS.value,
        }
    ),
    "jira": frozenset(
        {
            DatasetKey.INCIDENTS.value,
            DatasetKey.WORK_ITEMS.value,
            DatasetKey.WORK_ITEM_LABELS.value,
            DatasetKey.WORK_ITEM_PROJECTS.value,
            DatasetKey.WORK_ITEM_HISTORY.value,
            DatasetKey.WORK_ITEM_COMMENTS.value,
        }
    ),
    "linear": frozenset(
        {
            DatasetKey.WORK_ITEMS.value,
            DatasetKey.WORK_ITEM_LABELS.value,
            DatasetKey.WORK_ITEM_PROJECTS.value,
            DatasetKey.WORK_ITEM_HISTORY.value,
            DatasetKey.WORK_ITEM_COMMENTS.value,
        }
    ),
    "launchdarkly": frozenset({DatasetKey.FEATURE_FLAGS.value}),
    "pagerduty": frozenset(
        {
            DatasetKey.SERVICES.value,
            DatasetKey.BUSINESS_SERVICES.value,
            DatasetKey.ESCALATION_POLICIES.value,
            DatasetKey.SCHEDULES.value,
            DatasetKey.ON_CALLS.value,
            DatasetKey.USERS.value,
            DatasetKey.TEAMS.value,
            DatasetKey.INCIDENTS.value,
            DatasetKey.INCIDENT_ALERTS.value,
            DatasetKey.INCIDENT_LOG_ENTRIES.value,
            DatasetKey.INCIDENT_NOTES.value,
        }
    ),
}


def _cost_class(dataset_key: str) -> CostClass:
    if dataset_key in _LIGHT_DATASETS:
        return CostClass.LIGHT
    if dataset_key in _MEDIUM_DATASETS:
        return CostClass.MEDIUM
    if dataset_key in _HEAVY_DATASETS:
        return CostClass.HEAVY
    raise ValueError(f"Unknown dataset cost class for {dataset_key!r}")


def _watermark_behavior(dataset_key: str) -> WatermarkBehavior:
    if dataset_key in _NO_WATERMARK_DATASETS:
        return WatermarkBehavior.NONE
    return WatermarkBehavior.INCREMENTAL


def _build_spec(provider: str, dataset_key: str) -> DatasetSpec:
    provider_key = provider.lower()
    scope = (provider_key, dataset_key)
    return DatasetSpec(
        provider=provider_key,
        dataset_key=dataset_key,
        legacy_targets=(
            _PROVIDER_DATASET_LEGACY_TARGETS.get(
                scope,
                _PAGERDUTY_LEGACY_TARGET_OVERRIDES.get(
                    dataset_key, _LEGACY_TARGETS_BY_DATASET[dataset_key]
                )
                if provider_key == "pagerduty"
                else _LEGACY_TARGETS_BY_DATASET[dataset_key],
            )
        ),
        processor_flags=dict(
            _PROVIDER_DATASET_PROCESSOR_FLAGS.get(
                scope, _PROCESSOR_FLAGS_BY_DATASET.get(dataset_key, {})
            )
        ),
        default_cost_class=_PROVIDER_DATASET_COST_CLASSES.get(
            scope, _cost_class(dataset_key)
        ),
        watermark_behavior=_watermark_behavior(dataset_key),
        supported=True,
    )


_REGISTRY: dict[str, dict[str, DatasetSpec]] = {
    provider: {
        dataset_key: _build_spec(provider, dataset_key)
        for dataset_key in supported_dataset_keys
    }
    for provider, supported_dataset_keys in _PROVIDER_SUPPORTED_DATASETS.items()
}


def supported_datasets(provider: str) -> list[DatasetSpec]:
    provider_key = provider.lower()
    provider_specs = _REGISTRY.get(provider_key, {})
    return [
        provider_specs[key.value] for key in DatasetKey if key.value in provider_specs
    ]


def get_dataset_spec(provider: str, dataset_key: str) -> DatasetSpec | None:
    return _REGISTRY.get(provider.lower(), {}).get(dataset_key)


def supported_legacy_targets(provider: str) -> list[str]:
    targets: set[str] = set()
    for spec in supported_datasets(provider):
        targets.update(spec.legacy_targets)
    return [target for target in _LEGACY_TARGET_ORDER if target in targets]


def processor_sync_targets() -> list[str]:
    targets: set[str] = set()
    for dataset_key, flags in _PROCESSOR_FLAGS_BY_DATASET.items():
        if flags:
            targets.update(_LEGACY_TARGETS_BY_DATASET[dataset_key])
    return [target for target in _LEGACY_TARGET_ORDER if target in targets]
