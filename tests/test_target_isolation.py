from __future__ import annotations

from dev_health_ops.api.admin.routers.sync import (
    PROVIDER_SYNC_TARGETS,
    _planner_dataset_keys,
)
from dev_health_ops.processors.sync import (
    _sync_flags_for_target,
    processor_sync_targets,
)
from dev_health_ops.sync.datasets import (
    CostClass,
    DatasetKey,
    WatermarkBehavior,
    get_dataset_spec,
    supported_datasets,
)
from dev_health_ops.workers.task_utils import _merge_sync_flags

ALL_PROCESSOR_FLAGS = frozenset(
    {
        "sync_git",
        "sync_prs",
        "sync_cicd",
        "sync_deployments",
        "sync_incidents",
        "sync_security",
        "sync_tests",
        "blame_only",
    }
)


def test_dataset_key_contract_is_exact() -> None:
    assert [key.value for key in DatasetKey] == [
        "repo-metadata",
        "commits",
        "commit-stats",
        "files",
        "blame",
        "prs",
        "pr-reviews",
        "pr-comments",
        "cicd",
        "tests",
        "deployments",
        "incidents",
        "security",
        "work-items",
        "work-item-labels",
        "work-item-projects",
        "work-item-history",
        "work-item-comments",
        "feature-flags",
    ]


def test_registry_hardcodes_launchdarkly_support() -> None:
    specs = supported_datasets("launchdarkly")
    assert [spec.dataset_key for spec in specs] == ["feature-flags"]
    assert specs[0].provider == "launchdarkly"
    assert specs[0].legacy_targets == frozenset({"feature-flags"})


def test_registry_security_spec_maps_to_isolated_processor_flag() -> None:
    spec = get_dataset_spec("github", "security")
    assert spec is not None
    assert spec.default_cost_class is CostClass.MEDIUM
    assert spec.watermark_behavior is WatermarkBehavior.INCREMENTAL
    assert spec.legacy_targets == frozenset({"security"})
    assert spec.processor_flags == {"sync_security": True}


def test_registry_pr_family_maps_to_prs_legacy_target() -> None:
    for dataset_key in ("prs", "pr-reviews", "pr-comments"):
        spec = get_dataset_spec("gitlab", dataset_key)
        assert spec is not None
        assert spec.legacy_targets == frozenset({"prs"})
        assert spec.processor_flags == {"sync_prs": True}


def test_prs_target_does_not_enable_unrelated_processor_flags() -> None:
    flags = _merge_sync_flags(["prs"])
    assert flags == {
        "sync_git": False,
        "sync_prs": True,
        "sync_cicd": False,
        "sync_deployments": False,
        "sync_incidents": False,
        "sync_security": False,
        "sync_tests": False,
        "blame_only": False,
    }


def test_git_target_does_not_enable_unrelated_processor_flags() -> None:
    flags = _merge_sync_flags(["git"])
    assert flags == {
        "sync_git": True,
        "sync_prs": False,
        "sync_cicd": False,
        "sync_deployments": False,
        "sync_incidents": False,
        "sync_security": False,
        "sync_tests": False,
        "blame_only": False,
    }


def test_code_host_git_default_seeds_blame_dataset() -> None:
    for provider in ("github", "gitlab"):
        dataset_keys = _planner_dataset_keys(provider, ["git"])

        assert dataset_keys == [
            "repo-metadata",
            "commits",
            "commit-stats",
            "files",
            "blame",
        ]


def test_each_processor_target_has_explicit_flag_values() -> None:
    for target in processor_sync_targets():
        flags = _sync_flags_for_target(target)
        assert set(flags) == ALL_PROCESSOR_FLAGS
        assert all(isinstance(value, bool) for value in flags.values())


def test_api_provider_targets_are_generated_from_registry() -> None:
    assert "security" in PROVIDER_SYNC_TARGETS["github"]
    assert "security" in PROVIDER_SYNC_TARGETS["gitlab"]
    assert "tests" in PROVIDER_SYNC_TARGETS["github"]
    assert "tests" in PROVIDER_SYNC_TARGETS["gitlab"]
    assert PROVIDER_SYNC_TARGETS["launchdarkly"] == ["feature-flags"]
