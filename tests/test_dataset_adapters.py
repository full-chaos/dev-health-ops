from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime, timezone
from unittest.mock import AsyncMock, Mock, patch

import pytest

from dev_health_ops.processors.dataset_adapters import run_dataset_unit
from dev_health_ops.workers.sync_bootstrap import ProviderRuntime, SyncTaskContext

WINDOW_START = datetime(2026, 1, 10, tzinfo=timezone.utc)
WINDOW_END = datetime(2026, 1, 12, tzinfo=timezone.utc)


def _flags(**enabled: bool) -> dict[str, bool]:
    flags = {
        "sync_git": False,
        "sync_prs": False,
        "sync_cicd": False,
        "sync_deployments": False,
        "sync_incidents": False,
        "sync_security": False,
        "sync_tests": False,
        "blame_only": False,
        "sync_commits": False,
        "sync_commit_stats": False,
        "sync_files": False,
        "sync_blame": False,
    }
    flags.update(enabled)
    return flags


def _context(
    *,
    provider: str = "github",
    dataset_key: str,
    source_external_id: str = "full-chaos/dev-health",
    processor_flags: dict[str, bool] | None = None,
    credentials: Mapping[str, object] | None = None,
) -> SyncTaskContext:
    return SyncTaskContext(
        unit_id="unit-1",
        sync_run_id="run-1",
        org_id="org-1",
        integration_id="integration-1",
        source_id="source-1",
        source_external_id=source_external_id,
        provider=provider,
        dataset_key=dataset_key,
        cost_class="medium",
        mode="incremental",
        window_start=WINDOW_START,
        window_end=WINDOW_END,
        processor_flags=processor_flags or _flags(),
        credential_id="credential-1",
        decrypted_credentials=dict(credentials or {"token": "secret-token"}),
        db_url="clickhouse://localhost/default",
    )


def _runtime() -> ProviderRuntime:
    return ProviderRuntime(store=Mock(name="store"))


@pytest.mark.parametrize(
    ("dataset_key", "flags"),
    [
        ("repo-metadata", _flags()),
        ("commits", _flags(sync_git=True, sync_commits=True)),
        ("commit-stats", _flags(sync_git=True, sync_commit_stats=True)),
        ("files", _flags(sync_git=True, sync_files=True)),
        ("blame", _flags(blame_only=True, sync_blame=True)),
        ("prs", _flags(sync_prs=True)),
        ("pr-reviews", _flags(sync_prs=True)),
        ("pr-comments", _flags(sync_prs=True)),
        ("cicd", _flags(sync_cicd=True)),
        ("tests", _flags(sync_tests=True)),
        ("deployments", _flags(sync_deployments=True)),
        ("incidents", _flags(sync_incidents=True)),
        ("security", _flags(sync_security=True)),
    ],
)
def test_github_code_datasets_call_repo_processor_with_explicit_flags(
    dataset_key: str, flags: dict[str, bool]
) -> None:
    ctx = _context(dataset_key=dataset_key, processor_flags=flags)
    processor = AsyncMock()

    with patch("dev_health_ops.processors.github.process_github_repo", processor):
        result = run_dataset_unit(ctx, _runtime())

    processor.assert_awaited_once()
    await_args = processor.await_args
    assert await_args is not None
    kwargs = await_args.kwargs
    assert kwargs["owner"] == "full-chaos"
    assert kwargs["repo_name"] == "dev-health"
    assert kwargs["since"] == WINDOW_START
    assert kwargs["backfill_missing"] is False
    assert kwargs["fetch_blame"] is False
    assert kwargs["max_commits"] is None
    assert kwargs["sync_git"] is flags["sync_git"]
    assert kwargs["sync_commits"] is flags["sync_commits"]
    assert kwargs["sync_commit_stats"] is flags["sync_commit_stats"]
    assert kwargs["sync_files"] is flags["sync_files"]
    assert kwargs["sync_blame"] is flags["sync_blame"]
    assert kwargs["sync_prs"] is flags["sync_prs"]
    assert kwargs["sync_cicd"] is flags["sync_cicd"]
    assert kwargs["sync_tests"] is flags["sync_tests"]
    assert kwargs["sync_deployments"] is flags["sync_deployments"]
    assert kwargs["sync_incidents"] is flags["sync_incidents"]
    assert kwargs["sync_security"] is flags["sync_security"]
    assert kwargs["blame_only"] is flags["blame_only"]
    assert result["provider"] == "github"
    assert result["dataset"] == dataset_key
    assert result["source"] == "full-chaos/dev-health"


def test_github_prs_unit_does_not_over_fetch_deployments_incidents_or_security() -> (
    None
):
    ctx = _context(dataset_key="prs", processor_flags=_flags(sync_prs=True))
    processor = AsyncMock()

    with patch("dev_health_ops.processors.github.process_github_repo", processor):
        run_dataset_unit(ctx, _runtime())

    await_args = processor.await_args
    assert await_args is not None
    kwargs = await_args.kwargs
    assert kwargs["sync_prs"] is True
    assert kwargs["sync_security"] is False
    assert kwargs["sync_deployments"] is False
    assert kwargs["sync_incidents"] is False
    assert kwargs["sync_cicd"] is False
    assert kwargs["sync_tests"] is False


@pytest.mark.parametrize(
    ("dataset_key", "flags"),
    [
        ("repo-metadata", _flags()),
        ("commits", _flags(sync_git=True, sync_commits=True)),
        ("commit-stats", _flags(sync_git=True, sync_commit_stats=True)),
        ("files", _flags(sync_git=True, sync_files=True)),
        ("blame", _flags(blame_only=True, sync_blame=True)),
        ("prs", _flags(sync_prs=True)),
        ("pr-reviews", _flags(sync_prs=True)),
        ("pr-comments", _flags(sync_prs=True)),
        ("cicd", _flags(sync_cicd=True)),
        ("tests", _flags(sync_tests=True)),
        ("deployments", _flags(sync_deployments=True)),
        ("incidents", _flags(sync_incidents=True)),
        ("security", _flags(sync_security=True)),
    ],
)
def test_gitlab_code_datasets_call_project_processor_with_explicit_flags(
    dataset_key: str, flags: dict[str, bool]
) -> None:
    ctx = _context(
        provider="gitlab",
        dataset_key=dataset_key,
        source_external_id="123",
        processor_flags=flags,
        credentials={"token": "gitlab-token", "gitlab_url": "https://gitlab.example"},
    )
    processor = AsyncMock()

    with patch("dev_health_ops.processors.gitlab.process_gitlab_project", processor):
        result = run_dataset_unit(ctx, _runtime())

    processor.assert_awaited_once()
    await_args = processor.await_args
    assert await_args is not None
    kwargs = await_args.kwargs
    assert kwargs["project_id"] == 123
    assert kwargs["token"] == "gitlab-token"
    assert kwargs["gitlab_url"] == "https://gitlab.example"
    assert kwargs["since"] == WINDOW_START
    assert kwargs["backfill_missing"] is False
    assert kwargs["fetch_blame"] is False
    for flag_name, expected in flags.items():
        assert kwargs[flag_name] is expected
    assert result["provider"] == "gitlab"
    assert result["dataset"] == dataset_key
    assert result["source"] == "123"


@pytest.mark.parametrize(
    "provider, source_external_id, expected_extra",
    [
        ("github", "full-chaos/dev-health", {"repo_name": "full-chaos/dev-health"}),
        ("gitlab", "123", {"repo_name": "123"}),
        ("jira", "OPS", {"jira_project_keys": ["OPS"]}),
        ("linear", "TEAM", {"repo_name": "TEAM"}),
    ],
)
def test_work_item_datasets_route_to_work_item_sync_scoped_to_source(
    provider: str, source_external_id: str, expected_extra: dict[str, object]
) -> None:
    credentials: dict[str, object] = {
        "token": "token",
        "gitlab_url": "https://gitlab.example",
    }
    ctx = _context(
        provider=provider,
        dataset_key="work-items",
        source_external_id=source_external_id,
        credentials=credentials,
    )

    with patch(
        "dev_health_ops.metrics.job_work_items.run_work_items_sync_job"
    ) as work_items:
        result = run_dataset_unit(ctx, _runtime())

    work_items.assert_called_once()
    kwargs = work_items.call_args.kwargs
    assert kwargs["db_url"] == "clickhouse://localhost/default"
    assert kwargs["provider"] == provider
    assert kwargs["org_id"] == "org-1"
    assert kwargs["day"] == WINDOW_END.date()
    assert kwargs["backfill_days"] == 3
    for key, value in expected_extra.items():
        assert kwargs[key] == value
    assert result["work_items_synced"] is True
    assert result["source"] == source_external_id


def test_work_item_derivative_dataset_uses_same_work_item_path() -> None:
    ctx = _context(
        provider="jira", dataset_key="work-item-comments", source_external_id="ENG"
    )

    with patch(
        "dev_health_ops.metrics.job_work_items.run_work_items_sync_job"
    ) as work_items:
        run_dataset_unit(ctx, _runtime())

    work_items.assert_called_once()
    assert work_items.call_args.kwargs["jira_project_keys"] == ["ENG"]


def test_gitlab_feature_flags_route_to_existing_feature_flag_sync() -> None:
    ctx = _context(
        provider="gitlab",
        dataset_key="feature-flags",
        source_external_id="123",
        credentials={"token": "gitlab-token"},
    )

    with patch(
        "dev_health_ops.workers.sync_runtime._sync_gitlab_feature_flags",
        return_value={"flags_synced": 2},
    ) as feature_flags:
        result = run_dataset_unit(ctx, _runtime())

    feature_flags.assert_called_once_with(
        db_url="clickhouse://localhost/default",
        org_id="org-1",
        credentials={"token": "gitlab-token"},
        sync_options={"project_id": "123", "project_key": "123"},
    )
    assert result["feature_flags"] == {"flags_synced": 2}


def test_launchdarkly_feature_flags_route_to_existing_feature_flag_sync() -> None:
    ctx = _context(
        provider="launchdarkly",
        dataset_key="feature-flags",
        source_external_id="proj",
        credentials={"api_key": "ld-key"},
    )

    with patch(
        "dev_health_ops.workers.sync_runtime._sync_launchdarkly_feature_flags",
        return_value={"flags_synced": 3},
    ) as feature_flags:
        result = run_dataset_unit(ctx, _runtime())

    feature_flags.assert_called_once_with(
        db_url="clickhouse://localhost/default",
        org_id="org-1",
        credentials={"api_key": "ld-key"},
        sync_options={"project_id": "proj", "project_key": "proj"},
        since_dt=WINDOW_START,
    )
    assert result["feature_flags"] == {"flags_synced": 3}


def test_unsupported_provider_dataset_pair_raises_value_error() -> None:
    ctx = _context(provider="jira", dataset_key="commits", source_external_id="OPS")

    with pytest.raises(ValueError, match="Unsupported provider dataset unit"):
        run_dataset_unit(ctx, _runtime())


@pytest.mark.parametrize(
    ("dataset_key", "own_flag"),
    [
        ("commits", "sync_commits"),
        ("commit-stats", "sync_commit_stats"),
        ("files", "sync_files"),
        ("blame", "sync_blame"),
    ],
)
def test_registry_flags_make_split_git_datasets_actually_sync(
    dataset_key: str, own_flag: str
) -> None:
    # Regression (Codex Wave 2 critical): the planner persists the registry's
    # processor_flags verbatim. If the registry omits a split flag, the adapter
    # passes it as explicit False and the split processor SKIPS the dataset while
    # the unit still marks success -> silent incremental data loss. This test
    # uses the REAL registry flags (not hand-built) to lock the contract.
    from dev_health_ops.sync.datasets import get_dataset_spec

    spec = get_dataset_spec("github", dataset_key)
    assert spec is not None
    ctx = _context(dataset_key=dataset_key, processor_flags=dict(spec.processor_flags))
    processor = AsyncMock()

    with patch("dev_health_ops.processors.github.process_github_repo", processor):
        run_dataset_unit(ctx, _runtime())

    assert processor.await_args is not None
    kwargs = processor.await_args.kwargs
    assert kwargs[own_flag] is True
    split_flags = {"sync_commits", "sync_commit_stats", "sync_files", "sync_blame"}
    for other in split_flags - {own_flag}:
        assert kwargs[other] is False
