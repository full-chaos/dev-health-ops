from __future__ import annotations

from collections.abc import Mapping
from dataclasses import replace
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
    source_is_org_wide_placeholder: bool = False,
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
        source_is_org_wide_placeholder=source_is_org_wide_placeholder,
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
    assert kwargs["until"] == WINDOW_END
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
    assert kwargs["until"] == WINDOW_END
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
    if provider == "github":
        assert kwargs["include_issues"] is True
    else:
        assert "include_issues" not in kwargs
    assert result["work_items_synced"] is True
    assert result["source"] == source_external_id


def test_github_work_item_unit_threads_source_scope_contract() -> None:
    """CHAOS-2720: the adapter must hand ``run_work_items_sync_job`` the source
    identity that lets it scope a GitHub unit to its own repo — ``repo_name`` set
    to the ``owner/repo`` slug AND ``require_source=True`` (fail closed if the
    source repo is not discovered). ``run_work_items_sync_job`` then drops
    off-source GitHub repos before ingest, so one unit no longer fans out across
    every org repo (call-count proof in tests/test_work_item_source_scope.py).
    """
    ctx = _context(
        provider="github",
        dataset_key="work-items",
        source_external_id="full-chaos/dev-health",
    )

    with patch(
        "dev_health_ops.metrics.job_work_items.run_work_items_sync_job"
    ) as work_items:
        run_dataset_unit(ctx, _runtime())

    work_items.assert_called_once()
    kwargs = work_items.call_args.kwargs
    assert kwargs["repo_name"] == "full-chaos/dev-health"
    assert kwargs["require_source"] is True


def test_gitlab_work_item_unit_threads_source_scope_contract() -> None:
    """CHAOS-2763: gitlab twin of the GitHub contract above. The adapter must
    hand ``run_work_items_sync_job`` the unit's numeric GitLab project id (the
    dataset-adapter layer already threads ``context.source_external_id``
    through for provider in {github, gitlab, linear} — this pins that gitlab
    is not silently excluded) AND ``require_source=True``, so
    ``run_work_items_sync_job`` can scope the unit to its own project (call-
    count proof in tests/test_work_item_source_scope.py).
    """
    ctx = _context(
        provider="gitlab",
        dataset_key="work-items",
        source_external_id="123",
    )

    with patch(
        "dev_health_ops.metrics.job_work_items.run_work_items_sync_job"
    ) as work_items:
        run_dataset_unit(ctx, _runtime())

    work_items.assert_called_once()
    kwargs = work_items.call_args.kwargs
    assert kwargs["repo_name"] == "123"
    assert kwargs["require_source"] is True


def test_linear_org_wide_provider_name_placeholder_routes_to_no_source() -> None:
    ctx = _context(
        provider="linear",
        dataset_key="work-items",
        source_external_id="linear",
        source_is_org_wide_placeholder=True,
    )

    with patch(
        "dev_health_ops.metrics.job_work_items.run_work_items_sync_job"
    ) as work_items:
        result = run_dataset_unit(ctx, _runtime())

    work_items.assert_called_once()
    kwargs = work_items.call_args.kwargs
    assert kwargs["provider"] == "linear"
    assert kwargs["repo_name"] is None
    assert kwargs["require_source"] is False
    assert result["source"] == "linear"


def test_linear_provider_name_scoped_source_stays_visible_to_provider() -> None:
    ctx = _context(
        provider="linear",
        dataset_key="work-items",
        source_external_id="linear",
    )

    with patch(
        "dev_health_ops.metrics.job_work_items.run_work_items_sync_job"
    ) as work_items:
        run_dataset_unit(ctx, _runtime())

    work_items.assert_called_once()
    kwargs = work_items.call_args.kwargs
    assert kwargs["repo_name"] == "linear"
    assert kwargs["require_source"] is True


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
        "dev_health_ops.workers.feature_flag_sync._sync_gitlab_feature_flags",
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
        "dev_health_ops.workers.feature_flag_sync._sync_launchdarkly_feature_flags",
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


def test_launchdarkly_feature_flags_threads_usage_observations_to_result() -> None:
    """CHAOS-2761: provider_usage actuals drained by LaunchDarklyClient /
    LaunchDarklyCodeReferencesClient must reach the unit result's top-level
    ``observations``, mirroring the work-items dataset passthrough, so
    run_sync_unit's budget_comparison join can see them."""
    ctx = _context(
        provider="launchdarkly",
        dataset_key="feature-flags",
        source_external_id="proj",
        credentials={"api_key": "ld-key"},
    )
    observations = {
        "provider_usage": [
            {
                "transport": "rest",
                "route_family": "flags",
                "dimension": "rest_core",
                "request_count": 1,
            }
        ]
    }

    with patch(
        "dev_health_ops.workers.feature_flag_sync._sync_launchdarkly_feature_flags",
        return_value={"flags_synced": 3, "observations": observations},
    ):
        result = run_dataset_unit(ctx, _runtime())

    assert result["observations"] == observations


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


class _ClientAssertingStore:
    """Store double that replicates ClickHouseStore's client invariant.

    ``insert_repo`` asserts ``client is not None`` exactly like
    ``ClickHouseStore.insert_repo`` (storage/clickhouse.py:260), and ``client``
    is assigned only inside ``__aenter__``. A store handed to a processor
    WITHOUT being entered therefore reproduces the CHAOS-2592 AssertionError.
    """

    def __init__(self) -> None:
        self.client: object | None = None
        self.org_id: str | None = None
        self.entered = 0
        self.inserted: list[object] = []

    async def __aenter__(self) -> _ClientAssertingStore:
        self.client = object()
        self.entered += 1
        return self

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
        self.client = None

    async def insert_repo(self, repo: object) -> None:
        assert self.client is not None
        self.inserted.append(repo)


def test_github_dataset_through_runtime_cache_enters_store_before_insert(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Integration regression (CHAOS-2592): a store obtained through the REAL
    # ProviderRuntimeCache and threaded into run_dataset_unit must be entered,
    # so a processor that writes to it hits a live client instead of asserting.
    # The other adapter tests use a Mock() store, which masks this entire class
    # of "store handed over un-entered" bugs -- this test exercises the actual
    # cache -> adapter -> handler -> sink seam end to end.
    from dev_health_ops.workers.sync_bootstrap import ProviderRuntimeCache

    store = _ClientAssertingStore()
    monkeypatch.setattr("dev_health_ops.storage.create_store", lambda *a, **k: store)

    async def _fake_process_github_repo(**kwargs: object) -> None:
        # Mimic process_github_repo's first persistence call against the store.
        await kwargs["store"].insert_repo({"id": "repo-1"})  # type: ignore[attr-defined]

    ctx = _context(dataset_key="repo-metadata")
    runtime = ProviderRuntimeCache().get(ctx)

    with patch(
        "dev_health_ops.processors.github.process_github_repo",
        _fake_process_github_repo,
    ):
        result = run_dataset_unit(ctx, runtime)

    assert store.entered == 1
    assert store.client is not None
    assert store.inserted == [{"id": "repo-1"}]
    assert result["dataset"] == "repo-metadata"


def test_github_work_items_include_prs_when_prs_dataset_enabled() -> None:
    """CHAOS-646: github work-items ingest PRs as work items when PRS is enabled.

    The planner stamps ``sync_prs=True`` on the work-items unit when a PRS-family
    dataset is enabled for the config; the adapter must thread that into
    ``run_work_items_sync_job(include_pull_requests=True)``.
    """
    ctx = _context(
        provider="github",
        dataset_key="work-items",
        processor_flags=_flags(sync_prs=True),
    )

    with patch(
        "dev_health_ops.metrics.job_work_items.run_work_items_sync_job"
    ) as work_items:
        run_dataset_unit(ctx, _runtime())

    work_items.assert_called_once()
    assert work_items.call_args.kwargs["include_issues"] is True
    assert work_items.call_args.kwargs["include_pull_requests"] is True


def test_github_work_items_threads_usage_observations_to_result() -> None:
    ctx = _context(
        provider="github",
        dataset_key="work-items",
        processor_flags=_flags(sync_prs=True),
    )
    observations = {
        "github_usage": [
            {
                "transport": "rest",
                "operation": "GET /repos/full-chaos/dev-health/issues",
                "request_count": 1,
                "rate_limit": {"remaining": "4999", "reset": "1234567890"},
            }
        ]
    }

    with patch(
        "dev_health_ops.metrics.job_work_items.run_work_items_sync_job",
        return_value={"observations": observations},
    ):
        result = run_dataset_unit(ctx, _runtime())

    assert result["observations"] == observations


def test_github_work_items_exclude_prs_when_prs_dataset_disabled() -> None:
    """CHAOS-646 regression: PRs must NOT be ingested as work items when the PRS
    dataset is off. A missing/None value would let the github provider fall back
    to the GITHUB_INCLUDE_PRS env default (PRs ON), re-introducing the bug."""
    ctx = _context(
        provider="github",
        dataset_key="work-items",
        processor_flags=_flags(sync_prs=False),
    )

    with patch(
        "dev_health_ops.metrics.job_work_items.run_work_items_sync_job"
    ) as work_items:
        run_dataset_unit(ctx, _runtime())

    work_items.assert_called_once()
    assert work_items.call_args.kwargs["include_issues"] is True
    assert work_items.call_args.kwargs["include_pull_requests"] is False


def test_non_github_work_items_leave_include_pull_requests_unset() -> None:
    """Only github threads include_pull_requests (matching the legacy worker);
    other providers leave it unset so the provider default applies."""
    ctx = _context(
        provider="jira",
        dataset_key="work-items",
        source_external_id="ENG",
        processor_flags=_flags(),
    )

    with patch(
        "dev_health_ops.metrics.job_work_items.run_work_items_sync_job"
    ) as work_items:
        run_dataset_unit(ctx, _runtime())

    work_items.assert_called_once()
    assert "include_issues" not in work_items.call_args.kwargs
    assert "include_pull_requests" not in work_items.call_args.kwargs


@pytest.mark.parametrize(
    "dataset_key",
    ["work-item-labels", "work-item-projects"],
)
def test_work_item_derivative_preserves_non_null_window_start_not_midnight(
    dataset_key: str,
) -> None:
    """CHAOS-2707: work-item-labels and work-item-projects must echo the
    context's window_start in result["window_start"] verbatim, not fall back
    to midnight of the window day.
    """
    ctx = _context(
        provider="jira",
        dataset_key=dataset_key,
        source_external_id="ENG",
    )
    # WINDOW_START is 2026-01-10T00:00:00+00:00 — but the key invariant is
    # that a non-midnight context.window_start (e.g. mid-day) is preserved.
    mid_day_start = WINDOW_START.replace(hour=14, minute=30, second=0)
    ctx = replace(ctx, window_start=mid_day_start)

    with patch(
        "dev_health_ops.metrics.job_work_items.run_work_items_sync_job"
    ) as work_items:
        result = run_dataset_unit(ctx, _runtime())

    work_items.assert_called_once()
    # The adapter must NOT fall back to midnight; it must preserve the exact
    # window_start from context.
    assert result["window_start"] == mid_day_start.isoformat(), (
        f"Expected window_start={mid_day_start.isoformat()!r} but got {result['window_start']!r}; "
        "adapter fell back to midnight instead of preserving context.window_start"
    )
