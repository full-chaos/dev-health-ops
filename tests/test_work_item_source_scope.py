"""CHAOS-2720: a GitHub work-item unit must ingest only its own source repo.

``run_work_items_sync_job`` resolves repos via ``_discover_repos``, which only
short-circuits on ``repo_id`` and otherwise returns *every* repo for the org.
Before CHAOS-2720 the GitHub ingest loop iterated the full org list, so a single
config-aware work-item unit (scoped to one source repo) ran a full
``iter_ingest`` per org repo — N units × all repos of API amplification.

These tests pin the scope contract at the ``run_work_items_sync_job`` seam:

* a GitHub unit with a ``repo_name`` ingests exactly its source repo,
* the CLI/org-wide path (``repo_name is None``) still fans out to every repo,
* matching is case-insensitive on the ``owner/repo`` slug,
* a source repo absent from discovery fails closed (CHAOS-2737), and
* GitLab units are intentionally left org-wide (numeric project-id source id
  can't be matched against ``repos.repo`` path-with-namespace here).
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import date
from typing import Any

import pytest

import dev_health_ops.metrics.job_work_items as job
from dev_health_ops.metrics.work_items import DiscoveredRepo


@dataclass
class _Classification:
    investment_area: str = "Maintenance / Tech Debt"
    project_stream: str = ""
    confidence: float = 1.0
    rule_id: str = "test"


class _Classifier:
    def __init__(self, *_args: object, **_kwargs: object) -> None:
        pass

    def classify(self, _payload: object) -> _Classification:
        return _Classification()


class _FakeClickHouseSink:
    """Minimal metrics sink — records nothing, satisfies the write contract."""

    def __init__(self, _dsn: str) -> None:
        self.org_id = ""

    def ensure_tables(self) -> None:
        return None

    def query_dicts(
        self, _query: str, _params: dict[str, object]
    ) -> list[dict[str, object]]:
        return []

    def __getattr__(self, name: str) -> Any:
        # Any ``write_*`` / ``close`` call is a no-op; the scope tests only care
        # about which repos the provider ingest loop touched.
        if name.startswith("write_") or name == "close":
            return lambda *_args, **_kwargs: None
        raise AttributeError(name)


class _FakeGitHubProvider:
    """Records the repo of every ``iter_ingest`` call; yields no batches."""

    calls: list[str] = []

    def __init__(self, *_args: object, **_kwargs: object) -> None:
        pass

    def iter_ingest(self, ctx: Any) -> Any:
        type(self).calls.append(ctx.repo)
        return iter(())


def _patch_common(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(job, "ClickHouseMetricsSink", _FakeClickHouseSink)
    monkeypatch.setattr(job, "InvestmentClassifier", _Classifier)
    monkeypatch.setattr(
        job, "compute_work_item_metrics_daily", lambda **_kwargs: ([], [], [])
    )
    monkeypatch.setattr(
        job, "compute_estimate_coverage_metrics_daily", lambda **_kwargs: []
    )
    monkeypatch.setattr(
        job, "compute_work_item_team_attributions", lambda **_kwargs: []
    )
    monkeypatch.setattr(
        job, "compute_work_item_state_durations_daily", lambda **_kwargs: []
    )
    monkeypatch.setattr(job, "parse_github_projects_v2_env", lambda: [])


def _github_repos() -> list[DiscoveredRepo]:
    return [
        DiscoveredRepo(
            repo_id=uuid.uuid4(),
            full_name=f"acme/repo-{name}",
            source="github",
            settings={},
        )
        for name in ("a", "b", "c")
    ]


def _run_github(
    monkeypatch: pytest.MonkeyPatch,
    *,
    repo_name: str | None,
    require_source: bool,
    repos: list[DiscoveredRepo],
) -> None:
    _patch_common(monkeypatch)
    _FakeGitHubProvider.calls = []
    monkeypatch.setattr(job, "_discover_repos", lambda **_kwargs: list(repos))
    monkeypatch.setattr(job, "_build_github_work_client", lambda **_kwargs: object())
    monkeypatch.setattr(
        "dev_health_ops.providers.github.provider.GitHubProvider",
        _FakeGitHubProvider,
    )
    run_job: Any = job.run_work_items_sync_job
    run_job(
        db_url="clickhouse://test",
        day=date(2026, 5, 2),
        backfill_days=1,
        provider="github",
        org_id=str(uuid.uuid4()),
        repo_name=repo_name,
        require_source=require_source,
    )


def test_github_unit_scopes_iter_ingest_to_source_repo(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repos = _github_repos()  # acme/repo-a, acme/repo-b, acme/repo-c
    _run_github(
        monkeypatch,
        repo_name="acme/repo-a",
        require_source=True,
        repos=repos,
    )
    # Exactly one ingest, for the source repo only — no org-wide fan-out.
    assert _FakeGitHubProvider.calls == ["acme/repo-a"]


def test_github_unit_source_scope_is_case_insensitive(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repos = _github_repos()
    _run_github(
        monkeypatch,
        repo_name="ACME/Repo-A",
        require_source=True,
        repos=repos,
    )
    assert _FakeGitHubProvider.calls == ["acme/repo-a"]


def test_github_cli_org_wide_path_still_fans_out(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repos = _github_repos()
    # No source (CLI/org-wide) → discovery stays org-wide, every repo ingested.
    _run_github(
        monkeypatch,
        repo_name=None,
        require_source=False,
        repos=repos,
    )
    assert sorted(_FakeGitHubProvider.calls) == [
        "acme/repo-a",
        "acme/repo-b",
        "acme/repo-c",
    ]


def test_github_unit_missing_source_repo_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repos = _github_repos()  # none named repo-z
    with pytest.raises(ValueError, match="source was not discovered"):
        _run_github(
            monkeypatch,
            repo_name="acme/repo-z",
            require_source=True,
            repos=repos,
        )
    # Nothing was ingested — the unit refused to fan out to sibling repos.
    assert _FakeGitHubProvider.calls == []


def test_gitlab_unit_stays_org_wide(monkeypatch: pytest.MonkeyPatch) -> None:
    """Non-regression: GitLab units keep org-wide discovery (CHAOS-2720 scopes
    GitHub only). A GitLab source id is a numeric project id that does not match
    ``repos.repo`` (path_with_namespace), so it must not be used as a GitHub-style
    ``full_name`` filter — doing so would empty discovery and trip require_source.
    """
    _patch_common(monkeypatch)
    gitlab_repos = [
        DiscoveredRepo(
            repo_id=uuid.uuid4(),
            full_name=f"grp/proj-{name}",
            source="gitlab",
            settings={"project_id": pid},
        )
        for name, pid in (("a", 123), ("b", 456), ("c", 789))
    ]
    monkeypatch.setattr(job, "_discover_repos", lambda **_kwargs: list(gitlab_repos))
    monkeypatch.setattr(
        job, "_build_gitlab_work_client", lambda **_kwargs: ("gl-token", None)
    )

    captured: dict[str, Any] = {}

    def _fake_fetch(*, repos: list[DiscoveredRepo], **_kwargs: object) -> Any:
        captured["repos"] = list(repos)
        return ([], [], [])

    monkeypatch.setattr(job, "fetch_gitlab_work_items", _fake_fetch)

    run_job: Any = job.run_work_items_sync_job
    run_job(
        db_url="clickhouse://test",
        day=date(2026, 5, 2),
        backfill_days=1,
        provider="gitlab",
        org_id=str(uuid.uuid4()),
        repo_name="123",  # numeric project id — the GitLab source external id
        require_source=True,
    )

    # All three GitLab projects reach the fetcher — unchanged by CHAOS-2720.
    assert {r.full_name for r in captured["repos"]} == {
        "grp/proj-a",
        "grp/proj-b",
        "grp/proj-c",
    }
