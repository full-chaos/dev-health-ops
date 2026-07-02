"""CHAOS-2720 / CHAOS-2763: a GitHub or GitLab work-item unit must ingest only
its own source repo/project.

``run_work_items_sync_job`` resolves repos via ``_discover_repos``, which only
short-circuits on ``repo_id`` and otherwise returns *every* repo for the org.
Before CHAOS-2720/CHAOS-2763 the GitHub/GitLab ingest loops iterated the full
org list, so a single config-aware work-item unit (scoped to one source
repo/project) ran a full ingest per org repo — N units × all repos of API
amplification.

These tests pin the scope contract at the ``run_work_items_sync_job`` seam:

* a GitHub unit with a ``repo_name`` ingests exactly its source repo,
* a GitLab unit with a numeric ``repo_name`` (the project id) ingests exactly
  its source project, matched against the immutable ``settings.project_id``
  captured at code-dataset sync time — including when ``settings`` arrives as
  the raw JSON string ``discover_repos`` actually returns in production,
* the CLI/org-wide path (``repo_name is None``) still fans out to every repo,
* GitHub matching is case-insensitive on the ``owner/repo`` slug; GitLab
  path-shaped inputs (CLI ``--repo-name grp/proj``) match ``full_name`` the
  same way,
* a source repo/project absent from discovery fails closed (CHAOS-2737),
  including a gitlab row that exists but lacks ``settings.project_id`` (stale
  discovery data) — a numeric unit id never falls back to matching
  ``full_name``, even when a stale row's mutable path happens to equal the id
  string, and
* ``provider="all"`` + ``repo_name`` now scopes GitLab discovery the same way
  it already scoped GitHub (mixed-provider CLI/backfill semantics change).
"""

from __future__ import annotations

import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date
from typing import Any

import pytest

import dev_health_ops.metrics.job_work_items as job
from dev_health_ops.metrics.dependencies import get_metrics_dependencies
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


class _NoOpProvider:
    """Generic no-op provider double for blocks this module doesn't assert on
    (Linear in the ``provider="all"`` test) — constructs freely, yields no
    batches."""

    def __init__(self, *_args: object, **_kwargs: object) -> None:
        pass

    def iter_ingest(self, _ctx: Any) -> Any:
        return iter(())


class _RecordingGitLabAPIClient:
    """[CHAOS-2763 codex HIGH] Records the ``project_id_or_path`` every
    GitLab API call is actually made with — the identifier fetch_gitlab_work_items
    hands to python-gitlab, not just which ``DiscoveredRepo`` reached the
    fetcher. Yields no issues/MRs; zero network I/O."""

    calls: list[str] = []

    def iter_project_issues(self, *, project_id_or_path: str, **_kwargs: object) -> Any:
        type(self).calls.append(project_id_or_path)
        return iter(())

    def iter_project_merge_requests(
        self, *, project_id_or_path: str, **_kwargs: object
    ) -> Any:
        type(self).calls.append(project_id_or_path)
        return iter(())


def _patch_gitlab_client_factory(monkeypatch: pytest.MonkeyPatch) -> None:
    """Replace ``get_metrics_dependencies().gitlab_client_factory`` so the
    REAL ``fetch_gitlab_work_items`` runs against ``_RecordingGitLabAPIClient``
    instead of a mocked-away fetcher — the only way to assert on the actual
    API call identifier (codex HIGH: prior tests mocked ``fetch_gitlab_work_items``
    entirely and never exercised this code path)."""
    deps = get_metrics_dependencies()
    patched = deps.__class__(
        **{
            **deps.__dict__,
            "gitlab_client_factory": lambda **_kwargs: _RecordingGitLabAPIClient(),
        }
    )
    monkeypatch.setattr("dev_health_ops.metrics.dependencies._registry", patched)


def _gitlab_repos(
    *, settings_factory: Callable[[int], object] = lambda pid: {"project_id": pid}
) -> list[DiscoveredRepo]:
    return [
        DiscoveredRepo(
            repo_id=uuid.uuid4(),
            full_name=f"grp/proj-{name}",
            source="gitlab",
            # DiscoveredRepo.settings is typed dict[str, object], but the real
            # ClickHouse row (discover_repos, job_daily.py) hands back a raw
            # JSON string -- exactly the annotation-lying shape this fix
            # accounts for via _repo_settings_dict. settings_factory
            # deliberately exercises both shapes.
            settings=settings_factory(pid),  # type: ignore[arg-type]
        )
        for name, pid in (("a", 123), ("b", 456), ("c", 789))
    ]


def _run_gitlab(
    monkeypatch: pytest.MonkeyPatch,
    *,
    repo_name: str | None,
    require_source: bool,
    repos: list[DiscoveredRepo],
    provider: str = "gitlab",
) -> list[list[DiscoveredRepo]]:
    """Run the gitlab work-item sync job with a faked fetcher; returns the list
    of ``repos`` kwargs the fetcher was invoked with (one entry per call, so
    ``len(...)`` is the invocation count and an empty list means never called).
    """
    _patch_common(monkeypatch)
    monkeypatch.setattr(job, "_discover_repos", lambda **_kwargs: list(repos))
    monkeypatch.setattr(
        job, "_build_gitlab_work_client", lambda **_kwargs: ("gl-token", None)
    )

    calls: list[list[DiscoveredRepo]] = []

    def _fake_fetch(*, repos: list[DiscoveredRepo], **_kwargs: object) -> Any:
        calls.append(list(repos))
        return ([], [], [])

    monkeypatch.setattr(job, "fetch_gitlab_work_items", _fake_fetch)

    run_job: Any = job.run_work_items_sync_job
    run_job(
        db_url="clickhouse://test",
        day=date(2026, 5, 2),
        backfill_days=1,
        provider=provider,
        org_id=str(uuid.uuid4()),
        repo_name=repo_name,
        require_source=require_source,
    )
    return calls


def test_gitlab_unit_scopes_fetch_to_source_project(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CHAOS-2763: a GitLab unit's numeric ``repo_name`` (the project id) scopes
    discovery to that project's row via ``settings.project_id`` — the twin of
    the GitHub source-scoping contract above. Before this fix, GitLab units
    fanned out to every org gitlab project regardless of ``repo_name``.
    """
    repos = _gitlab_repos()  # grp/proj-a (123), grp/proj-b (456), grp/proj-c (789)
    calls = _run_gitlab(
        monkeypatch,
        repo_name="123",
        require_source=True,
        repos=repos,
    )
    assert len(calls) == 1
    assert {r.full_name for r in calls[0]} == {"grp/proj-a"}


def test_gitlab_unit_scopes_with_json_string_settings(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The real ``discover_repos`` production shape: ``settings`` arrives as a
    raw JSON string, not a pre-parsed dict (the literal ``settings={"project_id":
    ...}`` used elsewhere in this module is a test convenience, not what
    ClickHouse actually returns). ``_repo_settings_dict`` must parse it."""
    repos = _gitlab_repos(
        settings_factory=lambda pid: f'{{"project_id": {pid}}}',
    )
    calls = _run_gitlab(
        monkeypatch,
        repo_name="123",
        require_source=True,
        repos=repos,
    )
    assert len(calls) == 1
    assert {r.full_name for r in calls[0]} == {"grp/proj-a"}


def test_gitlab_cli_org_wide_path_still_fans_out(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repos = _gitlab_repos()
    # No source (CLI/org-wide) → discovery stays org-wide, every project reaches
    # the fetcher — unchanged by CHAOS-2763.
    calls = _run_gitlab(
        monkeypatch,
        repo_name=None,
        require_source=False,
        repos=repos,
    )
    assert len(calls) == 1
    assert {r.full_name for r in calls[0]} == {
        "grp/proj-a",
        "grp/proj-b",
        "grp/proj-c",
    }


def test_gitlab_unit_missing_source_project_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repos = _gitlab_repos()  # project ids 123, 456, 789 — none is 999
    with pytest.raises(ValueError, match="source was not discovered"):
        _run_gitlab(
            monkeypatch,
            repo_name="999",
            require_source=True,
            repos=repos,
        )


def test_gitlab_unit_missing_source_project_fetcher_never_invoked(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Same fail-closed case as above, isolated to assert the fetcher was
    never called (not just that repos ended up empty) — mirrors the GitHub
    ``fetcher.calls == []`` assertion."""
    repos = _gitlab_repos()
    calls: list[list[DiscoveredRepo]] = []
    with pytest.raises(ValueError):
        calls = _run_gitlab(
            monkeypatch,
            repo_name="999",
            require_source=True,
            repos=repos,
        )
    assert calls == []


def test_gitlab_unit_path_form_repo_name_matches_full_name(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Path-shaped (non-numeric) ``repo_name`` — the CLI ``--repo-name
    grp/proj-a`` form — exercises the ``full_name`` fallback branch, never the
    numeric ``settings.project_id`` branch."""
    repos = _gitlab_repos()
    calls = _run_gitlab(
        monkeypatch,
        repo_name="grp/proj-a",
        require_source=True,
        repos=repos,
    )
    assert len(calls) == 1
    assert {r.full_name for r in calls[0]} == {"grp/proj-a"}


def test_gitlab_stale_row_without_project_id_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """[codex MED-1] Rows written before ``settings.project_id`` existed (or a
    project whose code dataset never ran) have no ``project_id`` to match — a
    numeric unit id must fail closed rather than silently falling back to
    org-wide fan-out. This is the documented regression risk gated by G1."""
    stale_repos = [
        DiscoveredRepo(
            repo_id=uuid.uuid4(),
            full_name=f"grp/proj-{name}",
            source="gitlab",
            settings={},
        )
        for name in ("a", "b", "c")
    ]
    with pytest.raises(ValueError, match="source was not discovered"):
        _run_gitlab(
            monkeypatch,
            repo_name="123",
            require_source=True,
            repos=stale_repos,
        )


def test_gitlab_numeric_repo_name_never_matches_full_name(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """[codex MED-2 negative pin] A stale row whose ``full_name`` happens to
    equal the numeric unit id string must NOT match — numeric inputs match
    ONLY ``settings.project_id``, by design (a mutable display path could
    otherwise collide with an immutable id). Asserts the fetcher was never
    invoked, not just that the matched repo list came back empty."""
    stale_repo = DiscoveredRepo(
        repo_id=uuid.uuid4(),
        full_name="123",
        source="gitlab",
        settings={},
    )
    calls: list[list[DiscoveredRepo]] = []
    with pytest.raises(ValueError, match="source was not discovered"):
        calls = _run_gitlab(
            monkeypatch,
            repo_name="123",
            require_source=True,
            repos=[stale_repo],
        )
    assert calls == []


def test_gitlab_malformed_json_settings_falls_back_to_empty(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """[codex LOW-1] Malformed ``settings`` JSON (legacy/corrupt row) parses to
    ``{}`` via ``_repo_settings_dict`` rather than raising — deterministic
    fail-closed for a numeric id, same as the empty-dict stale-row case."""
    repos = [
        DiscoveredRepo(
            repo_id=uuid.uuid4(),
            full_name="grp/proj-a",
            source="gitlab",
            # Deliberately a malformed JSON *string*, not a dict -- see the
            # _gitlab_repos type-ignore note above.
            settings="{not json",  # type: ignore[arg-type]
        )
    ]
    with pytest.raises(ValueError, match="source was not discovered"):
        _run_gitlab(
            monkeypatch,
            repo_name="123",
            require_source=True,
            repos=repos,
        )


def test_provider_all_with_repo_name_scopes_both_providers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """[codex LOW-1] ``provider="all"`` + ``repo_name`` (the mixed-provider
    CLI/backfill invocation) now scopes GitLab discovery the same way GitHub
    already was scoped by CHAOS-2720 — a behavior change from the pre-2763
    org-wide GitLab fan-out (plan risk 4). ``require_source=False`` here, so no
    raise even though nothing on the gitlab side matches a github-shaped
    ``repo_name``."""
    _patch_common(monkeypatch)
    repos = [
        DiscoveredRepo(
            repo_id=uuid.uuid4(),
            full_name="acme/repo-a",
            source="github",
            settings={},
        ),
        DiscoveredRepo(
            repo_id=uuid.uuid4(),
            full_name="grp/proj-a",
            source="gitlab",
            settings={"project_id": 123},
        ),
    ]
    monkeypatch.setattr(job, "_discover_repos", lambda **_kwargs: list(repos))
    monkeypatch.setattr(job, "_build_github_work_client", lambda **_kwargs: object())
    monkeypatch.setattr(
        job, "_build_gitlab_work_client", lambda **_kwargs: ("gl-token", None)
    )
    monkeypatch.setattr(job, "_build_jira_work_client", lambda **_kwargs: object())
    monkeypatch.setattr(job, "_build_linear_work_client", lambda **_kwargs: object())
    monkeypatch.setattr(
        job,
        "fetch_jira_work_items_with_extras",
        lambda **_kwargs: ([], [], [], [], [], []),
    )
    monkeypatch.setattr(
        "dev_health_ops.metrics.work_items.fetch_synthetic_work_items",
        lambda **_kwargs: ([], []),
    )
    monkeypatch.setattr(
        "dev_health_ops.providers.github.provider.GitHubProvider",
        _FakeGitHubProvider,
    )
    monkeypatch.setattr(
        "dev_health_ops.providers.linear.provider.LinearProvider",
        _NoOpProvider,
    )
    _FakeGitHubProvider.calls = []

    gitlab_calls: list[list[DiscoveredRepo]] = []

    def _fake_fetch_gitlab(*, repos: list[DiscoveredRepo], **_kwargs: object) -> Any:
        gitlab_calls.append(list(repos))
        return ([], [], [])

    monkeypatch.setattr(job, "fetch_gitlab_work_items", _fake_fetch_gitlab)

    run_job: Any = job.run_work_items_sync_job
    run_job(
        db_url="clickhouse://test",
        day=date(2026, 5, 2),
        backfill_days=1,
        provider="all",
        org_id=str(uuid.uuid4()),
        repo_name="acme/repo-a",
        require_source=False,
    )

    # GitHub: scoped to the source repo per the existing CHAOS-2720 block.
    assert _FakeGitHubProvider.calls == ["acme/repo-a"]
    # GitLab: repo_name is path-shaped (non-numeric) here, so it's matched
    # against full_name -- "acme/repo-a" never matches "grp/proj-a", scoping
    # the gitlab-source repo out entirely. Fetcher still called once (no raise
    # since require_source=False) but with zero gitlab-source repos.
    assert len(gitlab_calls) == 1
    assert [r for r in gitlab_calls[0] if r.source == "gitlab"] == []


def test_gitlab_unit_fetches_by_numeric_project_id_not_stale_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """[codex HIGH re-pass] Matching a repo row by its immutable
    ``settings.project_id`` does NOT make the row's ``full_name`` safe to
    fetch by. If the project was renamed/moved after discovery and the stale
    path was later reused by a DIFFERENT project, fetching by path would
    silently pull the wrong project's issues/MRs and attribute them to this
    row's ``repo_id`` — defeating the isolation the id match exists to
    provide. This exercises the REAL ``fetch_gitlab_work_items`` (unlike the
    scoping tests above, which mock it away entirely) via a recording GitLab
    API client, and asserts on the actual ``project_id_or_path`` the client
    was called with — not just on which ``DiscoveredRepo`` reached the
    fetcher."""
    _patch_common(monkeypatch)
    _patch_gitlab_client_factory(monkeypatch)
    _RecordingGitLabAPIClient.calls = []
    stale_repo = DiscoveredRepo(
        repo_id=uuid.uuid4(),
        # Deliberately stale/mismatched: if the client is ever invoked with
        # this path instead of the numeric id, the bug has regressed.
        full_name="grp/now-a-totally-different-project",
        source="gitlab",
        settings={"project_id": 123},
    )
    monkeypatch.setattr(job, "_discover_repos", lambda **_kwargs: [stale_repo])
    monkeypatch.setattr(
        job, "_build_gitlab_work_client", lambda **_kwargs: ("gl-token", None)
    )

    run_job: Any = job.run_work_items_sync_job
    run_job(
        db_url="clickhouse://test",
        day=date(2026, 5, 2),
        backfill_days=1,
        provider="gitlab",
        org_id=str(uuid.uuid4()),
        repo_name="123",  # numeric project id -- triggers id-scoped fetch
        require_source=True,
    )

    assert _RecordingGitLabAPIClient.calls, "gitlab API client was never invoked"
    assert all(ref == "123" for ref in _RecordingGitLabAPIClient.calls), (
        _RecordingGitLabAPIClient.calls
    )
    assert "grp/now-a-totally-different-project" not in _RecordingGitLabAPIClient.calls


def test_gitlab_cli_org_wide_path_still_fetches_by_full_name(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Org-wide (no-source) runs are unaffected by the id-scoped-fetch fix —
    ``gitlab_id_scoped_project_ids`` is only populated for numeric-id-matched
    units, so an org-wide run keeps fetching every project by its
    ``full_name``/path exactly as before this fix."""
    _patch_common(monkeypatch)
    _patch_gitlab_client_factory(monkeypatch)
    _RecordingGitLabAPIClient.calls = []
    repos = _gitlab_repos()  # grp/proj-a (123), grp/proj-b (456), grp/proj-c (789)
    monkeypatch.setattr(job, "_discover_repos", lambda **_kwargs: list(repos))
    monkeypatch.setattr(
        job, "_build_gitlab_work_client", lambda **_kwargs: ("gl-token", None)
    )

    run_job: Any = job.run_work_items_sync_job
    run_job(
        db_url="clickhouse://test",
        day=date(2026, 5, 2),
        backfill_days=1,
        provider="gitlab",
        org_id=str(uuid.uuid4()),
        repo_name=None,
        require_source=False,
    )

    assert _RecordingGitLabAPIClient.calls
    assert set(_RecordingGitLabAPIClient.calls) <= {
        "grp/proj-a",
        "grp/proj-b",
        "grp/proj-c",
    }
    # The numeric project ids were never used as the API identifier here —
    # only path/full_name, unchanged from pre-fix behavior.
    assert not any(
        ref in {"123", "456", "789"} for ref in _RecordingGitLabAPIClient.calls
    )
