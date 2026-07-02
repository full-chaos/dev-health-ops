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
    gitlab_url: str | None = None,
) -> list[list[DiscoveredRepo]]:
    """Run the gitlab work-item sync job with a faked fetcher; returns the list
    of ``repos`` kwargs the fetcher was invoked with (one entry per call, so
    ``len(...)`` is the invocation count and an empty list means never called).

    ``gitlab_url`` stands in for this unit's resolved/authenticated GitLab
    instance (what ``_build_gitlab_work_client`` would normally return
    alongside the token). Defaults to ``None`` — "instance unknown" — which
    keeps every pre-CHAOS-2801 test in this module exercising the
    instance-check as a no-op (see ``normalize_gitlab_instance``,
    providers/gitlab/instance.py).
    """
    _patch_common(monkeypatch)
    monkeypatch.setattr(job, "_discover_repos", lambda **_kwargs: list(repos))
    monkeypatch.setattr(
        job,
        "_build_gitlab_work_client",
        lambda **_kwargs: ("gl-token", gitlab_url),
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


# --- CHAOS-2801: instance-scoped GitLab unit matching ------------------------
#
# [codex HIGH, PR #1143 round-3] Numeric ``project_id`` is only unique WITHIN
# one GitLab instance. Two GitLab integrations in the same org (two
# self-hosted instances, or one self-hosted + gitlab.com) can each discover a
# project with the SAME numeric id. Before this fix, a unit authenticated to
# instance A could match instance B's same-id row, fetch project 123 from A,
# and write/normalize the result under B's ``repo_id`` — silently mixing two
# tenants' GitLab data under the wrong row.
#
# Design semantic — the three-case instance rule (see job_work_items.py's
# scoping-block comment and docs/architecture/sync-unit-model.md). When the
# unit's instance is known, per project_id:
#   (a) a same-project_id row with a MATCHING discriminator exists -> scope
#       to the discriminated match ONLY; mismatching AND undiscriminated
#       (legacy) rows are dropped. Without the legacy drop, absent-accept
#       would act as a CO-MATCH (codex HIGH, PR #1148 round-1): both
#       repo_ids would enter gitlab_id_scoped_project_ids and one fetch
#       against instance A's client would also be written under the legacy
#       (possibly instance-B) row's repo_id.
#   (b) NO discriminated row exists at all -> ACCEPT legacy rows (zero
#       blast radius for today's pure-legacy single-instance orgs).
#   (c) only MISMATCHING discriminated row(s) exist -> FAIL CLOSED: the
#       mismatch rows are rejected and the legacy rows are dropped too
#       (codex HIGH, PR #1148 round-2) — a known mismatching discriminator
#       PROVES cross-instance ambiguity for that numeric id, and the legacy
#       row is plausibly the other instance's pre-discriminator row.
#       Nothing matches, so the CHAOS-2737 require_source path raises;
#       remediation is re-discovery (which now stamps discriminators).
# When the unit's instance is unknown, the check never engages
# (pre-CHAOS-2801 behavior, pinned separately below).
#
# ``normalize_gitlab_instance`` (providers/gitlab/instance.py) is the single
# shared normalizer for both the persisted discriminator and the comparison,
# so equivalent URL spellings (case, trailing slash, /api/v4 suffix,
# explicit default :443/:80 port) never false-mismatch (codex MED, PR #1148).

_INSTANCE_A = "https://gitlab-a.example.com"
_INSTANCE_B = "https://gitlab-b.example.com"


def _gitlab_repos_with_instance(
    *, entries: list[tuple[str, int, str | None]]
) -> list[DiscoveredRepo]:
    """Build GitLab ``DiscoveredRepo`` rows with an explicit
    ``gitlab_instance_url`` discriminator. ``entries`` is
    ``(full_name, project_id, instance_url_or_None)`` — ``None`` means "no
    discriminator" (a pre-CHAOS-2801 row)."""
    repos: list[DiscoveredRepo] = []
    for full_name, project_id, instance_url in entries:
        settings: dict[str, object] = {"project_id": project_id}
        if instance_url is not None:
            settings["gitlab_instance_url"] = instance_url
        repos.append(
            DiscoveredRepo(
                repo_id=uuid.uuid4(),
                full_name=full_name,
                source="gitlab",
                settings=settings,
            )
        )
    return repos


def test_gitlab_unit_instance_scope_rejects_cross_instance_project_id_collision(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """[CHAOS-2801 regression] A unit authenticated to instance A must match
    only instance A's row when two GitLab integrations both discover a
    project with the same numeric id."""
    repos = _gitlab_repos_with_instance(
        entries=[
            ("grp/proj-a", 123, _INSTANCE_A),
            ("other-grp/proj-x", 123, _INSTANCE_B),
        ]
    )
    calls = _run_gitlab(
        monkeypatch,
        repo_name="123",
        require_source=True,
        repos=repos,
        gitlab_url=_INSTANCE_A,
    )
    # Invocation-count assertion: the fetcher runs exactly once, for
    # instance A's row only — instance B's same-id row never reaches it.
    assert len(calls) == 1
    assert {r.full_name for r in calls[0]} == {"grp/proj-a"}


def test_gitlab_unit_instance_scope_real_fetch_invoked_once_for_matched_instance(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """[CHAOS-2801 regression] Exercises the REAL ``fetch_gitlab_work_items``
    (recording GitLab API client, not the mocked fetcher used above). Each
    matched project makes exactly 2 API calls (issues + MRs, ``org_id`` is
    set so MR-attribution scanning runs). If the cross-instance row or the
    legacy no-discriminator row were not filtered, they would ALSO reach the
    fetcher and the count would rise to 4 or 6 — the count is the signal,
    since every row carries the identical project id string and the calls
    can't be told apart by identifier alone. The legacy row here pins the
    codex-HIGH (PR #1148) shadow rule at the API level: one fetch, one
    write-target repo, even in a mixed legacy/discriminated org."""
    _patch_common(monkeypatch)
    _patch_gitlab_client_factory(monkeypatch)
    _RecordingGitLabAPIClient.calls = []
    repos = _gitlab_repos_with_instance(
        entries=[
            ("grp/proj-a", 123, _INSTANCE_A),
            ("other-grp/proj-x", 123, _INSTANCE_B),
            ("legacy-grp/proj-y", 123, None),
        ]
    )
    monkeypatch.setattr(job, "_discover_repos", lambda **_kwargs: list(repos))
    monkeypatch.setattr(
        job,
        "_build_gitlab_work_client",
        lambda **_kwargs: ("gl-token", _INSTANCE_A),
    )

    run_job: Any = job.run_work_items_sync_job
    run_job(
        db_url="clickhouse://test",
        day=date(2026, 5, 2),
        backfill_days=1,
        provider="gitlab",
        org_id=str(uuid.uuid4()),
        repo_name="123",
        require_source=True,
    )

    assert len(_RecordingGitLabAPIClient.calls) == 2, _RecordingGitLabAPIClient.calls
    assert set(_RecordingGitLabAPIClient.calls) == {"123"}


def test_gitlab_unit_instance_scope_accepts_row_with_no_discriminator(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """[CHAOS-2801 compat pin] A row with no ``gitlab_instance_url`` (every
    row written before this change) still matches when the unit's own
    instance IS known and NO discriminated same-id match exists — zero blast
    radius for today's pure-legacy single-GitLab-instance orgs."""
    repos = _gitlab_repos_with_instance(entries=[("grp/proj-a", 123, None)])
    calls = _run_gitlab(
        monkeypatch,
        repo_name="123",
        require_source=True,
        repos=repos,
        gitlab_url=_INSTANCE_A,
    )
    assert len(calls) == 1
    assert {r.full_name for r in calls[0]} == {"grp/proj-a"}


def test_gitlab_unit_instance_scope_discriminated_match_shadows_legacy_row(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """[CHAOS-2801 codex HIGH, PR #1148 regression] Absent-accept must NOT
    act as a co-match. With instance A's discriminated row matched AND a
    legacy no-discriminator row sharing the same project_id, only the
    discriminated row may be scoped: if both entered
    ``gitlab_id_scoped_project_ids``, the single fetch against A's client
    would also be written under the legacy row's repo_id — the exact
    cross-instance corruption this PR closes (the legacy row may well be
    instance B's project 123, just discovered before discriminators
    existed). Asserts the fetch is invoked once and the write targets ONLY
    the discriminated row's repo_id."""
    repos = _gitlab_repos_with_instance(
        entries=[
            ("grp/proj-a", 123, _INSTANCE_A),
            ("legacy-grp/proj-y", 123, None),
        ]
    )
    discriminated_repo_id = repos[0].repo_id
    calls = _run_gitlab(
        monkeypatch,
        repo_name="123",
        require_source=True,
        repos=repos,
        gitlab_url=_INSTANCE_A,
    )
    assert len(calls) == 1
    assert {r.full_name for r in calls[0]} == {"grp/proj-a"}
    # Write-target pin: the repos handed to the fetcher determine which
    # repo_id the fetched data is written/normalized under.
    assert {r.repo_id for r in calls[0]} == {discriminated_repo_id}


def test_gitlab_unit_instance_scope_mismatch_plus_legacy_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """[CHAOS-2801 codex HIGH, PR #1148 round-2, case (c)] A known
    MISMATCHING discriminator for the same project_id PROVES cross-instance
    ambiguity exists for that numeric id — the legacy no-discriminator row
    is plausibly that other instance's pre-discriminator row. Accepting it
    (as the round-1 shadow rule did — the previous version of this very
    test pinned that wrong call) risks fetching from instance A and writing
    under B's repo_id. New rule: legacy rows are accepted ONLY when NO
    same-project_id row carries ANY known discriminator; here one does
    (mismatching), so BOTH rows drop and the CHAOS-2737 ``require_source``
    fail-closed path raises. Remediation is re-discovery, which now stamps
    discriminators on every row."""
    repos = _gitlab_repos_with_instance(
        entries=[
            ("other-grp/proj-x", 123, _INSTANCE_B),
            ("legacy-grp/proj-y", 123, None),
        ]
    )
    calls: list[list[DiscoveredRepo]] = []
    with pytest.raises(ValueError, match="source was not discovered"):
        calls = _run_gitlab(
            monkeypatch,
            repo_name="123",
            require_source=True,
            repos=repos,
            gitlab_url=_INSTANCE_A,
        )
    assert calls == []


def test_gitlab_unit_instance_scope_three_row_mismatch_ambiguity_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """[CHAOS-2801 codex HIGH, PR #1148 round-2 — explicit three-row case]
    Unit on instance A; project_id=123 rows: known-B (mismatch), known-C
    (mismatch), and a legacy no-discriminator row. Every row is dropped —
    both mismatches rejected outright, the legacy row dropped for
    cross-instance ambiguity (two other instances demonstrably share this
    numeric id) — and the unit fails closed with ZERO fetch invocations."""
    repos = _gitlab_repos_with_instance(
        entries=[
            ("other-grp/proj-x", 123, _INSTANCE_B),
            ("third-grp/proj-z", 123, "https://gitlab-c.example.com"),
            ("legacy-grp/proj-y", 123, None),
        ]
    )
    calls: list[list[DiscoveredRepo]] = []
    with pytest.raises(ValueError, match="source was not discovered"):
        calls = _run_gitlab(
            monkeypatch,
            repo_name="123",
            require_source=True,
            repos=repos,
            gitlab_url=_INSTANCE_A,
        )
    assert calls == []


def test_gitlab_unit_instance_scope_noop_when_unit_instance_unknown(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """[CHAOS-2801] When THIS unit's own instance can't be resolved, the
    instance check never engages — a row that DOES carry a (different)
    discriminator is still matched, exactly as pre-CHAOS-2801."""
    repos = _gitlab_repos_with_instance(entries=[("grp/proj-a", 123, _INSTANCE_B)])
    calls = _run_gitlab(
        monkeypatch,
        repo_name="123",
        require_source=True,
        repos=repos,
        gitlab_url=None,
    )
    assert len(calls) == 1
    assert {r.full_name for r in calls[0]} == {"grp/proj-a"}


def test_gitlab_unit_instance_scope_mismatch_fails_closed_when_no_other_match(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """[CHAOS-2801] When the only project_id match is on a DIFFERENT known
    instance, the row is dropped and — since nothing else matches — the
    existing CHAOS-2737 ``require_source`` fail-closed path raises, exactly
    as if the project had never been discovered at all. The fetcher is never
    invoked."""
    repos = _gitlab_repos_with_instance(
        entries=[("other-grp/proj-x", 123, _INSTANCE_B)]
    )
    calls: list[list[DiscoveredRepo]] = []
    with pytest.raises(ValueError, match="source was not discovered"):
        calls = _run_gitlab(
            monkeypatch,
            repo_name="123",
            require_source=True,
            repos=repos,
            gitlab_url=_INSTANCE_A,
        )
    assert calls == []


def test_gitlab_unit_instance_scope_equivalent_url_spellings_match(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """[CHAOS-2801 codex MED, PR #1148] Equivalent spellings of the SAME
    endpoint must never false-mismatch: the persisted discriminator carries
    an explicit default :443 port + trailing slash while the unit's
    credential URL is uppercase with no port. A false mismatch here would
    reject every row of a healthy integration and trip the CHAOS-2737
    fail-closed path org-wide on a harmless credential formatting change."""
    repos = _gitlab_repos_with_instance(
        entries=[("grp/proj-a", 123, "https://gitlab-a.example.com:443/")]
    )
    calls = _run_gitlab(
        monkeypatch,
        repo_name="123",
        require_source=True,
        repos=repos,
        gitlab_url="https://GITLAB-A.example.com",
    )
    assert len(calls) == 1
    assert {r.full_name for r in calls[0]} == {"grp/proj-a"}


# --- normalize_gitlab_instance unit pins (codex MED, PR #1148) ---------------
#
# The single shared normalizer (providers/gitlab/instance.py) used at BOTH
# the write sites' persist path and the scoping comparison.


@pytest.mark.parametrize(
    ("left", "right"),
    [
        # Explicit default port is stripped (https:443, http:80).
        ("https://gitlab.example.com", "https://gitlab.example.com:443"),
        ("http://gitlab.example.com", "http://gitlab.example.com:80"),
        # Host + scheme casing.
        ("https://gitlab.example.com", "HTTPS://GITLAB.EXAMPLE.COM"),
        # Trailing slash and API path suffix.
        ("https://gitlab.example.com", "https://gitlab.example.com/"),
        ("https://gitlab.example.com", "https://gitlab.example.com/api/v4"),
        # Userinfo is discarded.
        ("https://gitlab.example.com", "https://user@gitlab.example.com"),
        # Scheme-less input defaults to https.
        ("https://gitlab.example.com", "gitlab.example.com"),
        # All of it at once.
        (
            "https://gitlab.example.com",
            "HTTPS://GitLab.Example.com:443/api/v4/",
        ),
    ],
)
def test_normalize_gitlab_instance_equivalent_spellings(left: str, right: str) -> None:
    from dev_health_ops.providers.gitlab.instance import normalize_gitlab_instance

    assert normalize_gitlab_instance(left) == normalize_gitlab_instance(right)
    # ``left`` is already in canonical form in every pair — normalization
    # must be a fixed point on it (scheme-specific: http rows stay http).
    assert normalize_gitlab_instance(left) == left


def test_normalize_gitlab_instance_non_default_port_still_distinct() -> None:
    """Two GitLab instances CAN legitimately run on different ports of one
    host — a non-default port must remain part of the discriminator."""
    from dev_health_ops.providers.gitlab.instance import normalize_gitlab_instance

    assert (
        normalize_gitlab_instance("https://gitlab.example.com:8443")
        == "https://gitlab.example.com:8443"
    )
    assert normalize_gitlab_instance(
        "https://gitlab.example.com:8443"
    ) != normalize_gitlab_instance("https://gitlab.example.com")
    # http on 443 is NOT http's default — preserved, and distinct from https.
    assert (
        normalize_gitlab_instance("http://gitlab.example.com:443")
        == "http://gitlab.example.com:443"
    )


def test_normalize_gitlab_instance_unknown_inputs() -> None:
    """Missing/blank/unparseable inputs are ``None`` — "unknown", never a
    distinct instance."""
    from dev_health_ops.providers.gitlab.instance import normalize_gitlab_instance

    assert normalize_gitlab_instance(None) is None
    assert normalize_gitlab_instance("") is None
    assert normalize_gitlab_instance("   ") is None
    assert normalize_gitlab_instance(123) is None
    assert normalize_gitlab_instance("https://gitlab.example.com:notaport") is None


# --- insert_repo -> discover_repos JSON round-trip (codex MED, PR #1148) -----
#
# ``ClickHouseStore.insert_repo`` encodes ``repos.settings`` via
# ``_json_or_none`` (json.dumps) and ``discover_repos`` (job_daily.py)
# decodes it via ``_parse_repo_settings`` (json.loads). These round-trip
# tests exercise the REAL encode + decode pair — not a hand-rolled stand-in
# — and then run the real scoping loop on the round-tripped settings, for
# both the canonical (discriminator present) and unknown (key omitted at
# the write site) shapes.


def _roundtrip_settings(settings: dict[str, object]) -> dict[str, object]:
    from dev_health_ops.metrics.job_daily import _parse_repo_settings
    from dev_health_ops.storage.clickhouse import ClickHouseStore

    encoded = ClickHouseStore._json_or_none(settings)
    assert isinstance(encoded, str)
    return _parse_repo_settings(encoded)


def test_gitlab_instance_discriminator_survives_settings_json_roundtrip(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Canonical case: a write-site settings dict carrying the normalized
    discriminator survives the real insert_repo/discover_repos JSON
    round-trip, and the scoping loop then (a) matches it for a same-instance
    unit and (b) rejects it fail-closed for a different-instance unit."""
    roundtripped = _roundtrip_settings(
        {
            "source": "gitlab",
            "project_id": 123,
            "gitlab_instance_url": _INSTANCE_A,
            "default_branch": "main",
        }
    )
    assert roundtripped["gitlab_instance_url"] == _INSTANCE_A
    repo = DiscoveredRepo(
        repo_id=uuid.uuid4(),
        full_name="grp/proj-a",
        source="gitlab",
        settings=roundtripped,
    )

    calls = _run_gitlab(
        monkeypatch,
        repo_name="123",
        require_source=True,
        repos=[repo],
        gitlab_url=_INSTANCE_A,
    )
    assert len(calls) == 1
    assert {r.full_name for r in calls[0]} == {"grp/proj-a"}

    with pytest.raises(ValueError, match="source was not discovered"):
        _run_gitlab(
            monkeypatch,
            repo_name="123",
            require_source=True,
            repos=[repo],
            gitlab_url=_INSTANCE_B,
        )


def test_gitlab_unknown_discriminator_survives_settings_json_roundtrip(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Unknown case: the write site OMITS ``gitlab_instance_url`` when the
    normalizer returns None; the omitted key survives the real JSON
    round-trip as an absent key (never a raw/None placeholder that could be
    mistaken for a distinct instance) and the scoping loop treats the row
    as legacy-unknown — accepted when no discriminated row exists (case b).
    """
    roundtripped = _roundtrip_settings(
        {
            "source": "gitlab",
            "project_id": 123,
            "default_branch": "main",
        }
    )
    assert "gitlab_instance_url" not in roundtripped
    repo = DiscoveredRepo(
        repo_id=uuid.uuid4(),
        full_name="legacy-grp/proj-y",
        source="gitlab",
        settings=roundtripped,
    )

    calls = _run_gitlab(
        monkeypatch,
        repo_name="123",
        require_source=True,
        repos=[repo],
        gitlab_url=_INSTANCE_A,
    )
    assert len(calls) == 1
    assert {r.full_name for r in calls[0]} == {"legacy-grp/proj-y"}
