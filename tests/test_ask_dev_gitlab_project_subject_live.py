"""CHAOS-3380 live-ClickHouse proof: a GitLab project resolves as a subject.

Mirrors ``tests/test_ask_dev_linear_project_subject_live.py`` (CHAOS-3365).
Both sides of the comparison come from REAL producers running against a real
migrated ClickHouse:

* reference side — ``gitlab_issue_to_work_item`` (the production GitLab issue
  normalizer) → ``ClickHouseMetricsSink.write_work_items``;
* catalog side — ``team_autoimport_gitlab.populate`` (the production worker,
  CHAOS-3380) → ``ClickHouseMetricsSink.write_projects``, driven by the real
  ``TeamDiscoveryService.discover_gitlab`` group/subgroup walk with only the
  HTTP round trip replaced (``GitLabWorkClient.get_project``).

and the comparison itself is the shipped oracle
(``scripts/ask_dev_project_subject_oracle.py``), parametrized to
``provider="gitlab"`` — the SAME oracle CHAOS-3365 proved against Linear,
exercised here rather than duplicated, so a change to the oracle's contract
cannot silently stop applying to one provider.

Identity choice under test: GitLab's catalog id is the RAW
``path_with_namespace`` (``providers/gitlab/normalize.py``: "For work
tracking metrics, treat the GitLab project path as the 'project' scope."),
the SAME id space ``work_items.project_id`` already carries — mirroring
Linear's raw-UUID catalog id, NOT Jira's ``{org}:jira:{key}`` prefixed id
(CHAOS-3374). This is why the plain ``project_id = {entity_id}`` predicate
below already matches a GitLab project on ``main``, with no dependency on the
CHAOS-3374 identity-join fix landing first (unlike Jira).

Opt-in (filtered from unit/CI by ``ci/run_tests.sh``'s
``-m "not benchmark and not clickhouse"`` in BOTH ``unit_tests()`` and
``ci_tests()``): ``pytest -m clickhouse`` with ``CLICKHOUSE_URI`` pointing at a
SCRATCH database — never the dev ``default``.
"""

from __future__ import annotations

import asyncio
import os
import uuid
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from typing import Any
from unittest.mock import MagicMock

import pytest

from dev_health_ops.api.admin.schemas_flat import DiscoveredMember, DiscoveredTeam
from dev_health_ops.api.dev.contracts_v2 import ResolutionOutcome
from dev_health_ops.api.dev.scope_catalog import ClickHouseAuthorizedEntityCatalog
from dev_health_ops.api.dev.scope_service import EntityKind, ScopeResolutionService
from dev_health_ops.api.services.configuration.team_discovery import (
    GitLabDiscoveryResult,
)
from dev_health_ops.metrics.schemas import ProjectRecord
from dev_health_ops.providers.gitlab.normalize import gitlab_issue_to_work_item
from dev_health_ops.providers.identity import IdentityResolver
from dev_health_ops.providers.status_mapping import StatusMapping
from dev_health_ops.workers import team_autoimport_gitlab
from scripts.ask_dev_project_subject_oracle import (
    OracleNotMeasured,
    project_subject_gaps,
)

CLICKHOUSE_URI = os.environ.get("CLICKHOUSE_URI")

pytestmark = [
    pytest.mark.clickhouse,
    pytest.mark.skipif(
        not CLICKHOUSE_URI,
        reason="Requires a migrated SCRATCH CLICKHOUSE_URI "
        "(e.g. clickhouse://ch:ch@localhost:8123/chaos3380_scratch)",
    ),
]

GITLAB_PROJECT_PATH = "full-chaos/dev-health"
GITLAB_PROJECT_NAME = "Dev Health"
OTHER_PROJECT_PATH = "full-chaos/platform"
OTHER_PROJECT_NAME = "Platform"

#: Database names this file refuses to touch — same guard as CHAOS-3365's
#: sibling file, duplicated deliberately rather than imported: a shared helper
#: importing from another test module would make this file's protection
#: depend on that module continuing to exist unchanged.
_PROTECTED_DATABASES = frozenset({"", "default"})


def _scratch_database(dsn: str) -> str:
    from urllib.parse import urlparse

    database = urlparse(dsn).path.lstrip("/")
    if database.strip().lower() in _PROTECTED_DATABASES:
        raise RuntimeError(
            "refusing to run schema migrations against ClickHouse database "
            f"{database or '<unset>'!r}: this suite calls ensure_schema(force=True), "
            "which rebuilds and drops tables. Point CLICKHOUSE_URI at a named "
            "SCRATCH database (e.g. .../chaos3380_scratch)."
        )
    return database


@pytest.fixture(scope="module")
def sink() -> Any:
    from dev_health_ops.metrics.sinks.clickhouse import ClickHouseMetricsSink

    assert CLICKHOUSE_URI is not None
    _scratch_database(CLICKHOUSE_URI)
    metrics_sink = ClickHouseMetricsSink(CLICKHOUSE_URI)
    metrics_sink.ensure_schema(force=True)
    yield metrics_sink
    metrics_sink.close()


@pytest.fixture
def raw_client() -> Any:
    import clickhouse_connect

    assert CLICKHOUSE_URI is not None
    client = clickhouse_connect.get_client(dsn=CLICKHOUSE_URI)
    try:
        yield client
    finally:
        client.close()


def _normalize(iid: int, project_full_path: str) -> Any:
    """Build one work item through the production GitLab issue normalizer."""

    identity = MagicMock(spec=IdentityResolver)
    identity.resolve.side_effect = lambda **kwargs: "user:dev@example.com"
    status_mapping = MagicMock(spec=StatusMapping)
    status_mapping.normalize_status.return_value = "in_progress"
    status_mapping.normalize_type.return_value = "task"

    issue = {
        "iid": iid,
        "title": f"Issue {project_full_path}#{iid}",
        "state": "opened",
        "created_at": "2026-07-01T10:00:00Z",
        "updated_at": "2026-07-30T12:00:00Z",
        "labels": [],
        "assignees": [],
        "author": None,
    }
    work_item, _ = gitlab_issue_to_work_item(
        issue=issue,
        project_full_path=project_full_path,
        repo_id=None,
        status_mapping=status_mapping,
        identity=identity,
    )
    return work_item


def _seed_reference_side(sink: Any, org_id: str) -> None:
    work_items = [
        replace(_normalize(1, GITLAB_PROJECT_PATH), org_id=org_id),
        replace(_normalize(1, OTHER_PROJECT_PATH), org_id=org_id),
    ]
    sink.org_id = org_id
    sink.write_work_items(work_items)


@dataclass
class _FakeGitLabProject:
    name: str
    archived: bool = False
    web_url: str = ""


def _fake_gitlab_work_client_cls(projects: dict[str, _FakeGitLabProject]) -> type:
    class _FakeGitLabWorkClient:
        def __init__(self, *, auth: Any, org_id: str | None = None) -> None:
            self.auth = auth
            self.org_id = org_id

        def get_project(self, project_id_or_path: str) -> _FakeGitLabProject:
            return projects[project_id_or_path]

    return _FakeGitLabWorkClient


async def _run_autoimport(
    monkeypatch: pytest.MonkeyPatch,
    org_id: str,
    *,
    projects: dict[str, _FakeGitLabProject],
) -> dict[str, Any]:
    """Run the production worker with only GitLab's HTTP round trips replaced.

    ``team_autoimport_gitlab.populate`` calls ``asyncio.run(...)`` directly
    (unlike Jira/Linear's thread-hopping ``_run()`` helper), which raises
    "cannot be called from a running event loop" when invoked from inside an
    ``async def`` test. Run it in its own thread so IT owns the event loop it
    creates, exactly like a Celery worker thread would.
    """

    async def discover_gitlab(
        self: object, token: str, group_path: str, url: str
    ) -> GitLabDiscoveryResult:
        return GitLabDiscoveryResult(
            teams=[
                DiscoveredTeam(
                    provider_type="gitlab",
                    provider_team_id="full-chaos",
                    name="Full Chaos",
                    associations={
                        "repo_patterns": list(projects.keys()),
                        "provider_org": group_path,
                    },
                )
            ]
        )

    async def discover_members_gitlab(
        self: object, token: str, group_path: str, url: str
    ) -> list[DiscoveredMember]:
        return []

    monkeypatch.setattr(
        team_autoimport_gitlab.TeamDiscoveryService,
        "discover_gitlab",
        discover_gitlab,
    )
    monkeypatch.setattr(
        team_autoimport_gitlab.TeamMembershipService,
        "discover_members_gitlab",
        discover_members_gitlab,
    )
    monkeypatch.setattr(
        team_autoimport_gitlab,
        "GitLabWorkClient",
        _fake_gitlab_work_client_cls(projects),
    )

    assert CLICKHOUSE_URI is not None
    return await asyncio.to_thread(
        team_autoimport_gitlab.populate,
        org_id=org_id,
        credentials={"token": "gl-token", "group_path": "full-chaos"},
        scope={
            "mode": "sync_config",
            "analytics_db": CLICKHOUSE_URI,
            "sync_options": {"auto_import_teams": True},
        },
    )


def _cleanup(client: Any, org_id: str) -> None:
    for table in (
        "work_items",
        "projects",
        "teams",
        "members",
        "team_memberships",
        "team_project_ownership",
    ):
        client.command(
            f"ALTER TABLE {table} DELETE WHERE org_id = {{org_id:String}}",
            parameters={"org_id": org_id},
        )


@pytest.mark.asyncio
async def test_gitlab_project_becomes_a_resolvable_subject(
    sink: Any, raw_client: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The observable pair: oracle RED before the autoimport, GREEN after."""

    org_id = f"chaos-3380-{uuid.uuid4().hex[:16]}"
    try:
        _seed_reference_side(sink, org_id)

        before = project_subject_gaps(
            raw_client,
            org_id=org_id,
            provider="gitlab",
            acceptance_project_ids=(GITLAB_PROJECT_PATH,),
        )
        assert {gap.project_id for gap in before} == {
            GITLAB_PROJECT_PATH,
            OTHER_PROJECT_PATH,
        }
        assert {gap.kind for gap in before} == {"missing"}

        summary = await _run_autoimport(
            monkeypatch,
            org_id,
            projects={
                GITLAB_PROJECT_PATH: _FakeGitLabProject(
                    name=GITLAB_PROJECT_NAME,
                    web_url=f"https://gitlab.com/{GITLAB_PROJECT_PATH}",
                ),
                OTHER_PROJECT_PATH: _FakeGitLabProject(
                    name=OTHER_PROJECT_NAME,
                    web_url=f"https://gitlab.com/{OTHER_PROJECT_PATH}",
                ),
            },
        )
        assert summary["native_projects_imported"] == 2
        assert summary["native_projects_complete"] is True

        after = project_subject_gaps(
            raw_client,
            org_id=org_id,
            provider="gitlab",
            acceptance_project_ids=(GITLAB_PROJECT_PATH,),
        )
        # "missing"/"inactive"/"name_ambiguous" must be fully cleared -- those
        # are what CHAOS-3380 exists to fix. "name_mismatch" survives here as
        # a PRE-EXISTING, orthogonal gap: unlike Linear's issue normalizer
        # (which caches project.name straight off the issue payload),
        # ``gitlab_issue_to_work_item`` never sets ``WorkItem.project_name``
        # at all (providers/gitlab/normalize.py has no such assignment), so
        # ``work_items.project_name`` is always empty for GitLab today. That
        # is a gap in the WORK-ITEM producer, not the catalog this ticket
        # adds -- and it does not affect subject resolution, which reads the
        # catalog's OWN name (proved by resolve_mention below, not by
        # work_items.project_name).
        assert {gap.kind for gap in after} == {"name_mismatch"}, (
            f"catalog still incomplete after autoimport: {after}"
        )

        service = ScopeResolutionService(ClickHouseAuthorizedEntityCatalog(sink))
        resolution = await service.resolve_mention(
            org_id,
            "permission-live",
            lookup_text=GITLAB_PROJECT_NAME,
            kinds=(EntityKind.PROJECT,),
        )
        assert resolution.outcome is ResolutionOutcome.EXACT_MATCH
        assert resolution.entity is not None
        assert resolution.entity.canonical_id == GITLAB_PROJECT_PATH
        assert resolution.entity.label == GITLAB_PROJECT_NAME
    finally:
        _cleanup(raw_client, org_id)


@pytest.mark.asyncio
async def test_gitlab_project_id_selects_the_projects_work_items(
    sink: Any, raw_client: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The resolved canonical id must be usable as a ``work_items`` filter.

    Deliberately checked with the SAME plain predicate
    ``native_status_change._WORK_ITEMS_SQL`` uses on current ``main``
    (``project_id = {entity_id} OR project_key = {entity_id}``) rather than
    calling into that module: this repo has a sibling lane
    (CHAOS-3374/chaos-3374-jira-project-identity, not merged as of this test)
    reworking that file's project-identity join for Jira's PREFIXED catalog
    id. GitLab's catalog id is the RAW path, the same id space Linear already
    used successfully with this exact predicate, so this assertion holds
    against ``main`` today and is expected to keep holding once 3374 merges
    (its own CTE preserves the ``project_id = {entity_id}`` arm verbatim).
    """

    org_id = f"chaos-3380-scope-{uuid.uuid4().hex[:12]}"
    try:
        _seed_reference_side(sink, org_id)
        await _run_autoimport(
            monkeypatch,
            org_id,
            projects={
                GITLAB_PROJECT_PATH: _FakeGitLabProject(name=GITLAB_PROJECT_NAME),
                OTHER_PROJECT_PATH: _FakeGitLabProject(name=OTHER_PROJECT_NAME),
            },
        )

        service = ScopeResolutionService(ClickHouseAuthorizedEntityCatalog(sink))
        resolution = await service.resolve_mention(
            org_id,
            "permission-live",
            lookup_text=GITLAB_PROJECT_NAME,
            kinds=(EntityKind.PROJECT,),
        )
        assert resolution.entity is not None
        entity_id = resolution.entity.canonical_id
        assert entity_id == GITLAB_PROJECT_PATH

        matched = raw_client.query(
            "SELECT count() FROM work_items WHERE org_id = {org_id:String} "
            "AND (project_id = {entity_id:String} OR project_key = {entity_id:String})",
            parameters={"org_id": org_id, "entity_id": entity_id},
        ).result_rows[0][0]
        assert matched == 1, (
            "the resolved project id selects no work items; the subject resolves "
            "but every answer about it would be empty"
        )
    finally:
        _cleanup(raw_client, org_id)


@pytest.mark.asyncio
async def test_archived_gitlab_project_is_not_resolvable(
    sink: Any, raw_client: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``archived`` retires the row; ``scope_catalog`` filters ``is_active = 1``."""

    org_id = f"chaos-3380-archived-{uuid.uuid4().hex[:12]}"
    try:
        _seed_reference_side(sink, org_id)
        await _run_autoimport(
            monkeypatch,
            org_id,
            projects={
                GITLAB_PROJECT_PATH: _FakeGitLabProject(
                    name=GITLAB_PROJECT_NAME, archived=True
                ),
                OTHER_PROJECT_PATH: _FakeGitLabProject(name=OTHER_PROJECT_NAME),
            },
        )

        row = raw_client.query(
            "SELECT is_active FROM projects FINAL "
            "WHERE org_id = {org_id:String} AND id = {pid:String} AND provider = 'gitlab'",
            parameters={"org_id": org_id, "pid": GITLAB_PROJECT_PATH},
        ).result_rows
        assert row == [(0,)]

        service = ScopeResolutionService(ClickHouseAuthorizedEntityCatalog(sink))
        resolution = await service.resolve_mention(
            org_id,
            "permission-live",
            lookup_text=GITLAB_PROJECT_NAME,
            kinds=(EntityKind.PROJECT,),
        )
        assert resolution.outcome is ResolutionOutcome.NO_AUTHORIZED_MATCH, (
            "an archived GitLab project is still resolvable"
        )
    finally:
        _cleanup(raw_client, org_id)


@pytest.mark.asyncio
async def test_gitlab_project_id_colliding_with_a_jira_catalog_id_stays_distinct(
    sink: Any, raw_client: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """CHAOS-3380 cross-provider coverage.

    ``projects``' ReplacingMergeTree key is ``(org_id, provider, id)``
    (native_status_change._PROJECT_IDENTITY_CTE's own comment on the sibling
    CHAOS-3374 branch) -- ``id`` alone is only unique WITHIN one provider. A
    GitLab project's raw path can coincidentally equal another provider's raw
    catalog id in the SAME org (nothing enforces cross-provider id
    uniqueness); this pins that BOTH rows survive ``FINAL`` as distinct rows
    scoped by provider, rather than one silently replacing the other.
    """

    org_id = f"chaos-3380-collide-{uuid.uuid4().hex[:10]}"
    now = datetime.now(timezone.utc)
    colliding_id = f"collide-{uuid.uuid4().hex[:8]}"
    try:
        # A Jira-provider row using the colliding string as its (prefixed-in-
        # production, but here deliberately bare) id.
        sink.write_projects(
            [
                ProjectRecord(
                    id=colliding_id,
                    org_id=org_id,
                    provider="jira",
                    project_key="COLLIDE",
                    name="Jira Side",
                    is_active=1,
                    updated_at=now,
                    last_synced=now,
                )
            ]
        )
        # A GitLab project whose raw path happens to be the SAME string,
        # written through the production worker.
        await _run_autoimport(
            monkeypatch,
            org_id,
            projects={colliding_id: _FakeGitLabProject(name="GitLab Side")},
        )

        rows = sorted(
            raw_client.query(
                "SELECT provider, name FROM projects FINAL "
                "WHERE org_id = {org_id:String} AND id = {pid:String}",
                parameters={"org_id": org_id, "pid": colliding_id},
            ).result_rows
        )
        assert rows == [("gitlab", "GitLab Side"), ("jira", "Jira Side")], (
            "the two providers' rows did not both survive FINAL as distinct rows: "
            f"{rows}"
        )
    finally:
        _cleanup(raw_client, org_id)


def test_the_suite_refuses_to_migrate_a_protected_database() -> None:
    with pytest.raises(RuntimeError, match="refusing to run schema migrations"):
        _scratch_database("clickhouse://ch:ch@localhost:8123/default")


def test_the_guard_still_admits_a_named_scratch_database() -> None:
    assert (
        _scratch_database("clickhouse://ch:ch@localhost:8123/chaos3380_scratch")
        == "chaos3380_scratch"
    )


def test_oracle_refuses_to_pass_for_gitlab_when_nothing_was_compared(
    raw_client: Any,
) -> None:
    with pytest.raises(OracleNotMeasured, match="nothing to compare"):
        project_subject_gaps(
            raw_client,
            org_id=f"empty-{uuid.uuid4().hex[:12]}",
            provider="gitlab",
        )
