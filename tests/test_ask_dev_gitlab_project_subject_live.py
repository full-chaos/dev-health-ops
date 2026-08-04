"""CHAOS-3380 live-ClickHouse proof: a GitLab project resolves as a subject.

Mirrors ``tests/test_ask_dev_linear_project_subject_live.py`` (CHAOS-3365).
Both sides of the comparison come from REAL producers running against a real
migrated ClickHouse:

* reference side — ``gitlab_issue_to_work_item`` (the production GitLab issue
  normalizer) → ``ClickHouseMetricsSink.write_work_items``;
* catalog side — ``team_autoimport_gitlab.populate`` (the production worker,
  CHAOS-3380) → ``ClickHouseMetricsSink.write_projects``, driven by the real
  ``TeamDiscoveryService.discover_gitlab`` flat, recursive project listing,
  with only the HTTP round trip replaced.

and the comparison itself is the shipped oracle
(``scripts/ask_dev_project_subject_oracle.py``), parametrized to
``provider="gitlab"`` — the SAME oracle CHAOS-3365 proved against Linear,
exercised here rather than duplicated, so a change to the oracle's contract
cannot silently stop applying to one provider.

Identity choice under test (CHAOS-3380 round 2, Codex HIGH -- mutable path
as canonical identity): GitLab's catalog id is GitLab's own IMMUTABLE
numeric project id, prefixed like Jira's (``f"{org_id}:gitlab:{numeric_id}"``)
-- NOT the raw, MUTABLE ``path_with_namespace`` a first cut of this ticket
used. ``project_key`` carries the CURRENT path instead, and
``providers/gitlab/normalize.py`` now writes that SAME current path onto
``work_items.project_key`` (not just ``project_id``) -- so a GitLab project
resolves through ``native_status_change._project_identity_match``'s
project_key arm, mirroring Jira rather than Linear. This is because a
GitLab project's PATH is mutable (rename, group transfer) while Linear's and
Jira's own native ids are not.

CHAOS-3374 (``native_status_change._PROJECT_IDENTITY_CTE`` /
``_project_identity_match``) merged to main as 61aae46af (#1460): every
project-scoped fact arm and ``PROJECT_REPOSITORIES_SQL`` join through the
catalog's own ``provider``/``project_key`` columns rather than comparing
``work_items.project_id``/``project_key`` to the entity id directly.
``test_gitlab_project_resolves_end_to_end_through_the_native_status_change_join``
below exercises that merged join directly (``ScopeResolutionService.
resolve_contract`` → ``StatusChangeService.status_snapshot``, both reading
through ``native_status_change.py``) with a REAL catalog row from this
ticket's worker.

Opt-in (filtered from unit/CI by ``ci/run_tests.sh``'s
``-m "not benchmark and not clickhouse"`` in BOTH ``unit_tests()`` and
``ci_tests()``): ``pytest -m clickhouse`` with ``CLICKHOUSE_URI`` pointing at a
SCRATCH database — never the dev ``default``.
"""

from __future__ import annotations

import asyncio
import os
import uuid
from dataclasses import replace
from datetime import datetime, timezone
from typing import Any
from unittest.mock import MagicMock

import pytest

from dev_health_ops.api.admin.schemas_flat import DiscoveredMember, DiscoveredTeam
from dev_health_ops.api.dev.contracts_v2 import ResolutionOutcome
from dev_health_ops.api.dev.native_status_change import (
    ClickHouseStatusChangeSource,
)
from dev_health_ops.api.dev.scope_catalog import ClickHouseAuthorizedEntityCatalog
from dev_health_ops.api.dev.scope_service import (
    EntityKind,
    ScopeRef,
    ScopeResolutionService,
    ScopeResolveRequest,
)
from dev_health_ops.api.dev.status_change_service import (
    StatusChangeService,
    StatusSnapshotRequest,
)
from dev_health_ops.api.services.configuration.team_discovery import (
    GitLabDiscoveredProject,
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

GITLAB_PROJECT_NUMERIC_ID = "9001"
GITLAB_PROJECT_PATH = "full-chaos/dev-health"
GITLAB_PROJECT_NAME = "Dev Health"
OTHER_PROJECT_NUMERIC_ID = "9002"
OTHER_PROJECT_PATH = "full-chaos/platform"
OTHER_PROJECT_NAME = "Platform"

#: Database names this file refuses to touch — same guard as CHAOS-3365's
#: sibling file, duplicated deliberately rather than imported: a shared helper
#: importing from another test module would make this file's protection
#: depend on that module continuing to exist unchanged.
_PROTECTED_DATABASES = frozenset({"", "default"})


def _catalog_id(org_id: str, numeric_id: str) -> str:
    return f"{org_id}:gitlab:{numeric_id}"


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


async def _run_autoimport(
    monkeypatch: pytest.MonkeyPatch,
    org_id: str,
    *,
    projects: list[GitLabDiscoveredProject],
    source_external_ids: list[str] | None = None,
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
                        "repo_patterns": [p.path_with_namespace for p in projects],
                        "provider_org": group_path,
                    },
                )
            ],
            projects=projects,
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

    assert CLICKHOUSE_URI is not None
    scope: dict[str, Any] = {
        "mode": "sync_config",
        "analytics_db": CLICKHOUSE_URI,
        "sync_options": {"auto_import_teams": True},
    }
    if source_external_ids is not None:
        scope["source_external_ids"] = source_external_ids
    return await asyncio.to_thread(
        team_autoimport_gitlab.populate,
        org_id=org_id,
        credentials={"token": "gl-token", "group_path": "full-chaos"},
        scope=scope,
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
            projects=[
                GitLabDiscoveredProject(
                    id=GITLAB_PROJECT_NUMERIC_ID,
                    path_with_namespace=GITLAB_PROJECT_PATH,
                    name=GITLAB_PROJECT_NAME,
                    archived=False,
                    web_url=f"https://gitlab.com/{GITLAB_PROJECT_PATH}",
                ),
                GitLabDiscoveredProject(
                    id=OTHER_PROJECT_NUMERIC_ID,
                    path_with_namespace=OTHER_PROJECT_PATH,
                    name=OTHER_PROJECT_NAME,
                    archived=False,
                    web_url=f"https://gitlab.com/{OTHER_PROJECT_PATH}",
                ),
            ],
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
        # are what CHAOS-3380 exists to fix, and the oracle now resolves a
        # GitLab reference through project_key (CHAOS-3380 round 2), not raw
        # id equality. "name_mismatch" survives here as a PRE-EXISTING,
        # orthogonal gap: unlike Linear's issue normalizer (which caches
        # project.name straight off the issue payload),
        # ``gitlab_issue_to_work_item`` never sets ``WorkItem.project_name``
        # at all, so ``work_items.project_name`` is always empty for GitLab
        # today. That is a gap in the WORK-ITEM producer, not the catalog
        # this ticket adds -- and it does not affect subject resolution,
        # which reads the catalog's OWN name (proved below).
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
        assert resolution.entity.canonical_id == _catalog_id(
            org_id, GITLAB_PROJECT_NUMERIC_ID
        )
        assert resolution.entity.label == GITLAB_PROJECT_NAME
    finally:
        _cleanup(raw_client, org_id)


@pytest.mark.asyncio
async def test_gitlab_project_id_selects_the_projects_work_items(
    sink: Any, raw_client: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The resolved subject's underlying identity must select real work items.

    The resolved canonical id (``{org}:gitlab:{numeric_id}``) never appears
    literally on ``work_items`` -- only the catalog row's OWN ``project_key``
    (the current path) does, which is what the real
    ``native_status_change._project_identity_match`` join actually compares
    (proved end to end in the next test). This test isolates that one
    property: the catalog row this subject resolved to carries a
    ``project_key`` that genuinely selects the seeded work item.
    """

    org_id = f"chaos-3380-scope-{uuid.uuid4().hex[:12]}"
    try:
        _seed_reference_side(sink, org_id)
        await _run_autoimport(
            monkeypatch,
            org_id,
            projects=[
                GitLabDiscoveredProject(
                    id=GITLAB_PROJECT_NUMERIC_ID,
                    path_with_namespace=GITLAB_PROJECT_PATH,
                    name=GITLAB_PROJECT_NAME,
                    archived=False,
                    web_url="",
                ),
                GitLabDiscoveredProject(
                    id=OTHER_PROJECT_NUMERIC_ID,
                    path_with_namespace=OTHER_PROJECT_PATH,
                    name=OTHER_PROJECT_NAME,
                    archived=False,
                    web_url="",
                ),
            ],
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
        assert entity_id == _catalog_id(org_id, GITLAB_PROJECT_NUMERIC_ID)

        project_key_row = raw_client.query(
            "SELECT project_key FROM projects FINAL "
            "WHERE org_id = {org_id:String} AND id = {pid:String}",
            parameters={"org_id": org_id, "pid": entity_id},
        ).result_rows
        assert project_key_row == [(GITLAB_PROJECT_PATH,)]
        project_key = project_key_row[0][0]

        matched = raw_client.query(
            "SELECT count() FROM work_items WHERE org_id = {org_id:String} "
            "AND (project_id = {key:String} OR project_key = {key:String})",
            parameters={"org_id": org_id, "key": project_key},
        ).result_rows[0][0]
        assert matched == 1, (
            "the resolved project's catalog project_key selects no work items; "
            "the subject resolves but every answer about it would be empty"
        )
    finally:
        _cleanup(raw_client, org_id)


@pytest.mark.asyncio
async def test_gitlab_project_resolves_end_to_end_through_the_native_status_change_join(
    sink: Any, raw_client: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The full CHAOS-3380 + CHAOS-3374 chain, exercised together.

    CHAOS-3374 (merged, main 61aae46af / #1460) reworked EVERY project-scoped
    arm in ``native_status_change.py`` -- ``_WORK_ITEMS_SQL``,
    ``PROJECT_REPOSITORIES_SQL``, and the rest -- to join through
    ``_PROJECT_IDENTITY_CTE`` / ``_project_identity_match`` (catalog
    ``provider`` + native key) instead of comparing ``work_items.project_id``/
    ``project_key`` to the entity id directly. This test uses the SAME two
    calls the sibling Jira live test uses (``resolve_contract`` then
    ``status_snapshot``) to prove the merged join actually joins for a
    GitLab project THROUGH THE PROJECT_KEY ARM (CHAOS-3380 round 2's
    immutable-id model): a work item committed against it comes back in the
    snapshot, and a work item under a DIFFERENT GitLab project (same
    provider, so the provider guard alone can't be what excludes it) does
    not.
    """

    org_id = f"chaos-3380-e2e-{uuid.uuid4().hex[:12]}"
    try:
        _seed_reference_side(sink, org_id)
        await _run_autoimport(
            monkeypatch,
            org_id,
            projects=[
                GitLabDiscoveredProject(
                    id=GITLAB_PROJECT_NUMERIC_ID,
                    path_with_namespace=GITLAB_PROJECT_PATH,
                    name=GITLAB_PROJECT_NAME,
                    archived=False,
                    web_url="",
                ),
                GitLabDiscoveredProject(
                    id=OTHER_PROJECT_NUMERIC_ID,
                    path_with_namespace=OTHER_PROJECT_PATH,
                    name=OTHER_PROJECT_NAME,
                    archived=False,
                    web_url="",
                ),
            ],
        )
        entity_id = _catalog_id(org_id, GITLAB_PROJECT_NUMERIC_ID)

        # The merged CHAOS-3374 join REQUIRES work_items.provider = the
        # catalog row's own provider (native_status_change._project_identity_
        # match: "provider = catalog_provider"), AND (round 2) matches
        # through project_key, not id. Verified here against the REAL rows
        # this test just wrote, not assumed.
        work_item_providers = {
            row[0]
            for row in raw_client.query(
                "SELECT DISTINCT provider FROM work_items "
                "WHERE org_id = {org_id:String}",
                parameters={"org_id": org_id},
            ).result_rows
        }
        catalog_row = raw_client.query(
            "SELECT provider, project_key FROM projects FINAL "
            "WHERE org_id = {org_id:String} AND id = {pid:String}",
            parameters={"org_id": org_id, "pid": entity_id},
        ).result_rows
        assert work_item_providers == {"gitlab"}, work_item_providers
        assert catalog_row == [("gitlab", GITLAB_PROJECT_PATH)], catalog_row

        catalog = ClickHouseAuthorizedEntityCatalog(sink)
        service = ScopeResolutionService(catalog)
        resolution = await service.resolve_contract(
            org_id,
            "permission-live",
            ScopeResolveRequest(
                explicit_refs=(ScopeRef(EntityKind.PROJECT, entity_id),)
            ),
        )
        assert resolution.outcome.value == "exact"
        scope = resolution.resolved_scope
        assert scope is not None
        # Repo-less, like every GitLab issue/MR today (providers/gitlab/
        # provider.py passes repo_id=None at normalize time) -- the zero-UUID
        # sentinel bucket, exactly like Linear, not a real repos row.
        assert scope.repositories == []

        snapshot = await StatusChangeService(
            ClickHouseStatusChangeSource(sink)
        ).status_snapshot(org_id, "permission-live", StatusSnapshotRequest(scope))

        seen = {child.entity_id for child in snapshot.children}
        own_work_item_id = f"gitlab:{GITLAB_PROJECT_PATH}#1"
        other_work_item_id = f"gitlab:{OTHER_PROJECT_PATH}#1"
        assert any(child.endswith(own_work_item_id) for child in seen), seen
        assert not any(child.endswith(other_work_item_id) for child in seen), (
            "a different GitLab project's work item leaked into this project's scope",
            seen,
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
            projects=[
                GitLabDiscoveredProject(
                    id=GITLAB_PROJECT_NUMERIC_ID,
                    path_with_namespace=GITLAB_PROJECT_PATH,
                    name=GITLAB_PROJECT_NAME,
                    archived=True,
                    web_url="",
                ),
                GitLabDiscoveredProject(
                    id=OTHER_PROJECT_NUMERIC_ID,
                    path_with_namespace=OTHER_PROJECT_PATH,
                    name=OTHER_PROJECT_NAME,
                    archived=False,
                    web_url="",
                ),
            ],
        )
        entity_id = _catalog_id(org_id, GITLAB_PROJECT_NUMERIC_ID)

        row = raw_client.query(
            "SELECT is_active FROM projects FINAL "
            "WHERE org_id = {org_id:String} AND id = {pid:String} AND provider = 'gitlab'",
            parameters={"org_id": org_id, "pid": entity_id},
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

        # And the OTHER entry point: an explicit committed ref (the path
        # native_status_change.py's own _PROJECT_IDENTITY_CTE is-active
        # guard exists for), not just name lookup -- both must fail closed.
        contract = await service.resolve_contract(
            org_id,
            "permission-live",
            ScopeResolveRequest(
                explicit_refs=(ScopeRef(EntityKind.PROJECT, entity_id),)
            ),
        )
        assert contract.outcome.value != "exact", (
            "an archived GitLab project still resolves as an explicit scope ref",
            contract.outcome,
        )
    finally:
        _cleanup(raw_client, org_id)


@pytest.mark.asyncio
async def test_gitlab_project_key_colliding_with_a_jira_catalog_key_stays_distinct(
    sink: Any, raw_client: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """CHAOS-3380 round 2 cross-provider coverage.

    Both Jira and GitLab now resolve through the SAME identity arm
    (``project_key``, CHAOS-3380 round 2's immutable-id model mirrors
    Jira's). A raw string shared between a GitLab project's current path and
    a Jira project's raw key (nothing prevents this -- they are independent
    id spaces) must never let one provider's project resolve into the
    other's rows. The catalog ``id`` column itself can no longer collide
    cross-provider now that GitLab's id is prefixed like Jira's (different
    provider segment in the same string), so the meaningful collision surface
    moved entirely to ``project_key`` -- this is that test, not a
    ``id``-collision repeat of the pre-round-2 version.
    """

    org_id = f"chaos-3380-collide-{uuid.uuid4().hex[:10]}"
    now = datetime.now(timezone.utc)
    colliding_key = f"collide-{uuid.uuid4().hex[:8]}"
    try:
        # A Jira-provider row using the colliding string as its raw key.
        sink.write_projects(
            [
                ProjectRecord(
                    id=f"{org_id}:jira:{colliding_key}",
                    org_id=org_id,
                    provider="jira",
                    project_key=colliding_key,
                    name="Jira Side",
                    is_active=1,
                    updated_at=now,
                    last_synced=now,
                )
            ]
        )
        # A GitLab project whose CURRENT path happens to be the same string,
        # written through the production worker.
        await _run_autoimport(
            monkeypatch,
            org_id,
            projects=[
                GitLabDiscoveredProject(
                    id="7001",
                    path_with_namespace=colliding_key,
                    name="GitLab Side",
                    archived=False,
                    web_url="",
                )
            ],
        )

        rows = sorted(
            raw_client.query(
                "SELECT provider, name FROM projects FINAL "
                "WHERE org_id = {org_id:String} AND project_key = {key:String}",
                parameters={"org_id": org_id, "key": colliding_key},
            ).result_rows
        )
        assert rows == [("gitlab", "GitLab Side"), ("jira", "Jira Side")], (
            "the two providers' rows did not both survive as distinct rows "
            f"keyed by project_key: {rows}"
        )

        # And the resolver-visible property: naming the shared key resolves
        # AMBIGUOUS (two active catalog rows share... no, they have DIFFERENT
        # names here, so each name resolves to exactly its own provider's row.
        service = ScopeResolutionService(ClickHouseAuthorizedEntityCatalog(sink))
        gitlab_resolution = await service.resolve_mention(
            org_id,
            "permission-live",
            lookup_text="GitLab Side",
            kinds=(EntityKind.PROJECT,),
        )
        assert gitlab_resolution.outcome is ResolutionOutcome.EXACT_MATCH
        assert gitlab_resolution.entity is not None
        assert gitlab_resolution.entity.canonical_id == _catalog_id(org_id, "7001")
    finally:
        _cleanup(raw_client, org_id)


@pytest.mark.asyncio
async def test_gitlab_source_selection_filters_unselected_projects_live(
    sink: Any, raw_client: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """CHAOS-3380 round 2 (Codex MEDIUM), verified against real ClickHouse
    rows rather than only the fake-sink unit test: an unselected project
    never becomes a resolvable Ask Dev subject."""

    org_id = f"chaos-3380-selection-{uuid.uuid4().hex[:10]}"
    try:
        summary = await _run_autoimport(
            monkeypatch,
            org_id,
            projects=[
                GitLabDiscoveredProject(
                    id=GITLAB_PROJECT_NUMERIC_ID,
                    path_with_namespace=GITLAB_PROJECT_PATH,
                    name=GITLAB_PROJECT_NAME,
                    archived=False,
                    web_url="",
                ),
                GitLabDiscoveredProject(
                    id=OTHER_PROJECT_NUMERIC_ID,
                    path_with_namespace=OTHER_PROJECT_PATH,
                    name=OTHER_PROJECT_NAME,
                    archived=False,
                    web_url="",
                ),
            ],
            source_external_ids=[GITLAB_PROJECT_NUMERIC_ID],
        )
        assert summary["native_projects_imported"] == 1

        rows = raw_client.query(
            "SELECT id FROM projects FINAL WHERE org_id = {org_id:String}",
            parameters={"org_id": org_id},
        ).result_rows
        assert {r[0] for r in rows} == {_catalog_id(org_id, GITLAB_PROJECT_NUMERIC_ID)}

        service = ScopeResolutionService(ClickHouseAuthorizedEntityCatalog(sink))
        unselected = await service.resolve_mention(
            org_id,
            "permission-live",
            lookup_text=OTHER_PROJECT_NAME,
            kinds=(EntityKind.PROJECT,),
        )
        assert unselected.outcome is ResolutionOutcome.NO_AUTHORIZED_MATCH, (
            "an unselected GitLab project resolved as an Ask Dev subject"
        )
    finally:
        _cleanup(raw_client, org_id)


@pytest.mark.asyncio
async def test_oracle_does_not_flag_a_gitlab_epic_group_path_as_missing(
    sink: Any, raw_client: Any
) -> None:
    """CHAOS-3380 round 2 (Codex MEDIUM, group-epics carve-out).

    ``providers/gitlab/normalize.gitlab_epic_to_work_item`` sets
    ``work_items.project_id`` to the GROUP path and ``type='epic'`` -- a
    wholly different identity space with NO catalog entity in this ticket.
    The oracle must not report that group path as a "missing" gap (it is a
    documented carve-out, not a defect), while a genuine uncataloged
    PROJECT-path reference still IS reported -- proving the exclusion is
    precise, not a blanket "ignore everything" that would hide real gaps too.
    """

    org_id = f"chaos-3380-epic-{uuid.uuid4().hex[:10]}"
    now = datetime.now(timezone.utc)
    try:
        sink.org_id = org_id
        epic_group_path = "full-chaos/platform-group"
        uncataloged_project = replace(
            _normalize(1, "full-chaos/no-catalog-row"), org_id=org_id
        )
        epic_item = replace(
            uncataloged_project,
            work_item_id=f"gitlab:{epic_group_path}:epic:1",
            project_id=epic_group_path,
            project_key=None,
            type="epic",
            updated_at=now,
        )
        sink.write_work_items([uncataloged_project, epic_item])

        gaps = project_subject_gaps(raw_client, org_id=org_id, provider="gitlab")
        gap_ids = {gap.project_id for gap in gaps}
        assert epic_group_path not in gap_ids, (
            "the epic's group path was reported as a gap; it is an explicit, "
            "out-of-scope carve-out, not a defect"
        )
        assert "full-chaos/no-catalog-row" in gap_ids, (
            "the exclusion swallowed a genuine, uncataloged PROJECT reference too"
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
