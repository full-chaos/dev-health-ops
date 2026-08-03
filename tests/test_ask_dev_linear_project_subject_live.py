"""CHAOS-3365 live-ClickHouse proof: a Linear project resolves as a subject.

Both sides of the comparison come from REAL producers running against a real
migrated ClickHouse:

* reference side — ``linear_issue_to_work_item`` (the production Linear issue
  normalizer) → ``ClickHouseMetricsSink.write_work_items``;
* catalog side — ``team_autoimport_linear.populate`` (the production worker)
  → ``ClickHouseMetricsSink.write_projects``, driven by the real
  ``LinearClient.iter_projects``/``PROJECTS_QUERY`` with only the HTTP round
  trip replaced.

and the comparison itself is the shipped oracle
(``scripts/ask_dev_project_subject_oracle.py``), the same code run against the
dev database. A test that seeded the catalog by hand would prove the oracle
works and nothing about whether the worker does.

Opt-in (filtered from unit/CI by ``ci/run_tests.sh``'s
``-m "not benchmark and not clickhouse"`` in BOTH ``unit_tests()`` and
``ci_tests()``): ``pytest -m clickhouse`` with ``CLICKHOUSE_URI`` pointing at a
SCRATCH database — never the dev ``default``.
"""

from __future__ import annotations

import os
import uuid
from dataclasses import replace
from typing import Any
from unittest.mock import MagicMock

import pytest

from dev_health_ops.api.admin.schemas_flat import DiscoveredMember, DiscoveredTeam
from dev_health_ops.api.dev.contracts_v2 import ResolutionOutcome
from dev_health_ops.api.dev.scope_catalog import ClickHouseAuthorizedEntityCatalog
from dev_health_ops.api.dev.scope_service import EntityKind, ScopeResolutionService
from dev_health_ops.providers.identity import IdentityResolver
from dev_health_ops.providers.linear.client import PROJECTS_QUERY, LinearClient
from dev_health_ops.providers.linear.normalize import linear_issue_to_work_item
from dev_health_ops.providers.status_mapping import StatusMapping
from dev_health_ops.workers import team_autoimport_linear
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
        "(e.g. clickhouse://ch:ch@localhost:8123/ci_local_validate)",
    ),
]

# The live dev workspace's Ask Dev project — the acceptance-set entry the
# oracle must keep rediscovering.
ASK_DEV_PROJECT_ID = "13e65c04-40ec-4a95-8216-f7c2ce233244"
ASK_DEV_PROJECT_NAME = "Ask Dev"
OTHER_PROJECT_ID = "32f7879e-00ed-486f-884a-bfb4e93e2f48"
OTHER_PROJECT_NAME = "Go Worker Runtime Migration"


#: Database names this file refuses to touch. ``ensure_schema(force=True)`` below
#: runs pending migrations, and this repo's ClickHouse migrations include table
#: rebuilds and drops — so pointing ``CLICKHOUSE_URI`` at the dev database and
#: running ``pytest -m clickhouse`` would alter real development data. A warning
#: in a docstring is not an enforcement boundary; this is.
_PROTECTED_DATABASES = frozenset({"", "default"})


def _scratch_database(dsn: str) -> str:
    """Return the DSN's database, or fail closed if it is a protected one."""

    from urllib.parse import urlparse

    database = urlparse(dsn).path.lstrip("/")
    if database.strip().lower() in _PROTECTED_DATABASES:
        raise RuntimeError(
            "refusing to run schema migrations against ClickHouse database "
            f"{database or '<unset>'!r}: this suite calls ensure_schema(force=True), "
            "which rebuilds and drops tables. Point CLICKHOUSE_URI at a named "
            "SCRATCH database (e.g. .../ci_local_validate)."
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


def _normalize(identifier: str, project_id: str, project_name: str) -> Any:
    """Build one work item through the production Linear normalizer."""

    identity = MagicMock(spec=IdentityResolver)
    identity.resolve.side_effect = lambda **kwargs: "user:dev@example.com"
    status_mapping = MagicMock(spec=StatusMapping)
    status_mapping.normalize_status.return_value = "in_progress"
    status_mapping.normalize_type.return_value = "task"

    issue = {
        "id": f"issue-{identifier}",
        "identifier": identifier,
        "title": f"Work item {identifier}",
        "createdAt": "2026-07-01T10:00:00Z",
        "updatedAt": "2026-07-30T12:00:00Z",
        "state": {"id": "s1", "name": "In Progress", "type": "started"},
        "team": {"id": "t1", "key": "CHAOS", "name": "Fullchaos"},
        "project": {"id": project_id, "name": project_name},
    }
    work_item, _ = linear_issue_to_work_item(
        issue=issue, status_mapping=status_mapping, identity=identity
    )
    return work_item


def _seed_reference_side(sink: Any, org_id: str) -> None:
    work_items = [
        replace(
            _normalize(f"CHAOS-{3365 + index}", project_id, project_name),
            org_id=org_id,
        )
        for index, (project_id, project_name) in enumerate(
            (
                (ASK_DEV_PROJECT_ID, ASK_DEV_PROJECT_NAME),
                (OTHER_PROJECT_ID, OTHER_PROJECT_NAME),
            )
        )
    ]
    sink.org_id = org_id
    sink.write_work_items(work_items)


def _project_page(*nodes: dict[str, Any]) -> dict[str, Any]:
    return {
        "projects": {
            "nodes": list(nodes),
            "pageInfo": {"hasNextPage": False, "endCursor": None},
        }
    }


def _run_autoimport(monkeypatch: pytest.MonkeyPatch, org_id: str) -> dict[str, Any]:
    """Run the production worker with only Linear's HTTP round trip replaced."""

    async def discover_linear(self: object, api_key: str) -> list[DiscoveredTeam]:
        return [
            DiscoveredTeam(
                provider_type="linear",
                provider_team_id="CHAOS",
                name="Fullchaos",
                associations={"project_keys": ["CHAOS"]},
            )
        ]

    async def discover_members_linear(
        self: object, api_key: str, team_key: str
    ) -> list[DiscoveredMember]:
        return [
            DiscoveredMember(
                provider_type="linear",
                provider_identity="dev@example.com",
                display_name="Dev User",
                email="dev@example.com",
            )
        ]

    page = _project_page(
        {
            "id": ASK_DEV_PROJECT_ID,
            "name": ASK_DEV_PROJECT_NAME,
            "status": {"id": "s1", "name": "In Progress", "type": "started"},
            "trashed": None,
            "updatedAt": "2026-07-30T12:00:00.000Z",
            "archivedAt": None,
        },
        {
            "id": OTHER_PROJECT_ID,
            "name": OTHER_PROJECT_NAME,
            # A COMPLETED project must stay resolvable: people ask about
            # finished work more often than unfinished work.
            "status": {"id": "s2", "name": "Shipped", "type": "completed"},
            "trashed": None,
            "targetDate": "2026-09-01",
            "url": f"https://linear.app/fullchaos/project/{OTHER_PROJECT_ID}",
            "updatedAt": "2026-07-29T12:00:00.000Z",
            "archivedAt": None,
        },
    )

    def _execute(
        self: LinearClient, query: str, variables: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        return page if query is PROJECTS_QUERY else {}

    monkeypatch.setattr(
        team_autoimport_linear.TeamDiscoveryService, "discover_linear", discover_linear
    )
    monkeypatch.setattr(
        team_autoimport_linear.TeamMembershipService,
        "discover_members_linear",
        discover_members_linear,
    )
    monkeypatch.setattr(LinearClient, "_execute", _execute)

    assert CLICKHOUSE_URI is not None
    return team_autoimport_linear.populate(
        org_id=org_id,
        credentials={"api_key": "lin-key"},
        scope={"mode": "sync_config", "analytics_db": CLICKHOUSE_URI},
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
async def test_linear_project_becomes_a_resolvable_subject(
    sink: Any, raw_client: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The observable pair: oracle RED before the autoimport, GREEN after.

    Asserting only the post-state would pass just as well against a database
    that had never been empty, so the pre-state is measured first.
    """

    org_id = f"chaos-3365-{uuid.uuid4().hex[:16]}"
    try:
        _seed_reference_side(sink, org_id)

        before = project_subject_gaps(
            raw_client, org_id=org_id, acceptance_project_ids=(ASK_DEV_PROJECT_ID,)
        )
        assert {gap.project_id for gap in before} == {
            ASK_DEV_PROJECT_ID,
            OTHER_PROJECT_ID,
        }
        assert {gap.kind for gap in before} == {"missing"}

        summary = _run_autoimport(monkeypatch, org_id)
        assert summary["native_projects_imported"] == 2

        after = project_subject_gaps(
            raw_client, org_id=org_id, acceptance_project_ids=(ASK_DEV_PROJECT_ID,)
        )
        assert after == [], f"catalog still incomplete after autoimport: {after}"

        # And the thing a user actually does: name the project.
        service = ScopeResolutionService(ClickHouseAuthorizedEntityCatalog(sink))
        resolution = await service.resolve_mention(
            org_id,
            "permission-live",
            lookup_text=ASK_DEV_PROJECT_NAME,
            kinds=(EntityKind.PROJECT,),
        )
        assert resolution.outcome is ResolutionOutcome.EXACT_MATCH
        assert resolution.entity is not None
        assert resolution.entity.canonical_id == ASK_DEV_PROJECT_ID
        assert resolution.entity.label == ASK_DEV_PROJECT_NAME
    finally:
        _cleanup(raw_client, org_id)


@pytest.mark.asyncio
async def test_completed_project_is_still_resolvable_by_name(
    sink: Any, raw_client: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``scope_catalog`` filters on ``is_active = 1``; a completed project must
    survive that filter or every finished piece of work becomes unaskable."""

    org_id = f"chaos-3365-done-{uuid.uuid4().hex[:12]}"
    try:
        _seed_reference_side(sink, org_id)
        _run_autoimport(monkeypatch, org_id)

        service = ScopeResolutionService(ClickHouseAuthorizedEntityCatalog(sink))
        resolution = await service.resolve_mention(
            org_id,
            "permission-live",
            lookup_text=OTHER_PROJECT_NAME,
            kinds=(EntityKind.PROJECT,),
        )
        assert resolution.outcome is ResolutionOutcome.EXACT_MATCH
        assert resolution.entity is not None
        assert resolution.entity.canonical_id == OTHER_PROJECT_ID
    finally:
        _cleanup(raw_client, org_id)


@pytest.mark.asyncio
async def test_project_scope_id_selects_the_projects_work_items(
    sink: Any, raw_client: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The resolved canonical id must be usable as a ``work_items`` filter.

    This is the whole point of choosing the Linear UUID id space: the native
    status/change SQL selects on ``project_id = {entity_id}``. An id that
    resolved but matched nothing would look like a working feature and answer
    every question with "no data".
    """

    org_id = f"chaos-3365-scope-{uuid.uuid4().hex[:12]}"
    try:
        _seed_reference_side(sink, org_id)
        _run_autoimport(monkeypatch, org_id)

        service = ScopeResolutionService(ClickHouseAuthorizedEntityCatalog(sink))
        resolution = await service.resolve_mention(
            org_id,
            "permission-live",
            lookup_text=ASK_DEV_PROJECT_NAME,
            kinds=(EntityKind.PROJECT,),
        )
        assert resolution.entity is not None
        entity_id = resolution.entity.canonical_id

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
async def test_team_name_colliding_with_a_project_name_is_reported_ambiguous(
    sink: Any, raw_client: Any
) -> None:
    """Pin the one behaviour change this adds to name-based resolution.

    ``projects`` now holds two id spaces: team-derived attribution rows named
    after the TEAM, and project rows named after the PROJECT. When a team and a
    project share a name, ``resolve_mention`` sees two candidates and returns
    ``AMBIGUOUS_CANDIDATES`` instead of ``EXACT_MATCH``. That is the designed
    outcome for a genuinely ambiguous mention — the caller gets both candidates
    to disambiguate — but it is a real behaviour change, so it is asserted here
    rather than discovered in production. No such collision exists in the dev
    workspace today (teams: Fullchaos, Ops Team).
    """

    from datetime import datetime, timezone

    from dev_health_ops.metrics.schemas import ProjectRecord

    org_id = f"chaos-3365-collide-{uuid.uuid4().hex[:10]}"
    now = datetime.now(timezone.utc)
    try:
        sink.write_projects(
            [
                ProjectRecord(
                    id=f"{org_id}:linear:CHAOS",
                    org_id=org_id,
                    provider="linear",
                    project_key="CHAOS",
                    name=ASK_DEV_PROJECT_NAME,
                    is_active=1,
                    updated_at=now,
                    last_synced=now,
                ),
                ProjectRecord(
                    id=ASK_DEV_PROJECT_ID,
                    org_id=org_id,
                    provider="linear",
                    project_key=None,
                    name=ASK_DEV_PROJECT_NAME,
                    is_active=1,
                    updated_at=now,
                    last_synced=now,
                ),
            ]
        )
        service = ScopeResolutionService(ClickHouseAuthorizedEntityCatalog(sink))
        resolution = await service.resolve_mention(
            org_id,
            "permission-live",
            lookup_text=ASK_DEV_PROJECT_NAME,
            kinds=(EntityKind.PROJECT,),
        )
        assert resolution.outcome is ResolutionOutcome.AMBIGUOUS_CANDIDATES
        assert {candidate.canonical_id for candidate in resolution.candidates} == {
            ASK_DEV_PROJECT_ID,
            f"{org_id}:linear:CHAOS",
        }
    finally:
        _cleanup(raw_client, org_id)


@pytest.mark.asyncio
async def test_oracle_does_not_resurrect_a_work_items_previous_project(
    sink: Any, raw_client: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``work_items`` is a ReplacingMergeTree keyed (org_id, repo_id, work_item_id).

    A work item moved between projects leaves BOTH rows in the table until a
    background merge collapses them. A plain ``DISTINCT project_id`` would
    report the abandoned project as still referenced, so the oracle would demand
    a catalog row for a project nothing points at any more and fail forever on a
    phantom.

    Merges are STOPPED for the duration, and the two-row state is asserted
    before the oracle runs. Both are load-bearing: ClickHouse often merges these
    parts within a second, and without them this test passes whether or not the
    oracle handles the case — verified by mutating the oracle to a plain
    ``DISTINCT`` and watching the naive version stay green.
    """

    org_id = f"chaos-3365-moved-{uuid.uuid4().hex[:10]}"
    abandoned_id = "00000000-1111-2222-3333-444444444444"
    raw_client.command("SYSTEM STOP MERGES work_items")
    try:
        sink.org_id = org_id
        # Same work_item_id written twice: first into a project it later leaves.
        sink.write_work_items(
            [
                replace(
                    _normalize("CHAOS-9001", abandoned_id, "Abandoned"), org_id=org_id
                )
            ]
        )
        sink.write_work_items(
            [
                replace(
                    _normalize("CHAOS-9001", ASK_DEV_PROJECT_ID, ASK_DEV_PROJECT_NAME),
                    org_id=org_id,
                )
            ]
        )

        unmerged = raw_client.query(
            "SELECT count() FROM work_items WHERE org_id = {org_id:String}",
            parameters={"org_id": org_id},
        ).result_rows[0][0]
        assert unmerged == 2, (
            "both versions of the moved work item must still be on disk, or this "
            "test cannot observe the defect it exists to catch"
        )

        _run_autoimport(monkeypatch, org_id)

        gaps = project_subject_gaps(
            raw_client, org_id=org_id, acceptance_project_ids=(ASK_DEV_PROJECT_ID,)
        )
        assert gaps == [], (
            f"the abandoned project was reported as still referenced: {gaps}"
        )
    finally:
        raw_client.command("SYSTEM START MERGES work_items")
        _cleanup(raw_client, org_id)


@pytest.mark.parametrize(
    "dsn",
    [
        "clickhouse://ch:ch@localhost:8123/default",
        "clickhouse://ch:ch@localhost:8123/",
        "clickhouse://ch:ch@localhost:8123",
        "clickhouse://ch:ch@localhost:8123/DEFAULT",
    ],
)
def test_the_suite_refuses_to_migrate_a_protected_database(dsn: str) -> None:
    """``ensure_schema(force=True)`` rebuilds and drops tables.

    Pointing ``CLICKHOUSE_URI`` at the dev database and running
    ``pytest -m clickhouse`` would therefore alter real development data. The
    repo's only existing guard for this lives in ``ci/local_validate.sh``, so
    anyone invoking pytest directly had no protection at all.
    """

    with pytest.raises(RuntimeError, match="refusing to run schema migrations"):
        _scratch_database(dsn)


def test_the_guard_still_admits_a_named_scratch_database() -> None:
    """A guard that rejects everything would be just as useless."""

    assert (
        _scratch_database("clickhouse://ch:ch@localhost:8123/ci_local_validate")
        == "ci_local_validate"
    )


@pytest.mark.asyncio
async def test_every_referenced_project_resolves_exactly_by_name(
    sink: Any, raw_client: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Presence in the catalog is not the same thing as resolvability.

    The oracle's SQL proves a row exists with the right id, name and activity.
    Only the real resolver proves the user's actual question works, so it is
    exercised here for EVERY referenced project rather than one sampled name.
    """

    org_id = f"chaos-3365-resolve-{uuid.uuid4().hex[:10]}"
    try:
        _seed_reference_side(sink, org_id)
        _run_autoimport(monkeypatch, org_id)

        service = ScopeResolutionService(ClickHouseAuthorizedEntityCatalog(sink))
        expected = {
            ASK_DEV_PROJECT_NAME: ASK_DEV_PROJECT_ID,
            OTHER_PROJECT_NAME: OTHER_PROJECT_ID,
        }
        for name, project_id in expected.items():
            resolution = await service.resolve_mention(
                org_id, "permission-live", lookup_text=name, kinds=(EntityKind.PROJECT,)
            )
            assert resolution.outcome is ResolutionOutcome.EXACT_MATCH, (
                f"{name!r} resolved {resolution.outcome}"
            )
            assert resolution.entity is not None
            assert resolution.entity.canonical_id == project_id
    finally:
        _cleanup(raw_client, org_id)


@pytest.mark.asyncio
async def test_oracle_reports_a_name_that_resolves_ambiguously_as_a_gap(
    sink: Any, raw_client: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A present-but-unresolvable subject must not read as a pass.

    Verified against the real resolver in the same test: the oracle's verdict
    and ``resolve_mention``'s outcome are asserted to agree, so the SQL check
    cannot drift away from the behaviour it stands in for.
    """

    from datetime import datetime, timezone

    from dev_health_ops.metrics.schemas import ProjectRecord

    org_id = f"chaos-3365-ambig-{uuid.uuid4().hex[:10]}"
    try:
        _seed_reference_side(sink, org_id)
        _run_autoimport(monkeypatch, org_id)

        clean = project_subject_gaps(
            raw_client, org_id=org_id, acceptance_project_ids=(ASK_DEV_PROJECT_ID,)
        )
        assert clean == []

        # A second ACTIVE row carrying the same label — exactly what a Linear
        # team sharing a project's name produces.
        now = datetime.now(timezone.utc)
        sink.write_projects(
            [
                ProjectRecord(
                    id=f"{org_id}:linear:CHAOS",
                    org_id=org_id,
                    provider="linear",
                    project_key="CHAOS",
                    name=ASK_DEV_PROJECT_NAME,
                    is_active=1,
                    updated_at=now,
                    last_synced=now,
                )
            ]
        )

        service = ScopeResolutionService(ClickHouseAuthorizedEntityCatalog(sink))
        resolution = await service.resolve_mention(
            org_id,
            "permission-live",
            lookup_text=ASK_DEV_PROJECT_NAME,
            kinds=(EntityKind.PROJECT,),
        )
        assert resolution.outcome is ResolutionOutcome.AMBIGUOUS_CANDIDATES

        gaps = project_subject_gaps(
            raw_client, org_id=org_id, acceptance_project_ids=(ASK_DEV_PROJECT_ID,)
        )
        assert [gap.kind for gap in gaps] == ["name_ambiguous"], (
            "the oracle reported OK for a name the resolver cannot resolve"
        )
    finally:
        _cleanup(raw_client, org_id)


def test_oracle_refuses_to_pass_when_nothing_was_compared(raw_client: Any) -> None:
    """A measurement that did not happen must FAIL, loudly.

    An org with no referenced projects has zero gaps by arithmetic. Reporting
    that as a pass is how an oracle silently stops being worth running.
    """

    with pytest.raises(OracleNotMeasured, match="nothing to compare"):
        project_subject_gaps(raw_client, org_id=f"empty-{uuid.uuid4().hex[:12]}")


@pytest.mark.asyncio
async def test_oracle_fails_when_the_acceptance_project_stops_being_referenced(
    sink: Any, raw_client: Any
) -> None:
    org_id = f"chaos-3365-acc-{uuid.uuid4().hex[:12]}"
    try:
        _seed_reference_side(sink, org_id)
        with pytest.raises(OracleNotMeasured, match="acceptance-set"):
            project_subject_gaps(
                raw_client,
                org_id=org_id,
                acceptance_project_ids=("00000000-0000-0000-0000-000000000000",),
            )
    finally:
        _cleanup(raw_client, org_id)


@pytest.mark.asyncio
@pytest.mark.parametrize("reversed_arrival", [False, True])
async def test_retirement_survives_a_real_replacingmergetree_merge(
    sink: Any, raw_client: Any, reversed_arrival: bool
) -> None:
    """The unit tests assert against a dict that overwrites by key.

    That models nothing about ReplacingMergeTree version selection, so they
    would stay green if the real writer stopped persisting ``is_active`` or if
    an older active row won the merge. This writes both versions through the
    PRODUCTION sink and reads back through ``FINAL``, in both arrival orders,
    because insert order is not delivery order.
    """

    from datetime import datetime, timedelta, timezone

    from dev_health_ops.metrics.schemas import ProjectRecord

    org_id = f"chaos-3365-rmt-{uuid.uuid4().hex[:10]}"
    older = datetime(2026, 7, 30, 12, tzinfo=timezone.utc)
    newer = older + timedelta(days=2)

    def _row(*, is_active: int, updated_at: datetime) -> ProjectRecord:
        return ProjectRecord(
            id=ASK_DEV_PROJECT_ID,
            org_id=org_id,
            provider="linear",
            project_key=None,
            name=ASK_DEV_PROJECT_NAME,
            is_active=is_active,
            state="" if is_active == 0 else "started",
            target_date=None,
            url="",
            updated_at=updated_at,
            last_synced=newer,
        )

    active = _row(is_active=1, updated_at=older)
    retired = _row(is_active=0, updated_at=newer)
    ordered = [retired, active] if reversed_arrival else [active, retired]

    try:
        for row in ordered:
            sink.write_projects([row])

        final = raw_client.query(
            "SELECT is_active FROM projects FINAL "
            "WHERE org_id = {org_id:String} AND id = {pid:String}",
            parameters={"org_id": org_id, "pid": ASK_DEV_PROJECT_ID},
        ).result_rows
        assert final == [(0,)], (
            f"the newer retiring row lost the merge (arrival reversed={reversed_arrival})"
        )

        # And the subject genuinely stops resolving, which is the user-visible point.
        service = ScopeResolutionService(ClickHouseAuthorizedEntityCatalog(sink))
        resolution = await service.resolve_mention(
            org_id,
            "permission-live",
            lookup_text=ASK_DEV_PROJECT_NAME,
            kinds=(EntityKind.PROJECT,),
        )
        assert resolution.outcome is ResolutionOutcome.NO_AUTHORIZED_MATCH, (
            "a retired project is still resolvable"
        )
    finally:
        _cleanup(raw_client, org_id)


@pytest.mark.asyncio
async def test_the_production_writer_persists_every_lifecycle_column(
    sink: Any, raw_client: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Both writers gained new columns; prove the real one round-trips them.

    A column missing from the insert list fails silently as a default value,
    which no fake-sink assertion can see.
    """

    from datetime import date

    org_id = f"chaos-3365-cols-{uuid.uuid4().hex[:10]}"
    try:
        _seed_reference_side(sink, org_id)
        _run_autoimport(monkeypatch, org_id)

        rows = raw_client.query(
            "SELECT state, target_date, url FROM projects FINAL "
            "WHERE org_id = {org_id:String} AND id = {pid:String}",
            parameters={"org_id": org_id, "pid": OTHER_PROJECT_ID},
        ).result_rows
        assert rows, "the project row was not written at all"
        state, target_date, url = rows[0]
        assert state == "completed", "status.type did not reach ClickHouse"
        assert target_date == date(2026, 9, 1)
        assert url.endswith(OTHER_PROJECT_ID)
    finally:
        _cleanup(raw_client, org_id)
