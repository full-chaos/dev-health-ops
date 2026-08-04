"""A committed PROJECT subject must read its own work items.

Every pre-existing project-scope status test hand-builds a ``DevScope`` whose
entity ref carries ``repository_id="repo-a"``. **Production can never produce
that shape**: ``ClickHouseAuthorizedEntityCatalog._query_for(EntityKind.PROJECT)``
selects ``NULL AS repository_id``, and
``ScopeResolutionService.committed_scope_for``/``_resolved_scope`` populate
``DevScope.repositories`` only for a REPOSITORY commit. So the hand-built
scopes proved the project SQL arms work while the real committed scope failed
closed in ``_authorized_repository_ids`` before reaching them -- coverage that
read as coverage and was not.

These tests build the scope with the **real producer**
(``ScopeResolutionService`` over the real ``ClickHouseAuthorizedEntityCatalog``)
and run the real ``ClickHouseStatusChangeSource`` SQL dispatch over it. Only the
ClickHouse round trip is replaced, and the two things a fake could quietly
invent are pinned:

* ``test_catalog_project_rows_carry_no_repository`` pins the catalog's own
  ``NULL AS repository_id``, so the committed-scope shape cannot drift; and
* ``_FakeWorkItems`` **evaluates** the derivation SQL's ``org_id``, ``as_of``
  and ``project_id``/``project_key`` predicates against a row store rather than
  returning a canned answer -- dropping any one of them changes the derived
  repository set and fails a test (mutation-checked).
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from dev_health_ops.api.dev import native_status_change, scope_catalog
from dev_health_ops.api.dev.contracts import DirectScope
from dev_health_ops.api.dev.native_status_change import ClickHouseStatusChangeSource
from dev_health_ops.api.dev.scope_catalog import ClickHouseAuthorizedEntityCatalog
from dev_health_ops.api.dev.scope_service import (
    EntityKind,
    ScopeRef,
    ScopeResolutionService,
    ScopeResolveRequest,
)
from dev_health_ops.api.dev.status_change_service import (
    ChangeSummaryRequest,
    StatusChangeService,
    StatusSnapshotRequest,
)

NOW = datetime(2026, 8, 4, 1, 44, tzinfo=UTC)
ORG_ID = "org-under-test"
OTHER_ORG_ID = "org-other-tenant"
PROJECT_ID = "13e65c04-40ec-4a95-8216-f7c2ce233244"
PROJECT_NAME = "Ask Dev"
TEAM_ID = "5b0d3f61-3e5f-4a2f-9a1e-1d2c3b4a5f60"

#: Linear work items carry no repository: the sync lands them under the zero
#: UUID, which has no ``repos`` row at all (verified against the live dev
#: ClickHouse). The fix must derive the project's repository set from
#: ``work_items`` itself, never from an existence check against ``repos``.
LINEAR_NO_REPOSITORY_ID = "00000000-0000-0000-0000-000000000000"
#: Reached only through the SQL's ``project_key`` arm.
KEYED_REPOSITORY_ID = "aaaaaaaa-0000-0000-0000-000000000001"
#: Same project, another tenant -- must never enter this org's derived set.
FOREIGN_REPOSITORY_ID = "bbbbbbbb-0000-0000-0000-000000000002"
#: Same project and tenant, but updated strictly after ``as_of``.
FUTURE_REPOSITORY_ID = "cccccccc-0000-0000-0000-000000000003"
#: Still referenced by this project's work items, but no longer in ``repos`` --
#: revoked/removed. ClickHouse enforces no foreign key, so the rows outlive the
#: authorization. The ORGANIZATION branch enumerates ``repos`` and would never
#: admit it; neither may the project branch.
REVOKED_REPOSITORY_ID = "dddddddd-0000-0000-0000-000000000004"

#: The org-scoped ``repos`` catalog. The zero UUID is deliberately absent: it
#: is the repository-*less* sentinel, not a repository.
REPOS_CATALOG = {
    ORG_ID: {KEYED_REPOSITORY_ID, FUTURE_REPOSITORY_ID},
    OTHER_ORG_ID: {FOREIGN_REPOSITORY_ID},
}

EXPECTED_DERIVED_REPOSITORY_IDS = sorted({LINEAR_NO_REPOSITORY_ID, KEYED_REPOSITORY_ID})

NO_REPOSITORY_SET_WARNING = (
    "Status reads require the complete authorized repository set; "
    "scope was not widened."
)
NO_CHANGE_SET_WARNING = "Observed-change scope was not widened."


def _row(
    *,
    work_item_id: str,
    title: str,
    status: str,
    repository_id: str,
    org_id: str = ORG_ID,
    project_id: str = PROJECT_ID,
    project_key: str = "",
    updated_at: datetime | None = None,
) -> dict[str, Any]:
    return {
        "org_id": org_id,
        "repository_id": repository_id,
        "work_item_id": work_item_id,
        "title": title,
        "status": status,
        "parent_id": "",
        "project_id": project_id,
        "project_key": project_key,
        "updated_at": updated_at or (NOW - timedelta(days=4)),
        "last_synced": NOW,
    }


#: One store, shared by the derivation query and the work-item read, so the
#: two can never disagree about what exists.
WORK_ITEM_STORE = [
    _row(
        work_item_id="linear:CHAOS-3367",
        title="Outcome copy contract",
        status="done",
        repository_id=LINEAR_NO_REPOSITORY_ID,
    ),
    _row(
        work_item_id="linear:CHAOS-3368",
        title="Project status snapshot",
        status="in_progress",
        repository_id=LINEAR_NO_REPOSITORY_ID,
    ),
    # Attributed by project_key rather than project_id.
    _row(
        work_item_id="jira:ASK-9",
        title="Keyed attribution",
        status="in_progress",
        repository_id=KEYED_REPOSITORY_ID,
        project_id="",
        project_key=PROJECT_ID,
    ),
    # Another tenant's item for the same project id.
    _row(
        work_item_id="linear:FOREIGN-1",
        title="Other tenant",
        status="done",
        repository_id=FOREIGN_REPOSITORY_ID,
        org_id=OTHER_ORG_ID,
    ),
    # This tenant, this project, but not yet visible at as_of.
    _row(
        work_item_id="linear:FUTURE-1",
        title="Not yet at as_of",
        status="todo",
        repository_id=FUTURE_REPOSITORY_ID,
        updated_at=NOW + timedelta(days=1),
    ),
    # This tenant, this project, in window -- but its repository is gone from
    # the authorization catalog.
    _row(
        work_item_id="linear:REVOKED-1",
        title="Repository no longer authorized",
        status="in_progress",
        repository_id=REVOKED_REPOSITORY_ID,
    ),
]

IN_SCOPE_WORK_ITEM_IDS = {
    "linear:CHAOS-3367",
    "linear:CHAOS-3368",
    "jira:ASK-9",
}


class _FakeWorkItems:
    """Evaluate the production SQL's predicates, don't fake past them.

    A canned-answer fake makes the ``org_id`` / ``as_of`` / ``project_key`` /
    ``repos`` predicates unfalsifiable -- deleting any of them from
    ``_PROJECT_REPOSITORIES_SQL`` would still return the same rows. This reads
    which predicates the SQL under test actually contains and applies exactly
    those, so a dropped predicate changes the derived repository set and a
    test fails.

    What it deliberately does NOT do, so nobody reads more into a green run
    than is there (Codex adversarial review, MEDIUM, 2026-08-03): it is a
    predicate evaluator, not a SQL engine. It cannot see boolean precedence,
    joins, or ``LIMIT`` -- flipping the project ``OR`` to ``AND`` or appending
    ``LIMIT 1`` changes real ClickHouse behavior while every substring it
    looks for is still present. Those two properties are pinned separately and
    structurally by ``test_derivation_sql_structure_is_pinned``, and the SQL's
    real-engine validity by the ``project_repositories`` entry in
    ``tests/api/dev/test_status_change_clickhouse_live.py``.
    """

    def __init__(self) -> None:
        self.sql: list[str] = []
        self.params: list[dict[str, Any]] = []

    def _matching(self, sql: str, params: dict[str, Any]) -> list[dict[str, Any]]:
        rows = list(WORK_ITEM_STORE)
        if "org_id = {org_id:String}" in sql:
            rows = [row for row in rows if row["org_id"] == params["org_id"]]
        if "updated_at <= {as_of:DateTime64(3, 'UTC')}" in sql:
            rows = [row for row in rows if row["updated_at"] <= params["as_of"]]
        if "toString(repo_id) IN {repository_ids:Array(String)}" in sql:
            rows = [
                row for row in rows if row["repository_id"] in params["repository_ids"]
            ]
        if "SELECT toString(id) FROM repos FINAL" in sql:
            authorized = REPOS_CATALOG.get(params["org_id"], set())
            sentinel_admitted = (
                "toString(repo_id) = '00000000-0000-0000-0000-000000000000'" in sql
            )
            rows = [
                row
                for row in rows
                if row["repository_id"] in authorized
                or (
                    sentinel_admitted
                    and row["repository_id"] == LINEAR_NO_REPOSITORY_ID
                )
            ]
        entity_id = params["entity_id"]
        arms = []
        if "project_id = {entity_id:String}" in sql:
            arms.append(lambda row: row["project_id"] == entity_id)
        if "project_key = {entity_id:String}" in sql:
            arms.append(lambda row: row["project_key"] == entity_id)
        return [row for row in rows if any(arm(row) for arm in arms)]

    async def __call__(
        self, _client: object, sql: str, params: dict[str, Any]
    ) -> list[dict[str, Any]]:
        self.sql.append(sql)
        self.params.append(dict(params))
        if "FROM work_items FINAL" in sql and "SELECT DISTINCT" in sql:
            return [
                {"repository_id": repository_id}
                for repository_id in sorted(
                    {row["repository_id"] for row in self._matching(sql, params)}
                )
            ]
        if "FROM work_items FINAL" in sql and "parent_id" in sql:
            return self._matching(sql, params)
        if "FROM work_graph_projection_runs" in sql:
            return [{"last_synced": NOW}]
        return []

    def work_item_read_params(self) -> dict[str, Any]:
        for sql, params in zip(self.sql, self.params, strict=True):
            if "FROM work_items FINAL" in sql and "parent_id" in sql:
                return params
        raise AssertionError("the work-item read was never issued")


def _install_catalog_client(monkeypatch: pytest.MonkeyPatch) -> None:
    """Serve the real catalog SQL the shapes production ClickHouse returns."""

    async def fake_query(
        _client: object, sql: str, _params: dict[str, Any]
    ) -> list[dict[str, Any]]:
        if "AS watermark" in sql:
            return [{"watermark": NOW}]
        if "FROM projects FINAL" in sql:
            return [
                {
                    "canonical_id": PROJECT_ID,
                    "label": PROJECT_NAME,
                    # The production SQL selects ``NULL AS repository_id`` --
                    # pinned by test_catalog_project_rows_carry_no_repository.
                    "repository_id": None,
                }
            ]
        if "FROM teams FINAL" in sql:
            # Only reached by the team-filtered case below; teams likewise
            # carry no repository dimension of their own.
            return [
                {
                    "canonical_id": TEAM_ID,
                    "label": "Team A",
                    "repository_id": None,
                }
            ]
        return []

    monkeypatch.setattr(scope_catalog, "query_dicts", fake_query)


def _install_status_client(monkeypatch: pytest.MonkeyPatch) -> _FakeWorkItems:
    fake = _FakeWorkItems()
    monkeypatch.setattr(native_status_change, "query_dicts", fake)
    return fake


async def _committed_project_scope(*, team_filter: str | None = None) -> Any:
    """The committed scope, from the real producer -- never hand-built."""

    service = ScopeResolutionService(ClickHouseAuthorizedEntityCatalog(object()))
    resolution = await service.resolve_contract(
        ORG_ID,
        "permission-fingerprint",
        ScopeResolveRequest(
            explicit_refs=(ScopeRef(EntityKind.PROJECT, PROJECT_ID),),
            team_filter_refs=(
                () if team_filter is None else (ScopeRef(EntityKind.TEAM, team_filter),)
            ),
        ),
        resolved_at=NOW,
    )
    # A team filter makes the resolver's own outcome "filtered" rather than
    # "exact"; both are committed outcomes carrying a resolved scope.
    assert resolution.outcome.value == ("filtered" if team_filter else "exact")
    scope = resolution.resolved_scope
    assert scope is not None
    assert scope.direct_scope is DirectScope.PROJECT
    return scope


def test_catalog_project_rows_carry_no_repository() -> None:
    """The production fact the fakes above stand in for.

    If the catalog ever starts selecting a real ``repository_id`` for a
    project, these tests would be asserting against a shape production no
    longer produces -- so pin it here rather than letting the fake silently
    become fiction.
    """

    sql = ClickHouseAuthorizedEntityCatalog._query_for(EntityKind.PROJECT, exact=True)
    assert "NULL AS repository_id" in sql


def test_derivation_matches_projects_exactly_as_the_queries_it_bounds_do() -> None:
    """The derivation can never be wider than the facts it bounds.

    The repository set exists only to bound ``_WORK_ITEMS_SQL`` and the
    delivery arms. If its project-matching predicate ever drifts from theirs,
    it could admit a repository those queries would not themselves draw from
    -- so the two are pinned as the same text, not merely as "both correct".
    """

    project_predicate = (
        "project_id = {entity_id:String} OR project_key = {entity_id:String}"
    )
    assert project_predicate in native_status_change._PROJECT_REPOSITORIES_SQL
    assert project_predicate in native_status_change._WORK_ITEMS_SQL


def test_derivation_sql_structure_is_pinned() -> None:
    """Structure the predicate-evaluating fake provably cannot see.

    Codex adversarial review (MEDIUM, 2026-08-03): a substring fake keeps
    matching after ``OR`` becomes ``AND`` or a ``LIMIT`` is appended, both of
    which change real ClickHouse results. Assert them on the SQL text itself.
    """

    sql = native_status_change._PROJECT_REPOSITORIES_SQL

    # The project arms are one parenthesised disjunction, not a conjunction:
    # a Jira row matches by key and a Linear row by id, never both at once, so
    # AND would return the empty set for every provider.
    assert (
        "  AND (project_id = {entity_id:String} OR project_key = {entity_id:String})\n"
        in sql
    )
    # No bound of its own: this resolves an authorization set, and a truncated
    # authorization set is a silently narrowed read, not a smaller page.
    assert "LIMIT" not in sql.upper()
    # Both authorization arms survive, and the tenant/as_of bounds with them.
    assert "toString(repo_id) = '00000000-0000-0000-0000-000000000000'" in sql
    assert "SELECT toString(id) FROM repos FINAL WHERE org_id = {org_id:String}" in sql
    # Anchored to ``work_items`` specifically: a bare
    # ``"WHERE org_id = {org_id:String}" in sql`` is satisfied by the ``repos``
    # sub-select alone, so it stays green with the fact table's own tenant
    # bound deleted. Because a repository id is org-unique, the ``repos`` arm
    # masks that deletion for every real repository -- leaving the
    # repository-less sentinel bucket, which is shared by every tenant, as the
    # one place the fact table's own bound is the only tenant boundary left.
    assert "FROM work_items FINAL\nWHERE org_id = {org_id:String}\n" in sql
    assert "AND updated_at <= {as_of:DateTime64(3, 'UTC')}" in sql


@pytest.mark.asyncio
async def test_committed_project_scope_carries_no_repository_dimension(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The structural premise: nothing on the wire names a repository."""

    _install_catalog_client(monkeypatch)
    scope = await _committed_project_scope()

    assert scope.repositories == []
    assert len(scope.entity_refs) == 1
    assert scope.entity_refs[0].entity_id == PROJECT_ID
    assert scope.entity_refs[0].repository_id is None


@pytest.mark.asyncio
async def test_committed_project_scope_status_snapshot_reads_its_work_items(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """RED before the fix: the snapshot came back empty and fail-closed."""

    _install_catalog_client(monkeypatch)
    scope = await _committed_project_scope()

    fake = _install_status_client(monkeypatch)
    service = StatusChangeService(ClickHouseStatusChangeSource(object(), now=NOW))

    snapshot = await service.status_snapshot(
        ORG_ID, "permission-fingerprint", StatusSnapshotRequest(scope)
    )

    assert NO_REPOSITORY_SET_WARNING not in snapshot.warnings
    # The work-item read must actually have been issued -- an assertion on
    # the returned facts alone would pass for a snapshot that never queried.
    fake.work_item_read_params()
    assert {child.entity_id for child in snapshot.children} == IN_SCOPE_WORK_ITEM_IDS


@pytest.mark.asyncio
async def test_team_filtered_project_scope_stays_fail_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A team filter must never be answered with the whole project.

    Codex adversarial review (HIGH, 2026-08-03). ``DevScope`` permits
    ``team_ids`` alongside ``direct_scope=PROJECT`` and the resolver populates
    it from ``team_filter_refs``, but no project SQL arm applies a team
    filter. Deriving the project's full repository set for such a request
    would answer "project P, team A only" with team B's work from the same
    project -- so it keeps the fail-closed behavior it had before the
    derivation existed, exactly like the team-filtered ORGANIZATION case.
    """

    _install_catalog_client(monkeypatch)
    scope = await _committed_project_scope(team_filter=TEAM_ID)
    assert scope.team_ids == [TEAM_ID]

    fake = _install_status_client(monkeypatch)
    service = StatusChangeService(ClickHouseStatusChangeSource(object(), now=NOW))

    snapshot = await service.status_snapshot(
        ORG_ID, "permission-fingerprint", StatusSnapshotRequest(scope)
    )

    assert NO_REPOSITORY_SET_WARNING in snapshot.warnings
    assert snapshot.children == ()
    # And it must not have leaked the derivation query either.
    assert fake.sql == []


@pytest.mark.asyncio
async def test_repository_revoked_from_the_catalog_is_never_admitted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Work-item rows outlive authorization; the derived bound must not.

    Codex adversarial review (HIGH, 2026-08-03). ClickHouse enforces no
    foreign key, so a repository removed from ``repos`` keeps its
    ``work_items`` rows. The ORGANIZATION branch enumerates ``repos`` and
    would never admit it; project scope must not either, purely because it
    reads a different table.
    """

    _install_catalog_client(monkeypatch)
    scope = await _committed_project_scope()
    fake = _install_status_client(monkeypatch)
    service = StatusChangeService(ClickHouseStatusChangeSource(object(), now=NOW))

    snapshot = await service.status_snapshot(
        ORG_ID, "permission-fingerprint", StatusSnapshotRequest(scope)
    )

    bound = fake.work_item_read_params()["repository_ids"]
    assert REVOKED_REPOSITORY_ID not in bound
    assert "linear:REVOKED-1" not in {child.entity_id for child in snapshot.children}
    # ...while the repository-less sentinel, which has no ``repos`` row and
    # never will, is still admitted -- otherwise the catalog check would have
    # re-broken every Linear project.
    assert LINEAR_NO_REPOSITORY_ID in bound


@pytest.mark.asyncio
async def test_derived_repository_set_is_exactly_the_projects_own_work(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The derived bound is neither too narrow nor too wide.

    Asserted on the ``repository_ids`` the real ``_WORK_ITEMS_SQL`` read was
    actually bound by -- the one observable that distinguishes a correct
    derivation from one that dropped the tenant bound, the ``as_of`` bound, or
    the ``project_key`` arm.
    """

    _install_catalog_client(monkeypatch)
    scope = await _committed_project_scope()
    fake = _install_status_client(monkeypatch)
    service = StatusChangeService(ClickHouseStatusChangeSource(object(), now=NOW))

    await service.status_snapshot(
        ORG_ID, "permission-fingerprint", StatusSnapshotRequest(scope)
    )

    bound = fake.work_item_read_params()["repository_ids"]
    assert sorted(bound) == EXPECTED_DERIVED_REPOSITORY_IDS
    assert FOREIGN_REPOSITORY_ID not in bound
    assert FUTURE_REPOSITORY_ID not in bound


@pytest.mark.asyncio
async def test_committed_project_scope_change_summary_is_not_fail_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``change_summary`` shares the same repository-bounding helper."""

    _install_catalog_client(monkeypatch)
    scope = await _committed_project_scope()
    assert scope.comparison_range is not None

    _install_status_client(monkeypatch)
    service = StatusChangeService(ClickHouseStatusChangeSource(object(), now=NOW))

    change = await service.change_summary(
        ORG_ID,
        "permission-fingerprint",
        ChangeSummaryRequest(
            scope=scope,
            current_start=scope.time_range.start,
            current_end=scope.time_range.end,
            comparison_start=scope.comparison_range.start,
            comparison_end=scope.comparison_range.end,
        ),
    )

    assert NO_CHANGE_SET_WARNING not in change.warnings


@pytest.mark.asyncio
async def test_project_repository_derivation_failure_stays_fail_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An unreadable attribution source is never "spans no repositories".

    Guard for the ``except`` arm of ``_project_repository_ids``: without it a
    ClickHouse outage would surface as a confident empty project snapshot
    instead of the disclosed fail-closed one.
    """

    _install_catalog_client(monkeypatch)
    scope = await _committed_project_scope()

    async def failing_query(
        _client: object, _sql: str, _params: dict[str, Any]
    ) -> list[dict[str, Any]]:
        raise RuntimeError("clickhouse unavailable")

    monkeypatch.setattr(native_status_change, "query_dicts", failing_query)
    service = StatusChangeService(ClickHouseStatusChangeSource(object(), now=NOW))

    snapshot = await service.status_snapshot(
        ORG_ID, "permission-fingerprint", StatusSnapshotRequest(scope)
    )

    assert NO_REPOSITORY_SET_WARNING in snapshot.warnings
    assert snapshot.children == ()


@pytest.mark.asyncio
async def test_empty_project_id_never_issues_a_derivation_query(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An empty subject id must not widen to "every unkeyed work item".

    ``DevScope.validate_direct_scope`` makes this unreachable through a
    committed project scope today, which is exactly why the guard is asserted
    directly: ``project_key`` is empty for every Linear work item, so an empty
    ``entity_id`` reaching the SQL would match the entire organization through
    the ``project_key`` arm.
    """

    fake = _install_status_client(monkeypatch)
    source = ClickHouseStatusChangeSource(object(), now=NOW)

    derived = await source._project_repository_ids(ORG_ID, "", as_of=NOW)

    assert derived == []
    assert fake.sql == []
