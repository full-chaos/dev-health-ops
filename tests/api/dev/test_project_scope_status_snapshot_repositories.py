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
* ``_FakeWorkItems`` **evaluates** the derivation SQL's ``org_id``, ``as_of``,
  ``provider``/``project_key`` identity join, and ``project_id``/``project_key``
  predicates against a row store rather than returning a canned answer --
  dropping any one of them changes the derived repository set and fails a test
  (mutation-checked).

CHAOS-3374: Jira project subjects were fail-closed end to end because
``team_autoimport_jira._project_id`` mints a Jira project's catalog id as
``f"{org_id}:jira:{project_key}"`` while ``providers/jira/normalize`` writes
the RAW Jira id/key onto ``work_items`` -- so neither
``project_id = {entity_id}`` nor the old ``project_key = {entity_id}`` ever
matched. The fix joins every project-scoped arm through the catalog's own
``provider``/``project_key`` columns (see ``native_status_change.
_project_identity_match``). ``_FakeWorkItems`` models that join with its own
``PROJECTS_CATALOG`` lookup so these tests exercise the real predicate shape,
not a stand-in for it.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from typing import Any

import pytest

from dev_health_ops.api.dev import native_status_change, scope_catalog
from dev_health_ops.api.dev.contracts import (
    DevEntityRef,
    DevScope,
    DevTimeRange,
    DirectScope,
    EntityType,
)
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

#: CHAOS-3374: the Jira catalog id is provider-prefixed (mirrors production's
#: ``team_autoimport_jira._project_id``), while the raw Jira project key below
#: is what actually lands on ``work_items.project_key``.
JIRA_PROJECT_ID = f"{ORG_ID}:jira:ASK"
JIRA_RAW_PROJECT_KEY = "ASK"
#: Jira's raw numeric project id -- never equal to the catalog id or key, so a
#: work item attributed only by this column would never resolve (it isn't
#: used by any test row; the Jira row below is reached via its raw key only).
JIRA_RAW_PROJECT_ID_ON_WORK_ITEMS = "10001"
#: A catalog id with no ``projects`` row on the native_status_change side at
#: all -- simulates the identity read observing a scope the catalog read
#: resolved a moment earlier (replication lag, a deleted project, etc.).
GHOST_PROJECT_ID = f"{ORG_ID}:ghost:VANISHED"
#: CHAOS-3374 round 2 (Codex MEDIUM): the catalog's real ReplacingMergeTree key
#: is ``(org_id, provider, id)``, not ``(org_id, id)`` -- so the SAME id can
#: legitimately survive ``FINAL`` twice, once per provider. Two catalog rows
#: share this id under different providers.
COLLIDING_PROJECT_ID = f"{ORG_ID}:collide:MULTI"
#: CHAOS-3374 round 2 (Codex MEDIUM): a project committed while active, then
#: retired (a newer ``is_active = 0`` row) before the identity read runs.
RETIRED_PROJECT_ID = f"{ORG_ID}:jira:RETIRED"
RETIRED_RAW_PROJECT_KEY = "RETIRED"

#: Linear work items carry no repository: the sync lands them under the zero
#: UUID, which has no ``repos`` row at all (verified against the live dev
#: ClickHouse). The fix must derive the project's repository set from
#: ``work_items`` itself, never from an existence check against ``repos``.
LINEAR_NO_REPOSITORY_ID = "00000000-0000-0000-0000-000000000000"
#: Reached only through the SQL's ``project_key`` arm -- now Jira's, since a
#: real Linear catalog row's own ``project_key`` is always empty (CHAOS-3374).
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
#: Where a cross-provider collision row lives -- authorized in ``repos`` (so a
#: leak is caught by the identity guard, not incidentally masked by repo
#: authorization).
COLLISION_REPOSITORY_ID = "ffffffff-0000-0000-0000-000000000006"
#: Round 2: where ``COLLIDING_PROJECT_ID``'s own (would-be, if the collision
#: guard failed) work item lives.
MULTI_PROVIDER_REPOSITORY_ID = "11111111-0000-0000-0000-000000000007"
#: Round 2: where ``RETIRED_PROJECT_ID``'s own (would-be, if the ``is_active``
#: guard failed) work item lives.
RETIRED_PROJECT_REPOSITORY_ID = "22222222-0000-0000-0000-000000000008"

#: The org-scoped ``repos`` catalog. The zero UUID is deliberately absent: it
#: is the repository-*less* sentinel, not a repository.
REPOS_CATALOG = {
    ORG_ID: {
        KEYED_REPOSITORY_ID,
        FUTURE_REPOSITORY_ID,
        COLLISION_REPOSITORY_ID,
        MULTI_PROVIDER_REPOSITORY_ID,
        RETIRED_PROJECT_REPOSITORY_ID,
    },
    OTHER_ORG_ID: {FOREIGN_REPOSITORY_ID},
}

#: Simulates the ``projects`` ClickHouse table as read by the
#: ``native_status_change`` identity CTE. Keyed by ``(org_id, id)`` for lookup
#: convenience, but the VALUE is a list of candidate rows -- exactly because
#: the real table's ReplacingMergeTree key is ``(org_id, provider, id)``, so
#: more than one row can legitimately survive ``FINAL`` for one ``(org_id,
#: id)`` pair (see ``COLLIDING_PROJECT_ID``). Each candidate carries its own
#: ``is_active`` (default ``1``) so retirement (``RETIRED_PROJECT_ID``) can be
#: modeled without a second dict shape.
PROJECTS_CATALOG: dict[tuple[str, str], list[dict[str, Any]]] = {
    (ORG_ID, PROJECT_ID): [{"provider": "linear", "project_key": None}],
    (ORG_ID, JIRA_PROJECT_ID): [
        {"provider": "jira", "project_key": JIRA_RAW_PROJECT_KEY}
    ],
    (ORG_ID, COLLIDING_PROJECT_ID): [
        {"provider": "jira", "project_key": "MULTI"},
        {"provider": "linear", "project_key": None},
    ],
    (ORG_ID, RETIRED_PROJECT_ID): [
        {
            "provider": "jira",
            "project_key": RETIRED_RAW_PROJECT_KEY,
            "is_active": 0,
        }
    ],
    # GHOST_PROJECT_ID deliberately absent.
}

EXPECTED_DERIVED_REPOSITORY_IDS = sorted({LINEAR_NO_REPOSITORY_ID})
JIRA_EXPECTED_REPOSITORY_IDS = sorted({KEYED_REPOSITORY_ID})

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
    provider: str,
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
        "provider": provider,
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
        provider="linear",
    ),
    _row(
        work_item_id="linear:CHAOS-3368",
        title="Project status snapshot",
        status="in_progress",
        repository_id=LINEAR_NO_REPOSITORY_ID,
        provider="linear",
    ),
    # The Jira project's own work item -- reached ONLY via the raw project_key
    # arm (its project_id is the unrelated raw Jira numeric id), and ONLY
    # because the catalog resolves JIRA_PROJECT_ID to provider="jira".
    _row(
        work_item_id="jira:ASK-9",
        title="Keyed attribution",
        status="in_progress",
        repository_id=KEYED_REPOSITORY_ID,
        provider="jira",
        project_id=JIRA_RAW_PROJECT_ID_ON_WORK_ITEMS,
        project_key=JIRA_RAW_PROJECT_KEY,
    ),
    # Another tenant's item for the same project id.
    _row(
        work_item_id="linear:FOREIGN-1",
        title="Other tenant",
        status="done",
        repository_id=FOREIGN_REPOSITORY_ID,
        provider="linear",
        org_id=OTHER_ORG_ID,
    ),
    # This tenant, this project, but not yet visible at as_of.
    _row(
        work_item_id="linear:FUTURE-1",
        title="Not yet at as_of",
        status="todo",
        repository_id=FUTURE_REPOSITORY_ID,
        provider="linear",
        updated_at=NOW + timedelta(days=1),
    ),
    # This tenant, this project, in window -- but its repository is gone from
    # the authorization catalog.
    _row(
        work_item_id="linear:REVOKED-1",
        title="Repository no longer authorized",
        status="in_progress",
        repository_id=REVOKED_REPOSITORY_ID,
        provider="linear",
    ),
    # CHAOS-3374 cross-provider collision matrix: three DIFFERENT providers'
    # rows all carry the SAME raw key/id Jira's project uses. Without the
    # provider guard on the identity join, any one of these would leak into
    # the Jira-scoped snapshot (via the project_key arm) or vice versa (via
    # the project_id arm, since GitLab's own id equals Jira's PREFIXED
    # catalog id here -- an intentionally adversarial, not realistic,
    # collision to prove the guard, not just the common case).
    _row(
        work_item_id="gitlab:collide-by-key",
        title="GitLab project sharing Jira's raw key",
        status="in_progress",
        repository_id=COLLISION_REPOSITORY_ID,
        provider="gitlab",
        project_id="some-group/some-project",
        project_key=JIRA_RAW_PROJECT_KEY,
    ),
    _row(
        work_item_id="linear:collide-by-key",
        title="Linear project sharing Jira's raw key",
        status="in_progress",
        repository_id=COLLISION_REPOSITORY_ID,
        provider="linear",
        project_id="11111111-2222-3333-4444-555555555555",
        project_key=JIRA_RAW_PROJECT_KEY,
    ),
    _row(
        work_item_id="gitlab:collide-by-id",
        title="GitLab project sharing Jira's prefixed catalog id",
        status="in_progress",
        repository_id=COLLISION_REPOSITORY_ID,
        provider="gitlab",
        project_id=JIRA_PROJECT_ID,
        project_key="",
    ),
    # Round 2: would-be members of COLLIDING_PROJECT_ID / RETIRED_PROJECT_ID.
    # Neither may ever be admitted -- the first because its catalog id is
    # ambiguous (two providers), the second because its only catalog row is
    # retired. Present so a regression back to "pick arbitrarily" or "ignore
    # is_active" has something real to wrongly admit, rather than the test
    # passing vacuously because no work item exists to admit in the first
    # place.
    _row(
        work_item_id="jira:MULTI-1",
        title="Would-be member of the colliding project",
        status="in_progress",
        repository_id=MULTI_PROVIDER_REPOSITORY_ID,
        provider="jira",
        project_id="99999",
        project_key="MULTI",
    ),
    _row(
        work_item_id="jira:RETIRED-1",
        title="Would-be member of the retired project",
        status="in_progress",
        repository_id=RETIRED_PROJECT_REPOSITORY_ID,
        provider="jira",
        project_id="88888",
        project_key=RETIRED_RAW_PROJECT_KEY,
    ),
]

IN_SCOPE_WORK_ITEM_IDS = {
    "linear:CHAOS-3367",
    "linear:CHAOS-3368",
}

JIRA_IN_SCOPE_WORK_ITEM_IDS = {
    "jira:ASK-9",
}

#: The collision rows above must never surface for EITHER scope.
COLLISION_WORK_ITEM_IDS = {
    "gitlab:collide-by-key",
    "linear:collide-by-key",
    "gitlab:collide-by-id",
}


class _FakeWorkItems:
    """Evaluate the production SQL's predicates, don't fake past them.

    A canned-answer fake makes the ``org_id`` / ``as_of`` / identity-join /
    ``repos`` predicates unfalsifiable -- deleting any of them from
    ``PROJECT_REPOSITORIES_SQL`` would still return the same rows. This reads
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

    CHAOS-3374: the identity join (``provider = catalog_provider`` / ``project_key
    = catalog_project_key``) is resolved against ``PROJECTS_CATALOG`` exactly
    like the real ``project AS (... HAVING count() = 1)`` CTE -- filtered to
    ``is_active`` candidates for this ``(org_id, id)``, and an identity is only
    admitted when EXACTLY ONE candidate survives that filter (zero -> no row;
    two or more, e.g. a same-id cross-provider collision -> ambiguous, both
    fail closed, mirroring ``HAVING count() = 1`` exactly).
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
        all_candidates = PROJECTS_CATALOG.get((params["org_id"], entity_id), [])
        # Read which identity predicates the SQL under test actually contains,
        # exactly like the rest of this fake -- so dropping ``is_active = 1``
        # or ``HAVING count() = 1`` from the real CTE changes what this fake
        # admits too, instead of the fake enforcing the fix independently of
        # whether the SQL text still has it (CHAOS-3374 round 2).
        if "is_active = 1" in sql:
            candidates = [row for row in all_candidates if row.get("is_active", 1) == 1]
        else:
            candidates = list(all_candidates)
        if "HAVING count() = 1" in sql:
            catalog = candidates[0] if len(candidates) == 1 else None
        else:
            # Mirrors the old, unguarded ``LIMIT 1`` (no ``ORDER BY``): picks
            # ARBITRARILY among candidates rather than failing closed on a
            # collision -- modeled here as "the first one", which is exactly
            # the nondeterministic-in-production, deterministic-in-this-fake
            # bug the round-2 fix closes.
            catalog = candidates[0] if candidates else None
        catalog_provider = catalog["provider"] if catalog else None
        catalog_project_key = (catalog.get("project_key") or "") if catalog else ""
        provider_gated = "provider = catalog_provider" in sql
        if provider_gated and catalog is None:
            # Mirrors the real ``project`` CTE resolving zero rows: zero
            # active candidates (never existed, or retired), or more than one
            # (a same-id, different-provider collision -- ``HAVING count() =
            # 1`` fails), both leave the provider-gated arm unable to match
            # anything.
            return []

        arms = []
        if "project_id = {entity_id:String}" in sql:
            arms.append(lambda row: row["project_id"] == entity_id)
        if "project_key = catalog_project_key" in sql:
            arms.append(
                lambda row: (
                    bool(catalog_project_key)
                    and row["project_key"] == catalog_project_key
                )
            )
        # Legacy substring, kept so a regression back to comparing project_key
        # against the raw entity_id (pre-CHAOS-3374) is also caught here.
        if "project_key = {entity_id:String}" in sql:
            arms.append(lambda row: row["project_key"] == entity_id)

        def matches(row: dict[str, Any]) -> bool:
            if provider_gated and row["provider"] != catalog_provider:
                return False
            return any(arm(row) for arm in arms)

        return [row for row in rows if matches(row)]

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


def _install_catalog_client(
    monkeypatch: pytest.MonkeyPatch, *, project_id: str = PROJECT_ID
) -> None:
    """Serve the real catalog SQL the shapes production ClickHouse returns."""

    async def fake_query(
        _client: object, sql: str, _params: dict[str, Any]
    ) -> list[dict[str, Any]]:
        if "AS watermark" in sql:
            return [{"watermark": NOW}]
        if "FROM projects FINAL" in sql:
            return [
                {
                    "canonical_id": project_id,
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


async def _committed_project_scope(
    *, team_filter: str | None = None, project_id: str = PROJECT_ID
) -> Any:
    """The committed scope, from the real producer -- never hand-built."""

    service = ScopeResolutionService(ClickHouseAuthorizedEntityCatalog(object()))
    resolution = await service.resolve_contract(
        ORG_ID,
        "permission-fingerprint",
        ScopeResolveRequest(
            explicit_refs=(ScopeRef(EntityKind.PROJECT, project_id),),
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

    CHAOS-3374: the predicate widened from the plain
    ``project_id = {entity_id:String} OR project_key = {entity_id:String}``
    (which never matched a Jira row -- see the module docstring) to a
    provider-and-native-key-aware join through the catalog's own ``projects``
    row. Old assertion (pre-CHAOS-3374):

        project_predicate = (
            "project_id = {entity_id:String} OR project_key = {entity_id:String}"
        )

    New assertion, below: the shared text now requires the catalog identity to
    resolve (``ifNull(catalog_provider, '') != ''``), the row's OWN provider to
    match the catalog row's provider, and either the raw id or the raw native
    key to match -- never the catalog's (possibly provider-prefixed) id
    against a raw column on the other side of an OR.
    """

    project_predicate = (
        "ifNull(catalog_provider, '') != '' AND provider = catalog_provider"
        " AND (project_id = {entity_id:String}"
        " OR (catalog_project_key != '' AND project_key = catalog_project_key))"
    )
    assert project_predicate in native_status_change.PROJECT_REPOSITORIES_SQL
    assert project_predicate in native_status_change._WORK_ITEMS_SQL
    # And both share the identical identity CTE that resolves catalog_provider
    # / catalog_project_key in the first place -- not two independently
    # drifting joins that merely happen to use the same column names.
    assert (
        native_status_change._PROJECT_IDENTITY_CTE
        in native_status_change.PROJECT_REPOSITORIES_SQL
    )
    assert (
        native_status_change._PROJECT_IDENTITY_CTE
        in native_status_change._WORK_ITEMS_SQL
    )


def test_derivation_sql_structure_is_pinned() -> None:
    """Structure the predicate-evaluating fake provably cannot see.

    Codex adversarial review (MEDIUM, 2026-08-03): a substring fake keeps
    matching after ``OR`` becomes ``AND`` or a ``LIMIT`` is appended, both of
    which change real ClickHouse results. Assert them on the SQL text itself.
    """

    sql = native_status_change.PROJECT_REPOSITORIES_SQL

    # The project arms are one parenthesised disjunction, not a conjunction:
    # a Jira row matches by key and a Linear row by id, never both at once, so
    # AND would return the empty set for every provider.
    assert (
        "  AND ifNull(catalog_provider, '') != '' AND provider = catalog_provider"
        " AND (project_id = {entity_id:String}"
        " OR (catalog_project_key != '' AND project_key = catalog_project_key))\n"
        in sql
    )
    # No bound anywhere in this query: a truncated authorization set would be
    # a silently narrowed read, not a smaller page. The identity CTE bounds
    # itself to at most one row via ``HAVING count() = 1`` (CHAOS-3374 round
    # 2 -- a bare ``LIMIT 1`` would pick nondeterministically between two
    # same-``(org_id, id)``, different-provider rows, since the catalog's own
    # ReplacingMergeTree key is ``(org_id, provider, id)``), never a ``LIMIT``.
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
    # ``INNER JOIN project ON 1 = 1`` sits between the FROM and the WHERE
    # (CHAOS-3374's identity join) -- anchored here too, so a future edit
    # can't quietly drop it while leaving this assertion green.
    assert (
        "FROM work_items FINAL\nINNER JOIN project ON 1 = 1\nWHERE org_id = {org_id:String}\n"
        in sql
    )
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
    # Cross-provider collision rows sharing Linear's raw id space or a Jira
    # key by coincidence must never leak into a Linear-scoped snapshot either.
    assert not COLLISION_WORK_ITEM_IDS & {
        child.entity_id for child in snapshot.children
    }


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


@pytest.mark.asyncio
async def test_repository_less_sentinel_is_admitted_without_a_repos_row(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The zero UUID's exception, asserted apart from the revocation rule.

    Deliberately a separate test from the revoked-repository one above: they
    pull in opposite directions on the same predicate, and a single test
    asserting both would still pass if the two arms were merged into one
    wrong rule. The sentinel has no ``repos`` row and never will (it is what
    the sink writes when a record has no repository at all), so a bare
    existence check would re-break every Linear project.
    """

    _install_catalog_client(monkeypatch)
    scope = await _committed_project_scope()
    fake = _install_status_client(monkeypatch)
    service = StatusChangeService(ClickHouseStatusChangeSource(object(), now=NOW))

    snapshot = await service.status_snapshot(
        ORG_ID, "permission-fingerprint", StatusSnapshotRequest(scope)
    )

    assert LINEAR_NO_REPOSITORY_ID not in REPOS_CATALOG[ORG_ID]
    assert LINEAR_NO_REPOSITORY_ID in fake.work_item_read_params()["repository_ids"]
    assert {"linear:CHAOS-3367", "linear:CHAOS-3368"} <= {
        child.entity_id for child in snapshot.children
    }


@pytest.mark.asyncio
async def test_jira_shaped_project_resolves_via_provider_scoped_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """GREEN after CHAOS-3374: the identity join makes Jira answerable.

    Before the fix (see ``test_derivation_matches_projects_exactly_as_the_
    queries_it_bounds_do``'s old-assertion comment), this exact scenario hit
    ``test_jira_shaped_project_keeps_todays_fail_closed_behaviour`` and
    asserted the OPPOSITE: ``NO_REPOSITORY_SET_WARNING in snapshot.warnings``
    and ``snapshot.children == ()``. That was a deliberate, documented
    decision -- a derivation-only fix would have turned a disclosed
    fail-closed refusal into a confident empty answer, which is worse. The
    coordinated fix implemented here (repository derivation AND every fact
    arm join through the catalog's own provider/project_key) makes the
    disclosed refusal unnecessary: the project now resolves for real.
    """

    _install_catalog_client(monkeypatch, project_id=JIRA_PROJECT_ID)
    scope = await _committed_project_scope(project_id=JIRA_PROJECT_ID)
    assert scope.entity_refs[0].entity_id == JIRA_PROJECT_ID

    fake = _install_status_client(monkeypatch)
    service = StatusChangeService(ClickHouseStatusChangeSource(object(), now=NOW))

    snapshot = await service.status_snapshot(
        ORG_ID, "permission-fingerprint", StatusSnapshotRequest(scope)
    )

    assert NO_REPOSITORY_SET_WARNING not in snapshot.warnings
    bound = fake.work_item_read_params()["repository_ids"]
    assert sorted(bound) == JIRA_EXPECTED_REPOSITORY_IDS
    assert {
        child.entity_id for child in snapshot.children
    } == JIRA_IN_SCOPE_WORK_ITEM_IDS
    # None of the cross-provider collision rows sharing Jira's raw key (or,
    # adversarially, its prefixed catalog id) may leak in.
    assert not COLLISION_WORK_ITEM_IDS & {
        child.entity_id for child in snapshot.children
    }


@pytest.mark.asyncio
async def test_jira_shaped_project_change_summary_is_not_fail_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``change_summary`` shares the same repository-bounding helper as Jira."""

    _install_catalog_client(monkeypatch, project_id=JIRA_PROJECT_ID)
    scope = await _committed_project_scope(project_id=JIRA_PROJECT_ID)
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
async def test_unresolvable_project_identity_at_read_time_stays_fail_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Negative control: a committed scope whose id has no live catalog row.

    The scope-resolution catalog (``scope_catalog``, backing
    ``ScopeResolutionService``) and the identity join inside
    ``PROJECT_REPOSITORIES_SQL``/``_WORK_ITEMS_SQL`` read the SAME
    ``projects`` table in production but are two separate reads. If the row
    the first read saw is gone by the second (replication lag, concurrent
    delete), the identity CTE resolves zero rows and the project arm must
    fail closed -- never fall through to matching on a null/empty provider,
    which would turn "unknown identity" into "no provider filter at all".
    """

    _install_catalog_client(monkeypatch, project_id=GHOST_PROJECT_ID)
    scope = await _committed_project_scope(project_id=GHOST_PROJECT_ID)
    assert GHOST_PROJECT_ID not in {key[1] for key in PROJECTS_CATALOG}

    fake = _install_status_client(monkeypatch)
    service = StatusChangeService(ClickHouseStatusChangeSource(object(), now=NOW))

    snapshot = await service.status_snapshot(
        ORG_ID, "permission-fingerprint", StatusSnapshotRequest(scope)
    )

    assert NO_REPOSITORY_SET_WARNING in snapshot.warnings
    assert snapshot.children == ()
    assert not any("parent_id" in sql for sql in fake.sql), (
        "no fact query may run on a repository set that was never resolved"
    )


@pytest.mark.asyncio
async def test_cross_provider_id_collision_at_read_time_stays_fail_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Codex adversarial review (MEDIUM, 2026-08-04), round 2.

    ``projects``' ReplacingMergeTree key is ``(org_id, provider, id)``, not
    ``(org_id, id)`` -- ``id`` alone is only unique WITHIN one provider. Two
    different providers minting the SAME id in the SAME org (nothing in the
    schema forbids it) both survive ``FINAL`` as distinct rows. The original
    ``project AS (... LIMIT 1)`` CTE (no ``ORDER BY``) would then pick one of
    them *nondeterministically*, and the provider guard would faithfully
    protect the WRONG provider's identity -- silently answering with a
    different project's repositories/facts depending on ClickHouse's whim.
    The fix (``HAVING count() = 1``) must instead treat this as unresolvable.
    """

    _install_catalog_client(monkeypatch, project_id=COLLIDING_PROJECT_ID)
    scope = await _committed_project_scope(project_id=COLLIDING_PROJECT_ID)
    assert len(PROJECTS_CATALOG[(ORG_ID, COLLIDING_PROJECT_ID)]) == 2

    fake = _install_status_client(monkeypatch)
    service = StatusChangeService(ClickHouseStatusChangeSource(object(), now=NOW))

    snapshot = await service.status_snapshot(
        ORG_ID, "permission-fingerprint", StatusSnapshotRequest(scope)
    )

    assert NO_REPOSITORY_SET_WARNING in snapshot.warnings
    assert snapshot.children == ()
    assert not any("parent_id" in sql for sql in fake.sql), (
        "no fact query may run on a repository set that was never resolved"
    )


@pytest.mark.asyncio
async def test_retired_project_identity_stays_fail_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Codex adversarial review (MEDIUM, 2026-08-04), round 2.

    The authorized-entity catalog's own committing query
    (``ClickHouseAuthorizedEntityCatalog._query_for(EntityKind.PROJECT)``)
    filters ``is_active = 1``, so a project retired AFTER a scope was
    committed against it (a newer ReplacingMergeTree row with
    ``is_active = 0``) must not keep answering here just because the identity
    CTE re-reads the same table without that filter. The scope-resolution
    catalog fake below still resolves ``RETIRED_PROJECT_ID`` (modeling a
    scope committed while the project was active); only the
    native_status_change-side catalog reflects the later retirement.
    """

    _install_catalog_client(monkeypatch, project_id=RETIRED_PROJECT_ID)
    scope = await _committed_project_scope(project_id=RETIRED_PROJECT_ID)
    assert PROJECTS_CATALOG[(ORG_ID, RETIRED_PROJECT_ID)][0]["is_active"] == 0

    fake = _install_status_client(monkeypatch)
    service = StatusChangeService(ClickHouseStatusChangeSource(object(), now=NOW))

    snapshot = await service.status_snapshot(
        ORG_ID, "permission-fingerprint", StatusSnapshotRequest(scope)
    )

    assert NO_REPOSITORY_SET_WARNING in snapshot.warnings
    assert snapshot.children == ()
    assert not any("parent_id" in sql for sql in fake.sql), (
        "no fact query may run on a repository set that was never resolved"
    )


@pytest.mark.asyncio
async def test_derived_repository_set_is_exactly_the_projects_own_work(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The derived bound is neither too narrow nor too wide.

    Asserted on the ``repository_ids`` the real ``_WORK_ITEMS_SQL`` read was
    actually bound by -- the one observable that distinguishes a correct
    derivation from one that dropped the tenant bound, the ``as_of`` bound, or
    the identity join.
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
    # The Jira-only repository must not leak into the Linear project's set --
    # the provider guard applies to the derivation, not just the fact reads.
    assert KEYED_REPOSITORY_ID not in bound
    assert COLLISION_REPOSITORY_ID not in bound


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


# ---------------------------------------------------------------------------
# CHAOS-3368: the project's own declared state / target date
# (projects.state/target_date, migration 073) must surface as status facts
# for a committed PROJECT subject, additive to (never a substitute for) the
# derived work-item completion tree this file already pins above.
# ---------------------------------------------------------------------------


def _install_status_client_with_project_row(
    monkeypatch: pytest.MonkeyPatch,
    *,
    project_row: dict[str, Any] | None,
) -> _FakeWorkItems:
    """``_install_status_client`` plus a controllable ``projects`` catalog
    read, without touching ``_FakeWorkItems`` itself (every other test in
    this file depends on its existing, unmodified behavior -- an unhandled
    query there already falls through to an empty result, which is exactly
    what "no project row" needs, so only the new query is intercepted here).

    Matched on ``"any(state) AS state"`` -- CHAOS-3368's
    ``_PROJECT_DECLARED_FACTS_SQL`` own, unique SELECT list -- rather than
    the broader ``"FROM projects FINAL"``: CHAOS-3374's
    ``_PROJECT_IDENTITY_CTE`` is now spliced into EVERY project-scoped arm
    (``_WORK_ITEMS_SQL``, ``PROJECT_REPOSITORIES_SQL``, ``_BLOCKERS_SQL``,
    ...) and that CTE also reads ``FROM projects FINAL``, so the broader
    substring would wrongly intercept those queries too and starve
    ``_FakeWorkItems`` of the calls it needs to answer them.
    """

    fake = _FakeWorkItems()

    async def wrapped(_client: object, sql: str, params: dict[str, Any]) -> Any:
        if "any(state) AS state" in sql:
            fake.sql.append(sql)
            fake.params.append(dict(params))
            if (
                project_row is None
                or params.get("org_id") != ORG_ID
                or params.get("entity_id") != PROJECT_ID
            ):
                return []
            return [project_row]
        return await fake(_client, sql, params)

    monkeypatch.setattr(native_status_change, "query_dicts", wrapped)
    return fake


@pytest.mark.asyncio
async def test_committed_project_scope_surfaces_declared_state_and_target_date(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CHAOS-3368 acceptance: a committed PROJECT subject whose catalog row
    carries both a declared state and a target date surfaces both as a
    status fact, alongside (not instead of) the derived work-item tree this
    file already pins in
    ``test_committed_project_scope_status_snapshot_reads_its_work_items``.
    """

    _install_catalog_client(monkeypatch)
    scope = await _committed_project_scope()

    fake = _install_status_client_with_project_row(
        monkeypatch,
        project_row={
            "state": "started",
            "target_date": date(2026, 9, 1),
            "updated_at": NOW,
            "last_synced": NOW,
        },
    )
    service = StatusChangeService(ClickHouseStatusChangeSource(object(), now=NOW))

    snapshot = await service.status_snapshot(
        ORG_ID, "permission-fingerprint", StatusSnapshotRequest(scope)
    )

    assert len(snapshot.project_facts) == 1
    fact = snapshot.project_facts[0]
    assert fact.entity_type == "project"
    assert fact.entity_id == PROJECT_ID
    assert "started" in fact.status
    assert "2026-09-01" in fact.status
    # The pre-existing derived work-item tree is unaffected.
    assert {child.entity_id for child in snapshot.children} == IN_SCOPE_WORK_ITEM_IDS
    # The declared-state read really was issued with this project's identity.
    assert fake.sql, "the projects catalog read was never issued"
    assert fake.params[0]["entity_id"] == PROJECT_ID
    assert fake.params[0]["org_id"] == ORG_ID


@pytest.mark.asyncio
async def test_committed_project_scope_declared_state_absent_when_catalog_row_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CHAOS-3368 negative control: no matching ``projects`` catalog row (the
    RED case on ``main`` today -- this ticket's whole premise is that the
    column data exists since #1450 but nothing reads it) must render as an
    absent fact, never a fabricated one, and must not disturb the derived
    tree.
    """

    _install_catalog_client(monkeypatch)
    scope = await _committed_project_scope()

    _install_status_client_with_project_row(monkeypatch, project_row=None)
    service = StatusChangeService(ClickHouseStatusChangeSource(object(), now=NOW))

    snapshot = await service.status_snapshot(
        ORG_ID, "permission-fingerprint", StatusSnapshotRequest(scope)
    )

    assert snapshot.project_facts == ()
    assert {child.entity_id for child in snapshot.children} == IN_SCOPE_WORK_ITEM_IDS


@pytest.mark.asyncio
async def test_committed_project_scope_declared_state_absent_when_columns_empty(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CHAOS-3368 negative control: a real catalog row that carries neither a
    declared state nor a target date (``state`` defaults to ``''``,
    ``target_date`` is ``NULL`` -- most providers today) must not fabricate
    an empty-string status or an epoch/zero date fact.
    """

    _install_catalog_client(monkeypatch)
    scope = await _committed_project_scope()

    _install_status_client_with_project_row(
        monkeypatch,
        project_row={
            "state": "",
            "target_date": None,
            "updated_at": NOW,
            "last_synced": NOW,
        },
    )
    service = StatusChangeService(ClickHouseStatusChangeSource(object(), now=NOW))

    snapshot = await service.status_snapshot(
        ORG_ID, "permission-fingerprint", StatusSnapshotRequest(scope)
    )

    assert snapshot.project_facts == ()


@pytest.mark.asyncio
async def test_issue_scope_never_queries_the_projects_catalog(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CHAOS-3368 negative control: an ISSUE-scope status_snapshot call must
    behave byte-identically to before this change -- in particular, it must
    never issue the new declared-state read at all.
    """

    entity_id = next(iter(IN_SCOPE_WORK_ITEM_IDS))
    scope = DevScope(
        schema_version="dev_scope.v1",
        organization_id=ORG_ID,
        direct_scope=DirectScope.ISSUE,
        repositories=[LINEAR_NO_REPOSITORY_ID],
        entity_refs=[
            DevEntityRef(
                entity_type=EntityType.ISSUE,
                entity_id=entity_id,
                display_label=entity_id,
                repository_id=LINEAR_NO_REPOSITORY_ID,
            )
        ],
        time_range=DevTimeRange(start=NOW - timedelta(days=7), end=NOW, timezone="UTC"),
    )

    fake = _install_status_client(monkeypatch)
    service = StatusChangeService(ClickHouseStatusChangeSource(object(), now=NOW))

    snapshot = await service.status_snapshot(
        ORG_ID, "permission-fingerprint", StatusSnapshotRequest(scope)
    )

    assert snapshot.project_facts == ()
    # Matched on the declared-state read's own unique SELECT list, not the
    # broader "FROM projects FINAL" -- CHAOS-3374's _PROJECT_IDENTITY_CTE is
    # spliced into _WORK_ITEMS_SQL itself now, so that substring legitimately
    # appears even for an ISSUE-scope call that never reaches CHAOS-3368's
    # own query.
    assert not any("any(state) AS state" in sql for sql in fake.sql)
