"""Live-ClickHouse proof for the committed-project repository derivation.

The unit suite for this fix
(``test_project_scope_status_snapshot_repositories.py``) replaces the
ClickHouse round trip with a predicate evaluator. That is honest about what it
covers and explicit about what it cannot: boolean precedence, the ``repos``
sub-select, ``LIMIT``, and anything else that is a property of the *engine*
rather than of the predicate list. This file closes that gap by executing
``PROJECT_REPOSITORIES_SQL`` against a real migrated ClickHouse over rows
written by the real producers, and then **mutation-checks the real results**:
each mutant SQL is executed too, and must return a materially different set.
Without that second half, a green assertion here would only prove the query
runs, not that the assertion could ever have failed.

Seeded shapes, one per rule the derivation has to get right:

* Linear (repository-less, zero-UUID ``repo_id``) — must be admitted;
* Linear, a nonzero AUTHORIZED repository — must be admitted;
* a nonzero repository absent from ``repos`` — must be excluded;
* a second tenant's rows for the same project id — must be excluded;
* a row updated after ``as_of`` — must be excluded.

CHAOS-3374 adds a second project, Jira-shaped exactly like production
(``team_autoimport_jira._project_id`` mints the catalog id as
``f"{org_id}:jira:{project_key}"``; ``providers/jira/normalize`` writes the
RAW key onto ``work_items.project_key`` via the real
``canonical_jira_issue_to_work_item`` producer), plus a cross-provider
collision row that shares the Jira project's raw key under a DIFFERENT
provider:

* Jira, attributed by the raw ``project_key`` (not ``project_id``) — must be
  admitted only under the Jira scope, proving the provider-scoped identity
  join added by CHAOS-3374 against a real engine, not a predicate fake;
* a Linear-provider row carrying that SAME raw key — must be excluded from
  the Jira scope (and was never admissible to the Linear scope either, since
  a real Linear catalog row's own ``project_key`` is always empty).

Opt-in (filtered from unit/CI by ``ci/run_tests.sh``'s
``-m "not benchmark and not clickhouse"``): ``pytest -m clickhouse`` with
``CLICKHOUSE_URI`` pointing at a SCRATCH database — never the dev ``default``.
"""

from __future__ import annotations

import os
import uuid
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import Any
from unittest.mock import MagicMock

import pytest
from atlassian import JiraIssue

from dev_health_ops.api.dev.native_status_change import (
    PROJECT_REPOSITORIES_SQL,
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
from dev_health_ops.api.queries.client import query_dicts
from dev_health_ops.metrics.schemas import ProjectRecord
from dev_health_ops.providers.identity import IdentityResolver
from dev_health_ops.providers.jira.normalize import canonical_jira_issue_to_work_item
from dev_health_ops.providers.linear.normalize import linear_issue_to_work_item
from dev_health_ops.providers.status_mapping import StatusMapping

CLICKHOUSE_URI = os.environ.get("CLICKHOUSE_URI")

NOW = datetime.now(UTC).replace(microsecond=0)
ZERO_REPOSITORY_ID = "00000000-0000-0000-0000-000000000000"
PROJECT_NAME = "Ask Dev Project Scope Live"

NO_REPOSITORY_SET_WARNING = (
    "Status reads require the complete authorized repository set; "
    "scope was not widened."
)

#: Database names this file refuses to touch — it writes rows. A warning in a
#: docstring is not an enforcement boundary; this is.
_PROTECTED_DATABASES = frozenset({"", "default"})


def _database_of(dsn: str | None) -> str:
    from urllib.parse import urlparse

    return urlparse(dsn or "").path.lstrip("/").strip().lower()


def _scratch_database(dsn: str) -> str:
    """Return the DSN's database, or fail closed if it is a protected one.

    Kept as a hard ``RuntimeError`` even though the skip below normally
    prevents it from firing: the skip is convenience, this is the boundary.
    A developer who forces the fixture past the marker must still not seed the
    dev database.
    """

    database = _database_of(dsn)
    if database in _PROTECTED_DATABASES:
        raise RuntimeError(
            "refusing to seed ClickHouse database "
            f"{database or '<unset>'!r}: point CLICKHOUSE_URI at a named "
            "SCRATCH database (e.g. .../ci_local_validate)."
        )
    return database


#: A developer's shell commonly exports ``CLICKHOUSE_URI`` pointing at the dev
#: ``default`` database. Skipping (loudly, with this reason) rather than
#: erroring keeps that from looking like a broken test -- while still never
#: reading as a pass: the measurement plainly did not happen.
_SKIP_REASON = (
    "Requires a migrated SCRATCH CLICKHOUSE_URI "
    "(e.g. clickhouse://ch:ch@localhost:8123/ci_local_validate); "
    f"got database {_database_of(CLICKHOUSE_URI) or '<unset>'!r}, which this "
    "suite refuses to seed"
)

pytestmark = [
    pytest.mark.clickhouse,
    pytest.mark.skipif(
        not CLICKHOUSE_URI or _database_of(CLICKHOUSE_URI) in _PROTECTED_DATABASES,
        reason=_SKIP_REASON,
    ),
]


@pytest.fixture
def sink() -> Any:
    from dev_health_ops.metrics.sinks.clickhouse import ClickHouseMetricsSink

    assert CLICKHOUSE_URI is not None
    _scratch_database(CLICKHOUSE_URI)
    metrics_sink = ClickHouseMetricsSink(CLICKHOUSE_URI)
    try:
        yield metrics_sink
    finally:
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


def _work_item(
    identifier: str,
    *,
    org_id: str,
    project_id: str,
    repo_id: uuid.UUID | None,
    project_key: str | None = None,
    updated_at: datetime | None = None,
) -> Any:
    """One work item through the production Linear normalizer.

    Hand-built fixtures pass unit tests while the live path fails; the fields
    this test actually varies (tenant, repository, project attribution, time)
    are patched onto the normalizer's own output rather than replacing it.
    """

    identity = MagicMock(spec=IdentityResolver)
    identity.resolve.side_effect = lambda **kwargs: "user:dev@example.com"
    status_mapping = MagicMock(spec=StatusMapping)
    status_mapping.normalize_status.return_value = "in_progress"
    status_mapping.normalize_type.return_value = "task"

    issue = {
        "id": f"issue-{identifier}",
        "identifier": identifier,
        "title": f"Work item {identifier}",
        "createdAt": (NOW - timedelta(days=10)).isoformat(),
        "updatedAt": (NOW - timedelta(days=1)).isoformat(),
        "state": {"id": "s1", "name": "In Progress", "type": "started"},
        "team": {"id": "t1", "key": "CHAOS", "name": "Fullchaos"},
        "project": {"id": project_id, "name": PROJECT_NAME},
    }
    item, _ = linear_issue_to_work_item(
        issue=issue, status_mapping=status_mapping, identity=identity
    )
    return replace(
        item,
        org_id=org_id,
        repo_id=repo_id,
        project_id=project_id,
        project_key=project_key,
        updated_at=updated_at or (NOW - timedelta(days=1)),
    )


def _jira_work_item(
    key: str,
    *,
    org_id: str,
    project_key: str,
    repo_id: uuid.UUID | None,
    updated_at: datetime | None = None,
) -> Any:
    """One work item through the production Jira canonical normalizer.

    ``canonical_jira_issue_to_work_item`` is the real producer that sets
    ``project_id=None`` and ``project_key=issue.project_key`` (see
    ``providers/jira/normalize.py``) -- the exact identity mismatch
    CHAOS-3374 fixes: the catalog id is ``f"{org_id}:jira:{project_key}"``
    (provider-prefixed) but ``work_items.project_key`` carries the RAW key.
    """

    identity = MagicMock(spec=IdentityResolver)
    identity.resolve.side_effect = lambda **kwargs: "user:dev@example.com"
    status_mapping = MagicMock(spec=StatusMapping)
    status_mapping.normalize_status.return_value = "in_progress"
    status_mapping.normalize_type.return_value = "task"

    issue = JiraIssue(
        cloud_id="cloud-live",
        key=key,
        project_key=project_key,
        issue_type="Task",
        status="In Progress",
        created_at=(NOW - timedelta(days=10)).isoformat(),
        updated_at=(NOW - timedelta(days=1)).isoformat(),
    )
    item = canonical_jira_issue_to_work_item(
        issue=issue,
        status_mapping=status_mapping,
        identity=identity,
        repo_id=repo_id,
    )
    return replace(
        item,
        org_id=org_id,
        updated_at=updated_at or (NOW - timedelta(days=1)),
    )


def _insert_repo(client: Any, *, org_id: str, repo_id: str, name: str) -> None:
    client.command(
        "INSERT INTO repos (id, repo, created_at, last_synced, org_id) VALUES "
        "({repo_id:UUID}, {name:String}, now64(3), now64(3), {org_id:String})",
        parameters={"repo_id": repo_id, "name": name, "org_id": org_id},
    )


class _Seeded:
    def __init__(self) -> None:
        self.org_id = f"proj-scope-{uuid.uuid4().hex[:16]}"
        self.other_org_id = f"proj-scope-other-{uuid.uuid4().hex[:16]}"
        self.project_id = str(uuid.uuid4())
        #: Referenced only by the *other* tenant, but WITH its own catalog row
        #: under that tenant -- so a mutant that drops the ``work_items``-level
        #: ``org_id`` bound (rather than merely the identity CTE's own) is what
        #: this org's probe against it exercises (see
        #: test_every_derivation_clause_changes_the_real_result's docstring).
        self.other_only_project_id = str(uuid.uuid4())
        self.authorized_repo = uuid.uuid4()
        self.stale_window_repo = uuid.uuid4()
        self.revoked_repo = uuid.uuid4()
        self.foreign_repo = uuid.uuid4()
        # CHAOS-3374: a Jira-shaped project. The catalog id is
        # provider-prefixed exactly like production; the raw key is what
        # actually lands on work_items.project_key.
        self.jira_project_key = f"ASK{uuid.uuid4().hex[:6].upper()}"
        self.jira_project_id = f"{self.org_id}:jira:{self.jira_project_key}"
        self.jira_repo = uuid.uuid4()
        #: A DIFFERENT provider's repository, holding a row that shares the
        #: Jira project's raw key -- must never be admitted by either scope.
        self.collision_repo = uuid.uuid4()

    @property
    def expected(self) -> list[str]:
        return sorted({ZERO_REPOSITORY_ID, str(self.authorized_repo)})

    @property
    def jira_expected(self) -> list[str]:
        return sorted({str(self.jira_repo)})


@pytest.fixture
def seeded(sink: Any, raw_client: Any) -> Any:
    data = _Seeded()
    try:
        # Authorization catalog: the revoked repository is deliberately absent,
        # while its work items below are not.
        _insert_repo(
            raw_client,
            org_id=data.org_id,
            repo_id=str(data.authorized_repo),
            name="org/authorized",
        )
        _insert_repo(
            raw_client,
            org_id=data.org_id,
            repo_id=str(data.stale_window_repo),
            name="org/stale-window",
        )
        _insert_repo(
            raw_client,
            org_id=data.other_org_id,
            repo_id=str(data.foreign_repo),
            name="other/repo",
        )
        _insert_repo(
            raw_client,
            org_id=data.org_id,
            repo_id=str(data.jira_repo),
            name="org/jira-project",
        )
        _insert_repo(
            raw_client,
            org_id=data.org_id,
            repo_id=str(data.collision_repo),
            name="org/collision",
        )

        sink.org_id = data.org_id
        sink.write_projects(
            [
                ProjectRecord(
                    id=data.project_id,
                    org_id=data.org_id,
                    provider="linear",
                    name=PROJECT_NAME,
                    is_active=1,
                    updated_at=NOW,
                    last_synced=NOW,
                ),
                ProjectRecord(
                    id=data.jira_project_id,
                    org_id=data.org_id,
                    provider="jira",
                    project_key=data.jira_project_key,
                    name=PROJECT_NAME + " (Jira)",
                    is_active=1,
                    updated_at=NOW,
                    last_synced=NOW,
                ),
                # CHAOS-3374: deliberately scoped to THIS org's catalog even
                # though the only matching work_items row (LIVE-7, below) is
                # written under other_org_id -- isolates the work_items-level
                # org_id bound as the ONLY thing standing between "excluded"
                # and "leaked" for the repository-less sentinel bucket, which
                # the ``repos`` authorization arm cannot gate (see
                # test_every_derivation_clause_changes_the_real_result's own
                # "org_id tenant bound" mutant). Scoping this row under
                # other_org_id instead would let the identity join's OWN
                # (unmutated) org_id check mask the work_items-level mutant
                # entirely, making it undetectable by either probe.
                ProjectRecord(
                    id=data.other_only_project_id,
                    org_id=data.org_id,
                    provider="linear",
                    name=PROJECT_NAME + " (other tenant's reference)",
                    is_active=1,
                    updated_at=NOW,
                    last_synced=NOW,
                ),
            ]
        )
        sink.write_work_items(
            [
                # Linear: repository-less, lands on the zero UUID.
                _work_item(
                    "LIVE-1",
                    org_id=data.org_id,
                    project_id=data.project_id,
                    repo_id=None,
                ),
                _work_item(
                    "LIVE-2",
                    org_id=data.org_id,
                    project_id=data.project_id,
                    repo_id=None,
                ),
                # A nonzero, AUTHORIZED repository -- proves the derivation
                # isn't reachable only through the zero-UUID sentinel.
                _work_item(
                    "LIVE-3",
                    org_id=data.org_id,
                    project_id=data.project_id,
                    repo_id=data.authorized_repo,
                ),
                # Authorized repository, but outside the as_of bound.
                _work_item(
                    "LIVE-4",
                    org_id=data.org_id,
                    project_id=data.project_id,
                    repo_id=data.stale_window_repo,
                    updated_at=NOW + timedelta(days=30),
                ),
                # Nonzero repository with no current ``repos`` row.
                _work_item(
                    "LIVE-5",
                    org_id=data.org_id,
                    project_id=data.project_id,
                    repo_id=data.revoked_repo,
                ),
                # CHAOS-3374: the Jira project's own work item, attributed
                # ONLY via the raw project_key arm (project_id is None, from
                # the real canonical_jira_issue_to_work_item producer).
                _jira_work_item(
                    "ASK-9",
                    org_id=data.org_id,
                    project_key=data.jira_project_key,
                    repo_id=data.jira_repo,
                ),
                # CHAOS-3374 cross-provider collision: a Linear-provider row
                # (adversarially) carrying the Jira project's raw key. Without
                # the provider guard on the identity join this would leak
                # into the Jira scope via the project_key arm.
                _work_item(
                    "COLLIDE-1",
                    org_id=data.org_id,
                    project_id="",
                    project_key=data.jira_project_key,
                    repo_id=data.collision_repo,
                ),
            ]
        )
        sink.org_id = data.other_org_id
        sink.write_work_items(
            [
                _work_item(
                    "LIVE-6",
                    org_id=data.other_org_id,
                    project_id=data.project_id,
                    repo_id=data.foreign_repo,
                ),
                # Repository-less, exactly like ours: the sentinel bucket is
                # the one place the ``repos`` arm cannot separate tenants, so
                # this is what makes the fact table's own ``org_id`` bound
                # load-bearing rather than merely redundant.
                _work_item(
                    "LIVE-7",
                    org_id=data.other_org_id,
                    project_id=data.other_only_project_id,
                    repo_id=None,
                ),
            ]
        )
        yield data
    finally:
        for table in ("work_items", "projects", "repos"):
            raw_client.command(
                f"ALTER TABLE {table} DELETE WHERE org_id IN "
                "({org_id:String}, {other_org_id:String})",
                parameters={
                    "org_id": data.org_id,
                    "other_org_id": data.other_org_id,
                },
            )


async def _derive(sink: Any, sql: str, seeded: Any, *, project_id: str) -> list[str]:
    rows = await query_dicts(
        sink,
        sql,
        {
            "org_id": seeded.org_id,
            "entity_id": project_id,
            "as_of": NOW,
        },
    )
    return sorted(
        {str(row["repository_id"]) for row in rows if row.get("repository_id")}
    )


async def _probes(sink: Any, sql: str, seeded: Any) -> tuple[list[str], list[str]]:
    """Both observable answers this query has to get right.

    The second probe is not redundant. A project referenced only by another
    tenant must derive *nothing* for this org; deriving the sentinel bucket
    for it instead would turn a disclosed fail-closed answer into a confident
    empty one -- a difference the first probe's repository set cannot show,
    because the sentinel is already in it legitimately.
    """

    return (
        await _derive(sink, sql, seeded, project_id=seeded.project_id),
        await _derive(sink, sql, seeded, project_id=seeded.other_only_project_id),
    )


@pytest.mark.asyncio
async def test_derivation_returns_exactly_the_authorized_project_repositories(
    sink: Any, seeded: Any
) -> None:
    own, other_tenants = await _probes(sink, PROJECT_REPOSITORIES_SQL, seeded)
    assert own == seeded.expected
    assert other_tenants == []


@pytest.mark.asyncio
async def test_jira_shaped_project_derivation_resolves_live(
    sink: Any, seeded: Any
) -> None:
    """CHAOS-3374: the provider-scoped identity join, against a real engine.

    Before the fix, ``PROJECT_REPOSITORIES_SQL`` compared
    ``work_items.project_id``/``project_key`` against the catalog's
    (provider-prefixed) id directly -- ``project_id = {entity_id:String} OR
    project_key = {entity_id:String}`` -- which a raw Jira key never matches.
    This probe is the RED/GREEN pair for that: it would have returned ``[]``
    on main.
    """

    own = await _derive(
        sink, PROJECT_REPOSITORIES_SQL, seeded, project_id=seeded.jira_project_id
    )
    assert own == seeded.jira_expected
    # The cross-provider collision row must not leak in via the key arm.
    assert str(seeded.collision_repo) not in own


@pytest.mark.asyncio
async def test_every_derivation_clause_changes_the_real_result(
    sink: Any, seeded: Any
) -> None:
    """Mutation-check against the engine, not against a fake.

    Each mutant is executed on the same seeded data. A mutant that returns the
    correct set is a clause the assertion above could never have caught -- so
    the failure message names the clause rather than just reporting inequality.

    Scoped to the clauses shared by every provider (tenant/time bounds, the
    ``project_id`` arm, and the ``repos`` authorization arms), probed against
    the LINEAR-shaped project -- a real Linear catalog row's own
    ``project_key`` is always empty, so the native-key arm and the provider
    guard are dead code for this project and belong in
    ``test_every_jira_identity_clause_changes_the_real_result`` instead, where
    they are load-bearing.
    """

    mutants = {
        "org_id tenant bound on work_items": (
            "FROM work_items FINAL\nINNER JOIN project ON 1 = 1\nWHERE org_id = {org_id:String}\n",
            "FROM work_items FINAL\nINNER JOIN project ON 1 = 1\nWHERE 1 = 1\n",
        ),
        "as_of bound": (
            "  AND updated_at <= {as_of:DateTime64(3, 'UTC')}\n",
            "",
        ),
        "project_id arm": (
            "(project_id = {entity_id:String}"
            " OR (catalog_project_key != '' AND project_key = catalog_project_key))",
            "(catalog_project_key != '' AND project_key = catalog_project_key)",
        ),
        "repos authorization arm": (
            "    toString(repo_id) = '00000000-0000-0000-0000-000000000000'\n"
            "    OR toString(repo_id) IN (\n"
            "      SELECT toString(id) FROM repos FINAL WHERE org_id = {org_id:String}\n"
            "    )\n",
            "    1 = 1\n",
        ),
        "repository-less sentinel arm": (
            "    toString(repo_id) = '00000000-0000-0000-0000-000000000000'\n    OR ",
            "    ",
        ),
    }

    correct: tuple[list[str], list[str]] = (seeded.expected, [])
    survived: list[str] = []
    for label, (original, mutated) in mutants.items():
        assert original in PROJECT_REPOSITORIES_SQL, f"stale mutant anchor: {label}"
        sql = PROJECT_REPOSITORIES_SQL.replace(original, mutated, 1)
        if await _probes(sink, sql, seeded) == correct:
            survived.append(label)
    assert not survived, (
        "these clauses do not affect the real result, so the assertion above "
        f"cannot be catching them: {sorted(survived)}"
    )


@pytest.mark.asyncio
async def test_every_jira_identity_clause_changes_the_real_result(
    sink: Any, seeded: Any
) -> None:
    """CHAOS-3374: mutation-check the identity-join clauses, against a real engine.

    Probed against the Jira-shaped project, where the native-key arm and the
    provider guard are the ONLY thing standing between "resolves correctly"
    and "leaks the cross-provider collision row" (``COLLIDE-1``, seeded with
    the identical raw key under ``provider="linear"``).
    """

    async def probe(sql: str) -> list[str]:
        return await _derive(sink, sql, seeded, project_id=seeded.jira_project_id)

    mutants = {
        "provider identity guard": (
            "ifNull(catalog_provider, '') != '' AND provider = catalog_provider AND ",
            "",
        ),
        "native project_key arm": (
            "(project_id = {entity_id:String}"
            " OR (catalog_project_key != '' AND project_key = catalog_project_key))",
            "(project_id = {entity_id:String})",
        ),
        "project disjunction (OR -> AND)": (
            "(project_id = {entity_id:String}"
            " OR (catalog_project_key != '' AND project_key = catalog_project_key))",
            "(project_id = {entity_id:String}"
            " AND (catalog_project_key != '' AND project_key = catalog_project_key))",
        ),
    }

    correct = seeded.jira_expected
    survived: list[str] = []
    for label, (original, mutated) in mutants.items():
        assert original in PROJECT_REPOSITORIES_SQL, f"stale mutant anchor: {label}"
        sql = PROJECT_REPOSITORIES_SQL.replace(original, mutated, 1)
        if await probe(sql) == correct:
            survived.append(label)
    assert not survived, (
        "these identity-join clauses do not affect the Jira-shaped result, so "
        f"the collision guard cannot be catching them: {sorted(survived)}"
    )
    # And the specific claim the "provider identity guard" mutant proves:
    # without it, the collision row's repository leaks in.
    unguarded = PROJECT_REPOSITORIES_SQL.replace(
        "ifNull(catalog_provider, '') != '' AND provider = catalog_provider AND ",
        "",
        1,
    )
    leaked = await probe(unguarded)
    assert str(seeded.collision_repo) in leaked, (
        "expected the provider-unguarded mutant to admit the colliding "
        f"cross-provider repository, got {leaked}"
    )


@pytest.mark.asyncio
async def test_committed_project_scope_snapshot_reads_its_work_items_live(
    sink: Any, seeded: Any
) -> None:
    """The whole path: real catalog, real resolver, real SQL, real engine."""

    service = ScopeResolutionService(ClickHouseAuthorizedEntityCatalog(sink))
    resolution = await service.resolve_contract(
        seeded.org_id,
        "permission-live",
        ScopeResolveRequest(
            explicit_refs=(ScopeRef(EntityKind.PROJECT, seeded.project_id),)
        ),
    )
    assert resolution.outcome.value == "exact"
    scope = resolution.resolved_scope
    assert scope is not None
    assert scope.repositories == []
    assert scope.entity_refs[0].repository_id is None

    snapshot = await StatusChangeService(
        ClickHouseStatusChangeSource(sink)
    ).status_snapshot(seeded.org_id, "permission-live", StatusSnapshotRequest(scope))

    assert NO_REPOSITORY_SET_WARNING not in snapshot.warnings
    seen = {child.entity_id for child in snapshot.children}
    assert any(child.endswith("LIVE-1") for child in seen), seen
    assert any(child.endswith("LIVE-3") for child in seen), seen
    assert not any(child.endswith("LIVE-5") for child in seen), (
        "a repository absent from the authorization catalog reached the read",
        seen,
    )
    assert not any(child.endswith("LIVE-6") for child in seen), (
        "another tenant's work item reached the read",
        seen,
    )
    # CHAOS-3374: the Jira project's own item, and the cross-provider
    # collision row, must never surface under the LINEAR scope either.
    assert not any(child.endswith("ASK-9") for child in seen), (
        "the Jira-shaped project's own item leaked into the Linear scope",
        seen,
    )
    assert not any(child.endswith("COLLIDE-1") for child in seen), (
        "the cross-provider collision row leaked into the Linear scope",
        seen,
    )


@pytest.mark.asyncio
async def test_jira_shaped_project_snapshot_reads_its_work_items_live(
    sink: Any, seeded: Any
) -> None:
    """CHAOS-3374 end to end: real catalog, real resolver, real SQL, real engine.

    RED before the fix: this scenario (a Jira project resolved through the
    real ``ScopeResolutionService``, then read through the real
    ``ClickHouseStatusChangeSource``) returned ``NO_REPOSITORY_SET_WARNING``
    and an empty ``snapshot.children`` on main -- the disclosed fail-closed
    refusal ``test_jira_shaped_project_keeps_todays_fail_closed_behaviour``
    (unit suite) pinned. GREEN after the fix: the project resolves its own
    work item, and the cross-provider collision row stays excluded.
    """

    service = ScopeResolutionService(ClickHouseAuthorizedEntityCatalog(sink))
    resolution = await service.resolve_contract(
        seeded.org_id,
        "permission-live",
        ScopeResolveRequest(
            explicit_refs=(ScopeRef(EntityKind.PROJECT, seeded.jira_project_id),)
        ),
    )
    assert resolution.outcome.value == "exact"
    scope = resolution.resolved_scope
    assert scope is not None
    assert scope.repositories == []

    snapshot = await StatusChangeService(
        ClickHouseStatusChangeSource(sink)
    ).status_snapshot(seeded.org_id, "permission-live", StatusSnapshotRequest(scope))

    assert NO_REPOSITORY_SET_WARNING not in snapshot.warnings
    seen = {child.entity_id for child in snapshot.children}
    assert any(child.endswith("ASK-9") for child in seen), seen
    assert not any(child.endswith("COLLIDE-1") for child in seen), (
        "a Linear-provider row sharing Jira's raw project key leaked into "
        "the Jira scope",
        seen,
    )
    assert not any(
        child.endswith("LIVE-1") or child.endswith("LIVE-3") for child in seen
    ), ("the Linear project's own items leaked into the Jira scope", seen)


@pytest.mark.asyncio
async def test_team_filtered_project_scope_is_fail_closed_live(
    sink: Any, seeded: Any, raw_client: Any
) -> None:
    """No project SQL arm applies a team filter, so the request is refused."""

    raw_client.command(
        "INSERT INTO teams (id, name, provider, is_active, updated_at, "
        "last_synced, org_id) VALUES ({team_id:String}, 'Team A', 'linear', 1, "
        "now64(3), now64(3), {org_id:String})",
        parameters={"team_id": "team-live-a", "org_id": seeded.org_id},
    )
    try:
        service = ScopeResolutionService(ClickHouseAuthorizedEntityCatalog(sink))
        resolution = await service.resolve_contract(
            seeded.org_id,
            "permission-live",
            ScopeResolveRequest(
                explicit_refs=(ScopeRef(EntityKind.PROJECT, seeded.project_id),),
                team_filter_refs=(ScopeRef(EntityKind.TEAM, "team-live-a"),),
            ),
        )
        scope = resolution.resolved_scope
        assert scope is not None
        assert scope.team_ids == ["team-live-a"]

        snapshot = await StatusChangeService(
            ClickHouseStatusChangeSource(sink)
        ).status_snapshot(
            seeded.org_id, "permission-live", StatusSnapshotRequest(scope)
        )

        assert NO_REPOSITORY_SET_WARNING in snapshot.warnings
        assert snapshot.children == ()
    finally:
        raw_client.command(
            "ALTER TABLE teams DELETE WHERE org_id = {org_id:String}",
            parameters={"org_id": seeded.org_id},
        )
