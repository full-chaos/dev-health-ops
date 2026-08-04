"""Live-ClickHouse proof for the committed-team repository derivation.

``ClickHouseStatusChangeSource.team_repository_ids`` re-derives a committed
TEAM subject's repositories from ``team_repo_ownership`` (CHAOS-3303). The
unit suites for this area mock ``query_dicts`` entirely and so cannot prove
anything about the query's own predicates -- this file closes that gap by
executing ``_TEAM_REPOSITORIES_SQL`` against a real migrated ClickHouse over
rows written directly to ``team_repo_ownership``/``repos``.

CHAOS-3375 (Codex adversarial review, HIGH): the first cut of this query
trusted every ``team_repo_ownership.repo_id`` outright. ClickHouse enforces
no foreign key, so a repository revoked from ``repos`` -- de-authorized --
can keep a stale ``team_repo_ownership`` row and become an admitted read
bound for team scope alone (an *unrelated, removed* repository's watermark
queries would then run). This file seeds exactly that shape and asserts the
orphaned repository is excluded, while a genuinely authorized one is
admitted.

Opt-in (filtered from unit/CI by ``ci/run_tests.sh``'s
``-m "not benchmark and not clickhouse"``): ``pytest -m clickhouse`` with
``CLICKHOUSE_URI`` pointing at a SCRATCH database -- never the dev
``default``.
"""

from __future__ import annotations

import os
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from dev_health_ops.api.dev.native_status_change import ClickHouseStatusChangeSource

CLICKHOUSE_URI = os.environ.get("CLICKHOUSE_URI")

NOW = datetime.now(UTC).replace(microsecond=0)

#: Database names this file refuses to touch -- it writes rows. A warning in
#: a docstring is not an enforcement boundary; this is.
_PROTECTED_DATABASES = frozenset({"", "default"})


def _database_of(dsn: str | None) -> str:
    from urllib.parse import urlparse

    return urlparse(dsn or "").path.lstrip("/").strip().lower()


def _scratch_database(dsn: str) -> str:
    """Return the DSN's database, or fail closed if it is a protected one.

    Kept as a hard ``RuntimeError`` even though the skip below normally
    prevents it from firing: the skip is convenience, this is the boundary.
    A developer who forces the fixture past the marker must still not seed
    the dev database.
    """

    database = _database_of(dsn)
    if database in _PROTECTED_DATABASES:
        raise RuntimeError(
            "refusing to seed ClickHouse database "
            f"{database or '<unset>'!r}: point CLICKHOUSE_URI at a named "
            "SCRATCH database (e.g. .../ci_local_validate)."
        )
    return database


#: A developer's shell commonly exports ``CLICKHOUSE_URI`` pointing at the
#: dev ``default`` database. Skipping (loudly, with this reason) rather than
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


def _insert_repo(client: Any, *, org_id: str, repo_id: str, name: str) -> None:
    client.command(
        "INSERT INTO repos (id, repo, created_at, last_synced, org_id) VALUES "
        "({repo_id:UUID}, {name:String}, now64(3), now64(3), {org_id:String})",
        parameters={"repo_id": repo_id, "name": name, "org_id": org_id},
    )


def _insert_team_repo_ownership(
    client: Any,
    *,
    org_id: str,
    team_id: str,
    repo_id: str,
    repo_full_name: str,
    valid_from: datetime,
    valid_to: datetime | None = None,
) -> None:
    client.command(
        "INSERT INTO team_repo_ownership "
        "(org_id, provider, team_id, repo_id, repo_full_name, match_type, "
        "source, is_primary, specificity, priority, valid_from, valid_to, "
        "updated_at) VALUES "
        "({org_id:String}, 'github', {team_id:String}, {repo_id:UUID}, "
        "{repo_full_name:String}, 'exact', 'native', 1, 100, 0, "
        "{valid_from:DateTime64(3, 'UTC')}, {valid_to:Nullable(DateTime64(3, 'UTC'))}, "
        "now64(3))",
        parameters={
            "org_id": org_id,
            "team_id": team_id,
            "repo_id": repo_id,
            "repo_full_name": repo_full_name,
            "valid_from": valid_from,
            "valid_to": valid_to,
        },
    )


class _Seeded:
    def __init__(self) -> None:
        self.org_id = f"team-scope-{uuid.uuid4().hex[:16]}"
        self.other_org_id = f"team-scope-other-{uuid.uuid4().hex[:16]}"
        self.team_id = f"team-{uuid.uuid4().hex[:12]}"
        self.other_team_id = f"team-other-{uuid.uuid4().hex[:12]}"
        self.authorized_repo = uuid.uuid4()
        #: Owned by the team in ``team_repo_ownership``, but with NO
        #: corresponding row in ``repos`` -- a revoked/deleted repository
        #: whose stale ownership row must never authorize a read.
        self.orphaned_repo = uuid.uuid4()
        #: Authorized in ``repos``, but the ownership window has already
        #: closed (``valid_to`` in the past) -- isolates the ``valid_to``
        #: clause: it is NOT also orphaned, so only that clause excludes it.
        self.expired_repo = uuid.uuid4()
        #: Authorized in ``repos``, but the ownership window has not started
        #: yet (``valid_from`` in the future) -- isolates the ``valid_from``
        #: clause the same way.
        self.future_repo = uuid.uuid4()
        #: Owned by a DIFFERENT team in the SAME org, authorized in
        #: ``repos`` -- isolates the ``team_id`` clause.
        self.other_team_repo = uuid.uuid4()
        #: Owned by ``team_id`` under a DIFFERENT org, but (deliberately)
        #: also present in THIS org's ``repos`` catalog -- isolates the
        #: ``team_repo_ownership``-level ``org_id`` clause specifically: if
        #: it were removed, only this row (not the repos-catalog check)
        #: would newly admit it.
        self.foreign_org_repo = uuid.uuid4()


@pytest.fixture
def seeded(sink: Any, raw_client: Any) -> Any:
    data = _Seeded()
    for repo_id, name in (
        (data.authorized_repo, "org/authorized"),
        (data.expired_repo, "org/expired"),
        (data.future_repo, "org/future"),
        (data.other_team_repo, "org/other-team"),
        (data.foreign_org_repo, "org/foreign-org"),
    ):
        _insert_repo(raw_client, org_id=data.org_id, repo_id=str(repo_id), name=name)
    # Deliberately no ``repos`` row for ``orphaned_repo`` under ANY org --
    # ownership rows alone must never be sufficient to authorize a
    # repository.
    _insert_team_repo_ownership(
        raw_client,
        org_id=data.org_id,
        team_id=data.team_id,
        repo_id=str(data.authorized_repo),
        repo_full_name="org/authorized",
        valid_from=NOW - timedelta(days=30),
    )
    _insert_team_repo_ownership(
        raw_client,
        org_id=data.org_id,
        team_id=data.team_id,
        repo_id=str(data.orphaned_repo),
        repo_full_name="org/orphaned",
        valid_from=NOW - timedelta(days=30),
    )
    _insert_team_repo_ownership(
        raw_client,
        org_id=data.org_id,
        team_id=data.team_id,
        repo_id=str(data.expired_repo),
        repo_full_name="org/expired",
        valid_from=NOW - timedelta(days=60),
        valid_to=NOW - timedelta(days=1),
    )
    _insert_team_repo_ownership(
        raw_client,
        org_id=data.org_id,
        team_id=data.team_id,
        repo_id=str(data.future_repo),
        repo_full_name="org/future",
        valid_from=NOW + timedelta(days=1),
    )
    _insert_team_repo_ownership(
        raw_client,
        org_id=data.org_id,
        team_id=data.other_team_id,
        repo_id=str(data.other_team_repo),
        repo_full_name="org/other-team",
        valid_from=NOW - timedelta(days=30),
    )
    _insert_team_repo_ownership(
        raw_client,
        org_id=data.other_org_id,
        team_id=data.team_id,
        repo_id=str(data.foreign_org_repo),
        repo_full_name="org/foreign-org",
        valid_from=NOW - timedelta(days=30),
    )
    return data


@pytest.mark.asyncio
async def test_orphaned_ownership_row_cannot_reach_any_watermark_query(
    sink: Any, seeded: Any
) -> None:
    """CHAOS-3375: a ``team_repo_ownership`` row with no ``repos`` catalog

    row must never be admitted -- proven against a real engine, not a
    predicate fake. Without the fix's ``repos``-catalog intersection, this
    assertion fails: the orphaned repository comes back alongside the
    authorized one.
    """

    source = ClickHouseStatusChangeSource(sink, now=NOW)
    result = await source.team_repository_ids(seeded.org_id, seeded.team_id, as_of=NOW)

    assert result.measured is True
    assert result.repository_ids == (str(seeded.authorized_repo),)
    assert str(seeded.orphaned_repo) not in result.repository_ids
    assert str(seeded.expired_repo) not in result.repository_ids
    assert str(seeded.future_repo) not in result.repository_ids
    assert str(seeded.other_team_repo) not in result.repository_ids
    assert str(seeded.foreign_org_repo) not in result.repository_ids


@pytest.mark.asyncio
async def test_every_derivation_clause_changes_the_real_result(
    sink: Any, seeded: Any
) -> None:
    """Mutation-check against the engine, not against a fake.

    Each mutant is executed on the same seeded data. A mutant that returns
    the correct set is a clause the assertion above could never have
    caught -- so the failure message names the clause rather than just
    reporting inequality.
    """

    from dev_health_ops.api.dev.native_status_change import _TEAM_REPOSITORIES_SQL
    from dev_health_ops.api.queries.client import query_dicts

    correct = [str(seeded.authorized_repo)]
    mutants = {
        "org_id tenant bound on team_repo_ownership": (
            "FROM team_repo_ownership\n  WHERE org_id = {org_id:String}\n",
            "FROM team_repo_ownership\n  WHERE 1 = 1\n",
        ),
        "team_id bound": (
            "AND team_id = {team_id:String}\n",
            "",
        ),
        "valid_from bound": (
            "AND valid_from <= {as_of:DateTime64(3, 'UTC')}\n",
            "",
        ),
        "valid_to bound": (
            "AND (valid_to IS NULL OR valid_to > {as_of:DateTime64(3, 'UTC')})\n",
            "",
        ),
        "repos authorization arm": (
            "AND toString(g.repo_id) IN (\n"
            "    SELECT toString(id) FROM repos FINAL WHERE org_id = {org_id:String}\n"
            "  )\n",
            "",
        ),
    }

    survived: list[str] = []
    for label, (original, mutated) in mutants.items():
        assert original in _TEAM_REPOSITORIES_SQL, f"stale mutant anchor: {label}"
        sql = _TEAM_REPOSITORIES_SQL.replace(original, mutated, 1)
        rows = await query_dicts(
            sink,
            sql,
            {"org_id": seeded.org_id, "team_id": seeded.team_id, "as_of": NOW},
        )
        observed = sorted(
            {str(row["repository_id"]) for row in rows if row.get("repository_id")}
        )
        if observed == sorted(correct):
            survived.append(label)

    assert not survived, (
        "these mutants returned the SAME result as the real query, so the "
        f"assertion above could never have caught them: {survived}"
    )
