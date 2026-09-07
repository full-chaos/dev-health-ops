"""``go_api_routing_admin`` against a REAL Postgres.

These need a real database, not a double: the whole module is
``ON CONFLICT DO UPDATE``, a ``GROUP BY``, a 4-column foreign key and a
composite primary key. A fake session would test the call shape and skip
every property that actually matters -- and "the rows looked fine" is
precisely how the 2026-09-01 failure survived six days.

Requires ``DEV_HEALTH_POSTGRES_TEST_URI`` (admin credentials able to
``CREATE DATABASE``), the same contract ``test_go_api_livelocal.py``
already uses; skipped without it. In CI that means these skip, so the
DB-backed half of the drift contract is ALSO proven on the Go plane,
where it runs against a Postgres testcontainer under the integration tag
(``cmd/query-api/registry_route_integration_test.go``). This file is the
Python-side proof of the writer and the status report, which have no Go
equivalent.
"""

from __future__ import annotations

import os
import uuid
from collections.abc import AsyncIterator
from typing import cast

import pytest
import pytest_asyncio
import sqlalchemy as sa
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from dev_health_ops.api.graphql.go_api_registry import (
    record_proof_run,
    register_candidate_build,
)
from dev_health_ops.api.graphql.go_api_routing_admin import (
    ENABLEMENT_PROOF_STAGE,
    ENABLEMENT_PROOF_TERMINAL_STATE,
    count_rows_by_schema_digest,
    enable_operation,
    operations_with_enablement_proof,
    routing_status_rows,
)
from dev_health_ops.models.git import Base
from dev_health_ops.models.go_api_registry import (
    CandidateBuild,
    ProofRun,
    RoutingState,
)

POSTGRES_TEST_URI = os.environ.get("DEV_HEALTH_POSTGRES_TEST_URI")

pytestmark = pytest.mark.skipif(
    not POSTGRES_TEST_URI,
    reason=(
        "Requires DEV_HEALTH_POSTGRES_TEST_URI (admin creds able to CREATE "
        "DATABASE) -- the go_api registry tables are Postgres-specific "
        "(ON CONFLICT, composite FK), so there is no in-memory substitute."
    ),
)

LIVE = "sha256:live-schema-digest"
SUPERSEDED = "sha256:superseded-schema-digest"
BUILD = "78fc68815e8209834c6e5acc37eb7653b52a7aa4"

CATALOG: tuple[tuple[str, str], ...] = (
    ("capacityForecast", "doc-capacity-forecast"),
    ("featureFlags", "doc-feature-flags"),
    ("reviewEdges", "doc-review-edges"),
)


@pytest_asyncio.fixture
async def session() -> AsyncIterator[AsyncSession]:
    """A scratch database with only the three go_api registry tables."""
    assert POSTGRES_TEST_URI is not None
    db_name = f"lane_go_api_routing_admin_{uuid.uuid4().hex}"
    admin = sa.create_engine(
        make_url(POSTGRES_TEST_URI).set(drivername="postgresql+psycopg2"),
        isolation_level="AUTOCOMMIT",
    )
    try:
        with admin.connect() as connection:
            connection.exec_driver_sql(f'CREATE DATABASE "{db_name}"')
    finally:
        admin.dispose()

    scratch_url = make_url(POSTGRES_TEST_URI).set(database=db_name)
    sync_engine = sa.create_engine(scratch_url.set(drivername="postgresql+psycopg2"))
    try:
        # cast: SQLAlchemy types ``__table__`` as FromClause, but
        # create_all wants Table. Same cast test_go_api_livelocal.py's
        # scratch-DB fixture already uses for these exact three tables.
        registry_tables = cast(
            list[sa.Table],
            [CandidateBuild.__table__, RoutingState.__table__, ProofRun.__table__],
        )
        Base.metadata.create_all(sync_engine, tables=registry_tables)
    finally:
        sync_engine.dispose()

    engine = create_async_engine(
        scratch_url.set(drivername="postgresql+asyncpg").render_as_string(
            hide_password=False
        )
    )
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with factory() as async_session:
            yield async_session
    finally:
        await engine.dispose()
        admin = sa.create_engine(
            make_url(POSTGRES_TEST_URI).set(drivername="postgresql+psycopg2"),
            isolation_level="AUTOCOMMIT",
        )
        try:
            with admin.connect() as connection:
                connection.exec_driver_sql(
                    f'DROP DATABASE IF EXISTS "{db_name}" WITH (FORCE)'
                )
        finally:
            admin.dispose()


async def _enable_all(
    session: AsyncSession, *, schema_digest: str = LIVE, mode: str = "canary"
) -> None:
    for operation, document_digest in CATALOG:
        await enable_operation(
            session,
            schema_digest=schema_digest,
            document_digest=document_digest,
            selected_operation=operation,
            candidate_build=BUILD,
            mode=mode,
        )
    await session.commit()


@pytest.mark.asyncio
async def test_enable_writes_rows_at_the_given_digest(session: AsyncSession) -> None:
    await _enable_all(session)

    rows = (await session.execute(sa.select(RoutingState))).scalars().all()
    assert {row.selected_operation for row in rows} == {op for op, _ in CATALOG}
    for row in rows:
        assert row.schema_digest == LIVE
        assert row.mode == "canary"
        assert row.owner == "go"
        assert row.rollout_percentage == 100
        assert row.current_candidate_build == BUILD

    # The 4-column FK means the candidate build must exist for each triple;
    # if enable_operation had written them in the wrong order this would
    # have raised rather than reached here.
    builds = (await session.execute(sa.select(CandidateBuild))).scalars().all()
    assert len(builds) == len(CATALOG)


@pytest.mark.asyncio
async def test_enable_is_idempotent(session: AsyncSession) -> None:
    """Re-running must be safe: recovering from a digest move should not
    require an operator to reason about whether they already ran it."""
    await _enable_all(session)
    first = (await session.execute(sa.select(RoutingState))).scalars().all()
    first_updated = {row.selected_operation: row.updated_at for row in first}

    await _enable_all(session)

    rows = (await session.execute(sa.select(RoutingState))).scalars().all()
    assert len(rows) == len(CATALOG), "re-running duplicated rows"
    builds = (await session.execute(sa.select(CandidateBuild))).scalars().all()
    assert len(builds) == len(CATALOG), "re-running duplicated candidate builds"
    # updated_at moves even on a no-change write, so "when was this last
    # asserted" stays answerable.
    for row in rows:
        assert row.updated_at >= first_updated[row.selected_operation]


@pytest.mark.asyncio
async def test_enable_repoints_an_existing_row_to_a_new_build(
    session: AsyncSession,
) -> None:
    """A rollback is a registry change, not an image rollback (plan §5)."""
    await _enable_all(session)
    newer = "f" * 40
    for operation, document_digest in CATALOG:
        await enable_operation(
            session,
            schema_digest=LIVE,
            document_digest=document_digest,
            selected_operation=operation,
            candidate_build=newer,
            mode="primary",
        )
    await session.commit()

    rows = (await session.execute(sa.select(RoutingState))).scalars().all()
    assert len(rows) == len(CATALOG)
    assert {row.current_candidate_build for row in rows} == {newer}
    assert {row.mode for row in rows} == {"primary"}
    # CandidateBuild is append-only: the old build is still on record, so a
    # rollback can point back at it.
    builds = (await session.execute(sa.select(CandidateBuild))).scalars().all()
    assert {b.candidate_build for b in builds} == {BUILD, newer}


@pytest.mark.asyncio
async def test_status_reports_match_for_live_rows(session: AsyncSession) -> None:
    await _enable_all(session)

    statuses = await routing_status_rows(
        session, live_schema_digest=LIVE, catalog=CATALOG
    )

    assert {s.operation for s in statuses} == {op for op, _ in CATALOG}
    for status in statuses:
        assert status.digest_state == "MATCH"
        assert status.reachable is True
        assert status.proven is False, "no proof run recorded yet"


@pytest.mark.asyncio
async def test_status_reports_stale_when_rows_are_at_a_superseded_digest(
    session: AsyncSession,
) -> None:
    """The 2026-09-01 shape. Rows present, all unreachable, and until now
    nothing in the system would say so."""
    await _enable_all(session, schema_digest=SUPERSEDED)

    statuses = await routing_status_rows(
        session, live_schema_digest=LIVE, catalog=CATALOG
    )

    for status in statuses:
        assert status.digest_state == "STALE"
        assert status.stale_digests == (SUPERSEDED,)
        assert status.reachable is False
        # Mode is deliberately not reported for a stale row: the row says
        # canary and the truth is "unreachable". Printing canary here is
        # exactly the misleading signal `psql` gave for six days.
        assert status.mode is None

    counts = await count_rows_by_schema_digest(session)
    assert counts == {SUPERSEDED: len(CATALOG)}


@pytest.mark.asyncio
async def test_status_reports_missing_for_operations_never_enabled(
    session: AsyncSession,
) -> None:
    """Driven by the CATALOG, not by the table.

    An operation with no row must be REPORTED as missing, not simply
    absent from the output -- "nothing printed" is how the outage stayed
    invisible.
    """
    operation, document_digest = CATALOG[0]
    await enable_operation(
        session,
        schema_digest=LIVE,
        document_digest=document_digest,
        selected_operation=operation,
        candidate_build=BUILD,
        mode="canary",
    )
    await session.commit()

    statuses = await routing_status_rows(
        session, live_schema_digest=LIVE, catalog=CATALOG
    )
    by_operation = {s.operation: s for s in statuses}

    assert len(statuses) == len(CATALOG)
    assert by_operation[operation].digest_state == "MATCH"
    for other, _ in CATALOG[1:]:
        assert by_operation[other].digest_state == "MISSING"
        assert by_operation[other].reachable is False


@pytest.mark.asyncio
async def test_status_reports_proven_only_with_a_matching_proof_run(
    session: AsyncSession,
) -> None:
    await _enable_all(session)
    operation, document_digest = CATALOG[0]

    await record_proof_run(
        session,
        schema_digest=LIVE,
        document_digest=document_digest,
        selected_operation=operation,
        candidate_build=BUILD,
        request_identity="test",
        stage=ENABLEMENT_PROOF_STAGE,
        terminal_state=ENABLEMENT_PROOF_TERMINAL_STATE,
    )
    await session.commit()

    statuses = await routing_status_rows(
        session, live_schema_digest=LIVE, catalog=CATALOG
    )
    by_operation = {s.operation: s for s in statuses}
    assert by_operation[operation].proven is True
    for other, _ in CATALOG[1:]:
        assert by_operation[other].proven is False


@pytest.mark.asyncio
async def test_proof_is_scoped_to_the_exact_build_and_digest(
    session: AsyncSession,
) -> None:
    """Plan §8.3: a proof is evidence for ONE immutable 4-column key.

    A proof recorded against a different build, a different schema digest,
    an earlier stage, or a non-``match`` outcome says nothing about the
    build being enabled now -- carrying any of them forward would make the
    gate look enforced while accepting evidence about something else.
    """
    operation, document_digest = CATALOG[0]
    other_build = "0" * 40

    for schema_digest, build, stage, terminal_state in (
        (LIVE, other_build, ENABLEMENT_PROOF_STAGE, "match"),  # wrong build
        (SUPERSEDED, BUILD, ENABLEMENT_PROOF_STAGE, "match"),  # wrong digest
        (LIVE, BUILD, "dual_run", "match"),  # earlier stage
        (LIVE, BUILD, ENABLEMENT_PROOF_STAGE, "mismatch"),  # failed proof
    ):
        # ProofRun carries a 4-column FK to CandidateBuild, so a proof can
        # only be recorded against a REGISTERED build -- the schema already
        # blocks attributing one to a build nobody registered. Register the
        # near-miss tuples so what is under test here is the QUERY's
        # scoping, not the FK's (which is proven by construction).
        await register_candidate_build(
            session,
            schema_digest=schema_digest,
            document_digest=document_digest,
            selected_operation=operation,
            candidate_build=build,
        )
        await record_proof_run(
            session,
            schema_digest=schema_digest,
            document_digest=document_digest,
            selected_operation=operation,
            candidate_build=build,
            request_identity="test",
            stage=stage,
            terminal_state=terminal_state,
        )
    await session.commit()

    assert (
        await operations_with_enablement_proof(
            session,
            schema_digest=LIVE,
            candidate_build=BUILD,
            operations=[operation],
        )
        == frozenset()
    )

    # ...and the one that genuinely qualifies is accepted.
    await record_proof_run(
        session,
        schema_digest=LIVE,
        document_digest=document_digest,
        selected_operation=operation,
        candidate_build=BUILD,
        request_identity="test",
        stage=ENABLEMENT_PROOF_STAGE,
        terminal_state=ENABLEMENT_PROOF_TERMINAL_STATE,
    )
    await session.commit()

    assert await operations_with_enablement_proof(
        session,
        schema_digest=LIVE,
        candidate_build=BUILD,
        operations=[operation],
    ) == frozenset({operation})


@pytest.mark.asyncio
async def test_count_rows_by_schema_digest_sees_every_digest(
    session: AsyncSession,
) -> None:
    await _enable_all(session, schema_digest=LIVE)
    await _enable_all(session, schema_digest=SUPERSEDED)

    assert await count_rows_by_schema_digest(session) == {
        LIVE: len(CATALOG),
        SUPERSEDED: len(CATALOG),
    }
