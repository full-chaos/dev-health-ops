"""``enable``'s fourth preflight and its actual writes, end to end.

Preflights 1-3 refuse before any database work and are covered without one
(``test_go_api_cli_preflight.py``). Preflight 4 -- "has this exact
candidate build been proven?" -- and the writes that follow it need a real
database, for the same reason ``test_go_api_routing_admin.py`` does: the
behaviour under test is a composite-key upsert and a 4-column-FK-scoped
query.

What this file adds over the admin tests is the OPERATOR path: the
command's own refusal, its acknowledgement escape hatch, and the fact that
a refusal leaves the table untouched. A gate that refuses AFTER a partial
write would be worse than no gate.

Requires ``DEV_HEALTH_POSTGRES_TEST_URI``; skipped without it, like every
other Postgres-backed test in this directory.
"""

from __future__ import annotations

import argparse
import contextlib
import os
import uuid
from collections.abc import AsyncIterator
from typing import Any, cast

import pytest
import pytest_asyncio
import sqlalchemy as sa
from _go_api_fake_query_api import FakeQueryAPI, registry_payload
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from dev_health_ops.api.graphql import go_api_cli
from dev_health_ops.api.graphql.go_api_operation_catalog import catalog_entries
from dev_health_ops.api.graphql.go_api_registry import (
    record_proof_run,
    register_candidate_build,
)
from dev_health_ops.api.graphql.go_api_routing_admin import (
    ENABLEMENT_PROOF_STAGE,
    ENABLEMENT_PROOF_TERMINAL_STATE,
)
from dev_health_ops.api.graphql.go_api_schema_digest import current_schema_digest
from dev_health_ops.models.git import Base
from dev_health_ops.models.go_api_registry import (
    CandidateBuild,
    ProofRun,
    RoutingState,
)

POSTGRES_TEST_URI = os.environ.get("DEV_HEALTH_POSTGRES_TEST_URI")

pytestmark = pytest.mark.skipif(
    not POSTGRES_TEST_URI,
    reason="Requires DEV_HEALTH_POSTGRES_TEST_URI (admin creds able to CREATE DATABASE)",
)

BUILD = "78fc68815e8209834c6e5acc37eb7653b52a7aa4"


@pytest_asyncio.fixture
async def session_factory(monkeypatch: pytest.MonkeyPatch) -> AsyncIterator[Any]:
    """A scratch DB, with ``get_postgres_session`` pointed at it.

    The CLI opens its own session (``from dev_health_ops.db import
    get_postgres_session``, resolved at call time), so redirecting that
    factory is what lets the command run unmodified against the scratch
    database -- the code path under test is the real one, not a
    test-only branch.
    """
    assert POSTGRES_TEST_URI is not None
    db_name = f"lane_go_api_cli_{uuid.uuid4().hex}"

    def _admin() -> sa.Engine:
        return sa.create_engine(
            make_url(POSTGRES_TEST_URI).set(drivername="postgresql+psycopg2"),
            isolation_level="AUTOCOMMIT",
        )

    admin = _admin()
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

    @contextlib.asynccontextmanager
    async def fake_get_postgres_session() -> AsyncIterator[Any]:
        async with factory() as session:
            yield session

    import dev_health_ops.db as db_module

    monkeypatch.setattr(db_module, "get_postgres_session", fake_get_postgres_session)

    try:
        yield factory
    finally:
        await engine.dispose()
        admin = _admin()
        try:
            with admin.connect() as connection:
                connection.exec_driver_sql(
                    f'DROP DATABASE IF EXISTS "{db_name}" WITH (FORCE)'
                )
        finally:
            admin.dispose()


def _ns(**overrides: Any) -> argparse.Namespace:
    ns = argparse.Namespace(
        operations="featureFlags",
        candidate_build=BUILD,
        mode="canary",
        rollout=100,
        query_api_url=None,
        acknowledge_unproven=False,
    )
    for key, value in overrides.items():
        setattr(ns, key, value)
    return ns


async def _rows(factory: Any) -> list[RoutingState]:
    async with factory() as session:
        return list((await session.execute(sa.select(RoutingState))).scalars().all())


@pytest.mark.asyncio
async def test_preflight_4_refuses_an_unproven_build_and_writes_nothing(
    session_factory: Any, capsys: pytest.CaptureFixture[str]
) -> None:
    with FakeQueryAPI(registry_payload()) as url:
        assert await go_api_cli._cmd_routing_enable(_ns(query_api_url=url)) == 2

    err = capsys.readouterr().err
    assert ENABLEMENT_PROOF_STAGE in err
    assert "featureFlags" in err
    assert "--acknowledge-unproven" in err
    assert await _rows(session_factory) == [], (
        "a refused enable wrote rows anyway -- a gate that refuses after a "
        "partial write is worse than no gate"
    )


@pytest.mark.asyncio
async def test_acknowledge_unproven_enables_and_warns_per_row(
    session_factory: Any, capsys: pytest.CaptureFixture[str]
) -> None:
    with FakeQueryAPI(registry_payload()) as url:
        assert (
            await go_api_cli._cmd_routing_enable(
                _ns(
                    operations="featureFlags,reviewEdges",
                    query_api_url=url,
                    acknowledge_unproven=True,
                )
            )
            == 0
        )

    captured = capsys.readouterr()
    # One line PER ROW: "some were unproven" is not actionable six weeks
    # later; "these two were" is.
    assert captured.err.count("go_api_routing.enabled_unproven") == 2
    assert "operation=featureFlags" in captured.err
    assert "operation=reviewEdges" in captured.err
    assert "(UNPROVEN)" in captured.out

    rows = await _rows(session_factory)
    catalog = dict(catalog_entries())
    assert {row.selected_operation for row in rows} == {"featureFlags", "reviewEdges"}
    for row in rows:
        # Written at the LIVE digest, computed at runtime -- never a
        # hand-typed value. This is the property whose absence caused the
        # 2026-09-01 outage.
        assert row.schema_digest == current_schema_digest()
        assert row.document_digest == catalog[row.selected_operation]
        assert row.mode == "canary"
        assert row.owner == "go"


@pytest.mark.asyncio
async def test_a_recorded_proof_run_lets_enable_proceed_without_acknowledgement(
    session_factory: Any,
) -> None:
    """The gate is passable by doing the right thing, not only by waiving it."""
    catalog = dict(catalog_entries())
    async with session_factory() as session:
        await register_candidate_build(
            session,
            schema_digest=current_schema_digest(),
            document_digest=catalog["featureFlags"],
            selected_operation="featureFlags",
            candidate_build=BUILD,
        )
        await record_proof_run(
            session,
            schema_digest=current_schema_digest(),
            document_digest=catalog["featureFlags"],
            selected_operation="featureFlags",
            candidate_build=BUILD,
            request_identity="test",
            stage=ENABLEMENT_PROOF_STAGE,
            terminal_state=ENABLEMENT_PROOF_TERMINAL_STATE,
        )
        # CHAOS-5484: admissibility now requires a RECORDED measurement
        # route. record_proof_run does not write one -- its only callers
        # are tests, the real writer is cmd/go-api-prove -- so stamp it
        # here, or this receipt would be refused for a reason that has
        # nothing to do with what the test asserts. `edge` because the
        # enable below targets canary, which is served through the
        # product edge.
        await session.execute(
            sa.update(ProofRun).values(measurement_route="edge", build_binding="absent")
        )
        await session.commit()

    with FakeQueryAPI(registry_payload()) as url:
        assert await go_api_cli._cmd_routing_enable(_ns(query_api_url=url)) == 0

    rows = await _rows(session_factory)
    assert [row.selected_operation for row in rows] == ["featureFlags"]


@pytest.mark.asyncio
async def test_enable_is_idempotent_through_the_cli(session_factory: Any) -> None:
    """Recovering from a digest move must be safely repeatable."""
    with FakeQueryAPI(registry_payload()) as url:
        for _ in range(3):
            assert (
                await go_api_cli._cmd_routing_enable(
                    _ns(query_api_url=url, acknowledge_unproven=True)
                )
                == 0
            )

    assert len(await _rows(session_factory)) == 1


@pytest.mark.asyncio
async def test_status_reports_both_planes_and_never_fails(
    session_factory: Any, capsys: pytest.CaptureFixture[str]
) -> None:
    with FakeQueryAPI(registry_payload()) as url:
        assert (
            await go_api_cli._cmd_routing_enable(
                _ns(query_api_url=url, acknowledge_unproven=True)
            )
            == 0
        )
        capsys.readouterr()
        assert (
            await go_api_cli._cmd_routing_status(
                argparse.Namespace(query_api_url=url, json=False)
            )
            == 0
        )

    out = capsys.readouterr().out
    assert current_schema_digest() in out
    assert "[AGREE]" in out
    assert "featureFlags" in out
    assert "MATCH" in out
    assert "UNPROVEN" in out
    # Every catalog operation is listed, including the ones with no row --
    # "nothing printed" is how the outage stayed invisible.
    for operation, _ in catalog_entries():
        assert operation in out
    assert "MISSING" in out


@pytest.mark.asyncio
async def test_status_reports_unreachable_rather_than_failing(
    session_factory: Any, capsys: pytest.CaptureFixture[str]
) -> None:
    """``status`` is what an operator runs WHEN things are broken.

    A diagnostic that refuses to run because the thing it diagnoses is
    down is useless exactly when it is needed.
    """
    assert (
        await go_api_cli._cmd_routing_status(
            argparse.Namespace(query_api_url="http://127.0.0.1:1", json=False)
        )
        == 0
    )
    out = capsys.readouterr().out
    assert "UNREACHABLE" in out
    assert current_schema_digest() in out


@pytest.mark.asyncio
async def test_status_json_reports_plane_disagreement(
    session_factory: Any, capsys: pytest.CaptureFixture[str]
) -> None:
    import json as json_module

    stale = "sha256:67b87d38e46f767511b5d8435ffbfdd7dbe8aeab9dbe4073c7d7706de572f706"
    with FakeQueryAPI(registry_payload(schema_digest=stale)) as url:
        assert (
            await go_api_cli._cmd_routing_status(
                argparse.Namespace(query_api_url=url, json=True)
            )
            == 0
        )

    payload = json_module.loads(capsys.readouterr().out)
    assert payload["planes_agree"] is False
    assert payload["go_plane_schema_digest"] == stale
    assert payload["python_plane_schema_digest"] == current_schema_digest()


# --- codex r1 fixes -------------------------------------------------------


@pytest.mark.asyncio
async def test_a_proof_for_a_different_document_does_not_authorize_enablement(
    session_factory: Any, capsys: pytest.CaptureFixture[str]
) -> None:
    """codex r1 P2: `document_digest` was missing from the proof lookup.

    The proof key is FOUR columns (plan section 8.3 -- "a proof is evidence
    for exactly one tuple, never carried forward across any of the four
    changing"). With only three matched, a `deployed_executed`/`match`
    proof recorded against a DIFFERENT registered document authorized the
    enablement.
    """
    catalog = dict(catalog_entries())
    wrong_document = "0" * 64
    assert wrong_document != catalog["featureFlags"]

    async with session_factory() as session:
        await register_candidate_build(
            session,
            schema_digest=current_schema_digest(),
            document_digest=wrong_document,
            selected_operation="featureFlags",
            candidate_build=BUILD,
        )
        await record_proof_run(
            session,
            schema_digest=current_schema_digest(),
            document_digest=wrong_document,
            selected_operation="featureFlags",
            candidate_build=BUILD,
            request_identity="test",
            stage=ENABLEMENT_PROOF_STAGE,
            terminal_state=ENABLEMENT_PROOF_TERMINAL_STATE,
        )
        await session.commit()

    with FakeQueryAPI(registry_payload()) as url:
        assert await go_api_cli._cmd_routing_enable(_ns(query_api_url=url)) == 2

    assert ENABLEMENT_PROOF_STAGE in capsys.readouterr().err
    assert await _rows(session_factory) == [], "a wrong-document proof enabled a row"


@pytest.mark.asyncio
async def test_the_cli_passes_its_own_mode_to_the_proof_predicate(
    session_factory: Any,
) -> None:
    """r1 P3: hardcoding ``"canary"`` at the call site passed all 8 tests.

    That mutation is the mode split defeated in one word. The only proof
    on record here is a PROOF-route receipt, which ``canary`` admits and
    ``primary`` must not: promotion to primary is promotion to real
    traffic, so it demands evidence that traversed the real edge.

    With the caller hardcoding canary, ``--mode primary`` would preflight
    against the laxer rule and enable a row into served traffic on
    measurement-only evidence -- silently, with a zero exit and no
    warning. Every existing test used ``mode="canary"``, so the argument
    was never observed to travel.
    """
    catalog = dict(catalog_entries())
    async with session_factory() as session:
        await register_candidate_build(
            session,
            schema_digest=current_schema_digest(),
            document_digest=catalog["featureFlags"],
            selected_operation="featureFlags",
            candidate_build=BUILD,
        )
        await record_proof_run(
            session,
            schema_digest=current_schema_digest(),
            document_digest=catalog["featureFlags"],
            selected_operation="featureFlags",
            candidate_build=BUILD,
            request_identity="test",
            stage=ENABLEMENT_PROOF_STAGE,
            terminal_state=ENABLEMENT_PROOF_TERMINAL_STATE,
        )
        # PROOF route: admissible for canary, inadmissible for primary.
        await session.execute(
            sa.update(ProofRun).values(
                measurement_route="proof", build_binding="per_request"
            )
        )
        await session.commit()

    with FakeQueryAPI(registry_payload()) as url:
        # canary accepts it...
        assert (
            await go_api_cli._cmd_routing_enable(_ns(query_api_url=url, mode="canary"))
            == 0
        )

    rows = await _rows(session_factory)
    assert [row.mode for row in rows] == ["canary"]

    with FakeQueryAPI(registry_payload()) as url:
        # ...and primary REFUSES it, on the same row, differing only in
        # the mode the CLI was asked for.
        assert (
            await go_api_cli._cmd_routing_enable(_ns(query_api_url=url, mode="primary"))
            != 0
        ), (
            "the CLI enabled a PRIMARY row on proof-route evidence: the "
            "target mode is not reaching the preflight predicate, so the "
            "canary/primary split authorizes nothing"
        )

    # And the refusal wrote nothing: the row is still canary.
    rows = await _rows(session_factory)
    assert [row.mode for row in rows] == ["canary"]
