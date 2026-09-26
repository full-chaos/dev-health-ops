"""``enable``'s fourth preflight and its actual writes, end to end.

Preflights 1-3 refuse before any database work and are covered without one
(``test_go_api_cli_preflight.py``). Preflight 4 -- "has this exact
candidate build been proven?" -- and the writes that follow it need a real
database, for the same reason ``test_go_api_routing_admin.py`` does: the
behaviour under test is a composite-key upsert and a 4-column-FK-scoped
query.

What this file adds over the admin tests is the OPERATOR path: the
command's own refusal, the proof run that lets it proceed, and the fact that
a refusal leaves the table untouched. A gate that refuses AFTER a partial
write would be worse than no gate.

Requires ``DEV_HEALTH_POSTGRES_TEST_URI``; skipped without it, like every
other Postgres-backed test in this directory.
"""

from __future__ import annotations

import argparse
import contextlib
import json
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
    enable_operation,
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


async def _rows(factory: Any) -> list[RoutingState]:
    async with factory() as session:
        return list((await session.execute(sa.select(RoutingState))).scalars().all())


@pytest.mark.asyncio
async def test_status_reports_both_planes_and_never_fails(
    session_factory: Any, capsys: pytest.CaptureFixture[str]
) -> None:
    catalog = dict(catalog_entries())
    await _seed_receipt(
        session_factory,
        document_digest=catalog["featureFlags"],
        measurement_route="edge",
        build_binding="per_request",
    )
    async with session_factory() as session:
        await enable_operation(
            session,
            schema_digest=current_schema_digest(),
            document_digest=catalog["featureFlags"],
            selected_operation="featureFlags",
            candidate_build=BUILD,
            mode="canary",
        )
        await session.commit()
    with FakeQueryAPI(registry_payload()) as url:
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


# --- Regression fixes ------------------------------------------------------


async def _seed_receipt(factory: Any, *, document_digest: str, **columns: Any) -> None:
    """One featureFlags receipt at BUILD, with ``columns`` set verbatim.

    Direct column writes on purpose: the shapes under test (a NULL
    build_binding, a cited mismatch) are rows the table really holds, and a
    current writer would not produce every one of them.
    """
    async with factory() as session:
        await register_candidate_build(
            session,
            schema_digest=current_schema_digest(),
            document_digest=document_digest,
            selected_operation="featureFlags",
            candidate_build=BUILD,
        )
        run = await record_proof_run(
            session,
            schema_digest=current_schema_digest(),
            document_digest=document_digest,
            selected_operation="featureFlags",
            candidate_build=BUILD,
            request_identity=f"test-{uuid.uuid4().hex[:8]}",
            stage=ENABLEMENT_PROOF_STAGE,
            terminal_state=ENABLEMENT_PROOF_TERMINAL_STATE,
        )
        await session.execute(
            sa.update(ProofRun).where(ProofRun.id == run.id).values(**columns)
        )
        await session.commit()


@pytest.mark.asyncio
async def test_status_renders_a_drifted_row_with_its_proof_and_its_document(
    session_factory: Any, capsys: pytest.CaptureFixture[str]
) -> None:
    """The renderer fix was unpinned, caught by mutants p17/p18.

    A DOCUMENT_DRIFT row is live and IS the row in the table, so its PROOF
    column must say ``ok``/``UNPROVEN`` -- as ``--json`` does -- not ``-``,
    and a line must name the document actually serving. Both regressions
    passed every test: printing ``-`` (p18) and dropping the line (p17).
    """
    catalog = dict(catalog_entries())
    drifted = "0" * 64
    await _seed_receipt(
        session_factory,
        document_digest=drifted,
        measurement_route="edge",
        build_binding="per_request",
    )
    async with session_factory() as session:
        session.add(
            RoutingState(
                schema_digest=current_schema_digest(),
                document_digest=drifted,
                selected_operation="featureFlags",
                current_candidate_build=BUILD,
                owner="go",
                mode="primary",
                rollout_percentage=100,
            )
        )
        await register_candidate_build(
            session,
            schema_digest=current_schema_digest(),
            document_digest=catalog["featureFlags"],
            selected_operation="featureFlags",
            candidate_build=BUILD,
        )
        session.add(
            RoutingState(
                schema_digest=current_schema_digest(),
                document_digest=catalog["featureFlags"],
                selected_operation="featureFlags",
                current_candidate_build=BUILD,
                owner="go",
                mode="canary",
                rollout_percentage=100,
            )
        )
        await session.commit()

    with FakeQueryAPI(registry_payload()) as url:
        assert (
            await go_api_cli._cmd_routing_status(
                argparse.Namespace(query_api_url=url, json=False)
            )
            == 0
        )
    out = capsys.readouterr().out
    lines = [line for line in out.splitlines() if line.startswith("featureFlags ")]
    drift_lines = [line for line in lines if "DOCUMENT_DRIFT" in line]
    match_lines = [line for line in lines if " MATCH " in line]
    assert len(drift_lines) == 1 and len(match_lines) == 1, out
    assert drift_lines[0].split()[-1] == "ok", (
        f"the drifted row HAS an admissible receipt of its own, and its PROOF "
        f"column printed {drift_lines[0].split()[-1]!r}: the terminal disagrees "
        f"with --json about the one row the operator is asked to notice\n{out}"
    )
    assert match_lines[0].split()[-1] == "UNPROVEN", out
    assert f"serving document {drifted}" in out, (
        f"no line names the document actually serving the drifted row -- the "
        f"only question DOCUMENT_DRIFT raises\n{out}"
    )

    # The other consumers of digest_state, on the SAME rows: `--json` and the
    # `reachable` property. The drifted row is live and proven but NOT
    # reachable -- the edge dispatches through the catalog, so no request can
    # land on it -- and the catalog's canary row is reachable and unproven.
    with FakeQueryAPI(registry_payload()) as url:
        assert (
            await go_api_cli._cmd_routing_status(
                argparse.Namespace(query_api_url=url, json=True)
            )
            == 0
        )
    payload = json.loads(capsys.readouterr().out)
    rows = {
        (row["digest_state"], row["document_digest"]): row
        for row in payload["operations"]
        if row["operation"] == "featureFlags"
    }
    drift_json = rows[("DOCUMENT_DRIFT", drifted)]
    match_json = rows[("MATCH", catalog["featureFlags"])]
    assert (drift_json["mode"], drift_json["proven"], drift_json["reachable"]) == (
        "primary",
        True,
        False,
    ), f"--json disagrees with the terminal about the drifted row: {drift_json}"
    assert (match_json["mode"], match_json["proven"], match_json["reachable"]) == (
        "canary",
        False,
        True,
    ), f"--json misreports the catalog's row: {match_json}"


@pytest.mark.asyncio
async def test_status_names_an_unregistered_live_row_on_both_outputs(
    session_factory: Any, capsys: pytest.CaptureFixture[str]
) -> None:
    """A live row for an operation the catalog does not
    register was named nowhere by `status`, text or `--json`, while the
    migration page named it. Both outputs now carry it, unreachable, with
    the proof `status` computes for it (an admissible receipt of its own
    here, so PROOF reads ``ok`` and ``proven`` is true on both)."""
    retired = "e" * 64
    async with session_factory() as session:
        await register_candidate_build(
            session,
            schema_digest=current_schema_digest(),
            document_digest=retired,
            selected_operation="retiredOperation",
            candidate_build=BUILD,
        )
        run = await record_proof_run(
            session,
            schema_digest=current_schema_digest(),
            document_digest=retired,
            selected_operation="retiredOperation",
            candidate_build=BUILD,
            request_identity=f"test-{uuid.uuid4().hex[:8]}",
            stage=ENABLEMENT_PROOF_STAGE,
            terminal_state=ENABLEMENT_PROOF_TERMINAL_STATE,
        )
        await session.execute(
            sa.update(ProofRun)
            .where(ProofRun.id == run.id)
            .values(measurement_route="edge", build_binding="per_request")
        )
        session.add(
            RoutingState(
                schema_digest=current_schema_digest(),
                document_digest=retired,
                selected_operation="retiredOperation",
                current_candidate_build=BUILD,
                owner="go",
                mode="primary",
                rollout_percentage=100,
            )
        )
        await session.commit()
    with FakeQueryAPI(registry_payload()) as url:
        assert (
            await go_api_cli._cmd_routing_status(
                argparse.Namespace(query_api_url=url, json=False)
            )
            == 0
        )
    out = capsys.readouterr().out
    line = [line for line in out.splitlines() if line.startswith("retiredOperation ")]
    assert len(line) == 1 and "UNREGISTERED" in line[0], out
    assert line[0].split()[-1] == "ok", (
        f"the unregistered row HAS an admissible receipt of its own, and its "
        f"PROOF column printed {line[0].split()[-1]!r}: the terminal disagrees "
        f"with --json\n{out}"
    )
    assert f"serving document {retired}" in out, out
    with FakeQueryAPI(registry_payload()) as url:
        assert (
            await go_api_cli._cmd_routing_status(
                argparse.Namespace(query_api_url=url, json=True)
            )
            == 0
        )
    rows = [
        r
        for r in json.loads(capsys.readouterr().out)["operations"]
        if r["operation"] == "retiredOperation"
    ]
    assert [
        (r["digest_state"], r["mode"], r["proven"], r["reachable"]) for r in rows
    ] == [("UNREGISTERED", "primary", True, False)], rows


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("digest", "expected"),
    # (a NULL digest cannot exist: the shape CHECK refuses the row)
    [("sha256:6810write", True), ("   ", False), ("\u00a0", False)],
    ids=["named_digest", "blank_digest", "nbsp_digest"],
)
async def test_status_reads_an_unregistered_row_by_its_write_receipt_too(
    session_factory: Any,
    capsys: pytest.CaptureFixture[str],
    digest: str,
    expected: bool,
) -> None:
    """CHAOS-6810: `enable` admits nothing for an operation of unknown kind, but
    this page is display-only: a live row for a retired operation reports what
    receipts it HAS, judged by each receipt's own form. A well-formed
    write_executed receipt reads proven; one whose digest names nothing does not."""
    retired = "f" * 64
    async with session_factory() as session:
        await register_candidate_build(
            session,
            schema_digest=current_schema_digest(),
            document_digest=retired,
            selected_operation="retiredMutation",
            candidate_build=BUILD,
        )
        await session.execute(
            sa.insert(ProofRun).values(
                id=uuid.uuid4(),
                schema_digest=current_schema_digest(),
                document_digest=retired,
                selected_operation="retiredMutation",
                candidate_build=BUILD,
                request_identity=f"test-{uuid.uuid4().hex[:8]}",
                stage="write_executed",
                terminal_state="match",
                measurement_route="edge",
                build_binding="per_request",
                side_effect_digest=digest,
            )
        )
        session.add(
            RoutingState(
                schema_digest=current_schema_digest(),
                document_digest=retired,
                selected_operation="retiredMutation",
                current_candidate_build=BUILD,
                owner="go",
                mode="primary",
                rollout_percentage=100,
            )
        )
        await session.commit()
    with FakeQueryAPI(registry_payload()) as url:
        assert (
            await go_api_cli._cmd_routing_status(
                argparse.Namespace(query_api_url=url, json=True)
            )
            == 0
        )
    rows = [
        r
        for r in json.loads(capsys.readouterr().out)["operations"]
        if r["operation"] == "retiredMutation"
    ]
    assert [(r["digest_state"], r["proven"]) for r in rows] == [
        ("UNREGISTERED", expected)
    ], rows


async def _reset(factory: Any) -> None:
    async with factory() as session:
        await session.execute(sa.delete(RoutingState))
        await session.execute(sa.delete(ProofRun))
        await session.commit()


@pytest.mark.asyncio
async def test_the_model_refuses_a_binding_outside_its_vocabulary(
    session_factory: Any,
) -> None:
    """The ORM CheckConstraint on build_binding was pinned by
    nothing -- the migration test covers alembic, but tables created from this
    metadata (SQLAlchemyStore.ensure_tables; this fixture) carry the model's
    own copy. The same vocabulary as 0129: per_request, absent, NULL."""
    catalog = dict(catalog_entries())
    for binding in ("per_request", "absent", None):
        await _reset(session_factory)
        await _seed_receipt(
            session_factory,
            document_digest=catalog["featureFlags"],
            build_binding=binding,
        )
    await _reset(session_factory)
    with pytest.raises(
        sa.exc.IntegrityError, match="ck_go_api_proof_run_build_binding"
    ):
        await _seed_receipt(
            session_factory,
            document_digest=catalog["featureFlags"],
            build_binding="run_level",
        )
