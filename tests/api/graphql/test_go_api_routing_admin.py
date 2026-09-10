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

import pytest
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession

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
    plan_disable,
    routing_status_rows,
)
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


async def _record_measurement_provenance(
    session: AsyncSession, *, route: str = "edge", binding: str = "absent"
) -> None:
    """Stamp measurement provenance on every proof run seeded so far.

    CHAOS-5484 made a RECORDED measurement route part of admissibility --
    "any route" is not "no route", because a NULL is a pre-0128 row that
    says nothing about how it was measured. ``record_proof_run`` does not
    write that column (its only callers are tests; the real writer is
    ``cmd/go-api-prove``), so these tests stamp it explicitly. Without
    this the rows below would be refused for a reason unrelated to what
    each test is actually about.
    """
    await session.execute(
        sa.update(ProofRun).values(measurement_route=route, build_binding=binding)
    )


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
    await _record_measurement_provenance(session)
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

    other_document = "f" * 64
    for schema_digest, build, doc, stage, terminal_state in (
        (LIVE, other_build, document_digest, ENABLEMENT_PROOF_STAGE, "match"),
        (SUPERSEDED, BUILD, document_digest, ENABLEMENT_PROOF_STAGE, "match"),
        # codex r1 (P2): the 4th column. A proof against a DIFFERENT
        # registered document must not authorize this one.
        (LIVE, BUILD, other_document, ENABLEMENT_PROOF_STAGE, "match"),
        (LIVE, BUILD, document_digest, "dual_run", "match"),
        (LIVE, BUILD, document_digest, ENABLEMENT_PROOF_STAGE, "mismatch"),
    ):
        # ProofRun carries a 4-column FK to CandidateBuild, so a proof can
        # only be recorded against a REGISTERED build -- the schema already
        # blocks attributing one to a build nobody registered. Register the
        # near-miss tuples so what is under test here is the QUERY's
        # scoping, not the FK's (which is proven by construction).
        await register_candidate_build(
            session,
            schema_digest=schema_digest,
            document_digest=doc,
            selected_operation=operation,
            candidate_build=build,
        )
        await record_proof_run(
            session,
            schema_digest=schema_digest,
            document_digest=doc,
            selected_operation=operation,
            candidate_build=build,
            request_identity="test",
            stage=stage,
            terminal_state=terminal_state,
        )
    # Every row above is a near-miss on some OTHER key column; leaving the
    # route NULL would let them be refused for a reason unrelated to the
    # scoping under test.
    await _record_measurement_provenance(session)
    await session.commit()

    assert (
        await operations_with_enablement_proof(
            session,
            schema_digest=LIVE,
            candidate_build=BUILD,
            operations={operation: document_digest},
            target_mode="canary",
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
    await _record_measurement_provenance(session)
    await session.commit()

    assert await operations_with_enablement_proof(
        session,
        schema_digest=LIVE,
        candidate_build=BUILD,
        operations={operation: document_digest},
        target_mode="canary",
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


@pytest.mark.asyncio
async def test_status_holds_a_primary_row_to_the_edge_rule(
    session: AsyncSession,
) -> None:
    """r1 P3: disabling the primary branch of the grouping passed all 9 tests.

    ``routing_status_rows`` groups by (candidate build, the mode's own
    route rule) precisely so a ``primary`` row is judged against the
    STRICTER rule. Collapsing that grouping to the laxer one reports a
    primary row as PROVEN on proof-route evidence -- measurement-only
    evidence certifying a row that is serving real traffic, which is the
    exact claim the canary/primary split exists to stop.

    It survived because every status test enabled rows at ``canary``,
    where both rules agree, so the branch was never taken.
    """
    await _enable_all(session, mode="primary")
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
    # PROOF route: admissible for canary, inadmissible for primary.
    await _record_measurement_provenance(session, route="proof", binding="per_request")
    await session.commit()

    statuses = await routing_status_rows(
        session, live_schema_digest=LIVE, catalog=CATALOG
    )
    by_operation = {s.operation: s for s in statuses}
    assert by_operation[operation].proven is False, (
        "a PRIMARY row was reported proven on proof-route evidence: the "
        "status grouping is judging it by the canary rule, so an operator "
        "reading `status` sees real traffic certified by a measurement "
        "that never traversed the edge"
    )


@pytest.mark.asyncio
async def test_status_proves_a_primary_row_on_edge_evidence(
    session: AsyncSession,
) -> None:
    """The control for the test above: the strict rule is satisfiable.

    Without this, the assertion above would also pass if `primary` rows
    could never be proven at all -- which would be a different defect
    reading as a fix.
    """
    await _enable_all(session, mode="primary")
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
    await _record_measurement_provenance(session, route="edge", binding="absent")
    await session.commit()

    statuses = await routing_status_rows(
        session, live_schema_digest=LIVE, catalog=CATALOG
    )
    by_operation = {s.operation: s for s in statuses}
    assert by_operation[operation].proven is True, (
        "a PRIMARY row with EDGE evidence was not proven: the strict rule "
        "is refusing what it is supposed to admit"
    )


@pytest.mark.asyncio
async def test_status_reads_the_catalogs_document_not_whichever_row_is_last(
    session: AsyncSession,
) -> None:
    """r2 P1 / Trap #120: keying by operation alone collapses documents.

    ``go_api_routing_state`` is keyed by ``(schema_digest,
    document_digest, selected_operation)``. A map keyed on the operation
    ALONE keeps whichever row the scan returned last, which is not
    deterministic -- so the same data can report differently between
    runs, and the reviewer reproduced exactly that live: the wrong row's
    mode.

    Here the catalog names ``doc-new``, which is ``canary``, while a row
    for the same operation at ``doc-old`` is ``python``. Status must
    report the CATALOG's row, whichever order the scan hands them over.
    """
    operation, catalog_document = CATALOG[0]
    old_document = "0" * 64

    await enable_operation(
        session,
        schema_digest=LIVE,
        document_digest=old_document,
        selected_operation=operation,
        candidate_build=BUILD,
        mode="python",
    )
    await enable_operation(
        session,
        schema_digest=LIVE,
        document_digest=catalog_document,
        selected_operation=operation,
        candidate_build=BUILD,
        mode="canary",
    )
    await session.commit()

    statuses = await routing_status_rows(
        session, live_schema_digest=LIVE, catalog=CATALOG
    )
    # Keyed by (operation, state): this list now holds more than one row
    # per operation by design, and a map keyed on the operation alone
    # would repeat the very collapsing under test.
    by_state = {(s.operation, s.digest_state): s for s in statuses}

    assert by_state[(operation, "MATCH")].document_digest == catalog_document
    assert by_state[(operation, "MATCH")].mode == "canary", (
        "status reported the mode of a row the catalog does not name: the "
        "live map is keyed by operation alone, so two documents under one "
        "schema collapse onto whichever row the scan returned last"
    )
    # And the other document is its OWN row, named rather than dropped:
    # count_rows_by_schema_digest groups by SCHEMA digest alone, so nothing
    # else in the system would have named it either.
    drift = by_state[(operation, "DOCUMENT_DRIFT")]
    assert drift.document_digest == old_document
    assert drift.mode == "python", (
        "the drifted row must carry ITS OWN mode -- an operator seeing this "
        "state needs to know what that row is set to"
    )


@pytest.mark.asyncio
async def test_status_reports_a_drifted_document_by_name_not_as_match(
    session: AsyncSession,
) -> None:
    """The catalog's document has NO row; only a drifted one exists.

    Before the fix this reported ``MATCH`` with the drifted row's mode --
    an operator reads it as serving when the edge cannot dispatch it at
    all, because the edge resolves a request to an operation through the
    catalog. Now the catalog's document reports ``MISSING`` (there is no
    row for it) and the row that DOES exist gets its own
    ``DOCUMENT_DRIFT`` line carrying its digest and mode. Neither half is
    silent, which is the whole point.
    """
    operation, catalog_document = CATALOG[0]
    old_document = "0" * 64

    await enable_operation(
        session,
        schema_digest=LIVE,
        document_digest=old_document,
        selected_operation=operation,
        candidate_build=BUILD,
        mode="canary",
    )
    await session.commit()

    statuses = await routing_status_rows(
        session, live_schema_digest=LIVE, catalog=CATALOG
    )
    catalog_row = next(
        s
        for s in statuses
        if s.operation == operation and s.document_digest == catalog_document
    )
    assert catalog_row.digest_state == "MISSING", (
        f"the CATALOG's document has no row, so its state is MISSING, got "
        f"{catalog_row.digest_state!r} -- reporting MATCH here is the r2 "
        "defect: an operator reads it as serving when the edge cannot "
        "dispatch it at all"
    )
    assert catalog_row.mode is None

    drift = [s for s in statuses if s.digest_state == "DOCUMENT_DRIFT"]
    assert len(drift) == 1, f"expected one DOCUMENT_DRIFT row, got {drift}"
    assert drift[0].document_digest == old_document
    assert drift[0].mode == "canary", (
        "the drifted row is the one actually in the table: its mode is what "
        "an operator needs to see, and MISSING alone would have shown "
        "nothing at all"
    )


@pytest.mark.asyncio
async def test_disable_plans_against_the_catalogs_document(
    session: AsyncSession,
) -> None:
    """The same Trap #120 defect in ``plan_disable``.

    A rollback plan naming the wrong row's ``current_mode`` is a plan an
    operator cannot check -- and rollback is the verb you reach for when
    something is already wrong.
    """
    operation, catalog_document = CATALOG[0]
    old_document = "0" * 64

    await enable_operation(
        session,
        schema_digest=LIVE,
        document_digest=old_document,
        selected_operation=operation,
        candidate_build=BUILD,
        mode="python",
    )
    await enable_operation(
        session,
        schema_digest=LIVE,
        document_digest=catalog_document,
        selected_operation=operation,
        candidate_build=BUILD,
        mode="canary",
    )
    await session.commit()

    changes, problems = await plan_disable(
        session,
        schema_digest=LIVE,
        operations={operation: catalog_document},
        new_mode="disabled",
    )
    assert problems == []
    assert len(changes) == 1
    change = changes[0]
    assert change.document_digest == catalog_document
    assert change.current_mode == "canary", (
        f"the rollback plan reported current_mode={change.current_mode!r} "
        "-- the mode of a row the caller did not name"
    )


@pytest.mark.asyncio
async def test_proof_never_transfers_between_documents_of_one_operation(
    session: AsyncSession,
) -> None:
    """astra r3 P1: status reported a catalog document PROVEN on a drifted
    row's proof, across a different build AND a different target mode.

    Two rows for one operation: the catalog's document at ``primary`` on
    one build, a drifted document at ``canary`` on another. Only the
    DRIFTED one has a proof run. The proof key is (schema_digest,
    document_digest, selected_operation, candidate_build) plus the target
    mode -- so nothing about that receipt says anything about the catalog's
    row, and status must not claim otherwise.

    It did, because the grouping keyed its inner dict by operation and the
    reader returns operation NAMES: the name came back proven and every
    document wearing that name inherited it.
    """
    operation, catalog_document = CATALOG[0]
    drifted_document = "0" * 64
    other_build = "1" * 40

    await enable_operation(
        session,
        schema_digest=LIVE,
        document_digest=catalog_document,
        selected_operation=operation,
        candidate_build=BUILD,
        mode="primary",
    )
    await enable_operation(
        session,
        schema_digest=LIVE,
        document_digest=drifted_document,
        selected_operation=operation,
        candidate_build=other_build,
        mode="canary",
    )
    # The proof belongs to the DRIFTED document, on the OTHER build.
    await register_candidate_build(
        session,
        schema_digest=LIVE,
        document_digest=drifted_document,
        selected_operation=operation,
        candidate_build=other_build,
    )
    await record_proof_run(
        session,
        schema_digest=LIVE,
        document_digest=drifted_document,
        selected_operation=operation,
        candidate_build=other_build,
        request_identity="test",
        stage=ENABLEMENT_PROOF_STAGE,
        terminal_state=ENABLEMENT_PROOF_TERMINAL_STATE,
    )
    await _record_measurement_provenance(session)
    await session.commit()

    statuses = await routing_status_rows(
        session, live_schema_digest=LIVE, catalog=CATALOG
    )
    by_state = {(s.operation, s.digest_state): s for s in statuses}

    catalog_row = by_state[(operation, "MATCH")]
    assert catalog_row.document_digest == catalog_document
    assert catalog_row.proven is False, (
        "the CATALOG's document was reported proven on a receipt recorded "
        "against a DIFFERENT document, a DIFFERENT build and a DIFFERENT "
        "target mode -- a promotion to primary certified by evidence that "
        "says nothing about it"
    )

    drift_row = by_state[(operation, "DOCUMENT_DRIFT")]
    assert drift_row.document_digest == drifted_document
    assert drift_row.proven is True, (
        "the drifted row owns that proof and must still report it -- "
        "otherwise this test would also pass with proof reporting broken "
        "for everything, which is a different defect"
    )
