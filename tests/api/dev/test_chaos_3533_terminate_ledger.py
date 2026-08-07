"""RED-first coverage for CHAOS-3533: a preflight TERMINATE persists at most
ONE resolution ledger row, so a run that terminated without an answer cannot
evidence what it resolved.

The mechanism, and why it is the mirror image of CHAOS-3424's:

``orchestrator.run()`` writes the WHOLE ledger the preflight built -- but only
under ``if preflight_result.decision is PreflightDecision.PROCEED``. The
TERMINATE branch writes exactly one row, ``terminating_resolution_entry``,
which ``subject_preflight._terminate``'s caller sets only when the terminating
mention's outcome is ``AMBIGUOUS_CANDIDATES`` (the only outcome that carries
clarification candidates the frame has to be authorized against). Every other
terminate -- ``no_authorized_match``, ``catalog_unavailable``,
``unsupported_kind``, and the committed-cohort D1 terminate -- persists
nothing at all.

CHAOS-3424 made exactly this argument for the PROCEED half and won it: "that
is exactly the run shape a widening/wrong-subject incident needs to be
auditable from data instead of a container log line". The TERMINATE half was
left behind, and it is the half where the run declined to answer.

Downstream, that absence is what makes eight corpus cases unsatisfiable BY
CONSTRUCTION rather than wrong: ``scripts/acceptance/corpus/resolution_path.
derive_resolution_path`` returns ``None`` for an empty ledger by contract, and
``invariants._resolution_path_in`` deliberately never matches ``None``. All
eight declare ``resolution_path: "miss-clarification"``, which is precisely
what ``derive_resolution_path`` returns once the row exists (a mention whose
LAST entry is not ``exact_match``).

Every test here drives the REAL ``DevOrchestrator.run()`` -> real
``PersistenceRunRecorder`` -> real ``DevPersistenceService`` -> a real (if
ephemeral) sqlite database, reusing the CHAOS-3423/3424 suite's own ``seeded``
fixture and ``_seed_run`` helper rather than a second, divergent definition of
what "a seeded run" means. The module's fake ``Recorder`` would prove nothing
here: the constraint this change has to survive
(``_authorize_clarification_candidates``) lives in the persistence service,
not in the orchestrator.
"""

from __future__ import annotations

import logging
import uuid
from typing import Any

import pytest
from sqlalchemy import select

from dev_health_ops.api.dev.orchestrator_persistence import PersistenceRunRecorder
from dev_health_ops.api.dev.orchestrator_states import RunState
from dev_health_ops.api.dev.persistence import DevPersistenceService
from dev_health_ops.api.dev.scope_service import AuthorizedEntity, EntityKind
from dev_health_ops.models.dev_persistence import (
    DevAnswerFrame,
    DevRun,
    DevRunResolution,
)
from scripts.acceptance.corpus._inner_ledger_query import _fetch_entries
from scripts.acceptance.corpus.resolution_path import (
    ResolutionLedgerEntry,
    attach_mention_texts,
    derive_resolution_path,
)
from tests._chaos_3292_preflight import (
    ASK_DEV_PROJECT,
    ATLAS_PROJECT_ONE,
    ATLAS_PROJECT_TWO,
    Recorder,
    run_preflight_orchestrator,
)

# The CHAOS-3423/3424 suite owns the one definition of a seeded Ask Dev
# database and of "an accepted run row that already exists, exactly like
# production". Imported rather than re-declared so a schema change to that
# fixture cannot leave this suite quietly testing a different world.
from tests.api.dev.test_chaos_3423_3424_persistence_prerequisites import (  # noqa: F401
    _seed_run,
    seeded,
)

#: The CHAOS-3534 corpus case's own two repositories, verbatim from
#: ``tests/acceptance/world/ask-dev-world.v1/corpus/
#: case-scope.bounded-subject-set.json`` -- both real, both individually
#: resolvable, named together in one question.
WEB_APP = AuthorizedEntity(
    EntityKind.REPOSITORY, "meridian/web-app", "meridian/web-app"
)
API_GATEWAY = AuthorizedEntity(
    EntityKind.REPOSITORY, "meridian/api-gateway", "meridian/api-gateway"
)


def _recorder_factory(
    service: DevPersistenceService,
    *,
    org_id: uuid.UUID,
    user_id: uuid.UUID,
    conversation_id: uuid.UUID,
    run_id: uuid.UUID,
):
    """A REAL ``PersistenceRunRecorder``, typed as the shared harness's fake.

    Same cast, and the same reason for it, as the CHAOS-3423/3424 suite's own
    inline factories: both types satisfy the ``RunRecorder`` protocol
    structurally, so ``DevOrchestrator`` neither knows nor cares, and the cast
    only narrows the STATIC type back to what ``run_preflight_orchestrator``
    declares.
    """

    def factory() -> Recorder:
        return PersistenceRunRecorder(  # type: ignore[return-value]
            service,
            org_id=org_id,
            user_id=user_id,
            conversation_id=conversation_id,
            run_id=run_id,
            provider_source="platform",
        )

    return factory


class _LedgerWriteFailsRecorder(PersistenceRunRecorder):
    """A real recorder whose ledger write raises, and nothing else.

    Deliberately a REAL ``PersistenceRunRecorder`` subclass rather than a
    fake: everything except ``append_resolution`` must behave exactly as
    production does, or the test proves something about a stub instead of
    about the orchestrator's failure handling.
    """

    async def append_resolution(self, entry: Any) -> None:
        raise RuntimeError("resolution ledger storage unavailable")


async def _ledger_rows(session, run_id: uuid.UUID) -> list[DevRunResolution]:
    return list(
        (
            await session.scalars(
                select(DevRunResolution)
                .where(DevRunResolution.run_id == run_id)
                .order_by(DevRunResolution.entry_ordinal)
            )
        ).all()
    )


async def _corpus_resolution_path(
    maker, run_id: uuid.UUID, *, mention_texts: tuple[str, ...] = ()
) -> str | None:
    """The ``resolution_path`` the Wave 4 corpus runner would derive for this
    run, through the PRODUCTION readers rather than a re-implementation.

    ``_inner_ledger_query._fetch_entries`` is the exact code the acceptance
    stack execs inside the api container, and ``derive_resolution_path`` /
    ``attach_mention_texts`` are the exact functions the live runner calls on
    its output. Executing them here -- rather than asserting the row shapes
    and reasoning about what the runner would do with them -- is what makes
    this a proof of the corpus claim instead of a restatement of it. A
    re-implementation could agree with a broken producer; this cannot.

    It does NOT replace the armed corpus run. It proves the eight cases are
    no longer unsatisfiable BY CONSTRUCTION, which is the specific claim
    CHAOS-3533 makes. Whether each case then passes end to end is a
    measurement only the armed run can make, and this lane has not made it.
    """

    uri = maker.kw["bind"].url.render_as_string(hide_password=False)
    entries = [
        ResolutionLedgerEntry(**entry) for entry in await _fetch_entries(run_id, uri)
    ]
    return derive_resolution_path(
        attach_mention_texts(entries, list(mention_texts)) if mention_texts else entries
    )


@pytest.mark.asyncio
async def test_chaos_3533_not_found_terminate_persists_its_resolution_ledger(
    seeded,  # noqa: F811
) -> None:
    """The class in its simplest shape: one named subject, no authorized
    match, the run terminates -- and today leaves zero ledger rows.

    This is the run shape behind ``scope.no-match``, ``scope.deleted-subject``,
    ``scope.outcome.unresolved``, ``subject-label.typo-tolerance`` and the rest
    of the eight. The run DID resolve something: it resolved to "no authorized
    match". That decision is a resolution outcome the ledger has a member for,
    and today it leaves no auditable trace whatsoever.
    """

    maker, org_id, user_id = seeded
    question = "What is the status of the Nightfall project?"
    conversation_id, run_id = await _seed_run(maker, org_id, user_id, question=question)

    async with maker() as session:
        service = DevPersistenceService(session)
        output = await run_preflight_orchestrator(
            question=question,
            entities=[(str(org_id), ASK_DEV_PROJECT)],
            org_id=str(org_id),
            user_id=str(user_id),
            conversation_id=str(conversation_id),
            run_id=str(run_id),
            answer_id=str(uuid.uuid4()),
            script_id="chaos-3533-not-found",
            recorder_factory=_recorder_factory(
                service,
                org_id=org_id,
                user_id=user_id,
                conversation_id=conversation_id,
                run_id=run_id,
            ),
        )
        await session.commit()

        # Setup control: this really is the not-found preflight TERMINATE and
        # not some other no-answer shape. Without this the assertion below
        # could pass for a run that never reached mention resolution at all.
        run_row = await session.get(DevRun, run_id)
        assert run_row is not None
        assert run_row.preflight_outcome == "unresolved_no_authorized_match"
        assert output.result.state is RunState.INSUFFICIENT_EVIDENCE
        assert output.result.answer is None

        rows = await _ledger_rows(session, run_id)
        assert len(rows) == 1, (
            "CHAOS-3533: a preflight TERMINATE must persist the resolution "
            "ledger it built, so a run that declined to answer can evidence "
            "what it resolved -- the same argument CHAOS-3424 already won for "
            f"the PROCEED half. Got {len(rows)} rows."
        )
        assert rows[0].outcome == "no_authorized_match", (
            "the persisted row must carry the outcome the run actually "
            "reached -- never fabricated as a match, and never silently "
            "downgraded to a generic failure"
        )

    # The claim the eight corpus cases actually make, closed end to end
    # against the rows this run really wrote. Each of them declares
    # ``resolution_path: "miss-clarification"`` in resolution-profiles/
    # deterministic-v1.json, and ``resolution_path_in`` never matches the
    # ``None`` an empty ledger produces.
    assert await _corpus_resolution_path(maker, run_id) == "miss-clarification", (
        "CHAOS-3533: the corpus derives 'miss-clarification' from exactly "
        "this ledger, so the eight cases declaring it are satisfiable once "
        "the row exists. Before this fix the derivation returned None on an "
        "empty ledger and resolution_path_in refused it by contract."
    )


@pytest.mark.asyncio
async def test_chaos_3533_committed_cohort_terminate_persists_every_entry(
    seeded,  # noqa: F811
) -> None:
    """CHAOS-3534's ledger half, and the reason that ticket's stated premise
    was wrong.

    A question naming two real repositories resolves BOTH exactly and commits
    a real ``dev_subject_set.v1``; D2 is never consulted, because D2 exists for
    PARTIAL cohorts. The run then terminates on D1 -- ``committed_cohort_v1_
    only``, "we committed every resolvable member and the v1 surface cannot
    render it" -- and, on today's code, persists none of the two exact matches
    it just made.

    That is the same row-writing rule as the eight, not a separate resolution
    failure: CHAOS-3534's ``scope_resolution_outcome='unresolved'`` with an
    empty ledger is what a fully-resolved cohort looks like through a
    TERMINATE that writes nothing.

    Asserted over EVERY entry, in ordinal order, for the same reason
    CHAOS-3424's own comment gives for the PROCEED branch: a cohort question's
    ``exact_match`` entries must stay alongside the rest exactly as the ledger
    recorded them, or the persisted history is a different story from the one
    the preflight actually decided.
    """

    maker, org_id, user_id = seeded
    question = (
        'What\'s the status of repo "meridian/web-app" and repo "meridian/api-gateway"?'
    )
    conversation_id, run_id = await _seed_run(maker, org_id, user_id, question=question)

    async with maker() as session:
        service = DevPersistenceService(session)
        output = await run_preflight_orchestrator(
            question=question,
            entities=[(str(org_id), WEB_APP), (str(org_id), API_GATEWAY)],
            org_id=str(org_id),
            user_id=str(user_id),
            conversation_id=str(conversation_id),
            run_id=str(run_id),
            answer_id=str(uuid.uuid4()),
            script_id="chaos-3533-cohort",
            recorder_factory=_recorder_factory(
                service,
                org_id=org_id,
                user_id=user_id,
                conversation_id=conversation_id,
                run_id=run_id,
            ),
        )
        await session.commit()

        # Setup control, and the direct refutation of CHAOS-3534's stated
        # premise: the cohort really did commit, so an empty ledger here is a
        # persistence gap and not a resolution failure.
        run_row = await session.get(DevRun, run_id)
        assert run_row is not None
        assert run_row.preflight_outcome == "committed_cohort_v1_only"
        assert output.result.answer is None

        rows = await _ledger_rows(session, run_id)
        assert [row.outcome for row in rows] == ["exact_match", "exact_match"], (
            "CHAOS-3533/3534: a committed-cohort TERMINATE must persist every "
            "entry of the ledger it built, in ordinal order -- both named "
            "repositories resolved exactly and the run must be able to say so. "
            f"Got {[row.outcome for row in rows]!r}."
        )
        assert [row.entry_ordinal for row in rows] == [0, 1]

    # CHAOS-3534's own declared value, closed end to end. Its profile entry
    # declares resolution_path 'deterministic-exact', and the mention spans
    # are the case's own ``expected_mention_texts`` -- the same out-of-band
    # strings the live runner attaches, because the persisted ledger carries
    # only opaque mention ids.
    #
    # This closes the RESOLUTION-PATH half of CHAOS-3534 only. Its other
    # half -- the wire ``scope.resolved`` event still publishing
    # ``unresolved`` for this fully-resolved cohort -- is a different
    # producer (contracts_v2.compat.scope_resolution_from_frame) and is NOT
    # fixed here.
    assert (
        await _corpus_resolution_path(
            maker,
            run_id,
            mention_texts=("meridian/web-app", "meridian/api-gateway"),
        )
        == "deterministic-exact"
    )


@pytest.mark.asyncio
async def test_chaos_3533_ambiguous_non_terminating_mention_never_poisons_the_frame(
    seeded,  # noqa: F811
) -> None:
    """THE regression guard for this change, and the one that must be watched
    failing against the naive fix.

    ``persistence.service._authorize_clarification_candidates`` (a CHAOS-3325
    Codex NO-SHIP finding) authorizes a frame's ``clarification_candidates``
    against "the highest-ordinal ``ambiguous_candidates`` ledger row for this
    run", in BOTH directions -- an empty frame against a non-empty row is
    rejected just as hard as a mismatch. That heuristic is only safe while at
    most ONE ambiguous row can exist per run, which is true today precisely
    because a TERMINATE writes at most one row.

    This run breaks that assumption. "Nightfall" (ordinal 0) has no authorized
    match and is therefore the terminating mention, so the frame carries NO
    candidates; "Atlas" (ordinal 1) is genuinely ambiguous and sits in the same
    ledger. A naive whole-ledger write inserts Atlas's ambiguous row, the
    authorize check finds it, compares it against an empty frame, and rejects
    -- rolling back ``record_frame`` and degrading a perfectly ordinary
    not-found terminal.

    Watch this test fail with the whole-ledger write alone; it passes only
    once the authorizing mention is named explicitly instead of guessed by
    ordinal. It is deliberately written against observable state -- the frame
    row exists, the ledger holds both entries -- rather than against the
    absence of an exception, because a swallowed rollback would still let an
    exception-shaped assertion pass.
    """

    maker, org_id, user_id = seeded
    question = "Compare project Nightfall and project Atlas"
    conversation_id, run_id = await _seed_run(maker, org_id, user_id, question=question)

    async with maker() as session:
        service = DevPersistenceService(session)
        output = await run_preflight_orchestrator(
            question=question,
            entities=[
                (str(org_id), ATLAS_PROJECT_ONE),
                (str(org_id), ATLAS_PROJECT_TWO),
                (str(org_id), ASK_DEV_PROJECT),
            ],
            org_id=str(org_id),
            user_id=str(user_id),
            conversation_id=str(conversation_id),
            run_id=str(run_id),
            answer_id=str(uuid.uuid4()),
            script_id="chaos-3533-mixed",
            recorder_factory=_recorder_factory(
                service,
                org_id=org_id,
                user_id=user_id,
                conversation_id=conversation_id,
                run_id=run_id,
            ),
        )
        await session.commit()

        run_row = await session.get(DevRun, run_id)
        assert run_row is not None
        assert run_row.preflight_outcome == "unresolved_no_authorized_match"
        assert output.result.answer is None

        rows = await _ledger_rows(session, run_id)
        outcomes = [row.outcome for row in rows]
        # The shape this guard exists for must actually be present: an
        # ambiguous row that is NOT the terminating mention's. If the catalog
        # or the not-found fallback ever stops producing it, this test would
        # otherwise keep passing while guarding nothing.
        assert outcomes == ["no_authorized_match", "ambiguous_candidates"], (
            "setup control: this run must hold a non-terminating ambiguous "
            f"entry alongside the terminating not-found one. Got {outcomes!r}."
        )

        frame_rows = (
            await session.scalars(
                select(DevAnswerFrame).where(DevAnswerFrame.run_id == run_id)
            )
        ).all()
        assert len(frame_rows) == 1, (
            "the not-found frame must still be persisted: a non-terminating "
            "mention's ambiguity is not a clarification offer and must never "
            "make the authorize check reject the frame that WAS built. Got "
            f"{len(frame_rows)} frame rows."
        )
        assert frame_rows[0].payload["clarification_candidates"] == [], (
            "and the frame must still carry no candidates -- the run offered "
            "none, and persisting Atlas's candidates here would disclose an "
            "offer the user was never made"
        )


@pytest.mark.asyncio
async def test_chaos_3325_ambiguous_terminate_still_authorizes_its_candidates(
    seeded,  # noqa: F811
) -> None:
    """Preserved-behaviour guard: the one terminate that already persisted a
    row must keep persisting it, and its frame must still be authorized
    against it.

    This is the CHAOS-3325 contract in its original shape -- a genuinely
    ambiguous single mention, candidates on the frame, a matching ledger row.
    Asserted here against the REAL persistence service so the mention_id
    amendment cannot quietly turn the authorize check into a no-op: the frame
    lands only if the check ran and passed.
    """

    maker, org_id, user_id = seeded
    question = "What's the status of the Atlas project?"
    conversation_id, run_id = await _seed_run(maker, org_id, user_id, question=question)

    async with maker() as session:
        service = DevPersistenceService(session)
        output = await run_preflight_orchestrator(
            question=question,
            entities=[
                (str(org_id), ATLAS_PROJECT_ONE),
                (str(org_id), ATLAS_PROJECT_TWO),
            ],
            org_id=str(org_id),
            user_id=str(user_id),
            conversation_id=str(conversation_id),
            run_id=str(run_id),
            answer_id=str(uuid.uuid4()),
            script_id="chaos-3533-ambiguous",
            recorder_factory=_recorder_factory(
                service,
                org_id=org_id,
                user_id=user_id,
                conversation_id=conversation_id,
                run_id=run_id,
            ),
        )
        await session.commit()

        assert output.result.error is not None
        assert output.result.error.code == "scope_ambiguous"

        rows = await _ledger_rows(session, run_id)
        assert [row.outcome for row in rows] == ["ambiguous_candidates"]

        frame_rows = (
            await session.scalars(
                select(DevAnswerFrame).where(DevAnswerFrame.run_id == run_id)
            )
        ).all()
        assert len(frame_rows) == 1
        candidates = frame_rows[0].payload["clarification_candidates"]
        assert candidates, (
            "the ambiguous terminal must still disclose its candidates -- if "
            "this ever empties, the authorize check has stopped comparing "
            "anything and passes vacuously"
        )
        assert [candidate["entity_ref"]["entity_id"] for candidate in candidates] == [
            candidate["entity_ref"]["entity_id"]
            for candidate in rows[0].payload["candidates"]
        ]


@pytest.mark.asyncio
async def test_chaos_3533_a_failed_ledger_write_still_leaves_a_terminal_frame(
    seeded,  # noqa: F811
    caplog,
) -> None:
    """Codex adversarial review, HIGH, CONFIRMED BY EXECUTION and fixed here.

    The TERMINATE branch wrote the ledger and the frame inside ONE
    try/except, then called ``finish(frame_already_recorded=True)``
    unconditionally. So if an ``append_resolution`` raised, control jumped to
    the ``except``, ``record_frame`` never ran -- and ``finish`` was still
    told the frame had already been recorded, so it skipped building the v1
    compatibility frame too. The run landed with no ledger AND no
    ``dev_answer_frames`` row.

    Reachability is what this change introduced: before it, a not-found or
    committed-cohort terminate called ``append_resolution`` ZERO times, so it
    could not fail there and always got its frame. Widening the ledger write
    to every terminate widened this failure mode to every terminate with it
    -- turning a hardening change into a new way to lose a terminal under
    stress, which is precisely the class CHAOS-3533 exists to close.

    The two writes are now separated. A ledger fault is logged and counted
    like its PROCEED-branch sibling (CHAOS-3424 round 3 made exactly this
    argument: "a lost ledger left no log line and no metric, so the exact
    forensic gap this exists to close could itself go unnoticed"), and
    ``frame_already_recorded`` now reports what actually happened, so
    ``finish`` builds the compatibility frame CHAOS-3297 P1 says every
    terminal path must have "structurally rather than by caller discipline".

    Asserted on observable state -- a frame row exists, and an operational
    signal was emitted -- never on the absence of an exception, which a
    swallowed rollback would satisfy while losing everything.
    """

    maker, org_id, user_id = seeded
    question = "What is the status of the Nightfall project?"
    conversation_id, run_id = await _seed_run(maker, org_id, user_id, question=question)

    async with maker() as session:
        service = DevPersistenceService(session)

        def recorder_factory() -> Recorder:
            return _LedgerWriteFailsRecorder(  # type: ignore[return-value]
                service,
                org_id=org_id,
                user_id=user_id,
                conversation_id=conversation_id,
                run_id=run_id,
                provider_source="platform",
            )

        with caplog.at_level(logging.ERROR):
            output = await run_preflight_orchestrator(
                question=question,
                entities=[(str(org_id), ASK_DEV_PROJECT)],
                org_id=str(org_id),
                user_id=str(user_id),
                conversation_id=str(conversation_id),
                run_id=str(run_id),
                answer_id=str(uuid.uuid4()),
                script_id="chaos-3533-ledger-fault",
                recorder_factory=recorder_factory,
            )
        await session.commit()

        # Setup control: the ledger write really did fail, so the assertions
        # below are about the failure path and not the happy one.
        assert await _ledger_rows(session, run_id) == []

        # The run is still a coherent terminal, not stranded.
        assert output.result.state is RunState.INSUFFICIENT_EVIDENCE
        assert output.result.error is not None

        frame_rows = (
            await session.scalars(
                select(DevAnswerFrame).where(DevAnswerFrame.run_id == run_id)
            )
        ).all()
        assert len(frame_rows) == 1, (
            "CHAOS-3297 P1: every terminal path persists a dev_answer_frame.v1. "
            "A ledger-write fault must cost the ledger, never the frame as "
            f"well -- got {len(frame_rows)} frame rows."
        )

        # And the dropped ledger must be visible to an operator rather than
        # silent -- the CHAOS-3424 round-3 argument, applied to this branch.
        assert any(
            "resolution_ledger_write_fault" in record.message
            for record in caplog.records
        ), (
            "a dropped resolution ledger must emit its own operational "
            "signal; silence here is how the forensic gap CHAOS-3533 closes "
            "would itself go unnoticed"
        )

        # The closed-vocabulary explanation of WHY the run terminated must
        # survive the rollback that discarded it.
        run_row = await session.get(DevRun, run_id)
        assert run_row is not None
        assert run_row.preflight_outcome == "unresolved_no_authorized_match"
