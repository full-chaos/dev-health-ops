"""CHAOS-3525: the QUA proposal stops being discarded and commits a subject.

CHAOS-3389 built the shadow seam and CHAOS-3452 armed it with a real,
separately-budgeted provider -- but the record it produces is a local
variable nothing reads, so a question naming a subject in natural form still
dead-ends. This module is the RED-first proof of the promotion: on a run
where the DETERMINISTIC layer declined, a verified high-confidence proposal
becomes the committed subject, disclosed in prose and on the wire.

The flagship case is literal, from CHAOS-3425 via CHAOS-3525: "What's the
status of the ACR project" against an organization whose project is
"Dev Health Agent Context Runtime (Context Fabric)". "ACR" is the acronym of
the label's INNER words -- the deterministic layer offers it as a candidate
and correctly refuses to pick it (``alias_matching``'s never-auto-commit
contract), which is exactly why it dead-ends today.

Structure of the guarantees, because they pull in opposite directions:

* promotion happens ONLY where the deterministic layer declined -- it never
  overrides a real deterministic commit;
* every degradation of the LLM (absent, failing, timing out, budget-
  exhausted, low confidence, out-of-slice proposal) leaves the run exactly
  as it would have been with the seam off;
* an accepted proposal is never silent: it is disclosed in user-facing prose
  AND published on the wire.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import pytest

from dev_health_ops.api.dev.contracts import ScopeResolutionOutcome
from dev_health_ops.api.dev.contracts_v2.subject import ResolutionOutcome
from dev_health_ops.api.dev.qua_shadow import (
    QUAShadowConfig,
    QUAShadowStatus,
    QuestionUnderstandingShadow,
)
from dev_health_ops.api.dev.scope_service import (
    AuthorizedEntity,
    EntityKind,
    ScopeRequestCache,
    ScopeResolutionService,
)
from dev_health_ops.llm.agent.contracts import AgentFinalAnswer
from dev_health_ops.llm.agent.scripted import ScriptedAgentProvider, ScriptedStep
from tests._chaos_3292_preflight import (
    ORG_ID,
    SeededCatalog,
    run_preflight_orchestrator,
)

pytestmark = pytest.mark.asyncio


#: The acceptance world's own project row, verbatim
#: (``tests/acceptance/world/ask-dev-world.v1/subjects.json``'s
#: ``subject.exact.context-fabric``). Reused rather than re-worded so this
#: unit proof and the acceptance corpus point at the same real label shape:
#: >=5 capitalized words AND a parenthetical qualifier, with an acronym
#: ("ACR") that matches the INNER words only.
ACR_PROJECT = AuthorizedEntity(
    EntityKind.PROJECT,
    "project-context-fabric",
    "Dev Health Agent Context Runtime (Context Fabric)",
)

ACR_QUESTION = "What's the status of the ACR project"


def _qua_response(
    *,
    intent_id: str = "entity_status",
    cardinality: str = "singular",
    mentions: list[dict],
    requires_clarification: bool = False,
) -> dict:
    return {
        "schema_version": "dev_question_understanding.v1",
        "intent_id": intent_id,
        "cardinality": cardinality,
        "mentions": mentions,
        "requires_clarification": requires_clarification,
    }


def _selecting_shadow(
    *,
    text_span: str,
    confidence: float = 0.95,
    selected_index: int | None = 0,
    outcome: str = "resolved",
    commit_enabled: bool = True,
    entities: Sequence[tuple[str, AuthorizedEntity]] | None = None,
    script_id: str = "qua-commit",
) -> QuestionUnderstandingShadow:
    """A shadow whose provider selects ``selected_index`` for one mention."""

    catalog = SeededCatalog(list(entities or [(ORG_ID, ACR_PROJECT)]))
    scope_service = ScopeResolutionService(catalog, cache=ScopeRequestCache())
    provider = ScriptedAgentProvider(
        [
            ScriptedStep(
                decision=AgentFinalAnswer(
                    _qua_response(
                        mentions=[
                            {
                                "text_span": text_span,
                                "outcome": outcome,
                                "selected_candidate_index": selected_index,
                                "candidate_indices": (
                                    [selected_index]
                                    if selected_index is not None
                                    else []
                                ),
                                "confidence": confidence,
                            }
                        ]
                    )
                )
            )
        ],
        script_id=script_id,
    )
    return QuestionUnderstandingShadow(
        provider=provider,
        scope_service=scope_service,
        config=QUAShadowConfig(enabled=True, commit_enabled=commit_enabled),
    )


def _shadow_records(output: Any) -> list[Any]:
    assert output.recorder is not None
    return list(output.recorder.qua_shadow_records)


# ---------------------------------------------------------------------------
# 1. the acceptance criterion, literally
# ---------------------------------------------------------------------------


async def test_the_acr_acronym_resolves_instead_of_dead_ending() -> None:
    """CHAOS-3525's literal acceptance criterion.

    Today this run terminates without a subject: the acronym is offered as a
    candidate and never picked. With the promotion armed it must commit the
    one authorized project the acronym names.
    """

    shadow = _selecting_shadow(text_span="ACR")

    output = await run_preflight_orchestrator(
        question=ACR_QUESTION,
        entities=[(ORG_ID, ACR_PROJECT)],
        script_id="acr-commit",
        qua_shadow=shadow,
    )

    [record] = _shadow_records(output)
    assert record.status is QUAShadowStatus.EVALUATED
    assert record.mentions[0].selected_entity == ACR_PROJECT

    published = output.result.scope_resolution
    assert published is not None, "the run must publish the scope it committed"
    assert published.outcome is ScopeResolutionOutcome.EXACT
    assert published.resolved_scope is not None
    assert published.authorized_entity_ids == [ACR_PROJECT.canonical_id]


async def test_the_dead_end_is_what_happens_without_the_promotion() -> None:
    """The control that makes the test above meaningful.

    Same question, same catalog, no shadow: the deterministic layer declines
    and the run reaches a no-answer terminal. If this ever starts resolving
    on its own, the test above stops proving the promotion did anything.
    """

    output = await run_preflight_orchestrator(
        question=ACR_QUESTION,
        entities=[(ORG_ID, ACR_PROJECT)],
        script_id="acr-baseline",
    )

    assert output.result.answer is None
    published = output.result.scope_resolution
    assert published is None or published.outcome is not ScopeResolutionOutcome.EXACT


# ---------------------------------------------------------------------------
# 2. the deterministic layer is never second-guessed
# ---------------------------------------------------------------------------


async def test_a_deterministic_commit_is_never_overridden() -> None:
    """The fast path stays the fast path.

    A model proposal must not be able to move a subject the catalog already
    matched literally. Without this, CHAOS-3388's deterministic layer becomes
    a suggestion the LLM may overrule -- and every deterministic guarantee
    downstream of it becomes conditional on the model agreeing.
    """

    from tests._chaos_3292_preflight import ASK_DEV_PROJECT

    # The proposal points at a DIFFERENT authorized entity than the one the
    # deterministic layer resolves for this question.
    shadow = _selecting_shadow(
        text_span="Ask Dev",
        entities=[(ORG_ID, ASK_DEV_PROJECT), (ORG_ID, ACR_PROJECT)],
        selected_index=0,
        script_id="qua-no-override",
    )

    baseline = await run_preflight_orchestrator(
        question="What's the status of the Ask Dev project?",
        entities=[(ORG_ID, ASK_DEV_PROJECT), (ORG_ID, ACR_PROJECT)],
        script_id="override-baseline",
    )
    promoted = await run_preflight_orchestrator(
        question="What's the status of the Ask Dev project?",
        entities=[(ORG_ID, ASK_DEV_PROJECT), (ORG_ID, ACR_PROJECT)],
        script_id="override-live",
        qua_shadow=shadow,
    )

    assert promoted.outcome_tuple() == baseline.outcome_tuple()
    assert promoted.preflight_outcomes() == ("proceeded_committed_subject",)


# ---------------------------------------------------------------------------
# 3. authz -- the hard invariant
# ---------------------------------------------------------------------------


async def test_an_entity_the_caller_cannot_see_is_not_committed() -> None:
    """The commit-time re-authorization, isolated.

    On the singular path this check is not defence in depth -- it is the ONLY
    receipt. ``committed_resolution_for`` mints no ``subject_set_fingerprint``
    and the executor's fingerprint cross-check covers only set batches, so
    nothing downstream re-verifies a singular committed entity.
    """

    from dev_health_ops.api.dev.qua_promotion import (
        QUAPromotion,
        verify_still_authorized,
    )

    other_org_project = AuthorizedEntity(
        EntityKind.PROJECT, "project-sibling-secret", "Sibling Secret"
    )
    # The catalog holds it, but under a DIFFERENT organization.
    catalog = SeededCatalog([("org_sibling_holdings", other_org_project)])
    scope_service = ScopeResolutionService(catalog, cache=ScopeRequestCache())

    authorized = await verify_still_authorized(
        QUAPromotion(
            mention_id="mention_01",
            text_span="Sibling Secret",
            entity=other_org_project,
            confidence=1.0,
        ),
        scope_service=scope_service,
        org_id=ORG_ID,
        permission_fingerprint="permissions_01",
        limit=25,
    )
    assert authorized is False, (
        "an entity belonging to another tenant must never verify for this org"
    )


async def test_a_verification_that_could_not_run_is_not_a_verification() -> None:
    """A catalog fault must read as 'not authorized', never as 'fine'."""

    from dev_health_ops.api.dev.qua_promotion import (
        QUAPromotion,
        verify_still_authorized,
    )

    class _ExplodingService:
        async def search(self, *_args, **_kwargs):
            raise RuntimeError("catalog is down")

    authorized = await verify_still_authorized(
        QUAPromotion(
            mention_id="mention_01",
            text_span="ACR",
            entity=ACR_PROJECT,
            confidence=1.0,
        ),
        scope_service=_ExplodingService(),  # type: ignore[arg-type]
        org_id=ORG_ID,
        permission_fingerprint="permissions_01",
        limit=25,
    )
    assert authorized is False


async def test_a_rejected_proposal_never_commits() -> None:
    """The shadow's own verifier already refused this one.

    ``rejected_reason`` is set when the model named an index outside that
    mention's own authorized slice. The promotion must treat a rejected
    assessment as no proposal at all, not as a proposal with a note attached.
    """

    from dev_health_ops.api.dev.contracts_v2.question_understanding import QUAOutcome
    from dev_health_ops.api.dev.qua_promotion import promotable_selection
    from dev_health_ops.api.dev.qua_shadow import (
        QUAShadowMentionAssessment,
        QUAShadowRecord,
    )

    record = QUAShadowRecord(
        status=QUAShadowStatus.EVALUATED,
        mentions=(
            QUAShadowMentionAssessment(
                mention_id="mention_01",
                text_span="ACR",
                outcome=QUAOutcome.RESOLVED,
                selected_entity=ACR_PROJECT,
                candidate_entities=(ACR_PROJECT,),
                confidence=0.99,
                rejected_reason="index_outside_mention_shortlist",
            ),
        ),
    )
    assert promotable_selection(record, deterministic_declined=True) is None


async def test_a_low_confidence_proposal_leaves_the_run_alone() -> None:
    """The floor is a real gate, not a recorded number.

    A proposal under ``QUA_COMMIT_MIN_CONFIDENCE`` must leave the run exactly
    where the deterministic layer left it -- which for this question is the
    dead end. Asserted against the no-shadow baseline rather than against a
    hand-written expectation, so it stays honest if that terminal changes.
    """

    from dev_health_ops.api.dev.qua_promotion import QUA_COMMIT_MIN_CONFIDENCE

    shadow = _selecting_shadow(
        text_span="ACR",
        confidence=QUA_COMMIT_MIN_CONFIDENCE - 0.01,
        script_id="qua-low-confidence",
    )

    baseline = await run_preflight_orchestrator(
        question=ACR_QUESTION,
        entities=[(ORG_ID, ACR_PROJECT)],
        script_id="low-conf-baseline",
    )
    attempted = await run_preflight_orchestrator(
        question=ACR_QUESTION,
        entities=[(ORG_ID, ACR_PROJECT)],
        script_id="low-conf-live",
        qua_shadow=shadow,
    )

    assert attempted.outcome_tuple() == baseline.outcome_tuple()


async def test_commit_mode_off_is_byte_identical_to_the_seam_being_off() -> None:
    """The ladder is real: arming the shadow is not arming the commit.

    Same successful, high-confidence, correct proposal as the acceptance test
    -- with only ``commit_enabled`` flipped off. CHAOS-3389's guarantee that
    the shadow observes without influencing has to survive this changeset,
    and this is the test that says so in commit mode's own terms.
    """

    shadow = _selecting_shadow(
        text_span="ACR", commit_enabled=False, script_id="qua-commit-off"
    )

    baseline = await run_preflight_orchestrator(
        question=ACR_QUESTION,
        entities=[(ORG_ID, ACR_PROJECT)],
        script_id="commit-off-baseline",
    )
    shadowed = await run_preflight_orchestrator(
        question=ACR_QUESTION,
        entities=[(ORG_ID, ACR_PROJECT)],
        script_id="commit-off-live",
        qua_shadow=shadow,
    )

    assert shadowed.outcome_tuple() == baseline.outcome_tuple()
    # The proposal was still MADE -- this is the shadow doing its job. If the
    # record were absent the test would pass for the wrong reason.
    [record] = _shadow_records(shadowed)
    assert record.status is QUAShadowStatus.EVALUATED
    assert record.mentions[0].selected_entity == ACR_PROJECT


# ---------------------------------------------------------------------------
# 4. a model-chosen subject is never silent
# ---------------------------------------------------------------------------


async def test_the_match_is_disclosed_in_prose_and_on_the_wire() -> None:
    """Promotion's price of admission.

    An LLM deciding what a question is about is a new kind of claim, and it
    must be checkable by the person reading the answer -- not only by an
    auditor reading a receipt. Asserted on both surfaces the CHAOS-3497 work
    made reachable: the answer's own warnings, and the ``warning`` frames the
    stream publishes from them.
    """

    import asyncio

    from dev_health_ops.api.dev.contracts import StreamEventType
    from dev_health_ops.api.dev.no_match_terminal import subject_matched_disclosure
    from dev_health_ops.api.dev.streaming import stream_orchestrator

    shadow = _selecting_shadow(text_span="ACR", script_id="qua-disclosure")
    output = await run_preflight_orchestrator(
        question=ACR_QUESTION,
        entities=[(ORG_ID, ACR_PROJECT)],
        script_id="acr-disclosure",
        qua_shadow=shadow,
    )

    answer = output.result.answer
    assert answer is not None, "the promotion must produce an answered run"
    expected = subject_matched_disclosure(span="ACR", label=ACR_PROJECT.label)
    assert expected in answer.warnings, (
        "a subject chosen by a model must say so in user-facing prose"
    )

    async def _replay(_sink):
        return output.result

    frames = [
        event
        async for event in stream_orchestrator(
            run_id="run_01", run_with_events=_replay, cancellation=asyncio.Event()
        )
    ]
    assert expected in [
        frame.warning for frame in frames if frame.event is StreamEventType.WARNING
    ]


async def test_the_commit_is_auditable_as_a_model_decision() -> None:
    """The receipt must not read as stronger provenance than the run had.

    An ``exact_match`` ledger entry looks identical whichever layer produced
    it, so without a distinct ``resolver_version`` an LLM commit would be
    indistinguishable from a literal catalog match after the fact -- and the
    corpus's ``derive_resolution_path`` would label it ``deterministic-*``.
    """

    from dev_health_ops.api.dev.qua_promotion import (
        QUA_COMMIT_DIAGNOSTIC,
        QUA_COMMIT_RESOLVER_VERSION,
    )

    shadow = _selecting_shadow(text_span="ACR", script_id="qua-audit")
    output = await run_preflight_orchestrator(
        question=ACR_QUESTION,
        entities=[(ORG_ID, ACR_PROJECT)],
        script_id="acr-audit",
        qua_shadow=shadow,
    )

    assert QUA_COMMIT_DIAGNOSTIC in output.preflight_outcomes()

    assert output.recorder is not None
    entries = output.recorder.resolutions
    assert entries, "the committing run must persist a resolution entry"
    qua_versions = {
        entry.resolver_version
        for entry in entries
        if entry.outcome is ResolutionOutcome.EXACT_MATCH
    }
    assert qua_versions == {QUA_COMMIT_RESOLVER_VERSION}

    # The claim is DISTINGUISHABILITY, so it has to be asserted against what a
    # deterministic commit actually stamps -- not against the constant under
    # test. Asserting `== QUA_COMMIT_RESOLVER_VERSION` alone passes for any
    # value of that constant, including one identical to the deterministic
    # resolver's: a test that cannot fail for the reason it exists. (Caught by
    # mutating the constant to `ask_dev_scope.v1` and watching this test stay
    # green.)
    from tests._chaos_3292_preflight import ASK_DEV_PROJECT

    deterministic = await run_preflight_orchestrator(
        question="What's the status of the Ask Dev project?",
        entities=[(ORG_ID, ASK_DEV_PROJECT)],
        script_id="audit-deterministic",
    )
    assert deterministic.recorder is not None
    deterministic_versions = {
        entry.resolver_version
        for entry in deterministic.recorder.resolutions
        if entry.outcome is ResolutionOutcome.EXACT_MATCH
    }
    assert deterministic_versions, "the control must itself commit a subject"
    assert qua_versions.isdisjoint(deterministic_versions), (
        "a model-committed entry must be distinguishable from a "
        f"deterministically-committed one; both stamped {qua_versions}"
    )


async def test_a_widened_bare_name_commits_instead_of_answering_org_wide() -> None:
    """The OTHER decline shape, and the one with different persistence.

    A bare name with no kind noun does not terminate -- it widens to
    organization scope and answers about everything (``proceeded_unresolved_
    bare_name``). That is a wrong-subject answer wearing a disclosure, and it
    is the second thing this promotion exists to replace.

    It also exercises a different write path: this branch already flushed its
    whole ledger, so promotion may append only the new entry. Writing the
    others again would collide on the ``(run_id, entry_ordinal)`` unique
    constraint -- a failure that would surface as a poisoned session rather
    than as anything legible.
    """

    from dev_health_ops.api.dev.qua_promotion import QUA_COMMIT_DIAGNOSTIC

    shadow = _selecting_shadow(text_span="ACR", script_id="qua-bare-name")

    baseline = await run_preflight_orchestrator(
        question="How is ACR doing?",
        entities=[(ORG_ID, ACR_PROJECT)],
        script_id="bare-name-baseline",
    )
    assert baseline.preflight_outcomes() == ("proceeded_unresolved_bare_name",), (
        "the control must actually take the widening branch, or this test "
        "proves nothing about it"
    )

    promoted = await run_preflight_orchestrator(
        question="How is ACR doing?",
        entities=[(ORG_ID, ACR_PROJECT)],
        script_id="bare-name-live",
        qua_shadow=shadow,
    )

    assert QUA_COMMIT_DIAGNOSTIC in promoted.preflight_outcomes()
    published = promoted.result.scope_resolution
    assert published is not None
    assert published.outcome is ScopeResolutionOutcome.EXACT
    assert published.authorized_entity_ids == [ACR_PROJECT.canonical_id]

    # No entry may be written twice: the ordinals the run persisted must be
    # exactly the ledger's own contiguous sequence.
    assert promoted.recorder is not None
    ordinals = [entry.entry_ordinal for entry in promoted.recorder.resolutions]
    assert len(ordinals) == len(set(ordinals)), (
        f"an entry was persisted twice (ordinals={ordinals}) -- the "
        "(run_id, entry_ordinal) unique constraint would reject this live"
    )


# ---------------------------------------------------------------------------
# 5. the production gate
# ---------------------------------------------------------------------------


def test_production_requires_both_flags_to_commit(monkeypatch) -> None:
    """The ladder, asserted where production actually reads it.

    ``ASK_DEV_QUA_COMMIT_ENABLED`` alone must not arm commit mode: with the
    shadow off there is no proposal to promote, and a flag that appeared to
    work while silently doing nothing is worse than one that is plainly off.
    Parametrized over all four combinations rather than only the happy one --
    the three that must stay off are the whole point.
    """

    from dev_health_ops.api.dev.production_runtime import _qua_commit_flag_enabled

    def _commit_enabled(shadow: str | None, commit: str | None) -> bool:
        for name, value in (
            ("ASK_DEV_QUA_SHADOW_ENABLED", shadow),
            ("ASK_DEV_QUA_COMMIT_ENABLED", commit),
        ):
            if value is None:
                monkeypatch.delenv(name, raising=False)
            else:
                monkeypatch.setenv(name, value)
        # Production's OWN predicate, not a restatement of it here: a test
        # that re-implements the expression it checks passes for any
        # production behaviour whatsoever.
        return _qua_commit_flag_enabled()

    assert _commit_enabled(None, None) is False
    assert _commit_enabled("1", None) is False
    assert _commit_enabled(None, "1") is False, (
        "the commit flag must be inert without the shadow flag that produces "
        "the proposal it promotes"
    )
    assert _commit_enabled("1", "1") is True


async def test_a_failed_commit_time_check_blocks_the_commit_end_to_end() -> None:
    """The singular path's ONLY receipt, exercised as the sole guard.

    ``committed_resolution_for`` mints no ``subject_set_fingerprint``, and the
    executor's fingerprint cross-check covers only SET batches -- so on a
    singular commit nothing downstream re-verifies the entity. This check is
    therefore not defence in depth; if it passes something it should not, the
    subject is committed unverified and no later stage will catch it.

    Asserted end to end rather than as a unit call on the verifier, because
    the claim is about the ORCHESTRATOR refusing to commit, not about the
    function returning False. The proposal here is otherwise perfect: right
    outcome, right entity, confidence well over the floor.
    """

    from dev_health_ops.api.dev import qua_promotion

    refusals: list[str] = []

    async def _always_refuse(promotion, **_kwargs) -> bool:
        refusals.append(promotion.entity.canonical_id)
        return False

    shadow = _selecting_shadow(text_span="ACR", script_id="qua-verify-refuse")

    baseline = await run_preflight_orchestrator(
        question=ACR_QUESTION,
        entities=[(ORG_ID, ACR_PROJECT)],
        script_id="verify-refuse-baseline",
    )

    import dev_health_ops.api.dev.orchestrator as orchestrator_module

    original = orchestrator_module.verify_still_authorized
    orchestrator_module.verify_still_authorized = _always_refuse
    try:
        refused = await run_preflight_orchestrator(
            question=ACR_QUESTION,
            entities=[(ORG_ID, ACR_PROJECT)],
            script_id="verify-refuse-live",
            qua_shadow=shadow,
        )
    finally:
        orchestrator_module.verify_still_authorized = original

    assert refusals == [ACR_PROJECT.canonical_id], (
        "the commit-time check must actually be consulted -- if it were "
        "skipped this test would pass for the wrong reason"
    )
    assert refused.outcome_tuple() == baseline.outcome_tuple(), (
        "a proposal that failed commit-time authorization must leave the run "
        "exactly where the deterministic layer left it"
    )
    assert qua_promotion.QUA_COMMIT_DIAGNOSTIC not in refused.preflight_outcomes()


async def test_the_disclosure_survives_a_full_warning_list() -> None:
    """ "Never silent" has to mean never, including at the contract's bound.

    ``DevAnswer.warnings`` caps at 20. The first cut returned the answer
    unchanged once that bound was spent, so a QUA-committed subject could
    reach the user with no disclosure at all while the commit still stood --
    the exact silent model-chosen subject this design exists to prevent,
    reachable without any authorization failure. (Adversarial review finding;
    reproduced directly before fixing.)

    The disclosure wins the last slot. A producer warning is displaced rather
    than the server's own statement of what the answer is ABOUT, because an
    undisclosed subject is a claim the reader cannot check and a dropped
    twentieth warning is not.
    """

    from dev_health_ops.api.dev.contract_fixtures import positive_fixtures
    from dev_health_ops.api.dev.contracts import DevAnswer
    from dev_health_ops.api.dev.no_match_terminal import (
        disclose_subject_match,
        subject_matched_disclosure,
    )

    answer = DevAnswer.model_validate(positive_fixtures()["dev_answer.v1"])
    saturated = answer.model_copy(
        update={"warnings": [f"producer warning {index}" for index in range(20)]}
    )

    disclosed = disclose_subject_match(saturated, span="ACR", label=ACR_PROJECT.label)
    expected = subject_matched_disclosure(span="ACR", label=ACR_PROJECT.label)

    assert expected in disclosed.warnings
    assert len(disclosed.warnings) == 20, "the contract bound still holds"
    # Idempotent at the bound too: disclosing twice must not displace a
    # second producer warning for a sentence that is already present.
    assert disclose_subject_match(
        disclosed, span="ACR", label=ACR_PROJECT.label
    ).warnings == list(disclosed.warnings)


async def test_authorization_is_checked_by_identity_not_by_search_rank() -> None:
    """A legitimate entity must not be refused for ranking below a cap.

    The re-check used to re-run a fuzzy search and look for the entity among
    the results, which makes authorization depend on how the entity happened
    to rank against everything else matching the same text. Adversarial
    review could not close that: with enough same-kind matches, a genuinely
    authorized entity can sit below the limit and be refused.

    Resolving the entity by its own identity removes the question entirely --
    the answer no longer depends on what else matched. Proven with a catalog
    deliberately stuffed past the candidate cap.
    """

    from dev_health_ops.api.dev.qua_promotion import (
        QUAPromotion,
        verify_still_authorized,
    )

    # Same-kind entities that match "ACR" the SAME way the real project does
    # (as an acronym, so they land in the same ranking tier) and whose labels
    # sort ahead of it, so the real project falls past the candidate cap.
    crowd = [
        (
            ORG_ID,
            AuthorizedEntity(
                EntityKind.PROJECT,
                f"project-crowd-{index:02d}",
                f"Alpha Content Repository {index:02d}",
            ),
        )
        for index in range(40)
    ]
    catalog = SeededCatalog([*crowd, (ORG_ID, ACR_PROJECT)])
    scope_service = ScopeResolutionService(catalog, cache=ScopeRequestCache())

    authorized = await verify_still_authorized(
        QUAPromotion(
            mention_id="mention_01",
            text_span="ACR",
            entity=ACR_PROJECT,
            confidence=0.99,
        ),
        scope_service=scope_service,
        org_id=ORG_ID,
        permission_fingerprint="permissions_01",
        limit=25,
    )
    assert authorized is True, (
        "an authorized entity must verify regardless of how many other "
        "same-kind entities match the same text"
    )
