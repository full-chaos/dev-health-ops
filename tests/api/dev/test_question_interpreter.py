"""Unit coverage for the server-owned question interpreter (CHAOS-3292)."""

from __future__ import annotations

from typing import Any

import pytest

from dev_health_ops.api.dev.contracts import QuestionClass
from dev_health_ops.api.dev.contracts_v2 import (
    Cardinality,
    EntityKind,
    QuestionIntentID,
)
from dev_health_ops.api.dev.question_interpreter import (
    CLARIFICATION_REASONS,
    FALLBACK_CONFIDENCE_FLOOR,
    INTERPRETER_VERSION,
    MAX_MENTIONS,
    ClassifierProposal,
    QuestionInterpreter,
    extract_mentions,
)
from tests._chaos_3292_preflight import fixed_now, request_for, sequential_ids


def _interpreter(**kwargs: Any) -> QuestionInterpreter:
    return QuestionInterpreter(mint_id=sequential_ids(), now=fixed_now, **kwargs)


# ---------------------------------------------------------------------------
# Recognizers
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("question", "expected"),
    [
        (
            "What's the status of the Ask Dev project?",
            QuestionIntentID.ENTITY_STATUS,
        ),
        (
            "What's the status across all projects?",
            QuestionIntentID.PORTFOLIO_STATUS,
        ),
        (
            "What's left on the Ask Dev project?",
            QuestionIntentID.REMAINING_WORK,
        ),
        (
            "What changed in the Ask Dev project since last week?",
            QuestionIntentID.OBSERVED_CHANGE,
        ),
        ("What metrics can you measure?", QuestionIntentID.REGISTERED_STATISTICS),
        (
            "Compare cycle time p50 against last quarter",
            QuestionIntentID.METRIC_COMPARISON,
        ),
        ("Is our deployment data stale?", QuestionIntentID.DATA_TRUST),
        ("How healthy is the Ask Dev project?", QuestionIntentID.PROJECT_HEALTH),
        ("Is the Platform team healthy?", QuestionIntentID.TEAM_HEALTH),
        (
            "Which team is overburdened right now?",
            QuestionIntentID.TEAM_WORKLOAD_BALANCE,
        ),
        (
            "List our operational deficiencies",
            QuestionIntentID.OPERATIONAL_DEFICIENCY_INVENTORY,
        ),
        ("Tell me something interesting", QuestionIntentID.BOUNDED_INVESTIGATION),
    ],
)
@pytest.mark.asyncio
async def test_each_launch_intent_has_a_deterministic_recognizer(
    question: str, expected: QuestionIntentID
) -> None:
    interpreted = await _interpreter().interpret(request_for(question))
    assert interpreted.intent.intent_id is expected


@pytest.mark.asyncio
async def test_an_unrecognized_question_degrades_rather_than_refusing() -> None:
    """R4: a recognizer miss must never block a question that works today."""

    interpreted = await _interpreter().interpret(
        request_for("Tell me something interesting")
    )
    assert interpreted.intent.intent_id is QuestionIntentID.BOUNDED_INVESTIGATION
    assert interpreted.intent.requires_clarification is False
    assert interpreted.intent.confidence < FALLBACK_CONFIDENCE_FLOOR


@pytest.mark.asyncio
async def test_recognizer_order_is_a_stable_tiebreak() -> None:
    """A question satisfying two recognizers always resolves the same way."""

    question = "What changed in the status of the Ask Dev project since last week?"
    seen = {
        (await _interpreter().interpret(request_for(question))).intent.intent_id
        for _ in range(5)
    }
    assert seen == {QuestionIntentID.OBSERVED_CHANGE}


@pytest.mark.asyncio
async def test_interpretation_reasons_carry_recognizer_ids_never_question_text() -> (
    None
):
    question = "What's the status of the Ask Dev project?"
    interpreted = await _interpreter().interpret(request_for(question))

    assert interpreted.intent.interpretation_reasons == ("status.singular",)
    for reason in interpreted.intent.interpretation_reasons:
        assert "Ask Dev" not in reason
        assert reason.lower() not in question.lower()


@pytest.mark.asyncio
async def test_interpreter_version_satisfies_the_platform_token_grammar() -> None:
    from dev_health_ops.api.dev.contracts_v2 import DevFrameVersions

    versions = DevFrameVersions(
        interpreter_version=INTERPRETER_VERSION,
        plan_id="status.entity.v2",
        plan_version="ask_dev_plan.v1",
        tool_contract_version="ask_dev_tools.v2",
        metric_definition_version="ask_dev_metrics.v1",
        query_version="ask_dev_queries.v1",
    )
    assert versions.interpreter_version == INTERPRETER_VERSION


# ---------------------------------------------------------------------------
# Mention extraction
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("question", "expected"),
    [
        ("What's the status of the Ask Dev project?", [("Ask Dev", "project")]),
        ("Can you check project Ask Dev status?", [("Ask Dev", "project")]),
        (
            "Compare project Ask Dev and project Nightfall",
            [("Ask Dev", "project"), ("Nightfall", "project")],
        ),
        ('How is repo "dev-health-ops" doing?', [("dev-health-ops", "repository")]),
        ("Is the Platform team overloaded?", [("Platform", "team")]),
        ("How is issue CHAOS-3292 going?", [("CHAOS-3292", "issue")]),
    ],
)
def test_mentions_are_extracted_in_reading_order(
    question: str, expected: list[tuple[str, str]]
) -> None:
    mentions = extract_mentions(question, mint_id=sequential_ids())
    assert [
        (mention.original_text_span, mention.requested_entity_kind.value)
        for mention in mentions
    ] == expected
    assert [mention.mention_ordinal for mention in mentions] == list(
        range(len(expected))
    )


@pytest.mark.parametrize(
    "question",
    [
        "Our team is overburdened",
        "Which team is at risk?",
        "Show me the status of everything",
        "What metrics can you measure?",
    ],
)
def test_pronouns_and_interrogatives_are_never_subjects(question: str) -> None:
    assert extract_mentions(question, mint_id=sequential_ids()) == ()


@pytest.mark.parametrize(
    ("question", "expected"),
    [
        # CHAOS-3388: a capitalized kind noun immediately after an acronym-
        # shaped name reads exactly like the lowercase form -- "the ACR
        # Project" is as much a named mention as "the ACR project" is. Before
        # the fix, `_NOUN_TRAILING`'s alternation was matched case-
        # sensitively against a lowercase-only noun list, so this question
        # produced zero typed mentions and fell to the untyped bare-name
        # path, which never terminates the run on an unresolved name.
        (
            "What's the status of the ACR Project and what drivers are "
            "blocking it? What's left to complete?",
            [("ACR", "project")],
        ),
        ("PROJECT Nightfall is behind schedule", [("Nightfall", "project")]),
        ("Is the ACR Team overloaded?", [("ACR", "team")]),
    ],
)
def test_a_capitalized_kind_noun_is_recognized_case_insensitively(
    question: str, expected: list[tuple[str, str]]
) -> None:
    mentions = extract_mentions(question, mint_id=sequential_ids())
    assert [
        (mention.original_text_span, mention.requested_entity_kind.value)
        for mention in mentions
    ] == expected


def test_a_noun_bound_to_a_following_name_is_not_also_read_backwards() -> None:
    """ "Compare project Ask Dev" must not mint a project called "Compare"."""

    mentions = extract_mentions(
        "Compare project Ask Dev and project Nightfall", mint_id=sequential_ids()
    )
    assert [mention.original_text_span for mention in mentions] == [
        "Ask Dev",
        "Nightfall",
    ]


def test_extraction_is_bounded_by_the_contract_maximum() -> None:
    question = " ".join(f"project P{index}" for index in range(40))
    mentions = extract_mentions(question, mint_id=sequential_ids())
    assert len(mentions) == MAX_MENTIONS
    assert mentions[-1].mention_ordinal == MAX_MENTIONS - 1


def test_context_refs_are_used_only_when_the_question_names_nothing() -> None:
    from dev_health_ops.api.dev.contracts import DevEntityRef, EntityType

    page_ref = DevEntityRef(
        entity_type=EntityType.PROJECT,
        entity_id="project-page-ctx",
        display_label="Beacon",
    )

    named = extract_mentions(
        "What's the status of the Ask Dev project?",
        context_refs=[page_ref],
        mint_id=sequential_ids(),
    )
    # The page context never overrides, and never silently joins, a named
    # subject: substituting page context for a name is the fabrication path in
    # a different costume.
    assert [mention.original_text_span for mention in named] == ["Ask Dev"]

    unnamed = extract_mentions(
        "What is the current state?", context_refs=[page_ref], mint_id=sequential_ids()
    )
    assert [mention.normalized_lookup_text for mention in unnamed] == [
        "project-page-ctx"
    ]


# ---------------------------------------------------------------------------
# Cardinality, comparison mode, and the client hint
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("question", "expected"),
    [
        ("How are we doing overall?", Cardinality.ORGANIZATION_WIDE),
        ("What's the status of the Ask Dev project?", Cardinality.SINGULAR),
        (
            "Compare project Ask Dev and project Nightfall",
            Cardinality.PLURAL_COHORT,
        ),
    ],
)
@pytest.mark.asyncio
async def test_cardinality_is_derived_from_the_mentions(
    question: str, expected: Cardinality
) -> None:
    interpreted = await _interpreter().interpret(request_for(question))
    assert interpreted.intent.cardinality is expected
    assert len(interpreted.intent.mention_ordinals) == len(interpreted.mentions)


@pytest.mark.asyncio
async def test_subject_kinds_are_deduplicated_and_sorted() -> None:
    interpreted = await _interpreter().interpret(
        request_for("How do project Ask Dev and the Platform team compare?")
    )
    assert interpreted.intent.subject_kinds == (EntityKind.PROJECT, EntityKind.TEAM)


@pytest.mark.asyncio
async def test_the_client_hint_is_recorded_with_its_mandatory_warning() -> None:
    interpreted = await _interpreter().interpret(
        request_for(
            "What's the status of the Ask Dev project?",
            question_class=QuestionClass.INVESTIGATION,
        )
    )
    assert interpreted.intent.client_question_class_hint is QuestionClass.INVESTIGATION
    assert interpreted.intent.client_hint_deprecation_warning is not None
    # ...and it did not steer planning.
    assert interpreted.intent.intent_id is QuestionIntentID.ENTITY_STATUS


# ---------------------------------------------------------------------------
# The constrained model fallback
# ---------------------------------------------------------------------------


class _Classifier:
    def __init__(self, proposal: ClassifierProposal | Exception | None) -> None:
        self.proposal = proposal
        self.calls = 0

    async def classify(self, *, question: str) -> ClassifierProposal | None:
        del question
        self.calls += 1
        if isinstance(self.proposal, Exception):
            raise self.proposal
        return self.proposal


@pytest.mark.asyncio
async def test_the_fallback_runs_only_below_the_confidence_floor() -> None:
    classifier = _Classifier(None)
    await QuestionInterpreter(
        classifier=classifier, mint_id=sequential_ids(), now=fixed_now
    ).interpret(request_for("What's the status of the Ask Dev project?"))
    assert classifier.calls == 0

    await QuestionInterpreter(
        classifier=classifier, mint_id=sequential_ids(), now=fixed_now
    ).interpret(request_for("Tell me something interesting"))
    assert classifier.calls == 1


@pytest.mark.asyncio
async def test_a_provider_failure_never_falls_back_to_organization_scope() -> None:
    """A named subject plus an unavailable classifier asks for clarification."""

    interpreted = await QuestionInterpreter(
        classifier=_Classifier(RuntimeError("provider down")),
        mint_id=sequential_ids(),
        now=fixed_now,
    ).interpret(request_for("Anything I should know about the Ask Dev project?"))

    assert interpreted.intent.requires_clarification is True
    assert (
        interpreted.intent.clarification_reason
        == CLARIFICATION_REASONS["interpreter_unavailable"]
    )
    assert interpreted.mentions  # the mention is preserved, not discarded


@pytest.mark.asyncio
async def test_a_provider_failure_on_a_subjectless_question_keeps_working() -> None:
    """With no named subject there is nothing to get wrong.

    Refusing here would regress an organization-wide question that answers
    correctly in production today.
    """

    interpreted = await QuestionInterpreter(
        classifier=_Classifier(RuntimeError("provider down")),
        mint_id=sequential_ids(),
        now=fixed_now,
    ).interpret(request_for("Tell me something interesting"))

    assert interpreted.intent.requires_clarification is False
    assert interpreted.intent.intent_id is QuestionIntentID.BOUNDED_INVESTIGATION


@pytest.mark.parametrize(
    "proposal",
    [
        ClassifierProposal(intent_id="not_a_real_intent", cardinality="singular"),
        ClassifierProposal(intent_id="entity_status", cardinality="not_a_cardinality"),
        ClassifierProposal(
            intent_id="entity_status", cardinality="singular", confidence=1.5
        ),
        ClassifierProposal(
            intent_id="entity_status",
            cardinality="singular",
            candidates=(("Nightfall", "project"),),
        ),
        ClassifierProposal(
            intent_id="entity_status",
            cardinality="singular",
            candidates=(("Something", "galaxy"),),
        ),
    ],
    ids=["bad_intent", "bad_cardinality", "bad_confidence", "absent_span", "bad_kind"],
)
@pytest.mark.asyncio
async def test_post_validation_rejects_every_out_of_contract_proposal(
    proposal: ClassifierProposal,
) -> None:
    interpreted = await QuestionInterpreter(
        classifier=_Classifier(proposal), mint_id=sequential_ids(), now=fixed_now
    ).interpret(request_for("Tell me something interesting"))

    assert "fallback.rejected" in interpreted.intent.interpretation_reasons
    assert interpreted.intent.intent_id is QuestionIntentID.BOUNDED_INVESTIGATION


@pytest.mark.asyncio
async def test_an_accepted_proposal_is_never_trusted_above_a_recognizer() -> None:
    interpreted = await QuestionInterpreter(
        classifier=_Classifier(
            ClassifierProposal(
                intent_id="project_health",
                cardinality="singular",
                candidates=(("Nightfall", "project"),),
                confidence=1.0,
            )
        ),
        mint_id=sequential_ids(),
        now=fixed_now,
    ).interpret(request_for("Anything about Nightfall worth knowing?"))

    assert interpreted.intent.intent_id is QuestionIntentID.PROJECT_HEALTH
    assert interpreted.intent.confidence < 1.0
    assert "fallback.model_assisted" in interpreted.intent.interpretation_reasons
    assert [mention.original_text_span for mention in interpreted.mentions] == [
        "Nightfall"
    ]


@pytest.mark.asyncio
async def test_a_proposal_cannot_clear_a_clarification_requirement() -> None:
    """There is no field on ``ClassifierProposal`` that could carry it."""

    assert not hasattr(ClassifierProposal, "requires_clarification")
    assert "requires_clarification" not in ClassifierProposal.__annotations__
    assert "mention_id" not in ClassifierProposal.__annotations__
