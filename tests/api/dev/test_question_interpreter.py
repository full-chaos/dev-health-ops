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
from dev_health_ops.api.dev.graph_investigation_query import CohortDiscoveryFamily
from dev_health_ops.api.dev.question_interpreter import (
    _KIND_NOUNS,
    _NON_NAMING_MODIFIERS,
    _WORK_HEAD_NOUNS,
    CLARIFICATION_REASONS,
    FALLBACK_CONFIDENCE_FLOOR,
    INTERPRETER_VERSION,
    MAX_MENTIONS,
    ClassifierProposal,
    QuestionInterpreter,
    classify_cohort_discovery_family,
    extract_mentions,
    organization_mention_spans,
    untyped_name_candidates,
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


@pytest.mark.parametrize(
    "question",
    [
        "Which teams are currently struggling?",
        "Which projects are capacity-constrained?",
    ],
)
@pytest.mark.asyncio
async def test_subjectless_cohort_question_routes_to_discovered_cohort(
    question: str,
) -> None:
    """CHAOS-3652: a zero-mention, bounded cohort-discovery question is
    recognized as ``QuestionIntentID.DISCOVERED_COHORT`` (approved on
    CHAOS-3660), distinct from a genuinely unbounded question, so it can be
    routed to graph-assisted cohort discovery instead of the legacy loop
    (routing itself is CHAOS-3502's job; this only proves the intent is
    reachable). Was RED before the ``contracts_v2`` enum + this recognizer
    landed -- see git history for the failing form.
    """

    interpreted = await _interpreter().interpret(request_for(question))
    assert interpreted.intent.intent_id is QuestionIntentID.DISCOVERED_COHORT
    assert interpreted.intent.cardinality is Cardinality.ORGANIZATION_WIDE
    assert interpreted.intent.interpretation_reasons[0] != "recognizer.none"


@pytest.mark.parametrize(
    "question",
    [
        "Is the Payments team currently struggling?",
        "Is Nightfall capacity-constrained right now?",
    ],
)
@pytest.mark.asyncio
async def test_a_named_subject_never_routes_to_discovered_cohort(
    question: str,
) -> None:
    """CHAOS-3652 guardrail (team-lead condition, load-bearing): a question
    that names a subject -- even one lexically shaped exactly like a
    cohort-discovery question -- must never route to ``DISCOVERED_COHORT``.
    That would be exactly the "unresolved named subject widens to
    organization/cohort scope" shape Wave 3.2 forbids. Enforced lexically
    here (the recognizer requires ``mention_count == 0``); enforced
    structurally at the contract level by
    ``test_discovered_cohort_rejects_singular_cardinality`` /
    ``test_discovered_cohort_rejects_plural_cohort_cardinality`` in
    ``test_contracts_v2.py``.
    """

    interpreted = await _interpreter().interpret(request_for(question))
    assert interpreted.intent.intent_id is not QuestionIntentID.DISCOVERED_COHORT
    assert interpreted.mentions, "expected a named subject to be extracted"


@pytest.mark.parametrize(
    ("question", "expected"),
    [
        ("Which teams are currently struggling?", CohortDiscoveryFamily.TEAM_PRESSURE),
        ("Which squads are falling behind?", CohortDiscoveryFamily.TEAM_PRESSURE),
        (
            "Which projects appear capacity-constrained?",
            CohortDiscoveryFamily.PROJECT_CAPACITY,
        ),
        (
            "Which projects are unusually lightly loaded relative to demand?",
            CohortDiscoveryFamily.PROJECT_CAPACITY,
        ),
    ],
)
def test_classify_cohort_discovery_family_matches_the_exclusive_pairing(
    question: str, expected: CohortDiscoveryFamily
) -> None:
    """CHAOS-3689: an exclusive (subject-kind, judgment-kind) pairing --
    exactly the shape ``cohort.discovery`` already recognizes as
    ``DISCOVERED_COHORT`` -- classifies to the matching family.
    """

    assert classify_cohort_discovery_family(question) is expected


@pytest.mark.parametrize(
    "question",
    [
        # No anchor of either group at all.
        "What is the sky?",
        # Mixed subject: both TEAM and PROJECT anchors present.
        "Which teams and projects are struggling?",
        # Mixed judgment: both PRESSURE and CAPACITY anchors present.
        "Which teams are struggling and capacity-constrained?",
        # Cross-mismatch: FAMILY_CANDIDATE_KINDS cannot satisfy team+capacity
        # (TEAM_PRESSURE needs a pressure judgment) or project+pressure
        # (PROJECT_CAPACITY needs a project subject, TEAM_PRESSURE needs a
        # team subject).
        "Which teams are capacity-constrained?",
        "Which projects are struggling?",
        # GraphEntityKind.REPOSITORY/SERVICE are distinct from PROJECT and
        # no family maps to either -- unclassifiable, not folded into
        # PROJECT_CAPACITY.
        "Which repos are struggling?",
        "Which services are capacity-constrained?",
    ],
)
def test_classify_cohort_discovery_family_is_honestly_unclassifiable(
    question: str,
) -> None:
    """CHAOS-3689: every combination this round doesn't cover returns
    ``None`` rather than guessing -- the orchestrator's routing-branch gate
    treats ``None`` as "never call the graph seam", falling back to the
    legacy loop exactly like any other unclassified question.
    """

    assert classify_cohort_discovery_family(question) is None


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
# Definite descriptions of a body of work (CHAOS-3648)
# ---------------------------------------------------------------------------
#
# Every phrasing below is invented for the test. None of it is drawn from the
# frozen investigation corpus: a recall rule justified by "it makes a corpus
# case pass" is tuning, and would read as capability it does not have.


@pytest.mark.parametrize(
    ("question", "expected"),
    [
        # Lowercase name, non-kind head noun: both readings are emitted,
        # specific first, because English does not settle without a catalog
        # whether the head noun belongs to the name.
        ("What about the payroll migration?", ("payroll migration", "payroll")),
        ("how's the checkout redesign going", ("checkout redesign", "checkout")),
        (
            "why has the invoicing consolidation stalled?",
            ("invoicing consolidation", "invoicing"),
        ),
        # Two modifiers are as much a name as one.
        (
            "what happened to the mobile onboarding rewrite?",
            ("mobile onboarding rewrite", "mobile onboarding"),
        ),
        # Determiners other than "the" are definite too.
        ("is our billing rollout on track?", ("billing rollout", "billing")),
    ],
)
def test_a_definite_description_of_work_names_a_subject(
    question: str, expected: tuple[str, ...]
) -> None:
    assert untyped_name_candidates(question) == expected


def test_a_capitalized_head_noun_keeps_the_head_inside_the_name() -> None:
    """A capitalized head belongs to the proper name, so no second reading.

    "the Payroll Migration" is one name; "the payroll migration" is two
    readings. Capitalizing the head noun is the writer's own evidence that it
    belongs to the name, so the classifier reading is not offered.
    """

    assert untyped_name_candidates("What about the Payroll Migration?") == (
        "Payroll Migration",
    )


@pytest.mark.parametrize(
    "question",
    [
        # Nothing but quantificational, temporal, or contrastive modifiers:
        # they restrict by recency or contrast, never by identity.
        "What about the current work?",
        "how is the remaining work going",
        "What about the other migration?",
        "is the whole rollout done?",
        # No definite determiner, so no presupposed referent: this is a
        # description of an activity, not a reference to a named one.
        "we did a lot of work this week",
        # A determiner immediately followed by the head noun names nothing.
        "What about the rewrite?",
    ],
)
def test_a_description_without_a_name_mints_nothing(question: str) -> None:
    assert untyped_name_candidates(question) == ()
    assert extract_mentions(question, mint_id=sequential_ids()) == ()


def test_a_leading_non_naming_modifier_is_not_part_of_the_name() -> None:
    assert untyped_name_candidates("how is the current payroll migration going") == (
        "payroll migration",
        "payroll",
    )


def test_a_definite_description_never_mints_a_typed_mention() -> None:
    """The kind is unstated, so the mention must stay untyped.

    A wrong *typed* mention terminates a question that works today; a wrong
    untyped one only fails to match. That asymmetry is the whole reason this
    rule lives in the untyped path.
    """

    assert extract_mentions("What about the payroll migration?") == ()


def test_a_kind_noun_head_stays_with_the_typed_grammar() -> None:
    """ "the Payroll project" states its kind, so the typed grammar claims it."""

    question = "What about the Payroll project?"
    mentions = extract_mentions(question, mint_id=sequential_ids())
    assert [
        (mention.original_text_span, mention.requested_entity_kind.value)
        for mention in mentions
    ] == [("Payroll", "project")]
    claimed = [mention.normalized_lookup_text for mention in mentions]
    assert untyped_name_candidates(question, claimed) == ()


def test_definite_descriptions_are_ordered_by_position_among_bare_names() -> None:
    question = "is the payroll migration blocking Beacon?"
    assert untyped_name_candidates(question) == (
        "payroll migration",
        "payroll",
        "Beacon",
    )


def test_the_work_noun_vocabulary_stays_disjoint_from_the_kind_nouns() -> None:
    """A head noun that names a kind belongs to the typed grammar, not here.

    Structural rather than exemplary: it holds for every future addition to
    either list, where a test naming today's members would not.
    """

    kind_nouns = {noun for noun, _kind in _KIND_NOUNS}
    assert not _WORK_HEAD_NOUNS & kind_nouns
    assert not _WORK_HEAD_NOUNS & _NON_NAMING_MODIFIERS
    # Single tokens only: a multi-word entry would be a phrase, and a phrase
    # is how a corpus-specific string gets smuggled into a "generic" list.
    assert all(" " not in noun for noun in _WORK_HEAD_NOUNS | _NON_NAMING_MODIFIERS)


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


# ---------------------------------------------------------------------------
# organization_mention_spans (CHAOS-3574)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("question", "expected"),
    [
        # Trailing: "<Name> organization"/"org" -- the corpus case's own shape.
        ("What's the status of the Orbit organization?", frozenset({"orbit"})),
        ("Is the Nightfall Holdings org on track?", frozenset({"nightfall holdings"})),
        # Leading: "organization/org <Name>".
        (
            'What is organization "Orbit" doing?',
            frozenset({"orbit"}),
        ),
        # No capitalized/quoted name adjacent to the noun at all.
        ("What's the status of our organization?", frozenset()),
        ("What's the status of the organization's repositories?", frozenset()),
        ("Purge my organization's expired conversations.", frozenset()),
        # A bare name with no organization noun nearby is not in scope for
        # this recognizer -- it is still an ordinary untyped bare name.
        ("What is our DORA score?", frozenset()),
        ("What's the status of Zephyr?", frozenset()),
        ("How is Nightfall doing?", frozenset()),
        # CHAOS-3574 review round 2 (CONFIRMED): attributive/idiomatic uses --
        # "organization"/"org" modifies a FOLLOWING noun rather than being
        # named by the PRECEDING one. A closed, stated-not-hidden
        # continuation list, not an attempt at exhaustive NLP.
        ("What's the status of the Atlas organization chart?", frozenset()),
        ("Can you share the Atlas org structure?", frozenset()),
        ("Where's the Meridian organization diagram?", frozenset()),
        # Possessive attributive use is excluded the same way.
        ("What's the Atlas organization's status?", frozenset()),
        # CHAOS-3574 review round 3 (CONFIRMED): the LEADING form ("org/
        # organization <Name>") is idiomatic too when the captured name is
        # itself one of the closed continuation words, capitalized the way a
        # question naturally capitalizes it mid-sentence -- "Org Chart" reads
        # exactly like "the Atlas organization chart" backwards. `_NAME`
        # requires only a leading capital, not a real proper noun, so
        # "Chart"/"Structure" satisfy it exactly as "Orbit" does.
        ("What's on the Org Chart?", frozenset()),
        ("Can you show me the Organization Structure?", frozenset()),
        # Positive control: a real org name after the noun still matches --
        # the guard rejects the closed continuation words, not every name.
        ('What is organization "Orbit" doing?', frozenset({"orbit"})),
        ("Ask about org Meridian please", frozenset({"meridian"})),
        # Still fires when the noun is not followed by an idiom continuation,
        # even with trailing words after it.
        (
            "Is the Nightfall Holdings org on track this quarter?",
            frozenset({"nightfall holdings"}),
        ),
    ],
)
def test_organization_mention_spans(question: str, expected: frozenset[str]) -> None:
    assert organization_mention_spans(question) == expected


def test_organization_mention_spans_ignores_stop_word_only_spans() -> None:
    """ "Our Org" mints no span -- "Our" is a closed stop word, matching every
    other recognizer's treatment of "Our team is overburdened"."""

    assert organization_mention_spans("How is Our Org doing?") == frozenset()
