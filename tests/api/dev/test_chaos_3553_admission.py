"""CHAOS-3553: the QUA commit gate stops being a number the model wrote.

CHAOS-3539 swept the commit gate over 42 mention shapes at 8 repeats (336
rows, ``.remember/chaos-3539-sweep-data.jsonl``) and returned a clean null:
no confidence threshold separates a correct commit from a wrong one. AUC
among rows that actually committed is 0.617, and 0.72 is the MODAL confidence
for true and false positives alike. A floor cannot be the safety control,
because there is no floor.

What the same data shows DOES separate is structure. All 12 false positives
came from two spans -- "Meridian" and "the Meridian projects" -- each an
under-specified reference to a family of entities rather than to one. This
module is the RED-first proof of the replacement: a three-clause structural
admission predicate, each clause observed failing on its own.

The three clauses, and why each is here rather than tuned:

1. **The mention's own authorized slice holds exactly one candidate.** A span
   that matched several authorized entities is ambiguous BY CONSTRUCTION, and
   a model picking one of them is breaking a tie it has no evidence to break.
2. **The selection IS that one candidate.** Clause 1 without this admits a
   proposal that named something outside the single-candidate slice.
3. **The span identifies the entity, not its family.** Either the span IS the
   label, or the LABEL CORROBORATES the span -- by deriving it as an acronym,
   or by having at least two of its content tokens accounted for. See
   ``qua_promotion._STRUCTURALLY_DISTINGUISHING_TOKENS`` for why two is a
   structural floor rather than a tuned number, and the comment block beneath
   it for why a bare match-class check is NOT a corroboration.

The trap that makes clause 3 load-bearing, and that any slice-size-only rule
walks into: ``neg.C2`` ("the Meridian projects") has a slice of EXACTLY ONE
and produced 8 of the 12 observed false positives. Clause 1 admits it.

Two further shapes clause 3 must refuse, both found by adversarial review
after the first draft and both reproduced before being fixed -- a class-only
version of clause 3 admitted the first, and counting articles as content
admitted the second:

* an uncorroborated parenthetical QUALIFIER ("the Legacy project" onto
  ``Payments (Legacy)``) -- CHAOS-3289's incident, verbatim;
* a label article standing in for a distinguishing token ("The Platform" onto
  ``The Platform Team``, where the interpreter itself dropped "Team").
"""

from __future__ import annotations

import pytest

from dev_health_ops.api.dev.alias_matching import (
    SpanMatch,
    SpanMatchClass,
    classify_span_match,
)
from dev_health_ops.api.dev.contracts_v2.base import Cardinality
from dev_health_ops.api.dev.contracts_v2.question_understanding import QUAOutcome
from dev_health_ops.api.dev.qua_promotion import (
    QUA_COMMIT_MIN_CONFIDENCE,
    promotable_selection,
)
from dev_health_ops.api.dev.qua_shadow import (
    QUAShadowMentionAssessment,
    QUAShadowRecord,
    QUAShadowStatus,
)
from dev_health_ops.api.dev.scope_service import AuthorizedEntity, EntityKind

MENTION_ID = "3fa85f64-5717-4562-b3fc-2c963f66afa6"

#: The label at the centre of every observed false positive. Four authorized
#: entities in the sweep's world share its first token; only one carries the
#: rest of it.
MWA_LABEL = "Meridian Web Application (MWA)"


def _entity(
    label: str,
    *,
    span: str,
    canonical_id: str = "meridian/web-app",
    kind: EntityKind = EntityKind.PROJECT,
) -> AuthorizedEntity:
    """An entity carrying the span provenance the catalog would have stamped.

    Built through the production classifier rather than by hand-writing a
    ``SpanMatch``: a hand-authored provenance is exactly how a fixture starts
    asserting a class the real producer never emits.
    """

    return AuthorizedEntity(
        kind=kind,
        canonical_id=canonical_id,
        label=label,
        span_match=classify_span_match(span=span, label=label),
    )


def _record(
    *,
    authorized_slice: tuple[AuthorizedEntity, ...],
    selected: AuthorizedEntity | None,
    confidence: float = 0.92,
    outcome: QUAOutcome = QUAOutcome.RESOLVED,
) -> QUAShadowRecord:
    return QUAShadowRecord(
        status=QUAShadowStatus.EVALUATED,
        cardinality=Cardinality.SINGULAR,
        mentions=(
            QUAShadowMentionAssessment(
                mention_id=MENTION_ID,
                text_span="the Meridian projects",
                outcome=outcome,
                selected_entity=selected,
                candidate_entities=(selected,) if selected is not None else (),
                authorized_slice=authorized_slice,
                confidence=confidence,
            ),
        ),
    )


# --------------------------------------------------------------------------
# The pure layer: how a span relates to a label.
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("span", "label", "expected"),
    [
        # Exact label -- the user named the entity, all of it.
        ("Meridian Web Application (MWA)", MWA_LABEL, SpanMatchClass.EXACT_LABEL),
        ("meridian web application (mwa)", MWA_LABEL, SpanMatchClass.EXACT_LABEL),
        # Parenthetical alias.
        (
            "Ground Control",
            "Platform Reliability (Ground Control)",
            SpanMatchClass.ALIAS,
        ),
        (
            "Context Fabric",
            "Dev Health Agent Context Runtime (Context Fabric)",
            SpanMatchClass.ALIAS,
        ),
        # Acronym of an inner window -- CHAOS-3388's flagship.
        (
            "ACR",
            "Dev Health Agent Context Runtime (Context Fabric)",
            SpanMatchClass.ACRONYM,
        ),
        # A label may satisfy two classes at once, and the more specific one
        # wins. "MWA" is BOTH the acronym of "Meridian Web Application" and
        # the label's literal parenthetical, so it classifies ALIAS. Asserted
        # rather than left implicit: an earlier draft of this test expected
        # ACRONYM and the production classifier was the one that was right.
        # Admission is unaffected -- both classes name the entity outright.
        ("MWA", MWA_LABEL, SpanMatchClass.ALIAS),
        # An acronym with no parenthetical to collide with.
        ("PMS", "Platform Mobile Squad", SpanMatchClass.ACRONYM),
        # Everything else the catalog's LIKE would have returned.
        ("Meridian", MWA_LABEL, SpanMatchClass.SUBSTRING_PARTIAL),
        ("Web Application", MWA_LABEL, SpanMatchClass.SUBSTRING_PARTIAL),
    ],
)
def test_span_match_class_is_derived_from_span_and_label(
    span: str, label: str, expected: SpanMatchClass
) -> None:
    assert classify_span_match(span=span, label=label).match_class is expected


@pytest.mark.parametrize(
    ("span", "label", "expected"),
    [
        # The whole observed failure mode: one token of a four-token label.
        ("Meridian", MWA_LABEL, 1),
        # The positives the predicate must keep admitting.
        ("Web Application", MWA_LABEL, 2),
        ("Meridian Web Application", MWA_LABEL, 3),
        ("Mobile Squad", "Platform Mobile Squad", 2),
        ("Platform Reliability", "Platform Reliability (Ground Control)", 2),
        # A span word absent from the label contributes nothing.
        ("Meridian projects", MWA_LABEL, 1),
    ],
)
def test_label_token_coverage_counts_distinct_label_tokens(
    span: str, label: str, expected: int
) -> None:
    assert classify_span_match(span=span, label=label).label_tokens_covered == expected


def test_span_match_is_a_typed_artifact_not_a_bare_string() -> None:
    """The provenance must survive as structure, not as a label to re-parse.

    CHAOS-3422's lesson, quoted in ``_dedupe_preserving_rank``: an
    ``AuthorizedEntity`` carrying no match provenance forced a later layer to
    re-derive it, and the re-derivation was wrong. A typed artifact is what
    makes that re-derivation unnecessary.
    """

    match = classify_span_match(span="ACR", label="Agent Context Runtime")
    assert isinstance(match, SpanMatch)
    assert match.match_class is SpanMatchClass.ACRONYM
    assert isinstance(match.label_tokens_covered, int)


# --------------------------------------------------------------------------
# Clause 1: the slice must hold exactly one candidate.
# --------------------------------------------------------------------------


def test_clause_1_refuses_a_multi_candidate_slice() -> None:
    """``neg.C1`` ("Meridian"): 4 of the 12 observed false positives.

    An untyped bare org prefix matched four authorized entities. The evidence
    that the mention was under-specified is sitting in the slice; before this
    ticket nothing consulted it.
    """

    slice_ = tuple(
        _entity(label, span="Meridian", canonical_id=f"meridian/{n}")
        for n, label in enumerate(
            (MWA_LABEL, "Meridian API Gateway", "Meridian Atlas", "Meridian Sandbox")
        )
    )
    record = _record(authorized_slice=slice_, selected=slice_[0])

    assert promotable_selection(record, deterministic_declined=True) is None


def test_clause_1_admits_a_single_candidate_slice_that_also_clears_clause_3() -> None:
    entity = _entity(MWA_LABEL, span="Web Application")
    record = _record(authorized_slice=(entity,), selected=entity)

    promotion = promotable_selection(record, deterministic_declined=True)

    assert promotion is not None
    assert promotion.entity.canonical_id == "meridian/web-app"


def test_clause_1_refuses_an_empty_slice() -> None:
    """A mention past ``max_total_candidates`` gets an empty slice.

    ``_verify`` already refuses its indices; this asserts the predicate does
    not treat "nothing authorized" as "nothing ambiguous".
    """

    entity = _entity(MWA_LABEL, span="Web Application")
    record = _record(authorized_slice=(), selected=entity)

    assert promotable_selection(record, deterministic_declined=True) is None


# --------------------------------------------------------------------------
# Clause 2: the selection must BE the one candidate.
# --------------------------------------------------------------------------


def test_clause_2_refuses_a_selection_outside_its_own_single_candidate_slice() -> None:
    """Slice-of-one is only evidence about the entity that is IN the slice.

    Without this clause a proposal naming some other entity inherits the
    unambiguity of a slice it was never part of.
    """

    in_slice = _entity(MWA_LABEL, span="Web Application")
    elsewhere = _entity(
        "Meridian API Gateway", span="Web Application", canonical_id="meridian/api-gw"
    )
    record = _record(authorized_slice=(in_slice,), selected=elsewhere)

    assert promotable_selection(record, deterministic_declined=True) is None


# --------------------------------------------------------------------------
# Clause 3: the span must identify the entity, not its family.
# --------------------------------------------------------------------------


def test_clause_3_refuses_a_one_token_partial_in_a_slice_of_one() -> None:
    """``neg.C2`` ("the Meridian projects"): 8 of the 12 false positives.

    THE trap. Typed to ``project``, the span matched exactly one authorized
    project, so its slice size is 1 and clause 1 admits it. "Meridian" is the
    org prefix -- it names the family, and the entity's own distinguishing
    words ("Web Application") are absent from what the user said.
    """

    entity = _entity(MWA_LABEL, span="Meridian")
    record = _record(authorized_slice=(entity,), selected=entity)

    assert entity.span_match is not None
    assert entity.span_match.match_class is SpanMatchClass.SUBSTRING_PARTIAL
    assert entity.span_match.label_tokens_covered == 1
    assert promotable_selection(record, deterministic_declined=True) is None


@pytest.mark.parametrize(
    ("span", "label", "expected_class"),
    [
        ("MWA", MWA_LABEL, SpanMatchClass.ALIAS),
        ("PMS", "Platform Mobile Squad", SpanMatchClass.ACRONYM),
        (
            "ACR",
            "Dev Health Agent Context Runtime (Context Fabric)",
            SpanMatchClass.ACRONYM,
        ),
        (
            "Context Fabric",
            "Dev Health Agent Context Runtime (Context Fabric)",
            SpanMatchClass.ALIAS,
        ),
        (MWA_LABEL, MWA_LABEL, SpanMatchClass.EXACT_LABEL),
    ],
)
def test_clause_3_admits_a_span_the_label_itself_corroborates(
    span: str, label: str, expected_class: SpanMatchClass
) -> None:
    """The label derives the span, or the span covers enough of the label.

    This is the clause that keeps CHAOS-3525's literal acceptance -- "the ACR
    project" and "the MWA project" -- an auto-commit rather than a
    clarification round trip, which a coverage rule alone would refuse (an
    acronym covers ZERO label tokens, so it clears on the derivation arm).
    """

    entity = _entity(label, span=span)
    record = _record(authorized_slice=(entity,), selected=entity)

    assert entity.span_match is not None
    assert entity.span_match.match_class is expected_class
    assert promotable_selection(record, deterministic_declined=True) is not None


@pytest.mark.parametrize(
    ("span", "label"),
    [
        # The CHAOS-3289 incident, verbatim from alias_matching's docstring.
        ("Legacy", "Payments (Legacy)"),
        ("Archived", "Reports (Archived)"),
        ("Deprecated", "Checkout (Deprecated)"),
    ],
)
def test_an_uncorroborated_parenthetical_qualifier_never_commits(
    span: str, label: str
) -> None:
    """CHAOS-3289 must not re-open through the QUA door.

    ``alias_matching``'s docstring records the incident: an earlier revision
    let a unique literal-alias match commit outright, which auto-committed
    "the Legacy project" onto whichever entity happened to carry "(Legacy)" --
    answering about an entity the user never named. Nothing in the catalog
    schema distinguishes an alternate NAME from a QUALIFIER.

    A class-only clause 3 ("admit EXACT_LABEL, ALIAS and ACRONYM outright")
    re-opens it exactly: slice of one, class ALIAS, admitted. Found by
    adversarial review and by direct execution against a catalog holding
    ``Payments (Legacy)``, where "the Legacy project" committed while "the
    Payments project" -- the entity's real primary name -- was refused.

    The requirement is CORROBORATION, and a qualifier has none: "Legacy" is
    the acronym of nothing and covers one label token. "MWA" survives the same
    rule because "Meridian Web Application" derives it; "Context Fabric"
    survives because it covers two.
    """

    entity = _entity(label, span=span, canonical_id="acme/thing")
    record = _record(authorized_slice=(entity,), selected=entity, confidence=1.0)

    assert entity.span_match is not None
    assert entity.span_match.match_class is SpanMatchClass.ALIAS
    assert entity.span_match.is_acronym_of_primary_name is False
    assert entity.span_match.label_tokens_covered == 1
    assert promotable_selection(record, deterministic_declined=True) is None


def test_a_label_article_is_not_distinguishing_content() -> None:
    """An article must not supply the second "distinguishing" token.

    Reachable, not theoretical. For "What is the status of The Platform Team"
    the interpreter emits the span "The Platform" -- it keeps the capitalised
    article and drops the entity's own distinguishing token "Team". Counting
    ``the`` as covered content makes that span clear a two-token bound having
    named ONE word of the entity, and it commits whenever the span happens to
    match a single authorized entity.

    The defect is an asymmetry rather than a missing stopword list:
    ``question_interpreter`` strips a leading lowercase article from a span,
    so "the Platform Team" yields "Platform" while "The Platform Team" yields
    "The Platform". Coverage was measuring which of the two normalizations ran.
    """

    entity = _entity("The Platform Team", span="The Platform", canonical_id="acme/plat")
    record = _record(authorized_slice=(entity,), selected=entity, confidence=1.0)

    assert entity.span_match is not None
    assert entity.span_match.label_tokens_covered == 1
    assert promotable_selection(record, deterministic_declined=True) is None

    # The same span with the entity's real distinguishing token still commits,
    # so this is an article rule and not a blanket refusal of the shape.
    named = _entity("The Platform Team", span="Platform Team", canonical_id="acme/plat")
    assert named.span_match is not None
    assert named.span_match.label_tokens_covered == 2
    assert (
        promotable_selection(
            _record(authorized_slice=(named,), selected=named),
            deterministic_declined=True,
        )
        is not None
    )


def test_clause_3_admits_a_two_token_partial() -> None:
    """``pos.mwa.partial`` ("the Web Application project").

    Two tokens is the smallest span that can distinguish inside a family, and
    this is the positive that proves the clause is not simply "refuse every
    partial".
    """

    entity = _entity(MWA_LABEL, span="Web Application")
    record = _record(authorized_slice=(entity,), selected=entity)

    assert promotable_selection(record, deterministic_declined=True) is not None


def test_unclassified_provenance_fails_closed() -> None:
    """An entity that reached promotion without provenance must not commit.

    ``span_match`` is optional on ``AuthorizedEntity`` -- the catalog's
    non-search paths (``exact()``, the organization roster) mint entities with
    no span to classify. If one of those ever reaches this predicate the
    answer is refusal, not an assumed class.
    """

    entity = AuthorizedEntity(
        kind=EntityKind.PROJECT, canonical_id="meridian/web-app", label=MWA_LABEL
    )
    assert entity.span_match is None
    record = _record(authorized_slice=(entity,), selected=entity)

    assert promotable_selection(record, deterministic_declined=True) is None


@pytest.mark.parametrize(
    ("span", "label"),
    [
        # The round-2 repro: "LO" is the acronym of the QUALIFIER "Legacy
        # Operations", and of nothing the catalog asserts as a name.
        ("LO", "Payments (Legacy Operations)"),
        ("AB", "Checkout (Archived Beta)"),
        # An acronym of a parenthetical that IS a real alternate name is
        # refused too -- doubly derived is doubly derived, and refusing costs
        # a clarification round trip rather than a wrong subject.
        ("CF", "Dev Health Agent Context Runtime (Context Fabric)"),
    ],
)
def test_an_acronym_of_a_parenthetical_corroborates_nothing(
    span: str, label: str
) -> None:
    """Corroboration must come from the name the CATALOG asserts.

    The first version of the corroboration rule tested against
    ``alias_forms(label).acronyms``, which unions the primary name's acronyms
    with those of every parenthetical. A MULTI-WORD qualifier therefore
    corroborated itself: "LO" against "Payments (Legacy Operations)" was
    admitted on the strength of an acronym of "Legacy Operations", walking
    straight back into the CHAOS-3289 qualifier bug the rule had just closed
    for the single-word case ("Legacy"). Found by adversarial review round 2
    and reproduced before fixing.

    An acronym of a parenthetical is derived from a derived form. The catalog
    asserts the primary name and nothing else, so that is the only thing that
    can corroborate.
    """

    entity = _entity(label, span=span, canonical_id="acme/thing")
    record = _record(authorized_slice=(entity,), selected=entity, confidence=1.0)

    assert entity.span_match is not None
    assert entity.span_match.is_acronym_of_primary_name is False
    assert promotable_selection(record, deterministic_declined=True) is None


def test_an_article_only_label_can_corroborate_nothing() -> None:
    """A label with no content words must not derive its own initials.

    "TA" against "The An" was admitted through the acronym arm: the label's
    two articles supplied the two words an acronym window needs. Articles are
    dropped before deriving, so the primary name has no content and generates
    no acronyms. Round-2 edge case, reproduced before fixing.
    """

    entity = _entity("The An", span="TA", canonical_id="acme/degenerate")
    record = _record(authorized_slice=(entity,), selected=entity, confidence=1.0)

    assert entity.span_match is not None
    assert entity.span_match.is_acronym_of_primary_name is False
    assert entity.span_match.label_tokens_covered == 0
    assert promotable_selection(record, deterministic_declined=True) is None


def test_a_short_acronym_window_still_admits_which_is_a_known_residual() -> None:
    """CHARACTERIZATION, not an endorsement. This shape is NOT closed.

    ``acronym_candidates`` generates every contiguous window of >= 2 words, so
    a long label yields short acronyms -- the label below yields 28 windows, 7
    of them two letters. The interpreter does extract a two-letter mention
    ("What is the status of the AC project" gives the span "AC"), so a user
    meaning something the catalog does not hold can land on the one entity
    whose label happens to contain two consecutive words with those initials.

    Asserted so the behaviour is VISIBLE and any future change to it is
    deliberate, and so nobody reads the corpus test's "refuses all 12 observed
    false positives" as "refuses everything". See
    ``qua_promotion._structurally_admissible``'s generalization-limit
    paragraph for why it is not closed here: it is not a regression (the
    previous gate admitted it on confidence alone), and the obvious fix is a
    minimum acronym length, which is precisely the unmeasured tuned number
    CHAOS-3539 exists to stop introducing.

    If this shape is ever MEASURED as a false positive, this test is the one
    to invert, together with the clause that earns the change.
    """

    label = "Dev Health Agent Context Runtime Platform Services Team"
    entity = _entity(label, span="AC", canonical_id="acme/dh")
    record = _record(authorized_slice=(entity,), selected=entity)

    assert entity.span_match is not None
    assert entity.span_match.match_class is SpanMatchClass.ACRONYM
    assert entity.span_match.is_acronym_of_primary_name is True
    assert entity.span_match.label_tokens_covered == 0
    assert promotable_selection(record, deterministic_declined=True) is not None


# --------------------------------------------------------------------------
# The floor is demoted, and says so.
# --------------------------------------------------------------------------


def test_the_floor_is_a_coarse_sanity_bound_beneath_every_observed_positive() -> None:
    """0.6 sits beneath the lowest confidence any true positive carried.

    Kept only so a degenerate near-zero proposal is not treated as evidence.
    It is NOT the control that prevents a wrong commit -- CHAOS-3539 measured
    that no value of it is -- and nothing in this codebase may describe it as
    one.
    """

    assert QUA_COMMIT_MIN_CONFIDENCE == 0.6


def test_the_old_floor_no_longer_admits_the_meridian_false_positive() -> None:
    """The regression this ticket exists to close.

    8 of the 12 observed false positives cleared the previous 0.85 floor. The
    structural predicate refuses them at confidence 1.0, which is the point:
    the number was never the thing that could tell them apart.
    """

    entity = _entity(MWA_LABEL, span="Meridian")
    record = _record(authorized_slice=(entity,), selected=entity, confidence=1.0)

    assert promotable_selection(record, deterministic_declined=True) is None


def test_the_floor_still_rejects_beneath_the_sanity_bound() -> None:
    entity = _entity(MWA_LABEL, span="Web Application")
    record = _record(
        authorized_slice=(entity,),
        selected=entity,
        confidence=QUA_COMMIT_MIN_CONFIDENCE - 0.01,
    )

    assert promotable_selection(record, deterministic_declined=True) is None
