"""Server-side semantic validators for the Wave 3.1 answer frame/narrative pair.

Mirrors how ``dev_health_ops.api.dev.contracts.DevAnswer.validate_answer_invariants``
works today (a server-side function checked against the wire shape), but
each guardrail named in CHAOS-3294's acceptance criteria is broken out into
its own top-level, independently importable function rather than folded
into one big method body. That split is deliberate: ``DevAnswerFrame``'s own
``model_validator`` calls these through the *module object*
(``_validators.validate_xxx(self)``), so each one can be disabled
independently in a test via ``monkeypatch.setattr(validators,
"validate_xxx", lambda *_: None)`` to prove, per the CHAOS-3294 acceptance
clause, that removing that one guard — and only that one — flips the
corresponding fixture from rejected to accepted while every other guard's
fixture is still rejected. A plain inline ``@model_validator`` method could
not be disabled this way: pydantic-core captures a direct reference to the
decorated function at class-build time, so monkeypatching the bound method
after the fact does not change already-compiled validation behavior.

The five guardrails named in the acceptance criteria:

(a) ``validate_no_internal_leakage``       — internal outcome leakage
(b) ``validate_outcome_consistency``       — contradictory public outcome vs. frame content
(c) ``validate_completion_denominator``    — completion rate without a full numerator/denominator
(d) ``validate_narrative_fact_references`` — narrative referencing missing fact IDs
(e) ``validate_relationship_refs_within_frame`` — relationship refs outside the frame

Two more guardrails close counterexamples that adversarial review
reproduced against those five:

(f) ``validate_no_answer_projection`` — a "no content" outcome (see
    ``NO_ANSWER_OUTCOMES``) is projected through a **total field allowlist**
    rather than scrubbed against a denylist. See "The no-answer allowlist
    projection" below; this is the round-2 structural replacement for the
    round-1 denylist (``validate_no_answer_content_leaks``, retained as an
    alias so the guard keeps its documented name).
(g) ``validate_narrative_frame_consistency`` — deterministic checks that
    bind narrative prose to the *specific* frame facts the narrative
    declares it is narrating. See "Narrative fact binding" below. This is
    still **not** general semantic contradiction detection: an arbitrary
    claim that is not reducible to a number, a readiness word, the
    subject's canonical identity, or a recommendation reference cannot be
    verified without a model in the loop. That layer is the TRD v2 §11
    layer-6 narrative-consistency validator, tracked as CHAOS-3297.
(h) ``validate_frame_grounding`` — F10 (CHAOS-3297 stack #3, ratified
    2026-08-02): every frame fact AND every metric block carries
    signer-minted evidence or an explicit no-evidence classification
    (``fact.disclosures`` / ``DevMetricRefV2.evidence_classification``).
    The grounding floor ``contracts_v2/validators.py`` was missing before
    stack #3.

Where the code lives
--------------------

The no-answer projection machinery — the policy enum, the per-model registry,
the canonical copy tables, and ``validate_no_answer_projection`` itself — is
defined in the leaf module ``no_answer_policy`` and re-exported here, so the
guard names keep this module as their documented home and the mutation tests
keep patching one place. That split is structural: ``frame`` and ``embedded``
both register a policy at import time while this module needs the frame's own
type to annotate its guards, which made the registration side of the graph a
genuine import cycle. See that module's docstring.

The no-answer allowlist projection
----------------------------------

Round-1 hardening scrubbed a fixed *denylist* of prohibited fields off a
no-answer frame. Adversarial review round 2 walked straight around it via
the fields the denylist did not name (``direct_answer``, ``conflicts``,
``limitations``, ``safe_follow_up_questions``, and a whole ``narrative``),
each of which is a free-form, producer-authored disclosure channel: a
``denied`` frame disclosed a private project's existence, its completion
percentage, and a cross-provider conflict about it, and the v1 projector
re-emitted that copy verbatim as ``DevError.safe_message``.

The replacement inverts the polarity. Every field of ``DevAnswerFrame`` and
``DevAnswerV2`` carries an explicit classification in
``NO_ANSWER_FRAME_FIELD_POLICY`` / ``NO_ANSWER_ANSWER_FIELD_POLICY``:

``ABSENT``
    Must be ``None`` or empty. Nothing about the subject survives here.
``CANONICAL``
    Must equal, exactly, the server-owned constant for this outcome
    (``CANONICAL_NO_ANSWER_COPY`` / ``CANONICAL_NO_ANSWER_DISPLAY_LABELS``).
    Producer-authored free text is never *reused*, only *replaced*.
``CLOSED_VOCABULARY``
    Every string this field reaches must be a member of a server-owned closed
    set registered alongside the policy (a ``StrEnum``'s values, a ``Literal``'s
    single value, the plan registry). The producer picks *from* the vocabulary;
    it cannot contribute *to* it.
``IDENTIFIER``
    Every string this field reaches must be a whitespace-free identifier
    token (``_IDENTIFIER_TOKEN_PATTERN``, the ``OpaqueID`` shape). This is a
    **runtime** predicate on the value, not a declaration about the type, so
    classifying a free-text field as ``IDENTIFIER`` does not create an
    escape hatch: the moment it carries prose, validation fails.
``NON_TEXT``
    Reaches no strings at all (timestamps, numbers, booleans).
``SELF_VALIDATED``
    A nested contract that carries its own registered policy, applied
    recursively.

``assert_no_answer_policy_is_total`` is called at **import time** by
``frame.py`` and ``answer.py``. A field added to either model without a
classification raises ``RuntimeError`` on import — the package will not
load, so the enumeration cannot silently fall behind the models. The
matching test derives the same enumeration from the model definitions and
additionally proves every ``ABSENT`` field is individually rejected.

Round 3 replaced the two remaining ``IDENTIFIER`` disclosure channels. The
``IDENTIFIER`` predicate constrains a token's *shape*, and a subject-derived
name is a perfectly well-shaped token: review put ``"private/Nightfall"`` in
``coverage.unavailable_required_sources`` and in ``versions.plan_id`` on a
``denied`` frame and both validated and serialized. The fix is polarity
again — from "any well-shaped token" to "a member of this set":

* ``coverage`` is now ``DevCoverageV2`` (``embedded.py``), whose source lists
  are the closed ``base.SourceClass`` enum, reached through ``SELF_VALIDATED``
  so its counts and timestamp are separately classified ``NON_TEXT``;
* ``versions`` is ``ABSENT`` — a no-answer outcome carries no provenance
  block at all. ``DevFrameVersions`` is seven free version strings plus a plan
  ID; constraining each one is a weaker statement than not emitting the block,
  and a denial's provenance is recoverable from ``run_id`` server-side. The
  frame requires ``versions`` for every outcome that *does* carry content
  (``validate_versions_presence``), so this is not a hole in the answered path;
* ``schema_version`` and ``public_outcome`` are ``CLOSED_VOCABULARY`` rather
  than ``IDENTIFIER``, which is what they always meant.

Round 4 closed the residual round 3 left, and corrected the claim round 3
made about it. The claim — "only ``frame_id``/``run_id`` remain" — was scoped
to ``DevAnswerFrame``, and the matching test asserted it against the frame's
policy table. The answer envelope one level out was never enumerated, so
``DevAnswerV2.answer_id`` and ``conversation_id`` accepted
``"private/Nightfall"`` on a denied answer. A partition test certifies
exactly what it enumerates, and that one enumerated a table rather than the
object graph the claim was about.

Every correlation handle is now ``base.ServerHandle`` — a canonical
hyphenated UUID, pinned to what ``models/dev_persistence.py`` already mints
and what ``orchestrator_persistence`` already parses back — applied
uniformly across all outcomes, since a run ID is chosen before the outcome is
known. ``IDENTIFIER`` survives as a policy class, but no field of the
no-answer envelope uses it: the surviving identifier cells are handles, and
``test_round4_every_identifier_on_a_denied_envelope_is_an_opaque_handle``
derives the reachable model set from the policy rather than assuming it.

Narrative fact binding
----------------------

Round-1 narrative checks matched tokens against the frame *globally*, which
adversarial review round 2 bypassed three ways: an unrelated comparison
value of ``100`` legitimized "100% complete" against a 75% completion block;
a substring match let "billing-health" satisfy a frame committed to
"full-chaos/dev-health"; and recommendation prose was grounded by the mere
existence of *some* recommendation fact anywhere in the frame. Each check is
now bound to the specific facts the narrative declares:

* numeric tokens are admitted **per sentence**, never from a pool shared
  across the body — see "Per-sentence numeric admission" below;
* a sentence that makes a completion claim may only cite a percentage the
  completion block itself supports;
* subject mentions are matched as a contiguous canonical token sequence,
  never as a substring;
* recommendation prose requires the narrative to reference the specific
  recommendation fact by ID.

Per-sentence numeric admission
------------------------------

Round 2 narrowed the numeric pool to the referenced facts, the frame's own
canonical copy, and the completion block — but it was still a *pool*, unioned
once and offered to every sentence. Round 3 showed that both directions of
that were wrong at once. It over-accepted: the completion block's numerator
and denominator legitimized any sentence citing those integers, so a frame
with a 3/4 completion block accepted the narrative claim "there are 4 open
security incidents", a number about something else entirely. And it
over-rejected: a subject genuinely named ``project-42`` could not be named,
because 42 appeared nowhere in the facts.

Both are the same bug — a number was admitted or refused by *membership in a
global set* rather than by *the sentence's own grounding*. There is now no
pool. Each sentence admits exactly:

(a) the numerals in the text of the facts the narrative declares it is
    narrating (``_narrative_bound_fact_ids``);
(b) the completion block's values — but only in a sentence that actually
    makes a completion claim, i.e. one that references the completion
    section (``_COMPLETION_CLAIM_PATTERN``); elsewhere those integers are
    ordinary numbers with no standing;
(c) the numerals in the committed subject's canonical identity forms, which
    are server-committed, not producer-chosen;
(d) the values of a comparison whose label that same sentence names
    (unchanged from round 2, and already per-sentence).

Note what is *not* in the list: ``direct_answer``, ``limitations``,
``safe_follow_up_questions`` and the readiness reasons. Those were in the
round-2 pool, and they are frame-level free text — a number appearing in one
of them grounds nothing about the sentence citing it. A narrative that wants
to restate a number must reference the fact carrying it.

Round 4 replaced (b)'s gate. Round 3 decided "does this sentence claim
completion?" with a five-word regex, and that regex refused truthful
narration of the frame's own block: "has made 75% progress" and "has passed
3 of 4 required checks" were both rejected while "finished" passed. An
over-rejecting guard is a defect, not a safe default — and no fixture caught
it because every fixture used a word from the list. Lengthening the list is
the same defect with a longer list, so admission no longer consults
vocabulary at all. It is decided by **how the sentence writes the number**:

* the completion proportion (rate and its complement) is admitted wherever it
  carries its unit — ``75%``, ``25%``, or the bare decimals ``0.75``/``0.25``.
  A proportion with a unit is not a plausible count, so it needs no gate;
  a bare ``75`` is not admitted by this rule;
* the counts (numerator, denominator) are admitted only in a sentence that
  renders the block's own ratio — ``3 of 4``, ``3/4``, ``3 out of 4``. Mere
  co-occurrence is not enough: "4 open security incidents and 3 unresolved
  alerts" contains both and grounds neither.

A bare completion count elsewhere stays inadmissible — nothing distinguishes
"3 required checks passed" from "3 open incidents" — and the escape is the
same one stated above: reference the fact carrying it.

Vocabulary survives in exactly one place, ``_COMPLETION_CLAIM_STEMS``, used
only to decide whether a *percentage* must additionally be checked against
the completion block. That direction is safe: a missing word makes the
stricter check apply less often, never opens an admission channel — the
opposite of its round-3 use, where a missing word caused a refusal.

Round 5 corrected both edges of that rule.

*Polarity.* Admission asks whether a number is *a* rendering of the completion
block, which cannot distinguish "75% complete" from "25% complete" against a
3/4 block — both cite a real value of it, one of them the wrong one. Round 4
admitted the rate and its complement interchangeably, so "has completed 25%"
validated. The claim stems are now split into ``_COMPLETED_CLAIM_STEMS`` and
``_REMAINING_CLAIM_STEMS``: a completed claim may cite only the rate, a
remaining claim only the complement, checked over percentages **and** bare
decimals (round 4's strict check ran only over percent tokens, which is how
the decimal form of the same falsehood escaped). Enforced only where it is
decidable — at a 1/2 block the two coincide, and a sentence claiming both
directions is ambiguous prose rather than a contradiction; rejecting either
would be over-rejection again.

*Typography.* Truthful narration was failing on punctuation: ``75 % complete``
(a space before ``%``, which the token pattern did not span) and ``3-of-4``
(the ratio pattern allowed only whitespace and ``/``). A percentage may now
carry an optional space before ``%``, and hyphens and spaces are
interchangeable around a ratio connector. Anything past that — polarity
carried by sentence structure rather than vocabulary, non-English phrasing, a
proportion bound to something other than the completion block by grammar this
layer cannot parse — is the CHAOS-3297 layer-6 residual.
"""

from __future__ import annotations

import re
from collections.abc import Sequence
from typing import TYPE_CHECKING

from .no_answer_policy import (
    CANONICAL_NO_ANSWER_COPY,
    CANONICAL_NO_ANSWER_DISPLAY_LABELS,
    CANONICAL_NO_ANSWER_REMEDIATION,
    NO_ANSWER_ANSWER_FIELD_POLICY,
    NO_ANSWER_FRAME_FIELD_POLICY,
    NO_ANSWER_OUTCOMES,
    SOURCE_CLASS_VOCABULARY,
    NoAnswerFieldPolicy,
    assert_no_answer_policy_is_total,
    literal_vocabulary,
    register_no_answer_policy,
    validate_no_answer_content_leaks,
    validate_no_answer_projection,
)
from .plan import PLAN_REGISTRY

if TYPE_CHECKING:
    from . import frame as _frame
    from . import narrative as _narrative

__all__ = [
    "ANSWERED_CONTENT_OUTCOMES",
    "CANONICAL_NO_ANSWER_COPY",
    "CANONICAL_NO_ANSWER_DISPLAY_LABELS",
    "CANONICAL_NO_ANSWER_REMEDIATION",
    "NO_ANSWER_ANSWER_FIELD_POLICY",
    "NO_ANSWER_FRAME_FIELD_POLICY",
    "NO_ANSWER_OUTCOMES",
    "PUBLIC_TEXT_FORBIDDEN_TOKENS",
    "SOURCE_CLASS_VOCABULARY",
    "NoAnswerFieldPolicy",
    "assert_no_answer_policy_is_total",
    "literal_vocabulary",
    "register_no_answer_policy",
    "scan_public_text",
    "validate_completion_denominator",
    "validate_frame_grounding",
    "validate_narrative_fact_references",
    "validate_narrative_frame_consistency",
    "validate_narrative_numeric_containment",
    "validate_narrative_readiness_claim",
    "validate_narrative_recommendation_claim",
    "validate_narrative_subject_claim",
    "validate_no_answer_content_leaks",
    "validate_no_answer_projection",
    "validate_no_internal_leakage",
    "validate_outcome_consistency",
    "validate_plan_registry_membership",
    "validate_relationship_refs_within_frame",
    "validate_structural_closure",
    "validate_versions_presence",
]

# ---------------------------------------------------------------------------
# (a) internal outcome leakage
# ---------------------------------------------------------------------------

#: Internal codes that must never appear in a public-facing copy field, per
#: CHAOS-3294: "Internal forbidden_or_not_found, scope_forbidden, rule IDs,
#: and source/provider reason enums do not enter user-facing display
#: fields." These are exact snake_case wire tokens, not ordinary English
#: words, so scanning for them as substrings does not produce false
#: positives against normal prose.
PUBLIC_TEXT_FORBIDDEN_TOKENS: frozenset[str] = frozenset(
    {
        # ScopeResolutionOutcome / resolution ledger internal reason codes
        "forbidden_or_not_found",
        "scope_forbidden",
        "no_authorized_match",
        "catalog_unavailable",
        "unsupported_kind",
        "ambiguous_candidates",
        "exact_match",
        # dev_source_observation.v1 internal state tokens
        "unauthorized_or_not_visible",
        "available_unknown",
        "not_measured",
        # DevError internal codes
        "provider_contract_violation",
        "internal_error",
        "answer_validation_failed",
    }
)

#: Internal rule/plan/query identifiers look like ``lowercase.words.vN``
#: (e.g. ``status.entity.v2``, ``health_rule.completion.v3``). Prose never
#: legitimately contains a dotted, versioned token like this.
_VERSIONED_ID_PATTERN = re.compile(r"\b[a-z][a-z0-9_]*(?:\.[a-z][a-z0-9_]*)+\.v\d+\b")


def scan_public_text(value: str) -> list[str]:
    """Return the forbidden tokens/patterns found in one public copy string."""

    hits: list[str] = []
    lowered = value.lower()
    for token in PUBLIC_TEXT_FORBIDDEN_TOKENS:
        if token in lowered:
            hits.append(token)
    if _VERSIONED_ID_PATTERN.search(lowered):
        hits.append("versioned-rule-id-pattern")
    return hits


def _public_copy_fields(frame: _frame.DevAnswerFrame) -> list[str]:
    values: list[str] = [frame.direct_answer]
    for section in frame.sections:
        values.append(section.title)
    for fact in frame.facts:
        values.append(fact.text)
    for conflict in frame.conflicts:
        values.append(conflict.summary)
    values.extend(frame.limitations)
    values.extend(frame.safe_follow_up_questions)
    if frame.readiness is not None:
        values.extend(frame.readiness.translated_user_reasons)
    return values


def validate_no_internal_leakage(frame: _frame.DevAnswerFrame) -> None:
    """(a) Reject internal outcome/rule/reason tokens in public copy fields."""

    for value in _public_copy_fields(frame):
        hits = scan_public_text(value)
        if hits:
            raise ValueError(
                f"public copy field leaks internal token(s) {sorted(hits)}: {value!r}"
            )


# ---------------------------------------------------------------------------
# (b) contradictory public outcome vs. frame content
# ---------------------------------------------------------------------------

#: Outcomes for which no answer content may be present — nothing useful was
#: produced, so sections/facts/completion/readiness must all be empty.
_EMPTY_CONTENT_OUTCOMES = frozenset(
    {
        "not_found",
        "temporarily_unavailable",
        "unsupported",
        "denied",
        "failed",
        "needs_clarification",
    }
)

#: Outcomes representing a genuine, server-owned answer.
ANSWERED_CONTENT_OUTCOMES = frozenset({"answered", "answered_with_gaps"})


def validate_outcome_consistency(frame: _frame.DevAnswerFrame) -> None:
    """(b) Reject a public outcome that contradicts the frame's own content."""

    outcome = frame.public_outcome.value
    has_content = bool(frame.sections) or bool(frame.facts)
    if outcome in _EMPTY_CONTENT_OUTCOMES and has_content:
        raise ValueError(
            f"public outcome {outcome!r} cannot carry answer sections/facts"
        )
    if outcome in ANSWERED_CONTENT_OUTCOMES and not has_content:
        raise ValueError(
            f"public outcome {outcome!r} requires answer sections and facts"
        )
    # CHAOS-3325: clarification_candidates is ABSENT-classified for the five
    # true no-answer outcomes (no_answer_policy.NO_ANSWER_FRAME_FIELD_POLICY),
    # but 'answered'/'answered_with_gaps' are not in NO_ANSWER_OUTCOMES either
    # (see that module's docstring), so this clause is what forbids it there
    # -- only 'needs_clarification' may carry it.
    if outcome in ANSWERED_CONTENT_OUTCOMES and frame.clarification_candidates:
        raise ValueError(
            f"public outcome {outcome!r} cannot carry clarification_candidates; "
            "only 'needs_clarification' may"
        )
    if outcome == "answered":
        if frame.limitations:
            raise ValueError(
                "'answered' cannot carry limitations; use answered_with_gaps"
            )
        if frame.completion is not None and frame.completion.calculable is False:
            raise ValueError(
                "'answered' cannot carry a non-calculable completion block; "
                "use answered_with_gaps"
            )
        # CHAOS-3297 flags gap (ratified 2026-08-02): a disclosure bars
        # 'answered' but does not by itself satisfy 'answered_with_gaps' --
        # a disclosed fact still requires an explanatory limitation or a
        # non-calculable completion block (checked below), so a builder
        # emitting a disclosure must emit both the disclosure and the
        # limitation that explains it.
        disclosed_facts = [fact.fact_id for fact in frame.facts if fact.disclosures]
        if disclosed_facts:
            raise ValueError(
                "'answered' cannot carry a fact disclosure (stale/uncertain/"
                f"conflicting/untrusted_source) on fact(s) {sorted(disclosed_facts)}; "
                "use answered_with_gaps"
            )
    if outcome == "answered_with_gaps" and not frame.limitations:
        if frame.completion is None or frame.completion.calculable is not False:
            raise ValueError(
                "'answered_with_gaps' requires disclosed limitations or a "
                "non-calculable completion block"
            )


def validate_frame_grounding(frame: _frame.DevAnswerFrame) -> None:
    """F10 (CHAOS-3297 stack #3, ratified 2026-08-02): every frame fact AND
    every metric block carries signer-minted evidence or an explicit
    no-evidence classification. Mirrors ``answer_validator.
    _answer_has_material_grounding``'s v1 shape (a grounding floor at the
    contract layer, not merely documented) applied here at the per-fact/
    per-metric granularity F10's own text specifies, rather than v1's
    whole-answer "something somewhere is grounded" check.

    ``fact.disclosures`` (CHAOS-3297 flags gap, ratified 2026-08-02) is the
    "explicit no-evidence classification" channel for a
    ``DevAnswerFact`` -- no new field is needed: a fact whose evidence
    the server does not trust enough to cite (stale/uncertain/conflicting/
    untrusted_source) still satisfies F10 by disclosing exactly why it has
    none, the same way ``DeficiencyFinding.evidence_classification``
    satisfies its own F10 requirement one layer down. A fact with BOTH
    empty ``evidence_ref_ids`` and empty ``disclosures`` is the one
    combination F10 forbids: neither real evidence nor an honest
    explanation for its absence.

    ``DevMetricRefV2.evidence_classification`` (CHAOS-3297 F10 metric half,
    ratified 2026-08-02, option (a) over scoping the requirement away in
    validator logic) closes the metric half of the SAME rule:
    ``wrap_legacy_answer_as_frame`` sets ``legacy_v1_unminted``
    unconditionally on every v1-sourced metric (that path's evidence is
    always scrubbed by ``production_runtime.py``'s ``query_metric.v1``
    tool), and the v2 investigation-plan path's ``_wire_metric_content``
    mints real evidence directly. ``DevMetricRefV2`` already enforces
    evidence XOR classification via its own ``model_validator`` -- this
    frame-level re-check is defense in depth (the SAME posture
    ``validate_frame_semantics`` takes for structural closure before its
    other guards), not the only enforcement point: a metric that reached
    ``frame.metrics`` via ``model_copy`` (which never reruns validators --
    see ``builtin_steps.py``'s construct-then-mint pattern) could otherwise
    carry an un-revalidated, silently invalid combination.
    """

    ungrounded_facts = [
        fact.fact_id
        for fact in frame.facts
        if not fact.evidence_ref_ids and not fact.disclosures
    ]
    if ungrounded_facts:
        raise ValueError(
            "F10: fact(s) "
            f"{sorted(ungrounded_facts)} carry neither evidence_ref_ids nor "
            "a disclosure -- every fact requires signer-minted evidence or "
            "an explicit no-evidence classification"
        )
    ungrounded_metrics = [
        metric.metric_ref_id
        for metric in frame.metrics
        if not metric.evidence_ref_ids and metric.evidence_classification is None
    ]
    if ungrounded_metrics:
        raise ValueError(
            "F10: metric(s) "
            f"{sorted(ungrounded_metrics)} carry neither evidence_ref_ids "
            "nor an evidence_classification -- every metric requires "
            "signer-minted evidence or an explicit no-evidence classification"
        )
    overgrounded_metrics = [
        metric.metric_ref_id
        for metric in frame.metrics
        if metric.evidence_ref_ids and metric.evidence_classification is not None
    ]
    if overgrounded_metrics:
        raise ValueError(
            "F10: metric(s) "
            f"{sorted(overgrounded_metrics)} carry BOTH evidence_ref_ids and "
            "an evidence_classification -- the classification exists only "
            "for the no-evidence case"
        )


def validate_versions_presence(frame: _frame.DevAnswerFrame) -> None:
    """Provenance is optional only for the outcomes that carry no content.

    ``DevAnswerFrame.versions`` is ``ABSENT`` under the no-answer projection
    (see the module docstring), which requires the field to be optional on
    the model. This is the other half of that: every outcome that is *not* a
    no-answer outcome must carry the provenance block, so making it optional
    did not quietly make it droppable from an answered frame.
    """

    if frame.public_outcome.value in NO_ANSWER_OUTCOMES:
        return
    if frame.versions is None:
        raise ValueError(
            f"public outcome {frame.public_outcome.value!r} requires a versions "
            "provenance block; only a no-answer outcome may omit it"
        )


def validate_plan_registry_membership(frame: _frame.DevAnswerFrame) -> None:
    """Every ``versions`` block names a real, registered plan (CHAOS-3297 Codex
    review MEDIUM #3).

    ``DevFrameVersions.plan_id``'s type (``PlanRegistryID`` =
    ``PlatformVersionToken``) only enforces the dotted, versioned *grammar* --
    membership of ``contracts_v2.plan.PLAN_REGISTRY`` is the stronger, closed
    *semantic* check (see ``plan.py``'s own docstring on the distinction), and
    before this guard it was applied only inside the no-answer projection's
    own totality checks, never here. A frame reaches this check whenever
    ``versions`` is present at all -- every outcome that is not a no-answer
    outcome (``validate_versions_presence`` already requires the block there),
    including ``needs_clarification`` -- so a caller cannot attach provenance
    naming a plan that never ran, whether by typo or by reusing an
    unregistered compatibility marker.
    """

    versions = frame.versions
    if versions is None:
        return
    if versions.plan_id not in PLAN_REGISTRY:
        raise ValueError(
            f"plan id {versions.plan_id!r} is not a member of "
            "contracts_v2.plan.PLAN_REGISTRY"
        )


# ---------------------------------------------------------------------------
# (c) completion rate without a full numerator/denominator
# ---------------------------------------------------------------------------


def validate_completion_denominator(frame: _frame.DevAnswerFrame) -> None:
    """(c) Completion rate cannot exist without numerator, denominator, rule, calculable=true."""

    completion = frame.completion
    if completion is None:
        return
    numerator = completion.numerator
    denominator = completion.denominator
    has_numerator = numerator is not None
    has_denominator = denominator is not None
    if completion.calculable:
        if numerator is None or denominator is None:
            raise ValueError(
                "calculable completion requires both numerator and denominator"
            )
        if denominator == 0:
            raise ValueError(
                "calculable completion cannot divide by a zero denominator"
            )
        if completion.rule_id is None or completion.rule_version is None:
            raise ValueError("calculable completion requires a rule id and version")
        if completion.rate is None:
            raise ValueError("calculable completion requires a rate")
        expected = numerator / denominator
        if abs(expected - completion.rate) > 1e-9:
            raise ValueError("completion rate does not match numerator/denominator")
    else:
        # Never inferred: a non-calculable completion must not carry a rate,
        # even if a numerator/denominator happen to be present.
        if completion.rate is not None:
            raise ValueError(
                "a non-calculable completion cannot carry an inferred rate"
            )
        if has_numerator != has_denominator:
            raise ValueError(
                "completion numerator and denominator must both be present or both absent"
            )


# ---------------------------------------------------------------------------
# (d) narrative referencing missing fact IDs
# ---------------------------------------------------------------------------


def validate_narrative_fact_references(
    narrative: _narrative.DevNarrative, frame: _frame.DevAnswerFrame
) -> None:
    """(d) Reject a narrative that references a fact or section ID absent from the frame."""

    if narrative.frame_id != frame.frame_id:
        raise ValueError("narrative frame_id does not match the supplied frame")
    known_facts = {fact.fact_id for fact in frame.facts}
    known_sections = {section.section_id for section in frame.sections}
    unknown_facts = set(narrative.referenced_fact_ids) - known_facts
    if unknown_facts:
        raise ValueError(
            f"narrative references unknown fact IDs: {sorted(unknown_facts)}"
        )
    unknown_sections = set(narrative.referenced_section_ids) - known_sections
    if unknown_sections:
        raise ValueError(
            f"narrative references unknown section IDs: {sorted(unknown_sections)}"
        )


# ---------------------------------------------------------------------------
# (g) narrative prose bound to the specific frame facts it narrates. See the
# module docstring's "Narrative fact binding" section for the three round-2
# bypasses this replaced and for what remains out of contract-level reach
# (CHAOS-3297, the layer-6 narrative-consistency validator).
# ---------------------------------------------------------------------------

#: Any bare numeral or percentage token in prose, e.g. ``100``, ``75%``,
#: ``0.75``.
_NUMERIC_TOKEN_PATTERN = re.compile(r"\d+(?:\.\d+)?(?:\s*%)?")
#: A percentage specifically — the form a completion claim takes.
_PERCENT_TOKEN_PATTERN = re.compile(r"(\d+(?:\.\d+)?)\s*%")
_SENTENCE_SPLIT_PATTERN = re.compile(r"(?<=[.!?])\s+")
_WORD_TOKEN_PATTERN = re.compile(r"[a-z0-9]+")
_READY_WORD_PATTERN = re.compile(r"\bready\b", re.IGNORECASE)
_NOT_READY_PHRASE_PATTERN = re.compile(r"\bnot[\s-]+ready\b", re.IGNORECASE)
_RECOMMENDATION_CLAIM_PATTERN = re.compile(
    r"\brecommend(?:s|ed|ation|ations)?\b", re.IGNORECASE
)
#: The **explicit** completion-claim grammar. This is the one place textual
#: detection survives, and it is used only to decide whether a *percentage* in
#: a sentence must be checked against the completion block — never to decide
#: whether a number is admitted (see "Per-sentence numeric admission"). The
#: asymmetry is what makes a generous list safe here: widening this vocabulary
#: makes the percentage check apply to *more* sentences, so a missing word can
#: only ever weaken a check, never open an admission channel.
#: Prose that claims the **completed** share. A proportion cited in one of
#: these sentences must be the completion rate, never its complement.
_COMPLETED_CLAIM_STEMS = (
    "complete",
    "completes",
    "completed",
    "completion",
    "done",
    "finish",
    "finishes",
    "finished",
    "progress",
    "progressed",
    "pass",
    "passes",
    "passed",
    "close",
    "closes",
    "closed",
    "deliver",
    "delivers",
    "delivered",
    "ship",
    "ships",
    "shipped",
)

#: Prose that claims the **remaining** share — the complement. Round 4 folded
#: these in with the completed stems, which lost the polarity: "has completed
#: 25%" validated against a 3/4 block because 25 is a legitimate rendering of
#: *something* about that block. It is the wrong something.
#: ``\bcomplete\b`` does not match "incomplete" (no word boundary before the
#: ``c``), so the two lists stay disjoint on the words that matter.
_REMAINING_CLAIM_STEMS = (
    "remaining",
    "remains",
    "remain",
    "outstanding",
    "left",
    "unfinished",
    "incomplete",
    "undone",
    "todo",
)

_COMPLETED_CLAIM_PATTERN = re.compile(
    r"\b(?:" + "|".join(_COMPLETED_CLAIM_STEMS) + r")\b", re.IGNORECASE
)
_REMAINING_CLAIM_PATTERN = re.compile(
    r"\b(?:" + "|".join(_REMAINING_CLAIM_STEMS) + r")\b", re.IGNORECASE
)
#: Either polarity — the sentences to which the "percentage must be supported"
#: rule applies. Retained under its round-3 name because that rule is unchanged.
_COMPLETION_CLAIM_STEMS = _COMPLETED_CLAIM_STEMS + _REMAINING_CLAIM_STEMS
_COMPLETION_CLAIM_PATTERN = re.compile(
    r"\b(?:" + "|".join(_COMPLETION_CLAIM_STEMS) + r")\b", re.IGNORECASE
)

#: Ordinary typography for the block's own ratio: ``3 of 4``, ``3/4``,
#: ``3 out of 4``, ``3 of the 4``, ``3-of-4``. Hyphens and spaces are
#: interchangeable around the connector — a hyphenated ratio is the same claim
#: as a spaced one, and round 4 rejected it purely on punctuation.
_RATIO_SEPARATOR = r"[-\s]*"
_COMPLETION_RATIO_TEMPLATE = (
    r"\b{numerator}"
    + _RATIO_SEPARATOR
    + r"(?:/|of"
    + _RATIO_SEPARATOR
    + r"|out"
    + _RATIO_SEPARATOR
    + r"of"
    + _RATIO_SEPARATOR
    + r")(?:the"
    + _RATIO_SEPARATOR
    + r")?{denominator}\b"
)


def _is_percent_token(raw_token: str) -> bool:
    return raw_token.rstrip().endswith("%")


def _numeric_value(raw_token: str) -> float:
    token = raw_token.strip()
    if token.endswith("%"):
        token = token[:-1].strip()
    return float(token)


def _value_in(haystack: set[float], value: float, *, tolerance: float = 1e-6) -> bool:
    return any(abs(value - candidate) <= tolerance for candidate in haystack)


def _word_tokens(text: str) -> list[str]:
    return _WORD_TOKEN_PATTERN.findall(text.lower())


def _contains_token_sequence(haystack: Sequence[str], needle: Sequence[str]) -> bool:
    """True when ``needle`` occurs as a contiguous run of whole tokens.

    Token-sequence containment, never substring containment: "billing-health"
    tokenizes to ``["billing", "health"]``, which does not contain the
    contiguous run ``["dev", "health"]``, so it can no longer satisfy a frame
    committed to "full-chaos/dev-health".
    """

    if not needle:
        return False
    span = len(needle)
    return any(
        list(haystack[index : index + span]) == list(needle)
        for index in range(len(haystack) - span + 1)
    )


def _narrative_bound_fact_ids(
    narrative: _narrative.DevNarrative, frame: _frame.DevAnswerFrame
) -> set[str]:
    """Fact IDs the narrative declared it is narrating.

    Direct ``referenced_fact_ids`` plus the facts of any section the
    narrative referenced — a section reference is a declaration that the
    narrative covers that section's facts.
    """

    bound = set(narrative.referenced_fact_ids)
    referenced_sections = set(narrative.referenced_section_ids)
    for section in frame.sections:
        if section.section_id in referenced_sections:
            bound.update(section.fact_ids)
    return bound


def _completion_percent_values(frame: _frame.DevAnswerFrame) -> set[float]:
    """The completion proportions, as a sentence would write them with ``%``.

    Both the rate and its complement: "75% complete" and "25% remaining" are
    equally truthful renderings of the same block.
    """

    completion = frame.completion
    if completion is None or completion.rate is None:
        return set()
    return {
        round(completion.rate * 100, 6),
        round((1 - completion.rate) * 100, 6),
    }


def _completion_decimal_values(frame: _frame.DevAnswerFrame) -> set[float]:
    """The same two proportions written as bare decimals ("0.75", "0.25")."""

    completion = frame.completion
    if completion is None or completion.rate is None:
        return set()
    return {round(completion.rate, 6), round(1 - completion.rate, 6)}


def _completion_count_values(frame: _frame.DevAnswerFrame) -> set[float]:
    """The raw numerator and denominator."""

    completion = frame.completion
    values: set[float] = set()
    if completion is None:
        return values
    if completion.numerator is not None:
        values.add(float(completion.numerator))
    if completion.denominator is not None:
        values.add(float(completion.denominator))
    return values


def _completion_polarity_error(
    sentence: str, raw_token: str, value: float, frame: _frame.DevAnswerFrame
) -> str | None:
    """Reject a completion proportion cited with the wrong polarity.

    Admission (above) only asks whether a number is *a* rendering of the
    completion block. It cannot tell "75% complete" from "25% complete",
    because both cite a real value of a 3/4 block — one of them is simply the
    wrong one. Round 4 stopped there, so "has completed 25%" validated.

    Polarity is decided by which vocabulary the sentence uses, and only when
    the value belongs to exactly one side: at rate 0.5 the two coincide and
    there is nothing to get wrong, and a sentence claiming both directions
    ("25% remaining of the work completed") is ambiguous prose rather than a
    contradiction, so neither is rejected.
    """

    rate = frame.completion.rate if frame.completion is not None else None
    if rate is None:
        return None
    scale = 100 if _is_percent_token(raw_token) else 1
    is_rate = abs(value - round(rate * scale, 6)) <= 1e-6
    is_complement = abs(value - round((1 - rate) * scale, 6)) <= 1e-6
    if is_rate == is_complement:  # neither, or the two coincide at 50%
        return None

    claims_completed = bool(_COMPLETED_CLAIM_PATTERN.search(sentence))
    claims_remaining = bool(_REMAINING_CLAIM_PATTERN.search(sentence))
    if claims_completed and not claims_remaining and is_complement:
        return (
            f"narrative cites {raw_token.strip()} as completed, but that is the "
            "share of the completion block still remaining"
        )
    if claims_remaining and not claims_completed and is_rate:
        return (
            f"narrative cites {raw_token.strip()} as remaining, but that is the "
            "share of the completion block already completed"
        )
    return None


def _sentence_cites_completion_ratio(
    sentence: str, frame: _frame.DevAnswerFrame
) -> bool:
    """True when the sentence renders the completion block's own ratio.

    A bare completion count is genuinely ambiguous — nothing distinguishes
    "3 required checks passed" from "3 open incidents" — so a count is
    admitted only where the sentence writes the ratio the block actually has
    ("3 of 4", "3/4", "3 out of 4"). Co-occurrence is deliberately *not*
    enough: "4 open security incidents and 3 unresolved alerts" contains both
    integers and grounds neither.
    """

    completion = frame.completion
    if completion is None:
        return False
    if completion.numerator is None or completion.denominator is None:
        return False
    pattern = _COMPLETION_RATIO_TEMPLATE.format(
        numerator=re.escape(str(completion.numerator)),
        denominator=re.escape(str(completion.denominator)),
    )
    return re.search(pattern, sentence, re.IGNORECASE) is not None


def _numeric_values_in(texts: Sequence[str]) -> set[float]:
    return {
        _numeric_value(match.group())
        for text in texts
        for match in _NUMERIC_TOKEN_PATTERN.finditer(text)
    }


def _referenced_fact_values(
    narrative: _narrative.DevNarrative, frame: _frame.DevAnswerFrame
) -> set[float]:
    """(a) Numerals in the text of the facts the narrative declares it narrates.

    Only the referenced facts — not ``direct_answer``, ``limitations``,
    ``safe_follow_up_questions`` or the readiness reasons, which are
    frame-level free text that grounds nothing about the citing sentence. See
    the module docstring's "Per-sentence numeric admission".
    """

    bound_fact_ids = _narrative_bound_fact_ids(narrative, frame)
    return _numeric_values_in(
        [fact.text for fact in frame.facts if fact.fact_id in bound_fact_ids]
    )


def _subject_identity_values(frame: _frame.DevAnswerFrame) -> set[float]:
    """(c) Numerals belonging to the committed subject's own identity.

    A subject legitimately named ``project-42`` must be nameable. These are
    server-committed identity tokens, not producer-chosen figures, so they
    carry no numeric claim — admitting them removes the round-3
    over-rejection without widening what counts as a grounded number.
    """

    subject = frame.subject_ref
    if subject is None:
        return set()
    return _numeric_values_in([subject.display_label, subject.entity_id])


def _sentence_comparison_values(
    sentence_tokens: Sequence[str], frame: _frame.DevAnswerFrame
) -> set[float]:
    values: set[float] = set()
    for point in frame.comparisons:
        label_tokens = _word_tokens(point.label)
        if label_tokens and _contains_token_sequence(sentence_tokens, label_tokens):
            values.add(round(point.current_value, 6))
            if point.comparison_value is not None:
                values.add(round(point.comparison_value, 6))
    return values


def validate_narrative_numeric_containment(
    narrative: _narrative.DevNarrative, frame: _frame.DevAnswerFrame
) -> None:
    """(g.1) Bind every narrative numeral to that sentence's own grounding.

    There is no numeric pool shared across the body, and — since round 4 — no
    keyword gate on *admission* either. A number is admitted by how the
    sentence writes it, not by whether the sentence happens to contain a word
    from a vocabulary list. Per sentence, a numeral is admitted when it is:

    (a) a numeral in the text of a fact the narrative references;
    (b) a numeral in the committed subject's canonical identity;
    (c) a value of a comparison whose label that sentence names;
    (d) a completion proportion written with its unit — ``75%`` or ``25%``
        against a 3/4 block — or written as a bare decimal (``0.75``,
        ``0.25``), which is not a plausible count;
    (e) the completion numerator or denominator, but *only* in a sentence
        that renders the block's own ratio (``3 of 4``, ``3/4``).

    A bare completion count in any other position is not admitted: nothing
    distinguishes "3 required checks passed" from "3 open incidents", so a
    narrative wanting to cite one must reference the fact that carries it.
    See the module docstring's "Per-sentence numeric admission".

    On top of containment, a sentence that makes a completion claim (the
    explicit ``_COMPLETION_CLAIM_STEMS`` grammar) may only cite a percentage
    the completion block supports or a comparison it names, and may not cite
    one at all when there is no calculable completion block. That check is the
    only remaining use of completion vocabulary, and over-inclusion in it is
    safe by construction — it can only make the check apply more widely.
    """

    grounded_values = _referenced_fact_values(narrative, frame) | (
        _subject_identity_values(frame)
    )
    percent_values = _completion_percent_values(frame)
    decimal_values = _completion_decimal_values(frame)
    count_values = _completion_count_values(frame)
    calculable = frame.completion is not None and frame.completion.calculable
    for sentence in _SENTENCE_SPLIT_PATTERN.split(narrative.body):
        sentence_tokens = _word_tokens(sentence)
        comparison_values = _sentence_comparison_values(sentence_tokens, frame)
        anywhere = grounded_values | comparison_values
        if _sentence_cites_completion_ratio(sentence, frame):
            anywhere = anywhere | count_values
        offenders: set[str] = set()
        for match in _NUMERIC_TOKEN_PATTERN.finditer(sentence):
            raw_token = match.group()
            value = _numeric_value(raw_token)
            unit_bound = (
                percent_values if _is_percent_token(raw_token) else decimal_values
            )
            admitted_as_proportion = _value_in(unit_bound, value)
            if admitted_as_proportion:
                # Admitted as a completion proportion — so it must be the
                # proportion this sentence actually claims.
                polarity_error = _completion_polarity_error(
                    sentence, raw_token, value, frame
                )
                if polarity_error is not None:
                    raise ValueError(polarity_error)
            if _value_in(anywhere, value) or admitted_as_proportion:
                continue
            offenders.add(raw_token)
        if offenders:
            raise ValueError(
                f"narrative sentence cites number(s) {sorted(offenders)} that no "
                "fact it references, no comparison it names, and no completion "
                "proportion or ratio it renders supports"
            )
        if not _COMPLETION_CLAIM_PATTERN.search(sentence):
            continue
        for match in _PERCENT_TOKEN_PATTERN.finditer(sentence):
            claimed = float(match.group(1))
            if not calculable:
                raise ValueError(
                    f"narrative claims {claimed}% completion but the frame "
                    "carries no calculable completion block"
                )
            if not _value_in(percent_values, claimed) and not _value_in(
                comparison_values, claimed
            ):
                raise ValueError(
                    f"narrative claims {claimed}% completion, which the frame's "
                    "own completion block does not support"
                )


def validate_narrative_readiness_claim(
    narrative: _narrative.DevNarrative, frame: _frame.DevAnswerFrame
) -> None:
    """(g.2) Reject a narrative readiness claim that contradicts the frame.

    Deterministic because ``DevReadinessBlock.state`` is a closed
    three-value enum.
    """

    if frame.readiness is None:
        return
    state = frame.readiness.state
    body_without_not_ready = _NOT_READY_PHRASE_PATTERN.sub("", narrative.body)
    claims_ready = bool(_READY_WORD_PATTERN.search(body_without_not_ready))
    claims_not_ready = bool(_NOT_READY_PHRASE_PATTERN.search(narrative.body))
    if state == "ready" and claims_not_ready:
        raise ValueError(
            "narrative claims not-ready but the frame's readiness is 'ready'"
        )
    if state != "ready" and claims_ready:
        raise ValueError(
            f"narrative claims ready but the frame's readiness is {state!r}"
        )


def _subject_identity_forms(display_label: str, entity_id: str) -> list[list[str]]:
    """The canonical token sequences that count as naming this subject.

    The full label, its last path segment (the ordinary shorthand: "dev-health"
    for "full-chaos/dev-health"), and the entity ID. Each must appear as a
    *contiguous* token run, so a different subject that merely shares a token
    ("billing-health") does not satisfy it.
    """

    forms: list[list[str]] = []
    for candidate in (
        _word_tokens(display_label),
        _word_tokens(display_label.rsplit("/", 1)[-1]),
        _word_tokens(entity_id),
    ):
        if candidate and candidate not in forms:
            forms.append(candidate)
    return forms


def validate_narrative_subject_claim(
    narrative: _narrative.DevNarrative, frame: _frame.DevAnswerFrame
) -> None:
    """(g.3) Require the narrative to name the frame's committed subject.

    Canonical identity, not substring: at least one of the subject's
    canonical token sequences must occur contiguously in the narrative body.
    A narrative that names only some *other* subject sharing a single token
    is rejected.
    """

    subject = frame.subject_ref
    if subject is None:
        return
    forms = _subject_identity_forms(subject.display_label, subject.entity_id)
    if not forms:
        return
    body_tokens = _word_tokens(narrative.body)
    if not any(_contains_token_sequence(body_tokens, form) for form in forms):
        raise ValueError(
            "narrative body never names the frame's committed subject "
            f"({subject.display_label!r}) by its canonical identity"
        )


def validate_narrative_recommendation_claim(
    narrative: _narrative.DevNarrative, frame: _frame.DevAnswerFrame
) -> None:
    """(g.4) Bind recommendation prose to a specific referenced recommendation fact.

    The existence of *some* recommendation fact somewhere in the frame is
    not grounding — that is what let a narrative recommend one thing while
    the frame recommended another. The narrative must name the recommendation
    fact it is narrating in ``referenced_fact_ids`` directly (a section
    reference does not stand in for it).
    """

    if not _RECOMMENDATION_CLAIM_PATTERN.search(narrative.body):
        return
    referenced = set(narrative.referenced_fact_ids)
    grounded = any(
        fact.kind == "recommendation" and fact.fact_id in referenced
        for fact in frame.facts
    )
    if not grounded:
        raise ValueError(
            "narrative reads as a recommendation but references no "
            "recommendation fact of the frame to ground it"
        )


def validate_narrative_frame_consistency(
    narrative: _narrative.DevNarrative, frame: _frame.DevAnswerFrame
) -> None:
    """(g) Run all four narrative/frame binding checks (g.1-g.4)."""

    validate_narrative_numeric_containment(narrative, frame)
    validate_narrative_readiness_claim(narrative, frame)
    validate_narrative_subject_claim(narrative, frame)
    validate_narrative_recommendation_claim(narrative, frame)


# ---------------------------------------------------------------------------
# (e) relationship references outside the frame
# ---------------------------------------------------------------------------


def validate_relationship_refs_within_frame(frame: _frame.DevAnswerFrame) -> None:
    """(e) Reject a fact/readiness reference to a relationship or fact outside the frame."""

    known_relationship_ids = {path.path_id for path in frame.relationship_paths}
    known_fact_ids = {fact.fact_id for fact in frame.facts}
    for fact in frame.facts:
        unknown_paths = set(fact.relationship_path_ids) - known_relationship_ids
        if unknown_paths:
            raise ValueError(
                f"fact {fact.fact_id!r} references relationship path(s) not present "
                f"in the frame: {sorted(unknown_paths)}"
            )
    if frame.readiness is not None:
        unknown_blocking = set(frame.readiness.blocking_fact_ids) - known_fact_ids
        if unknown_blocking:
            raise ValueError(
                f"readiness references unknown blocking fact IDs: {sorted(unknown_blocking)}"
            )
    if frame.subject_ref is not None and frame.relationship_paths:
        reachable = {frame.subject_ref.entity_id}
        changed = True
        while changed:
            changed = False
            for path in frame.relationship_paths:
                if (
                    path.source_entity_id in reachable
                    and path.target_entity_id not in reachable
                ):
                    reachable.add(path.target_entity_id)
                    changed = True
        for path in frame.relationship_paths:
            if path.source_entity_id not in reachable:
                raise ValueError(
                    "relationship path does not chain back to the frame's committed "
                    "subject (a relationship reference outside the frame)"
                )


# ---------------------------------------------------------------------------
# Structural closure (baseline wire integrity, not one of the five named
# guardrails, but required for the frame to be internally coherent).
# ---------------------------------------------------------------------------


def validate_structural_closure(frame: _frame.DevAnswerFrame) -> None:
    fact_ids = [fact.fact_id for fact in frame.facts]
    if len(fact_ids) != len(set(fact_ids)):
        raise ValueError("fact IDs must be unique")
    known_facts = set(fact_ids)
    for section in frame.sections:
        if not set(section.fact_ids) <= known_facts:
            raise ValueError(
                f"section {section.section_id!r} references unknown fact IDs"
            )
    known_evidence = {item.evidence_ref_id for item in frame.evidence}
    for fact in frame.facts:
        if not set(fact.evidence_ref_ids) <= known_evidence:
            raise ValueError(f"fact {fact.fact_id!r} references unknown evidence IDs")
