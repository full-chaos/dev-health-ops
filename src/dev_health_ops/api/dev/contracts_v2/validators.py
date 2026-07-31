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

CHAOS-3294 Codex adversarial-review hardening (post-merge) adds two more
guardrails that are not part of the original five but close counterexamples
the review reproduced against them:

(f) ``validate_no_answer_content_leaks`` — a "no content" outcome (see
    ``NO_ANSWER_OUTCOMES`` below) must carry *nothing* beyond the bare
    outcome: no completion, readiness, metrics, comparisons, relationship
    paths, evidence, source observations, or internal cross-references
    (``health_profile_refs``/``finding_refs``/``deficiency_refs``), and
    ``denied`` specifically must not disclose which subject was asked
    about. ``needs_clarification`` is deliberately **not** in
    ``NO_ANSWER_OUTCOMES``: unlike the five outcomes that project to a v1
    ``DevError`` (see ``compat.py``'s ``_ERROR_OUTCOME_CODES``),
    ``needs_clarification`` projects to a v1 ``DevAnswer`` with
    ``insufficient_evidence`` status and its frame may legitimately carry a
    disambiguation-relevant ``subject_ref`` (see
    ``compat._project_needs_clarification``) — only its answer *content*
    (sections/facts) is required to stay empty, which the pre-existing
    ``validate_outcome_consistency`` check already enforces.
(g) ``validate_narrative_frame_consistency`` — a battery of deterministic,
    narrow checks (numeral/percentage containment, readiness-word polarity,
    committed-subject-name presence, recommendation-keyword grounding) that
    reject a narrative that plainly contradicts its paired frame. This is
    **not** general semantic contradiction detection — arbitrary claims a
    narrative could make that aren't reducible to one of these four
    deterministic signals cannot be verified at the contract level. Full
    narrative/frame semantic consistency checking is owned by the TRD v2
    §11 layer-6 narrative-consistency validator, tracked as CHAOS-3297; this
    module only closes the subset that is mechanically checkable without a
    model in the loop.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .frame import DevAnswerFrame
    from .narrative import DevNarrative

__all__ = [
    "ANSWERED_CONTENT_OUTCOMES",
    "NO_ANSWER_OUTCOMES",
    "PUBLIC_TEXT_FORBIDDEN_TOKENS",
    "scan_public_text",
    "validate_completion_denominator",
    "validate_narrative_fact_references",
    "validate_narrative_frame_consistency",
    "validate_narrative_numeric_containment",
    "validate_narrative_readiness_claim",
    "validate_narrative_recommendation_claim",
    "validate_narrative_subject_claim",
    "validate_no_answer_content_leaks",
    "validate_no_internal_leakage",
    "validate_outcome_consistency",
    "validate_relationship_refs_within_frame",
    "validate_structural_closure",
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


def _public_copy_fields(frame: DevAnswerFrame) -> list[str]:
    values: list[str] = [frame.direct_answer]
    for section in frame.sections:
        values.append(section.title)
    for fact in frame.facts:
        values.append(fact.text)
    values.extend(frame.limitations)
    values.extend(frame.safe_follow_up_questions)
    if frame.readiness is not None:
        values.extend(frame.readiness.translated_user_reasons)
    return values


def validate_no_internal_leakage(frame: DevAnswerFrame) -> None:
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

#: Outcomes for which the server produced *no* answer whatsoever. This is a
#: strict subset of ``_EMPTY_CONTENT_OUTCOMES`` — it deliberately excludes
#: ``needs_clarification`` (see module docstring guardrail (f) for why) — and
#: is exactly the outcome set ``compat.py``'s ``_ERROR_OUTCOME_CODES`` maps to
#: a v1 ``DevError`` rather than a v1 ``DevAnswer``. Codex adversarial review
#: (CHAOS-3294): a ``denied`` frame validated with a subject reference, a
#: 3/4 completion rate, evidence, and source observations all still present
#: because the old guard only checked ``sections``/``facts``.
NO_ANSWER_OUTCOMES = frozenset(
    {"not_found", "temporarily_unavailable", "unsupported", "denied", "failed"}
)

#: Frame list/optional fields that must be empty for a ``NO_ANSWER_OUTCOMES``
#: outcome, beyond ``sections``/``facts`` (already covered above).
_NO_ANSWER_PROHIBITED_LIST_FIELDS: tuple[str, ...] = (
    "metrics",
    "comparisons",
    "relationship_paths",
    "evidence",
    "source_observations",
    "health_profile_refs",
    "finding_refs",
    "deficiency_refs",
)


def validate_outcome_consistency(frame: DevAnswerFrame) -> None:
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
    if outcome == "answered_with_gaps" and not frame.limitations:
        if frame.completion is None or frame.completion.calculable is not False:
            raise ValueError(
                "'answered_with_gaps' requires disclosed limitations or a "
                "non-calculable completion block"
            )


def validate_no_answer_content_leaks(frame: DevAnswerFrame) -> None:
    """(f) A no-content outcome (``NO_ANSWER_OUTCOMES``) must carry nothing.

    Closes the Codex-review counterexample: the pre-existing outcome-content
    check only inspected ``sections``/``facts``, so a ``denied`` (or
    ``not_found``/``unsupported``/``temporarily_unavailable``/``failed``)
    frame could still validate with a completion rate, readiness, metrics,
    comparisons, relationship paths, evidence, source observations, or
    internal cross-references intact — none of which any client should ever
    see for an outcome that means "the server produced no answer". ``denied``
    additionally may not disclose *which* subject was asked about (the
    outcome itself must not confirm or deny the subject's existence).
    """

    outcome = frame.public_outcome.value
    if outcome not in NO_ANSWER_OUTCOMES:
        return
    if frame.completion is not None:
        raise ValueError(f"public outcome {outcome!r} cannot carry a completion block")
    if frame.readiness is not None:
        raise ValueError(f"public outcome {outcome!r} cannot carry a readiness block")
    for field_name in _NO_ANSWER_PROHIBITED_LIST_FIELDS:
        if getattr(frame, field_name):
            raise ValueError(f"public outcome {outcome!r} cannot carry {field_name}")
    if outcome == "denied" and (
        frame.subject_ref is not None or frame.subject_set_ref is not None
    ):
        raise ValueError(
            "'denied' cannot disclose the subject identity that was asked about"
        )


# ---------------------------------------------------------------------------
# (c) completion rate without a full numerator/denominator
# ---------------------------------------------------------------------------


def validate_completion_denominator(frame: DevAnswerFrame) -> None:
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
    narrative: DevNarrative, frame: DevAnswerFrame
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
# (g) narrative text contradicting its paired frame (Codex adversarial
# review, CHAOS-3294). See module docstring guardrail (g) for the explicit
# scope statement: these are four narrow, deterministic checks, not general
# semantic contradiction detection. Full semantic consistency is the TRD v2
# §11 layer-6 narrative-consistency validator, CHAOS-3297.
# ---------------------------------------------------------------------------

#: Any bare numeral or percentage token in narrative prose, e.g. ``100``,
#: ``75%``, ``0.75``.
_NUMERIC_TOKEN_PATTERN = re.compile(r"\d+(?:\.\d+)?%?")

_READY_WORD_PATTERN = re.compile(r"\bready\b", re.IGNORECASE)
_NOT_READY_PHRASE_PATTERN = re.compile(r"\bnot[\s-]+ready\b", re.IGNORECASE)
_TOKEN_SPLIT_PATTERN = re.compile(r"[^a-z0-9]+")
_RECOMMENDATION_CLAIM_PATTERN = re.compile(
    r"\brecommend(?:s|ed|ation|ations)?\b", re.IGNORECASE
)


def _numeric_value(raw_token: str) -> float:
    return float(raw_token[:-1]) if raw_token.endswith("%") else float(raw_token)


def _frame_numeric_value_set(frame: DevAnswerFrame) -> set[float]:
    """Every numeral the frame itself renders, in the units it renders them.

    A completion ``rate`` of ``0.75`` is added both as ``0.75`` (fraction
    form) and ``75`` (percent form) so a narrative may correctly say either
    "0.75" or "75%" — the guard only rejects a number the frame never
    rendered in *any* of its own forms, e.g. "100%" when the frame's own
    rate is 75%.
    """

    values: set[float] = set()
    for match in _NUMERIC_TOKEN_PATTERN.finditer(frame.direct_answer):
        values.add(_numeric_value(match.group()))
    for fact in frame.facts:
        for match in _NUMERIC_TOKEN_PATTERN.finditer(fact.text):
            values.add(_numeric_value(match.group()))
    for text in (*frame.limitations, *frame.safe_follow_up_questions):
        for match in _NUMERIC_TOKEN_PATTERN.finditer(text):
            values.add(_numeric_value(match.group()))
    if frame.readiness is not None:
        for text in frame.readiness.translated_user_reasons:
            for match in _NUMERIC_TOKEN_PATTERN.finditer(text):
                values.add(_numeric_value(match.group()))
    completion = frame.completion
    if completion is not None:
        if completion.numerator is not None:
            values.add(float(completion.numerator))
        if completion.denominator is not None:
            values.add(float(completion.denominator))
        if completion.rate is not None:
            values.add(round(completion.rate, 6))
            values.add(round(completion.rate * 100, 6))
    for point in frame.comparisons:
        values.add(round(point.current_value, 6))
        if point.comparison_value is not None:
            values.add(round(point.comparison_value, 6))
    return values


def _value_in(haystack: set[float], value: float, *, tolerance: float = 1e-6) -> bool:
    return any(abs(value - candidate) <= tolerance for candidate in haystack)


def validate_narrative_numeric_containment(
    narrative: DevNarrative, frame: DevAnswerFrame
) -> None:
    """(g.1) Reject a narrative numeral/percentage absent from the frame's own values.

    Deterministic: extracts every bare number/percent token from the
    narrative body and requires each to match (within floating-point
    tolerance) a number the frame itself renders somewhere — its own
    ``direct_answer``, fact text, limitations, follow-ups, readiness
    reasons, completion numerator/denominator/rate, or comparison values.
    Closes the counterexample where a narrative claimed "100% complete"
    against a frame whose completion rate was 3/4 (75%).
    """

    offenders = sorted(
        {
            match.group()
            for match in _NUMERIC_TOKEN_PATTERN.finditer(narrative.body)
            if not _value_in(
                _frame_numeric_value_set(frame), _numeric_value(match.group())
            )
        }
    )
    if offenders:
        raise ValueError(
            f"narrative cites number(s) {offenders} that do not appear in the "
            "frame's own facts, completion, or comparisons"
        )


def validate_narrative_readiness_claim(
    narrative: DevNarrative, frame: DevAnswerFrame
) -> None:
    """(g.2) Reject a narrative readiness claim that contradicts the frame.

    Closes the counterexample where a narrative said "ready" against a frame
    whose readiness state was ``not_ready``. Deterministic because
    ``DevReadinessBlock.state`` is a closed three-value enum.
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


def validate_narrative_subject_claim(
    narrative: DevNarrative, frame: DevAnswerFrame
) -> None:
    """(g.3) Reject a narrative that never names the frame's committed subject.

    Deliberately a loose, false-positive-averse presence check (any
    ``len >= 3`` token split out of ``subject_ref.display_label``, not an
    exact-string match) rather than a strict containment rule, so ordinary
    prose that abbreviates or reformats a display label ("dev-health" for
    "full-chaos/dev-health") still passes. It still catches a narrative that
    names a wholly different subject.
    """

    subject = frame.subject_ref
    if subject is None:
        return
    tokens = [
        token
        for token in _TOKEN_SPLIT_PATTERN.split(subject.display_label.lower())
        if len(token) >= 3
    ]
    if not tokens:
        return
    lowered_body = narrative.body.lower()
    if not any(token in lowered_body for token in tokens):
        raise ValueError(
            "narrative body never names the frame's committed subject "
            f"({subject.display_label!r})"
        )


def validate_narrative_recommendation_claim(
    narrative: DevNarrative, frame: DevAnswerFrame
) -> None:
    """(g.4) Reject narrative recommendation language ungrounded in the frame.

    If the narrative reads as making a recommendation ("recommend(s|ed)",
    "recommendation(s)"), the frame must actually carry at least one fact of
    ``kind == "recommendation"`` to ground it.
    """

    if not _RECOMMENDATION_CLAIM_PATTERN.search(narrative.body):
        return
    if not any(fact.kind == "recommendation" for fact in frame.facts):
        raise ValueError(
            "narrative reads as a recommendation but the frame has no "
            "recommendation fact to ground it"
        )


def validate_narrative_frame_consistency(
    narrative: DevNarrative, frame: DevAnswerFrame
) -> None:
    """(g) Run all four narrative/frame contradiction checks (g.1-g.4).

    See the module docstring guardrail (g) and each sub-check's own
    docstring for exactly what is — and is not — covered: this is bounded,
    deterministic pattern matching, not semantic narrative understanding.
    """

    validate_narrative_numeric_containment(narrative, frame)
    validate_narrative_readiness_claim(narrative, frame)
    validate_narrative_subject_claim(narrative, frame)
    validate_narrative_recommendation_claim(narrative, frame)


# ---------------------------------------------------------------------------
# (e) relationship references outside the frame
# ---------------------------------------------------------------------------


def validate_relationship_refs_within_frame(frame: DevAnswerFrame) -> None:
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


def validate_structural_closure(frame: DevAnswerFrame) -> None:
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
