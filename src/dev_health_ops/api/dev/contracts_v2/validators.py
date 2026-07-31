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
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .frame import DevAnswerFrame
    from .narrative import DevNarrative

__all__ = [
    "ANSWERED_CONTENT_OUTCOMES",
    "PUBLIC_TEXT_FORBIDDEN_TOKENS",
    "scan_public_text",
    "validate_completion_denominator",
    "validate_narrative_fact_references",
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
