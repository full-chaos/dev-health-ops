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
``IDENTIFIER``
    Every string this field reaches must be a whitespace-free identifier
    token (``_IDENTIFIER_TOKEN_PATTERN``, the ``OpaqueID`` shape). This is a
    **runtime** predicate on the value, not a declaration about the type, so
    classifying a free-text field as ``IDENTIFIER`` does not create an
    escape hatch: the moment it carries prose, validation fails.
``NON_TEXT``
    Reaches no strings at all (timestamps, numbers, booleans).
``SELF_VALIDATED``
    A nested v2 contract that carries its own registered policy, applied
    recursively.

``assert_no_answer_policy_is_total`` is called at **import time** by
``frame.py`` and ``answer.py``. A field added to either model without a
classification raises ``RuntimeError`` on import — the package will not
load, so the enumeration cannot silently fall behind the models. The
matching test derives the same enumeration from the model definitions and
additionally proves every ``ABSENT`` field is individually rejected.

Narrative fact binding
----------------------

Round-1 narrative checks matched tokens against the frame *globally*, which
adversarial review round 2 bypassed three ways: an unrelated comparison
value of ``100`` legitimized "100% complete" against a 75% completion block;
a substring match let "billing-health" satisfy a frame committed to
"full-chaos/dev-health"; and recommendation prose was grounded by the mere
existence of *some* recommendation fact anywhere in the frame. Each check is
now bound to the specific facts the narrative declares:

* numeric tokens are drawn only from the referenced facts' own text (plus
  server-owned canonical copy and the completion block), never from the
  whole frame; comparison values are admitted per sentence, and only in a
  sentence that names that comparison's label;
* a sentence that makes a completion claim may only cite a percentage the
  completion block itself supports;
* subject mentions are matched as a contiguous canonical token sequence,
  never as a substring;
* recommendation prose requires the narrative to reference the specific
  recommendation fact by ID.
"""

from __future__ import annotations

import re
from collections.abc import Iterator, Mapping, Sequence
from enum import StrEnum
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel

if TYPE_CHECKING:
    from .frame import DevAnswerFrame
    from .narrative import DevNarrative

__all__ = [
    "ANSWERED_CONTENT_OUTCOMES",
    "CANONICAL_NO_ANSWER_COPY",
    "CANONICAL_NO_ANSWER_DISPLAY_LABELS",
    "CANONICAL_NO_ANSWER_REMEDIATION",
    "NO_ANSWER_ANSWER_FIELD_POLICY",
    "NO_ANSWER_FRAME_FIELD_POLICY",
    "NO_ANSWER_OUTCOMES",
    "PUBLIC_TEXT_FORBIDDEN_TOKENS",
    "NoAnswerFieldPolicy",
    "assert_no_answer_policy_is_total",
    "register_no_answer_policy",
    "scan_public_text",
    "validate_completion_denominator",
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
    for conflict in frame.conflicts:
        values.append(conflict.summary)
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
#: ``needs_clarification`` — and is exactly the outcome set ``compat.py``'s
#: ``_ERROR_OUTCOME_CODES`` maps to a v1 ``DevError`` rather than a v1
#: ``DevAnswer``. ``needs_clarification`` projects to a v1 ``DevAnswer`` with
#: ``insufficient_evidence`` status and its frame may legitimately carry a
#: disambiguation-relevant ``subject_ref`` (see
#: ``compat._project_needs_clarification``); only its answer *content*
#: (sections/facts) must stay empty, which ``validate_outcome_consistency``
#: already enforces.
NO_ANSWER_OUTCOMES = frozenset(
    {"not_found", "temporarily_unavailable", "unsupported", "denied", "failed"}
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


# ---------------------------------------------------------------------------
# (f) the no-answer allowlist projection — see the module docstring section
# "The no-answer allowlist projection" for the design and why round-1's
# denylist was replaced wholesale.
# ---------------------------------------------------------------------------

#: The single server-owned public sentence each no-answer outcome is allowed
#: to render. A no-answer frame's ``direct_answer`` must equal this exactly:
#: producer-authored copy is replaced, never trimmed or re-emitted.
CANONICAL_NO_ANSWER_COPY: Mapping[str, str] = {
    "not_found": "No matching subject was found for this question.",
    "temporarily_unavailable": (
        "This answer is temporarily unavailable. Please try again shortly."
    ),
    "unsupported": "This question is not supported yet.",
    "denied": "You do not have access to ask about this.",
    "failed": "Something went wrong while preparing this answer.",
}

#: Server-owned remediation for each no-answer outcome, used by the v1
#: projector. Round-1's projector used the frame's own
#: ``safe_follow_up_questions`` here, which is producer-authored text about
#: the subject — the same reuse channel as ``direct_answer``.
CANONICAL_NO_ANSWER_REMEDIATION: Mapping[str, tuple[str, ...]] = {
    "not_found": ("Check the name and try again.",),
    "temporarily_unavailable": ("Try the question again in a few minutes.",),
    "unsupported": ("Try a status, health, or metric question instead.",),
    "denied": ("Ask an administrator for access to this area.",),
    "failed": ("Try the question again.",),
}

#: The matching ``dev_answer.v2`` display labels, kept in step with
#: ``answer._OUTCOME_DISPLAY_LABELS`` by an import-time assertion there.
CANONICAL_NO_ANSWER_DISPLAY_LABELS: Mapping[str, str] = {
    "not_found": "Not found",
    "temporarily_unavailable": "Temporarily unavailable",
    "unsupported": "Not supported yet",
    "denied": "Not permitted",
    "failed": "Something went wrong",
}

#: The ``OpaqueID`` shape: a whitespace-free identifier token. A string that
#: matches this cannot carry a sentence of producer-authored prose.
_IDENTIFIER_TOKEN_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:/#-]{0,127}$")


class NoAnswerFieldPolicy(StrEnum):
    """How one field of a no-answer contract object is projected."""

    ABSENT = "absent"
    CANONICAL = "canonical"
    IDENTIFIER = "identifier"
    NON_TEXT = "non_text"
    SELF_VALIDATED = "self_validated"


#: Every field of ``DevAnswerFrame``, classified. Checked for totality
#: against the model at import time by ``frame.py``.
NO_ANSWER_FRAME_FIELD_POLICY: Mapping[str, NoAnswerFieldPolicy] = {
    "schema_version": NoAnswerFieldPolicy.IDENTIFIER,
    "frame_id": NoAnswerFieldPolicy.IDENTIFIER,
    "run_id": NoAnswerFieldPolicy.IDENTIFIER,
    "generated_at": NoAnswerFieldPolicy.NON_TEXT,
    "public_outcome": NoAnswerFieldPolicy.IDENTIFIER,
    "subject_ref": NoAnswerFieldPolicy.ABSENT,
    "subject_set_ref": NoAnswerFieldPolicy.ABSENT,
    "direct_answer": NoAnswerFieldPolicy.CANONICAL,
    "completion": NoAnswerFieldPolicy.ABSENT,
    "readiness": NoAnswerFieldPolicy.ABSENT,
    "sections": NoAnswerFieldPolicy.ABSENT,
    "facts": NoAnswerFieldPolicy.ABSENT,
    "metrics": NoAnswerFieldPolicy.ABSENT,
    "comparisons": NoAnswerFieldPolicy.ABSENT,
    "relationship_paths": NoAnswerFieldPolicy.ABSENT,
    "health_profile_refs": NoAnswerFieldPolicy.ABSENT,
    "finding_refs": NoAnswerFieldPolicy.ABSENT,
    "deficiency_refs": NoAnswerFieldPolicy.ABSENT,
    "conflicts": NoAnswerFieldPolicy.ABSENT,
    "limitations": NoAnswerFieldPolicy.ABSENT,
    "source_observations": NoAnswerFieldPolicy.ABSENT,
    # Coverage carries only counts, a timestamp, and source-class
    # identifiers from a fixed platform vocabulary — never subject-derived
    # text — so it is admitted as identifiers rather than blanked, which
    # keeps "how many sources were required" answerable for a denial.
    "coverage": NoAnswerFieldPolicy.IDENTIFIER,
    "evidence": NoAnswerFieldPolicy.ABSENT,
    "safe_follow_up_questions": NoAnswerFieldPolicy.ABSENT,
    # Plan/rule/query versions are platform provenance tokens, not copy.
    "versions": NoAnswerFieldPolicy.IDENTIFIER,
}

#: Every field of ``DevAnswerV2``, classified. ``narrative`` is ``ABSENT``:
#: an optional provider narrative is exactly the free-form channel a
#: no-answer outcome must not have.
NO_ANSWER_ANSWER_FIELD_POLICY: Mapping[str, NoAnswerFieldPolicy] = {
    "schema_version": NoAnswerFieldPolicy.IDENTIFIER,
    "answer_id": NoAnswerFieldPolicy.IDENTIFIER,
    "conversation_id": NoAnswerFieldPolicy.IDENTIFIER,
    "run_id": NoAnswerFieldPolicy.IDENTIFIER,
    "generated_at": NoAnswerFieldPolicy.NON_TEXT,
    "public_outcome": NoAnswerFieldPolicy.IDENTIFIER,
    "outcome_display_label": NoAnswerFieldPolicy.CANONICAL,
    "frame": NoAnswerFieldPolicy.SELF_VALIDATED,
    "narrative": NoAnswerFieldPolicy.ABSENT,
}

#: Registered ``(policy, canonical tables)`` per model class, populated at
#: import time. ``SELF_VALIDATED`` resolves through this registry, so a
#: nested contract cannot be delegated to unless it is itself classified.
_POLICY_REGISTRY: dict[
    type[BaseModel], tuple[Mapping[str, NoAnswerFieldPolicy], Mapping[str, Any]]
] = {}


def _string_leaves(value: object) -> Iterator[str]:
    """Every string reachable from ``value``, through models and collections."""

    if value is None:
        return
    if isinstance(value, str):
        yield value
        return
    if isinstance(value, BaseModel):
        for name in type(value).model_fields:
            yield from _string_leaves(getattr(value, name))
        return
    if isinstance(value, Mapping):
        for key, item in value.items():
            yield from _string_leaves(key)
            yield from _string_leaves(item)
        return
    if isinstance(value, (tuple, list, set, frozenset)):
        for item in value:
            yield from _string_leaves(item)


def _is_absent(value: object) -> bool:
    if value is None:
        return True
    if isinstance(value, (str, bytes)):
        return False
    if isinstance(value, (tuple, list, set, frozenset, Mapping)):
        return not value
    return False


def register_no_answer_policy(
    model_cls: type[BaseModel],
    policy: Mapping[str, NoAnswerFieldPolicy],
    canonical: Mapping[str, Mapping[str, str]],
) -> None:
    """Register and immediately total-check one model's no-answer policy."""

    assert_no_answer_policy_is_total(model_cls, policy, canonical)
    _POLICY_REGISTRY[model_cls] = (policy, canonical)


def assert_no_answer_policy_is_total(
    model_cls: type[BaseModel],
    policy: Mapping[str, NoAnswerFieldPolicy],
    canonical: Mapping[str, Mapping[str, str]],
) -> None:
    """Raise unless every field of ``model_cls`` carries a classification.

    Called at import time, so adding a field to a no-answer-bearing contract
    without classifying it breaks the package import rather than silently
    opening a new disclosure channel.
    """

    declared = set(policy)
    actual = set(model_cls.model_fields)
    unclassified = sorted(actual - declared)
    if unclassified:
        raise RuntimeError(
            f"{model_cls.__name__} field(s) {unclassified} have no no-answer "
            "projection policy; classify them in validators.py "
            "(ABSENT / CANONICAL / IDENTIFIER / NON_TEXT / SELF_VALIDATED)"
        )
    stale = sorted(declared - actual)
    if stale:
        raise RuntimeError(
            f"{model_cls.__name__} no-answer policy names removed field(s) {stale}"
        )
    for field_name, rule in policy.items():
        if rule is NoAnswerFieldPolicy.CANONICAL:
            table = canonical.get(field_name)
            if table is None or set(table) != set(NO_ANSWER_OUTCOMES):
                raise RuntimeError(
                    f"{model_cls.__name__}.{field_name} is CANONICAL but has no "
                    "canonical value for every no-answer outcome"
                )
        elif rule is NoAnswerFieldPolicy.SELF_VALIDATED:
            nested = model_cls.model_fields[field_name].annotation
            if not (isinstance(nested, type) and nested in _POLICY_REGISTRY):
                raise RuntimeError(
                    f"{model_cls.__name__}.{field_name} delegates to a nested "
                    "contract that has no registered no-answer policy"
                )


def _enforce_no_answer_projection(obj: BaseModel, outcome: str) -> None:
    policy, canonical = _POLICY_REGISTRY[type(obj)]
    label = type(obj).__name__
    for field_name, rule in policy.items():
        value = getattr(obj, field_name)
        if rule is NoAnswerFieldPolicy.ABSENT:
            if not _is_absent(value):
                raise ValueError(
                    f"public outcome {outcome!r} cannot carry {label}.{field_name}"
                )
        elif rule is NoAnswerFieldPolicy.CANONICAL:
            expected = canonical[field_name][outcome]
            if value != expected:
                raise ValueError(
                    f"public outcome {outcome!r} requires the canonical server "
                    f"copy for {label}.{field_name}; producer-authored text is "
                    "never reused for a no-answer outcome"
                )
        elif rule is NoAnswerFieldPolicy.IDENTIFIER:
            for leaf in _string_leaves(value):
                if not _IDENTIFIER_TOKEN_PATTERN.match(leaf):
                    raise ValueError(
                        f"public outcome {outcome!r} allows only identifier "
                        f"tokens in {label}.{field_name}, not free text: {leaf!r}"
                    )
        elif rule is NoAnswerFieldPolicy.NON_TEXT:
            leaked = list(_string_leaves(value))
            if leaked:
                raise ValueError(
                    f"public outcome {outcome!r} allows no text in "
                    f"{label}.{field_name}: {leaked[0]!r}"
                )
        elif value is not None:
            _enforce_no_answer_projection(value, outcome)


def validate_no_answer_projection(obj: BaseModel) -> None:
    """(f) Project a no-answer contract object through its field allowlist.

    Applies to ``DevAnswerFrame`` and ``DevAnswerV2`` (whichever registered
    policy matches ``type(obj)``). For an outcome that is not a no-answer
    outcome this is a no-op — those objects are governed by the ordinary
    content validators instead.
    """

    outcome = obj.public_outcome.value  # type: ignore[attr-defined]
    if outcome not in NO_ANSWER_OUTCOMES:
        return
    _enforce_no_answer_projection(obj, outcome)


#: Retained name for guardrail (f). The round-1 denylist implementation was
#: replaced by the allowlist projection above; the name is kept because
#: ``frame.py`` calls guards through the module object and the mutation
#: tests disable them by name.
validate_no_answer_content_leaks = validate_no_answer_projection


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
# (g) narrative prose bound to the specific frame facts it narrates. See the
# module docstring's "Narrative fact binding" section for the three round-2
# bypasses this replaced and for what remains out of contract-level reach
# (CHAOS-3297, the layer-6 narrative-consistency validator).
# ---------------------------------------------------------------------------

#: Any bare numeral or percentage token in prose, e.g. ``100``, ``75%``,
#: ``0.75``.
_NUMERIC_TOKEN_PATTERN = re.compile(r"\d+(?:\.\d+)?%?")
#: A percentage specifically — the form a completion claim takes.
_PERCENT_TOKEN_PATTERN = re.compile(r"(\d+(?:\.\d+)?)%")
_SENTENCE_SPLIT_PATTERN = re.compile(r"(?<=[.!?])\s+")
_WORD_TOKEN_PATTERN = re.compile(r"[a-z0-9]+")
_READY_WORD_PATTERN = re.compile(r"\bready\b", re.IGNORECASE)
_NOT_READY_PHRASE_PATTERN = re.compile(r"\bnot[\s-]+ready\b", re.IGNORECASE)
_RECOMMENDATION_CLAIM_PATTERN = re.compile(
    r"\brecommend(?:s|ed|ation|ations)?\b", re.IGNORECASE
)
_COMPLETION_CLAIM_PATTERN = re.compile(
    r"\b(?:complete|completed|completion|done|finished)\b", re.IGNORECASE
)


def _numeric_value(raw_token: str) -> float:
    return float(raw_token[:-1]) if raw_token.endswith("%") else float(raw_token)


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
    narrative: DevNarrative, frame: DevAnswerFrame
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


def _completion_value_set(frame: DevAnswerFrame) -> set[float]:
    completion = frame.completion
    values: set[float] = set()
    if completion is None:
        return values
    if completion.numerator is not None:
        values.add(float(completion.numerator))
    if completion.denominator is not None:
        values.add(float(completion.denominator))
    if completion.rate is not None:
        # Both renderings are legitimate: "0.75" and "75%".
        values.add(round(completion.rate, 6))
        values.add(round(completion.rate * 100, 6))
    return values


def _bound_numeric_values(narrative: DevNarrative, frame: DevAnswerFrame) -> set[float]:
    """Numbers the narrative is allowed to cite anywhere in its body.

    Deliberately excludes ``comparisons``: an unrelated comparison value is
    exactly what let "100% complete" pass against a 75% completion block.
    Comparison values are admitted per sentence instead, and only when that
    sentence names the comparison's own label.
    """

    bound_fact_ids = _narrative_bound_fact_ids(narrative, frame)
    texts: list[str] = [frame.direct_answer]
    texts.extend(fact.text for fact in frame.facts if fact.fact_id in bound_fact_ids)
    texts.extend(frame.limitations)
    texts.extend(frame.safe_follow_up_questions)
    if frame.readiness is not None:
        texts.extend(frame.readiness.translated_user_reasons)
    values = {
        _numeric_value(match.group())
        for text in texts
        for match in _NUMERIC_TOKEN_PATTERN.finditer(text)
    }
    return values | _completion_value_set(frame)


def _sentence_comparison_values(
    sentence_tokens: Sequence[str], frame: DevAnswerFrame
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
    narrative: DevNarrative, frame: DevAnswerFrame
) -> None:
    """(g.1) Bind every narrative numeral to the facts the narrative references.

    Two independent rules, applied per sentence:

    1. *Containment.* Each numeral must appear in the bound value set — the
       referenced facts' own text, the frame's canonical copy, and the
       completion block — or in a comparison whose label that same sentence
       names.
    2. *Completion claims.* A sentence using completion vocabulary may only
       cite a percentage the completion block itself supports, and may not
       cite one at all when there is no calculable completion block. This
       holds even if some unrelated frame value happens to equal the cited
       number, which is what defeated the round-1 global check.
    """

    bound_values = _bound_numeric_values(narrative, frame)
    completion_values = _completion_value_set(frame)
    calculable = frame.completion is not None and frame.completion.calculable
    for sentence in _SENTENCE_SPLIT_PATTERN.split(narrative.body):
        sentence_tokens = _word_tokens(sentence)
        allowed = bound_values | _sentence_comparison_values(sentence_tokens, frame)
        offenders = sorted(
            {
                match.group()
                for match in _NUMERIC_TOKEN_PATTERN.finditer(sentence)
                if not _value_in(allowed, _numeric_value(match.group()))
            }
        )
        if offenders:
            raise ValueError(
                f"narrative cites number(s) {offenders} that no fact it "
                "references, and no comparison it names, supports"
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
            if not _value_in(completion_values, claimed):
                raise ValueError(
                    f"narrative claims {claimed}% completion, which the frame's "
                    "own completion block does not support"
                )


def validate_narrative_readiness_claim(
    narrative: DevNarrative, frame: DevAnswerFrame
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
    narrative: DevNarrative, frame: DevAnswerFrame
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
    narrative: DevNarrative, frame: DevAnswerFrame
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
    narrative: DevNarrative, frame: DevAnswerFrame
) -> None:
    """(g) Run all four narrative/frame binding checks (g.1-g.4)."""

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
