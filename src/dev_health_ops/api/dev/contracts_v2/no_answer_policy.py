"""The no-answer field-projection policy — a leaf of the contracts_v2 graph.

This module owns the machinery behind guardrail (f): the field-classification
enum, the per-model policy registry, the canonical server-owned copy tables,
and the projection that enforces them. See the ``validators`` module docstring
for the design and the four review rounds that shaped it.

It exists as a separate module for a structural reason. ``frame.py`` and
``embedded.py`` both *register* a policy at import time, and ``validators.py``
needs the frame's own type to annotate its guards. With everything in one
module that made ``embedded -> validators -> frame -> embedded`` and
``frame -> validators -> frame`` genuine import cycles, in which a name could
be read before it was bound depending on which module an importer reached
first (CodeQL ``py/unsafe-cyclic-import``, 17 alerts). Nothing here imports
anything from this package except ``base``, so the registration side of the
graph is a leaf and the cycles are gone rather than suppressed.

Everything public here is re-exported from ``validators`` so the guard names
keep their documented home, and so the mutation tests that disable a guard by
patching the ``validators`` module object continue to work unchanged.
"""

from __future__ import annotations

import re
from collections.abc import Iterator, Mapping
from enum import StrEnum
from typing import Any, get_args

from pydantic import BaseModel

from .base import SourceClass

__all__ = [
    "CANONICAL_NO_ANSWER_COPY",
    "CANONICAL_NO_ANSWER_DISPLAY_LABELS",
    "CANONICAL_NO_ANSWER_REMEDIATION",
    "NO_ANSWER_ANSWER_FIELD_POLICY",
    "NO_ANSWER_FRAME_FIELD_POLICY",
    "NO_ANSWER_OUTCOMES",
    "SOURCE_CLASS_VOCABULARY",
    "NoAnswerFieldPolicy",
    "assert_no_answer_policy_is_total",
    "literal_vocabulary",
    "register_no_answer_policy",
    "validate_no_answer_content_leaks",
    "validate_no_answer_projection",
]


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
    CLOSED_VOCABULARY = "closed_vocabulary"
    IDENTIFIER = "identifier"
    NON_TEXT = "non_text"
    SELF_VALIDATED = "self_validated"


#: The closed set of source-class tokens a coverage block may disclose. Taken
#: from the enum itself so the vocabulary cannot drift from the type.
SOURCE_CLASS_VOCABULARY: frozenset[str] = frozenset(
    member.value for member in SourceClass
)


def literal_vocabulary(model_cls: type[BaseModel], field_name: str) -> frozenset[str]:
    """The ``Literal`` values one field admits, read off the model itself.

    Used for ``schema_version``, so the registered closed vocabulary is the
    annotation rather than a hand-copied string that could drift from it.
    """

    annotation = model_cls.model_fields[field_name].annotation
    values = frozenset(
        argument for argument in get_args(annotation) if isinstance(argument, str)
    )
    if not values:
        raise RuntimeError(
            f"{model_cls.__name__}.{field_name} is not a Literal of strings, so "
            "it has no literal vocabulary to register"
        )
    return values


#: Every field of ``DevAnswerFrame``, classified. Checked for totality
#: against the model at import time by ``frame.py``.
NO_ANSWER_FRAME_FIELD_POLICY: Mapping[str, NoAnswerFieldPolicy] = {
    "schema_version": NoAnswerFieldPolicy.CLOSED_VOCABULARY,
    # The two correlation handles — the documented residual of the round-3
    # closure; see the module docstring.
    "frame_id": NoAnswerFieldPolicy.IDENTIFIER,
    "run_id": NoAnswerFieldPolicy.IDENTIFIER,
    "generated_at": NoAnswerFieldPolicy.NON_TEXT,
    "public_outcome": NoAnswerFieldPolicy.CLOSED_VOCABULARY,
    "subject_ref": NoAnswerFieldPolicy.ABSENT,
    "subject_set_ref": NoAnswerFieldPolicy.ABSENT,
    # CHAOS-3325: real per-mention candidates, only ever populated for
    # needs_clarification -- which is not one of NO_ANSWER_OUTCOMES (see the
    # module docstring), so this ABSENT cell governs the five true no-answer
    # outcomes; validators.validate_outcome_consistency separately forbids it
    # on 'answered'/'answered_with_gaps', the two outcomes this policy does
    # not reach.
    "clarification_candidates": NoAnswerFieldPolicy.ABSENT,
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
    # Coverage keeps "how many sources were required" answerable for a
    # denial. Round 2 admitted it as identifier-shaped tokens, which round 3
    # walked through with a subject-derived source name; it now delegates to
    # DevCoverageV2's own policy, where the source lists are the closed
    # SourceClass vocabulary and everything else is NON_TEXT.
    "coverage": NoAnswerFieldPolicy.SELF_VALIDATED,
    "evidence": NoAnswerFieldPolicy.ABSENT,
    "safe_follow_up_questions": NoAnswerFieldPolicy.ABSENT,
    # A no-answer outcome carries no provenance block at all — see the module
    # docstring for why the block is dropped rather than constrained.
    "versions": NoAnswerFieldPolicy.ABSENT,
}

#: Every field of ``DevAnswerV2``, classified. ``narrative`` is ``ABSENT``:
#: an optional provider narrative is exactly the free-form channel a
#: no-answer outcome must not have.
NO_ANSWER_ANSWER_FIELD_POLICY: Mapping[str, NoAnswerFieldPolicy] = {
    "schema_version": NoAnswerFieldPolicy.CLOSED_VOCABULARY,
    "answer_id": NoAnswerFieldPolicy.IDENTIFIER,
    "conversation_id": NoAnswerFieldPolicy.IDENTIFIER,
    "run_id": NoAnswerFieldPolicy.IDENTIFIER,
    "generated_at": NoAnswerFieldPolicy.NON_TEXT,
    "public_outcome": NoAnswerFieldPolicy.CLOSED_VOCABULARY,
    "outcome_display_label": NoAnswerFieldPolicy.CANONICAL,
    "frame": NoAnswerFieldPolicy.SELF_VALIDATED,
    "narrative": NoAnswerFieldPolicy.ABSENT,
}

#: Registered ``(policy, canonical tables, closed vocabularies)`` per model
#: class, populated at import time. ``SELF_VALIDATED`` resolves through this
#: registry, so a nested contract cannot be delegated to unless it is itself
#: classified.
_POLICY_REGISTRY: dict[
    type[BaseModel],
    tuple[
        Mapping[str, NoAnswerFieldPolicy],
        Mapping[str, Any],
        Mapping[str, frozenset[str]],
    ],
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
    vocabularies: Mapping[str, frozenset[str]] | None = None,
) -> None:
    """Register and immediately total-check one model's no-answer policy."""

    closed = dict(vocabularies or {})
    assert_no_answer_policy_is_total(model_cls, policy, canonical, closed)
    _POLICY_REGISTRY[model_cls] = (policy, canonical, closed)


def assert_no_answer_policy_is_total(
    model_cls: type[BaseModel],
    policy: Mapping[str, NoAnswerFieldPolicy],
    canonical: Mapping[str, Mapping[str, str]],
    vocabularies: Mapping[str, frozenset[str]] | None = None,
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
    closed = dict(vocabularies or {})
    for field_name, rule in policy.items():
        if rule is NoAnswerFieldPolicy.CANONICAL:
            table = canonical.get(field_name)
            if table is None or set(table) != set(NO_ANSWER_OUTCOMES):
                raise RuntimeError(
                    f"{model_cls.__name__}.{field_name} is CANONICAL but has no "
                    "canonical value for every no-answer outcome"
                )
        elif rule is NoAnswerFieldPolicy.CLOSED_VOCABULARY:
            if not closed.get(field_name):
                raise RuntimeError(
                    f"{model_cls.__name__}.{field_name} is CLOSED_VOCABULARY but "
                    "has no registered non-empty vocabulary; a closed set that "
                    "is not supplied would admit everything"
                )
        elif rule is NoAnswerFieldPolicy.SELF_VALIDATED:
            nested = model_cls.model_fields[field_name].annotation
            if not (isinstance(nested, type) and nested in _POLICY_REGISTRY):
                raise RuntimeError(
                    f"{model_cls.__name__}.{field_name} delegates to a nested "
                    "contract that has no registered no-answer policy"
                )
    misdeclared = sorted(
        name
        for name in closed
        if policy.get(name) is not NoAnswerFieldPolicy.CLOSED_VOCABULARY
    )
    if misdeclared:
        raise RuntimeError(
            f"{model_cls.__name__} registers closed vocabularies for field(s) "
            f"{misdeclared} that are not classified CLOSED_VOCABULARY"
        )


def _enforce_no_answer_projection(obj: BaseModel, outcome: str) -> None:
    policy, canonical, vocabularies = _POLICY_REGISTRY[type(obj)]
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
        elif rule is NoAnswerFieldPolicy.CLOSED_VOCABULARY:
            allowed = vocabularies[field_name]
            for leaf in _string_leaves(value):
                if leaf not in allowed:
                    raise ValueError(
                        f"public outcome {outcome!r} allows only the "
                        f"server-owned vocabulary in {label}.{field_name}; "
                        f"{leaf!r} is not one of its values"
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
