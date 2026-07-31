"""Deeply immutable v2 mirrors of the v1 contract objects v2 embeds.

Why this module exists
----------------------

``ContractModelV2``'s ``frozen=True`` blocks attribute *rebinding*; it does
nothing about the contents of a ``list`` field. Every v2-native contract
therefore declares its collections as ``tuple``. But ``DevAnswerFrame``
embedded v1 objects (``DevCoverage``, ``DevEvidenceRef``, ``DevMetricRef``),
and ``DevStreamEventV2``/``DevMessageRequestV2`` embedded ``DevError`` and
``DevScope`` — all of which declare ``list`` fields. Adversarial review round
3 walked straight through that seam::

    frame = DevAnswerFrame.model_validate(payload)   # fully validated
    frame.coverage.unavailable_required_sources.append("private/Nightfall")
    frame.model_dump(mode="json")                    # ...and it serializes

Post-validation mutation of a *validated* wire object is exactly the defect
the tuple convention exists to prevent, so "v1 is frozen for CHAOS-3294, so
its lists are an acknowledged boundary" was not a closure — the boundary was
inside the object graph the v2 validators had just certified.

How the mirrors work
--------------------

Each mirror inherits from **both** ``ContractModelV2`` and its v1 original,
and overrides only the mutable fields with their immutable equivalents. That
is deliberate rather than a hand-written copy: every v1 field constraint,
``field_validator`` and ``model_validator`` (``DevScope``'s direct-scope and
surface-context invariants are substantial) is inherited rather than
duplicated, so the mirror cannot drift from the original the way a
reimplementation would. v1 itself is untouched, keeping CHAOS-3294 additive.

Two consequences worth stating:

* A mirror ``isinstance``-passes as its v1 original, but its collections are
  tuples, so it must not be handed to a v1-typed field directly — the v1
  serializer would be asked to emit a ``list`` and receive a ``tuple``. The
  one place that matters is the v2-to-v1 projector, which converts
  explicitly (see ``compat._as_v1``).
* A v1 model with no collection field at any depth (``DevEntityRef``,
  ``DevTimeRange``, ``DevCitationLink``, ``DevEvidenceFlags``,
  ``DevMetricPoint``, ``DevModelMetadata``) is already immutable in fact
  under ``frozen=True`` and needs no mirror. That predicate — *no mutable
  collection anywhere in the closure reachable from a v2 contract* — is what
  ``test_contracts_v2`` asserts by recursive introspection, so a newly
  embedded v1 model with a ``list`` fails there rather than silently
  reopening this seam.

``DevCoverageV2`` additionally narrows its two source lists from ``OpaqueID``
to the closed ``SourceClass`` vocabulary; see that enum's docstring for why a
denied answer's coverage block was a disclosure channel.
"""

from __future__ import annotations

from pydantic import Field

from dev_health_ops.api.dev.contracts import (
    DevCoverage,
    DevEntityRef,
    DevError,
    DevEvidenceRef,
    DevMetricPoint,
    DevMetricRef,
    DevScope,
    DevSurfaceContext,
)

from . import validators as _validators
from .base import ContractModelV2, Label, OpaqueID, ShortText, SourceClass

__all__ = [
    "DevCoverageV2",
    "DevErrorV2",
    "DevEvidenceRefV2",
    "DevMetricRefV2",
    "DevScopeV2",
    "DevSurfaceContextV2",
]


# Narrowing an inherited ``list[X]`` field to ``tuple[X, ...]`` is what every
# class below exists to do, and mypy reports each one as an unsound attribute
# override: a subclass instance could be used where the mutable-list base type
# is expected. That substitutability is exactly what this module does *not*
# rely on — the mirrors are frozen, and the one place a mirror crosses back
# into a v1-typed field converts explicitly through ``compat._as_v1`` rather
# than passing the instance. The suppression is per-line so an unrelated
# assignment error in this file is still reported.


class DevCoverageV2(ContractModelV2, DevCoverage):
    """``dev_coverage`` with immutable, closed-vocabulary source lists."""

    unavailable_required_sources: tuple[SourceClass, ...] = Field(  # type: ignore[assignment]
        default_factory=tuple, max_length=25
    )
    stale_required_sources: tuple[SourceClass, ...] = Field(  # type: ignore[assignment]
        default_factory=tuple, max_length=25
    )


class DevSurfaceContextV2(ContractModelV2, DevSurfaceContext):
    entity_refs: tuple[DevEntityRef, ...] = Field(  # type: ignore[assignment]
        default_factory=tuple, max_length=20
    )


class DevScopeV2(ContractModelV2, DevScope):
    repositories: tuple[OpaqueID, ...] = Field(  # type: ignore[assignment]
        default_factory=tuple, max_length=20
    )
    entity_refs: tuple[DevEntityRef, ...] = Field(  # type: ignore[assignment]
        default_factory=tuple, max_length=20
    )
    team_ids: tuple[OpaqueID, ...] = Field(  # type: ignore[assignment]
        default_factory=tuple, max_length=20
    )
    surface_context: DevSurfaceContextV2 | None = None


class DevEvidenceRefV2(ContractModelV2, DevEvidenceRef):
    repository_ids: tuple[OpaqueID, ...] = Field(  # type: ignore[assignment]
        default_factory=tuple, max_length=20
    )
    valid_entity_ids: tuple[OpaqueID, ...] = Field(  # type: ignore[assignment]
        default_factory=tuple, max_length=20
    )


class DevMetricRefV2(ContractModelV2, DevMetricRef):
    resolved_scope: DevScopeV2
    dimensions: tuple[Label, ...] = Field(  # type: ignore[assignment]
        default_factory=tuple, max_length=12
    )
    series: tuple[DevMetricPoint, ...] = Field(  # type: ignore[assignment]
        default_factory=tuple, max_length=366
    )
    evidence_ref_ids: tuple[OpaqueID, ...] = Field(  # type: ignore[assignment]
        default_factory=tuple, max_length=25
    )


class DevErrorV2(ContractModelV2, DevError):
    remediation: tuple[ShortText, ...] = Field(  # type: ignore[assignment]
        default_factory=tuple, max_length=5
    )


#: ``DevAnswerFrame.coverage`` is the one field a no-answer outcome keeps that
#: is not canonical server copy, so it carries its own registered policy and
#: is reached through ``SELF_VALIDATED``. Every leaf is either a count, a
#: timestamp, or a member of the closed ``SourceClass`` vocabulary — the frame
#: can still answer "how many sources were required" for a denial without any
#: field able to carry a producer-chosen string.
_validators.register_no_answer_policy(
    DevCoverageV2,
    {
        "required_source_count": _validators.NoAnswerFieldPolicy.NON_TEXT,
        "available_source_count": _validators.NoAnswerFieldPolicy.NON_TEXT,
        "unavailable_required_sources": (
            _validators.NoAnswerFieldPolicy.CLOSED_VOCABULARY
        ),
        "stale_required_sources": _validators.NoAnswerFieldPolicy.CLOSED_VOCABULARY,
        "as_of": _validators.NoAnswerFieldPolicy.NON_TEXT,
    },
    canonical={},
    vocabularies={
        "unavailable_required_sources": _validators.SOURCE_CLASS_VOCABULARY,
        "stale_required_sources": _validators.SOURCE_CLASS_VOCABULARY,
    },
)
