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

from enum import StrEnum
from typing import Self

from pydantic import Field, model_validator

from dev_health_ops.api.dev.contracts import (
    DevCIFact,
    DevCoverage,
    DevDeploymentFact,
    DevEntityRef,
    DevError,
    DevEvidenceRef,
    DevGraphEdge,
    DevIncidentFact,
    DevMetricPoint,
    DevMetricRef,
    DevPullRequestFact,
    DevRequiredChildFact,
    DevScope,
    DevStatusFact,
    DevSurfaceContext,
)

from . import no_answer_policy as _policy
from .base import (
    ContractModelV2,
    EvidenceHandle,
    Label,
    OpaqueID,
    ShortText,
    SourceClass,
)

__all__ = [
    "DevCIFactV2",
    "DevCoverageV2",
    "DevDeploymentFactV2",
    "DevErrorV2",
    "DevEvidenceRefV2",
    "DevGraphEdgeV2",
    "DevIncidentFactV2",
    "DevMetricRefV2",
    "DevPullRequestFactV2",
    "DevRequiredChildFactV2",
    "DevScopeV2",
    "DevStatusFactV2",
    "DevSurfaceContextV2",
    "MetricEvidenceClassification",
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
    evidence_ref_id: EvidenceHandle
    repository_ids: tuple[OpaqueID, ...] = Field(  # type: ignore[assignment]
        default_factory=tuple, max_length=20
    )
    valid_entity_ids: tuple[OpaqueID, ...] = Field(  # type: ignore[assignment]
        default_factory=tuple, max_length=20
    )


class MetricEvidenceClassification(StrEnum):
    """Closed reasons a ``DevMetricRefV2`` may carry no per-metric evidence
    ref (F10, CHAOS-3297 stack #3, ratified 2026-08-02) -- the same
    "evidence XOR an explicit no-evidence classification" pattern
    ``contracts_v2.deficiency.DeficiencyEvidenceClassification`` already
    established, applied to metrics here.
    """

    #: The metric arrived via the legacy v1 model-tool-choice loop's own
    #: ``query_metric.v1`` tool (``production_runtime.py``), which
    #: deliberately scrubs ``evidence_ref_ids`` to ``()`` on every call --
    #: the metric service's own source refs are not signer-minted
    #: ``DevEvidenceRef`` ids. Named for what happened (the refs WERE
    #: scrubbed, not merely absent) so a frame consumer -- including
    #: CHAOS-3298's web renderer -- can distinguish "known legacy gap" from
    #: "a bug": an unclassified, evidence-free metric is always the latter.
    LEGACY_V1_UNMINTED = "legacy_v1_unminted"


class DevMetricRefV2(ContractModelV2, DevMetricRef):
    resolved_scope: DevScopeV2
    dimensions: tuple[Label, ...] = Field(  # type: ignore[assignment]
        default_factory=tuple, max_length=12
    )
    series: tuple[DevMetricPoint, ...] = Field(  # type: ignore[assignment]
        default_factory=tuple, max_length=366
    )
    evidence_ref_ids: tuple[EvidenceHandle, ...] = Field(  # type: ignore[assignment]
        default_factory=tuple, max_length=25
    )
    #: F10 (CHAOS-3297 stack #3): evidence_ref_ids XOR evidence_classification
    #: -- never both, never neither. See validate_evidence_or_classification.
    evidence_classification: MetricEvidenceClassification | None = None

    @model_validator(mode="after")
    def validate_evidence_or_classification(self) -> Self:
        has_evidence = bool(self.evidence_ref_ids)
        has_classification = self.evidence_classification is not None
        if has_evidence and has_classification:
            raise ValueError(
                "a metric with real evidence_ref_ids must not also carry an "
                "evidence_classification -- the classification exists only "
                "for the no-evidence case"
            )
        if not has_evidence and not has_classification:
            raise ValueError(
                "a DevMetricRefV2 requires either evidence_ref_ids or an "
                "explicit evidence_classification (F10) -- neither is not a "
                "valid disclosure"
            )
        return self


class DevStatusFactV2(ContractModelV2, DevStatusFact):
    """Mirrors ``DevStatusFact`` for CHAOS-3295's per-step content payload."""

    evidence_ref_ids: tuple[OpaqueID, ...] = Field(  # type: ignore[assignment]
        min_length=1, max_length=25
    )


class DevRequiredChildFactV2(ContractModelV2, DevRequiredChildFact):
    evidence_ref_ids: tuple[OpaqueID, ...] = Field(  # type: ignore[assignment]
        default_factory=tuple, max_length=25
    )


class DevPullRequestFactV2(ContractModelV2, DevPullRequestFact):
    evidence_ref_ids: tuple[OpaqueID, ...] = Field(  # type: ignore[assignment]
        default_factory=tuple, max_length=25
    )


class DevCIFactV2(ContractModelV2, DevCIFact):
    evidence_ref_ids: tuple[OpaqueID, ...] = Field(  # type: ignore[assignment]
        default_factory=tuple, max_length=25
    )


class DevDeploymentFactV2(ContractModelV2, DevDeploymentFact):
    evidence_ref_ids: tuple[OpaqueID, ...] = Field(  # type: ignore[assignment]
        default_factory=tuple, max_length=25
    )


class DevIncidentFactV2(ContractModelV2, DevIncidentFact):
    evidence_ref_ids: tuple[OpaqueID, ...] = Field(  # type: ignore[assignment]
        default_factory=tuple, max_length=25
    )


class DevGraphEdgeV2(ContractModelV2, DevGraphEdge):
    #: CHAOS-3296 round-4 closure: v1's ``DevGraphEdge`` never carried the
    #: work-graph edge's own canonical identity -- only its endpoints
    #: (``source_entity_id``/``target_entity_id``) and relationship token
    #: survived to the wire. ``_wire_work_graph_content`` (builtin_steps.py)
    #: always mints each edge's evidence handle against ``item.edge_id``
    #: (``WorkGraphNeighborEdge.edge_id``), but that identity was previously
    #: DISCARDED before construction, so no verifier could ever check a
    #: handle against the edge it actually claims to back -- Codex round 3
    #: (2026-08-02, [HIGH]) confirmed this let one legitimately-minted edge
    #: handle "verify" an arbitrary second, fabricated edge. A v2-only
    #: addition (v1 stays untouched, additive per this module's own
    #: mirror-not-reimplement posture) -- required, not defaulted, so every
    #: construction site must be deliberate about identity.
    edge_id: OpaqueID
    evidence_ref_ids: tuple[OpaqueID, ...] = Field(  # type: ignore[assignment]
        default_factory=tuple, max_length=25
    )


class DevErrorV2(ContractModelV2, DevError):
    # request_id is inherited as the loose v1 OpaqueID on purpose: the
    # router populates it from `body.request_id or header_request_id`,
    # i.e. a client-supplied value.
    remediation: tuple[ShortText, ...] = Field(  # type: ignore[assignment]
        default_factory=tuple, max_length=5
    )


#: ``DevAnswerFrame.coverage`` is the one field a no-answer outcome keeps that
#: is not canonical server copy, so it carries its own registered policy and
#: is reached through ``SELF_VALIDATED``. Every leaf is either a count, a
#: timestamp, or a member of the closed ``SourceClass`` vocabulary — the frame
#: can still answer "how many sources were required" for a denial without any
#: field able to carry a producer-chosen string.
_policy.register_no_answer_policy(
    DevCoverageV2,
    {
        "required_source_count": _policy.NoAnswerFieldPolicy.NON_TEXT,
        "available_source_count": _policy.NoAnswerFieldPolicy.NON_TEXT,
        "unavailable_required_sources": (_policy.NoAnswerFieldPolicy.CLOSED_VOCABULARY),
        "stale_required_sources": _policy.NoAnswerFieldPolicy.CLOSED_VOCABULARY,
        "as_of": _policy.NoAnswerFieldPolicy.NON_TEXT,
    },
    canonical={},
    vocabularies={
        "unavailable_required_sources": _policy.SOURCE_CLASS_VOCABULARY,
        "stale_required_sources": _policy.SOURCE_CLASS_VOCABULARY,
    },
)
