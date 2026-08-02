"""The versioned source/relationship matrix (CHAOS-3296, issue "Required
implementation" §1).

A closed, code-owned registry -- same ``frozenset``/``StrEnum`` posture as
``PLAN_REGISTRY``/``SourceClass`` -- naming, for every :class:`SourceClass`,
whether that source is a *direct* or *supporting* role and which
relationship tokens a fact from that source may legitimately claim on a
``dev_relationship_path.v1`` hop from a committed subject. An unapproved
relationship token fails construction (:func:`approved_relationship` returns
``False``, and the executor's path-minting never emits it), never merely a
lint/review finding.

Deliberately excludes ``PLAN_REGISTRY``/plan-execution concerns (CHAOS-3295)
and team-direct-scope semantics (CHAOS-3301) -- this module only answers "is
this (source_class, relationship) pair one the platform has proven it can
verify," independent of which plan or subject kind asked.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from ..contracts_v2.base import SourceClass
from ..work_graph_neighbors_service import ALLOWED_RELATIONSHIP_TYPES

__all__ = [
    "MIN_RELATIONSHIP_CONFIDENCE",
    "RelationshipMatrixEntry",
    "RELATIONSHIP_MATRIX",
    "approved_relationship",
]

#: A path minted below this confidence is treated the same as an unapproved
#: relationship type: never emitted. Distinguishes a "stale/low-confidence
#: edge" (acceptance criterion) from a genuinely absent one -- the edge may
#: still appear in ``content`` (the adapter's own bounded output is
#: untouched), it simply never contributes a verified relationship path.
MIN_RELATIONSHIP_CONFIDENCE = 0.3

Role = Literal["direct", "supporting"]
Requirement = Literal["required", "conditional", "not_applicable"]


@dataclass(frozen=True, slots=True)
class RelationshipMatrixEntry:
    source_class: SourceClass
    role: Role
    requirement: Requirement
    #: Closed vocabulary of relationship tokens a fact from this source may
    #: carry on a minted ``DevRelationshipPath.relationship``. Empty means
    #: this source class mints no per-fact relationship path at all (its
    #: facts are either not entity-linked -- source health -- or no adapter
    #: registered against the six core plans populates it yet).
    approved_relationship_types: frozenset[str]
    freshness_policy: str
    evidence_expansion_capability: bool


def _entry(
    source_class: SourceClass,
    *,
    role: Role,
    requirement: Requirement,
    approved_relationship_types: frozenset[str] = frozenset(),
    freshness_policy: str,
    evidence_expansion_capability: bool,
) -> RelationshipMatrixEntry:
    return RelationshipMatrixEntry(
        source_class=source_class,
        role=role,
        requirement=requirement,
        approved_relationship_types=approved_relationship_types,
        freshness_policy=freshness_policy,
        evidence_expansion_capability=evidence_expansion_capability,
    )


#: One entry per :class:`SourceClass` member -- completeness is asserted at
#: import time below, not left to a test to eventually notice drift.
RELATIONSHIP_MATRIX: dict[SourceClass, RelationshipMatrixEntry] = {
    entry.source_class: entry
    for entry in (
        _entry(
            SourceClass.STATUS_CHANGE,
            role="direct",
            requirement="required",
            # "status_assessment" covers the flattened declared/child/blocker
            # facts status_snapshot.v1 wires into content.status_facts (v1's
            # own wire shape already merges those three categories into one
            # list -- the executor cannot recover which sub-kind a flattened
            # fact came from, so it mints one honest, non-overclaiming label
            # rather than a fabricated finer-grained one). "required_child"
            # is reserved for content.required_children, which *does* stay
            # separately typed end to end. status.entity.v2's single
            # status_snapshot step is also the (only, today) adapter that
            # mints the linked_pull_request/linked_ci_run/linked_deployment/
            # linked_incident facts -- under a STATUS_CHANGE source
            # requirement, not a dedicated one -- so those tokens are
            # approved here too; the standalone PULL_REQUEST/CI_RUN/
            # DEPLOYMENT/INCIDENT entries below stay reserved for a future
            # *dedicated* adapter minting the same tokens under their own
            # source requirement.
            approved_relationship_types=frozenset(
                {
                    "status_assessment",
                    "required_child",
                    "observed_change",
                    "linked_pull_request",
                    "linked_ci_run",
                    "linked_deployment",
                    "linked_incident",
                }
            ),
            freshness_policy="status_snapshot_freshness.v1",
            evidence_expansion_capability=True,
        ),
        _entry(
            SourceClass.WORK_ITEM,
            role="supporting",
            requirement="conditional",
            approved_relationship_types=frozenset({"metric_scoped_to_subject"}),
            freshness_policy="metric_freshness.v1",
            evidence_expansion_capability=True,
        ),
        _entry(
            SourceClass.WORK_GRAPH,
            role="supporting",
            requirement="conditional",
            # Reuses the exact vocabulary work_graph_neighbors_service already
            # validates a request against (ALLOWED_RELATIONSHIP_TYPES) --
            # imported, not re-declared, so the two cannot drift apart.
            approved_relationship_types=frozenset(ALLOWED_RELATIONSHIP_TYPES),
            freshness_policy="work_graph_freshness.v1",
            evidence_expansion_capability=True,
        ),
        _entry(
            SourceClass.PULL_REQUEST,
            role="supporting",
            requirement="conditional",
            approved_relationship_types=frozenset({"linked_pull_request"}),
            freshness_policy="status_snapshot_freshness.v1",
            evidence_expansion_capability=True,
        ),
        _entry(
            SourceClass.CI_RUN,
            role="supporting",
            requirement="conditional",
            approved_relationship_types=frozenset({"linked_ci_run"}),
            freshness_policy="status_snapshot_freshness.v1",
            evidence_expansion_capability=True,
        ),
        _entry(
            SourceClass.DEPLOYMENT,
            role="supporting",
            requirement="conditional",
            approved_relationship_types=frozenset({"linked_deployment"}),
            freshness_policy="status_snapshot_freshness.v1",
            evidence_expansion_capability=True,
        ),
        _entry(
            SourceClass.INCIDENT,
            role="supporting",
            requirement="conditional",
            approved_relationship_types=frozenset({"linked_incident"}),
            freshness_policy="status_snapshot_freshness.v1",
            evidence_expansion_capability=True,
        ),
        _entry(
            SourceClass.SOURCE_HEALTH,
            role="supporting",
            requirement="required",
            # Source health describes a *source's* freshness/availability, not
            # a fact about the subject -- it never carries a relationship path
            # of its own (no DevSourceContent slot exists for it either; see
            # that contract's own docstring).
            freshness_policy="data_health_freshness.v1",
            evidence_expansion_capability=False,
        ),
        # Not yet minted by any of the six core plans' registered steps. Kept
        # in the matrix (drift test requires every SourceClass present) with
        # an honest empty vocabulary rather than a fabricated one --
        # CHAOS-3303's/3304's/3305's own adapters amend these entries when
        # they land, the same way they extend PLAN_REGISTRY.
        _entry(
            SourceClass.CODE_CHANGE,
            role="supporting",
            requirement="not_applicable",
            freshness_policy="unversioned",
            evidence_expansion_capability=False,
        ),
        _entry(
            SourceClass.REVIEW,
            role="supporting",
            requirement="not_applicable",
            freshness_policy="unversioned",
            evidence_expansion_capability=False,
        ),
        _entry(
            SourceClass.TEST_REPORT,
            role="supporting",
            requirement="not_applicable",
            freshness_policy="unversioned",
            evidence_expansion_capability=False,
        ),
        _entry(
            SourceClass.OPERATIONAL_CONTROL,
            role="supporting",
            requirement="not_applicable",
            freshness_policy="unversioned",
            evidence_expansion_capability=False,
        ),
    )
}

_missing = set(SourceClass) - set(RELATIONSHIP_MATRIX)
if _missing:
    raise RuntimeError(
        f"relationship_matrix.v1 is missing entries for: {sorted(_missing)}"
    )


def approved_relationship(source_class: SourceClass, relationship: str) -> bool:
    """Whether ``relationship`` is a token this source class may mint.

    ``False`` for any source class not in the matrix is unreachable (the
    import-time completeness check above guarantees every member has an
    entry), so this never needs a default/fallback branch of its own.
    """

    return relationship in RELATIONSHIP_MATRIX[source_class].approved_relationship_types
