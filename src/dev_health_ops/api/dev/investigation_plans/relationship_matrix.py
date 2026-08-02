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
from ..contracts_v2.result import DevSourceContent
from ..work_graph_neighbors_service import ALLOWED_RELATIONSHIP_TYPES

__all__ = [
    "APPROVED_CONTENT_SLOTS",
    "CONTENT_SLOT_FIELDS",
    "MAX_RELATIONSHIP_PATHS",
    "MIN_RELATIONSHIP_CONFIDENCE",
    "RelationshipMatrixEntry",
    "RELATIONSHIP_MATRIX",
    "approved_relationship",
    "content_slot_violations",
]

#: A path minted below this confidence is treated the same as an unapproved
#: relationship type: never emitted. Distinguishes a "stale/low-confidence
#: edge" (acceptance criterion) from a genuinely absent one -- the edge may
#: still appear in ``content`` (the adapter's own bounded output is
#: untouched), it simply never contributes a verified relationship path.
MIN_RELATIONSHIP_CONFIDENCE = 0.3

#: The most relationship paths one observation may carry -- must match
#: ``DevSourceObservation.relationship_paths``'s own ``max_length=25``
#: (``contracts_v2/result.py``). Codex finding (HIGH, 2026-08-01): a dense
#: but otherwise valid result minting more than 25 verified paths raised
#: ``too_long`` deep inside ``DevSourceObservation`` construction, turning a
#: real (if partial) answer into a whole-run ``internal_error``. The
#: executor applies this budget itself, deterministically, before
#: construction -- see ``executor._mint_relationship_paths``.
MAX_RELATIONSHIP_PATHS = 25

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


#: Every ``DevSourceContent`` collection field name -- the closed vocabulary
#: :func:`content_slot_violations` walks. Written out rather than derived
#: from ``DevSourceContent.model_fields`` so a future field addition there
#: is a visible, reviewed edit here too (the same posture as
#: ``PLAN_ID_BY_INTENT``'s written-out mapping), not a silently-included slot
#: no ``SourceClass`` entry below has judged yet.
CONTENT_SLOT_FIELDS: tuple[str, ...] = (
    "status_facts",
    "required_children",
    "pull_requests",
    "ci_checks",
    "deployments",
    "incidents",
    "graph_edges",
    "observed_changes",
    "metric_refs",
)

#: Which ``DevSourceContent`` slot(s) each ``SourceClass``'s own registered
#: builtin steps (``investigation_plans/builtin_steps.py``) are ever allowed
#: to populate. Codex finding (MEDIUM, 2026-08-01): nothing previously
#: enforced this at observation construction -- a step registered under one
#: source class returning content shaped for a *different* one (e.g.
#: STATUS_CHANGE content carrying ``graph_edges``) would be minted,
#: relationship-path-checked, persisted, and presented exactly like
#: legitimate content. One entry per :class:`SourceClass` -- completeness is
#: asserted at import time below, the same posture as ``RELATIONSHIP_MATRIX``
#: itself. Derived from -- and must stay in lockstep with -- which
#: ``_wire_*_content`` helper each registered step in ``builtin_steps.py``
#: calls: ``status_snapshot``/``change_summary`` both register under
#: STATUS_CHANGE (the six flattened status categories plus
#: ``observed_changes``); ``work_graph_expansion`` under WORK_GRAPH
#: (``graph_edges`` only); the metric steps under WORK_ITEM (``metric_refs``
#: only). SOURCE_HEALTH and every not-yet-adapted class mint no content at
#: all (``StepOutcome.content`` stays ``None`` for those steps), matching
#: ``RELATIONSHIP_MATRIX``'s own "empty vocabulary, not fabricated" posture
#: for the same not-yet-landed classes.
APPROVED_CONTENT_SLOTS: dict[SourceClass, frozenset[str]] = {
    SourceClass.STATUS_CHANGE: frozenset(
        {
            "status_facts",
            "required_children",
            "pull_requests",
            "ci_checks",
            "deployments",
            "incidents",
            "observed_changes",
        }
    ),
    SourceClass.WORK_ITEM: frozenset({"metric_refs"}),
    SourceClass.WORK_GRAPH: frozenset({"graph_edges"}),
    SourceClass.PULL_REQUEST: frozenset(),
    SourceClass.CI_RUN: frozenset(),
    SourceClass.DEPLOYMENT: frozenset(),
    SourceClass.INCIDENT: frozenset(),
    SourceClass.SOURCE_HEALTH: frozenset(),
    SourceClass.CODE_CHANGE: frozenset(),
    SourceClass.REVIEW: frozenset(),
    SourceClass.TEST_REPORT: frozenset(),
    SourceClass.OPERATIONAL_CONTROL: frozenset(),
}

_missing_content_slots = set(SourceClass) - set(APPROVED_CONTENT_SLOTS)
if _missing_content_slots:
    raise RuntimeError(
        f"content_slot_matrix.v1 is missing entries for: {sorted(_missing_content_slots)}"
    )
_unknown_content_fields = {
    field for slots in APPROVED_CONTENT_SLOTS.values() for field in slots
} - set(CONTENT_SLOT_FIELDS)
if _unknown_content_fields:
    raise RuntimeError(
        f"content_slot_matrix.v1 approves unknown DevSourceContent fields: "
        f"{sorted(_unknown_content_fields)}"
    )


def content_slot_violations(
    source_class: SourceClass, content: DevSourceContent
) -> tuple[str, ...]:
    """Every populated ``DevSourceContent`` field name ``source_class`` is
    not approved to carry, sorted for a deterministic limitation string.

    Empty for legitimate content -- including an all-empty
    ``DevSourceContent`` for a queried-but-empty result, which populates no
    field at all regardless of ``source_class``.
    """

    populated = {field for field in CONTENT_SLOT_FIELDS if getattr(content, field)}
    return tuple(sorted(populated - APPROVED_CONTENT_SLOTS[source_class]))
