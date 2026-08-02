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
from typing import Any, Literal

from ..contracts_v2.base import SourceClass
from ..contracts_v2.result import DevSourceContent
from ..work_graph_neighbors_service import ALLOWED_RELATIONSHIP_TYPES

# CHAOS-3296 round-4 closure: imported BY REFERENCE, not duplicated by value
# -- unlike ``_STATUS_ENTITY_SOURCE_SYSTEM``'s existing by-value posture
# elsewhere in this package (circular-import driven), there is no cycle here
# (``builtin_steps.py`` imports neither this module nor ``executor.py``), so
# a real import is strictly stronger than a copy: if a minting call site's
# identity derivation ever changes shape, this import breaks at MODULE LOAD
# for the whole test suite, not just whenever a specific drift test happens
# to run. This is exactly the "the cell's test breaks if minting changes"
# property the round-4 closure requires -- enforced at the strongest
# available layer, with :func:`test_evidence_identity_table_matches_the_real_minting_call_sites`-style
# tests (see ``tests/api/dev/test_chaos_3296_round4_evidence_identity_table.py``)
# as the second, still-necessary layer for logic an import alone cannot
# catch (a constant staying correct while the *call* that uses it changes).
from .builtin_steps import (
    _CHANGE_CATEGORY_SOURCE_SYSTEM,
    _CHANGE_COLLISION_PRONE_CATEGORIES,
    _CHANGE_EVIDENCE_SOURCE_VERSION,
    _CI_ACCEPTANCE_CHECK_MARKER,
    _GRAPH_EVIDENCE_SOURCE_VERSION,
    _METRIC_EVIDENCE_SOURCE_VERSION,
    _STATUS_ENTITY_SOURCE_SYSTEM,
    _STATUS_EVIDENCE_SOURCE_VERSION,
)

__all__ = [
    "APPROVED_CONTENT_SLOTS",
    "CONTENT_SLOT_FIELDS",
    "EVIDENCE_IDENTITY_TABLE",
    "EvidenceIdentityCell",
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
#:
#: Dual-purpose (CHAOS-3296 Codex finding, HIGH, 2026-08-01): the order here
#: is ALSO ``executor._budgeted_observation``'s truncation priority --
#: issue body Section 6 "Evidence prioritization and bounds" ranks direct
#: verdict > required incomplete work > blocking/delivery gates > optional
#: context, which maps onto this exact field order (earliest = highest
#: priority = dropped last; ``metric_refs`` at the tail is dropped first).
#: One deliberate list rather than two that could silently drift apart --
#: a future field addition here is reviewed for BOTH its approved-slot
#: membership and where it ranks under budget pressure in the same edit.
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


# -- Evidence identity table (CHAOS-3296 round-4 closure) -------------------
#
# Codex round 3 (2026-08-02) confirmed the mint-receipt verification built in
# rounds 1-2 was reject-known-bad (flag a handle only if it is PRESENT and
# wrong) rather than require-known-good (every evidence-capable fact MUST
# cite >=1 receipted handle whose FULL identity matches). That shape left
# three holes: a fact with zero evidence_ref_ids skipped every check
# entirely; graph_edges was excluded from identity comparison because
# DevGraphEdgeV2 never preserved the edge_id minting bound identity to; and
# CI-check identity collapsed to the run level (entity_type/entity_id only),
# discarding the check-specific discriminator the real signer's HMAC
# actually binds into source_version.
#
# This table closes all three by construction: one cell per
# CONTENT_SLOT_FIELDS entry (import-time-total, same posture as
# APPROVED_CONTENT_SLOTS), each either "required" (every fact in that field
# must cite >=1 handle whose (source_system, source_version, entity_type,
# entity_id) -- the exact four fields the real EvidenceReferenceSigner binds
# into its HMAC, evidence_service.py's EvidenceReferenceSigner._payload --
# match what THIS field's own minting call site actually passed) or
# "accepted_risk" (a category the platform has explicitly decided may
# legitimately carry no evidence, with its own rationale -- there are
# currently none; every wire_* helper in builtin_steps.py mints
# unconditionally for every fact it constructs, so an empty cell here would
# be a fabricated exemption, not an honest one).


@dataclass(frozen=True, slots=True)
class EvidenceIdentityCell:
    mode: Literal["required", "accepted_risk"]
    #: ``None`` only for "accepted_risk" cells. Takes one fact object from
    #: the corresponding ``DevSourceContent`` field's tuple and returns
    #: exactly ``(source_system, source_version, entity_type, entity_id)``
    #: -- must match what that field's own ``builtin_steps.py`` wiring
    #: function passed to ``mint_evidence`` for that same fact, which the
    #: source-anchored tests prove by spying on a real mint call and
    #: comparing.
    derive: Any | None = None
    #: ``None`` for "required" cells; a short, reviewed sentence for
    #: "accepted_risk" ones explaining why no evidence is, and always will
    #: be, legitimate for that category.
    rationale: str | None = None


def _identity_status_like(fact: Any) -> tuple[str, str, str, str]:
    """``status_facts`` / ``required_children`` -- both wired by
    ``_wire_status_snapshot_content``'s ``mint_status``, which mints against
    the raw ``StatusFact.entity_type``/``entity_id`` pair the wire fact's own
    ``fact_id`` encodes as ``f"{entity_type}:{entity_id}"``."""

    entity_type, _sep, entity_id = fact.fact_id.partition(":")
    source_system = _STATUS_ENTITY_SOURCE_SYSTEM.get(entity_type, "work_items")
    return source_system, _STATUS_EVIDENCE_SOURCE_VERSION, entity_type, entity_id


def _identity_pull_request(fact: Any) -> tuple[str, str, str, str]:
    """``wire_pull_request`` -> ``mint_delivery(source_system="pull_requests",
    entity_type="pull_request", entity_id=fact.entity_id)`` with the default
    (unoverridden) ``source_version``."""

    return (
        "pull_requests",
        _STATUS_EVIDENCE_SOURCE_VERSION,
        "pull_request",
        fact.entity_id,
    )


def _identity_ci_check(fact: Any) -> tuple[str, str, str, str]:
    """``wire_ci`` coarsens ``fact.entity_id`` (strip the
    ``#check...`` acceptance-check suffix) for the minted ``entity_id``, but
    -- when coarsening actually changed anything -- embeds the FULL,
    uncoarsened id into ``source_version`` specifically so two checks on the
    same run mint distinct, non-interchangeable handles. Losing that
    discriminator (round 2's gap) let one check's handle verify another's
    fabricated fact; comparing ``source_version`` too closes it."""

    lookup_entity_id = fact.entity_id.split(_CI_ACCEPTANCE_CHECK_MARKER, 1)[0]
    source_version = (
        f"{_STATUS_EVIDENCE_SOURCE_VERSION}:{fact.entity_id}"
        if lookup_entity_id != fact.entity_id
        else _STATUS_EVIDENCE_SOURCE_VERSION
    )
    return "ci_runs", source_version, "ci_run", lookup_entity_id


def _identity_deployment(fact: Any) -> tuple[str, str, str, str]:
    return "deployments", _STATUS_EVIDENCE_SOURCE_VERSION, "deployment", fact.entity_id


def _identity_incident(fact: Any) -> tuple[str, str, str, str]:
    return "incidents", _STATUS_EVIDENCE_SOURCE_VERSION, "incident", fact.entity_id


def _identity_observed_change(change: Any) -> tuple[str, str, str, str]:
    """Mirrors ``_wire_change_summary_content``'s ``change_evidence_identity``
    closure exactly: a ``"relationship"``-category change mints against its
    own ``change_id`` (never ``entity_id``); a collision-prone category
    (``status``/``metric``) embeds ``change_id`` into ``source_version`` on
    top of minting against ``entity_id``; every other category mints
    straight against ``entity_id`` with the base source_version. Every input
    (``category``, ``change_id``, ``entity_id``, ``entity_type``) survives
    unchanged onto ``DevObservedChangeV2``, so this is fully reconstructible
    from the wire fact alone."""

    category = change.category
    source_system = _CHANGE_CATEGORY_SOURCE_SYSTEM.get(category, "work_items")
    if category == "relationship":
        return (
            source_system,
            _CHANGE_EVIDENCE_SOURCE_VERSION,
            change.entity_type,
            change.change_id,
        )
    if category in _CHANGE_COLLISION_PRONE_CATEGORIES:
        return (
            source_system,
            f"{_CHANGE_EVIDENCE_SOURCE_VERSION}:{change.change_id}",
            change.entity_type,
            change.entity_id,
        )
    return (
        source_system,
        _CHANGE_EVIDENCE_SOURCE_VERSION,
        change.entity_type,
        change.entity_id,
    )


def _identity_graph_edge(edge: Any) -> tuple[str, str, str, str]:
    """``wire_edge`` mints against ``item.edge_id`` -- CHAOS-3296 round-4
    adds ``DevGraphEdgeV2.edge_id`` specifically so this identity survives
    to the wire; round 1-3 could not implement this cell at all."""

    return "work_graph", _GRAPH_EVIDENCE_SOURCE_VERSION, "work_graph_edge", edge.edge_id


def _identity_metric_ref(ref: Any) -> tuple[str, str, str, str]:
    return "metrics", _METRIC_EVIDENCE_SOURCE_VERSION, "metric", ref.metric_ref_id


EVIDENCE_IDENTITY_TABLE: dict[str, EvidenceIdentityCell] = {
    "status_facts": EvidenceIdentityCell(mode="required", derive=_identity_status_like),
    "required_children": EvidenceIdentityCell(
        mode="required", derive=_identity_status_like
    ),
    "pull_requests": EvidenceIdentityCell(
        mode="required", derive=_identity_pull_request
    ),
    "ci_checks": EvidenceIdentityCell(mode="required", derive=_identity_ci_check),
    "deployments": EvidenceIdentityCell(mode="required", derive=_identity_deployment),
    "incidents": EvidenceIdentityCell(mode="required", derive=_identity_incident),
    "graph_edges": EvidenceIdentityCell(mode="required", derive=_identity_graph_edge),
    "observed_changes": EvidenceIdentityCell(
        mode="required", derive=_identity_observed_change
    ),
    "metric_refs": EvidenceIdentityCell(mode="required", derive=_identity_metric_ref),
}

_missing_evidence_cells = set(CONTENT_SLOT_FIELDS) - set(EVIDENCE_IDENTITY_TABLE)
if _missing_evidence_cells:
    raise RuntimeError(
        f"evidence_identity_table.v1 is missing cells for: {sorted(_missing_evidence_cells)}"
    )
_unknown_evidence_cells = set(EVIDENCE_IDENTITY_TABLE) - set(CONTENT_SLOT_FIELDS)
if _unknown_evidence_cells:
    raise RuntimeError(
        f"evidence_identity_table.v1 has cells for unknown fields: "
        f"{sorted(_unknown_evidence_cells)}"
    )
_malformed_evidence_cells = sorted(
    field
    for field, cell in EVIDENCE_IDENTITY_TABLE.items()
    if (cell.mode == "required") == (cell.derive is None)
    or (cell.mode == "accepted_risk") == (cell.rationale is None)
)
if _malformed_evidence_cells:
    raise RuntimeError(
        f"evidence_identity_table.v1 cells must pair mode='required' with a "
        f"derive function (never both/neither), and mode='accepted_risk' "
        f"with a rationale: {_malformed_evidence_cells}"
    )
