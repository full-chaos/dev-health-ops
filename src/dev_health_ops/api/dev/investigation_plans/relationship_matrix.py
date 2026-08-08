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
    _GRAPH_EVIDENCE_SOURCE_VERSION,
    _METRIC_EVIDENCE_SOURCE_VERSION,
    _STATUS_ENTITY_SOURCE_SYSTEM,
    _STATUS_EVIDENCE_SOURCE_VERSION,
    _bind_content,
    _ci_check_source_version,
    _ci_evidence_identity,
    _claim_projection,
    _metric_ref_id,
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
        # CHAOS-3304 merge reconciliation (SourceClass reconciliation,
        # 2026-08-02): COGNITIVE_LOAD/INVESTMENT_ALLOCATION landed on
        # ``contracts_v2.base.SourceClass`` via #1374, which never touched
        # this module -- 3304's health rules/adapters are wired directly
        # against ``user_metrics_daily``/``team_metrics_daily``/
        # ``investment_metrics_daily``, not through PLAN_REGISTRY/this
        # investigation-plan executor. Same "not yet landed here, honest
        # empty vocabulary" posture as CODE_CHANGE/REVIEW/TEST_REPORT/
        # OPERATIONAL_CONTROL above -- amend when a plan step actually
        # mints content under one of these classes.
        _entry(
            SourceClass.COGNITIVE_LOAD,
            role="supporting",
            requirement="not_applicable",
            freshness_policy="unversioned",
            evidence_expansion_capability=False,
        ),
        _entry(
            SourceClass.INVESTMENT_ALLOCATION,
            role="supporting",
            requirement="not_applicable",
            freshness_policy="unversioned",
            evidence_expansion_capability=False,
        ),
        # CHAOS-3297 stack #3: HealthRuleFinding/DeficiencyFinding are
        # DERIVED aggregates -- a health.project.v1/health.team.v1/
        # status.portfolio.v1/balance.team_workload.v1/deficiency.
        # operational.v1 step's own content IS the direct verdict for its
        # plan (unlike the "supporting" classes above), so role="direct".
        # Neither finding type carries a per-fact relationship-path claim
        # of its own the way a status/PR/CI/graph fact does (a finding is
        # already the terminal judgment over several such facts, not one
        # more entity-linked hop from the subject) -- empty relationship
        # vocabulary, same posture as SOURCE_HEALTH. Not evidence-expansion
        # capable: neither wire type resolves through EvidenceService's
        # ``get_evidence.v1`` the way a primary fact does (see
        # EVIDENCE_IDENTITY_TABLE's matching "accepted_risk" cells below
        # for the full rationale).
        _entry(
            SourceClass.HEALTH_PROFILE,
            role="direct",
            requirement="required",
            freshness_policy="health_profile_freshness.v1",
            evidence_expansion_capability=False,
        ),
        _entry(
            SourceClass.DEFICIENCY_INVENTORY,
            role="direct",
            requirement="required",
            freshness_policy="deficiency_inventory_freshness.v1",
            evidence_expansion_capability=False,
        ),
        # CHAOS-3567 (flag-off scaffold, no runtime behavior change): same
        # "not yet landed, honest empty vocabulary" posture as CODE_CHANGE/
        # REVIEW/TEST_REPORT/OPERATIONAL_CONTROL/COGNITIVE_LOAD/
        # INVESTMENT_ALLOCATION above -- no plan or step mints content under
        # TEMPORAL_CONTEXT yet, so it carries no relationship vocabulary and
        # is not evidence-expansion capable. Amend when the post-ADR
        # implementation issue actually wires a plan against it (see the
        # Linear project doc "CHAOS-3567: Ask Dev temporal source -- design
        # + registry-impact map", Context Fabric project).
        _entry(
            SourceClass.TEMPORAL_CONTEXT,
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
    # CHAOS-3297 stack #3: appended at the tail (lowest truncation
    # priority) rather than reasoned into the middle of the existing
    # order -- APPROVED_CONTENT_SLOTS below never lets a HEALTH_PROFILE/
    # DEFICIENCY_INVENTORY step co-populate these alongside any field
    # above (each source class populates exactly one slot set), so no
    # observation this priority order ever ranks against exists yet;
    # appending is the lowest-blast-radius choice for the six already-
    # shipped plans' own truncation behavior.
    "health_findings",
    "deficiency_findings",
    # CHAOS-3393: appended at the tail for the same reason -- no observation
    # this priority order ever ranks against exists yet (HEALTH_PROFILE's
    # portfolio adapter never co-populates status_facts/graph_edges/etc.
    # alongside this slot).
    "portfolio_project_statuses",
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
    # CHAOS-3304 merge reconciliation -- see the matching RELATIONSHIP_MATRIX
    # entries above for the full rationale.
    SourceClass.COGNITIVE_LOAD: frozenset(),
    SourceClass.INVESTMENT_ALLOCATION: frozenset(),
    # CHAOS-3297 stack #3 -- see the matching RELATIONSHIP_MATRIX entries
    # above. Each is the ONLY slot its own steps (registered in the new
    # wave_3_1_plans.py module) are ever allowed to populate.
    # CHAOS-3393: the portfolio adapter (status.portfolio.v1) is also
    # registered under HEALTH_PROFILE (a portfolio batch is several
    # HEALTH_PROFILE evaluations flattened into one -- see SourceClass.
    # HEALTH_PROFILE's own docstring), so it shares this source class's
    # approved-slots entry with the project/team health adapters.
    SourceClass.HEALTH_PROFILE: frozenset(
        {"health_findings", "portfolio_project_statuses"}
    ),
    SourceClass.DEFICIENCY_INVENTORY: frozenset({"deficiency_findings"}),
    # CHAOS-3567 (flag-off scaffold) -- no registered step mints content
    # under TEMPORAL_CONTEXT yet; see the matching RELATIONSHIP_MATRIX entry
    # above for the full rationale.
    SourceClass.TEMPORAL_CONTEXT: frozenset(),
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


def _status_entity(fact: Any) -> tuple[str, str]:
    entity_type, _sep, entity_id = fact.fact_id.partition(":")
    return entity_type, entity_id


def _identity_status_fact(fact: Any) -> tuple[str, str, str, str]:
    """``status_facts`` -- wired by ``_wire_status_snapshot_content``'s
    ``wire_status_fact``, which mints against the raw
    ``StatusFact.entity_type``/``entity_id`` pair the wire fact's own
    ``fact_id`` encodes as ``f"{entity_type}:{entity_id}"``.
    ``source_version`` binds :func:`~.builtin_steps._claim_projection` of
    ``fact`` itself (round 6: every ``DevStatusFactV2`` field except
    ``schema_version``/``evidence_ref_ids``, derived programmatically --
    never a hand-picked subset)."""

    entity_type, entity_id = _status_entity(fact)
    source_system = _STATUS_ENTITY_SOURCE_SYSTEM.get(entity_type, "work_items")
    source_version = _bind_content(
        _STATUS_EVIDENCE_SOURCE_VERSION, _claim_projection(fact)
    )
    return source_system, source_version, entity_type, entity_id


def _identity_required_child(fact: Any) -> tuple[str, str, str, str]:
    """``required_children`` -- the same raw ``StatusFact`` source as
    ``status_facts``, wired to a DIFFERENT shape (``DevRequiredChildFactV2``
    keeps ``text``=label and ``status`` as two SEPARATE fields, rather than
    one composed string). Projecting THIS model binds them as separate
    JSON keys, not a delimiter-joined string -- a forged fact cannot
    re-split a genuine "Repo: Alpha"/"blocked" claim into "Repo"/
    "Alpha: blocked" (or any other split producing the same composed
    string) and still verify, the collision round 6 specifically asked to
    rule out."""

    entity_type, entity_id = _status_entity(fact)
    source_system = _STATUS_ENTITY_SOURCE_SYSTEM.get(entity_type, "work_items")
    source_version = _bind_content(
        _STATUS_EVIDENCE_SOURCE_VERSION, _claim_projection(fact)
    )
    return source_system, source_version, entity_type, entity_id


def _identity_pull_request(fact: Any) -> tuple[str, str, str, str]:
    """``wire_pull_request`` -> ``mint_delivery(source_system="pull_requests",
    entity_type="pull_request", entity_id=fact.entity_id)``, ``source_version``
    binding :func:`~.builtin_steps._claim_projection` of ``fact`` (round 6:
    every ``DevPullRequestFactV2`` field except schema_version/
    evidence_ref_ids)."""

    source_version = _bind_content(
        _STATUS_EVIDENCE_SOURCE_VERSION, _claim_projection(fact)
    )
    return "pull_requests", source_version, "pull_request", fact.entity_id


def _identity_ci_check(fact: Any) -> tuple[str, str, str, str]:
    """``wire_ci`` coarsens ``fact.entity_id`` (strip the ``#check...``
    acceptance-check suffix) for the minted ``entity_id``, and mints against
    a ``source_version`` built by the exact same
    ``builtin_steps._ci_check_source_version`` this cell calls -- never a
    parallel reimplementation. That shared function embeds the FULL,
    uncoarsened id (round 4: distinguishes two checks on the same run) and a
    canonical digest of :func:`~.builtin_steps._claim_projection` of ``fact``
    (round 6: every ``DevCIFactV2`` field, programmatically derived --
    round 5 hand-picked only conclusion/required/skipped_required_work)."""

    lookup_entity_id = _ci_evidence_identity(fact.entity_id)
    source_version = _ci_check_source_version(
        fact.entity_id, claim=_claim_projection(fact)
    )
    return "ci_runs", source_version, "ci_run", lookup_entity_id


def _identity_deployment(fact: Any) -> tuple[str, str, str, str]:
    """``source_version`` binds :func:`~.builtin_steps._claim_projection`
    of ``fact`` (round 6: every ``DevDeploymentFactV2`` field)."""

    source_version = _bind_content(
        _STATUS_EVIDENCE_SOURCE_VERSION, _claim_projection(fact)
    )
    return "deployments", source_version, "deployment", fact.entity_id


def _identity_incident(fact: Any) -> tuple[str, str, str, str]:
    """``source_version`` binds :func:`~.builtin_steps._claim_projection`
    of ``fact`` (round 6: every ``DevIncidentFactV2`` field)."""

    source_version = _bind_content(
        _STATUS_EVIDENCE_SOURCE_VERSION, _claim_projection(fact)
    )
    return "incidents", source_version, "incident", fact.entity_id


def _identity_observed_change(change: Any) -> tuple[str, str, str, str]:
    """Mirrors ``_wire_change_summary_content``'s ``change_base_identity``
    closure exactly: a ``"relationship"``-category change mints against its
    own ``change_id`` (never ``entity_id``); a collision-prone category
    (``status``/``metric``) embeds ``change_id`` into ``source_version`` on
    top of minting against ``entity_id``; every other category mints
    straight against ``entity_id`` with the base source_version -- THEN
    every branch binds :func:`~.builtin_steps._claim_projection` of
    ``change`` (round 6: every ``DevObservedChangeV2`` field -- ``before``/
    ``after`` remain the only ones that actually vary meaningfully, since
    ``claim_kind``/``relationship_chain``/``metric_value``/
    ``metric_comparison_value`` never survive onto the wire type at all)."""

    category = change.category
    source_system = _CHANGE_CATEGORY_SOURCE_SYSTEM.get(category, "work_items")
    if category == "relationship":
        base_source_version = _CHANGE_EVIDENCE_SOURCE_VERSION
        entity_id = change.change_id
    elif category in _CHANGE_COLLISION_PRONE_CATEGORIES:
        base_source_version = f"{_CHANGE_EVIDENCE_SOURCE_VERSION}:{change.change_id}"
        entity_id = change.entity_id
    else:
        base_source_version = _CHANGE_EVIDENCE_SOURCE_VERSION
        entity_id = change.entity_id
    source_version = _bind_content(base_source_version, _claim_projection(change))
    return source_system, source_version, change.entity_type, entity_id


def _identity_graph_edge(edge: Any) -> tuple[str, str, str, str]:
    """``wire_edge`` mints against ``item.edge_id`` -- CHAOS-3296 round-4
    adds ``DevGraphEdgeV2.edge_id`` specifically so this identity survives
    to the wire; round 1-3 could not implement this cell at all.
    ``source_version`` binds :func:`~.builtin_steps._claim_projection` of
    ``edge`` (round 6: every ``DevGraphEdgeV2`` field, including
    ``provenance``/``confidence``/``observed_at`` -- round 5 hand-picked
    only relationship/orientation and Codex forged the rest on a genuine
    handle)."""

    source_version = _bind_content(
        _GRAPH_EVIDENCE_SOURCE_VERSION, _claim_projection(edge)
    )
    return "work_graph", source_version, "work_graph_edge", edge.edge_id


def _identity_metric_ref(ref: Any) -> tuple[str, str, str, str]:
    """``entity_id`` is INDEPENDENTLY RECOMPUTED from ``ref``'s own
    ``metric_id``/``dimensions``/``current_window`` via the exact same
    :func:`~.builtin_steps._metric_ref_id` mint used -- never trusted as a
    free-form ``ref.metric_ref_id`` the wire fact merely claims (round 6
    ask: "recompute/validate metric_ref_id from the same inputs"). A fact
    whose claimed ``metric_ref_id`` does not match what its OWN claimed
    metric_id/dimensions/window would genuinely hash to fails verification
    here structurally: the candidate's entity_id (this recomputed value)
    would not equal what a genuine handle for THAT metric_ref_id was ever
    signed against. ``source_version`` binds
    :func:`~.builtin_steps._claim_projection` of ``ref`` (round 6: every
    ``DevMetricRefV2`` field -- round 5 hand-picked only value/
    comparison_value, so Codex re-labeled the metric and forged its series
    while leaving those two fields untouched)."""

    entity_id = _metric_ref_id(
        metric_id=ref.metric_id.value,
        dimensions=ref.dimensions,
        window_start=ref.current_window.start.isoformat(),
        window_end=ref.current_window.end.isoformat(),
    )
    source_version = _bind_content(
        _METRIC_EVIDENCE_SOURCE_VERSION, _claim_projection(ref)
    )
    return "metrics", source_version, "metric", entity_id


EVIDENCE_IDENTITY_TABLE: dict[str, EvidenceIdentityCell] = {
    "status_facts": EvidenceIdentityCell(mode="required", derive=_identity_status_fact),
    "required_children": EvidenceIdentityCell(
        mode="required", derive=_identity_required_child
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
    # CHAOS-3297 stack #3 (team-lead amendment, 2026-08-02): "required" mode
    # verifies that a fact's OWN evidence_ref_ids resolve to a genuinely
    # minted handle whose identity matches what that SAME fact's own
    # wire fields would recompute -- it exists to catch a fabricated
    # PRIMARY fact minted against, or citing, the wrong identity.
    # HealthRuleFinding/DeficiencyFinding are not primary facts: they are
    # DERIVED judgments the health-rule/deficiency evaluation engines
    # compute over several already-canonical facts from OTHER source
    # classes (status_change/source_health/cognitive_load/investment_
    # allocation), so there is no single (source_system, source_version,
    # entity_type, entity_id) tuple on the finding itself for a "required"
    # cell's derive function to recompute and compare against -- and
    # HealthRuleFinding structurally has no evidence_ref_ids field at all
    # (contracts_v2.health_rules.HealthRuleFinding only names
    # evidence_source_classes, a closed SourceClass vocabulary, never a
    # specific handle). Per-finding evidence discipline is instead F10,
    # enforced at the CONTRACT layer by each finding type itself:
    # DeficiencyFinding.validate_evidence_or_classification requires
    # evidence_ref_ids XOR evidence_classification on every instance: a
    # finding that DOES cite real evidence still satisfies F10 by its own
    # construction, one layer up from this table. classification-only is
    # the accepted design (never "required" mode) for both cells.
    "health_findings": EvidenceIdentityCell(
        mode="accepted_risk",
        rationale=(
            "HealthRuleFinding is a derived aggregate with no per-finding "
            "evidence_ref_ids field to bind -- provenance is the closed "
            "evidence_source_classes vocabulary, never a specific minted "
            "handle; see this table's module-level comment above."
        ),
    ),
    "deficiency_findings": EvidenceIdentityCell(
        mode="accepted_risk",
        rationale=(
            "DeficiencyFinding is a derived aggregate over several "
            "already-canonical facts; its own F10 model_validator "
            "(validate_evidence_or_classification) enforces evidence_ref_ids "
            "XOR evidence_classification at the contract layer, which is "
            "the correct enforcement point for a derived finding -- this "
            "executor-level identity-binding check exists to catch a "
            "fabricated PRIMARY fact and has no single recomputable "
            "identity tuple to check a derived finding against."
        ),
    ),
    # CHAOS-3393: same posture as health_findings/deficiency_findings above
    # -- DevPortfolioProjectStatusV2 is itself a derived aggregate (worst
    # state + finding count rolled up from that project's own, already
    # HEALTH_PROFILE-identity-checked HealthRuleFinding set), with no
    # per-row evidence_ref_ids field to bind.
    "portfolio_project_statuses": EvidenceIdentityCell(
        mode="accepted_risk",
        rationale=(
            "DevPortfolioProjectStatusV2 is a derived rollup (worst state, "
            "finding count) over a project's own already-identity-checked "
            "health_findings -- it has no per-row evidence_ref_ids field to "
            "bind; per-finding evidence discipline is enforced one layer "
            "down, on health_findings itself."
        ),
    ),
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
