"""Native-arm adapter: today's ops/ACR read paths, against the pinned corpus.

A **baseline component** (harness-design.md §3.1), not a candidate. Grounded
entirely in ``docs/baseline-inventory.md``, with the same ``file:line``
evidence cited inline as ``degraded_reasons`` text, so a reader does not have
to cross-reference the appendix to see why a given oracle was declared
unanswerable rather than silently answered wrong.

**What native can genuinely answer today: Q7 only.**
``operational_service_repository_mappings`` carries real ``valid_from``/
``valid_to`` columns (baseline-inventory.md §4) and every as-of filter found
applies a genuine interval bound (``operational_edges.py:44-48``,
``source_queries.go:62``). The documented ``valid_from`` Nullable defect is
reproduced exactly -- not papered over -- so ``O7_null_valid_from`` measures
the real thing: a ``NULL`` comparison is false in ClickHouse, so a
null-started row is silently dropped, on both axes.

**Q2 is answered, but wrongly, on purpose.** ``work_graph_edges`` has no
``valid_to`` column at all (§5): there is no way to ask "what blocked this
on July 15" and get the July-15 answer. What native's real
``_BLOCKERS_SQL`` (``native_status_change.py:456-499``) actually returns
regardless of ``as_of``/``axis`` is the CURRENT blocker set -- reproduced
here rather than an invented empty gap, because "an arm that answers every
as-of question with present state" is corpus case C03's own named defect
(``corpus/cases.py``), not a hypothetical this adapter needs to invent.

**Everything else is an honest, declared gap**, not a wrong answer: no
episode read path (§7, Q1/Q4), no ``supersedes`` edge type (§5, Q3), no
conflict history because the RMT key omits ``discovered_at`` (§5, Q5), no
shared-cause edge type (§5, Q6).
"""

from __future__ import annotations

from ...corpus import ground_truth as gt
from ..contracts import (
    ArmOutcome,
    ArmResponse,
    ClaimKind,
    Invalidation,
    QueryMode,
    TemporalFact,
)
from ..oracle import Oracle
from ..runner import ArmRole

ARM_NAME = "native"
_PROJECTION_VERSION = "native.v1"

#: operational_service_repository_mappings-shaped rows this adapter can see.
#: Same fact_keys, same evidence_refs as ground_truth -- this describes the
#: SAME planted world from native's storage perspective, not a parallel
#: dataset that could drift from it.
_MAPPING_FACT_KEYS = ("gt_svc_repo_v1", "gt_svc_repo_v2", "gt_svc_repo_null_start")

#: work_graph_edges-shaped blocker rows (no valid_to column, so only ever
#: read as "current", never historically).
_BLOCKER_FACT_KEYS = ("gt_blocks_101_110", "gt_blocks_105_110_backfilled")


def _mapping_valid_at(row: gt.GroundTruthFact, as_of) -> bool:
    """``valid_from <= {as_of} AND (valid_to IS NULL OR valid_to > {as_of})``,
    with ClickHouse's NULL comparison semantics: ``NULL <= anything`` is
    false, unconditionally -- not Python's "None means unbounded" reading
    that :meth:`GroundTruthFact.true_at` uses. That divergence from ground
    truth is the point: it is the exact defect ``O7_null_valid_from``
    measures (baseline-inventory.md §4).
    """
    if row.valid_from is None:
        return False
    if row.valid_from > as_of:
        return False
    if row.valid_to is not None and row.valid_to <= as_of:
        return False
    return True


def _to_fact(row: gt.GroundTruthFact) -> TemporalFact:
    invalidated_by = None
    if row.valid_to is not None:
        # Self-evidencing: the SAME structured row carries its own closure
        # (see corpus/ground_truth.py's SELF_EVIDENCING_CLOSURES -- native
        # genuinely can represent this, since it is one canonical row with
        # both endpoints, not a narrative closure stated elsewhere).
        invalidated_by = Invalidation(
            refs=row.evidence_refs or row.source_event_refs,
            invalidation_claim_kind=ClaimKind.OBSERVED,
        )
    return TemporalFact(
        fact_id=f"tf_native_{row.fact_key}",
        subject_ref=row.subject,
        predicate=row.predicate,
        object_ref=row.object,
        observed_at=row.observed_at,
        claim_kind=row.claim_kind,
        projection_version=_PROJECTION_VERSION,
        valid_from=row.valid_from,
        valid_to=row.valid_to,
        reference_time=row.valid_from,
        invalidated_by=invalidated_by,
        evidence_refs=row.evidence_refs,
        source_event_refs=row.source_event_refs,
    )


def _degraded(oracle: Oracle, reason: str) -> ArmResponse:
    return ArmResponse(
        arm=ARM_NAME,
        outcome=ArmOutcome.ANSWERED,
        query=oracle.query,
        facts=(),
        degraded_reasons=(reason,),
        indexed_through=gt.TRIAL_NOW,
        versions={"native": _PROJECTION_VERSION},
    )


def _answer_q7(oracle: Oracle) -> ArmResponse:
    query = oracle.query
    rows = [gt.GROUND_TRUTH_BY_KEY[key] for key in _MAPPING_FACT_KEYS]
    subjects = frozenset(query.subjects) if query.subjects else None
    candidates = [
        row
        for row in rows
        if subjects is None or row.subject in subjects or row.object in subjects
    ]

    if query.query_mode is QueryMode.AS_OF:
        as_of = query.as_of or gt.TRIAL_NOW
        selected = [row for row in candidates if _mapping_valid_at(row, as_of)]
        return ArmResponse(
            arm=ARM_NAME,
            outcome=ArmOutcome.ANSWERED,
            query=query,
            facts=tuple(_to_fact(row) for row in selected),
            indexed_through=gt.TRIAL_NOW,
            versions={"native": _PROJECTION_VERSION},
        )

    # TIMELINE / unpinned (O7_unpinned): no server default exists
    # (read_adapter.go:64-65 passes as_of/time_window through verbatim), so
    # native returns everything unbounded and does NOT declare that a bound
    # was applied -- the trial's bounded default is its own contribution,
    # not inherited behaviour. This is an honest, expected gap: O7_unpinned
    # requires `temporal_default_time_bound_applied`, which native has no
    # concept of.
    return ArmResponse(
        arm=ARM_NAME,
        outcome=ArmOutcome.ANSWERED,
        query=query,
        facts=tuple(_to_fact(row) for row in candidates),
        indexed_through=gt.TRIAL_NOW,
        versions={"native": _PROJECTION_VERSION},
    )


def _answer_q2_current_state(oracle: Oracle) -> ArmResponse:
    rows = [gt.GROUND_TRUTH_BY_KEY[key] for key in _BLOCKER_FACT_KEYS]
    current = [row for row in rows if row.true_at(gt.TRIAL_NOW)]
    return ArmResponse(
        arm=ARM_NAME,
        outcome=ArmOutcome.ANSWERED,
        query=oracle.query,
        facts=tuple(_to_fact(row) for row in current),
        degraded_reasons=("no_relationship_valid_time:work_graph_edges_no_valid_to",),
        indexed_through=gt.TRIAL_NOW,
        versions={"native": _PROJECTION_VERSION},
    )


_DEGRADED_REASON_BY_QUESTION = {
    "Q1": "no_episode_read_path:acr_internal_storage_interfaces_go_85_91",
    "Q4": "no_episode_read_path:acr_internal_storage_interfaces_go_85_91",
    "Q3": "no_supersedes_edge_type:ops_work_graph_models_EdgeType_37_84",
    "Q5": "no_conflict_history:work_graph_edges_RMT_key_omits_discovered_at",
    "Q6": "no_shared_cause_edge_type:ops_work_graph_models_EdgeType_37_84",
}


def answer(oracle: Oracle) -> ArmResponse:
    """The response today's native ops/ACR read paths would give.

    Registered as a baseline component (``ArmRole.BASELINE_COMPONENT``), not
    a candidate -- see :mod:`harness.arms`'s module docstring for the shape
    of what step 1 can and cannot prove.
    """
    if oracle.question_id == "Q7":
        return _answer_q7(oracle)
    if oracle.question_id == "Q2":
        return _answer_q2_current_state(oracle)
    reason = _DEGRADED_REASON_BY_QUESTION.get(
        oracle.question_id, "native_arm_has_no_mapping_for_this_question"
    )
    return _degraded(oracle, reason)


# The one role this arm may ever be registered under -- see
# ArmRegistry.register's enforcement (harness/runner.py) and finding 10's
# ruling on harness/arms/extraction.py.
answer.declared_role = ArmRole.BASELINE_COMPONENT  # type: ignore[attr-defined]
