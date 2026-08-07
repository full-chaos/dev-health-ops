"""Builds the response a perfectly-behaved arm would return.

**The golden response is derived from ground truth, never from the oracle.**
That separation is what makes ``test_golden_response_passes`` a real
assertion: if the golden builder read the oracle's own ``must_include`` list
and echoed it back, the test would pass for an oracle that asserts nothing
and for one that asserts everything, and it would prove neither.

What the oracle *does* supply is the query -- subjects, mode, axis, as-of,
allowed relation types -- exactly as a real arm receives it. Everything else
comes from :mod:`corpus.ground_truth` plus a per-oracle
:class:`Scenario` describing the environment (what is visible, what has been
deleted, what is degraded).
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime

from ..corpus import ground_truth as gt
from ..harness.contracts import (
    ArmOutcome,
    ArmResponse,
    ClaimKind,
    FactFlags,
    Invalidation,
    QueryMode,
    SourceCoverage,
    TemporalFact,
    TimeAxis,
)
from ..harness.oracle import Oracle

#: Query modes that ask about history rather than about a single instant.
#: For these, closed validity windows belong in the answer.
_HISTORY_MODES = frozenset(
    {
        QueryMode.TIMELINE,
        QueryMode.PRIOR_ATTEMPTS,
        QueryMode.SUPERSEDED_DECISIONS,
        QueryMode.RELATED_CHANGES,
        QueryMode.RECURRING_PATTERNS,
        QueryMode.CONFLICTS,
    }
)

_PROJECTION_VERSION = "temporal-projector.v1"


@dataclass(frozen=True)
class Scenario:
    """The environment an oracle is evaluated in.

    Environment, not expectation: nothing here says which facts are correct,
    only which facts are *reachable* and how the system is behaving.
    """

    visibility: gt.VisibilityContext = gt.ALPHA_FULL_VISIBILITY
    outcome: ArmOutcome = ArmOutcome.ANSWERED
    warnings: tuple[str, ...] = ()
    degraded_reasons: tuple[str, ...] = ()
    coverage: Mapping[str, SourceCoverage] = field(default_factory=dict)
    indexed_through: datetime | None = gt.REQUIRED_WATERMARK
    include_adversarial: bool = False
    suppress_fact_keys: frozenset[str] = frozenset()
    #: fact_key -> flags, for scenarios where the projector marks facts
    #: conflicting or stale.
    flags: Mapping[str, FactFlags] = field(default_factory=dict)
    answer_is_empty: bool = False


_DEFAULT = Scenario()

SCENARIOS: Mapping[str, Scenario] = {
    "O1_ci_prior_attempts": _DEFAULT,
    "O1_ci_prior_attempts_stale": Scenario(
        indexed_through=gt.STALLED_WATERMARK,
        warnings=("source_stale:temporal_graph.v1",),
        flags={
            "gt_ep1_touched": FactFlags(stale=True),
            "gt_ep2_touched": FactFlags(stale=True),
            "gt_ep3_touched": FactFlags(stale=True),
            "gt_ep4_sole_support": FactFlags(stale=True),
            "gt_ep5_web_repo": FactFlags(stale=True),
        },
    ),
    "O1_ci_prior_attempts_squash": Scenario(
        warnings=(
            "source_unavailable:work_graph_pr_commit:squash_merge_linkage_absent",
        ),
        coverage={
            "work_graph_pr_commit": SourceCoverage(
                source="work_graph_pr_commit",
                available=False,
                reason="squash_merge_linkage_absent",
                row_estimate=0,
            )
        },
    ),
    "O2_blocking_valid": _DEFAULT,
    "O2_blocking_observed": _DEFAULT,
    "O3_supersession": _DEFAULT,
    "O3_supersession_extraction_down": Scenario(
        degraded_reasons=("extraction_provider_unavailable",),
        coverage={
            "extraction": SourceCoverage(
                source="extraction",
                available=False,
                reason="provider_malformed_structured_output",
            )
        },
        suppress_fact_keys=frozenset({"gt_adr021_supersedes_adr014"}),
    ),
    "O3_supersession_deterministic_only": Scenario(
        degraded_reasons=("extraction_disallowed_by_policy",),
        coverage={
            "extraction": SourceCoverage(
                source="extraction",
                available=False,
                reason="provider_policy_disallows_model_providers",
            )
        },
        suppress_fact_keys=frozenset({"gt_adr021_supersedes_adr014"}),
    ),
    "O4_prior_attempts": _DEFAULT,
    "O4_prior_attempts_manipulated": Scenario(include_adversarial=True),
    "O4_prior_attempts_after_redaction": Scenario(
        visibility=gt.VisibilityContext(
            org_id=gt.ORG_ALPHA,
            visible_repos=gt.ALPHA_FULL_VISIBILITY.visible_repos,
            deleted_source_event_refs=frozenset({"sevt_ep_0004"}),
            redacted_source_event_refs=frozenset({"sevt_ep_0001"}),
        ),
        degraded_reasons=("provenance_redacted",),
    ),
    "O4_prior_attempts_after_revocation": Scenario(visibility=gt.ALPHA_WEB_REVOKED),
    "O4_prior_attempts_graph_outage": Scenario(
        outcome=ArmOutcome.UNAVAILABLE,
        answer_is_empty=True,
        indexed_through=None,
        degraded_reasons=("temporal_graph_unavailable",),
        coverage={
            "temporal_graph.v1": SourceCoverage(
                source="temporal_graph.v1",
                available=False,
                reason="graph_datastore_outage",
            )
        },
    ),
    "O5_conflicts": Scenario(
        flags={
            "gt_conflict_side_a": FactFlags(conflicting=True),
            "gt_conflict_side_b": FactFlags(conflicting=True),
        }
    ),
    "O5_conflicts_injected": Scenario(
        flags={
            "gt_conflict_side_a": FactFlags(conflicting=True, untrusted_content=True),
            "gt_conflict_side_b": FactFlags(conflicting=True, untrusted_content=True),
        }
    ),
    "O5_conflicts_poisoned": _DEFAULT,
    "O6_recurring_pattern": _DEFAULT,
    "O7_valid": _DEFAULT,
    "O7_null_valid_from": _DEFAULT,
    "O7_unpinned": Scenario(warnings=("temporal_default_time_bound_applied",)),
}


def _to_temporal_fact(
    source: gt.GroundTruthFact,
    *,
    axis: TimeAxis,
    as_of: datetime,
    apply_time_filter: bool,
    flags: FactFlags,
) -> TemporalFact:
    """Project a ground-truth fact onto the axis the query asked for.

    On the observed-time axis a validity window whose *end* had not yet been
    ingested is presented as still open, and its invalidation provenance is
    withheld. That is not a convenience: on 15 July, Dev Health genuinely did
    not know the window had closed, and reporting the closure would be
    answering the valid-time question while claiming the observed-time one.
    """
    valid_to = source.valid_to
    invalidated_by: Invalidation | None = None

    endpoint_known = True
    if (
        axis is TimeAxis.OBSERVED_TIME
        and apply_time_filter
        and source.invalidation_observed_at is not None
        and as_of < source.invalidation_observed_at
    ):
        endpoint_known = False

    if valid_to is not None and endpoint_known:
        invalidated_by = Invalidation(
            refs=source.evidence_refs or source.source_event_refs,
            invalidation_claim_kind=ClaimKind.OBSERVED,
        )
    elif not endpoint_known:
        valid_to = None

    return TemporalFact(
        fact_id=f"tf_{source.fact_key}",
        subject_ref=source.subject,
        predicate=source.predicate,
        object_ref=source.object,
        observed_at=source.observed_at,
        claim_kind=source.claim_kind,
        projection_version=_PROJECTION_VERSION,
        valid_from=source.valid_from,
        valid_to=valid_to,
        reference_time=source.valid_from,
        invalidated_by=invalidated_by,
        evidence_refs=source.evidence_refs,
        source_event_refs=source.source_event_refs,
        confidence=None,
        flags=flags,
    )


def golden_response(oracle: Oracle, arm: str) -> ArmResponse:
    """The response a correct arm returns for ``oracle``'s query."""
    scenario = SCENARIOS[oracle.oracle_id]
    query = oracle.query

    apply_time_filter = query.query_mode not in _HISTORY_MODES
    axis = query.axis or TimeAxis.VALID_TIME
    as_of = query.as_of or gt.TRIAL_NOW

    if scenario.answer_is_empty:
        facts: tuple[TemporalFact, ...] = ()
    else:
        predicates = (
            frozenset(query.allowed_relation_types)
            if query.allowed_relation_types
            else None
        )
        selected = gt.select(
            as_of=as_of,
            axis=axis.value,
            visibility=scenario.visibility,
            predicates=predicates,
            include_adversarial=scenario.include_adversarial,
            apply_time_filter=apply_time_filter,
            suppress_fact_keys=scenario.suppress_fact_keys,
        )
        facts = tuple(
            _to_temporal_fact(
                source,
                axis=axis,
                as_of=as_of,
                apply_time_filter=apply_time_filter,
                flags=scenario.flags.get(source.fact_key, FactFlags()),
            )
            for source in selected
        )[: query.max_results]

    return ArmResponse(
        arm=arm,
        outcome=scenario.outcome,
        query=query,
        facts=facts,
        warnings=scenario.warnings,
        degraded_reasons=scenario.degraded_reasons,
        source_coverage=dict(scenario.coverage),
        indexed_through=scenario.indexed_through,
        versions={
            "projection": _PROJECTION_VERSION,
            "query": query.schema_version,
        },
        truncated=False,
    )
