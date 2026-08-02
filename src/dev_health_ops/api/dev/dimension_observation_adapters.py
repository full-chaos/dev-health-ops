"""Typed ``dimension_observation.v1`` adapters (CHAOS-3303).

Pure functions that turn an already-computed canonical service result
(:class:`~.data_health_service.DataHealthResult`,
:class:`~.status_change_service.StatusSnapshotResult`,
:class:`~.metrics.service.MetricQueryResult`) into a
:class:`~.contracts_v2.health_rules.DimensionObservation` for exactly one
:class:`~.contracts_v2.health_rules.HealthRuleDefinition`. No adapter here
computes a NEW metric, ratio, or threshold -- each one only re-expresses a
value the canonical service already produced in the shape
``evaluate_rule`` requires. A rule whose ``comparison_unit`` has no
canonical, scope-safe source yet (see ``health_profile_synthesis``'s
``UNBOUND_RULE_LIMITATIONS``) is never approximated by a different unit's
value here -- it is reported honestly via :func:`unavailable_observation`
instead, matching "No missing data as zero" and "No model-created
dimension, severity, percentage, or finding".

Reuses ``investigation_plans.state_mapping`` for every canonical
service-state -> ``SourceRequirementState`` mapping rather than
re-deriving it (CHAOS-3295's own module, already exhaustive/mypy-proven).
``DimensionObservation.validate_zero_semantics`` enforces one invariant
this module leans on throughout: whenever ``current_value`` is not
``None``, ``data_semantics`` must be ``"measured_zero"`` -- *regardless* of
whether the value itself happens to be numerically zero. ``"measured_zero"``
therefore means "this dimension carries a real, queried value", not "the
value is literally 0"; :func:`_value_semantics` below is the one place that
distinction is applied so every adapter agrees on it.
"""

from __future__ import annotations

from datetime import datetime

from .contracts_v2.base import SourceRequirementState
from .contracts_v2.health_rules import (
    DataSemantics,
    DimensionObservation,
    RuleApplicability,
)
from .data_health_service import DataHealthResult
from .investigation_plans.state_mapping import (
    UNMEASURED_REQUIREMENT_STATES,
    data_health_state_to_requirement_state,
    metric_data_state_to_requirement_state,
    status_result_state_to_requirement_state,
)
from .metrics.service import MetricDataState, MetricQueryResult
from .native_team_workload import TeamCognitiveLoadResult, TeamInvestmentMixResult
from .status_change_service import StatusSnapshotResult

__all__ = [
    "after_hours_pressure_observation",
    "change_failure_rate_observation",
    "data_trust_observation",
    "incident_load_observation",
    "investment_allocation_shift_observation",
    "pr_interruption_load_observation",
    "review_request_load_observation",
    "unavailable_observation",
]

#: Severity order (least to most available) for picking the worst mapped
#: state across several required data-health sources -- mirrors
#: ``investigation_plans.builtin_steps._data_health_outcome``'s own table
#: (that helper is module-private, so this is a deliberate, small,
#: independently-testable re-derivation over the same public
#: ``state_mapping`` output, not a duplicate of its business logic).
_STATE_SEVERITY: dict[SourceRequirementState, int] = {
    SourceRequirementState.UNAUTHORIZED_OR_NOT_VISIBLE: 0,
    SourceRequirementState.UNAVAILABLE: 1,
    SourceRequirementState.UNCONFIGURED: 2,
    SourceRequirementState.AVAILABLE_STALE: 3,
    SourceRequirementState.AVAILABLE_UNKNOWN: 4,
    SourceRequirementState.AVAILABLE_CURRENT: 5,
    SourceRequirementState.NOT_APPLICABLE: 6,
    SourceRequirementState.TRUNCATED: 3,
}


def _value_semantics(current_value: float | None) -> DataSemantics:
    """``measured_zero`` iff a real value is present -- see module docstring."""

    return "measured_zero" if current_value is not None else "no_data"


def unavailable_observation(
    *,
    subject_kind: RuleApplicability,
    subject_id: str,
    cohort_size: int | None,
    window_index: int,
    observed_at: datetime,
    state: SourceRequirementState = SourceRequirementState.UNAVAILABLE,
) -> DimensionObservation:
    """An honest, structural gap: no canonical source is wired for this rule yet.

    Never a stand-in for a real zero -- ``evaluate_rule`` maps every
    ``not_measured`` observation straight to ``DimensionState.UNKNOWN``
    (never ``healthy``), so a caller cannot mistake this for "measured and
    fine".
    """

    if state not in UNMEASURED_REQUIREMENT_STATES:
        raise ValueError(f"{state!r} is not an unmeasured SourceRequirementState")
    return DimensionObservation(
        schema_version="dimension_observation.v1",
        subject_kind=subject_kind,
        subject_id=subject_id,
        cohort_size=cohort_size,
        observed_states=(state,),
        data_semantics="not_measured",
        sample_count=None,
        coverage=0.0,
        current_value=None,
        comparison_value=None,
        denominator_present=False,
        attribution_present=False,
        window_index=window_index,
        observed_at=observed_at,
    )


def data_trust_observation(
    result: DataHealthResult,
    *,
    subject_kind: RuleApplicability,
    subject_id: str,
    cohort_size: int | None,
    window_index: int,
    observed_at: datetime,
) -> DimensionObservation:
    """``health_rule.data_trust_broken.v1``: deterministic, from ``complete_eligible``.

    ``current_value`` is ``1.0`` (broken -- triggers the deterministic
    condition) when ``complete_eligible`` is ``False``, else ``0.0``. The
    worst individually-mapped source state across ``result.sources`` decides
    whether the dimension was measured at all: if every required source is
    itself unmeasured (unconfigured/unavailable/unauthorized), the whole
    dimension is honestly unmeasured too, never a fabricated "not broken".
    """

    if not result.sources:
        return DimensionObservation(
            schema_version="dimension_observation.v1",
            subject_kind=subject_kind,
            subject_id=subject_id,
            cohort_size=cohort_size,
            observed_states=(SourceRequirementState.AVAILABLE_CURRENT,),
            data_semantics="no_data",
            sample_count=0,
            coverage=0.0,
            current_value=None,
            comparison_value=None,
            denominator_present=False,
            attribution_present=False,
            window_index=window_index,
            observed_at=observed_at,
        )
    mapped = [data_health_state_to_requirement_state(s.state) for s in result.sources]
    worst = min(mapped, key=lambda state: _STATE_SEVERITY[state])
    coverage = sum(s.coverage for s in result.sources) / len(result.sources)
    if worst in UNMEASURED_REQUIREMENT_STATES:
        return unavailable_observation(
            subject_kind=subject_kind,
            subject_id=subject_id,
            cohort_size=cohort_size,
            window_index=window_index,
            observed_at=observed_at,
            state=worst,
        )
    current_value = 0.0 if result.complete_eligible else 1.0
    return DimensionObservation(
        schema_version="dimension_observation.v1",
        subject_kind=subject_kind,
        subject_id=subject_id,
        cohort_size=cohort_size,
        observed_states=(worst,),
        data_semantics=_value_semantics(current_value),
        sample_count=len(result.sources),
        coverage=coverage,
        current_value=current_value,
        comparison_value=None,
        denominator_present=False,
        attribution_present=False,
        window_index=window_index,
        observed_at=observed_at,
    )


def incident_load_observation(
    snapshot: StatusSnapshotResult,
    *,
    subject_kind: RuleApplicability,
    subject_id: str,
    cohort_size: int | None,
    window_index: int,
    observed_at: datetime,
) -> DimensionObservation:
    """``health_rule.incident_load.v1``: the raw incident count from a status snapshot.

    ``StatusSnapshotResult.incidents`` is a real, already-queried fact list
    (CHAOS-3295's mandatory ``status_snapshot`` step); its length is exactly
    the rule's ``incident_count`` comparison unit -- no derived ratio, no
    invented threshold. A snapshot whose overall state never queried
    anything (``INSUFFICIENT_EVIDENCE`` -> ``UNAVAILABLE``) is reported
    unmeasured rather than a fabricated zero-incident count.
    """

    mapped = status_result_state_to_requirement_state(snapshot.state)
    if mapped in UNMEASURED_REQUIREMENT_STATES:
        return unavailable_observation(
            subject_kind=subject_kind,
            subject_id=subject_id,
            cohort_size=cohort_size,
            window_index=window_index,
            observed_at=observed_at,
            state=mapped,
        )
    current_value = float(len(snapshot.incidents))
    return DimensionObservation(
        schema_version="dimension_observation.v1",
        subject_kind=subject_kind,
        subject_id=subject_id,
        cohort_size=cohort_size,
        observed_states=(mapped,),
        data_semantics=_value_semantics(current_value),
        sample_count=len(snapshot.incidents),
        coverage=1.0,
        current_value=current_value,
        comparison_value=None,
        denominator_present=False,
        attribution_present=False,
        window_index=window_index,
        observed_at=observed_at,
    )


def change_failure_rate_observation(
    result: MetricQueryResult,
    *,
    subject_kind: RuleApplicability,
    subject_id: str,
    cohort_size: int | None,
    window_index: int,
    observed_at: datetime,
) -> DimensionObservation:
    """``health_rule.change_failure_rate.v1``: the canonical ``CHANGE_FAILURE_RATE`` metric.

    Reuses ``MetricQueryService``'s own ``change_failure_rate`` computation
    verbatim -- this adapter only re-expresses ``MetricQueryResult`` as a
    ``DimensionObservation``, it never recomputes the ratio. A zero
    denominator (``MetricDataState.INSUFFICIENT_EVIDENCE``, the metric
    service's own "no denominator" projection) is reported with
    ``denominator_present=False``, never as a healthy zero rate.
    """

    mapped = metric_data_state_to_requirement_state(result.state)
    if mapped in UNMEASURED_REQUIREMENT_STATES:
        return unavailable_observation(
            subject_kind=subject_kind,
            subject_id=subject_id,
            cohort_size=cohort_size,
            window_index=window_index,
            observed_at=observed_at,
            state=mapped,
        )
    value = result.values[0] if result.values else None
    current_value = value.value if value is not None else None
    comparison_value = value.comparison_value if value is not None else None
    denominator_present = (
        result.state is not MetricDataState.INSUFFICIENT_EVIDENCE
        and current_value is not None
    )
    return DimensionObservation(
        schema_version="dimension_observation.v1",
        subject_kind=subject_kind,
        subject_id=subject_id,
        cohort_size=cohort_size,
        observed_states=(mapped,),
        data_semantics=_value_semantics(current_value),
        sample_count=None,
        coverage=result.coverage,
        current_value=current_value,
        comparison_value=comparison_value,
        denominator_present=denominator_present,
        attribution_present=False,
        window_index=window_index,
        observed_at=observed_at,
    )


# ---------------------------------------------------------------------------
# CHAOS-3304: team workload / investment-balance adapters.
#
# Both sources below (``TeamCognitiveLoadResult``/``TeamInvestmentMixResult``,
# ``native_team_workload.py``) are already team-scoped by construction: every
# underlying query filters ``team_id`` directly on tables that carry it at
# ingest time (unlike status/change facts, which re-derive an owned-
# repository set from ``team_repo_ownership``). But "scoped by the column"
# is not "attributed by the canonical resolver" -- see
# ``native_team_workload``'s module docstring for the CHAOS-3331 finding.
#
# ALL FOUR adapters below report ``attribution_present=False`` for a
# genuinely measured result (Codex-confirmed finding, round 2, 2026-08-02,
# corrects this module's earlier claim that ``investment_allocation_shift_
# observation`` was exempt): ``user_metrics_daily``/``team_metrics_daily``
# writers (``compute.py``, ``compute_wellbeing.py``) use a legacy repo-
# pattern/identity-map resolver that never consults canonical attribution
# at all; ``investment_metrics_daily``'s writer (``job_work_items.py``)
# resolves via the canonical ``resolve_team_attribution`` +
# ``attribution_context`` path in the common case, but that path's
# ``attribution_context`` load can itself fail and fall open to the SAME
# legacy resolver chain -- and no field anywhere records which path
# produced a given row, so this module cannot tell them apart on the read
# side. Each of the four rules already carries
# ``attribution_required=True`` (``health_rule_registry.py``), so this
# disclosure alone -- no new field, no protocol change -- correctly
# suppresses every finding from all four rules to
# ``UNKNOWN``/``missing_attribution`` via ``evaluate_rule``'s existing
# guardrail, until CHAOS-3331 lands and this module is updated to match.
# ---------------------------------------------------------------------------


def after_hours_pressure_observation(
    result: TeamCognitiveLoadResult,
    *,
    subject_kind: RuleApplicability,
    subject_id: str,
    cohort_size: int | None,
    comparison_value: float | None,
    window_index: int,
    observed_at: datetime,
) -> DimensionObservation:
    """``health_rule.after_hours_pressure_sustained.v1``: team-scoped after-hours commit ratio.

    A ratio (after-hours commits / total commits, ``team_metrics_daily``'s
    own AVG-of-latest-per-team projection) is already self-normalized
    against the team's own commit population -- unlike a raw count, it
    needs no separate contributor-count denominator to be meaningful.
    ``denominator_present`` therefore tracks only whether the ratio was
    genuinely computed at all (``result.measured``), matching the rule's
    own ``denominator_required=False``. Per the PRD/TRD, this is a
    *pressure signal only* -- the caller must never treat this dimension
    alone as a burden/overburdened conclusion (enforced structurally by
    ``qualify_team_needs_attention``'s two-independent-dimension
    requirement, not by this adapter).

    ``attribution_present=False`` unconditionally (CHAOS-3331, module
    docstring): ``team_metrics_daily``'s writer resolves ``team_id`` via a
    legacy repo-pattern/identity-map resolver, never the canonical
    ``resolve_team_attribution``. Paired with this rule's own
    ``attribution_required=True``, every finding here is honestly
    suppressed to ``UNKNOWN``/``missing_attribution`` until that gap
    closes -- never a fabricated ``watch``/``at_risk`` on team attribution
    this module cannot actually vouch for.
    """

    if not result.measured or result.after_hours_commit_ratio is None:
        return unavailable_observation(
            subject_kind=subject_kind,
            subject_id=subject_id,
            cohort_size=cohort_size,
            window_index=window_index,
            observed_at=observed_at,
        )
    current_value = result.after_hours_commit_ratio
    return DimensionObservation(
        schema_version="dimension_observation.v1",
        subject_kind=subject_kind,
        subject_id=subject_id,
        cohort_size=cohort_size,
        observed_states=(SourceRequirementState.AVAILABLE_CURRENT,),
        data_semantics=_value_semantics(current_value),
        sample_count=result.sample_days,
        coverage=1.0,
        current_value=current_value,
        comparison_value=comparison_value,
        denominator_present=True,
        # CHAOS-3331: legacy resolver, not canonical primary attribution --
        # see native_team_workload.COGNITIVE_LOAD_ATTRIBUTION_PROVENANCE_LIMITATION.
        attribution_present=False,
        window_index=window_index,
        observed_at=observed_at,
    )


def _per_active_contributor_observation(
    raw_value: float | None,
    *,
    measured: bool,
    sample_days: int,
    active_contributor_count: int | None,
    subject_kind: RuleApplicability,
    subject_id: str,
    cohort_size: int | None,
    comparison_value: float | None,
    window_index: int,
    observed_at: datetime,
) -> DimensionObservation:
    """Shared shape for a raw cognitive-load count divided by the team's
    observed active-contributor count (denominator-contract preference
    order item 2: "observed active work/review population").

    Configured team membership (order item 1) has no canonical, scope-safe
    source wired yet -- see ``native_team_workload.ClickHouseTeamWorkloadSource``'s
    module docstring. When ``active_contributor_count`` is ``None`` or zero,
    the raw count is still reported (so the finding can honestly say
    "measured pressure, denominator unavailable" rather than nothing at
    all), but ``denominator_present=False`` -- the rule's own
    ``denominator_required=True`` then suppresses any burden conclusion to
    ``UNKNOWN``/``missing_denominator`` (PRD 8.1's "not calculable").

    ``attribution_present=False`` unconditionally (CHAOS-3331): shared by
    both callers of this helper (``review_request_load_observation``,
    ``pr_interruption_load_observation``), both sourced from
    ``user_metrics_daily``, whose writer uses the same legacy repo-pattern/
    identity-map resolver as ``team_metrics_daily`` -- see
    ``after_hours_pressure_observation``'s docstring for the full
    rationale.
    """

    if not measured or raw_value is None:
        return unavailable_observation(
            subject_kind=subject_kind,
            subject_id=subject_id,
            cohort_size=cohort_size,
            window_index=window_index,
            observed_at=observed_at,
        )
    denominator_present = bool(
        active_contributor_count and active_contributor_count > 0
    )
    current_value = (
        raw_value / active_contributor_count
        if denominator_present and active_contributor_count is not None
        else raw_value
    )
    return DimensionObservation(
        schema_version="dimension_observation.v1",
        subject_kind=subject_kind,
        subject_id=subject_id,
        cohort_size=cohort_size,
        observed_states=(SourceRequirementState.AVAILABLE_CURRENT,),
        data_semantics=_value_semantics(current_value),
        sample_count=sample_days,
        coverage=1.0,
        current_value=current_value,
        comparison_value=comparison_value,
        denominator_present=denominator_present,
        # CHAOS-3331: legacy resolver, not canonical primary attribution --
        # see native_team_workload.COGNITIVE_LOAD_ATTRIBUTION_PROVENANCE_LIMITATION.
        attribution_present=False,
        window_index=window_index,
        observed_at=observed_at,
    )


def review_request_load_observation(
    result: TeamCognitiveLoadResult,
    *,
    active_contributor_count: int | None,
    subject_kind: RuleApplicability,
    subject_id: str,
    cohort_size: int | None,
    comparison_value: float | None,
    window_index: int,
    observed_at: datetime,
) -> DimensionObservation:
    """``health_rule.review_request_load_pressure.v1``: review requests per active contributor."""

    return _per_active_contributor_observation(
        result.review_request_load,
        measured=result.measured,
        sample_days=result.sample_days,
        active_contributor_count=active_contributor_count,
        subject_kind=subject_kind,
        subject_id=subject_id,
        cohort_size=cohort_size,
        comparison_value=comparison_value,
        window_index=window_index,
        observed_at=observed_at,
    )


def pr_interruption_load_observation(
    result: TeamCognitiveLoadResult,
    *,
    active_contributor_count: int | None,
    subject_kind: RuleApplicability,
    subject_id: str,
    cohort_size: int | None,
    comparison_value: float | None,
    window_index: int,
    observed_at: datetime,
) -> DimensionObservation:
    """``health_rule.pr_interruption_load_pressure.v1``: PR interruptions per active contributor."""

    return _per_active_contributor_observation(
        result.pr_interruption_load,
        measured=result.measured,
        sample_days=result.sample_days,
        active_contributor_count=active_contributor_count,
        subject_kind=subject_kind,
        subject_id=subject_id,
        cohort_size=cohort_size,
        comparison_value=comparison_value,
        window_index=window_index,
        observed_at=observed_at,
    )


def investment_allocation_shift_observation(
    current: TeamInvestmentMixResult,
    comparison: TeamInvestmentMixResult | None,
    *,
    subject_kind: RuleApplicability,
    subject_id: str,
    cohort_size: int | None,
    window_index: int,
    observed_at: datetime,
) -> DimensionObservation:
    """``health_rule.investment_allocation_shift.v1``: |new-value share shift| between windows.

    Deliberately magnitude-only (``abs(current_share - comparison_share)``),
    never signed -- a shift *toward* new-value work is not reported as
    "better" than a shift *away* from it (PRD 6.6/10's "No feature-work
    value judgment" guardrail). A team with high KTLO/security/infra but a
    *stable* mix reports ``current_value=0.0`` (``measured_zero`` -- a
    shift was genuinely computed and found to be zero), never a value
    judgment about the mix's composition itself.

    Computing a shift requires a genuinely measured comparison window; a
    missing/unmeasured comparison is reported as ``no_data``
    (``current_value=None``), never coerced to ``0.0`` -- collapsing
    "no baseline to compare against" into "no shift observed" would be
    exactly the "missing data as zero" the platform-wide contract forbids.

    ``attribution_present=False`` unconditionally (Codex-confirmed finding,
    round 2, 2026-08-02, corrects this adapter's own earlier claim of
    exemption): ``investment_metrics_daily``'s writer
    (``metrics/job_work_items.py``) resolves ``team_id`` via
    ``resolve_team_attribution`` + ``attribution_context`` in the common
    case, but the ``attribution_context`` load itself is wrapped in a
    ``try/except`` that FAILS OPEN -- a load failure continues with
    ``attribution_context=None``, and ``resolve_team_attribution`` then
    falls back through its legacy/native/project/assignee candidate chain
    and still writes a row. There is no field on ``TeamInvestmentMixResult``
    (or anywhere in ``investment_metrics_daily`` itself) recording which
    path produced a given row, so this adapter cannot distinguish a
    canonically-attributed row from a fail-open one on the read side. This
    rule is therefore CHAOS-3331-blocked exactly like the three
    cognitive-load adapters above (ratified team-lead decision,
    2026-08-02: the writer-side provenance fix belongs to CHAOS-3331, not
    this branch) -- see
    ``health_rule_registry.CHAOS_3331_BLOCKED_SOURCE_CLASSES``. The
    exemption returns once CHAOS-3331 persists which attribution path
    produced each row on the writer path itself.
    """

    if not current.measured or current.total_units <= 0:
        return unavailable_observation(
            subject_kind=subject_kind,
            subject_id=subject_id,
            cohort_size=cohort_size,
            window_index=window_index,
            observed_at=observed_at,
        )
    current_share = current.new_value_share
    if current_share is None:
        return unavailable_observation(
            subject_kind=subject_kind,
            subject_id=subject_id,
            cohort_size=cohort_size,
            window_index=window_index,
            observed_at=observed_at,
        )
    comparison_share = (
        comparison.new_value_share
        if comparison is not None and comparison.measured
        else None
    )
    if comparison_share is None:
        return DimensionObservation(
            schema_version="dimension_observation.v1",
            subject_kind=subject_kind,
            subject_id=subject_id,
            cohort_size=cohort_size,
            observed_states=(SourceRequirementState.AVAILABLE_CURRENT,),
            data_semantics="no_data",
            sample_count=None,
            coverage=current.classification_coverage,
            current_value=None,
            comparison_value=None,
            denominator_present=False,
            # CHAOS-3331: fail-open writer path, see docstring above.
            attribution_present=False,
            window_index=window_index,
            observed_at=observed_at,
        )
    current_value = abs(current_share - comparison_share)
    return DimensionObservation(
        schema_version="dimension_observation.v1",
        subject_kind=subject_kind,
        subject_id=subject_id,
        cohort_size=cohort_size,
        observed_states=(SourceRequirementState.AVAILABLE_CURRENT,),
        data_semantics=_value_semantics(current_value),
        sample_count=None,
        coverage=current.classification_coverage,
        current_value=current_value,
        comparison_value=comparison_share,
        denominator_present=True,
        # CHAOS-3331: fail-open writer path, see docstring above.
        attribution_present=False,
        window_index=window_index,
        observed_at=observed_at,
    )
