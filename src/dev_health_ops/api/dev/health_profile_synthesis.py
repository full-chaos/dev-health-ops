"""Shared rule-application engine for CHAOS-3303's project/team health profiles.

Builds one :class:`~.contracts_v2.health_rules.DimensionObservation` per
applicable :data:`~.health_rule_registry.HEALTH_RULE_REGISTRY` rule for a
subject, evaluates the whole batch through
:func:`~.health_rule_registry.evaluate_registry` (CHAOS-3302's production
seam, hard-bound to the canonical registry), and returns
:class:`HealthProfileResult` with the SAME three-way split
``evaluate_registry`` itself returns (``launch_findings``/
``shadow_findings``/``suppressed_findings``), never collapsed into one
tuple. No composite score is ever computed; a caller reads
dimension-by-dimension, per the CHAOS-3302/3303 "no default composite
health score" guardrail.

Every rule shipped in ``HEALTH_RULE_REGISTRY`` today is ``provisional``
(CHAOS-3302's own ``test_no_shipped_rule_is_launch_authorized`` totality
test), so ``launch_findings`` is empty and every finding this module
produces today lands in ``shadow_findings``. That is expected, not a
defect: calibration has not yet approved any rule for launch. A caller
(e.g. ``PortfolioStatusService``) that computed status/ordering from a
merged bucket would therefore be treating pure calibration data as launch
authority -- ``launch_findings`` staying separate is what keeps that
distinction available at every call site, not just this one. The moment a
future calibration review promotes a rule, the exact same synthesis code
starts populating ``launch_findings`` with no changes required here.

``_UNBOUND_RULE_LIMITATIONS`` is a closed, exhaustive table (checked at
import time against every rule currently in the registry): a rule whose
``comparison_unit`` has no canonical, scope-safe source wired yet is
reported as an honest, unmeasured gap -- never approximated with a
different unit's value, and never silently skipped. Adding a rule to
``HEALTH_RULE_REGISTRY`` without either binding it in
``_observation_for_rule`` or documenting it here fails construction of this
module loudly, rather than silently producing no observation for it.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime

from .contracts_v2.base import SourceRequirementState
from .contracts_v2.health_rules import (
    DimensionObservation,
    HealthRuleDefinition,
    HealthRuleFinding,
    RuleApplicability,
)
from .data_health_service import DataHealthResult
from .dimension_observation_adapters import (
    change_failure_rate_observation,
    data_trust_observation,
    incident_load_observation,
    unavailable_observation,
)
from .health_rule_registry import HEALTH_RULE_REGISTRY, evaluate_registry
from .metrics.service import MetricQueryResult
from .status_change_service import StatusSnapshotResult

__all__ = [
    "HealthEvaluationSources",
    "HealthProfileResult",
    "synthesize_health_profile",
]


@dataclass(frozen=True, slots=True)
class HealthEvaluationSources:
    """Already-fetched canonical results this module composes -- never fetched here.

    Callers (``ProjectHealthService``/``TeamHealthService``) own every
    ``PlanExecutorRuntime`` call; this module only maps results it is
    handed.
    """

    data_health: DataHealthResult | None = None
    status_snapshot: StatusSnapshotResult | None = None
    change_failure_rate_metric: MetricQueryResult | None = None
    #: True when the change-failure-rate metric is structurally
    #: inapplicable to this subject's direct scope (e.g. TEAM --
    #: ``CHANGE_FAILURE_RATE``'s ``supported_scopes`` excludes
    #: ``DirectScope.TEAM`` and ``supports_team_filter`` is ``False``), as
    #: opposed to merely unqueried. Distinct so the resulting observation
    #: reports ``NOT_APPLICABLE`` rather than ``UNAVAILABLE``.
    change_failure_rate_not_applicable: bool = False


@dataclass(frozen=True, slots=True)
class HealthProfileResult:
    """Mirrors ``HealthRuleEvaluationResult``'s own three-way split exactly --
    see the module docstring for why a caller must never merge these back
    into one bucket.
    """

    subject_kind: RuleApplicability
    subject_id: str
    observations: tuple[DimensionObservation, ...]
    launch_findings: tuple[HealthRuleFinding, ...]
    shadow_findings: tuple[HealthRuleFinding, ...]
    suppressed_findings: tuple[HealthRuleFinding, ...]


#: Rules with a bound canonical source, handled directly in
#: ``_observation_for_rule``. Every other registry rule id must appear in
#: ``_UNBOUND_RULE_LIMITATIONS`` below (enforced at import time).
_BOUND_RULE_IDS: frozenset[str] = frozenset(
    {
        "health_rule.data_trust_broken.v1",
        "health_rule.incident_load.v1",
        "health_rule.change_failure_rate.v1",
    }
)

#: Every other CHAOS-3302 rule id, and why it has no canonical, scope-safe
#: source wired yet -- see ``dimension_observation_adapters``'s module
#: docstring. A limitation string here is never surfaced as a percentage or
#: finding; it only labels why the dimension is honestly ``unavailable``.
_UNBOUND_RULE_LIMITATIONS: Mapping[str, str] = {
    "health_rule.completion_stalled.v1": "no_canonical_stalled_work_item_ratio_source",
    "health_rule.review_latency_sustained.v1": "no_canonical_review_latency_source",
    "health_rule.wip_congestion.v1": "no_canonical_wip_congestion_ratio_source",
    "health_rule.review_bottleneck_hours.v1": "no_canonical_review_latency_source",
    "health_rule.flaky_test_rate.v1": "no_canonical_test_report_source",
    "health_rule.high_churn.v1": "no_canonical_rework_churn_ratio_source",
}

_undocumented_rule_ids = (
    frozenset(HEALTH_RULE_REGISTRY)
    - _BOUND_RULE_IDS
    - frozenset(_UNBOUND_RULE_LIMITATIONS)
)
if _undocumented_rule_ids:
    raise RuntimeError(
        "health_profile_synthesis has no source binding or documented "
        f"limitation for rule id(s): {sorted(_undocumented_rule_ids)} -- add "
        "a binding in _observation_for_rule or an entry in "
        "_UNBOUND_RULE_LIMITATIONS"
    )


def _observation_for_rule(
    rule: HealthRuleDefinition,
    sources: HealthEvaluationSources,
    *,
    subject_kind: RuleApplicability,
    subject_id: str,
    cohort_size: int | None,
    observed_at: datetime,
) -> DimensionObservation:
    if rule.rule_id == "health_rule.data_trust_broken.v1":
        if sources.data_health is None:
            return unavailable_observation(
                subject_kind=subject_kind,
                subject_id=subject_id,
                cohort_size=cohort_size,
                window_index=0,
                observed_at=observed_at,
            )
        return data_trust_observation(
            sources.data_health,
            subject_kind=subject_kind,
            subject_id=subject_id,
            cohort_size=cohort_size,
            window_index=0,
            observed_at=observed_at,
        )
    if rule.rule_id == "health_rule.incident_load.v1":
        if sources.status_snapshot is None:
            return unavailable_observation(
                subject_kind=subject_kind,
                subject_id=subject_id,
                cohort_size=cohort_size,
                window_index=0,
                observed_at=observed_at,
            )
        return incident_load_observation(
            sources.status_snapshot,
            subject_kind=subject_kind,
            subject_id=subject_id,
            cohort_size=cohort_size,
            window_index=0,
            observed_at=observed_at,
        )
    if rule.rule_id == "health_rule.change_failure_rate.v1":
        if sources.change_failure_rate_not_applicable:
            return unavailable_observation(
                subject_kind=subject_kind,
                subject_id=subject_id,
                cohort_size=cohort_size,
                window_index=0,
                observed_at=observed_at,
                state=SourceRequirementState.NOT_APPLICABLE,
            )
        if sources.change_failure_rate_metric is None:
            return unavailable_observation(
                subject_kind=subject_kind,
                subject_id=subject_id,
                cohort_size=cohort_size,
                window_index=0,
                observed_at=observed_at,
            )
        return change_failure_rate_observation(
            sources.change_failure_rate_metric,
            subject_kind=subject_kind,
            subject_id=subject_id,
            cohort_size=cohort_size,
            window_index=0,
            observed_at=observed_at,
        )
    if rule.rule_id not in _UNBOUND_RULE_LIMITATIONS:
        # Unreachable given the module-level totality check above; kept as a
        # loud failure rather than a silent fallthrough if that check is
        # ever weakened.
        raise AssertionError(
            f"rule {rule.rule_id!r} has no source binding or documented limitation"
        )
    return unavailable_observation(
        subject_kind=subject_kind,
        subject_id=subject_id,
        cohort_size=cohort_size,
        window_index=0,
        observed_at=observed_at,
    )


def synthesize_health_profile(
    *,
    applicability: RuleApplicability,
    subject_id: str,
    cohort_size: int | None,
    sources: HealthEvaluationSources,
    org_id: str,
    observed_at: datetime,
) -> HealthProfileResult:
    """Evaluate every ``HEALTH_RULE_REGISTRY`` rule applicable to ``applicability``.

    Each of ``launch_findings``/``shadow_findings``/``suppressed_findings``
    is independently sorted deterministically by ``(dimension, rule_id)`` so
    two evaluations of the same inputs always produce the same ordering
    within each bucket.
    """

    applicable_rules = tuple(
        rule
        for rule in HEALTH_RULE_REGISTRY.values()
        if applicability in rule.applicability
    )
    observations_by_rule: dict[str, list[DimensionObservation]] = {}
    ordered_observations: list[DimensionObservation] = []
    for rule in applicable_rules:
        observation = _observation_for_rule(
            rule,
            sources,
            subject_kind=applicability,
            subject_id=subject_id,
            cohort_size=cohort_size,
            observed_at=observed_at,
        )
        observations_by_rule[rule.rule_id] = [observation]
        ordered_observations.append(observation)

    evaluation = evaluate_registry(observations_by_rule, org_id=org_id)

    def _ordered(
        findings: Sequence[HealthRuleFinding],
    ) -> tuple[HealthRuleFinding, ...]:
        return tuple(
            sorted(
                findings,
                key=lambda finding: (finding.dimension.value, finding.rule_id),
            )
        )

    return HealthProfileResult(
        subject_kind=applicability,
        subject_id=subject_id,
        observations=tuple(ordered_observations),
        launch_findings=_ordered(evaluation.launch_findings),
        shadow_findings=_ordered(evaluation.shadow_findings),
        suppressed_findings=_ordered(evaluation.suppressed_findings),
    )
