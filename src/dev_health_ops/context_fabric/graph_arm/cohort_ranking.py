"""CHAOS-3667 W4 slice 2: rank an authorized cohort with canonical signals.

The graph discovers who is comparable. This adapter asks the already-built
canonical enrichment services what each authorized member can support, then
keeps the service-owned states attached to that member. It does not read
graph measurement attributes, derive a ratio, or create a health score.

Two boundaries are intentional:

* healthy and single-signal members are recorded as question-specific
  exclusions, while unknown, stale, unavailable and not-applicable members
  stay visible in the adapter result so a coverage gap cannot read as health;
* ordering is a lexicographic vector over the canonical dimensions, followed
  by the canonical id. There is no sum, weight, percentile or universal
  composite score for a team or project.

This is an internal adapter result. The frozen packet's
``ComparisonCohort`` has no rank field, so packet assembly can consume this
result in a later slice without changing the wire contract here.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from typing import Protocol, cast

from dev_health_ops.api.dev.canonical_enrichment import (
    CanonicalEnrichment,
    EnrichmentGap,
)
from dev_health_ops.api.dev.contracts import (
    DevEntityRef,
    DevMetricRef,
    DevScope,
    DirectScope,
    EntityType,
    FreshnessState,
)
from dev_health_ops.api.dev.contracts_v2.base import (
    SourceClass,
    SourceRequirementState,
)
from dev_health_ops.api.dev.contracts_v2.deficiency import (
    DeficiencyFinding,
    OperationalDeficiencyInventory,
)
from dev_health_ops.api.dev.contracts_v2.health_rules import (
    DimensionState,
    HealthDimension,
)
from dev_health_ops.api.dev.health_profile_synthesis import HealthProfileResult
from dev_health_ops.api.dev.investigation_contract import (
    CohortExclusionReason,
)

from .cohort import CohortCandidate, CohortExclusionRecord, CohortProposal
from .vocabulary import GraphEntityKind

__all__ = [
    "CanonicalCohortRankingAdapter",
    "CanonicalCohortSignal",
    "CohortRankDisposition",
    "CohortRankedMember",
    "CohortRankingResult",
]


class _CanonicalEnrichmentSource(Protocol):
    async def enrich(
        self,
        *,
        org_id: str,
        permission_fingerprint: str,
        scope: DevScope,
        now: datetime | None = None,
    ) -> CanonicalEnrichment: ...


class CohortRankDisposition(StrEnum):
    """How the adapter treats an authorized candidate."""

    INCLUDED = "included"
    UNKNOWN = "unknown"


@dataclass(frozen=True, slots=True)
class CanonicalCohortSignal:
    """One service-owned signal retained for a cohort member.

    ``observed_states`` and ``data_semantics`` are copied from the canonical
    source rather than normalized into a single boolean. ``coverage`` and
    denominator/attribution flags stay separate for the same reason: a stale
    measurement, a partial denominator and an inapplicable rule are different
    facts, even when all three prevent a current ranking claim.
    """

    signal_id: str
    source: str
    observed_states: tuple[SourceRequirementState, ...]
    data_semantics: str
    freshness: FreshnessState | None = None
    coverage: float | None = None
    denominator_present: bool | None = None
    attribution_present: bool | None = None
    dimension: HealthDimension | None = None
    state: DimensionState | None = None
    evidence_ref_ids: tuple[str, ...] = ()
    evidence_source_classes: tuple[SourceClass, ...] = ()
    limitation: str | None = None
    gap: EnrichmentGap | None = None

    @property
    def is_current(self) -> bool:
        """Whether this signal can support a current, qualified comparison."""

        return (
            bool(self.observed_states)
            and all(
                state is SourceRequirementState.AVAILABLE_CURRENT
                for state in self.observed_states
            )
            and (self.coverage is None or self.coverage >= 1.0)
            and self.denominator_present is not False
            and self.attribution_present is not False
        )

    @property
    def has_usable_value(self) -> bool:
        """Whether the signal is measured, rather than only disclosed."""

        return bool(
            self.observed_states
            and all(
                state
                in {
                    SourceRequirementState.AVAILABLE_CURRENT,
                    SourceRequirementState.AVAILABLE_STALE,
                    SourceRequirementState.AVAILABLE_UNKNOWN,
                }
                for state in self.observed_states
            )
            and self.data_semantics in {"measured_zero", "no_data"}
        )


@dataclass(frozen=True, slots=True)
class CohortRankedMember:
    """An authorized candidate with its uncollapsed canonical disclosures."""

    candidate: CohortCandidate
    signals: tuple[CanonicalCohortSignal, ...]
    disposition: CohortRankDisposition
    pressure_dimensions: tuple[HealthDimension, ...] = ()


@dataclass(frozen=True, slots=True)
class CohortRankingResult:
    """The internal ranked cohort plus safe exclusions and authorization count."""

    ranked_members: tuple[CohortRankedMember, ...]
    exclusions: tuple[CohortExclusionRecord, ...]
    authorization_filtered_count: int


_ENTITY_SCOPE: Mapping[GraphEntityKind, tuple[DirectScope, EntityType]] = {
    GraphEntityKind.PROJECT: (DirectScope.PROJECT, EntityType.PROJECT),
    GraphEntityKind.TEAM: (DirectScope.TEAM, EntityType.TEAM),
}

_PRESSURE_STATES: frozenset[DimensionState] = frozenset(
    {
        DimensionState.WATCH,
        DimensionState.AT_RISK,
        DimensionState.CRITICAL,
    }
)

# This is an ordering of independent dimensions, not a scoring formula. A
# dimension may be added only by an explicit contract change, and ties still
# resolve by canonical id. No value is summed or weighted.
_DIMENSION_ORDER: tuple[HealthDimension, ...] = tuple(HealthDimension)
_DIMENSION_ORDINAL = {
    dimension: index for index, dimension in enumerate(_DIMENSION_ORDER)
}
_SEVERITY_ORDINAL = {
    DimensionState.CRITICAL: 3,
    DimensionState.AT_RISK: 2,
    DimensionState.WATCH: 1,
}


def _gap_state(gap: EnrichmentGap) -> SourceRequirementState:
    if gap is EnrichmentGap.NOT_APPLICABLE:
        return SourceRequirementState.NOT_APPLICABLE
    if gap is EnrichmentGap.UNAUTHORIZED:
        return SourceRequirementState.UNAUTHORIZED_OR_NOT_VISIBLE
    if gap is EnrichmentGap.UNAVAILABLE:
        return SourceRequirementState.UNAVAILABLE
    # ``NO_DATA`` is a queried-but-empty canonical result, not an unavailable
    # source. The adapter retains that distinction in ``gap`` while mapping
    # it to the contract's queried unknown state for ordering.
    return SourceRequirementState.AVAILABLE_UNKNOWN


def _gap_signal(source: str, gap: EnrichmentGap) -> CanonicalCohortSignal:
    return CanonicalCohortSignal(
        signal_id=source,
        source=source,
        observed_states=(_gap_state(gap),),
        data_semantics=(
            "not_measured"
            if _gap_state(gap)
            in {
                SourceRequirementState.UNAVAILABLE,
                SourceRequirementState.UNAUTHORIZED_OR_NOT_VISIBLE,
                SourceRequirementState.NOT_APPLICABLE,
            }
            else "no_data"
        ),
        limitation=f"canonical_{gap.value}",
        gap=gap,
    )


def _states(
    value: object, default: SourceRequirementState
) -> tuple[SourceRequirementState, ...]:
    raw = getattr(value, "observed_states", ())
    if not raw:
        return (default,)
    return tuple(raw)


def _observation_default_state(observation: object) -> DimensionState:
    """Keep an observation's source state from becoming implicit health."""

    observed_states = _states(observation, SourceRequirementState.AVAILABLE_UNKNOWN)
    if all(state is SourceRequirementState.NOT_APPLICABLE for state in observed_states):
        return DimensionState.NOT_APPLICABLE
    if any(
        state is not SourceRequirementState.AVAILABLE_CURRENT
        for state in observed_states
    ):
        return DimensionState.UNKNOWN
    coverage = getattr(observation, "coverage", None)
    if (
        (coverage is not None and coverage < 1.0)
        or getattr(observation, "denominator_present", None) is False
        or getattr(observation, "attribution_present", None) is False
    ):
        return DimensionState.UNKNOWN
    return DimensionState.HEALTHY


def _profile_signals(
    source: str, profile: HealthProfileResult
) -> tuple[CanonicalCohortSignal, ...]:
    """Copy health/workload profile observations and findings without folding them."""

    all_findings = tuple(
        getattr(profile, name, ())
        for name in ("launch_findings", "shadow_findings", "suppressed_findings")
    )
    findings = tuple(finding for bucket in all_findings for finding in bucket)
    by_rule: Mapping[str, object] = getattr(profile, "observations_by_rule", {})
    observations = tuple(sorted(by_rule.items(), key=lambda item: item[0]))
    if not observations:
        observations = tuple(
            (
                str(index),
                observation,
            )
            for index, observation in enumerate(getattr(profile, "observations", ()))
        )

    output: list[CanonicalCohortSignal] = []
    seen_rules: set[str] = set()
    for rule_id, observation in observations:
        rule = str(rule_id)
        seen_rules.add(rule)
        matching = tuple(item for item in findings if item.rule_id == rule)
        if not matching:
            output.append(
                _observation_signal(
                    source,
                    rule,
                    observation,
                    dimension=None,
                    state=_observation_default_state(observation),
                    limitation=None,
                )
            )
            continue
        for finding in matching:
            output.append(
                _observation_signal(
                    source,
                    rule,
                    observation,
                    dimension=getattr(finding, "dimension", None),
                    state=getattr(finding, "state", None),
                    limitation=getattr(finding, "suppressed_reason", None),
                    evidence_ref_ids=tuple(getattr(finding, "evidence_ref_ids", ())),
                    evidence_source_classes=tuple(
                        getattr(finding, "evidence_source_classes", ())
                    ),
                )
            )

    # A malformed/partially mocked canonical result must not cause a finding
    # to disappear merely because its observation map was incomplete. Real
    # profiles are closed by their own service contract; this branch is the
    # adapter's defensive preservation boundary for an unavailable source or
    # an additive future rule.
    for finding in sorted(findings, key=lambda item: str(item.rule_id)):
        rule = str(finding.rule_id)
        if rule in seen_rules:
            continue
        output.append(
            _finding_signal(
                source,
                rule,
                finding,
            )
        )
    return tuple(sorted(output, key=lambda item: item.signal_id))


def _observation_signal(
    source: str,
    rule_id: str,
    observation: object,
    *,
    dimension: HealthDimension | None,
    state: DimensionState | None,
    limitation: str | None,
    evidence_ref_ids: tuple[str, ...] = (),
    evidence_source_classes: tuple[SourceClass, ...] = (),
) -> CanonicalCohortSignal:
    observed_states = _states(observation, SourceRequirementState.AVAILABLE_UNKNOWN)
    return CanonicalCohortSignal(
        signal_id=f"{source}:{rule_id}",
        source=source,
        observed_states=observed_states,
        data_semantics=str(getattr(observation, "data_semantics", "not_measured")),
        coverage=getattr(observation, "coverage", None),
        denominator_present=getattr(observation, "denominator_present", None),
        attribution_present=getattr(observation, "attribution_present", None),
        dimension=dimension,
        state=state,
        evidence_ref_ids=evidence_ref_ids,
        evidence_source_classes=evidence_source_classes,
        limitation=limitation,
    )


def _finding_signal(
    source: str, rule_id: str, finding: object
) -> CanonicalCohortSignal:
    state = getattr(finding, "state", None)
    return CanonicalCohortSignal(
        signal_id=f"{source}:{rule_id}",
        source=source,
        observed_states=(SourceRequirementState.AVAILABLE_UNKNOWN,),
        data_semantics="no_data",
        dimension=getattr(finding, "dimension", None),
        state=state,
        evidence_ref_ids=tuple(getattr(finding, "evidence_ref_ids", ())),
        evidence_source_classes=tuple(getattr(finding, "evidence_source_classes", ())),
        limitation=getattr(finding, "suppressed_reason", None),
    )


def _deficiency_signals(
    inventory: OperationalDeficiencyInventory,
) -> tuple[CanonicalCohortSignal, ...]:
    output: list[CanonicalCohortSignal] = []
    findings_by_category: dict[object, list[DeficiencyFinding]] = {}
    for finding in inventory.findings:
        findings_by_category.setdefault(finding.category, []).append(finding)
    for status in inventory.category_statuses:
        findings = findings_by_category.get(status.category, [])
        if not findings:
            observed = status.applicability_states_observed or (
                (SourceRequirementState.AVAILABLE_CURRENT,)
                if status.evaluated
                else (SourceRequirementState.AVAILABLE_UNKNOWN,)
            )
            output.append(
                CanonicalCohortSignal(
                    signal_id=f"readiness:{status.category.value}",
                    source="readiness",
                    observed_states=tuple(observed),
                    data_semantics=("no_data" if status.evaluated else "not_measured"),
                    state=(DimensionState.HEALTHY if status.evaluated else None),
                    limitation=status.limitation,
                )
            )
            continue
        for finding in findings:
            state = {
                "critical": DimensionState.CRITICAL,
                "at_risk": DimensionState.AT_RISK,
                "watch": DimensionState.WATCH,
            }.get(finding.severity.value)
            output.append(
                CanonicalCohortSignal(
                    signal_id=f"readiness:{finding.rule_id}",
                    source="readiness",
                    observed_states=(finding.observed_state,),
                    data_semantics=finding.data_semantics,
                    coverage=finding.coverage,
                    dimension=None,
                    state=state,
                    evidence_ref_ids=tuple(finding.evidence_ref_ids),
                    limitation=(
                        finding.limitations[0] if finding.limitations else None
                    ),
                )
            )
    return tuple(sorted(output, key=lambda item: item.signal_id))


def _metric_signals(
    metrics: Sequence[DevMetricRef],
) -> tuple[CanonicalCohortSignal, ...]:
    output: list[CanonicalCohortSignal] = []
    freshness_to_state = {
        FreshnessState.FRESH: SourceRequirementState.AVAILABLE_CURRENT,
        FreshnessState.STALE: SourceRequirementState.AVAILABLE_STALE,
        FreshnessState.UNKNOWN: SourceRequirementState.AVAILABLE_UNKNOWN,
        FreshnessState.UNAVAILABLE: SourceRequirementState.UNAVAILABLE,
    }
    for metric in metrics:
        freshness = getattr(metric, "freshness", FreshnessState.UNKNOWN)
        value = getattr(metric, "value", None)
        output.append(
            CanonicalCohortSignal(
                signal_id=f"metrics:{metric.metric_ref_id}",
                source="metrics",
                observed_states=(freshness_to_state[freshness],),
                data_semantics=("measured_zero" if value is not None else "no_data"),
                freshness=freshness,
                coverage=getattr(metric, "coverage", None),
                evidence_ref_ids=tuple(getattr(metric, "evidence_ref_ids", ())),
            )
        )
    return tuple(sorted(output, key=lambda item: item.signal_id))


def _enrichment_signals(
    enrichment: CanonicalEnrichment,
) -> tuple[CanonicalCohortSignal, ...]:
    output: list[CanonicalCohortSignal] = []
    for source_name, value in (
        ("status", enrichment.status),
        ("health", enrichment.health),
        ("workload", enrichment.workload),
        ("readiness", enrichment.readiness),
    ):
        if isinstance(value, EnrichmentGap):
            output.append(_gap_signal(source_name, value))
        elif source_name in {"health", "workload"}:
            output.extend(
                _profile_signals(source_name, cast(HealthProfileResult, value))
            )
        elif source_name == "readiness":
            output.extend(
                _deficiency_signals(cast(OperationalDeficiencyInventory, value))
            )
        else:
            # StatusSnapshotResult has no DimensionObservation equivalent.
            # Its own state and source freshness stay visible as one
            # canonical signal; no status-derived scalar is invented here.
            result_state = getattr(value, "state", None)
            observed = {
                "complete": SourceRequirementState.AVAILABLE_CURRENT,
                "partial": SourceRequirementState.AVAILABLE_STALE,
                "degraded": SourceRequirementState.AVAILABLE_UNKNOWN,
                "insufficient_evidence": SourceRequirementState.AVAILABLE_UNKNOWN,
            }.get(
                str(getattr(result_state, "value", result_state)),
                SourceRequirementState.AVAILABLE_UNKNOWN,
            )
            output.append(
                CanonicalCohortSignal(
                    signal_id="status:snapshot",
                    source="status",
                    observed_states=(observed,),
                    data_semantics="no_data",
                    limitation=(
                        str(getattr(value, "state", "unknown"))
                        if observed is not SourceRequirementState.AVAILABLE_CURRENT
                        else None
                    ),
                )
            )
    output.extend(_metric_signals(enrichment.metrics))
    if not output:
        output.append(_gap_signal("canonical_enrichment", EnrichmentGap.NO_DATA))
    return tuple(sorted(output, key=lambda item: item.signal_id))


def _pressure_signals(
    signals: Sequence[CanonicalCohortSignal],
) -> tuple[CanonicalCohortSignal, ...]:
    return tuple(
        signal
        for signal in signals
        if signal.state in _PRESSURE_STATES
        and signal.is_current
        and signal.dimension is not None
    )


def _has_state_gap(signals: Sequence[CanonicalCohortSignal]) -> bool:
    # A canonical service may disclose that one dimension is structurally
    # inapplicable (for example, a status source for a project that has no
    # status integration) alongside current observations from other sources.
    # Preserve that disclosure on the member, but do not let it turn an
    # otherwise current healthy/single-signal result into ``unknown``.  An
    # all-inapplicable result remains unknown because there is no comparable
    # observation at all.
    applicable = tuple(
        signal
        for signal in signals
        if signal.gap is not EnrichmentGap.NOT_APPLICABLE
        and any(
            state is not SourceRequirementState.NOT_APPLICABLE
            for state in signal.observed_states
        )
    )
    if not applicable:
        return True
    return any(
        signal.gap is not None
        or any(
            state
            in {
                SourceRequirementState.AVAILABLE_STALE,
                SourceRequirementState.AVAILABLE_UNKNOWN,
                SourceRequirementState.UNCONFIGURED,
                SourceRequirementState.UNAVAILABLE,
                SourceRequirementState.UNAUTHORIZED_OR_NOT_VISIBLE,
                SourceRequirementState.TRUNCATED,
            }
            for state in signal.observed_states
        )
        or signal.coverage is not None
        and signal.coverage < 1.0
        or signal.denominator_present is False
        or signal.attribution_present is False
        for signal in applicable
    )


def _critical_is_qualified(signal: CanonicalCohortSignal) -> bool:
    return (
        signal.state is DimensionState.CRITICAL
        and signal.is_current
        and bool(signal.evidence_ref_ids or signal.evidence_source_classes)
    )


def _classify(
    candidate: CohortCandidate,
    signals: tuple[CanonicalCohortSignal, ...],
) -> tuple[CohortRankDisposition, tuple[HealthDimension, ...], str | None]:
    pressure = _pressure_signals(signals)
    dimensions = tuple(
        sorted(
            {signal.dimension for signal in pressure if signal.dimension is not None},
            key=lambda item: _DIMENSION_ORDINAL[item],
        )
    )
    if len(dimensions) >= 2 or any(
        _critical_is_qualified(signal) for signal in pressure
    ):
        return CohortRankDisposition.INCLUDED, dimensions, None

    if _has_state_gap(signals):
        return CohortRankDisposition.UNKNOWN, dimensions, None

    if pressure:
        return (
            CohortRankDisposition.UNKNOWN,
            dimensions,
            (
                f"this {candidate.kind.value} has one canonical pressure signal, "
                "which is a single signal and insufficient corroboration for "
                "this cohort question"
            ),
        )

    return (
        CohortRankDisposition.UNKNOWN,
        dimensions,
        (
            f"this {candidate.kind.value} has no canonical pressure signal; "
            "the service observations are current and complete"
        ),
    )


def _member_rank_key(
    member: CohortRankedMember,
) -> tuple[int, tuple[int, ...], str]:
    """A deterministic vector order, not a composite score.

    The first component keeps current qualified pressure ahead of unknown
    members. The second is one slot per canonical dimension, so two members
    with different dimensions are not collapsed into one invented number.
    Canonical id is the final total-order tie-break.
    """

    current_pressure = _pressure_signals(member.signals)
    state_class = 0 if current_pressure else 1
    severity_by_dimension = {
        signal.dimension: _SEVERITY_ORDINAL[signal.state]
        for signal in current_pressure
        if signal.dimension is not None and signal.state in _SEVERITY_ORDINAL
    }
    vector = tuple(
        -severity_by_dimension.get(dimension, 0) for dimension in _DIMENSION_ORDER
    )
    return (state_class, vector, member.candidate.canonical_id)


def _member_scope(base_scope: DevScope, candidate: CohortCandidate) -> DevScope | None:
    resolved = _ENTITY_SCOPE.get(candidate.kind)
    if resolved is None:
        return None
    direct_scope, entity_type = resolved
    return DevScope(
        schema_version=base_scope.schema_version,
        organization_id=base_scope.organization_id,
        direct_scope=direct_scope,
        entity_refs=[
            DevEntityRef(
                entity_type=entity_type,
                entity_id=candidate.canonical_id,
                display_label=candidate.display_label,
            )
        ],
        team_ids=([candidate.canonical_id] if direct_scope is DirectScope.TEAM else []),
        time_range=base_scope.time_range,
        comparison_range=base_scope.comparison_range,
    )


class CanonicalCohortRankingAdapter:
    """Evaluate and deterministically order one authorized cohort.

    The adapter deliberately evaluates members sequentially: the canonical
    services share production database clients, and a ranking result must not
    turn a per-member source failure into a whole-cohort failure. A service
    exception becomes one disclosed unavailable signal for that member.
    """

    def __init__(self, source: _CanonicalEnrichmentSource) -> None:
        self._source = source

    async def rank(
        self,
        *,
        proposal: CohortProposal,
        authorized_entity_ids: Sequence[str] | frozenset[str],
        scope: DevScope,
        permission_fingerprint: str,
        now: datetime | None = None,
    ) -> CohortRankingResult:
        authorized = frozenset(authorized_entity_ids)
        observed_at = now or datetime.now(UTC)
        if observed_at.tzinfo is None:
            raise ValueError("now must be timezone-aware")

        ranked: list[CohortRankedMember] = []
        exclusions: list[CohortExclusionRecord] = []
        filtered = proposal.authorization_filtered_count
        for candidate in sorted(proposal.members, key=lambda item: item.canonical_id):
            if candidate.canonical_id not in authorized:
                # An unauthorized id is counted but never re-emitted as a
                # member or an exclusion. Naming a withheld candidate in a
                # rationale would disclose the same entity the filter hides.
                filtered += 1
                continue

            member_scope = _member_scope(scope, candidate)
            if member_scope is None:
                signals: tuple[CanonicalCohortSignal, ...] = (
                    _gap_signal("canonical_enrichment", EnrichmentGap.NOT_APPLICABLE),
                )
            else:
                try:
                    enrichment = await self._source.enrich(
                        org_id=scope.organization_id,
                        permission_fingerprint=permission_fingerprint,
                        scope=member_scope,
                        now=observed_at,
                    )
                    signals = _enrichment_signals(enrichment)
                except Exception:
                    signals = (
                        _gap_signal("canonical_enrichment", EnrichmentGap.UNAVAILABLE),
                    )

            disposition, dimensions, exclusion_rationale = _classify(candidate, signals)
            member = CohortRankedMember(
                candidate=candidate,
                signals=signals,
                disposition=disposition,
                pressure_dimensions=dimensions,
            )
            if exclusion_rationale is not None:
                exclusions.append(
                    CohortExclusionRecord(
                        canonical_id=candidate.canonical_id,
                        kind=candidate.kind,
                        reason=CohortExclusionReason.EXCLUDED_BY_QUESTION,
                        rationale=exclusion_rationale,
                    )
                )
            else:
                ranked.append(member)

        return CohortRankingResult(
            ranked_members=tuple(sorted(ranked, key=_member_rank_key)),
            exclusions=tuple(sorted(exclusions, key=lambda item: item.canonical_id)),
            authorization_filtered_count=filtered,
        )
