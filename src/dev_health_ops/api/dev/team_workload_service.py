"""``TeamWorkloadService`` (CHAOS-3304): canonical team workload-pressure and

investment-balance synthesis for the ``balance.team_workload.v1`` plan
(Amendment TRD v2 section 5).

Structurally, this is ``TeamHealthService`` (CHAOS-3303) plus two extra
canonical sources (cognitive-load and investment-mix, both from
``native_team_workload.ClickHouseTeamWorkloadSource``) feeding the four new
CHAOS-3304 rules bound in ``health_profile_synthesis``. It reuses the exact
same ``synthesize_health_profile`` engine and ``HealthEvaluationSources``
shape -- every dimension is still reported independently ("no default
composite health score"), and every finding is still a
``DimensionObservation``/``HealthRuleFinding`` pair a caller can embed
without translation.

Denominator-contract discipline (Amendment TRD v2 section 8.1) lives at the
adapter layer (``dimension_observation_adapters.py``'s
``_per_active_contributor_observation``/``investment_allocation_shift_observation``),
never here: this service's only job is to resolve which canonical calls a
team subject needs and hand their *already-computed* results to that layer,
exactly like ``TeamHealthService``. It never divides, ranks, or compares
values itself.

Cohort/comparison-basis scope note: this service produces ONE team's
observations. Comparing several teams into a ranked cohort (Amendment TRD v2
section 8.2's "authorized cohort of >= 3 comparable teams") is a caller-side
concern -- a caller that has already resolved and evaluated an authorized
peer cohort may read each team's ``DimensionObservation.comparison_value``
and ``cohort_size`` to build that comparison; this service never asserts
peer values itself (constraint: cohort/attribution facts come from canonical
sources resolved server-side, never caller-supplied numbers -- there is
structurally no parameter here through which a caller could inject one).
"""

from __future__ import annotations

from datetime import datetime
from typing import Protocol

from .contracts import DevScope, DirectScope
from .contracts_v2.health_rules import RuleApplicability
from .health_profile_synthesis import (
    HealthEvaluationSources,
    HealthProfileResult,
    synthesize_health_profile,
)
from .investigation_plans.builtin_steps import PlanExecutorRuntime
from .native_team_workload import TeamCognitiveLoadResult, TeamInvestmentMixResult

__all__ = ["TeamWorkloadDataSource", "TeamWorkloadService"]


class TeamWorkloadDataSource(Protocol):
    """The narrow CHAOS-3304 source port -- mirrors ``PlanExecutorRuntime``'s
    own "exact canonical-service surface" discipline (``builtin_steps.py``),
    scoped to the two sources this service adds beyond the six core plans.
    Production wiring implements this directly over
    ``native_team_workload.ClickHouseTeamWorkloadSource``.
    """

    async def cognitive_load(
        self, *, org_id: str, team_id: str, start: datetime, end: datetime
    ) -> TeamCognitiveLoadResult: ...

    async def active_contributor_count(
        self, *, org_id: str, team_id: str, start: datetime, end: datetime
    ) -> int | None: ...

    async def investment_mix(
        self, *, org_id: str, team_id: str, start: datetime, end: datetime
    ) -> TeamInvestmentMixResult: ...


class TeamWorkloadService:
    """Evaluate CHAOS-3302/3304 workload and investment-balance rules for
    one committed team subject.
    """

    def __init__(
        self,
        runtime: PlanExecutorRuntime,
        workload_source: TeamWorkloadDataSource,
    ) -> None:
        self._runtime = runtime
        self._workload_source = workload_source

    async def evaluate_workload(
        self,
        *,
        org_id: str,
        permission_fingerprint: str,
        scope: DevScope,
        team_id: str,
        cohort_size: int | None,
        now: datetime,
    ) -> HealthProfileResult:
        if scope.direct_scope is not DirectScope.TEAM:
            raise ValueError("TeamWorkloadService requires a team direct scope")
        if scope.team_ids != [team_id]:
            raise ValueError("scope.team_ids must name exactly this team subject")

        # Sequential awaits throughout -- never asyncio.gather over these
        # calls. Several share a ClickHouse client the same way an
        # AsyncSession cannot be used concurrently; this service never
        # fans requests out in parallel over it.
        status_snapshot = await self._runtime.status_snapshot(
            org_id=org_id, permission_fingerprint=permission_fingerprint, scope=scope
        )
        data_health = await self._runtime.data_health(
            org_id=org_id, permission_fingerprint=permission_fingerprint, scope=scope
        )

        current_range = scope.time_range
        cognitive_load = await self._workload_source.cognitive_load(
            org_id=org_id,
            team_id=team_id,
            start=current_range.start,
            end=current_range.end,
        )
        active_contributor_count = await self._workload_source.active_contributor_count(
            org_id=org_id,
            team_id=team_id,
            start=current_range.start,
            end=current_range.end,
        )
        investment_mix = await self._workload_source.investment_mix(
            org_id=org_id,
            team_id=team_id,
            start=current_range.start,
            end=current_range.end,
        )

        cognitive_load_comparison: TeamCognitiveLoadResult | None = None
        investment_mix_comparison: TeamInvestmentMixResult | None = None
        if scope.comparison_range is not None:
            comparison_range = scope.comparison_range
            cognitive_load_comparison = await self._workload_source.cognitive_load(
                org_id=org_id,
                team_id=team_id,
                start=comparison_range.start,
                end=comparison_range.end,
            )
            investment_mix_comparison = await self._workload_source.investment_mix(
                org_id=org_id,
                team_id=team_id,
                start=comparison_range.start,
                end=comparison_range.end,
            )

        sources = HealthEvaluationSources(
            data_health=data_health,
            status_snapshot=status_snapshot,
            change_failure_rate_metric=None,
            change_failure_rate_not_applicable=True,
            cognitive_load=cognitive_load,
            cognitive_load_comparison=cognitive_load_comparison,
            active_contributor_count=active_contributor_count,
            investment_mix=investment_mix,
            investment_mix_comparison=investment_mix_comparison,
        )
        return synthesize_health_profile(
            applicability=RuleApplicability.TEAM,
            subject_id=team_id,
            cohort_size=cohort_size,
            sources=sources,
            org_id=org_id,
            observed_at=now,
        )
