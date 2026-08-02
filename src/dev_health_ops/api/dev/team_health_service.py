"""``TeamHealthService`` (CHAOS-3303): canonical team health synthesis.

Mirrors ``ProjectHealthService`` exactly, over a ``DirectScope.TEAM``
subject instead of ``DirectScope.PROJECT``. Two structural differences
carry the ticket's "use only team-attributed observations" and "no
person-level output" requirements:

* ``cohort_size`` is a caller-supplied, already-resolved count of the
  team's attributed repositories/projects (CHAOS-3301's
  ``team_repo_ownership``/``team_project_ownership``). This service never
  computes that count itself -- ``native_status_change.py``'s
  ``_authorized_repository_ids`` re-derives the *query-time* repository set
  for the same team independently (never carried on the wire), but a
  cohort size is a caller-level attribution-quality judgment, not a byproduct
  of one query. Every CHAOS-3302 rule applicable to ``team`` carries a
  ``minimum_cohort_size`` (``HealthRuleDefinition.validate_cohort_
  requirement``), so a caller that cannot establish real attribution passes
  ``cohort_size=0`` (or leaves it unresolved) and every applicable rule is
  honestly suppressed as ``insufficient_cohort`` -> ``UNKNOWN`` -- never a
  fabricated healthy/zero for a team with no valid attribution.
* ``health_rule.change_failure_rate.v1`` is unconditionally reported
  ``not_applicable`` for a team subject: ``MetricID.CHANGE_FAILURE_RATE``'s
  own ``supported_scopes`` (``metrics/definitions.py``) does not include
  ``DirectScope.TEAM`` and ``supports_team_filter`` is ``False`` --
  structurally inapplicable, not merely unqueried.

``native_status_change.py``'s ``ClickHouseStatusChangeSource`` now re-derives
a team's owned repositories from ``team_repo_ownership`` at query time and
executes real team-scoped pull-request/CI/deployment/incident reads (see
``TEAM_NOT_APPLICABLE_SOURCES``, now limited to the two sources -- declared/
children work items and their blockers -- that structurally describe a
single entity's completion tree, not a team cohort). A team with no
resolved ``team_repo_ownership`` rows (or a genuinely failing lookup) still
falls back to the same explicit ``FreshnessState.UNAVAILABLE`` observation
as before -- never a silently empty answer.
"""

from __future__ import annotations

from datetime import datetime

from .contracts import DevScope, DirectScope
from .contracts_v2.health_rules import RuleApplicability
from .health_profile_synthesis import (
    HealthEvaluationSources,
    HealthProfileResult,
    synthesize_health_profile,
)
from .investigation_plans.builtin_steps import PlanExecutorRuntime

__all__ = ["TeamHealthService"]


class TeamHealthService:
    """Evaluate CHAOS-3302 health rules for one committed team subject."""

    def __init__(self, runtime: PlanExecutorRuntime) -> None:
        self._runtime = runtime

    async def evaluate_team(
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
            raise ValueError("TeamHealthService requires a team direct scope")
        if scope.team_ids != [team_id]:
            raise ValueError("scope.team_ids must name exactly this team subject")

        status_snapshot = await self._runtime.status_snapshot(
            org_id=org_id, permission_fingerprint=permission_fingerprint, scope=scope
        )
        data_health = await self._runtime.data_health(
            org_id=org_id, permission_fingerprint=permission_fingerprint, scope=scope
        )

        sources = HealthEvaluationSources(
            data_health=data_health,
            status_snapshot=status_snapshot,
            change_failure_rate_metric=None,
            change_failure_rate_not_applicable=True,
        )
        return synthesize_health_profile(
            applicability=RuleApplicability.TEAM,
            subject_id=team_id,
            cohort_size=cohort_size,
            sources=sources,
            org_id=org_id,
            observed_at=now,
        )
