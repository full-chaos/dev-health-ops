"""``TeamHealthService`` (CHAOS-3303): canonical team health synthesis.

Mirrors ``ProjectHealthService`` exactly, over a ``DirectScope.TEAM``
subject instead of ``DirectScope.PROJECT``. Two structural differences
carry the ticket's "use only team-attributed observations" and "no
person-level output" requirements:

* ``cohort_size`` is resolved by this service itself, from
  :class:`TeamAttributionSource` -- the canonical ``team_repo_ownership``
  read ``native_status_change.ClickHouseStatusChangeSource.
  team_repository_ids`` exposes (CHAOS-3303's own team SQL arms). Codex
  finding (HIGH, 2026-08-02): a caller-asserted naked ``cohort_size: int``
  could claim e.g. ``25`` for a team with zero real ``team_repo_ownership``
  rows, and a runtime returning otherwise-complete sources would then
  report a fabricated healthy finding for an unattributed team. There is no
  parameter left for a caller to assert a cohort size through -- it is
  always the length of the *verified* repository set this service itself
  queries for the exact ``team_id``/``as_of`` the evaluation uses. Every
  CHAOS-3302 rule applicable to ``team`` carries a ``minimum_cohort_size``
  (``HealthRuleDefinition.validate_cohort_requirement``), so a team with no
  resolved attribution (``cohort_size=0``) still has every applicable rule
  honestly suppressed as ``insufficient_cohort`` -> ``UNKNOWN``.
* ``health_rule.change_failure_rate.v1`` is unconditionally reported
  ``not_applicable`` for a team subject: ``MetricID.CHANGE_FAILURE_RATE``'s
  own ``supported_scopes`` (``metrics/definitions.py``) does not include
  ``DirectScope.TEAM`` and ``supports_team_filter`` is ``False`` --
  structurally inapplicable, not merely unqueried.

``native_status_change.py``'s ``ClickHouseStatusChangeSource`` re-derives a
team's owned repositories from ``team_repo_ownership`` at query time and
executes real team-scoped pull-request/CI/deployment/incident reads (see
``TEAM_NOT_APPLICABLE_SOURCES``, limited to the two sources -- declared/
children work items and their blockers -- that structurally describe a
single entity's completion tree, not a team cohort). A team with no
resolved ``team_repo_ownership`` rows (or a genuinely failing lookup) still
falls back to the same explicit ``FreshnessState.UNAVAILABLE`` observation
as before -- never a silently empty answer.
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

__all__ = ["TeamAttributionSource", "TeamHealthService"]


class TeamAttributionSource(Protocol):
    """The exact seam ``ClickHouseStatusChangeSource.team_repository_ids``
    satisfies -- a narrow protocol (not the full status-change source) so a
    test double only has to implement what ``TeamHealthService`` actually
    calls.
    """

    async def team_repository_ids(
        self, org_id: str, team_id: str, *, as_of: datetime
    ) -> list[str]: ...


class TeamHealthService:
    """Evaluate CHAOS-3302 health rules for one committed team subject."""

    def __init__(
        self, runtime: PlanExecutorRuntime, attribution: TeamAttributionSource
    ) -> None:
        self._runtime = runtime
        self._attribution = attribution

    async def evaluate_team(
        self,
        *,
        org_id: str,
        permission_fingerprint: str,
        scope: DevScope,
        team_id: str,
        now: datetime,
    ) -> HealthProfileResult:
        if scope.direct_scope is not DirectScope.TEAM:
            raise ValueError("TeamHealthService requires a team direct scope")
        if scope.team_ids != [team_id]:
            raise ValueError("scope.team_ids must name exactly this team subject")

        # The ONLY source of cohort_size: verified, queried at evaluation
        # time, never a caller-supplied assertion -- see module docstring.
        owned_repository_ids = await self._attribution.team_repository_ids(
            org_id, team_id, as_of=now
        )
        cohort_size = len(owned_repository_ids)

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
