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
  queries. Every CHAOS-3302 rule applicable to ``team`` carries a
  ``minimum_cohort_size`` (``HealthRuleDefinition.validate_cohort_
  requirement``), so a team with no resolved attribution (``cohort_size=0``)
  still has every applicable rule honestly suppressed as
  ``insufficient_cohort`` -> ``UNKNOWN``.

  Codex finding (MEDIUM, 2026-08-02): the attribution snapshot is resolved
  at ``scope.time_range.end`` -- the SAME instant
  ``StatusSnapshotRequest.as_of`` defaults to inside
  ``StatusChangeService.status_snapshot`` (and therefore the same instant
  the runtime's own internal ``team_repository_ids`` call, made through
  ``_authorized_repository_ids``, uses) -- never wall-clock ``now``. An
  ownership row valid at scope-end but since expired by ``now`` would
  otherwise pair real, historical, in-window facts with a wrongly-zeroed
  cohort_size, misreporting a genuinely attributed team as
  ``insufficient_cohort``. ``ClickHouseStatusChangeSource`` additionally
  caches by the exact ``(org_id, team_id, as_of)`` key, so when this
  service and the wrapped runtime share one source instance (the natural
  production wiring), the internal call is a cache hit rather than a
  second round trip -- one resolved attribution snapshot, reused, without
  changing ``PlanExecutorRuntime``'s wire contract (a team ``DevScope``
  cannot carry its own repository list -- CHAOS-3301 addendum, ratified).

  A genuine attribution-lookup FAILURE (``measured=False``) is never
  treated as a zero cohort: cohort_size is UNKNOWABLE, not zero, so this
  service reports every dimension honestly unmeasured (routing through
  ``health_profile_synthesis``'s existing unbound/unavailable path) rather
  than the misleading ``insufficient_cohort`` suppression a measured-zero
  cohort would produce. The runtime is not even called in that case --
  nothing meaningful could be reported regardless of what it returns.
* ``health_rule.change_failure_rate.v1`` is unconditionally reported
  ``not_applicable`` for a team subject: ``MetricID.CHANGE_FAILURE_RATE``'s
  own ``supported_scopes`` (``metrics/definitions.py``) does not include
  ``DirectScope.TEAM`` and ``supports_team_filter`` is ``False`` --
  structurally inapplicable, not merely unqueried.

``native_status_change.py``'s ``ClickHouseStatusChangeSource`` re-derives a
team's owned repositories from ``team_repo_ownership`` at query time,
filtered through canonical-primary work-item attribution (CHAOS-3303 round
2), and executes real team-scoped pull-request/CI/deployment/incident
reads (see ``TEAM_NOT_APPLICABLE_SOURCES``, limited to the two sources --
declared/children work items and their blockers -- that structurally
describe a single entity's completion tree, not a team cohort). A team
with no resolved ``team_repo_ownership`` rows (or a genuinely failing
lookup) still falls back to the same explicit ``FreshnessState.UNAVAILABLE``
observation as before -- never a silently empty answer.
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
from .native_status_change import TeamAttributionResult

__all__ = ["TeamAttributionSource", "TeamHealthService"]


class TeamAttributionSource(Protocol):
    """The exact seam ``ClickHouseStatusChangeSource.team_repository_ids``
    satisfies -- a narrow protocol (not the full status-change source) so a
    test double only has to implement what ``TeamHealthService`` actually
    calls.
    """

    async def team_repository_ids(
        self, org_id: str, team_id: str, *, as_of: datetime
    ) -> TeamAttributionResult: ...


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
        # as_of=scope.time_range.end (not `now`) matches the instant the
        # runtime's own internal team-repository lookup uses.
        attribution = await self._attribution.team_repository_ids(
            org_id, team_id, as_of=scope.time_range.end
        )
        if not attribution.measured:
            # The lookup itself failed -- cohort_size is unknowable, not a
            # measured zero. Every dimension must be honestly unmeasured;
            # calling the runtime would produce facts we cannot responsibly
            # attach a trustworthy cohort to.
            return synthesize_health_profile(
                applicability=RuleApplicability.TEAM,
                subject_id=team_id,
                cohort_size=None,
                sources=HealthEvaluationSources(
                    change_failure_rate_not_applicable=True
                ),
                org_id=org_id,
                observed_at=now,
            )
        cohort_size = len(attribution.repository_ids)

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
