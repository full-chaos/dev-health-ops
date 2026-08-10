"""CHAOS-3664: canonical project/team enrichment for graph-assisted Ask Dev.

Approved on CHAOS-3660 (the enrichment-accessor proposal): a thin
composition layer over the FIVE canonical services that already exist and
are already constructed in ``production_runtime.py`` --
:class:`~.status_change_service.StatusChangeService`,
:class:`~.team_health_service.TeamHealthService`,
:class:`~.team_workload_service.TeamWorkloadService`,
:class:`~.operational_deficiency_service.OperationalDeficiencyService`, and
:class:`~.metrics.service.MetricQueryService`. This module introduces no new
authorization surface -- every constituent call re-runs that service's own
existing authorization path over the caller's ``DevScope``, exactly as
``EvidenceService.search`` already does across its native adapters.

**Every field is independently present or absent.** A subject kind a given
service does not apply to (health/workload/readiness are team- or
project-scoped rules; see each service's own docstring) is
:attr:`EnrichmentGap.NOT_APPLICABLE`, never a silently blank field -- the
Wave 3.2 handoff's "missing/stale/unavailable/unconfigured/unauthorized/
no-data/truncated/conflicted/not-applicable stay distinct" invariant,
applied here. A caller renders "status unavailable: <reason>", never omits
the row.

**Not in scope here**: ``source_health`` already exists on the graph
investigation packet contract (populated elsewhere, by Lane D's packet
assembly) -- this module does not duplicate it.

**Metrics**: per the CHAOS-3660 metric-table finding, this pulls every
:class:`~.metrics.definitions.MetricDefinition`
:meth:`~.metrics.service.MetricQueryService.list_metrics` returns for the
scope -- that call already does the correct ``direct_scope``/team-filter
filtering, and the real registry is a closed set of exactly eight
``MetricID``s. Curating a further subset per subject kind would encode
which measurements a TRIAL fixture corpus happened to author, not which
real metrics are actually inapplicable -- see the CHAOS-3660 comment.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from typing import Protocol

from .contracts import DevMetricRef, DevScope, DirectScope
from .contracts_v2.deficiency import OperationalDeficiencyInventory
from .health_profile_synthesis import HealthProfileResult
from .metrics.definitions import MetricDefinition
from .metrics.service import MetricQueryRequest
from .status_change_service import StatusSnapshotRequest, StatusSnapshotResult


class _StatusSource(Protocol):
    """Structurally :class:`~.status_change_service.StatusChangeService` --
    a Protocol, not the concrete class, so a test double satisfies it by
    shape alone (the same convention ``EvidenceService``'s own
    dependencies already use: see ``evidence_service.EvidenceScopeAuthorizer``)."""

    async def status_snapshot(
        self,
        org_id: str,
        permission_fingerprint: str,
        request: StatusSnapshotRequest,
    ) -> StatusSnapshotResult: ...


class _TeamRuleSource(Protocol):
    """Structurally :class:`~.team_health_service.TeamHealthService` OR
    :class:`~.team_workload_service.TeamWorkloadService` -- both expose
    exactly one of these two methods with an identical signature; a real
    instance of either satisfies this Protocol via whichever method it has."""

    async def evaluate_team(
        self,
        *,
        org_id: str,
        permission_fingerprint: str,
        scope: DevScope,
        team_id: str,
        now: datetime,
    ) -> HealthProfileResult: ...


class _WorkloadSource(Protocol):
    async def evaluate_workload(
        self,
        *,
        org_id: str,
        permission_fingerprint: str,
        scope: DevScope,
        team_id: str,
        now: datetime,
    ) -> HealthProfileResult: ...


class _ReadinessSource(Protocol):
    """Structurally :class:`~.operational_deficiency_service.OperationalDeficiencyService`."""

    async def evaluate_project(
        self,
        *,
        org_id: str,
        permission_fingerprint: str,
        scope: DevScope,
        now: datetime,
    ) -> OperationalDeficiencyInventory: ...

    async def evaluate_team(
        self,
        *,
        org_id: str,
        permission_fingerprint: str,
        scope: DevScope,
        team_id: str,
        now: datetime,
    ) -> OperationalDeficiencyInventory: ...


class _MetricQueryResult(Protocol):
    def contract_refs(self, scope: DevScope) -> tuple[DevMetricRef, ...]: ...


class _MetricSource(Protocol):
    """Structurally :class:`~.metrics.service.MetricQueryService`."""

    def list_metrics(self, scope: DevScope) -> tuple[MetricDefinition, ...]: ...

    async def query(
        self,
        org_id: str,
        permission_fingerprint: str,
        request: MetricQueryRequest,
        *,
        now: datetime | None = ...,
    ) -> _MetricQueryResult: ...


class EnrichmentGap(StrEnum):
    """Why a :class:`CanonicalEnrichment` field is absent -- always a
    disclosed reason, never an omitted key."""

    #: The subject kind this scope names does not have this rule at all
    #: (e.g. health/workload/readiness for a subject that is not a team or
    #: project). Determined BEFORE calling the underlying service --
    #: never inferred by catching that service's own ``ValueError``, which
    #: would conflate "wrong kind of subject" with "the service itself is
    #: broken."
    NOT_APPLICABLE = "not_applicable"
    UNAUTHORIZED = "unauthorized"
    UNAVAILABLE = "unavailable"
    NO_DATA = "no_data"


@dataclass(frozen=True, slots=True)
class CanonicalEnrichment:
    """One resolved subject's canonical cross-section."""

    status: StatusSnapshotResult | EnrichmentGap
    health: HealthProfileResult | EnrichmentGap
    workload: HealthProfileResult | EnrichmentGap
    readiness: OperationalDeficiencyInventory | EnrichmentGap
    metrics: tuple[DevMetricRef, ...]


class CanonicalEnrichmentAccessor:
    """Composes the five canonical services into one
    :class:`CanonicalEnrichment` per resolved subject. Holds only
    already-constructed service instances -- never builds their own
    dependencies (``PlanExecutorRuntime``, attribution/workload sources,
    ...), which stay wherever ``production_runtime.py`` already assembles
    them.
    """

    def __init__(
        self,
        *,
        status: _StatusSource,
        health: _TeamRuleSource,
        workload: _WorkloadSource,
        readiness: _ReadinessSource,
        metrics: _MetricSource,
    ) -> None:
        self._status = status
        self._health = health
        self._workload = workload
        self._readiness = readiness
        self._metrics = metrics

    async def enrich(
        self,
        *,
        org_id: str,
        permission_fingerprint: str,
        scope: DevScope,
        now: datetime | None = None,
    ) -> CanonicalEnrichment:
        as_of = now or datetime.now(UTC)
        return CanonicalEnrichment(
            status=await self._enrich_status(org_id, permission_fingerprint, scope),
            health=await self._enrich_health(
                org_id, permission_fingerprint, scope, as_of
            ),
            workload=await self._enrich_workload(
                org_id, permission_fingerprint, scope, as_of
            ),
            readiness=await self._enrich_readiness(
                org_id, permission_fingerprint, scope, as_of
            ),
            metrics=await self._enrich_metrics(org_id, permission_fingerprint, scope),
        )

    async def _enrich_status(
        self, org_id: str, permission_fingerprint: str, scope: DevScope
    ) -> StatusSnapshotResult | EnrichmentGap:
        try:
            return await self._status.status_snapshot(
                org_id,
                permission_fingerprint,
                StatusSnapshotRequest(scope=scope),
            )
        except Exception:
            # A caller-authorization failure and a genuine backend outage
            # are both "the caller gets nothing" from this accessor's own
            # perspective -- StatusChangeService's own scope resolution
            # already ran (and would have refused an unauthorized scope
            # before this call could even be constructed by a legitimate
            # caller); an exception here is a real availability failure.
            return EnrichmentGap.UNAVAILABLE

    def _single_team_id(self, scope: DevScope) -> str | None:
        if scope.direct_scope is not DirectScope.TEAM:
            return None
        # DevScope's own validator guarantees team_ids == (the single
        # named team,) whenever direct_scope is TEAM -- never checked
        # again here (an unreachable-under-contract length check would be
        # untestable dead code, not real defense in depth).
        return scope.team_ids[0]

    async def _enrich_health(
        self,
        org_id: str,
        permission_fingerprint: str,
        scope: DevScope,
        now: datetime,
    ) -> HealthProfileResult | EnrichmentGap:
        team_id = self._single_team_id(scope)
        if team_id is None:
            return EnrichmentGap.NOT_APPLICABLE
        try:
            return await self._health.evaluate_team(
                org_id=org_id,
                permission_fingerprint=permission_fingerprint,
                scope=scope,
                team_id=team_id,
                now=now,
            )
        except Exception:
            return EnrichmentGap.UNAVAILABLE

    async def _enrich_workload(
        self,
        org_id: str,
        permission_fingerprint: str,
        scope: DevScope,
        now: datetime,
    ) -> HealthProfileResult | EnrichmentGap:
        team_id = self._single_team_id(scope)
        if team_id is None:
            return EnrichmentGap.NOT_APPLICABLE
        try:
            return await self._workload.evaluate_workload(
                org_id=org_id,
                permission_fingerprint=permission_fingerprint,
                scope=scope,
                team_id=team_id,
                now=now,
            )
        except Exception:
            return EnrichmentGap.UNAVAILABLE

    async def _enrich_readiness(
        self,
        org_id: str,
        permission_fingerprint: str,
        scope: DevScope,
        now: datetime,
    ) -> OperationalDeficiencyInventory | EnrichmentGap:
        if scope.direct_scope is DirectScope.PROJECT:
            try:
                return await self._readiness.evaluate_project(
                    org_id=org_id,
                    permission_fingerprint=permission_fingerprint,
                    scope=scope,
                    now=now,
                )
            except Exception:
                return EnrichmentGap.UNAVAILABLE
        team_id = self._single_team_id(scope)
        if team_id is not None:
            try:
                return await self._readiness.evaluate_team(
                    org_id=org_id,
                    permission_fingerprint=permission_fingerprint,
                    scope=scope,
                    team_id=team_id,
                    now=now,
                )
            except Exception:
                return EnrichmentGap.UNAVAILABLE
        return EnrichmentGap.NOT_APPLICABLE

    async def _enrich_metrics(
        self, org_id: str, permission_fingerprint: str, scope: DevScope
    ) -> tuple[DevMetricRef, ...]:
        # CHAOS-3660: every scope-applicable definition, not a curated
        # subset -- list_metrics(scope) already does the real filtering
        # (direct_scope membership, team-filter support), and the
        # registry itself is a closed set of eight. A metric this service
        # fails to compute for THIS scope is silently absent from the
        # result (the same "no row" semantics DevMetricRef's own
        # `MetricEvidenceClassification` machinery elsewhere in the
        # codebase already accepts) rather than aborting every other
        # metric in the round.
        refs: list[DevMetricRef] = []
        for definition in self._metrics.list_metrics(scope):
            try:
                result = await self._metrics.query(
                    org_id,
                    permission_fingerprint,
                    MetricQueryRequest(metric_id=definition.metric_id, scope=scope),
                )
            except Exception:
                continue
            refs.extend(result.contract_refs(scope))
        return tuple(refs)
