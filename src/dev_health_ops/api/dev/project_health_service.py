"""``ProjectHealthService`` (CHAOS-3303): canonical project health synthesis.

Composes ``StatusChangeService``/``DataHealthService``/``MetricQueryService``
through the exact ``PlanExecutorRuntime`` seam CHAOS-3295 already built and
``production_runtime.py`` already wires for the six core plans -- never a
second, parallel query path. Every dimension is reported independently
(``health_profile_synthesis``'s ``no default composite health score``); this
service only resolves *which* canonical calls a project subject needs and
hands their results to the shared synthesis engine.
"""

from __future__ import annotations

from datetime import datetime

from .contracts import DevScope, DirectScope, MetricID
from .contracts_v2.health_rules import RuleApplicability
from .health_profile_synthesis import (
    HealthEvaluationSources,
    HealthProfileResult,
    synthesize_health_profile,
)
from .investigation_plans.builtin_steps import PlanExecutorRuntime

__all__ = ["CHANGE_FAILURE_RATE_SUPPORTED_SCOPES", "ProjectHealthService"]

#: ``MetricID.CHANGE_FAILURE_RATE``'s own ``supported_scopes``
#: (``metrics/definitions.py``'s ``_ALL_REPO_SCOPES``) -- every direct scope
#: for which ``MetricQueryService`` will actually accept a query. Named here
#: rather than re-imported from that module's private tuple so a caller can
#: decide "not applicable" *before* calling the metric service, instead of
#: catching the ``ValueError`` its own ``_validate_request`` would raise for
#: an unsupported scope.
CHANGE_FAILURE_RATE_SUPPORTED_SCOPES: frozenset[DirectScope] = frozenset(
    {
        DirectScope.ORGANIZATION,
        DirectScope.REPOSITORY,
        DirectScope.PROJECT,
        DirectScope.WORK_UNIT,
        DirectScope.ISSUE,
        DirectScope.PULL_REQUEST,
    }
)


class ProjectHealthService:
    """Evaluate CHAOS-3302 health rules for one committed project subject."""

    def __init__(self, runtime: PlanExecutorRuntime) -> None:
        self._runtime = runtime

    async def evaluate_project(
        self,
        *,
        org_id: str,
        permission_fingerprint: str,
        scope: DevScope,
        now: datetime,
    ) -> HealthProfileResult:
        if scope.direct_scope is not DirectScope.PROJECT:
            raise ValueError("ProjectHealthService requires a project direct scope")
        # Codex finding (HIGH, 2026-08-02): project_id used to be a caller-
        # supplied label independent of `scope`, so the SAME committed
        # DevScope submitted under two different asserted labels ("alias-a",
        # "alias-b") minted two portfolio "subjects" with identical
        # underlying data -- dedup (and the resulting findings' subject_id)
        # tracked the label, never the validated scope identity. DevScope's
        # own validator already guarantees a PROJECT direct scope carries
        # exactly one matching entity_ref, so its entity_id is the only
        # subject identity this service ever uses -- there is no longer a
        # separate value a caller can assert.
        project_id = scope.entity_refs[0].entity_id
        if scope.comparison_range is None:
            # health_rule.change_failure_rate.v1's comparison_value and every
            # future trend-aware rule need a resolved comparison window;
            # PlanExecutorRuntime.query_metric always requests one (mirrors
            # production_runtime._ProductionPlanExecutorRuntime.query_metric),
            # so failing here is clearer than the ValueError
            # MetricQueryService._validate_request would raise deeper in the
            # call.
            raise ValueError(
                "ProjectHealthService requires scope.comparison_range to be "
                "resolved for trend/comparison observations"
            )

        status_snapshot = await self._runtime.status_snapshot(
            org_id=org_id, permission_fingerprint=permission_fingerprint, scope=scope
        )
        data_health = await self._runtime.data_health(
            org_id=org_id, permission_fingerprint=permission_fingerprint, scope=scope
        )
        change_failure_rate_not_applicable = (
            scope.direct_scope not in CHANGE_FAILURE_RATE_SUPPORTED_SCOPES
        )
        change_failure_rate_metric = None
        if not change_failure_rate_not_applicable:
            change_failure_rate_metric = await self._runtime.query_metric(
                org_id=org_id,
                permission_fingerprint=permission_fingerprint,
                metric_id=MetricID.CHANGE_FAILURE_RATE,
                scope=scope,
            )

        sources = HealthEvaluationSources(
            data_health=data_health,
            status_snapshot=status_snapshot,
            change_failure_rate_metric=change_failure_rate_metric,
            change_failure_rate_not_applicable=change_failure_rate_not_applicable,
        )
        return synthesize_health_profile(
            applicability=RuleApplicability.PROJECT,
            subject_id=project_id,
            cohort_size=None,
            sources=sources,
            org_id=org_id,
            observed_at=now,
        )
