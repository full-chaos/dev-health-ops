"""``PortfolioStatusService`` (CHAOS-3303): bounded multi-project status.

Batches ``ProjectHealthService.evaluate_project`` across a bounded set of
committed project subjects (``status.portfolio.v1`` / ``PROJECT_STATUS``
intent) via ``asyncio.gather`` -- one bounded fan-out, never a per-project
model loop. Never averages completion percentages or dimension states
across projects (CHAOS-3303's own guardrail: "Do not average incompatible
project completion percentages or treat unknown denominators as
complete") -- every project's own findings stay independent; only the
*ordering* and *counts* are computed here.
"""

from __future__ import annotations

import asyncio
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime

from .contracts import DevScope
from .contracts_v2.health_rules import DimensionState
from .health_profile_synthesis import HealthProfileResult
from .project_health_service import ProjectHealthService

__all__ = [
    "MAX_PORTFOLIO_PROJECTS",
    "PortfolioProjectScope",
    "PortfolioStatusResult",
    "PortfolioStatusService",
]

#: Mirrors ``DevSubjectSet``'s own bound (``contracts_v2.subject``:
#: ``committed_entity_refs`` is ``Field(min_length=1, max_length=25)``) --
#: a portfolio batch is exactly a subject set's worth of projects, never
#: unbounded.
MAX_PORTFOLIO_PROJECTS = 25

#: Worst-to-best ordering for deterministic portfolio ranking. Lower sorts
#: first (worst-first), matching "prioritized findings"/"ordered by
#: deterministic severity and evidence quality".
_DIMENSION_STATE_SEVERITY: Mapping[DimensionState, int] = {
    DimensionState.CRITICAL: 0,
    DimensionState.AT_RISK: 1,
    DimensionState.WATCH: 2,
    DimensionState.UNKNOWN: 3,
    DimensionState.NOT_APPLICABLE: 4,
    DimensionState.HEALTHY: 5,
}


@dataclass(frozen=True, slots=True)
class PortfolioProjectScope:
    """One resolved project subject to include in a portfolio batch.

    ``scope`` must already be a committed, validated ``DirectScope.PROJECT``
    ``DevScope`` (subject resolution and cohort bounding are CHAOS-3301's
    territory, not this service's) -- this service performs no further
    subject resolution of its own.
    """

    project_id: str
    scope: DevScope


@dataclass(frozen=True, slots=True)
class PortfolioProjectFailure:
    """A project whose canonical-service calls raised, isolated from the batch.

    Never a silently-dropped project: it still appears in
    ``PortfolioStatusResult.failures``, and its ``project_id`` is excluded
    from ``projects`` rather than reported with fabricated findings.
    """

    project_id: str
    error: str


@dataclass(frozen=True, slots=True)
class PortfolioStatusResult:
    #: Worst-severity-first, then ``project_id`` for stability -- see
    #: ``_sort_key``.
    projects: tuple[HealthProfileResult, ...]
    counts_by_worst_state: Mapping[DimensionState, int]
    failures: tuple[PortfolioProjectFailure, ...]
    unresolved_mention_ids: tuple[str, ...]
    ambiguous_mention_ids: tuple[str, ...]
    warnings: tuple[str, ...]
    evaluated_at: datetime


def _worst_state(result: HealthProfileResult) -> DimensionState:
    if not result.findings:
        return DimensionState.UNKNOWN
    return min(
        (finding.state for finding in result.findings),
        key=lambda state: _DIMENSION_STATE_SEVERITY[state],
    )


def _sort_key(result: HealthProfileResult) -> tuple[int, str]:
    return (_DIMENSION_STATE_SEVERITY[_worst_state(result)], result.subject_id)


class PortfolioStatusService:
    """Batch-evaluate a bounded set of committed project subjects."""

    def __init__(self, project_health_service: ProjectHealthService) -> None:
        self._project_health_service = project_health_service

    async def evaluate_portfolio(
        self,
        *,
        org_id: str,
        permission_fingerprint: str,
        projects: Sequence[PortfolioProjectScope],
        now: datetime,
        unresolved_mention_ids: Sequence[str] = (),
        ambiguous_mention_ids: Sequence[str] = (),
        warnings: Sequence[str] = (),
    ) -> PortfolioStatusResult:
        if not projects:
            raise ValueError("evaluate_portfolio requires at least one project")
        if len(projects) > MAX_PORTFOLIO_PROJECTS:
            raise ValueError(
                f"portfolio batch exceeds the bounded maximum of "
                f"{MAX_PORTFOLIO_PROJECTS} projects"
            )
        project_ids = [item.project_id for item in projects]
        if len(set(project_ids)) != len(project_ids):
            raise ValueError("duplicate project_id in portfolio batch")

        outcomes = await asyncio.gather(
            *(
                self._project_health_service.evaluate_project(
                    org_id=org_id,
                    permission_fingerprint=permission_fingerprint,
                    scope=item.scope,
                    project_id=item.project_id,
                    now=now,
                )
                for item in projects
            ),
            return_exceptions=True,
        )

        results: list[HealthProfileResult] = []
        failures: list[PortfolioProjectFailure] = []
        for item, outcome in zip(projects, outcomes, strict=True):
            if isinstance(outcome, BaseException):
                failures.append(
                    PortfolioProjectFailure(
                        project_id=item.project_id, error=repr(outcome)
                    )
                )
                continue
            results.append(outcome)

        ordered = tuple(sorted(results, key=_sort_key))
        counts: dict[DimensionState, int] = {state: 0 for state in DimensionState}
        for result in ordered:
            counts[_worst_state(result)] += 1

        return PortfolioStatusResult(
            projects=ordered,
            counts_by_worst_state=counts,
            failures=tuple(sorted(failures, key=lambda failure: failure.project_id)),
            unresolved_mention_ids=tuple(unresolved_mention_ids),
            ambiguous_mention_ids=tuple(ambiguous_mention_ids),
            warnings=tuple(warnings),
            evaluated_at=now,
        )
