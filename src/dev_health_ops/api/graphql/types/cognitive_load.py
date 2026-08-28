"""GraphQL types for the Cognitive Load surface (CHAOS-2077).

Two source paths, chosen by which input filters are set
(``resolvers/cognitive_load.py::resolve_cognitive_load``):

- Org-wide (no ``teamId``) or team+repo COMBINED (``teamId`` AND ``repoId``
  both set): merged from ``user_metrics_daily`` (per-developer load) and
  ``team_metrics_daily`` (per-team commit-timing ratios), each deduplicated
  via ``argMax(<col>, computed_at)``.
- Single-team (``teamId`` set, ``repoId`` NOT set): read directly from
  ``team_cognitive_load_daily`` (CHAOS-4365 item 2) instead -- that table is
  already team-scoped and OWNERSHIP-resolved (CHAOS-4321) at write time,
  unlike ``user_metrics_daily``/``team_metrics_daily``'s own ``team_id``
  column, which CHAOS-4396 found can fall back to author-membership
  resolution. The team+repo combined case still uses the merge path above,
  since ``team_cognitive_load_daily`` carries no ``repo_id`` dimension to
  filter by.
"""

from __future__ import annotations

from datetime import date

import strawberry


@strawberry.input
class CognitiveLoadInput:
    """Input for the ``cognitiveLoad`` query."""

    org_id: str = strawberry.field(name="orgId")
    since_date: date = strawberry.field(name="sinceDate")
    until_date: date = strawberry.field(name="untilDate")
    #: Optional filter to a single team.  When absent, data across all teams
    #: is aggregated.
    team_id: str | None = strawberry.field(default=None, name="teamId")
    #: Optional filter to a single repo (UUID string). Mirrors the ``team_id``
    #: filter above; only ``user_metrics_daily`` carries a ``repo_id`` column,
    #: so this has no effect on the team-level after-hours/weekend ratios.
    repo_id: str | None = strawberry.field(default=None, name="repoId")


@strawberry.type
class CognitiveLoadSignal:
    """One day's cognitive-load signals.

    Source depends on the query path (see the module docstring): either
    merged from ``user_metrics_daily``/``team_metrics_daily``, or read
    directly from ``team_cognitive_load_daily`` for a single-team query.
    Field semantics are the same either way.

    ``prInterruptionLoad``, ``contextSpreadCount``, and ``reviewRequestLoad``
    are summed across all developers in the org (or team when ``teamId`` is
    supplied).

    ``afterHoursCommitRatio`` and ``weekendCommitRatio`` are team-level
    ratios; they are ``null`` when no source row has after-hours/weekend
    commit data for the day.
    """

    day: date
    pr_interruption_load: float = strawberry.field(name="prInterruptionLoad")
    context_spread_count: float = strawberry.field(name="contextSpreadCount")
    review_request_load: float = strawberry.field(name="reviewRequestLoad")
    after_hours_commit_ratio: float | None = strawberry.field(
        default=None, name="afterHoursCommitRatio"
    )
    weekend_commit_ratio: float | None = strawberry.field(
        default=None, name="weekendCommitRatio"
    )


@strawberry.type
class CognitiveLoadResult:
    """Response for ``cognitiveLoad``."""

    org_id: str = strawberry.field(name="orgId")
    team_id: str | None = strawberry.field(default=None, name="teamId")
    signals: list[CognitiveLoadSignal]
    #: Number of distinct calendar days returned.
    total_days: int = strawberry.field(name="totalDays")
