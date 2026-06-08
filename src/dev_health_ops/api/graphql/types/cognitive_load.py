"""GraphQL types for the Cognitive Load surface (CHAOS-2077).

Exposes two signals merged from existing ClickHouse tables:
- ``user_metrics_daily``   — per-developer daily load metrics
- ``team_metrics_daily``   — per-team daily commit-timing ratios

No new tables or ETL are introduced; this is a pure schema addition.
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


@strawberry.type
class CognitiveLoadSignal:
    """One day's cognitive-load signals, merged from user + team tables.

    ``prInterruptionLoad``, ``contextSpreadCount``, and ``reviewRequestLoad``
    are summed across all developers in the org (or team when ``teamId`` is
    supplied).

    ``afterHoursCommitRatio`` and ``weekendCommitRatio`` are team-level averages;
    they are ``null`` when ``team_metrics_daily`` has no row for the day.
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
