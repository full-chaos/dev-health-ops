"""GraphQL types for the Complexity surface (CHAOS-1756).

Exposes two read-only queries backed by the existing ClickHouse tables:
- ``repo_complexity_daily``
- ``file_complexity_snapshots``
- ``file_hotspot_daily``

No new tables or ETL are introduced; this is a pure schema addition.
"""

from __future__ import annotations

from datetime import date, datetime
from enum import Enum

import strawberry


@strawberry.enum
class ComplexityScope(Enum):
    """Scope for complexity timeseries queries.

    ``REPO`` returns one point per repo per day/week.
    ``FILE`` returns one point per file per snapshot day.
    """

    REPO = "repo"
    FILE = "file"


@strawberry.enum
class TimeGranularity(Enum):
    """Time bucketing granularity for timeseries queries."""

    DAY = "day"
    WEEK = "week"


@strawberry.input
class ComplexityTimeseriesInput:
    """Input for the ``complexityTimeseries`` query."""

    org_id: str = strawberry.field(name="orgId")
    since_utc: datetime = strawberry.field(name="sinceUtc")
    until_utc: datetime = strawberry.field(name="untilUtc")
    granularity: TimeGranularity
    scope: ComplexityScope
    repo_ids: list[str] | None = strawberry.field(default=None, name="repoIds")
    team_ids: list[str] | None = strawberry.field(default=None, name="teamIds")
    #: Repo scope cap; default 500, hard max 1000. Returned points remain bounded by scope × bucket safety limits.
    limit: int | None = None


@strawberry.type
class ComplexityPoint:
    """One data point in a complexity timeseries.

    For ``scope=REPO``: ``scopeId`` is the repo UUID, ``scopeName`` is the
    repo full name.  ``locTotal`` and ``cyclomaticPerKloc`` are populated.

    For ``scope=FILE``: ``scopeId`` is ``<repoId>/<filePath>``,
    ``scopeName`` is the file path.  ``locTotal`` / ``cyclomaticPerKloc``
    are ``null`` (not stored per-file in v1 tables).
    """

    # Using the Python attribute name ``point_date`` to avoid shadowing the
    # imported ``date`` type; the GraphQL field is aliased to "date".
    point_date: date = strawberry.field(name="date")
    scope_id: str = strawberry.field(name="scopeId")
    scope_name: str = strawberry.field(name="scopeName")

    # Repo-scope only (null for file scope)
    loc_total: int | None = strawberry.field(default=None, name="locTotal")
    cyclomatic_per_kloc: float | None = strawberry.field(
        default=None, name="cyclomaticPerKloc"
    )

    # Both scopes
    cyclomatic_total: int | None = strawberry.field(
        default=None, name="cyclomaticTotal"
    )
    cyclomatic_avg: float | None = strawberry.field(default=None, name="cyclomaticAvg")
    high_complexity_functions: int | None = strawberry.field(
        default=None, name="highComplexityFunctions"
    )
    very_high_complexity_functions: int | None = strawberry.field(
        default=None, name="veryHighComplexityFunctions"
    )


@strawberry.type
class ComplexityTimeseriesResult:
    """Response for ``complexityTimeseries``."""

    points: list[ComplexityPoint]
    #: Number of distinct scope IDs (repos or files) present in ``points``.
    total_scope: int = strawberry.field(name="totalScope")


@strawberry.input
class HotspotsInput:
    """Input for the ``hotspots`` query."""

    org_id: str = strawberry.field(name="orgId")
    since_utc: datetime = strawberry.field(name="sinceUtc")
    until_utc: datetime = strawberry.field(name="untilUtc")
    repo_ids: list[str] | None = strawberry.field(default=None, name="repoIds")
    team_ids: list[str] | None = strawberry.field(default=None, name="teamIds")
    #: Row cap; default 50, hard max 500.
    limit: int | None = None


@strawberry.type
class HotspotRow:
    """One file hotspot row from ``file_hotspot_daily``."""

    file_path: str = strawberry.field(name="filePath")
    repo_id: str = strawberry.field(name="repoId")
    repo_name: str = strawberry.field(name="repoName")
    churn_loc_30d: int = strawberry.field(name="churnLoc30d")
    churn_commits_30d: int = strawberry.field(name="churnCommits30d")
    cyclomatic_total: int = strawberry.field(name="cyclomaticTotal")
    cyclomatic_avg: float = strawberry.field(name="cyclomaticAvg")
    blame_concentration: float | None = strawberry.field(
        default=None, name="blameConcentration"
    )
    #: Hotspot score from ``file_hotspot_daily.risk_score``.
    risk_score: float = strawberry.field(name="riskScore")
    #: Deterministic deeplink; built client-side from ``file_path``.
    evidence_url: str | None = strawberry.field(default=None, name="evidenceUrl")


@strawberry.type
class HotspotsResult:
    """Response for ``hotspots``."""

    rows: list[HotspotRow]
