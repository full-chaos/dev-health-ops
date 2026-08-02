"""Bounded ClickHouse reader for CHAOS-3304 team workload/investment facts.

Every query here is scoped by ``org_id`` AND ``team_id`` directly against a
table that carries ``team_id`` at ingest time (``user_metrics_daily``,
``team_metrics_daily``, ``investment_metrics_daily``) -- unlike
``native_status_change.py``'s status/change facts, this reader never needs to
re-derive an owned-repository set from ``team_repo_ownership`` first. A
measured row is therefore genuinely team-attributed by construction, not a
broader org/repo fact riding along under a team label.

Denominator-contract note (CHAOS-3304, Amendment TRD v2 section 8.1): the
approved preference order is (1) configured team membership/active
contributors, (2) observed active work/review population, (3) the team's own
prior period, (4) an authorized cohort of >= 3 comparable teams.
``active_contributor_count`` below implements order item 2 (a genuinely
queried distinct-author count over the window) -- order item 1 (configured
team membership, e.g. via an identities/roster table) has no canonical,
scope-safe source wired yet and is *not* approximated by this function; a
caller needing a stronger membership denominator must resolve it separately
and prefer it over this one. This module never fabricates order item 1 by
mislabeling order item 2 as "membership".
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from dev_health_ops.api.queries.client import query_dicts

__all__ = [
    "NATIVE_TEAM_WORKLOAD_QUERY_VERSION",
    "ClickHouseTeamWorkloadSource",
    "TeamCognitiveLoadResult",
    "TeamInvestmentMixResult",
]

NATIVE_TEAM_WORKLOAD_QUERY_VERSION = "native-team-workload-query.v1"
QUERY_TIMEOUT_SECONDS = 15

#: Canonical investment-area string -> the four buckets already recognized by
#: ``metrics/operating_review.py``'s own ``_investment_key``. Mirrored here
#: (not imported -- that function is private and, more importantly, silently
#: *drops* an unmapped area instead of bucketing it) so every observed
#: ``delivery_units`` row is accounted for: recognized areas fall into one of
#: the four buckets below, everything else becomes ``unclassified`` in
#: ``_bucket_investment_rows`` -- never dropped.
_INVESTMENT_BUCKETS: dict[str, str] = {
    "ktlo": "ktlo",
    "maintenance": "ktlo",
    "maintenance tech debt": "ktlo",
    "new value": "new_value",
    "feature delivery": "new_value",
    "features": "new_value",
    "security": "security",
    "risk security": "security",
    "infra": "infra",
    "infrastructure": "infra",
    "operational support": "infra",
}


def _investment_bucket(investment_area: str) -> str:
    normalized = investment_area.replace("_", " ").replace("/", " ").strip().lower()
    return _INVESTMENT_BUCKETS.get(normalized, "unclassified")


@dataclass(frozen=True, slots=True)
class TeamCognitiveLoadResult:
    """One window's team-scoped cognitive-load signals.

    ``measured=False`` means the underlying tables returned zero rows for
    this ``(org_id, team_id, window)`` -- the tables are append-only day
    rollups (see ``resolvers/cognitive_load.py``), so an org that never
    computed cognitive-load metrics for this team genuinely has no rows,
    distinct from a computed-and-zero day.
    """

    after_hours_commit_ratio: float | None
    weekend_commit_ratio: float | None
    pr_interruption_load: float | None
    review_request_load: float | None
    context_spread_count: float | None
    sample_days: int
    measured: bool


@dataclass(frozen=True, slots=True)
class TeamInvestmentMixResult:
    """One window's team-scoped, classified completed-work investment mix.

    ``unclassified_units`` is every observed ``delivery_units`` row whose
    ``investment_area`` does not resolve to one of the four canonical
    buckets -- never silently dropped (see ``_investment_bucket``).
    ``measured=False`` means the team produced zero investment-classified
    rows for the window at all (nothing to compute a mix over -- not the
    same as "classified everything as zero").
    """

    new_value_units: float
    ktlo_units: float
    security_units: float
    infra_units: float
    unclassified_units: float
    total_units: float
    measured: bool

    @property
    def classification_coverage(self) -> float:
        if self.total_units <= 0:
            return 0.0
        return (self.total_units - self.unclassified_units) / self.total_units

    @property
    def new_value_share(self) -> float | None:
        if self.total_units <= 0:
            return None
        return self.new_value_units / self.total_units


_TEAM_COGNITIVE_LOAD_USER_SQL = """
SELECT
    sum(pr_interruption_load) AS pr_interruption_load,
    sum(context_spread_count) AS context_spread_count,
    sum(review_request_load)  AS review_request_load,
    count(DISTINCT day)       AS sample_days
FROM (
    SELECT
        day,
        repo_id,
        author_email,
        argMax(pr_interruption_load, computed_at) AS pr_interruption_load,
        argMax(context_spread_count, computed_at) AS context_spread_count,
        argMax(review_request_load,  computed_at) AS review_request_load
    FROM user_metrics_daily
    WHERE org_id = {org_id:String}
      AND team_id = {team_id:String}
      AND day >= {start_date:Date}
      AND day < {end_date:Date}
    GROUP BY day, repo_id, author_email
)
"""

_TEAM_COGNITIVE_LOAD_TEAM_SQL = """
SELECT
    avg(after_hours_commit_ratio) AS after_hours_commit_ratio,
    avg(weekend_commit_ratio)     AS weekend_commit_ratio,
    count(DISTINCT day)           AS sample_days
FROM (
    SELECT
        day,
        team_id,
        argMax(after_hours_commit_ratio, computed_at) AS after_hours_commit_ratio,
        argMax(weekend_commit_ratio,     computed_at) AS weekend_commit_ratio
    FROM team_metrics_daily
    WHERE org_id = {org_id:String}
      AND team_id = {team_id:String}
      AND day >= {start_date:Date}
      AND day < {end_date:Date}
    GROUP BY day, team_id
)
"""

#: Denominator-contract order item 2: "observed active work/review
#: population" -- the distinct set of authors with at least one
#: cognitive-load row for this team in the window. Order item 1 (configured
#: team membership) is intentionally not queried here -- see module
#: docstring.
_TEAM_ACTIVE_CONTRIBUTOR_COUNT_SQL = """
SELECT count(DISTINCT author_email) AS active_contributor_count
FROM user_metrics_daily
WHERE org_id = {org_id:String}
  AND team_id = {team_id:String}
  AND day >= {start_date:Date}
  AND day < {end_date:Date}
"""

_TEAM_INVESTMENT_MIX_SQL = """
SELECT investment_area, sum(delivery_units) AS delivery_units
FROM (
    SELECT
        day,
        repo_id,
        investment_area,
        project_stream,
        argMax(delivery_units, computed_at) AS delivery_units
    FROM investment_metrics_daily
    WHERE org_id = {org_id:String}
      AND team_id = {team_id:String}
      AND day >= {start_date:Date}
      AND day < {end_date:Date}
    GROUP BY day, repo_id, investment_area, project_stream
)
GROUP BY investment_area
"""


def _date_bounds(start: datetime, end: datetime) -> tuple[str, str]:
    return (
        start.astimezone(UTC).date().isoformat(),
        end.astimezone(UTC).date().isoformat(),
    )


def _nfloat(row: dict[str, Any], key: str) -> float | None:
    value = row.get(key)
    return float(value) if value is not None else None


class ClickHouseTeamWorkloadSource:
    """CHAOS-3304 canonical team workload/investment reader.

    Every ClickHouse round trip below is a single, sequential ``await`` --
    never ``asyncio.gather`` over these calls -- per this ticket's
    concurrency constraint (no fan-out that could race a shared
    connection/session). A caller that needs current- and comparison-window
    results calls the same method twice, sequentially.
    """

    def __init__(self, client: Any) -> None:
        self._client = client

    async def cognitive_load(
        self, *, org_id: str, team_id: str, start: datetime, end: datetime
    ) -> TeamCognitiveLoadResult:
        start_date, end_date = _date_bounds(start, end)
        params = {
            "org_id": org_id,
            "team_id": team_id,
            "start_date": start_date,
            "end_date": end_date,
        }
        try:
            async with asyncio.timeout(QUERY_TIMEOUT_SECONDS):
                user_rows = await query_dicts(
                    self._client, _TEAM_COGNITIVE_LOAD_USER_SQL, params
                )
            async with asyncio.timeout(QUERY_TIMEOUT_SECONDS):
                team_rows = await query_dicts(
                    self._client, _TEAM_COGNITIVE_LOAD_TEAM_SQL, params
                )
        except Exception:
            return TeamCognitiveLoadResult(
                after_hours_commit_ratio=None,
                weekend_commit_ratio=None,
                pr_interruption_load=None,
                review_request_load=None,
                context_spread_count=None,
                sample_days=0,
                measured=False,
            )
        user_row = user_rows[0] if user_rows else {}
        team_row = team_rows[0] if team_rows else {}
        user_sample = int(user_row.get("sample_days") or 0)
        team_sample = int(team_row.get("sample_days") or 0)
        measured = user_sample > 0 or team_sample > 0
        if not measured:
            return TeamCognitiveLoadResult(
                after_hours_commit_ratio=None,
                weekend_commit_ratio=None,
                pr_interruption_load=None,
                review_request_load=None,
                context_spread_count=None,
                sample_days=0,
                measured=False,
            )
        return TeamCognitiveLoadResult(
            after_hours_commit_ratio=_nfloat(team_row, "after_hours_commit_ratio"),
            weekend_commit_ratio=_nfloat(team_row, "weekend_commit_ratio"),
            pr_interruption_load=_nfloat(user_row, "pr_interruption_load"),
            review_request_load=_nfloat(user_row, "review_request_load"),
            context_spread_count=_nfloat(user_row, "context_spread_count"),
            sample_days=max(user_sample, team_sample),
            measured=True,
        )

    async def active_contributor_count(
        self, *, org_id: str, team_id: str, start: datetime, end: datetime
    ) -> int | None:
        """Denominator-contract order item 2 -- see module docstring."""

        start_date, end_date = _date_bounds(start, end)
        params = {
            "org_id": org_id,
            "team_id": team_id,
            "start_date": start_date,
            "end_date": end_date,
        }
        try:
            async with asyncio.timeout(QUERY_TIMEOUT_SECONDS):
                rows = await query_dicts(
                    self._client, _TEAM_ACTIVE_CONTRIBUTOR_COUNT_SQL, params
                )
        except Exception:
            return None
        if not rows:
            return None
        count = rows[0].get("active_contributor_count")
        return int(count) if count is not None else None

    async def investment_mix(
        self, *, org_id: str, team_id: str, start: datetime, end: datetime
    ) -> TeamInvestmentMixResult:
        start_date, end_date = _date_bounds(start, end)
        params = {
            "org_id": org_id,
            "team_id": team_id,
            "start_date": start_date,
            "end_date": end_date,
        }
        try:
            async with asyncio.timeout(QUERY_TIMEOUT_SECONDS):
                rows = await query_dicts(self._client, _TEAM_INVESTMENT_MIX_SQL, params)
        except Exception:
            rows = None
        if not rows:
            return TeamInvestmentMixResult(
                new_value_units=0.0,
                ktlo_units=0.0,
                security_units=0.0,
                infra_units=0.0,
                unclassified_units=0.0,
                total_units=0.0,
                measured=False,
            )
        units = {"ktlo": 0.0, "new_value": 0.0, "security": 0.0, "infra": 0.0}
        unclassified = 0.0
        for row in rows:
            area = str(row.get("investment_area") or "")
            delivery_units = float(row.get("delivery_units") or 0.0)
            bucket = _investment_bucket(area)
            if bucket == "unclassified":
                unclassified += delivery_units
            else:
                units[bucket] += delivery_units
        total = sum(units.values()) + unclassified
        return TeamInvestmentMixResult(
            new_value_units=units["new_value"],
            ktlo_units=units["ktlo"],
            security_units=units["security"],
            infra_units=units["infra"],
            unclassified_units=unclassified,
            total_units=total,
            measured=total > 0,
        )
