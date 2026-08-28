"""Team-keyed cyclomatic-complexity rollup, OWNERSHIP-scoped (CHAOS-4365
item 3 / 4347-C).

CHAOS-4321 hard rule: team attribution is project/repo OWNERSHIP only, never
person->membership inference. This module aggregates already-persisted
``repo_complexity_daily`` rows -- read back from ClickHouse, one row per
repo per day, already ``argMax(*, computed_at)``-deduped by the caller (see
``job_daily.py::_fetch_repo_complexity_for_day``) -- BY ``repo_id``, and
maps each repo to a team via a caller-supplied ``repo_to_team`` map (built
from ``team_repo_ownership`` merged over ``teams.repo_patterns``, the same
resolution ``team_cognitive_load.py`` and ``compounding_risk.py`` use).

``repo_complexity_daily`` carries no ``team_id`` column of its own (unlike
``user_metrics_daily``/``team_metrics_daily``, CHAOS-4396's taint source),
so there is nothing to deliberately avoid reading here -- the ownership
resolution path is reused for consistency, not to route around a tainted
column.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

from dev_health_ops.metrics.schemas import TeamComplexityDailyRecord


@dataclass
class _TeamComplexityBucket:
    loc_total: int = 0
    cyclomatic_total: int = 0
    high_complexity_functions: int = 0
    very_high_complexity_functions: int = 0
    repo_ids: set[str] | None = None

    def __post_init__(self) -> None:
        if self.repo_ids is None:
            self.repo_ids = set()


def build_team_complexity_rows_for_day(
    *,
    day: date,
    org_id: str,
    repo_complexity_rows: Iterable[Any],
    repo_to_team: dict[str, str],
    computed_at: datetime,
) -> list[TeamComplexityDailyRecord]:
    """Compose one ``TeamComplexityDailyRecord`` per team that owns at
    least one repo with a ``repo_complexity_daily`` row this day.

    Args:
        day: The day this row is computed *for*.
        org_id: Org id for partitioning.
        repo_complexity_rows: This day's already-argMax-deduped
            ``repo_complexity_daily`` rows (or duck-typed equivalents
            exposing ``.repo_id``, ``.loc_total``, ``.cyclomatic_total``,
            ``.high_complexity_functions``, ``.very_high_complexity_functions``).
        repo_to_team: ``{repo_id_str: team_id}``, ownership-resolved by the
            caller. A repo with no entry contributes to no team row (never
            guessed).
        computed_at: UTC compute moment, passed explicitly for determinism.

    Returns:
        One row per team with at least one owned repo contributing a
        ``repo_complexity_daily`` row this day. ``loc_total``,
        ``cyclomatic_total``, ``high_complexity_functions`` and
        ``very_high_complexity_functions`` are summed across those repos;
        ``cyclomatic_per_kloc`` is recomputed from the summed totals
        (``cyclomatic_total / (loc_total / 1000)``, ``0.0`` when
        ``loc_total`` is ``0`` -- the same convention
        ``job_complexity.py``'s repo-level compute uses) rather than
        averaged directly across owned repos' own per-repo ratios.
    """
    buckets: dict[str, _TeamComplexityBucket] = {}

    def _bucket(team_id: str) -> _TeamComplexityBucket:
        existing = buckets.get(team_id)
        if existing is None:
            existing = _TeamComplexityBucket()
            buckets[team_id] = existing
        return existing

    for row in repo_complexity_rows:
        repo_id = str(getattr(row, "repo_id", None) or "")
        if not repo_id:
            continue
        team_id = repo_to_team.get(repo_id)
        if not team_id:
            continue
        bucket = _bucket(team_id)
        bucket.loc_total += int(getattr(row, "loc_total", 0) or 0)
        bucket.cyclomatic_total += int(getattr(row, "cyclomatic_total", 0) or 0)
        bucket.high_complexity_functions += int(
            getattr(row, "high_complexity_functions", 0) or 0
        )
        bucket.very_high_complexity_functions += int(
            getattr(row, "very_high_complexity_functions", 0) or 0
        )
        bucket.repo_ids.add(repo_id)  # type: ignore[union-attr]

    records: list[TeamComplexityDailyRecord] = []
    for team_id in sorted(buckets):
        bucket = buckets[team_id]
        cyclomatic_per_kloc = (
            bucket.cyclomatic_total / (bucket.loc_total / 1000.0)
            if bucket.loc_total > 0
            else 0.0
        )
        records.append(
            TeamComplexityDailyRecord(
                org_id=org_id,
                team_id=team_id,
                day=day,
                loc_total=bucket.loc_total,
                cyclomatic_total=bucket.cyclomatic_total,
                cyclomatic_per_kloc=cyclomatic_per_kloc,
                high_complexity_functions=bucket.high_complexity_functions,
                very_high_complexity_functions=bucket.very_high_complexity_functions,
                contributing_repo_count=len(bucket.repo_ids or ()),
                computed_at=computed_at,
            )
        )
    return records


__all__ = ["build_team_complexity_rows_for_day"]
