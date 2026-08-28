"""Team-keyed cognitive load, OWNERSHIP-scoped (CHAOS-4365 item 2 / 4347-C).

CHAOS-4321 hard rule: team attribution is project/repo OWNERSHIP only, never
person->membership inference. This module aggregates the in-process
``UserMetricsDailyRecord`` / ``TeamMetricsDailyRecord`` rows job_daily.py has
already computed for the day -- BY ``repo_id`` -- and maps each repo to a
team via a caller-supplied ``repo_to_team`` map (built from
``team_repo_ownership`` merged over ``teams.repo_patterns``, see
``providers/teams.py::load_team_repo_ownership_map`` and
``job_daily.py::_repo_to_team_map_for_compounding_risk`` for the identical
merge pattern CHAOS-4365 item 1 established).

Deliberately NEVER reads either input row's own ``team_id`` field:
CHAOS-4396 found both ``user_metrics_daily`` and ``team_metrics_daily`` are
written with a two-tier resolver (repo ownership first, author-membership
fallback) that taints that column -- a reader cannot tell which tier
produced a given row's ``team_id``. This module only ever reads
``repo_id`` from those rows and resolves the team itself.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

from dev_health_ops.metrics.schemas import TeamCognitiveLoadDailyRecord


@dataclass
class _TeamBucket:
    pr_interruption_load: float = 0.0
    review_request_load: float = 0.0
    after_hours_commits_count: int = 0
    weekend_commits_count: int = 0
    commits_count: int = 0
    has_team_metrics_row: bool = False
    repo_ids: set[str] | None = None
    authors: set[str] | None = None
    # (author_email, repo_id) pairs contributing to this team -- NOT a sum
    # of UserMetricsDailyRecord.context_spread_count (codex R3 P2:
    # context_spread_count is already the author's TOTAL distinct-repo
    # count for the day, copied identically onto every one of that
    # author's per-repo rows -- summing it across a team's N owned repos
    # an author touched would report N * that count, not the count
    # itself). One row IS one (author, repo) pair, so the team's true
    # context-spread count is the number of distinct pairs across its
    # owned repos.
    context_spread_pairs: set[tuple[str, str]] | None = None

    def __post_init__(self) -> None:
        if self.repo_ids is None:
            self.repo_ids = set()
        if self.authors is None:
            self.authors = set()
        if self.context_spread_pairs is None:
            self.context_spread_pairs = set()


def build_team_cognitive_load_rows_for_day(
    *,
    day: date,
    org_id: str,
    user_metrics_rows: Iterable[Any],
    team_wellbeing_rows: Iterable[Any],
    repo_to_team: dict[str, str],
    computed_at: datetime,
) -> list[TeamCognitiveLoadDailyRecord]:
    """Compose one ``TeamCognitiveLoadDailyRecord`` per team that owns at
    least one repo contributing a signal this day.

    Args:
        day: The day this row is computed *for*.
        org_id: Org id for partitioning.
        user_metrics_rows: This run's in-process ``UserMetricsDailyRecord``
            list (or duck-typed equivalents exposing ``.repo_id``,
            ``.author_email``, ``.pr_interruption_load``,
            ``.context_spread_count``, ``.review_request_load``).
        team_wellbeing_rows: This run's in-process ``TeamMetricsDailyRecord``
            list (or duck-typed equivalents exposing ``.repo_id`` [legacy
            ``""`` rows are skipped -- see migration 080's dedup contract],
            ``.commits_count``, ``.after_hours_commits_count``,
            ``.weekend_commits_count``).
        repo_to_team: ``{repo_id_str: team_id}``, ownership-resolved by the
            caller. A repo with no entry contributes to no team row (never
            guessed).
        computed_at: UTC compute moment, passed explicitly for determinism.

    Returns:
        One row per team with at least one contributing repo this day. A
        team whose owned repos have `user_metrics_daily` rows but no
        `team_metrics_daily` row (or vice versa) still gets a row, with the
        missing side's fields left at their empty default (0.0 for the
        summed load counters, ``None`` for the ratios -- see the schema
        docstring on why a ratio is never defaulted to 0.0).
    """
    buckets: dict[str, _TeamBucket] = {}

    def _bucket(team_id: str) -> _TeamBucket:
        existing = buckets.get(team_id)
        if existing is None:
            existing = _TeamBucket()
            buckets[team_id] = existing
        return existing

    for row in user_metrics_rows:
        repo_id = str(getattr(row, "repo_id", None) or "")
        team_id = repo_to_team.get(repo_id)
        if not team_id:
            continue
        bucket = _bucket(team_id)
        bucket.pr_interruption_load += float(
            getattr(row, "pr_interruption_load", 0) or 0
        )
        bucket.review_request_load += float(getattr(row, "review_request_load", 0) or 0)
        bucket.repo_ids.add(repo_id)  # type: ignore[union-attr]
        author_email = getattr(row, "author_email", None)
        if author_email:
            author_email = str(author_email)
            bucket.authors.add(author_email)  # type: ignore[union-attr]
            bucket.context_spread_pairs.add((author_email, repo_id))  # type: ignore[union-attr]

    for row in team_wellbeing_rows:
        repo_id = str(getattr(row, "repo_id", "") or "")
        if not repo_id:
            # Legacy "" sentinel (pre-migration-080 rows) -- see that
            # migration's dedup contract: never a real, ownership-resolvable
            # repo, so it can never map to a team here either.
            continue
        team_id = repo_to_team.get(repo_id)
        if not team_id:
            continue
        bucket = _bucket(team_id)
        bucket.after_hours_commits_count += int(
            getattr(row, "after_hours_commits_count", 0) or 0
        )
        bucket.weekend_commits_count += int(
            getattr(row, "weekend_commits_count", 0) or 0
        )
        bucket.commits_count += int(getattr(row, "commits_count", 0) or 0)
        bucket.has_team_metrics_row = True
        bucket.repo_ids.add(repo_id)  # type: ignore[union-attr]

    records: list[TeamCognitiveLoadDailyRecord] = []
    for team_id in sorted(buckets):
        bucket = buckets[team_id]
        if bucket.has_team_metrics_row and bucket.commits_count > 0:
            after_hours_ratio: float | None = (
                bucket.after_hours_commits_count / bucket.commits_count
            )
            weekend_ratio: float | None = (
                bucket.weekend_commits_count / bucket.commits_count
            )
        elif bucket.has_team_metrics_row:
            # A team_metrics_daily row exists but summed to zero commits
            # (e.g. every owned repo's row was itself a legitimate all-zero
            # day) -- measured, not missing: report 0.0, not None.
            after_hours_ratio = 0.0
            weekend_ratio = 0.0
        else:
            after_hours_ratio = None
            weekend_ratio = None

        records.append(
            TeamCognitiveLoadDailyRecord(
                org_id=org_id,
                team_id=team_id,
                day=day,
                pr_interruption_load=bucket.pr_interruption_load,
                context_spread_count=float(len(bucket.context_spread_pairs or ())),
                review_request_load=bucket.review_request_load,
                after_hours_commit_ratio=after_hours_ratio,
                weekend_commit_ratio=weekend_ratio,
                contributing_repo_count=len(bucket.repo_ids or ()),
                sample_author_count=len(bucket.authors or ()),
                computed_at=computed_at,
            )
        )
    return records


__all__ = ["build_team_cognitive_load_rows_for_day"]
