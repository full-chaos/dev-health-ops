"""Cognitive Load GraphQL resolver (CHAOS-2077).

Reads from two append-only ClickHouse tables:
- ``user_metrics_daily``  — per-developer load signals (SUM across developers)
- ``team_metrics_daily``  — per-team commit-timing ratios (AVG across teams)

Both tables are plain ``MergeTree`` (NOT ``ReplacingMergeTree``): a recompute /
backfill appends a NEW row for the same logical key rather than replacing the
old one. Live data confirms duplicates (user_metrics_daily: 2344 duplicate-key
rows; team_metrics_daily: 66). We therefore collapse to the latest row per key
via ``argMax(<col>, computed_at)`` BEFORE aggregating — mirroring the
established ``resolvers/complexity.py`` convention. Without this, SUM/AVG would
double-count backfilled rows (naive SUM of pr_interruption_load was 3296 vs the
correct 1744 in the demo org).

The two result sets are merged on ``day`` (over the UNION of days) in Python
before being returned. All reads are org-scoped via ``require_org_id`` +
parametrized ``org_id``. No data is written or recomputed — pure surface of
persisted metrics.

CHAOS-4329: ``team_metrics_daily`` gained a ``repo_id`` column (``''`` on
legacy rows -- migration 080). ``_fetch_team_metrics`` dedups per
``(team_id, repo_id, day)``, SUMs the additive counts across a team's repos,
and RECOMPUTES the ratio from those summed counts before averaging across
teams -- a team owning N repos no longer loses N-1 of them to a bare
``(team_id, day)`` argMax collapse.

CHAOS-4365 (confirmation-pass finding): a SINGLE-team query (``team_id`` set,
``repo_id`` unset) instead reads ``team_cognitive_load_daily`` directly
(``_fetch_team_cognitive_load``) -- one dedup query, no merge. The two-query
merge above filtered ``user_metrics_daily``/``team_metrics_daily`` on their
OWN ``team_id`` column, which CHAOS-4396 found can fall back to
author-membership resolution (or stay unset for a native org whose teams
have empty ``repo_patterns``): for a real org that column is empty/impure,
so a single-team filter silently returned zero signals while the org-wide
query worked. ``team_cognitive_load_daily`` is already team-scoped and
OWNERSHIP-resolved (CHAOS-4321) at write time. The org-wide path (no
``team_id``) is unchanged.

CHAOS-4406: the team+repo COMBINED path (both set) used to fall through to
the SAME tainted two-query merge as the pre-CHAOS-4365 single-team path
(``team_cognitive_load_daily`` has no ``repo_id`` dimension to filter by, so
it can't serve this case). Fixed the same way CHAOS-4365 fixed the
single-team case: stop trusting ``user_metrics_daily``/``team_metrics_daily``'s
own ``team_id`` column. ``_resolve_owned_repo_id`` confirms ownership via
``team_repo_ownership`` (CHAOS-4321, ownership-only attribution -- the SAME
source ``api/dev/native_status_change.py``'s canonical
``team_repository_ids`` re-derives from); once confirmed, every signal for
that repo belongs to the requesting team BY DEFINITION, so both queries
filter by ``repo_id`` alone (``_fetch_user_metrics`` already supported this;
``_fetch_repo_scoped_team_metrics`` is new) instead of by the tainted
``team_id`` column. An unowned (or nonexistent) repo returns an explicit
empty result rather than either the wrong team's data or a confusing error.
"""

from __future__ import annotations

import logging
from typing import Any

from dev_health_ops.api.queries.client import query_dicts

from ..authz import require_org_id
from ..context import GraphQLContext
from ..types.cognitive_load import (
    CognitiveLoadInput,
    CognitiveLoadResult,
    CognitiveLoadSignal,
)

logger = logging.getLogger(__name__)


def _require_client(context: GraphQLContext) -> Any:
    if context.client is None:
        raise RuntimeError("Database client not available for CognitiveLoad resolver")
    return context.client


def _nfloat(row: dict[str, Any], key: str) -> float | None:
    """Return ``float(row[key])`` or ``None`` when absent/null."""
    v = row.get(key)
    return float(v) if v is not None else None


def _merge_user_and_team_rows(
    user_rows: list[dict[str, Any]],
    team_rows: list[dict[str, Any]],
) -> list[CognitiveLoadSignal]:
    """Outer-join ``user_rows``/``team_rows`` on ``day`` into signals.

    Shared by the org-wide/team+repo-combined path and the (also
    repo-scoped) CHAOS-4406 combined path: both fire one user-load query
    and one team-ratio query and merge them identically. A day present in
    only one side is still emitted, with zero/``None`` for the missing
    side's fields -- e.g. a weekend with commit-timing data but no
    per-developer load rows.
    """
    user_by_day: dict[Any, dict[str, Any]] = {row["day"]: row for row in user_rows}
    team_by_day: dict[Any, dict[str, Any]] = {row["day"]: row for row in team_rows}
    all_days = sorted(set(user_by_day) | set(team_by_day))

    return [
        CognitiveLoadSignal(
            day=day_val,
            pr_interruption_load=float(
                user_by_day.get(day_val, {}).get("pr_interruption_load") or 0.0
            ),
            context_spread_count=float(
                user_by_day.get(day_val, {}).get("context_spread_count") or 0.0
            ),
            review_request_load=float(
                user_by_day.get(day_val, {}).get("review_request_load") or 0.0
            ),
            after_hours_commit_ratio=_nfloat(
                team_by_day.get(day_val, {}), "after_hours_commit_ratio"
            ),
            weekend_commit_ratio=_nfloat(
                team_by_day.get(day_val, {}), "weekend_commit_ratio"
            ),
        )
        for day_val in all_days
    ]


# ---------------------------------------------------------------------------
# ClickHouse fetch helpers
# ---------------------------------------------------------------------------


async def _fetch_user_metrics(
    client: Any,
    *,
    org_id: str,
    since_date: str,
    until_date: str,
    team_id: str | None,
    repo_id: str | None,
) -> list[dict[str, Any]]:
    """SUM of latest-per-developer cognitive load columns, grouped by day.

    ``user_metrics_daily`` is append-only (plain MergeTree), so a backfill
    writes a duplicate row for the same ``(org_id, repo_id, author_email, day)``
    key. The inner subquery selects the latest row per key via
    ``argMax(<col>, computed_at)``; the outer query SUMs those deduplicated
    rows by day. This prevents double-counting from re-computation passes.

    Filters by ``org_id`` (always), date range, and optionally ``team_id`` /
    ``repo_id``. ``team_metrics_daily`` gained a ``repo_id`` column in
    CHAOS-4329, but ``_fetch_team_metrics`` does not (yet) accept a
    ``repo_id`` filter -- it always aggregates across every repo a team
    owns, matching this resolver's existing team-scoped (not repo-scoped)
    contract; only ``_fetch_user_metrics`` here takes a ``repo_id`` filter.

    The GraphQL ``repoId`` input may be EITHER the repo's UUID (``repos.id``)
    OR its human-readable full_name/slug (``repos.repo``, e.g.
    ``"org/repo"``) — the web repo picker's option list
    (``/api/v1/filters/options``) is populated from ``repos.repo`` slugs, not
    UUIDs (see CHAOS-2745 for the broader platform-wide follow-up to make
    every repo-scoped resolver accept slugs consistently). The predicate
    below resolves either form via an org-scoped subquery over ``repos``
    rather than comparing ``repo_id`` (a ``UUID``-typed column) directly
    against the ``String`` parameter, which would force ClickHouse to parse
    the parameter as a UUID and raise ``CANNOT_PARSE_UUID`` whenever a slug
    is supplied.
    """
    inner_where = """
            WHERE org_id = {org_id:String}
              AND day >= {since_date:Date}
              AND day <= {until_date:Date}
    """
    params: dict[str, Any] = {
        "org_id": org_id,
        "since_date": since_date,
        "until_date": until_date,
    }
    if team_id:
        inner_where += "\n              AND team_id = {team_id:String}"
        params["team_id"] = team_id
    if repo_id:
        inner_where += """
              AND repo_id IN (
                  SELECT id FROM repos
                  WHERE org_id = {org_id:String}
                    AND (repo = {repo_id:String} OR toString(id) = {repo_id:String})
              )"""
        params["repo_id"] = repo_id

    query = f"""
        SELECT
            day,
            SUM(pr_interruption_load) AS pr_interruption_load,
            SUM(context_spread_count) AS context_spread_count,
            SUM(review_request_load)  AS review_request_load
        FROM (
            SELECT
                day,
                repo_id,
                author_email,
                argMax(pr_interruption_load, computed_at) AS pr_interruption_load,
                argMax(context_spread_count, computed_at) AS context_spread_count,
                argMax(review_request_load,  computed_at) AS review_request_load
            FROM user_metrics_daily
            {inner_where}
            GROUP BY day, repo_id, author_email
        )
        GROUP BY day
        ORDER BY day
    """
    return await query_dicts(client, query, params)


async def _fetch_team_metrics(
    client: Any,
    *,
    org_id: str,
    since_date: str,
    until_date: str,
    team_id: str | None,
) -> list[dict[str, Any]]:
    """AVG across teams of each team's after-hours / weekend commit ratio, by day.

    ``team_metrics_daily`` is append-only (plain MergeTree) and, since
    CHAOS-4329, carries a ``repo_id`` per row (``''`` on legacy rows written
    before that migration -- see its comment for the dedup contract). A team
    owning N repos writes one row PER (team_id, repo_id, day); collapsing
    straight to ``(team_id, day)`` the old way keeps only one repo's slice.

    Four layers: (1) the innermost subquery collapses each
    ``(org_id, team_id, repo_id, day)`` key to its latest row via
    ``argMax(<col>, computed_at)``; (2) a filter drops the legacy
    ``repo_id=''`` bucket for a (team_id, day) key WHENEVER real per-repo
    buckets also exist for that same key -- a historical day that gets a
    real per-repo backfill/re-drive after migration 080 must not have its
    old pre-migration aggregate summed together with the new per-repo rows,
    which would double-count that day (codex CHAOS-4329 round 1, P1); (3)
    the middle layer SUMs the additive counts across a team's remaining
    repos for each day and RECOMPUTES the ratio from those summed counts --
    a ratio is not additive across rows, so it is never averaged directly
    across repos; (4) the outer query AVGs those per-team-day ratios across
    teams, unchanged from before. When ``team_id`` is supplied we filter to
    that team; otherwise we average across all teams to produce an org-wide
    signal.
    """
    inner_where = """
            WHERE org_id = {org_id:String}
              AND day >= {since_date:Date}
              AND day <= {until_date:Date}
    """
    params: dict[str, Any] = {
        "org_id": org_id,
        "since_date": since_date,
        "until_date": until_date,
    }
    if team_id:
        inner_where += "\n              AND team_id = {team_id:String}"
        params["team_id"] = team_id

    query = f"""
        SELECT
            day,
            AVG(after_hours_commit_ratio) AS after_hours_commit_ratio,
            AVG(weekend_commit_ratio)     AS weekend_commit_ratio
        FROM (
            SELECT
                day,
                team_id,
                sum(commits_count)             AS total_commits,
                sum(after_hours_commits_count) AS total_after_hours_commits,
                sum(weekend_commits_count)      AS total_weekend_commits,
                if(total_commits > 0,
                   total_after_hours_commits / total_commits, 0.0
                ) AS after_hours_commit_ratio,
                if(total_commits > 0,
                   total_weekend_commits / total_commits, 0.0
                ) AS weekend_commit_ratio
            FROM (
                SELECT day, team_id, repo_id, commits_count, after_hours_commits_count, weekend_commits_count
                FROM (
                    SELECT
                        day,
                        team_id,
                        repo_id,
                        argMax(commits_count,             computed_at) AS commits_count,
                        argMax(after_hours_commits_count, computed_at) AS after_hours_commits_count,
                        argMax(weekend_commits_count,      computed_at) AS weekend_commits_count,
                        countIf(repo_id != '') OVER (PARTITION BY day, team_id) AS real_repo_count
                    FROM team_metrics_daily
                    {inner_where}
                    GROUP BY day, team_id, repo_id
                )
                WHERE repo_id != '' OR real_repo_count = 0
            )
            GROUP BY day, team_id
        )
        GROUP BY day
        ORDER BY day
    """
    return await query_dicts(client, query, params)


# ---------------------------------------------------------------------------
# team_repo_ownership (CHAOS-4406, team+repo combined path)
# ---------------------------------------------------------------------------


async def _resolve_owned_repo_id(
    client: Any,
    *,
    org_id: str,
    team_id: str,
    repo_id: str,
) -> str | None:
    """Resolve ``repo_id`` (a UUID or ``repos.repo`` slug) to its canonical
    repo UUID IFF ``team_id`` currently owns it per ``team_repo_ownership``
    -- ``None`` if the repo does not exist, or is not currently owned by
    this team.

    CHAOS-4406: the team+repo COMBINED path can trust neither
    ``user_metrics_daily``/``team_metrics_daily``'s own ``team_id`` column
    (tainted, CHAOS-4396) nor simply drop the team filter and serve every
    signal for the repo regardless of owner. ``team_repo_ownership`` is the
    one ownership-only (CHAOS-4321) source of truth for "does this team own
    this repo" -- the SAME table, same temporal predicate
    (``valid_from``/``valid_to``), and same collapse-to-latest-row
    (``argMax(..., (updated_at, valid_from))``) as
    ``api/dev/native_status_change.py``'s canonical ``team_repository_ids``
    re-derivation, narrowed here to one candidate repo instead of a team's
    full owned set, and reusing the identical repo UUID-or-slug resolution
    predicate ``_fetch_user_metrics`` already applies.

    Ownership is checked as of "now" (``now64(3)``), not a caller-supplied
    ``as_of``: this call decides WHICH SQL path answers the request (a
    routing decision), not the attributed data itself -- the daily rows
    returned afterwards are still date-ranged historical repo-level facts,
    unaffected by today's ownership snapshot.
    """
    query = """
        SELECT toString(g.repo_id) AS repo_id
        FROM (
            SELECT
                org_id,
                provider,
                repo_full_name,
                team_id,
                argMax(repo_id, (updated_at, valid_from)) AS repo_id
            FROM team_repo_ownership
            WHERE org_id = {org_id:String}
              AND team_id = {team_id:String}
              AND valid_from <= now64(3)
              AND (valid_to IS NULL OR valid_to > now64(3))
            GROUP BY org_id, provider, repo_full_name, team_id
        ) AS g
        WHERE g.repo_id IS NOT NULL
          AND g.repo_id IN (
              SELECT id FROM repos
              WHERE org_id = {org_id:String}
                AND (repo = {repo_id:String} OR toString(id) = {repo_id:String})
          )
        LIMIT 1
    """
    params = {"org_id": org_id, "team_id": team_id, "repo_id": repo_id}
    rows = await query_dicts(client, query, params)
    if not rows:
        return None
    return str(rows[0]["repo_id"])


async def _fetch_repo_scoped_team_metrics(
    client: Any,
    *,
    org_id: str,
    since_date: str,
    until_date: str,
    repo_id: str,
) -> list[dict[str, Any]]:
    """One repo's after-hours / weekend commit-timing ratio, by day --
    collapsed across every ``team_id`` label ``team_metrics_daily``
    attached to that repo's rows, never filtered BY ``team_id``.

    CHAOS-4406: ``team_metrics_daily``'s per-row ``team_id`` is resolved
    PER COMMIT at write time (``metrics/compute_wellbeing.py``:
    repo-pattern/ownership resolution first, author-membership fallback
    second -- the CHAOS-4396 taint), so one repo's commits can be split
    across several ``(team_id, repo_id, day)`` buckets when the fallback
    fires for some authors. Once the CALLER (``resolve_cognitive_load``)
    has independently confirmed, via ``team_repo_ownership`` /
    ``_resolve_owned_repo_id`` (ownership-only attribution, CHAOS-4321),
    that this specific repo belongs to the requested team, EVERY commit
    against this repo belongs to that team by definition -- regardless of
    which (possibly wrong) ``team_id`` an individual commit's row carries.
    So this reads by ``repo_id`` ALONE, summing the additive counts across
    every ``team_id`` fragment for that repo before recomputing the ratio
    -- the same SUM-then-recompute discipline ``_fetch_team_metrics``
    already uses across a team's several repos, applied along the opposite
    axis (several ``team_id`` labels collapsing onto one repo instead of
    several repos collapsing onto one team).

    Repo resolution mirrors ``_fetch_user_metrics``'s org-scoped ``repos``
    subquery (accepts either a UUID or a ``repos.repo`` slug).
    """
    query = """
        SELECT
            day,
            if(total_commits > 0,
               total_after_hours_commits / total_commits, 0.0
            ) AS after_hours_commit_ratio,
            if(total_commits > 0,
               total_weekend_commits / total_commits, 0.0
            ) AS weekend_commit_ratio
        FROM (
            SELECT
                day,
                sum(commits_count)             AS total_commits,
                sum(after_hours_commits_count) AS total_after_hours_commits,
                sum(weekend_commits_count)      AS total_weekend_commits
            FROM (
                SELECT
                    day,
                    team_id,
                    argMax(commits_count,             computed_at) AS commits_count,
                    argMax(after_hours_commits_count, computed_at) AS after_hours_commits_count,
                    argMax(weekend_commits_count,      computed_at) AS weekend_commits_count
                FROM team_metrics_daily
                WHERE org_id = {org_id:String}
                  AND day >= {since_date:Date}
                  AND day <= {until_date:Date}
                  AND repo_id IN (
                      SELECT id FROM repos
                      WHERE org_id = {org_id:String}
                        AND (repo = {repo_id:String} OR toString(id) = {repo_id:String})
                  )
                GROUP BY day, team_id
            )
            GROUP BY day
        )
        ORDER BY day
    """
    params = {
        "org_id": org_id,
        "since_date": since_date,
        "until_date": until_date,
        "repo_id": repo_id,
    }
    return await query_dicts(client, query, params)


# ---------------------------------------------------------------------------
# team_cognitive_load_daily (CHAOS-4365 item 2, single-team path)
# ---------------------------------------------------------------------------


async def _fetch_team_cognitive_load(
    client: Any,
    *,
    org_id: str,
    team_id: str,
    since_date: str,
    until_date: str,
) -> list[dict[str, Any]]:
    """Read directly from ``team_cognitive_load_daily`` for ONE team.

    CHAOS-4365 confirmation-pass finding: the old single-team path filtered
    ``user_metrics_daily``/``team_metrics_daily`` on their OWN ``team_id``
    column, which CHAOS-4396 found can fall back to author-membership
    resolution (or stay unset for a native org whose teams have empty
    ``repo_patterns``) -- for a real org that column is empty/impure, so a
    single-team query silently returned zero signals while the org-wide
    (no team filter) query worked fine. ``team_cognitive_load_daily`` is
    already team-scoped and OWNERSHIP-resolved (CHAOS-4321) at write time
    (``metrics/team_cognitive_load.py``), so this is a single dedup read,
    not a merge of two tables.

    Codex R1 (P2): the ratio fields are ``Nullable(Float64)`` (migration
    081 -- ``None`` means unmeasured, distinct from a measured ``0.0``, see
    that migration's comment). A bare ``argMax(nullable_col, computed_at)``
    per column independently skips NULL arguments, so a day recomputed from
    "measured" to "unmeasured" (the latest row's ratio genuinely NULL)
    would keep returning the STALE non-null ratio from an older row instead
    of the latest row's true NULL. Bundling every field into one
    ``argMax(tuple(...), computed_at)`` picks the whole row atomically from
    the single latest ``computed_at``, so a NULL in the latest row stays
    NULL -- same fix as ``compounding_risk.py``'s ``_fetch_latest_rows``.
    """
    query = """
        SELECT
            day,
            tupleElement(latest_row, 1) AS pr_interruption_load,
            tupleElement(latest_row, 2) AS context_spread_count,
            tupleElement(latest_row, 3) AS review_request_load,
            tupleElement(latest_row, 4) AS after_hours_commit_ratio,
            tupleElement(latest_row, 5) AS weekend_commit_ratio
        FROM (
            SELECT
                day,
                argMax(
                    tuple(
                        pr_interruption_load,
                        context_spread_count,
                        review_request_load,
                        after_hours_commit_ratio,
                        weekend_commit_ratio
                    ),
                    computed_at
                ) AS latest_row
            FROM team_cognitive_load_daily
            WHERE org_id = {org_id:String}
              AND team_id = {team_id:String}
              AND day >= {since_date:Date}
              AND day <= {until_date:Date}
            GROUP BY day
        )
        ORDER BY day
    """
    params = {
        "org_id": org_id,
        "team_id": team_id,
        "since_date": since_date,
        "until_date": until_date,
    }
    return await query_dicts(client, query, params)


# ---------------------------------------------------------------------------
# Public resolver
# ---------------------------------------------------------------------------


async def resolve_cognitive_load(
    context: GraphQLContext,
    input: CognitiveLoadInput,
) -> CognitiveLoadResult:
    """Serve cognitive-load signals from ClickHouse (read-only).

    Org-gate is enforced via ``require_org_id``; any mismatch between the
    JWT org and the GraphQL ``orgId`` argument is logged and the JWT org wins.

    Single-team path (``team_id`` set, ``repo_id`` NOT set): reads
    ``team_cognitive_load_daily`` directly -- one dedup query, already
    team-scoped and OWNERSHIP-resolved (see ``_fetch_team_cognitive_load``).

    Team+repo COMBINED path (both set, CHAOS-4406): ``_resolve_owned_repo_id``
    confirms via ``team_repo_ownership`` that the requested repo is
    currently owned by the requested team. If not owned (or the repo does
    not exist), returns an explicit empty result rather than either the
    wrong team's data or a confusing error. If owned, both queries filter
    by ``repo_id`` alone -- never the tainted ``team_id`` column -- since
    ownership already scopes every signal for that repo to this team.

    Org-wide path (``team_id`` unset): the original two-query merge over
    ``user_metrics_daily``/``team_metrics_daily``, each deduplicating
    append-only rows via ``argMax(..., computed_at)`` before aggregating,
    then merged over the UNION of days.
    """
    authorized_org_id = require_org_id(context)
    if input.org_id != authorized_org_id:
        logger.debug(
            "Ignoring GraphQL orgId %r in favor of authorized org %r",
            input.org_id,
            authorized_org_id,
        )

    client = _require_client(context)

    since_date = input.since_date.isoformat()
    until_date = input.until_date.isoformat()

    if input.team_id and not input.repo_id:
        team_load_rows = await _fetch_team_cognitive_load(
            client,
            org_id=authorized_org_id,
            team_id=input.team_id,
            since_date=since_date,
            until_date=until_date,
        )
        team_load_signals = [
            CognitiveLoadSignal(
                day=row["day"],
                pr_interruption_load=float(row.get("pr_interruption_load") or 0.0),
                context_spread_count=float(row.get("context_spread_count") or 0.0),
                review_request_load=float(row.get("review_request_load") or 0.0),
                after_hours_commit_ratio=_nfloat(row, "after_hours_commit_ratio"),
                weekend_commit_ratio=_nfloat(row, "weekend_commit_ratio"),
            )
            for row in team_load_rows
        ]
        return CognitiveLoadResult(
            org_id=authorized_org_id,
            team_id=input.team_id,
            signals=team_load_signals,
            total_days=len(team_load_signals),
        )

    if input.team_id and input.repo_id:
        owned_repo_id = await _resolve_owned_repo_id(
            client,
            org_id=authorized_org_id,
            team_id=input.team_id,
            repo_id=input.repo_id,
        )
        if owned_repo_id is None:
            logger.debug(
                "cognitive_load: team %r does not currently own repo %r "
                "(team_repo_ownership) -- returning an explicit empty "
                "result for the team+repo combined query",
                input.team_id,
                input.repo_id,
            )
            return CognitiveLoadResult(
                org_id=authorized_org_id,
                team_id=input.team_id,
                signals=[],
                total_days=0,
            )
        user_rows = await _fetch_user_metrics(
            client,
            org_id=authorized_org_id,
            since_date=since_date,
            until_date=until_date,
            team_id=None,
            repo_id=owned_repo_id,
        )
        team_rows = await _fetch_repo_scoped_team_metrics(
            client,
            org_id=authorized_org_id,
            since_date=since_date,
            until_date=until_date,
            repo_id=owned_repo_id,
        )
        signals = _merge_user_and_team_rows(user_rows, team_rows)
        return CognitiveLoadResult(
            org_id=authorized_org_id,
            team_id=input.team_id,
            signals=signals,
            total_days=len(signals),
        )

    # Org-wide path: no team_id at all.
    user_rows = await _fetch_user_metrics(
        client,
        org_id=authorized_org_id,
        since_date=since_date,
        until_date=until_date,
        team_id=input.team_id,
        repo_id=input.repo_id,
    )
    team_rows = await _fetch_team_metrics(
        client,
        org_id=authorized_org_id,
        since_date=since_date,
        until_date=until_date,
        team_id=input.team_id,
    )
    signals = _merge_user_and_team_rows(user_rows, team_rows)

    return CognitiveLoadResult(
        org_id=authorized_org_id,
        team_id=input.team_id,
        signals=signals,
        total_days=len(signals),
    )
