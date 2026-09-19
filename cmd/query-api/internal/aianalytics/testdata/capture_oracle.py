"""Captures oracle_cases.json from the real Python AI rollup resolvers.

Run from the repository root with the project venv:

    .venv/bin/python cmd/query-api/internal/aianalytics/testdata/capture_oracle.py \
        > cmd/query-api/internal/aianalytics/testdata/oracle_cases.json

Every case scripts the loader boundary (the rollup rows, labels, raw-PR
engagement rows and reviewer loads) and records what the Python resolver
returns for it. The Go tests replay the same inputs through a scripted
ClickHouse client and require the same output.
"""

from __future__ import annotations

import asyncio
import dataclasses
import json
import re
import sys
import uuid
from datetime import date, datetime, timezone
from enum import Enum
from unittest import mock

from dev_health_ops.api.graphql.models.ai import (
    AIAttributionBucketInput,
    AIDateRangeInput,
    AIScopeInput,
)
from dev_health_ops.api.graphql.resolvers import ai as ai_mod
from dev_health_ops.metrics.loaders.ai_impact import (
    AIImpactClickHouseLoader,
    _gini,
    _to_record,
)

ORG = "org-1"
REPO_A = "11111111-1111-1111-1111-111111111111"
REPO_B = "22222222-2222-2222-2222-222222222222"
REPO_C = "33333333-3333-3333-3333-333333333333"
COMPUTED = datetime(2026, 9, 1, 12, 0, 0, 123000, tzinfo=timezone.utc)


def row(day, repo, bucket, team="", **kw):
    base = {
        "org_id": ORG,
        "team_id": team,
        "repo_id": repo,
        "work_type": "pull_request",
        "day": date.fromisoformat(day),
        "attribution_bucket": bucket,
        "prs_total": 0,
        "prs_merged": 0,
        "ai_assisted_prs": 0,
        "agent_created_prs": 0,
        "human_prs": 0,
        "unknown_prs": 0,
        "ai_assisted_pr_ratio": None,
        "cycle_time_avg_hours": None,
        "ai_cycle_time_delta_hours": None,
        "reviews_per_pr": None,
        "ai_review_amplification": None,
        "changes_requested_per_pr": None,
        "rework_prs": 0,
        "rework_drag_rate": None,
        "followup_commits_count": 0,
        "revert_prs": 0,
        "revert_rate": None,
        "incidents_count": 0,
        "incident_drag_rate": None,
        "test_gap_prs": 0,
        "test_gap_rate": None,
        "leverage_prs_component": 0.0,
        "leverage_cycle_time_component": None,
        "leverage_review_component": None,
        "leverage_rework_component": None,
        "leverage_test_component": None,
        "leverage_incident_component": None,
        "computed_at": COMPUTED,
    }
    base.update(kw)
    return base


DAILY_MIXED = [
    row(
        "2026-08-01",
        REPO_A,
        "ai_assisted",
        "team-a",
        prs_total=7,
        prs_merged=5,
        ai_assisted_prs=7,
        ai_assisted_pr_ratio=0.7,
        cycle_time_avg_hours=10.1,
        ai_cycle_time_delta_hours=-1.3,
        reviews_per_pr=1.5,
        ai_review_amplification=1.1,
        changes_requested_per_pr=0.3,
        rework_prs=2,
        rework_drag_rate=0.285714285714,
        followup_commits_count=4,
        revert_prs=1,
        revert_rate=0.142857142857,
        incidents_count=1,
        incident_drag_rate=0.142857142857,
        test_gap_prs=3,
        test_gap_rate=0.428571428571,
        leverage_prs_component=0.7,
        leverage_cycle_time_component=0.2,
        leverage_review_component=0.1,
    ),
    row(
        "2026-08-01",
        REPO_A,
        "human",
        "team-a",
        prs_total=3,
        prs_merged=3,
        human_prs=3,
        cycle_time_avg_hours=14.7,
        reviews_per_pr=2.5,
        changes_requested_per_pr=0.1,
        rework_prs=0,
        rework_drag_rate=0.0,
        followup_commits_count=1,
        revert_rate=0.0,
        incident_drag_rate=0.0,
        test_gap_rate=0.1,
        leverage_prs_component=0.3,
    ),
    row(
        "2026-08-02",
        REPO_A,
        "ai_assisted",
        "team-a",
        prs_total=3,
        prs_merged=2,
        ai_assisted_prs=3,
        ai_assisted_pr_ratio=0.5,
        cycle_time_avg_hours=9.3,
        reviews_per_pr=0.5,
        rework_prs=1,
        rework_drag_rate=0.333333333333,
        test_gap_rate=0.2,
        leverage_prs_component=0.5,
        leverage_cycle_time_component=0.4,
    ),
    row(
        "2026-08-02",
        REPO_B,
        "agent_created",
        "team-b",
        prs_total=4,
        prs_merged=0,
        agent_created_prs=4,
        cycle_time_avg_hours=None,
        reviews_per_pr=0.125,
        rework_drag_rate=0.25,
        rework_prs=1,
        leverage_prs_component=0.9,
    ),
    row(
        "2026-08-02",
        REPO_B,
        "human",
        "team-b",
        prs_total=6,
        prs_merged=6,
        human_prs=6,
        cycle_time_avg_hours=20.05,
        reviews_per_pr=3.0,
        rework_drag_rate=0.5,
        rework_prs=3,
        leverage_prs_component=0.1,
    ),
    row(
        "2026-08-03",
        REPO_C,
        "unknown",
        "",
        prs_total=2,
        prs_merged=1,
        unknown_prs=2,
        reviews_per_pr=0.25,
        rework_drag_rate=None,
        leverage_prs_component=0.0,
    ),
    row(
        "2026-08-03",
        REPO_C,
        "ai_review",
        "",
        prs_total=0,
        prs_merged=0,
        ai_assisted_pr_ratio=0.9,
        rework_drag_rate=0.9,
    ),
    row(
        "2026-08-03",
        REPO_A,
        "human",
        "team-a",
        prs_total=5,
        prs_merged=4,
        human_prs=5,
        cycle_time_avg_hours=8.0,
        reviews_per_pr=0.3,
        rework_drag_rate=0.2,
        rework_prs=1,
        leverage_prs_component=0.25,
        computed_at=datetime(2026, 9, 3, 8, 30, 0, 5000, tzinfo=timezone.utc),
    ),
]

# 0.5 * 3 = 1.5 -> banker's rounding gives 2; 2.5 * 1 = 2.5 -> 2; 0.5 * 1 = 0.5 -> 0.
DAILY_ROUNDING = [
    row(
        "2026-08-01",
        REPO_A,
        "ai_assisted",
        "team-a",
        prs_total=3,
        prs_merged=3,
        ai_assisted_prs=3,
        reviews_per_pr=0.5,
        followup_commits_count=3,
    ),
    row(
        "2026-08-01",
        REPO_A,
        "human",
        "team-a",
        prs_total=1,
        prs_merged=1,
        human_prs=1,
        reviews_per_pr=2.5,
        followup_commits_count=1,
    ),
    row(
        "2026-08-02",
        REPO_A,
        "human",
        "team-a",
        prs_total=1,
        prs_merged=1,
        human_prs=1,
        reviews_per_pr=0.5,
    ),
    row(
        "2026-08-02",
        REPO_A,
        "ai_assisted",
        "team-a",
        prs_total=2,
        prs_merged=2,
        ai_assisted_prs=2,
        reviews_per_pr=None,
    ),
]

ENGAGEMENT = [
    {
        "bucket": "ai_assisted",
        "day": date(2026, 8, 1),
        "prs_with_first_review": 5,
        "pickup_latency_hours": 3.5,
        "review_comments_total": 40,
        "loc_total": 400,
    },
    {
        "bucket": "ai_assisted",
        "day": date(2026, 8, 2),
        "prs_with_first_review": 2,
        "pickup_latency_hours": 8.25,
        "review_comments_total": 3,
        "loc_total": 0,
    },
    {
        "bucket": "human",
        "day": date(2026, 8, 1),
        "prs_with_first_review": 3,
        "pickup_latency_hours": 1.0,
        "review_comments_total": 9,
        "loc_total": 90,
    },
    {
        "bucket": "human",
        "day": date(2026, 8, 2),
        "prs_with_first_review": 0,
        "pickup_latency_hours": None,
        "review_comments_total": 0,
        "loc_total": 0,
    },
    {
        "bucket": "",
        "day": date(2026, 8, 3),
        "prs_with_first_review": 1,
        "pickup_latency_hours": 2.0,
        "review_comments_total": 1,
        "loc_total": 10,
    },
]

REPO_LABELS = {REPO_A: "acme/alpha", REPO_B: "acme/beta"}
TEAM_LABELS = {"team-a": "Team A"}


def jsonable(value):
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return {
            re.sub(r"_([a-z])", lambda m: m.group(1).upper(), f.name): jsonable(
                getattr(value, f.name)
            )
            for f in dataclasses.fields(value)
        }
    if isinstance(value, (list, tuple)):
        return [jsonable(v) for v in value]
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return value


class Ctx:
    org_id = ORG
    client = object()


def run(
    fn_name,
    args,
    daily,
    repo_labels,
    team_labels,
    engagement,
    loads,
    slug_rows=None,
    team_rows=None,
    repo_rows=None,
    engagement_error=False,
):
    records = [_to_record(dict(r)) for r in daily]

    async def load_daily(self, **kw):
        return list(records)

    async def load_repo_labels(self, ids):
        return {k: v for k, v in repo_labels.items() if k in ids}

    async def load_team_labels(self, ids):
        return {k: v for k, v in team_labels.items() if k in ids}

    async def load_engagement(self, **kw):
        if engagement_error:
            raise RuntimeError("boom")
        return [dict(r) for r in engagement]

    async def load_concentration(self, **kw):
        if not loads:
            return None, 0
        return _gini([float(v) for v in loads]), len(loads)

    async def qd(client, query, params):
        if "FROM teams" in query:
            return team_rows or []
        if "FROM repos" in query and "slug" in params:
            return slug_rows or []
        if "FROM repos" in query:
            return repo_rows or []
        return []

    with (
        mock.patch.object(
            AIImpactClickHouseLoader, "load_ai_impact_metrics", load_daily
        ),
        mock.patch.object(
            AIImpactClickHouseLoader, "load_repo_labels", load_repo_labels
        ),
        mock.patch.object(
            AIImpactClickHouseLoader, "load_team_labels", load_team_labels
        ),
        mock.patch.object(
            AIImpactClickHouseLoader, "load_review_engagement", load_engagement
        ),
        mock.patch.object(
            AIImpactClickHouseLoader, "load_reviewer_concentration", load_concentration
        ),
        mock.patch.object(ai_mod, "query_dicts", qd),
        mock.patch("dev_health_ops.api.queries.client.query_dicts", qd),
    ):
        result = asyncio.run(getattr(ai_mod, fn_name)(Ctx(), *args))
    return jsonable(result)


DR = AIDateRangeInput(start_date=date(2026, 8, 1), end_date=date(2026, 8, 3))
CASES = []


def DAILY_JSON(rows):
    out = []
    for r in rows:
        r = dict(r)
        r["day"] = r["day"].isoformat()
        r["computed_at"] = r["computed_at"].isoformat()
        out.append(r)
    return out


def ENG_JSON(rows):
    out = []
    for r in rows:
        r = dict(r)
        r["day"] = r["day"].isoformat()
        out.append(r)
    return out


def team_repo_ids(team_id, team_rows, repo_rows):
    async def qd(client, query, params):
        if "FROM teams" in query:
            return team_rows or []
        return repo_rows or []

    with mock.patch("dev_health_ops.api.queries.client.query_dicts", qd):
        got = asyncio.run(
            ai_mod._resolve_team_repo_ids(object(), org_id=ORG, team_id=team_id)
        )
    return None if got is None else sorted(str(u) for u in got)


DATASETS = {}


def dataset(name, daily, engagement, loads):
    DATASETS[name] = {
        "daily": DAILY_JSON(daily),
        "engagement": ENG_JSON(engagement),
    }


def add(
    name,
    fn,
    scope,
    ds,
    daily,
    engagement=None,
    loads=None,
    repo_labels=None,
    team_labels=None,
    **kw,
):
    engagement = engagement or []
    loads = loads or []
    rl = REPO_LABELS if repo_labels is None else repo_labels
    tl = TEAM_LABELS if team_labels is None else team_labels
    out = run(fn, (DR, scope), daily, rl, tl, engagement, loads, **kw)
    if ds not in DATASETS:
        dataset(ds, daily, engagement, loads)
    CASES.append(
        {
            "name": name,
            "fn": fn,
            "dataset": ds,
            "scope": None
            if scope is None
            else {
                "repoId": scope.repo_id,
                "teamId": scope.team_id,
                "workType": scope.work_type,
                "buckets": [b.name for b in (scope.buckets or [])],
            },
            "loads": loads,
            "repoLabels": rl,
            "teamLabels": tl,
            "slugRows": [str(r["id"]) for r in kw.get("slug_rows") or []],
            "teamRows": kw.get("team_rows"),
            "repoRows": kw.get("repo_rows"),
            "engagementError": kw.get("engagement_error", False),
            "teamRepoIds": (
                team_repo_ids(scope.team_id, kw.get("team_rows"), kw.get("repo_rows"))
                if scope is not None and scope.team_id
                else None
            ),
            "expected": out,
        }
    )


for fn in (
    "resolve_ai_impact_summary",
    "resolve_ai_comparison",
    "resolve_ai_review_load",
):
    short = fn.replace("resolve_ai_", "")
    HUMAN = [r for r in DAILY_MIXED if r["attribution_bucket"] == "human"]
    AIONLY = [r for r in DAILY_MIXED if r["attribution_bucket"] != "human"]
    add(f"{short}/mixed", fn, None, "mixed", DAILY_MIXED, ENGAGEMENT, [3, 9, 1, 0, 27])
    add(f"{short}/empty", fn, None, "empty", [], [], [])
    add(f"{short}/rounding", fn, None, "rounding", DAILY_ROUNDING, [], [2, 2])
    add(f"{short}/human_only", fn, None, "human", HUMAN, ENGAGEMENT, [5])
    add(f"{short}/ai_only", fn, None, "ai", AIONLY, [], [])
    add(
        f"{short}/workType",
        fn,
        AIScopeInput(work_type="pull_request"),
        "mixed",
        DAILY_MIXED,
        ENGAGEMENT,
        [4, 4],
    )
    add(
        f"{short}/buckets",
        fn,
        AIScopeInput(
            buckets=[
                AIAttributionBucketInput.AI_ASSISTED,
                AIAttributionBucketInput.HUMAN,
            ]
        ),
        "mixed",
        DAILY_MIXED,
        ENGAGEMENT,
        [1, 2, 3],
    )
    add(
        f"{short}/repoUnresolved",
        fn,
        AIScopeInput(repo_id="no-such/repo"),
        "mixed",
        DAILY_MIXED,
        ENGAGEMENT,
        [1],
        slug_rows=[],
    )
    add(
        f"{short}/repoSlug",
        fn,
        AIScopeInput(repo_id="acme/alpha"),
        "mixed",
        DAILY_MIXED,
        ENGAGEMENT,
        [1, 7],
        slug_rows=[{"id": uuid.UUID(REPO_A)}],
    )
    add(
        f"{short}/repoUuidUpper",
        fn,
        AIScopeInput(repo_id=REPO_A.upper()),
        "mixed",
        DAILY_MIXED,
        ENGAGEMENT,
        [1, 7],
    )
    add(
        f"{short}/teamNoRepos",
        fn,
        AIScopeInput(team_id="team-a"),
        "mixed",
        DAILY_MIXED,
        ENGAGEMENT,
        [6, 6],
        team_rows=[
            {"id": "team-a", "name": "Team A", "repo_patterns": ["acme/nothing"]}
        ],
        repo_rows=[{"repo_id": REPO_A, "full_name": "acme/alpha"}],
    )
    add(
        f"{short}/teamPatterns",
        fn,
        AIScopeInput(team_id="team-a"),
        "mixed",
        DAILY_MIXED,
        ENGAGEMENT,
        [6, 6],
        team_rows=[
            {"id": "team-a", "name": "Team A", "repo_patterns": ["ACME/*"]},
            {"id": "team-b", "name": "B", "repo_patterns": ["acme/beta"]},
        ],
        repo_rows=[
            {"repo_id": REPO_A, "full_name": "acme/alpha"},
            {"repo_id": REPO_B, "full_name": "Acme/Beta"},
        ],
    )
    add(
        f"{short}/repoBlank",
        fn,
        AIScopeInput(repo_id=""),
        "mixed",
        DAILY_MIXED,
        ENGAGEMENT,
        [1, 7],
    )
    add(
        f"{short}/teamBlank",
        fn,
        AIScopeInput(team_id=""),
        "mixed",
        DAILY_MIXED,
        ENGAGEMENT,
        [1, 7],
    )
    add(f"{short}/zeroLoads", fn, None, "mixed", DAILY_MIXED, ENGAGEMENT, [0, 0, 0])
    add(
        f"{short}/engagementError",
        fn,
        None,
        "mixed",
        DAILY_MIXED,
        ENGAGEMENT,
        [6, 6],
        engagement_error=True,
    )
    add(
        f"{short}/labelsMissing",
        fn,
        None,
        "mixed",
        DAILY_MIXED,
        ENGAGEMENT,
        [6, 6],
        repo_labels={},
        team_labels={},
    )

# eleven repositories and eleven teams with tied AI volumes: the ranked
# rollups keep the ten largest, ties by scope id.
MANY = []
for i in range(11):
    repo = f"{i + 1:08d}-0000-0000-0000-000000000000"
    MANY.append(
        row(
            "2026-08-01",
            repo,
            "ai_assisted",
            f"t{i:02d}",
            prs_total=10 - (i // 2) % 5,
            prs_merged=1,
            ai_assisted_prs=1,
            rework_drag_rate=0.1 * (i % 4),
        )
    )
    MANY.append(
        row(
            "2026-08-01",
            repo,
            "human",
            f"t{i:02d}",
            prs_total=5,
            prs_merged=5,
            human_prs=5,
            rework_drag_rate=0.2,
        )
    )
for fn in ("resolve_ai_impact_summary",):
    add(
        "impact_summary/manyScopes",
        fn,
        None,
        "many",
        MANY,
        [],
        [],
        repo_labels={},
        team_labels={"t03": "Three"},
    )

NOTEAM = [
    row(
        "2026-08-01",
        REPO_A,
        "ai_assisted",
        "",
        prs_total=4,
        prs_merged=2,
        ai_assisted_prs=4,
        rework_drag_rate=0.5,
    )
]
add(
    "impact_summary/noTeamRollups",
    "resolve_ai_impact_summary",
    None,
    "noteam",
    NOTEAM,
    [],
    [],
)
HUMANONLY = [
    row("2026-08-01", REPO_A, "human", "team-a", prs_total=4, prs_merged=2, human_prs=4)
]
add(
    "impact_summary/noRollups",
    "resolve_ai_impact_summary",
    None,
    "humanonly",
    HUMANONLY,
    [],
    [],
)

json.dump(
    {"datasets": DATASETS, "cases": CASES},
    sys.stdout,
    indent=1,
    sort_keys=True,
    default=str,
)
sys.stdout.write("\n")
