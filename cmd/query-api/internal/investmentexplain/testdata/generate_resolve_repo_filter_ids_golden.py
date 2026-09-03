"""Golden generator for ResolveRepoFilterIDs (repofilter.go) -- CHAOS-4977
scope-gap closure. Calls the REAL resolve_repo_filter_ids
(api/services/filtering.py:95-110) end to end, with query_dicts
monkeypatched to a small in-memory fixture (no live database) that mirrors
repos/user_metrics_daily's shape closely enough to exercise every branch:
scope.level in {org, repo, team}, a mixed UUID/slug repo_refs list, an
unresolvable slug (silently skipped), and what.repos supplementing scope.

Run from the repo root (needs `uv sync --extra dev` once):
    uv run python cmd/query-api/internal/investmentexplain/testdata/generate_resolve_repo_filter_ids_golden.py
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any
from unittest import mock

from dev_health_ops.api.models.filters import MetricFilter, ScopeFilter, WhatFilter
from dev_health_ops.api.queries import scopes as scopes_module
from dev_health_ops.api.services.filtering import resolve_repo_filter_ids

OUT_DIR = Path(__file__).parent
ORG_ID = "org-1"

REPO_ONE_ID = "11111111-1111-4111-8111-111111111111"
REPO_TWO_ID = "22222222-2222-4222-8222-222222222222"
REPO_THREE_ID = "33333333-3333-4333-8333-333333333333"

# (id, org_id, repo)
REPOS = [
    (REPO_ONE_ID, ORG_ID, "myorg/repo-one"),
    (REPO_TWO_ID, ORG_ID, "myorg/repo-two"),
    (REPO_THREE_ID, "org-2", "otherorg/repo-three"),  # different org
]

# (team_id, org_id, repo_id)
USER_METRICS_DAILY = [
    ("team-a", ORG_ID, REPO_ONE_ID),
    ("team-a", ORG_ID, REPO_TWO_ID),
    ("team-b", ORG_ID, REPO_ONE_ID),
]


async def _fake_query_dicts(
    sink, query: str, params: dict[str, Any]
) -> list[dict[str, Any]]:
    if "FROM repos" in query and "WHERE id = " in query:
        repo_id = params["repo_id"]
        org_id = params["org_id"]
        return [
            {"id": r_id}
            for (r_id, r_org, _r_name) in REPOS
            if r_id == repo_id and r_org == org_id
        ][:1]
    if "FROM repos" in query and "WHERE repo = " in query:
        repo_name = params["repo_name"]
        org_id = params["org_id"]
        return [
            {"id": r_id}
            for (r_id, r_org, r_name) in REPOS
            if r_name == repo_name and r_org == org_id
        ][:1]
    if "FROM user_metrics_daily" in query:
        team_ids = set(params["team_ids"])
        org_id = params["org_id"]
        seen: list[str] = []
        for t_id, o_id, r_id in USER_METRICS_DAILY:
            if t_id in team_ids and o_id == org_id and r_id not in seen:
                seen.append(r_id)
        return [{"id": r_id} for r_id in seen]
    raise AssertionError(f"unexpected query in fixture: {query!r}")


async def run_case(
    case_name: str, *, scope_level: str, scope_ids: list[str], what_repos: list[str]
) -> dict[str, Any]:
    filters = MetricFilter(
        scope=ScopeFilter(level=scope_level, ids=scope_ids),
        what=WhatFilter(repos=what_repos),
    )
    with mock.patch.object(scopes_module, "query_dicts", _fake_query_dicts):
        resolved = await resolve_repo_filter_ids(
            sink=object(), filters=filters, org_id=ORG_ID
        )
    return {"case": case_name, "resolved": resolved}


async def main() -> None:
    cases = [
        dict(
            case_name="org_scope_no_repos",
            scope_level="org",
            scope_ids=[],
            what_repos=[],
        ),
        dict(
            case_name="repo_scope_mixed_uuid_and_slug",
            scope_level="repo",
            scope_ids=[REPO_ONE_ID, "myorg/repo-two"],
            what_repos=[],
        ),
        dict(
            case_name="repo_scope_unresolvable_slug_skipped",
            scope_level="repo",
            scope_ids=["myorg/repo-one", "nonexistent/repo"],
            what_repos=[],
        ),
        dict(
            case_name="org_scope_with_what_repos",
            scope_level="org",
            scope_ids=[],
            what_repos=["myorg/repo-one"],
        ),
        dict(
            case_name="team_scope",
            scope_level="team",
            scope_ids=["team-a"],
            what_repos=[],
        ),
        dict(
            case_name="team_scope_with_what_repos",
            scope_level="team",
            scope_ids=["team-b"],
            what_repos=["myorg/repo-two"],
        ),
    ]
    for case in cases:
        golden = await run_case(**case)
        out_path = OUT_DIR / f"resolve_repo_filter_ids__{golden['case']}.json"
        out_path.write_text(json.dumps(golden, indent=2, sort_keys=True) + "\n")
        print(f"wrote {out_path}  resolved={golden['resolved']}")


if __name__ == "__main__":
    asyncio.run(main())
