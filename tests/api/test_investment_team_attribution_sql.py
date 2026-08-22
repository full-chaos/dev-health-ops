from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, cast

import pytest

import dev_health_ops.api.queries.investment as investment_queries
from dev_health_ops.metrics.sinks.base import BaseMetricsSink


def assert_team_attribution_sql(sql: str) -> None:
    assert "WHERE org_id = %(org_id)s" in sql
    assert "arrayDistinct(arrayConcat(" in sql
    assert "JSONExtract(structural_evidence_json, 'issues', 'Array(String)')" in sql
    # CHAOS-2416: the `prs` array is the second bridge to a team. A unit whose
    # PR work item already carries a resolver-computed team must not collapse
    # to TEAM:unassigned just because the unit has no issue ref of its own.
    assert "JSONExtract(structural_evidence_json, 'prs', 'Array(String)')" in sql
    # ... resolved into the work_items id space through the repos table, per
    # provider (`ghpr:{repo}#{n}` / `gitlab:{repo}!{n}`).
    assert "evidence_repo.repo_uuid = splitByString('#pr', evidence_ref)[1]" in sql
    assert "'gitlab:', 'ghpr:'" in sql
    # The PR arm is keyed on the ref SHAPE, so an `issues` ref is never
    # rewritten and an unresolvable PR ref resolves to '' rather than leaking
    # a raw work-graph node id in as a live join key.
    assert "'^[0-9a-fA-F-]{36}#pr[0-9]+$'" in sql
    assert "t.work_item_id = multiIf(" in sql
    # CHAOS-2416: the old `[work_unit_id]` arm was dead code -- a work_unit_id
    # is a content hash, never a work_item_id, so it could not match a single
    # attribution row. It must stay deleted.
    assert "[work_unit_investments.work_unit_id]" not in sql
    assert "t.work_item_id = issue_id" not in sql
    # CHAOS-2833: team resolution MUST read the authoritative primary
    # ClickHouse attribution rows, never the legacy cycle-times rollup.
    assert "FROM work_item_team_attributions FINAL" in sql
    assert "is_primary = 1" in sql
    assert "(work_item_id, computed_at) IN" in sql
    assert "max(computed_at)" in sql
    assert "work_item_cycle_times" not in sql


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fetcher",
    [
        investment_queries.fetch_investment_team_edges,
        investment_queries.fetch_investment_repo_team_edges,
        investment_queries.fetch_investment_team_category_repo_edges,
        investment_queries.fetch_investment_team_subcategory_repo_edges,
        investment_queries.fetch_investment_unassigned_counts,
    ],
)
async def test_investment_team_attribution_sql_scopes_org_and_bridges_pr_evidence(
    monkeypatch: pytest.MonkeyPatch,
    fetcher: Any,
) -> None:
    captured: dict[str, Any] = {"sql": "", "params": {}}

    async def fake_query_dicts(
        _sink: BaseMetricsSink, sql: str, params: dict[str, Any]
    ):
        captured["sql"] = sql
        captured["params"] = params
        return []

    monkeypatch.setattr(investment_queries, "query_dicts", fake_query_dicts)

    await fetcher(
        cast(BaseMetricsSink, object()),
        start_ts=datetime(2026, 5, 24, tzinfo=timezone.utc),
        end_ts=datetime(2026, 6, 8, tzinfo=timezone.utc),
        scope_filter="",
        scope_params={},
        org_id="org-1",
    )

    assert_team_attribution_sql(str(captured["sql"]))
    assert captured["params"]["org_id"] == "org-1"


@pytest.mark.asyncio
async def test_investment_quality_stats_team_scope_sql_uses_primary_attribution(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CHAOS-2833: fetch_investment_quality_stats' team_scope_ids join is the
    same unit_team pattern duplicated across investment.py -- it must be
    migrated to the primary attribution source too, not just the edge
    fetchers covered above.
    """
    captured: dict[str, Any] = {"sql": "", "params": {}}

    async def fake_query_dicts(
        _sink: BaseMetricsSink, sql: str, params: dict[str, Any]
    ):
        captured["sql"] = sql
        captured["params"] = params
        return []

    monkeypatch.setattr(investment_queries, "query_dicts", fake_query_dicts)

    await investment_queries.fetch_investment_quality_stats(
        cast(BaseMetricsSink, object()),
        start_ts=datetime(2026, 5, 24, tzinfo=timezone.utc),
        end_ts=datetime(2026, 6, 8, tzinfo=timezone.utc),
        scope_filter="",
        scope_params={},
        org_id="org-1",
        team_scope_ids=["team-1"],
    )

    assert_team_attribution_sql(str(captured["sql"]))
    assert captured["params"]["org_id"] == "org-1"


def test_provider_ambiguous_repo_uuid_fails_closed_in_every_rendered_bridge() -> None:
    """`derive_repo_uuid` seeds on the slug WITHOUT the provider, so a GitHub
    and a GitLab repo sharing a slug collide on one UUID. Electing a provider
    by `argMax` there is a coin flip, and if BOTH namespaces' work items carry
    attribution the loser's team can be attached -- a WRONG team, not a
    missing one. So the bridge fails closed: a UUID resolving to more than one
    provider yields an empty provider and bridges nothing.

    This is asserted on the rendered SQL rather than end-to-end because the
    ambiguity is only OBSERVABLE while both ReplacingMergeTree versions are
    un-merged -- `repos` is keyed on `(org_id, id)`, so a merge collapses them
    to a single provider. Reproducing that window live means suppressing
    merges, which deadlocks the mutation-based cleanup these tests use; a test
    that depends on merge timing is a flake, and a flake that silently passes
    is worse than no test. The residual -- once merged, the surviving row's
    provider is simply whichever synced last -- is a limitation of the id seed
    and cannot be repaired in the query layer; it is documented at
    `WORK_UNIT_EVIDENCE_REPO_SOURCE`.
    """
    rendered = investment_queries.build_unit_team_subquery(
        source="latest_work_unit_investments AS work_unit_investments"
    )
    assert "if(uniqExact(provider) = 1, argMax(provider, last_synced), '')" in rendered
    assert "evidence_repo.provider = ''" in rendered
    # ... and the guard must survive into every real call site, not just a
    # default render.
    for sql_path in (
        investment_queries.build_unit_team_subquery(
            source=investment_queries.REPO_ALLOCATED_WORK_UNIT_INVESTMENTS_SOURCE,
            include_team_id=True,
        ),
        investment_queries.build_unit_team_subquery(
            source="latest_work_unit_investments AS work_unit_investments",
            inner_team_alias="team",
            outer_team_alias="team_label",
            include_team_id=True,
        ),
    ):
        assert "uniqExact(provider) = 1" in sql_path
        assert "evidence_repo.provider = ''" in sql_path


def test_unit_team_vote_is_canonical_and_deduplicated() -> None:
    """Mechanism guard for SQL<->drilldown parity (CHAOS-2416).

    `api/services/work_units.py:_majority_team_for_issues` resolves a unit's
    team in Python. It can only agree with the SQL if both apply the SAME
    three rules, and each rule is asserted here on the rendered SQL rather
    than through an outcome: a behavioural test can pass while the mechanism
    is wrong, because a degenerate tie-break may happen to return the expected
    team anyway.

    1. Votes are grouped by TEAM ID, never by the rendered label -- otherwise
       one team_id spelled with two team_names splits into two candidates on
       the SQL side only.
    2. The label is `max(label)` over the winning id, so it is a function of
       the id rather than of row order.
    3. The vote counts DISTINCT RESOLVED work-item ids. `arrayDistinct` only
       collapses identical ref strings, and one work item can appear both as a
       resolved `issues` entry and as a `prs` ref -- two strings, one item.
    """
    rendered = investment_queries.build_unit_team_subquery(
        source="latest_work_unit_investments AS work_unit_investments",
        include_team_id=True,
    )
    team_expr = "ifNull(nullIf(t.team_name, ''), nullIf(t.team_id, ''))"

    # 1. grouped by the id alone -- the label must NOT be a grouping key.
    assert "GROUP BY work_unit_id, resolved_team_id" in rendered
    assert "GROUP BY work_unit_id, resolved_team_id, resolved_team" not in rendered

    # 2. label derived deterministically from the winning id.
    assert f"max({team_expr}) AS resolved_team" in rendered

    # 3. distinct RESOLVED ids, not raw refs.
    assert "uniqExactIf(" in rendered
    assert f"countIf({team_expr} IS NOT NULL) AS cnt" not in rendered

    # Tie-break on (count, team id) on both projections, so the id and the
    # label come from one winning row rather than being spliced.
    assert "argMax(resolved_team, (cnt, resolved_team_id))" in rendered
    assert "argMax(resolved_team_id, (cnt, resolved_team_id))" in rendered

    # The vote column must not share a name with any caller's output alias:
    # `argMax(team_label, (cnt, team_label)) AS team_label` is an aggregate
    # nested in itself -> ILLEGAL_AGGREGATION (184).
    for outer in ("team", "team_label"):
        sql = investment_queries.build_unit_team_subquery(
            source="latest_work_unit_investments AS work_unit_investments",
            inner_team_alias=outer,
            include_team_id=True,
        )
        assert f"argMax(resolved_team, (cnt, resolved_team_id)) AS {outer}" in sql


def test_repo_identity_is_resolved_per_tenant_and_per_version() -> None:
    """Mechanism guard for the repos lookup behind the PR bridge.

    `repos` is a ReplacingMergeTree keyed on `(org_id, id)` (migration 027 --
    migration 000 still declares the stale `(id)`), and `repos.id` is derived
    from the repo slug with NO org in the seed, so two tenants syncing one
    repo mint the same UUID and each keep a row. Resolving by UUID alone would
    argMax across tenants and hand one org another's slug or provider.

    Version selection is asserted here rather than through a live fixture: the
    pre-merge state that distinguishes argMax from argMin is only observable
    until a background merge runs, so a behavioural test of it is
    merge-timing-dependent.
    """
    rendered = investment_queries.build_unit_team_subquery(
        source="latest_work_unit_investments AS work_unit_investments"
    )
    # Tenant scoping: filtered, grouped AND joined on org_id.
    assert "WHERE org_id = %(org_id)s" in rendered
    assert "GROUP BY org_id, id" in rendered
    assert "ON evidence_repo.org_id = work_unit_investments.org_id" in rendered
    # Version selection: newest by last_synced, never a raw row.
    assert "argMax(repo, last_synced) AS repo" in rendered
    assert "argMax(provider, last_synced)" in rendered
