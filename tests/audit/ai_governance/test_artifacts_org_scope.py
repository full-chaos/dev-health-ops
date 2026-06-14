"""CHAOS-2396: the AI-governance artifacts join must be org-scoped.

Aligning the GitHub ai_attribution ``subject_id`` to the bare PR number makes the
governance loader's ``git_pull_requests`` join finally fire for GitHub. That join
keyed only on ``a.repo_id = pr.repo_id AND a.subject_id = toString(pr.number)``;
because ``(repo_id, number)`` repeats across orgs (duplicate ``repos.id``), an
unscoped join would let one tenant's attribution enrich from another tenant's PR
(reviews_count -> human_reviewed) and inflate coverage counts via fan-out. The
sibling readers (ai_detector, ai_impact) already carry the tenant key in the
join; this locks the governance loader to the same invariant.
"""

from __future__ import annotations

from dev_health_ops.audit.ai_governance.loaders import _ARTIFACTS_SQL


def test_git_pull_requests_join_is_org_scoped() -> None:
    sql = _ARTIFACTS_SQL
    join_start = sql.index("LEFT JOIN git_pull_requests AS pr")
    # The subject_id match is the last predicate of this join's ON clause.
    join_end = sql.index("a.subject_id = toString(pr.number)", join_start)
    join_block = sql[join_start:join_end]

    # The tenant key must be part of the JOIN (not just the outer WHERE on `a`),
    # otherwise pr can be another org's row sharing repo_id + number.
    assert "pr.org_id = {org_id:String}" in join_block
    # The outer query still filters the attribution side too (defence in depth).
    assert "WHERE toString(a.org_id) = {org_id:String}" in sql


def test_scan_and_finding_subqueries_are_org_scoped() -> None:
    # CHAOS-2396: the ci_pipeline_runs (scan) and security_alerts (finding)
    # enrichment subqueries aggregate by repo_id and join on a.repo_id; without
    # an org predicate they would inherit another tenant's security_scanned /
    # license_or_dependency_finding booleans via the duplicate-repos.id artifact.
    sql = _ARTIFACTS_SQL
    # Forward window from each source table to its GROUP BY (the subquery body).
    scan_i = sql.index("FROM ci_pipeline_runs")
    scan_block = sql[scan_i : sql.index("GROUP BY repo_id", scan_i)]
    finding_i = sql.index("FROM security_alerts")
    finding_block = sql[finding_i : sql.index("GROUP BY repo_id", finding_i)]
    assert "org_id = {org_id:String}" in scan_block
    assert "org_id = {org_id:String}" in finding_block
