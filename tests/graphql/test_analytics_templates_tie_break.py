"""SQL-text pins: analytics SQL templates carry a deterministic ORDER BY tie-break
at their truncation boundary (CHAOS-4495).

Every ``ORDER BY value DESC LIMIT n`` (or ``count DESC LIMIT n``) template in
``sql/templates.py`` had no secondary sort key, so ClickHouse's block-parallel
execution does not guarantee a stable row order/set among rows tied on the
primary sort column -- a tie sitting at the ``LIMIT`` boundary can return a
different row subset on different runs, and on the Python vs. Go planes,
making the CHAOS-4381 stage-2 dual-run comparator unable to ever reach a
stable ``match`` verdict. Same class as CHAOS-4421 (reviewEdges, PR #1980)
and CHAOS-4472 (hotspots, PR #1996), both already Done.

Fix: append the template's own GROUP BY / SELECT dimension key(s) -- already
selected, no synthetic tiebreaker column -- to the ORDER BY, same shape as
#1980/#1996.

One test per template (not one test asserting a list): a per-template
failure names the template, per AGENTS.md verification rule 2 (``observe
each guard fail``). Each test is RED on the untouched baseline
(``origin/feature/chaos-4352-go-api`` @ 1bc76e8cf) and GREEN with the fix --
proven against a committed, unmodified checkout of that exact SHA in a
throwaway detached worktree (11 failed naming their own template, 1 passed
-- the already-correct L515 guard), never via ``git stash`` (see PR
TEST-EVIDENCE).

Two sites were reviewed and found ALREADY deterministic, so are deliberately
left unchanged (see PR body for the full site inventory):
  - ``catalog_values_team_template`` (templates.py:515): tie-break already
    present.
  - The repo catalog inline query in ``compiler.py`` (``compile_catalog_values``,
    ~line 601): ``ORDER BY value`` where ``value`` IS the ``GROUP BY
    canonical_repo`` key -- already a total order, nothing to add.
"""

from __future__ import annotations

from dev_health_ops.api.graphql.sql.templates import (
    breakdown_template,
    catalog_values_template,
    flow_matrix_repo_edges_template,
    flow_matrix_repo_nodes_template,
    flow_matrix_team_edges_template,
    flow_matrix_team_nodes_template,
    flow_matrix_work_type_edges_template,
    flow_matrix_work_type_nodes_template,
    sankey_edges_template,
    sankey_nodes_template,
    timeseries_template,
)
from dev_health_ops.api.graphql.sql.validate import BucketInterval, Dimension, Measure


def test_timeseries_order_by_has_deterministic_tie_break():
    """timeseries: GROUP BY bucket, dimension_value -- dimension_value is the
    tie-break (bucket already orders; value ties leave dimension_value as the
    only remaining discriminator over the GROUP BY key)."""
    sql = timeseries_template(Dimension.REPO, Measure.COUNT, BucketInterval.DAY)
    assert "ORDER BY bucket ASC, value DESC, dimension_value ASC" in sql, (
        "timeseries_template must carry dimension_value as the tie-break "
        f"after bucket/value: {sql}"
    )


def test_breakdown_order_by_has_deterministic_tie_break():
    """breakdown: GROUP BY dimension_value -- the tie-break IS the group key."""
    sql = breakdown_template(Dimension.REPO, Measure.COUNT)
    assert "ORDER BY value DESC, dimension_value ASC" in sql, (
        f"breakdown_template must carry dimension_value as the tie-break: {sql}"
    )
    assert "ORDER BY value DESC, dimension_value ASC\nLIMIT %(top_n)s" in sql


def test_sankey_nodes_order_by_has_deterministic_tie_break():
    """sankey nodes: GROUP BY node_id -- the tie-break IS the group key."""
    sql = sankey_nodes_template([Dimension.REPO, Dimension.TEAM], Measure.COUNT)
    assert "ORDER BY value DESC, node_id ASC" in sql, (
        f"sankey_nodes_template must carry node_id as the tie-break: {sql}"
    )
    assert "ORDER BY value DESC, node_id ASC\nLIMIT %(limit_per_dim)s" in sql


def test_sankey_edges_order_by_has_deterministic_tie_break():
    """sankey edges: GROUP BY source, target -- both columns are the tie-break."""
    sql = sankey_edges_template(Dimension.REPO, Dimension.TEAM, Measure.COUNT)
    assert "ORDER BY value DESC, source ASC, target ASC" in sql, (
        f"sankey_edges_template must carry source, target as the tie-break: {sql}"
    )
    assert "ORDER BY value DESC, source ASC, target ASC\nLIMIT %(max_edges)s" in sql


def test_flow_matrix_team_nodes_order_by_has_deterministic_tie_break():
    sql = flow_matrix_team_nodes_template()
    assert "ORDER BY value DESC, node_id ASC" in sql, (
        f"flow_matrix_team_nodes_template must carry node_id as the tie-break: {sql}"
    )


def test_flow_matrix_team_edges_order_by_has_deterministic_tie_break():
    sql = flow_matrix_team_edges_template()
    assert "ORDER BY value DESC, source ASC, target ASC" in sql, (
        "flow_matrix_team_edges_template must carry source, target as the "
        f"tie-break: {sql}"
    )


def test_flow_matrix_repo_nodes_order_by_has_deterministic_tie_break():
    sql = flow_matrix_repo_nodes_template()
    assert "ORDER BY value DESC, node_id ASC" in sql, (
        f"flow_matrix_repo_nodes_template must carry node_id as the tie-break: {sql}"
    )


def test_flow_matrix_repo_edges_order_by_has_deterministic_tie_break():
    sql = flow_matrix_repo_edges_template()
    assert "ORDER BY value DESC, source ASC, target ASC" in sql, (
        "flow_matrix_repo_edges_template must carry source, target as the "
        f"tie-break: {sql}"
    )


def test_flow_matrix_work_type_nodes_order_by_has_deterministic_tie_break():
    sql = flow_matrix_work_type_nodes_template()
    assert "ORDER BY value DESC, node_id ASC" in sql, (
        "flow_matrix_work_type_nodes_template must carry node_id as the "
        f"tie-break: {sql}"
    )


def test_flow_matrix_work_type_edges_order_by_has_deterministic_tie_break():
    sql = flow_matrix_work_type_edges_template()
    assert "ORDER BY value DESC, source ASC, target ASC" in sql, (
        "flow_matrix_work_type_edges_template must carry source, target as "
        f"the tie-break: {sql}"
    )


def test_catalog_values_order_by_has_deterministic_tie_break():
    """catalog values: GROUP BY value -- the tie-break IS the group key
    (distinct from catalog_values_team_template, which was already correct)."""
    sql = catalog_values_template(Dimension.REPO)
    assert "ORDER BY count DESC, value ASC" in sql, (
        f"catalog_values_template must carry value as the tie-break: {sql}"
    )
    assert "ORDER BY count DESC, value ASC\nLIMIT %(limit)s" in sql


def test_catalog_values_team_template_unchanged_already_deterministic():
    """Regression guard: catalog_values_team_template was ALREADY correct
    (``ORDER BY count DESC, t.name ASC``) -- CHAOS-4495 leaves it alone.
    Pinned here so a future edit that drops the tie-break is caught."""
    from dev_health_ops.api.graphql.sql.templates import catalog_values_team_template

    sql = catalog_values_team_template()
    assert "ORDER BY count DESC, t.name ASC" in sql
