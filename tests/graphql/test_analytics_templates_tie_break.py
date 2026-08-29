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
each guard fail``). Each of the original 11 sites' tests is RED on the
untouched baseline (``origin/feature/chaos-4352-go-api`` @ 1bc76e8cf) and
GREEN with the fix -- proven against a committed, unmodified checkout of
that exact SHA in a throwaway detached worktree (11 failed naming their own
template, 1 passed -- see the CORRECTION below for why that 12th pass was
itself wrong), never via ``git stash`` (see PR TEST-EVIDENCE).

CORRECTION (codex review on PR #2005): ``catalog_values_team_template``
(templates.py:515) was originally believed already-deterministic
(``ORDER BY count DESC, t.name ASC``) and left unchanged -- that belief was
wrong. The returned ``value`` column is ``t.id``, not ``t.name``, and
``t.name`` has no uniqueness constraint, so two teams sharing both ``name``
and activity ``count`` genuinely tie. It is now fixed the same as every
other site (``+ t.id ASC``) -- see
``test_catalog_values_team_template_order_by_has_deterministic_tie_break``
and the duplicate-name defect-planting test below it.

One site remains reviewed and deliberately left unchanged (see PR body for
the full site inventory): the repo catalog inline query in ``compiler.py``
(``compile_catalog_values``, ~line 601): ``ORDER BY value`` where ``value``
IS the ``GROUP BY canonical_repo`` key -- already a total order, nothing to
add.
"""

from __future__ import annotations

from dev_health_ops.api.graphql.sql.templates import (
    breakdown_template,
    catalog_values_team_template,
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


def test_catalog_values_team_template_order_by_has_deterministic_tie_break():
    """``catalog_values_team_template`` was believed already-deterministic
    (``ORDER BY count DESC, t.name ASC``) and this fix originally left it
    unchanged -- CORRECTION (codex review on PR #2005, CHAOS-4495): that
    belief was wrong. The returned ``value`` column is ``t.id``
    (``SELECT t.id AS value``), not ``t.name`` -- and ``t.name`` carries no
    uniqueness constraint (``ClickHouseTeamAdminService.create_or_update``,
    ``api/services/configuration/clickhouse_team_admin.py``, accepts any
    name with no check against other teams' names; ``POST /teams`` calls it
    with no pre-check either). Two active teams sharing both ``name`` and
    activity ``count`` at the ``LIMIT`` boundary genuinely tie on
    ``(count, name)`` -- which one's ``t.id`` (the actual returned value)
    wins, and in what order, was undefined. Fix: append ``t.id ASC``, the
    actually-returned and uniquely-identifying column, same shape as every
    other site in this PR.

    This correction also propagated beyond this PR: the "L515 already
    deterministic" claim originated in the CHAOS-4495 ticket itself and was
    carried into the CHAOS-4370 Wave 4 split comment, the canonical
    handoff, and every Wave 4 lane brief -- see PR #2005 RISK-NOTES for the
    record correction.
    """
    sql = catalog_values_team_template()
    assert "ORDER BY count DESC, t.name ASC, t.id ASC" in sql, (
        f"catalog_values_team_template must carry t.id as the final "
        f"tie-break -- t.name alone does not distinguish two teams with "
        f"equal (count, name): {sql}"
    )
    assert "ORDER BY count DESC, t.name ASC, t.id ASC\nLIMIT %(limit)s" in sql


def test_catalog_values_team_template_duplicate_name_and_count_tie_without_id():
    """Plants the specific defect this fix's tie-break closes (AGENTS.md
    verification rule 2): two teams with equal ``name`` and equal ``count``
    are NOT distinguished by ``(count, name)`` alone -- ``t.id`` is required
    to reach a total order over the actually-returned ``value`` column.

    This does not require a live ClickHouse engine -- it demonstrates the
    ordering-key gap directly: sorting by the OLD key leaves the two rows'
    relative position unresolved (a stable sort just preserves whatever
    order they arrived in, which is not a query contract), while sorting by
    the FIXED key (with ``id`` appended) is total and reproducible
    regardless of input order.
    """
    # Two active teams, same name, same count -- the exact tie condition
    # ``(count, name)`` cannot resolve. Listed in two different input orders
    # to prove the OLD key's "sortedness" was an accident of input order,
    # not a query guarantee.
    team_b_first = [
        {"id": "team-b", "name": "Core Platform", "count": 5},
        {"id": "team-a", "name": "Core Platform", "count": 5},
    ]
    team_a_first = [
        {"id": "team-a", "name": "Core Platform", "count": 5},
        {"id": "team-b", "name": "Core Platform", "count": 5},
    ]

    def old_key(row: dict) -> tuple:
        return (-row["count"], row["name"])  # matches "count DESC, t.name ASC"

    def fixed_key(row: dict) -> tuple:
        return (-row["count"], row["name"], row["id"])  # + t.id ASC

    old_from_b_first = sorted(team_b_first, key=old_key)
    old_from_a_first = sorted(team_a_first, key=old_key)
    # OLD key: both are "sorted" (equal keys means any order satisfies the
    # comparator), but a stable sort just preserves input order -- the two
    # runs disagree on which team.id comes first, exactly the defect this
    # ticket exists to fix.
    assert [r["id"] for r in old_from_b_first] == ["team-b", "team-a"]
    assert [r["id"] for r in old_from_a_first] == ["team-a", "team-b"]
    assert [r["id"] for r in old_from_b_first] != [r["id"] for r in old_from_a_first], (
        "sanity check: the OLD (count, name) key must NOT resolve this tie "
        "-- if it does, the seeded fixture stopped being a genuine tie"
    )

    fixed_from_b_first = sorted(team_b_first, key=fixed_key)
    fixed_from_a_first = sorted(team_a_first, key=fixed_key)
    # FIXED key: input order no longer matters -- both converge on the same
    # id-ordered result, which is what "ORDER BY count DESC, t.name ASC,
    # t.id ASC" guarantees ClickHouse will also converge on.
    assert [r["id"] for r in fixed_from_b_first] == ["team-a", "team-b"]
    assert [r["id"] for r in fixed_from_a_first] == ["team-a", "team-b"]
