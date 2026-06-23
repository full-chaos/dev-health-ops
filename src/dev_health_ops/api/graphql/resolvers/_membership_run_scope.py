"""Shared latest-complete-run scoping for ``work_unit_membership`` reads (CHAOS-2433).

Every reader of ``work_unit_membership`` MUST scope identically: select the latest
COMPLETE run via ``work_unit_membership_runs`` (a run is complete only once its
marker row exists), recognise the seeded ``__legacy__`` marker (migration 048) and
map it back to each node's latest pre-migration (``run_id = ''``) row, and treat an
org with NO complete run as having no membership (the empty-string guard
``latest_run.latest_run_id != ''`` is the caller's responsibility).

These were originally private to ``resolvers/work_graph.py``. They live here now so
the work-graph annotation reader and the work-unit team-attribution reader cannot
drift: a partial reimplementation that scopes with a plain
``run_id = argMax(run_id, completed_at)`` silently drops results for migrated/idle
tenants whose latest marker is still ``__legacy__`` while their rows are
``run_id = ''`` (CHAOS-2608 codex finding).

Usage (mirror work_graph.py)::

    WITH latest_run AS ({LATEST_COMPLETE_RUN_SUBQUERY})
    SELECT ...
    FROM work_unit_membership AS m
    INNER JOIN latest_run ON 1 = 1
    {LEGACY_NODE_MAX_JOIN}
    WHERE m.org_id = %(org_id)s
      AND latest_run.latest_run_id != ''
      AND ({RUN_SCOPE_PREDICATE})

The aliases are fixed: ``m`` = ``work_unit_membership``, ``latest_run`` =
``LATEST_COMPLETE_RUN_SUBQUERY``, ``lnm`` = ``LEGACY_NODE_MAX_JOIN``. The
subqueries are org-scoped via ``%(org_id)s``.
"""

from __future__ import annotations

LEGACY_RUN_ID = "__legacy__"

LATEST_COMPLETE_RUN_SUBQUERY = """
        SELECT argMax(run_id, completed_at) AS latest_run_id
        FROM work_unit_membership_runs
        WHERE org_id = %(org_id)s
"""

# LEFT JOIN to an inline derived table computing, per (org, node), the MAX
# computed_at among LEGACY rows (run_id = '').  Bound to ``m`` by (org, node) and
# aliased ``lnm`` so the scope predicate can restrict the legacy branch to each
# node's most recent pre-migration row — the exact old per-node-latest behavior.
# Inlined (not a top-level CTE) so it works identically in every reader.  Only
# meaningful when the latest run is the legacy marker; harmless (unmatched LEFT
# JOIN) otherwise.  Org-scoped via %(org_id)s.
LEGACY_NODE_MAX_JOIN = """
            LEFT JOIN (
                SELECT
                    org_id,
                    node_type,
                    node_id,
                    max(computed_at) AS legacy_max_computed_at
                FROM work_unit_membership
                WHERE org_id = %(org_id)s AND run_id = ''
                GROUP BY org_id, node_type, node_id
            ) AS lnm
                ON lnm.org_id = m.org_id
                AND lnm.node_type = m.node_type
                AND lnm.node_id = m.node_id
"""

# Scope predicate (legacy-vs-real conditional).  REAL run -> run_id equality (one
# generation per run_id).  LEGACY run -> the node's latest pre-migration row only
# (run_id='' AND computed_at == that node's legacy max), NOT a blanket run_id=''
# match (which would resurface stale rows from earlier re-materializations whose
# distinct category values survived the old non-run_id dedup key).  The
# empty-string guard (latest_run.latest_run_id != '') stays the caller's
# responsibility so orgs with no complete run resolve to "no membership".
RUN_SCOPE_PREDICATE = (
    f"(latest_run.latest_run_id != '{LEGACY_RUN_ID}' "
    "AND m.run_id = latest_run.latest_run_id) "
    f"OR (latest_run.latest_run_id = '{LEGACY_RUN_ID}' AND m.run_id = '' "
    "AND m.computed_at = lnm.legacy_max_computed_at)"
)
