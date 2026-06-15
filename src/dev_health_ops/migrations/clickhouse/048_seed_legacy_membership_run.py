"""Migration 048: seed a LEGACY completion marker for pre-existing membership.

CHAOS-2433 finding #3 (migration 047 orphans existing rows)
-----------------------------------------------------------
Migration 047 added ``run_id String DEFAULT ''`` to ``work_unit_membership`` and
created ``work_unit_membership_runs`` (the completion-marker table).  The new
read path scopes every membership query to the latest COMPLETE run, selected via
``argMax(run_id, completed_at) FROM work_unit_membership_runs``.

Immediately after 047 deploys, every PRE-EXISTING membership row carries the
empty default ``run_id = ''`` and NO marker exists in ``work_unit_membership_runs``.
The reader would therefore find no complete run and treat the org as having no
membership at all — theme filters and annotations would break until the next
real (org-wide) run published a marker.  On idle-sync orgs that could be hours
or a full day (the daily backfill cadence).

FIX (low-risk, no heavy live mutation)
---------------------------------------
We do NOT rewrite the (potentially millions of) existing rows.  Instead we seed
ONE synthetic marker per distinct ``org_id`` that has membership rows, using the
reserved run_id ``__legacy__`` and ``completed_at = max(computed_at)`` of that
org's existing membership rows:

    INSERT INTO work_unit_membership_runs (org_id, run_id, completed_at)
    SELECT org_id, '__legacy__', max(computed_at)
    FROM work_unit_membership
    GROUP BY org_id

The reader (resolvers/work_graph.py, ``_RUN_SCOPE_JOIN_ON``) recognises the
reserved ``__legacy__`` run_id and, when it is the org's latest complete run,
matches the pre-existing rows (``run_id = ''``) rather than rows literally tagged
``__legacy__``.  So existing membership stays readable the instant 047+048 land.

CONVERGENCE / RETIREMENT
------------------------
The legacy marker's ``completed_at`` is the max of HISTORICAL membership rows
(wall-clock in the past).  Every real run publishes its marker with
``completed_at = now()`` (strictly greater), so ``argMax`` selects the real run
the moment one completes and the legacy marker stops being the latest — the
legacy read path retires automatically with no cleanup.  We never delete the
legacy marker (harmless once superseded; deleting it would only matter if an org
later had ALL real markers removed, which does not happen).

IDEMPOTENT
----------
``work_unit_membership_runs`` is a ``ReplacingMergeTree(completed_at)`` keyed on
``(org_id, run_id)``.  Re-running this migration re-inserts the same
``(org_id, '__legacy__')`` key with the same (or a not-greater) completed_at, so
the dedup collapses it — no duplicate legacy markers.  We also guard: if an org
already has a NON-legacy marker we still seed the legacy one (it is harmless and
already superseded by argMax), keeping the migration a pure additive backfill.

NOTE: loaded standalone by the migration runner
(importlib.util.spec_from_file_location), so it must not import from sibling
migration modules.
"""

import logging

log = logging.getLogger(__name__)

_LEGACY_RUN_ID = "__legacy__"


def _table_exists(client, table: str) -> bool:
    """Return whether ``table`` exists — FAIL CLOSED (CHAOS-2433 round-6).

    Let an unexpected system.tables probe error PROPAGATE so the migration RAISES
    and is NOT recorded as applied (retryable), rather than being swallowed into a
    silent "table absent" skip that marks the seed migration applied without
    seeding. The table is treated as absent ONLY when a SUCCESSFUL probe returns
    zero rows (the genuine fresh-DB / dry-run no-op path).
    """
    res = client.query(
        "SELECT count() FROM system.tables "
        "WHERE database = currentDatabase() AND name = {name:String}",
        parameters={"name": table},
    )
    rows = getattr(res, "result_rows", None) or []
    return bool(rows and rows[0] and rows[0][0] > 0)


def upgrade(client):
    """Seed one __legacy__ marker per org that has pre-existing membership rows."""
    log.info("=== Migration 048: seed legacy membership completion marker ===")

    # 046 creates work_unit_membership and 047 creates work_unit_membership_runs,
    # both EARLIER in this same migration pass. If either is genuinely absent
    # there is simply nothing to seed (no pre-existing membership rows to make
    # readable) — a fresh database is a clean no-op, not an error. Skip
    # gracefully rather than fail closed so the migration is safe to run against
    # any database state (and against a dry-run / mocked client).
    for table in ("work_unit_membership", "work_unit_membership_runs"):
        if not _table_exists(client, table):
            log.info(
                "  %s does not exist yet — nothing to seed, skipping (no "
                "pre-existing membership to make readable)",
                table,
            )
            return

    # One marker per org that HAS membership rows, stamped with that org's
    # newest existing computed_at.  ReplacingMergeTree(completed_at) on
    # (org_id, run_id) makes this idempotent.  Orgs with no membership rows
    # produce no marker (genuine no-op, correctly stays "no membership").
    client.command(
        """
        INSERT INTO work_unit_membership_runs (org_id, run_id, completed_at)
        SELECT
            org_id,
            {legacy_run_id:String} AS run_id,
            max(computed_at) AS completed_at
        FROM work_unit_membership
        GROUP BY org_id
        """,
        parameters={"legacy_run_id": _LEGACY_RUN_ID},
    )

    log.info("=== Migration 048: Complete (legacy markers seeded) ===")
