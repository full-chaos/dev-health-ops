"""CLI-integrated database migrations for PostgreSQL and ClickHouse.

PostgreSQL migrations are managed via Alembic.  ClickHouse migrations use a
lightweight custom runner that applies numbered ``.sql`` and ``.py`` scripts
from ``migrations/clickhouse/`` and tracks applied versions in a
``schema_migrations`` table inside ClickHouse.

This makes ``dev-hops migrate`` work inside Docker containers where only the
installed wheel exists.
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

from dev_health_ops.db import get_postgres_uri, resolve_sink_uri

logger = logging.getLogger(__name__)

# Resolve the alembic directory from the *installed* package tree,
# not from a hard-coded source path.
_ALEMBIC_DIR = Path(__file__).resolve().parent / "alembic"


def _make_alembic_config(db_url: str | None = None):
    """Build an Alembic ``Config`` programmatically."""
    from alembic.config import Config

    cfg = Config()
    cfg.set_main_option("script_location", str(_ALEMBIC_DIR))

    url = db_url or get_postgres_uri()
    if url:
        cfg.set_main_option("sqlalchemy.url", url)

    return cfg


# ── individual commands ────────────────────────────────────────────


def _run_upgrade(ns: argparse.Namespace) -> int:
    from alembic import command

    cfg = _make_alembic_config(getattr(ns, "db", None))
    command.upgrade(cfg, ns.revision)
    return 0


def _run_downgrade(ns: argparse.Namespace) -> int:
    from alembic import command

    cfg = _make_alembic_config(getattr(ns, "db", None))
    command.downgrade(cfg, ns.revision)
    return 0


def _run_current(ns: argparse.Namespace) -> int:
    from alembic import command

    cfg = _make_alembic_config(getattr(ns, "db", None))
    command.current(cfg, verbose=ns.verbose)
    return 0


def _run_history(ns: argparse.Namespace) -> int:
    from alembic import command

    cfg = _make_alembic_config(getattr(ns, "db", None))
    command.history(cfg, verbose=ns.verbose)
    return 0


def _run_heads(ns: argparse.Namespace) -> int:
    from alembic import command

    cfg = _make_alembic_config(getattr(ns, "db", None))
    command.heads(cfg, verbose=ns.verbose)
    return 0


# ── ClickHouse commands ────────────────────────────────────────────

_CH_MIGRATIONS_DIR = Path(__file__).resolve().parent / "migrations" / "clickhouse"


def _run_clickhouse_upgrade(ns: argparse.Namespace) -> int:
    from dev_health_ops.metrics.sinks.clickhouse import ClickHouseMetricsSink

    uri = resolve_sink_uri(ns)
    sink = ClickHouseMetricsSink(dsn=uri)
    try:
        sink.ensure_schema()
    finally:
        sink.close()
    logger.info("ClickHouse migrations applied successfully.")
    return 0


def _run_clickhouse_status(ns: argparse.Namespace) -> int:
    import clickhouse_connect

    uri = resolve_sink_uri(ns)
    client = clickhouse_connect.get_client(dsn=uri)
    try:
        try:
            result = client.query(
                "SELECT version, applied_at FROM schema_migrations ORDER BY version"
            )
            applied = {row[0]: row[1] for row in (result.result_rows or [])}
        except Exception:
            applied = {}

        available = sorted(
            p.name
            for p in list(_CH_MIGRATIONS_DIR.glob("*.sql"))
            + list(_CH_MIGRATIONS_DIR.glob("*.py"))
        )

        if not available:
            print("No ClickHouse migration files found.")
            return 0

        pending_count = 0
        for name in available:
            if name in applied:
                ts = applied[name]
                print(f"  [applied {ts}]  {name}")
            else:
                pending_count += 1
                print(f"  [pending]            {name}")

        print()
        print(
            f"{len(applied)} applied, {pending_count} pending, {len(available)} total"
        )
    finally:
        client.close()
    return 0


# --- ClickHouse repair (stale duplicate rows in `repos`) ----------------

# Background: ``repos`` is ``ReplacingMergeTree(last_synced)`` ordered by
# ``(org_id, id)``. Earlier ``ClickHouseStore.insert_repo`` behavior
# short-circuited on existing rows, so re-running ``fixtures generate`` (or any
# sync) under a different ``--org`` left the table with multiple rows for the
# same ``id`` under different ``org_id`` values. Older rows are stale duplicates
# that clutter the table.
#
# This repair identifies, per ``id``, the row with the newest ``last_synced``
# as the newest repository row; every other ``(id, org_id)`` row is stale.

_REPAIR_DETECT_QUERY = """
WITH latest AS (
    SELECT
        id,
        argMax(org_id, last_synced) AS active_org_id,
        max(last_synced) AS active_last_synced
    FROM repos
    GROUP BY id
    HAVING uniqExact(org_id) > 1
)
SELECT
    toString(r.id) AS id,
    r.repo AS repo,
    r.org_id AS stale_org_id,
    l.active_org_id AS active_org_id,
    r.last_synced AS stale_last_synced,
    l.active_last_synced AS active_last_synced
FROM repos r
INNER JOIN latest l ON r.id = l.id
WHERE r.org_id != l.active_org_id
{org_filter}
ORDER BY r.repo, r.org_id
"""


def _run_clickhouse_repair(ns: argparse.Namespace) -> int:
    """Find (and optionally delete) stale duplicate rows in ``repos``.

    Dry-run by default. Pass ``--apply`` to issue ``ALTER TABLE repos DELETE``
    for each stale duplicate row. Optional ``--org`` scopes the repair to
    duplicate groups whose newest repository row belongs to that org_id.
    """
    import clickhouse_connect

    uri = resolve_sink_uri(ns)
    apply = bool(getattr(ns, "apply", False))
    org_filter_value: str | None = getattr(ns, "org", None)

    if org_filter_value:
        org_filter_sql = "AND l.active_org_id = {active_org:String}"
        params: dict[str, str] = {"active_org": org_filter_value}
    else:
        org_filter_sql = ""
        params = {}

    detect_query = _REPAIR_DETECT_QUERY.format(org_filter=org_filter_sql)

    client = clickhouse_connect.get_client(dsn=uri)
    try:
        result = client.query(detect_query, parameters=params)
        stale_rows = list(result.result_rows or [])

        if not stale_rows:
            if org_filter_value:
                print(
                    f"No stale duplicate rows found where the newest row "
                    f"belongs to org {org_filter_value}."
                )
            else:
                print("No stale duplicate rows found in repos.")
            return 0

        print(f"Found {len(stale_rows)} stale duplicate row(s) in repos:")
        print()
        header = ("repo", "stale_org_id", "active_org_id", "stale_last_synced")
        print(f"  {header[0]:<40s}  {header[1]:<40s}  {header[2]:<40s}  {header[3]}")
        for row in stale_rows:
            (_id, repo, stale_org, active_org, stale_ts, _active_ts) = row
            print(
                f"  {str(repo):<40s}  {str(stale_org):<40s}  "
                f"{str(active_org):<40s}  {stale_ts}"
            )
        print()

        if not apply:
            print("Dry-run: pass --apply to delete these stale duplicate rows.")
            return 0

        print("Applying ALTER TABLE repos DELETE for each stale duplicate row...")
        deleted = 0
        for row in stale_rows:
            (id_str, _repo, stale_org, _active_org, _stale_ts, _active_ts) = row
            client.command(
                "ALTER TABLE repos DELETE "
                "WHERE id = {id:UUID} AND org_id = {org:String} "
                "SETTINGS mutations_sync=2",
                parameters={"id": str(id_str), "org": str(stale_org)},
            )
            deleted += 1

        print(f"Deleted {deleted} stale duplicate row(s) from repos.")
    finally:
        client.close()
    return 0


# ── CLI registration ───────────────────────────────────────────────


def _register_postgres_subcommands(
    sub: argparse._SubParsersAction,
) -> None:
    up = sub.add_parser("upgrade", help="Upgrade to a later revision.")
    up.add_argument(
        "revision", nargs="?", default="head", help="Target revision (default: head)."
    )
    up.set_defaults(func=_run_upgrade)

    down = sub.add_parser("downgrade", help="Revert to a previous revision.")
    down.add_argument(
        "revision", help="Target revision (e.g. -1, base, or a specific rev)."
    )
    down.set_defaults(func=_run_downgrade)

    cur = sub.add_parser("current", help="Show current revision.")
    cur.add_argument("-v", "--verbose", action="store_true")
    cur.set_defaults(func=_run_current)

    hist = sub.add_parser("history", help="Show migration history.")
    hist.add_argument("-v", "--verbose", action="store_true")
    hist.set_defaults(func=_run_history)

    hd = sub.add_parser("heads", help="Show available heads.")
    hd.add_argument("-v", "--verbose", action="store_true")
    hd.set_defaults(func=_run_heads)


def _register_clickhouse_subcommands(
    sub: argparse._SubParsersAction,
) -> None:
    ch_up = sub.add_parser("upgrade", help="Apply all pending migrations.")
    ch_up.set_defaults(func=_run_clickhouse_upgrade)

    ch_status = sub.add_parser("status", help="Show applied and pending migrations.")
    ch_status.set_defaults(func=_run_clickhouse_status)

    ch_repair = sub.add_parser(
        "repair",
        help=(
            "Remediate stale duplicate rows in repos. Dry-run unless --apply is passed."
        ),
    )
    ch_repair.add_argument(
        "--apply",
        action="store_true",
        help="Apply the repair by deleting the stale duplicate rows shown in the dry-run preview.",
    )
    ch_repair.add_argument(
        "--org",
        default=argparse.SUPPRESS,
        help=(
            "Only include duplicate groups whose newest repository row belongs "
            "to this org_id. Without this flag, all orgs are checked."
        ),
    )
    ch_repair.set_defaults(func=_run_clickhouse_repair)


def register_commands(subparsers: argparse._SubParsersAction) -> None:
    migrate_parser = subparsers.add_parser(
        "migrate", help="Run database schema migrations."
    )
    migrate_sub = migrate_parser.add_subparsers(dest="migrate_command", required=True)

    # -- `migrate postgres [upgrade|downgrade|current|history|heads]` --

    pg_parser = migrate_sub.add_parser(
        "postgres", help="Run PostgreSQL schema migrations (Alembic)."
    )
    pg_sub = pg_parser.add_subparsers(dest="pg_command")
    _register_postgres_subcommands(pg_sub)
    pg_parser.set_defaults(func=_run_upgrade, revision="head")

    # -- `migrate clickhouse [upgrade|status]` --

    ch_parser = migrate_sub.add_parser(
        "clickhouse", help="Run ClickHouse schema migrations."
    )
    ch_sub = ch_parser.add_subparsers(dest="ch_command")
    _register_clickhouse_subcommands(ch_sub)
    ch_parser.set_defaults(func=_run_clickhouse_upgrade)

    # -- backward-compat aliases (flat: `migrate upgrade`, etc.) --

    _register_postgres_subcommands(migrate_sub)
