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

from alembic import command
from alembic.config import Config

from dev_health_ops.db import get_postgres_uri, resolve_sink_uri

logger = logging.getLogger(__name__)

# Resolve the alembic directory from the *installed* package tree,
# not from a hard-coded source path.
_ALEMBIC_DIR = Path(__file__).resolve().parent / "alembic"


def _make_alembic_config(db_url: str | None = None) -> Config:
    """Build an Alembic ``Config`` programmatically."""
    cfg = Config()
    cfg.set_main_option("script_location", str(_ALEMBIC_DIR))

    url = db_url or get_postgres_uri()
    if url:
        cfg.set_main_option("sqlalchemy.url", url)

    return cfg


# ── individual commands ────────────────────────────────────────────


def _run_upgrade(ns: argparse.Namespace) -> int:
    cfg = _make_alembic_config(getattr(ns, "db", None))
    command.upgrade(cfg, ns.revision)
    return 0


def _run_downgrade(ns: argparse.Namespace) -> int:
    cfg = _make_alembic_config(getattr(ns, "db", None))
    command.downgrade(cfg, ns.revision)
    return 0


def _run_current(ns: argparse.Namespace) -> int:
    cfg = _make_alembic_config(getattr(ns, "db", None))
    command.current(cfg, verbose=ns.verbose)
    return 0


def _run_history(ns: argparse.Namespace) -> int:
    cfg = _make_alembic_config(getattr(ns, "db", None))
    command.history(cfg, verbose=ns.verbose)
    return 0


def _run_heads(ns: argparse.Namespace) -> int:
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
