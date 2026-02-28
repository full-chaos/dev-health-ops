"""CLI-integrated Alembic migrations for PostgreSQL.

Wraps Alembic commands so migrations run from the installed package
without requiring ``alembic.ini`` on disk.  This makes ``dev-hops migrate``
work inside Docker containers where only the installed wheel exists.
"""

from __future__ import annotations

import argparse
from pathlib import Path

from alembic import command
from alembic.config import Config

from dev_health_ops.db import get_postgres_uri

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


# ── CLI registration ───────────────────────────────────────────────


def register_commands(subparsers: argparse._SubParsersAction) -> None:
    migrate_parser = subparsers.add_parser(
        "migrate", help="Run PostgreSQL schema migrations (Alembic)."
    )
    migrate_sub = migrate_parser.add_subparsers(dest="migrate_command", required=True)

    # upgrade
    up = migrate_sub.add_parser("upgrade", help="Upgrade to a later revision.")
    up.add_argument(
        "revision", nargs="?", default="head", help="Target revision (default: head)."
    )
    up.set_defaults(func=_run_upgrade)

    # downgrade
    down = migrate_sub.add_parser("downgrade", help="Revert to a previous revision.")
    down.add_argument(
        "revision", help="Target revision (e.g. -1, base, or a specific rev)."
    )
    down.set_defaults(func=_run_downgrade)

    # current
    cur = migrate_sub.add_parser("current", help="Show current revision.")
    cur.add_argument("-v", "--verbose", action="store_true")
    cur.set_defaults(func=_run_current)

    # history
    hist = migrate_sub.add_parser("history", help="Show migration history.")
    hist.add_argument("-v", "--verbose", action="store_true")
    hist.set_defaults(func=_run_history)

    # heads
    hd = migrate_sub.add_parser("heads", help="Show available heads.")
    hd.add_argument("-v", "--verbose", action="store_true")
    hd.set_defaults(func=_run_heads)
