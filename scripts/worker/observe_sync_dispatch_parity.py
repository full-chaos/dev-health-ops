#!/usr/bin/env python3
"""Read one redacted Python sync-dispatch observation from an imported snapshot.

This helper is intentionally run by ``dev-health-sync-parity`` only. The Go
coordinator keeps the exporting read-only transaction open while this process
imports its PostgreSQL snapshot. The process prints aggregate parity data and
a candidate digest only; it never prints the snapshot token, database URI,
candidate identifiers, tenant data, or errors from a database driver.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from dev_health_ops.sync.dispatch_outbox import observe_due_outbox_rows

_SNAPSHOT_ID_RE = re.compile(r"^[0-9A-Fa-f-]{3,128}$")
_MIN_LIMIT = 1
_MAX_LIMIT = 100


class ParityHelperError(RuntimeError):
    """A safe, non-diagnostic failure reason for the coordinator."""


def _parse_cutoff(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise ParityHelperError("invalid_cutoff") from error
    if parsed.tzinfo is None:
        raise ParityHelperError("invalid_cutoff")
    return parsed.astimezone(timezone.utc)


def _database_uri() -> str:
    value = os.environ.get("SYNC_DISPATCH_PARITY_DATABASE_URI", "")
    if not value:
        raise ParityHelperError("database_unavailable")
    if value.startswith("postgres://"):
        return "postgresql+psycopg2://" + value.removeprefix("postgres://")
    if value.startswith("postgresql://"):
        return "postgresql+psycopg2://" + value.removeprefix("postgresql://")
    if value.startswith("postgresql+psycopg2://"):
        return value
    raise ParityHelperError("database_unavailable")


def _snapshot_id() -> str:
    value = os.environ.get("SYNC_DISPATCH_PARITY_SNAPSHOT_ID", "")
    if not _SNAPSHOT_ID_RE.fullmatch(value):
        raise ParityHelperError("snapshot_unavailable")
    return value


def observe_imported_snapshot(*, cutoff: datetime, limit: int) -> dict[str, Any]:
    """Import the coordinator's snapshot and run the existing Python observer."""
    if not _MIN_LIMIT <= limit <= _MAX_LIMIT:
        raise ParityHelperError("invalid_limit")
    snapshot_id = _snapshot_id()
    engine = create_engine(_database_uri(), pool_pre_ping=False)
    try:
        with engine.connect() as connection:
            # The snapshot must be imported before the first query in this
            # transaction. It is validated above before becoming SQL text.
            connection.exec_driver_sql(
                "BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"
            )
            connection.exec_driver_sql(f"SET TRANSACTION SNAPSHOT '{snapshot_id}'")
            session = Session(bind=connection, autoflush=False)
            try:
                return observe_due_outbox_rows(session, now=cutoff, limit=limit)
            finally:
                session.close()
                connection.exec_driver_sql("ROLLBACK")
    except ParityHelperError:
        raise
    except Exception as error:
        raise ParityHelperError("observation_unavailable") from error
    finally:
        engine.dispose()


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--cutoff", required=True)
    parser.add_argument("--limit", type=int, required=True)
    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    try:
        args = parse_args(argv)
        observation = observe_imported_snapshot(
            cutoff=_parse_cutoff(args.cutoff), limit=args.limit
        )
    except (ParityHelperError, SystemExit):
        # argparse and database errors can contain local paths, DSNs, or SQL.
        # The coordinator needs a stable, safe-to-display failure only.
        print(json.dumps({"status": "error", "reason": "unavailable"}))
        return 1
    print(json.dumps(observation, separators=(",", ":"), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
