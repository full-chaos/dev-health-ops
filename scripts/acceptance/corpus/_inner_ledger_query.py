#!/usr/bin/env python3
"""Runs INSIDE the ``ask-dev-acceptance-api`` container, never on the host.

Invoked by ``db_verify.query_resolution_ledger_via_exec`` as
``docker compose ... exec -T api python -m
scripts.acceptance.corpus._inner_ledger_query --run-id <uuid>`` (team-lead
ruling 2026-08-06: the resolution-ledger read is one of exactly two harness
concerns allowed through the container boundary -- see
``compose_context.py``'s module docstring).

Queries ``dev_run_resolutions`` directly via SQLAlchemy against
``DATABASE_URI`` (already present in the container's own environment --
``compose.yml``'s ``api`` service sets it to this project's own pgbouncer,
the SAME value ``dev-hops`` itself uses) and prints exactly ONE JSON line to
stdout: ``{"entries": [{"outcome": ..., "mention_id": ..., "committed_label":
..., "committed_canonical_id": ...}, ...]}``. All diagnostics go to stderr --
stdout is a strict machine contract the host-side ``db_verify`` parses
verbatim.

Deliberately extracts only the fields ``resolution_path.py`` needs
(``ResolutionLedgerEntry``'s shape) rather than dumping the full row/payload
-- this is a narrow, purpose-built verification-plane reader, not a general
database export tool.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import uuid
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from dev_health_ops.models.dev_persistence import DevRunResolution


async def _fetch_entries(run_id: uuid.UUID, database_uri: str) -> list[dict[str, Any]]:
    engine = create_async_engine(database_uri)
    try:
        maker = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
        async with maker() as session:
            rows = (
                await session.scalars(
                    select(DevRunResolution)
                    .where(DevRunResolution.run_id == run_id)
                    .order_by(DevRunResolution.entry_ordinal)
                )
            ).all()
            entries: list[dict[str, Any]] = []
            for row in rows:
                payload = row.payload if isinstance(row.payload, dict) else {}
                committed_raw = payload.get("committed_entity_ref")
                committed: dict[str, Any] = (
                    committed_raw if isinstance(committed_raw, dict) else {}
                )
                entries.append(
                    {
                        "outcome": row.outcome,
                        "mention_id": str(row.mention_id),
                        "committed_label": committed.get("display_label"),
                        "committed_canonical_id": committed.get("entity_id"),
                    }
                )
            return entries
    finally:
        await engine.dispose()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--database-uri", default=os.getenv("DATABASE_URI"))
    args = parser.parse_args(argv)

    if not args.database_uri:
        print(
            "DATABASE_URI is not set in this container's environment and "
            "--database-uri was not provided",
            file=sys.stderr,
        )
        return 1
    try:
        run_id = uuid.UUID(args.run_id)
    except ValueError as exc:
        print(f"--run-id {args.run_id!r} is not a valid UUID: {exc}", file=sys.stderr)
        return 1

    entries = asyncio.run(_fetch_entries(run_id, args.database_uri))
    print(json.dumps({"entries": entries}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
