#!/usr/bin/env python3
"""Runs INSIDE the ``ask-dev-acceptance-api`` container, never on the host.

Invoked by ``db_verify.query_transcript_assistant_schema_versions_via_exec``
as ``docker compose ... exec -T api python -m
scripts.acceptance.corpus._inner_transcript_query --conversation-id <uuid>``
(team-lead ruling 2026-08-06: exactly two -- now three, per the
``terminal_persists_assistant_row`` invariant checker -- harness concerns are
allowed through the container boundary; see ``compose_context.py``'s module
docstring).

Queries ``dev_messages`` directly via SQLAlchemy against ``DATABASE_URI``
for every ``role="assistant"`` row on one conversation, and prints exactly
ONE JSON line to stdout: ``{"assistant_schema_versions": [<schema_version
or null>, ...]}``, in row-creation order. Deliberately extracts only the
``answer_payload.schema_version`` field (never the payload body, never
``content``) -- this is a narrow, purpose-built verification-plane reader
proving CHAOS-3423's own "every terminal persists a real transcript row"
guarantee from the corpus side, not a general transcript export tool.
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

from dev_health_ops.models.dev_persistence import DevMessage


async def _fetch_assistant_schema_versions(
    conversation_id: uuid.UUID, database_uri: str
) -> list[Any]:
    engine = create_async_engine(database_uri)
    try:
        maker = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
        async with maker() as session:
            rows = (
                await session.scalars(
                    select(DevMessage)
                    .where(
                        DevMessage.conversation_id == conversation_id,
                        DevMessage.role == "assistant",
                    )
                    .order_by(DevMessage.created_at, DevMessage.id)
                )
            ).all()
            versions: list[Any] = []
            for row in rows:
                payload = (
                    row.answer_payload if isinstance(row.answer_payload, dict) else {}
                )
                versions.append(payload.get("schema_version"))
            return versions
    finally:
        await engine.dispose()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--conversation-id", required=True)
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
        conversation_id = uuid.UUID(args.conversation_id)
    except ValueError as exc:
        print(
            f"--conversation-id {args.conversation_id!r} is not a valid UUID: {exc}",
            file=sys.stderr,
        )
        return 1

    versions = asyncio.run(
        _fetch_assistant_schema_versions(conversation_id, args.database_uri)
    )
    print(json.dumps({"assistant_schema_versions": versions}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
