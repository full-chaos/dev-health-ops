"""Integration coverage for ``scripts.acceptance.corpus._inner_ledger_query``'s
query logic against a real (ephemeral, sqlite-backed) database.

``_fetch_entries`` is plain, DB-agnostic SQLAlchemy -- exercising it via
aiosqlite here (rather than requiring a live Postgres) mirrors this repo's
own established pattern for testing otherwise-Postgres-oriented production
code (``test_ask_dev_quota_headroom.py``'s "aiosqlite in-memory DB" comment
makes the same tradeoff explicitly). What is NOT covered here: the actual
``docker compose exec`` invocation and the container's real ``DATABASE_URI``
(asyncpg/Postgres) -- those need a genuinely booted acceptance stack, see
``db_verify.py``'s own docstring.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
import pytest_asyncio
from sqlalchemy import event
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from dev_health_ops.models.dev_persistence import (
    DevConversation,
    DevMessage,
    DevRun,
    DevRunResolution,
)
from dev_health_ops.models.git import Base
from dev_health_ops.models.users import Organization, User
from scripts.acceptance.corpus._inner_ledger_query import _fetch_entries
from tests._helpers import tables_of

_TABLES = tables_of(
    User, Organization, DevConversation, DevMessage, DevRun, DevRunResolution
)


@pytest_asyncio.fixture
async def seeded_run(tmp_path: Path):
    database = tmp_path / "inner-ledger-query.db"
    database_uri = f"sqlite+aiosqlite:///{database}"
    engine = create_async_engine(database_uri)

    @event.listens_for(engine.sync_engine, "connect")
    def _enable_foreign_keys(dbapi_connection: Any, _record: Any) -> None:
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()

    async with engine.begin() as connection:
        await connection.run_sync(
            lambda sync: Base.metadata.create_all(sync, tables=_TABLES)
        )
    maker = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    org_id, user_id, conversation_id, run_id = (
        uuid.uuid4(),
        uuid.uuid4(),
        uuid.uuid4(),
        uuid.uuid4(),
    )
    async with maker() as session:
        session.add_all(
            [
                Organization(id=org_id, slug="ledger-query", name="Ledger Query"),
                User(id=user_id, email="ledger-query@example.com"),
            ]
        )
        await session.flush()
        session.add(
            DevConversation(
                id=conversation_id,
                org_id=org_id,
                user_id=user_id,
                current_scope={},
                retention_days=30,
            )
        )
        await session.flush()
        session.add(
            DevRun(
                id=run_id,
                request_id=uuid.uuid4(),
                org_id=org_id,
                user_id=user_id,
                conversation_id=conversation_id,
                state="completed",
            )
        )
        await session.commit()
    try:
        yield maker, database_uri, run_id
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_fetch_entries_returns_ordered_shaped_rows(seeded_run) -> None:
    maker, database_uri, run_id = seeded_run
    async with maker() as session:
        session.add_all(
            [
                DevRunResolution(
                    run_id=run_id,
                    org_id=(await session.get(DevRun, run_id)).org_id,
                    user_id=(await session.get(DevRun, run_id)).user_id,
                    entry_ordinal=1,
                    mention_id=uuid.uuid4(),
                    outcome="ambiguous_candidates",
                    payload={"outcome": "ambiguous_candidates"},
                    resolved_at=datetime.now(UTC),
                ),
                DevRunResolution(
                    run_id=run_id,
                    org_id=(await session.get(DevRun, run_id)).org_id,
                    user_id=(await session.get(DevRun, run_id)).user_id,
                    entry_ordinal=0,
                    mention_id=uuid.uuid4(),
                    outcome="exact_match",
                    payload={
                        "outcome": "exact_match",
                        "committed_entity_ref": {
                            "display_label": "meridian/web-app",
                            "entity_id": "repo-01",
                        },
                    },
                    resolved_at=datetime.now(UTC),
                ),
            ]
        )
        await session.commit()

    entries = await _fetch_entries(run_id, database_uri)
    assert len(entries) == 2
    # Ordered by entry_ordinal (0 before 1), regardless of insertion order.
    assert entries[0]["outcome"] == "exact_match"
    assert entries[0]["committed_label"] == "meridian/web-app"
    assert entries[0]["committed_canonical_id"] == "repo-01"
    assert entries[1]["outcome"] == "ambiguous_candidates"
    assert entries[1]["committed_label"] is None
    assert entries[1]["committed_canonical_id"] is None


@pytest.mark.asyncio
async def test_fetch_entries_empty_ledger_returns_empty_list(seeded_run) -> None:
    _maker, database_uri, run_id = seeded_run
    entries = await _fetch_entries(run_id, database_uri)
    assert entries == []


@pytest.mark.asyncio
async def test_fetch_entries_only_returns_rows_for_the_requested_run(
    seeded_run,
) -> None:
    maker, database_uri, run_id = seeded_run
    other_run_id = uuid.uuid4()
    async with maker() as session:
        run = await session.get(DevRun, run_id)
        session.add(
            DevRun(
                id=other_run_id,
                request_id=uuid.uuid4(),
                org_id=run.org_id,
                user_id=run.user_id,
                conversation_id=run.conversation_id,
                state="completed",
            )
        )
        await session.flush()
        session.add(
            DevRunResolution(
                run_id=other_run_id,
                org_id=run.org_id,
                user_id=run.user_id,
                entry_ordinal=0,
                mention_id=uuid.uuid4(),
                outcome="no_authorized_match",
                payload={"outcome": "no_authorized_match"},
                resolved_at=datetime.now(UTC),
            )
        )
        await session.commit()

    entries = await _fetch_entries(run_id, database_uri)
    assert entries == []
