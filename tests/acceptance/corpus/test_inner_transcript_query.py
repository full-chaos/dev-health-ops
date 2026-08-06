"""Integration coverage for
``scripts.acceptance.corpus._inner_transcript_query``'s query logic against
a real (ephemeral, sqlite-backed) database -- same tradeoff as
``test_inner_ledger_query.py``: aiosqlite here stands in for the container's
real asyncpg/Postgres connection, proving the query/shape logic without
needing a live Postgres. The actual ``docker compose exec`` invocation is
exercised only against a genuinely booted acceptance stack.
"""

from __future__ import annotations

import uuid
from pathlib import Path
from typing import Any

import pytest
import pytest_asyncio
from sqlalchemy import event
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from dev_health_ops.models.dev_persistence import DevConversation, DevMessage
from dev_health_ops.models.git import Base
from dev_health_ops.models.users import Organization, User
from scripts.acceptance.corpus._inner_transcript_query import (
    _fetch_assistant_schema_versions,
)
from tests._helpers import tables_of

_TABLES = tables_of(User, Organization, DevConversation, DevMessage)


@pytest_asyncio.fixture
async def seeded_conversation(tmp_path: Path):
    database = tmp_path / "inner-transcript-query.db"
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
    org_id, user_id, conversation_id = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    async with maker() as session:
        session.add_all(
            [
                Organization(
                    id=org_id, slug="transcript-query", name="Transcript Query"
                ),
                User(id=user_id, email="transcript-query@example.com"),
            ]
        )
        await session.flush()
        session.add(
            DevConversation(
                id=conversation_id, org_id=org_id, user_id=user_id, current_scope={}
            )
        )
        await session.commit()
    try:
        yield maker, database_uri, org_id, user_id, conversation_id
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_no_assistant_rows_returns_empty_list(seeded_conversation) -> None:
    _maker, database_uri, _org_id, _user_id, conversation_id = seeded_conversation
    versions = await _fetch_assistant_schema_versions(conversation_id, database_uri)
    assert versions == []


@pytest.mark.asyncio
async def test_returns_schema_versions_in_creation_order(seeded_conversation) -> None:
    maker, database_uri, org_id, user_id, conversation_id = seeded_conversation
    async with maker() as session:
        session.add_all(
            [
                DevMessage(
                    conversation_id=conversation_id,
                    org_id=org_id,
                    user_id=user_id,
                    role="user",
                    content="what's the status?",
                    client_message_id=uuid.uuid4(),
                ),
                DevMessage(
                    conversation_id=conversation_id,
                    org_id=org_id,
                    user_id=user_id,
                    role="assistant",
                    answer_id=uuid.uuid4(),
                    answer_payload={"schema_version": "dev_answer.v2"},
                ),
            ]
        )
        await session.commit()

    versions = await _fetch_assistant_schema_versions(conversation_id, database_uri)
    assert versions == ["dev_answer.v2"]


@pytest.mark.asyncio
async def test_only_returns_rows_for_the_requested_conversation(
    seeded_conversation,
) -> None:
    maker, database_uri, org_id, user_id, conversation_id = seeded_conversation
    other_conversation_id = uuid.uuid4()
    async with maker() as session:
        session.add(
            DevConversation(
                id=other_conversation_id,
                org_id=org_id,
                user_id=user_id,
                current_scope={},
            )
        )
        await session.flush()
        session.add(
            DevMessage(
                conversation_id=other_conversation_id,
                org_id=org_id,
                user_id=user_id,
                role="assistant",
                answer_id=uuid.uuid4(),
                answer_payload={"schema_version": "dev_error.v1"},
            )
        )
        await session.commit()

    versions = await _fetch_assistant_schema_versions(conversation_id, database_uri)
    assert versions == []
