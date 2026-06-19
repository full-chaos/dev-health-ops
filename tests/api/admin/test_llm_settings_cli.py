from __future__ import annotations

import argparse
import json
import os
import uuid
from collections.abc import AsyncIterator

import pytest
import pytest_asyncio
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

from dev_health_ops.api.admin import cli as admin_cli
from dev_health_ops.cli import build_parser
from dev_health_ops.core.encryption import decrypt_value
from dev_health_ops.models.git import Base
from dev_health_ops.models.licensing import OrgLicense
from dev_health_ops.models.settings import Setting, SettingCategory
from dev_health_ops.models.users import Organization
from tests._helpers import tables_of

os.environ.setdefault("SETTINGS_ENCRYPTION_KEY", "test-encryption-key")

_TABLES = tables_of(Organization, OrgLicense, Setting)


@pytest_asyncio.fixture
async def settings_session_maker() -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    async with engine.begin() as conn:
        await conn.run_sync(
            lambda sync_conn: Base.metadata.create_all(sync_conn, tables=_TABLES)
        )
    try:
        yield async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    finally:
        await engine.dispose()


@pytest.fixture
def cli_session(monkeypatch, settings_session_maker):
    async def _get_session(_ns: argparse.Namespace) -> AsyncSession:
        return settings_session_maker()

    monkeypatch.setattr(admin_cli, "_get_session", _get_session)


@pytest_asyncio.fixture
async def eligible_org(settings_session_maker) -> str:
    return await _seed_org(settings_session_maker, "team")


async def _seed_org(
    settings_session_maker: async_sessionmaker[AsyncSession], tier: str
) -> str:
    org_id = uuid.uuid4()
    async with settings_session_maker() as session:
        session.add(
            Organization(
                id=org_id,
                slug=f"{tier}-{org_id.hex[:8]}",
                name="CLI Test Org",
                tier=tier,
            )
        )
        session.add(OrgLicense(org_id=org_id, tier=tier))
        await session.commit()
    return str(org_id)


async def _llm_settings_count(
    settings_session_maker: async_sessionmaker[AsyncSession], org_id: str
) -> int:
    async with settings_session_maker() as session:
        result = await session.execute(
            select(Setting).where(
                Setting.org_id == org_id,
                Setting.category == SettingCategory.LLM.value,
            )
        )
        return len(result.scalars().all())


def _ns(**kwargs) -> argparse.Namespace:
    defaults = {
        "org": str(uuid.uuid4()),
        "provider": None,
        "model": None,
        "api_key": None,
        "base_url": None,
    }
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


def test_admin_llm_settings_cli_routes_get_set_delete_commands():
    parser = build_parser()

    get_ns = parser.parse_args(["admin", "llm-settings", "get"])
    assert get_ns.func.__name__ == "llm_settings_get"

    set_ns = parser.parse_args(
        [
            "admin",
            "llm-settings",
            "set",
            "--provider",
            "openai",
            "--model",
            "gpt-test",
        ]
    )
    assert set_ns.func.__name__ == "llm_settings_set"
    assert set_ns.provider == "openai"
    assert set_ns.model == "gpt-test"

    delete_ns = parser.parse_args(["admin", "llm-settings", "delete"])
    assert delete_ns.func.__name__ == "llm_settings_delete"


@pytest.mark.asyncio
async def test_admin_llm_settings_cli_set_get_delete_round_trip(
    cli_session, settings_session_maker, eligible_org, capsys
):
    set_result = await admin_cli._llm_settings_set_async(
        _ns(
            org=eligible_org,
            provider="OpenAI",
            model="gpt-test",
            api_key="sk-secret-value",
            base_url="https://api.example.test/v1",
        )
    )
    assert set_result == 0
    set_body = json.loads(capsys.readouterr().out)
    assert set_body == {
        "provider": "openai",
        "model": "gpt-test",
        "api_key": "sk-s…alue",
        "base_url": "https://api.example.test/v1",
    }

    get_result = await admin_cli._llm_settings_get_async(_ns(org=eligible_org))
    assert get_result == 0
    get_body = json.loads(capsys.readouterr().out)
    assert get_body == set_body

    async with settings_session_maker() as session:
        result = await session.execute(
            select(Setting).where(
                Setting.org_id == eligible_org,
                Setting.category == SettingCategory.LLM.value,
                Setting.key == "api_key",
            )
        )
        api_key = result.scalar_one()
        assert api_key.is_encrypted is True
        assert api_key.value != "sk-secret-value"
        assert decrypt_value(api_key.value or "") == "sk-secret-value"

    delete_result = await admin_cli._llm_settings_delete_async(_ns(org=eligible_org))
    assert delete_result == 0
    assert json.loads(capsys.readouterr().out) == {"deleted": True}

    get_after_delete_result = await admin_cli._llm_settings_get_async(
        _ns(org=eligible_org)
    )
    assert get_after_delete_result == 0
    assert json.loads(capsys.readouterr().out) == {
        "provider": None,
        "model": None,
        "api_key": None,
        "base_url": None,
    }


@pytest.mark.asyncio
async def test_admin_llm_settings_cli_invalid_input_returns_clear_error(
    cli_session, capsys
):
    result = await admin_cli._llm_settings_set_async(
        _ns(provider="  ", model="gpt-test")
    )

    assert result == 1
    out = capsys.readouterr().out
    assert "Error: invalid LLM settings input" in out


@pytest.mark.asyncio
async def test_admin_llm_settings_cli_rejects_community_org_without_writing(
    cli_session, settings_session_maker, capsys
):
    org_id = await _seed_org(settings_session_maker, "community")

    result = await admin_cli._llm_settings_set_async(
        _ns(org=org_id, provider="openai", model="gpt-test", api_key="sk-secret")
    )

    assert result == 1
    out = capsys.readouterr().out
    assert "BYO LLM settings require team tier" in out
    assert await _llm_settings_count(settings_session_maker, org_id) == 0


@pytest.mark.asyncio
async def test_admin_llm_settings_cli_rejects_nonexistent_org_without_writing(
    cli_session, settings_session_maker, capsys
):
    missing_org_id = str(uuid.uuid4())

    result = await admin_cli._llm_settings_set_async(
        _ns(
            org=missing_org_id,
            provider="openai",
            model="gpt-test",
            api_key="sk-secret",
        )
    )

    assert result == 1
    out = capsys.readouterr().out
    assert "Organization not found" in out
    assert await _llm_settings_count(settings_session_maker, missing_org_id) == 0


@pytest.mark.asyncio
async def test_admin_llm_settings_cli_rejects_invalid_org_without_writing(
    cli_session, settings_session_maker, capsys
):
    invalid_org_id = "not-a-uuid"

    result = await admin_cli._llm_settings_set_async(
        _ns(org=invalid_org_id, provider="openai", model="gpt-test")
    )

    assert result == 1
    out = capsys.readouterr().out
    assert "Organization not found" in out
    assert await _llm_settings_count(settings_session_maker, invalid_org_id) == 0


@pytest.mark.asyncio
async def test_admin_llm_settings_cli_allows_eligible_org(
    cli_session, settings_session_maker, capsys
):
    org_id = await _seed_org(settings_session_maker, "team")

    result = await admin_cli._llm_settings_set_async(
        _ns(org=org_id, provider="openai", model="gpt-test")
    )

    assert result == 0
    assert json.loads(capsys.readouterr().out)["provider"] == "openai"
    assert await _llm_settings_count(settings_session_maker, org_id) == 3


@pytest.mark.asyncio
async def test_admin_llm_settings_cli_write_error_redacts_secret(
    cli_session, monkeypatch, eligible_org, capsys
):
    from dev_health_ops.api.admin import llm_settings

    async def _raise_secret_error(*_args, **_kwargs):
        raise RuntimeError("database failed while binding sk-secret-value")

    monkeypatch.setattr(llm_settings, "upsert_llm_settings", _raise_secret_error)

    result = await admin_cli._llm_settings_set_async(
        _ns(
            org=eligible_org,
            provider="openai",
            model="gpt-test",
            api_key="sk-secret-value",
        )
    )

    assert result == 1
    out = capsys.readouterr().out
    assert "Error: failed to update LLM settings" in out
    assert "sk-secret-value" not in out
