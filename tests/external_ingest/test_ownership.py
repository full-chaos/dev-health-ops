"""Unit tests for CHAOS-2695's one-active-owner resolution (brief §9).

Exercises the CC5 per-provider matching matrix (GitHub/Jira exact, GitLab
metadata path, Linear org-wide placeholder, case-insensitive provider) and
``resolve_effective_mode``'s precedence rules, against a real aiosqlite DB.
"""

from __future__ import annotations

import uuid
from pathlib import Path

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from dev_health_ops.external_ingest.ownership import (
    find_active_managed_owner,
    resolve_effective_mode,
)
from dev_health_ops.models.git import Base
from dev_health_ops.models.ingest_auth import IngestSource, IngestSourceMode
from dev_health_ops.models.integrations import Integration, IntegrationSource
from tests._helpers import tables_of

_TABLES = tables_of(IngestSource, Integration, IntegrationSource)

ORG = "org-1"


@pytest_asyncio.fixture
async def session_maker(tmp_path: Path):
    engine = create_async_engine(
        f"sqlite+aiosqlite:///{tmp_path / 'external-ingest-ownership.db'}"
    )
    async with engine.begin() as conn:
        await conn.run_sync(
            lambda sync_conn: Base.metadata.create_all(sync_conn, tables=_TABLES)
        )
    maker = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        yield maker
    finally:
        await engine.dispose()


async def _seed_managed(
    session_maker,
    *,
    provider: str,
    external_id: str,
    full_name: str,
    name: str | None = None,
    metadata_: dict | None = None,
    config: dict | None = None,
    source_enabled: bool = True,
    integration_active: bool = True,
    org_id: str = ORG,
) -> None:
    integration_id = uuid.uuid4()
    async with session_maker() as session:
        session.add(
            Integration(
                id=integration_id,
                org_id=org_id,
                provider=provider,
                name=f"{provider}-integration",
                config=config or {},
                is_active=integration_active,
            )
        )
        session.add(
            IntegrationSource(
                org_id=org_id,
                integration_id=integration_id,
                provider=provider,
                source_type="repository",
                external_id=external_id,
                name=name or full_name,
                full_name=full_name,
                metadata_=metadata_ or {},
                is_enabled=source_enabled,
            )
        )
        await session.commit()


async def _seed_explicit(
    session_maker,
    *,
    system: str = "github",
    instance: str = "acme/api",
    mode: str = IngestSourceMode.CUSTOMER_PUSH.value,
    enabled: bool = True,
) -> None:
    async with session_maker() as session:
        session.add(
            IngestSource(
                org_id=ORG, system=system, instance=instance, mode=mode, enabled=enabled
            )
        )
        await session.commit()


async def _mode(
    session_maker,
    *,
    system: str = "github",
    instance: str = "acme/api",
    entity_family: str = "legacy",
):
    async with session_maker() as session:
        return await resolve_effective_mode(
            session,
            org_id=ORG,
            system=system,
            instance=instance,
            entity_family=entity_family,
        )


# ---------------------------------------------------------------------------
# Explicit-row precedence
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_unclaimed_when_no_rows_anywhere(session_maker):
    assert await _mode(session_maker) == "unclaimed"


@pytest.mark.asyncio
async def test_customer_push_when_explicit_row_enabled(session_maker):
    await _seed_explicit(session_maker)
    assert await _mode(session_maker) == "customer_push"


@pytest.mark.asyncio
async def test_disabled_when_explicit_row_disabled(session_maker):
    await _seed_explicit(session_maker, enabled=False)
    assert await _mode(session_maker) == "disabled"


@pytest.mark.asyncio
async def test_disabled_when_explicit_row_mode_disabled(session_maker):
    await _seed_explicit(session_maker, mode=IngestSourceMode.DISABLED.value)
    assert await _mode(session_maker) == "disabled"


@pytest.mark.asyncio
async def test_fullchaos_when_explicit_row_mode_fullchaos(session_maker):
    await _seed_explicit(session_maker, mode=IngestSourceMode.FULLCHAOS_SYNC.value)
    assert await _mode(session_maker) == "fullchaos_sync"


@pytest.mark.asyncio
async def test_explicit_customer_push_overridden_by_active_managed_owner(
    session_maker,
):
    """Defense in depth (brief decision 12): managed sync connected AFTER the
    customer_push registration flips the effective mode back to
    fullchaos_sync at accept time."""
    await _seed_explicit(session_maker)
    await _seed_managed(
        session_maker, provider="github", external_id="acme/api", full_name="acme/api"
    )
    assert await _mode(session_maker) == "fullchaos_sync"


@pytest.mark.asyncio
async def test_explicit_customer_push_survives_inactive_managed_match(session_maker):
    await _seed_explicit(session_maker)
    await _seed_managed(
        session_maker,
        provider="github",
        external_id="acme/api",
        full_name="acme/api",
        integration_active=False,
    )
    assert await _mode(session_maker) == "customer_push"


# ---------------------------------------------------------------------------
# Derived fullchaos_sync (CC5 per-provider matching)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fullchaos_sync_derived_from_github_external_id(session_maker):
    await _seed_managed(
        session_maker, provider="github", external_id="acme/api", full_name="acme/api"
    )
    assert await _mode(session_maker) == "fullchaos_sync"


@pytest.mark.asyncio
async def test_fullchaos_sync_derived_case_insensitive_provider(session_maker):
    """Managed rows may carry mixed-case providers ('GitHub'); the lookup is
    lowercased on both sides or the one-active-owner check silently never
    fires (2696 adversarial-review finding, kept by the shared predicates)."""
    await _seed_managed(
        session_maker, provider="GitHub", external_id="acme/api", full_name="acme/api"
    )
    assert await _mode(session_maker) == "fullchaos_sync"


@pytest.mark.asyncio
async def test_instance_matching_is_case_insensitive_both_directions(session_maker):
    """Adversarial-review finding: provider identifiers are case-insensitive
    (GitHub full names, GitLab paths, Jira keys) but sync stores them as the
    provider API returned them -- exact comparison would let `Acme/API`
    managed rows fail to block an `acme/api` customer push."""
    await _seed_managed(
        session_maker, provider="github", external_id="Acme/API", full_name="Acme/API"
    )
    assert await _mode(session_maker, instance="acme/api") == "fullchaos_sync"
    assert await _mode(session_maker, instance="ACME/api") == "fullchaos_sync"


@pytest.mark.asyncio
async def test_gitlab_metadata_path_case_insensitive(session_maker):
    await _seed_managed(
        session_maker,
        provider="gitlab",
        external_id="12345",
        full_name="Group/Project",
        metadata_={"path_with_namespace": "Group/Project"},
    )
    assert (
        await _mode(session_maker, system="gitlab", instance="group/project")
        == "fullchaos_sync"
    )


@pytest.mark.asyncio
async def test_operational_family_is_owned_by_managed_github_host(session_maker):
    await _seed_managed(
        session_maker, provider="github", external_id="acme/api", full_name="acme/api"
    )

    assert (
        await _mode(
            session_maker,
            instance="https://github.com/api/v3",
            entity_family="operational",
        )
        == "fullchaos_sync"
    )


@pytest.mark.asyncio
async def test_operational_family_keeps_unrelated_host_and_legacy_path_independent(
    session_maker,
):
    await _seed_managed(
        session_maker,
        provider="gitlab",
        external_id="12345",
        full_name="group/project",
        config={"gitlab_url": "https://gitlab.acme.test:8443/api/v4"},
    )

    assert (
        await _mode(
            session_maker,
            system="gitlab",
            instance="gitlab.other.test:8443",
            entity_family="operational",
        )
        == "unclaimed"
    )
    assert await _mode(session_maker, system="gitlab", instance="group/project") == (
        "fullchaos_sync"
    )


@pytest.mark.asyncio
async def test_jira_key_case_insensitive(session_maker):
    await _seed_managed(
        session_maker, provider="jira", external_id="PROJ", full_name="PROJ"
    )
    assert await _mode(session_maker, system="jira", instance="proj") == (
        "fullchaos_sync"
    )


@pytest.mark.asyncio
async def test_gitlab_matches_metadata_path_not_numeric_external_id(session_maker):
    """GitLab stores the NUMERIC project id in external_id (CC5) -- the
    human-readable instance string must match via full_name/metadata path."""
    await _seed_managed(
        session_maker,
        provider="gitlab",
        external_id="12345",
        full_name="group/project",
        metadata_={"path_with_namespace": "group/project"},
    )
    assert (
        await _mode(session_maker, system="gitlab", instance="group/project")
        == "fullchaos_sync"
    )
    assert (
        await _mode(session_maker, system="gitlab", instance="12345")
        == "fullchaos_sync"
    )
    assert (
        await _mode(session_maker, system="gitlab", instance="group/other")
        == "unclaimed"
    )


@pytest.mark.asyncio
async def test_jira_matches_external_id(session_maker):
    await _seed_managed(
        session_maker, provider="jira", external_id="PROJ", full_name="PROJ"
    )
    assert await _mode(session_maker, system="jira", instance="PROJ") == (
        "fullchaos_sync"
    )


@pytest.mark.asyncio
async def test_linear_org_wide_placeholder_owns_all_teams(session_maker):
    await _seed_managed(
        session_maker, provider="linear", external_id="linear", full_name="Linear"
    )
    assert (
        await _mode(session_maker, system="linear", instance="ANY-TEAM")
        == "fullchaos_sync"
    )


@pytest.mark.asyncio
async def test_linear_placeholder_via_metadata_flag(session_maker):
    await _seed_managed(
        session_maker,
        provider="linear",
        external_id=str(uuid.uuid4()),
        full_name="Workspace",
        metadata_={"org_wide_placeholder": True},
    )
    assert (
        await _mode(session_maker, system="linear", instance="CHAOS")
        == "fullchaos_sync"
    )


@pytest.mark.asyncio
async def test_linear_team_matches_by_name(session_maker):
    await _seed_managed(
        session_maker,
        provider="linear",
        external_id=str(uuid.uuid4()),
        full_name="team-chaos",
        name="CHAOS",
    )
    assert (
        await _mode(session_maker, system="linear", instance="CHAOS")
        == "fullchaos_sync"
    )
    assert await _mode(session_maker, system="linear", instance="OTHER") == "unclaimed"


@pytest.mark.asyncio
async def test_not_derived_when_integration_inactive(session_maker):
    await _seed_managed(
        session_maker,
        provider="github",
        external_id="acme/api",
        full_name="acme/api",
        integration_active=False,
    )
    assert await _mode(session_maker) == "unclaimed"


@pytest.mark.asyncio
async def test_not_derived_when_source_disabled(session_maker):
    await _seed_managed(
        session_maker,
        provider="github",
        external_id="acme/api",
        full_name="acme/api",
        source_enabled=False,
    )
    assert await _mode(session_maker) == "unclaimed"


@pytest.mark.asyncio
async def test_other_org_rows_never_match(session_maker):
    await _seed_managed(
        session_maker,
        provider="github",
        external_id="acme/api",
        full_name="acme/api",
        org_id="other-org",
    )
    assert await _mode(session_maker) == "unclaimed"


@pytest.mark.asyncio
async def test_custom_never_conflicts(session_maker):
    await _seed_managed(
        session_maker, provider="custom", external_id="my-tool", full_name="my-tool"
    )
    await _seed_explicit(session_maker, system="custom", instance="my-tool")
    assert (
        await _mode(session_maker, system="custom", instance="my-tool")
        == "customer_push"
    )


@pytest.mark.asyncio
async def test_find_active_managed_owner_returns_matching_row(session_maker):
    await _seed_managed(
        session_maker, provider="github", external_id="acme/api", full_name="acme/api"
    )
    async with session_maker() as session:
        owner = await find_active_managed_owner(
            session, org_id=ORG, system="github", instance="acme/api"
        )
        assert owner is not None
        assert owner.external_id == "acme/api"
        assert (
            await find_active_managed_owner(
                session, org_id=ORG, system="github", instance="acme/other"
            )
            is None
        )
