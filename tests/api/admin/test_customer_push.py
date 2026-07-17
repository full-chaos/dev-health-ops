"""Tests for the customer-push admin API (CHAOS-2696).

Covers source registration + per-provider ownership matching (CC5), token
issue/rotate/revoke, and audit logging. Follows the direct-app fixture style
established by tests/api/admin/test_integrations.py.
"""

from __future__ import annotations

import importlib
import json
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from dev_health_ops.api.external_ingest.schemas import SCHEMA_VERSION
from dev_health_ops.api.services.auth import AuthenticatedUser
from dev_health_ops.core.encryption import encrypt_value
from dev_health_ops.models.audit import AuditLog
from dev_health_ops.models.git import Base
from dev_health_ops.models.ingest_auth import IngestSource, IngestToken
from dev_health_ops.models.integrations import Integration, IntegrationSource
from dev_health_ops.models.licensing import FeatureFlag, OrgFeatureOverride, OrgLicense
from dev_health_ops.models.settings import IntegrationCredential
from dev_health_ops.models.users import Organization, User
from tests._helpers import tables_of

admin_router_module = importlib.import_module("dev_health_ops.api.admin")
auth_router_module = importlib.import_module("dev_health_ops.api.auth.router")

_TABLES = tables_of(
    User,
    Organization,
    Integration,
    IntegrationSource,
    IntegrationCredential,
    IngestSource,
    IngestToken,
    FeatureFlag,
    OrgFeatureOverride,
    OrgLicense,
    AuditLog,
)

_CUSTOMER_PUSH_FEATURE = "customer_push_ingest"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture
async def session_maker(tmp_path: Path):
    db_path = tmp_path / "customer_push.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{db_path}")

    async with engine.begin() as conn:
        await conn.run_sync(
            lambda sync_conn: Base.metadata.create_all(sync_conn, tables=_TABLES)
        )

    maker = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        yield maker
    finally:
        await engine.dispose()


@pytest_asyncio.fixture
async def seeded_state(session_maker):
    org_id = uuid.uuid4()
    user_id = uuid.uuid4()
    org = Organization(id=org_id, slug="test-org", name="Test Org", tier="team")
    user = User(id=user_id, email="admin@example.com", is_active=True)
    feature = FeatureFlag(
        key=_CUSTOMER_PUSH_FEATURE,
        name="Customer Push Ingest",
        category="integrations",
        min_tier="team",
    )

    async with session_maker() as session:
        session.add_all([org, user, feature])
        await session.commit()

    return {"org_id": str(org_id), "user_id": str(user_id)}


@pytest_asyncio.fixture
async def client(session_maker, seeded_state):
    app = FastAPI()
    app.include_router(admin_router_module.router)

    admin_user = AuthenticatedUser(
        user_id=seeded_state["user_id"],
        email="admin@example.com",
        org_id=seeded_state["org_id"],
        role="owner",
        is_superuser=False,
    )

    async def _session_override():
        async with session_maker() as session:
            yield session
            await session.commit()

    app.dependency_overrides[auth_router_module.get_current_user] = lambda: admin_user
    app.dependency_overrides[admin_router_module.get_session] = _session_override

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac, seeded_state

    app.dependency_overrides.clear()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def _seed_integration_source(
    session_maker,
    org_id: str,
    *,
    provider: str,
    external_id: str,
    full_name: str,
    name: str | None = None,
    metadata_: dict | None = None,
    credential_base_url: str | None = None,
    credential_encrypted_override: str | None = None,
    is_enabled: bool = True,
    integration_is_active: bool = True,
) -> None:
    integration_id = uuid.uuid4()
    async with session_maker() as session:
        credential_id = None
        if credential_base_url is not None or credential_encrypted_override is not None:
            credential = IntegrationCredential(
                org_id=org_id,
                provider=provider,
                name=f"{provider}-credential",
                credentials_encrypted=credential_encrypted_override
                or encrypt_value(
                    json.dumps({"token": "test-token", "base_url": credential_base_url})
                ),
            )
            session.add(credential)
            await session.flush()
            credential_id = credential.id
        session.add(
            Integration(
                id=integration_id,
                org_id=org_id,
                provider=provider,
                credential_id=credential_id,
                name=f"{provider}-integration",
                is_active=integration_is_active,
            )
        )
        session.add(
            IntegrationSource(
                id=uuid.uuid4(),
                org_id=org_id,
                integration_id=integration_id,
                provider=provider,
                source_type="repository",
                external_id=external_id,
                name=name or full_name,
                full_name=full_name,
                metadata_=metadata_ or {},
                is_enabled=is_enabled,
            )
        )
        await session.commit()


async def _seed_active_integration(session_maker, org_id: str, provider: str) -> None:
    async with session_maker() as session:
        session.add(
            Integration(
                id=uuid.uuid4(),
                org_id=org_id,
                provider=provider,
                name=f"{provider}-integration",
                is_active=True,
            )
        )
        await session.commit()


async def _create_source(
    ac, system: str = "github", instance: str = "acme/api", **extra
):
    payload = {"system": system, "instance": instance, **extra}
    return await ac.post("/api/v1/admin/customer-push/sources", json=payload)


async def _set_customer_push_feature_enabled(session_maker, *, enabled: bool) -> None:
    async with session_maker() as session:
        result = await session.execute(
            select(FeatureFlag).where(FeatureFlag.key == _CUSTOMER_PUSH_FEATURE)
        )
        feature = result.scalar_one()
        feature.is_enabled = enabled
        await session.commit()


async def _set_org_tier(session_maker, org_id: str, *, tier: str) -> None:
    async with session_maker() as session:
        org = await session.get(Organization, uuid.UUID(org_id))
        assert org is not None
        org.tier = tier
        await session.commit()


async def _audit_actions(session_maker, org_id: str) -> list[str]:
    async with session_maker() as session:
        result = await session.execute(
            select(AuditLog.action).where(AuditLog.org_id == uuid.UUID(org_id))
        )
        return [row[0] for row in result.all()]


# ---------------------------------------------------------------------------
# Source registration
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_create_source_defaults_to_customer_push_enabled(client):
    ac, _ = client
    resp = await _create_source(ac, system="github", instance="acme/api")
    assert resp.status_code == 201, resp.text
    body = resp.json()
    assert body["mode"] == "customer_push"
    assert body["enabled"] is True
    assert body["webhook_mode"] == "disabled"
    assert body["matched_integration_source_id"] is None
    assert body["warnings"] == []


@pytest.mark.asyncio
async def test_create_source_denies_org_below_customer_push_tier(session_maker, client):
    ac, state = client
    await _set_org_tier(session_maker, state["org_id"], tier="community")

    resp = await _create_source(ac, system="github", instance="acme/api")

    assert resp.status_code == 402
    body = resp.json()["detail"]
    assert body["error"] == "feature_not_licensed"
    assert body["feature"] == _CUSTOMER_PUSH_FEATURE


@pytest.mark.asyncio
async def test_create_source_denies_when_customer_push_flag_disabled(
    session_maker, client
):
    ac, _ = client
    await _set_customer_push_feature_enabled(session_maker, enabled=False)

    resp = await _create_source(ac, system="github", instance="acme/api")

    assert resp.status_code == 403
    body = resp.json()["detail"]
    assert body["error"] == "feature_not_enabled"
    assert body["feature"] == _CUSTOMER_PUSH_FEATURE


@pytest.mark.asyncio
async def test_create_source_invalid_system_400(client):
    ac, _ = client
    resp = await _create_source(ac, system="bitbucket", instance="acme/api")
    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_create_source_invalid_mode_400(client):
    ac, _ = client
    resp = await _create_source(ac, mode="not_a_mode")
    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_create_source_fullchaos_hosted_webhook_mode_rejected_400(client):
    # adr-004 two-layer contract: the schema TYPE accepts fullchaos_hosted
    # (no 422 -- see test_create_source_fullchaos_hosted_webhook_mode_passes_schema_validation
    # below), but the router's business-logic layer 400s it before persisting.
    ac, _ = client
    resp = await _create_source(ac, webhook_mode="fullchaos_hosted")
    assert resp.status_code == 400
    assert "fullchaos_hosted" in resp.json()["detail"]


@pytest.mark.asyncio
async def test_create_source_unknown_webhook_mode_422(client):
    ac, _ = client
    resp = await _create_source(ac, webhook_mode="not_a_real_mode")
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_patch_source_fullchaos_hosted_webhook_mode_rejected_400(client):
    ac, _ = client
    created = await _create_source(ac, system="github", instance="acme/api")
    source_id = created.json()["id"]

    resp = await ac.patch(
        f"/api/v1/admin/customer-push/sources/{source_id}",
        json={"webhook_mode": "fullchaos_hosted"},
    )
    assert resp.status_code == 400
    assert "fullchaos_hosted" in resp.json()["detail"]


@pytest.mark.asyncio
async def test_create_source_duplicate_org_system_instance_409(client):
    ac, _ = client
    first = await _create_source(ac, system="github", instance="acme/api")
    assert first.status_code == 201
    second = await _create_source(ac, system="github", instance="acme/api")
    assert second.status_code == 409


@pytest.mark.asyncio
async def test_list_sources_scoped_to_org(session_maker, client):
    ac, state = client
    await _create_source(ac, system="github", instance="acme/api")

    other_org = uuid.uuid4()
    async with session_maker() as session:
        session.add(
            IngestSource(
                org_id=str(other_org),
                system="github",
                instance="other/repo",
                mode="customer_push",
                enabled=True,
            )
        )
        await session.commit()

    resp = await ac.get("/api/v1/admin/customer-push/sources")
    assert resp.status_code == 200
    instances = [s["instance"] for s in resp.json()]
    assert instances == ["acme/api"]


@pytest.mark.asyncio
async def test_get_source_not_found_404(client):
    ac, _ = client
    resp = await ac.get(f"/api/v1/admin/customer-push/sources/{uuid.uuid4()}")
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# CC5 per-provider ownership matching
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_github_enabled_managed_match_by_external_id_409(session_maker, client):
    ac, state = client
    await _seed_integration_source(
        session_maker,
        state["org_id"],
        provider="github",
        external_id="acme/api",
        full_name="acme/api",
    )
    resp = await _create_source(ac, system="github", instance="acme/api")
    assert resp.status_code == 409
    assert resp.json()["detail"]["code"] == "source_owned_by_fullchaos_sync"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("provider", "instance"),
    (
        ("github", "https://ghe.acme.test:8443/api/v3"),
        ("gitlab", "https://gitlab.acme.test:8443/api/v4"),
    ),
)
async def test_operational_registration_rejects_linked_credential_host(
    session_maker, client, monkeypatch, provider: str, instance: str
):
    # Given: a managed self-hosted provider with its host only on its credential.
    ac, state = client
    monkeypatch.setenv("SETTINGS_ENCRYPTION_KEY", "test-encryption-key")
    await _seed_integration_source(
        session_maker,
        state["org_id"],
        provider=provider,
        external_id="acme/api",
        full_name="acme/api",
        credential_base_url=instance,
    )

    # When: a customer push registers the operational family for that host.
    response = await _create_source(
        ac,
        system=provider,
        instance=instance,
        entity_family="operational",
    )

    # Then: the managed integration remains the only owner.
    assert response.status_code == 409
    assert response.json()["detail"]["code"] == "source_owned_by_fullchaos_sync"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("provider", "instance"),
    (
        ("github", "https://ghe.acme.test:8443/api/v3"),
        ("gitlab", "https://gitlab.acme.test:8443/api/v4"),
    ),
)
async def test_operational_registration_rejects_undecryptable_linked_credential_host(
    session_maker, client, monkeypatch, provider: str, instance: str
):
    # Given: an active managed self-hosted provider whose linked credential is unreadable.
    ac, state = client
    monkeypatch.setenv("SETTINGS_ENCRYPTION_KEY", "test-encryption-key")
    await _seed_integration_source(
        session_maker,
        state["org_id"],
        provider=provider,
        external_id="acme/api",
        full_name="acme/api",
        credential_encrypted_override="undecryptable",
    )

    # When: a customer push registers the operational family for a self-hosted host.
    response = await _create_source(
        ac,
        system=provider,
        instance=instance,
        entity_family="operational",
    )

    # Then: registration fails closed instead of assuming the public default host.
    assert response.status_code != 201
    assert response.status_code == 409
    assert response.json()["detail"]["code"] == "ownership_resolution_unavailable"
    assert "linked managed credential" in response.json()["detail"]["message"]
    assert "undecryptable" not in response.json()["detail"]["message"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("provider", "instance"),
    (
        ("github", "https://ghe.acme.test:8443/api/v3"),
        ("gitlab", "https://gitlab.acme.test:8443/api/v4"),
    ),
)
async def test_operational_registration_rejects_linked_credential_without_base_url(
    session_maker, client, monkeypatch, provider: str, instance: str
):
    # Given: an active managed self-hosted provider with a readable credential lacking a host.
    ac, state = client
    monkeypatch.setenv("SETTINGS_ENCRYPTION_KEY", "test-encryption-key")
    await _seed_integration_source(
        session_maker,
        state["org_id"],
        provider=provider,
        external_id="acme/api",
        full_name="acme/api",
        credential_encrypted_override=encrypt_value(
            json.dumps({"token": "test-token"})
        ),
    )

    # When: a customer push registers the operational family for a self-hosted host.
    response = await _create_source(
        ac,
        system=provider,
        instance=instance,
        entity_family="operational",
    )

    # Then: the incomplete credential is also conservative for self-hosted ownership.
    assert response.status_code != 201
    assert response.status_code == 409
    assert response.json()["detail"]["code"] == "ownership_resolution_unavailable"


@pytest.mark.asyncio
async def test_operational_registration_rejects_non_mapping_linked_credential(
    session_maker, client, monkeypatch
):
    # Given: an active managed GitHub integration with a decryptable but invalid credential shape.
    ac, state = client
    monkeypatch.setenv("SETTINGS_ENCRYPTION_KEY", "test-encryption-key")
    await _seed_integration_source(
        session_maker,
        state["org_id"],
        provider="github",
        external_id="acme/api",
        full_name="acme/api",
        credential_encrypted_override=encrypt_value(json.dumps("not-a-mapping")),
    )

    # When: a customer push registers a self-hosted operational source.
    response = await _create_source(
        ac,
        system="github",
        instance="https://ghe.acme.test:8443/api/v3",
        entity_family="operational",
    )

    # Then: malformed credential content returns the controlled ownership error.
    assert response.status_code == 409
    assert response.json()["detail"]["code"] == "ownership_resolution_unavailable"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("provider", "instance"),
    (
        ("github", "https://github.com"),
        ("gitlab", "https://gitlab.com"),
    ),
)
async def test_operational_registration_rejects_public_default_host(
    session_maker, client, provider: str, instance: str
):
    ac, state = client
    await _seed_integration_source(
        session_maker,
        state["org_id"],
        provider=provider,
        external_id="acme/api",
        full_name="acme/api",
    )

    response = await _create_source(
        ac,
        system=provider,
        instance=instance,
        entity_family="operational",
    )

    assert response.status_code == 409


@pytest.mark.asyncio
async def test_operational_registration_allows_unrelated_credential_host(
    session_maker, client, monkeypatch
):
    ac, state = client
    monkeypatch.setenv("SETTINGS_ENCRYPTION_KEY", "test-encryption-key")
    await _seed_integration_source(
        session_maker,
        state["org_id"],
        provider="github",
        external_id="acme/api",
        full_name="acme/api",
        credential_base_url="https://ghe.acme.test:8443/api/v3",
    )

    response = await _create_source(
        ac,
        system="github",
        instance="https://ghe.other.test:8443/api/v3",
        entity_family="operational",
    )

    assert response.status_code == 201


@pytest.mark.asyncio
async def test_github_disabled_managed_match_allows_and_stores_id(
    session_maker, client
):
    ac, state = client
    await _seed_integration_source(
        session_maker,
        state["org_id"],
        provider="github",
        external_id="acme/api",
        full_name="acme/api",
        is_enabled=False,
    )
    resp = await _create_source(ac, system="github", instance="acme/api")
    assert resp.status_code == 201, resp.text
    assert resp.json()["matched_integration_source_id"] is not None


@pytest.mark.asyncio
async def test_gitlab_matches_numeric_external_id(session_maker, client):
    ac, state = client
    await _seed_integration_source(
        session_maker,
        state["org_id"],
        provider="gitlab",
        external_id="12345",
        full_name="group/sub/project",
        metadata_={"path_with_namespace": "group/sub/project"},
    )
    resp = await _create_source(ac, system="gitlab", instance="12345")
    assert resp.status_code == 409


@pytest.mark.asyncio
async def test_gitlab_matches_path_with_namespace_metadata(session_maker, client):
    ac, state = client
    await _seed_integration_source(
        session_maker,
        state["org_id"],
        provider="gitlab",
        external_id="99999",
        full_name="group/sub/project",
        metadata_={"path_with_namespace": "group/sub/project"},
    )
    resp = await _create_source(ac, system="gitlab", instance="group/sub/project")
    assert resp.status_code == 409


@pytest.mark.asyncio
async def test_gitlab_no_match_by_unrelated_instance_succeeds(session_maker, client):
    ac, state = client
    await _seed_integration_source(
        session_maker,
        state["org_id"],
        provider="gitlab",
        external_id="99999",
        full_name="group/sub/project",
        metadata_={"path_with_namespace": "group/sub/project"},
    )
    resp = await _create_source(ac, system="gitlab", instance="group/other-project")
    assert resp.status_code == 201


@pytest.mark.asyncio
async def test_jira_matches_external_id_or_full_name(session_maker, client):
    ac, state = client
    await _seed_integration_source(
        session_maker,
        state["org_id"],
        provider="jira",
        external_id="ABC",
        full_name="ABC",
    )
    resp = await _create_source(ac, system="jira", instance="ABC")
    assert resp.status_code == 409


@pytest.mark.asyncio
async def test_linear_matches_exact_instance(session_maker, client):
    ac, state = client
    await _seed_integration_source(
        session_maker,
        state["org_id"],
        provider="linear",
        external_id="team-uuid-123",
        full_name="team-uuid-123",
        name="CHAOS",
    )
    resp = await _create_source(ac, system="linear", instance="CHAOS")
    assert resp.status_code == 409


@pytest.mark.asyncio
async def test_linear_org_wide_placeholder_owns_all_instances(session_maker, client):
    ac, state = client
    await _seed_integration_source(
        session_maker,
        state["org_id"],
        provider="linear",
        external_id="linear",
        full_name="linear",
        name="linear",
        metadata_={"org_wide_placeholder": True},
    )
    resp = await _create_source(ac, system="linear", instance="ANY-TEAM")
    assert resp.status_code == 409


@pytest.mark.asyncio
async def test_linear_org_wide_placeholder_via_external_id_literal(
    session_maker, client
):
    ac, state = client
    await _seed_integration_source(
        session_maker,
        state["org_id"],
        provider="linear",
        external_id="linear",
        full_name="Linear Org Sync",
        name="Linear Org Sync",
    )
    resp = await _create_source(ac, system="linear", instance="SOME-OTHER-TEAM")
    assert resp.status_code == 409


@pytest.mark.asyncio
async def test_custom_system_never_conflicts(session_maker, client):
    ac, state = client
    # Seed a managed source that would match if this were treated as "github".
    await _seed_integration_source(
        session_maker,
        state["org_id"],
        provider="github",
        external_id="acme/api",
        full_name="acme/api",
    )
    resp = await _create_source(ac, system="custom", instance="acme/api")
    assert resp.status_code == 201
    assert resp.json()["matched_integration_source_id"] is None


@pytest.mark.asyncio
async def test_mixed_case_managed_provider_still_hits_409(session_maker, client):
    """Regression: nearby sync-creation paths don't enforce lowercase
    Integration/IntegrationSource.provider values -- a mixed-case managed row
    must still block a lowercase customer-push registration."""
    ac, state = client
    await _seed_integration_source(
        session_maker,
        state["org_id"],
        provider="GitHub",
        external_id="acme/api",
        full_name="acme/api",
    )
    resp = await _create_source(ac, system="github", instance="acme/api")
    assert resp.status_code == 409


@pytest.mark.asyncio
async def test_mixed_case_managed_provider_warns_not_409_when_different_instance(
    session_maker, client
):
    ac, state = client
    await _seed_active_integration(session_maker, state["org_id"], "GitHub")
    resp = await _create_source(ac, system="github", instance="acme/repo-b")
    assert resp.status_code == 201, resp.text
    assert resp.json()["warnings"]


@pytest.mark.asyncio
async def test_active_integration_different_instance_warns_not_409(
    session_maker, client
):
    ac, state = client
    await _seed_active_integration(session_maker, state["org_id"], "github")

    resp = await _create_source(ac, system="github", instance="acme/repo-b")
    assert resp.status_code == 201, resp.text
    body = resp.json()
    assert body["warnings"]
    assert "github" in body["warnings"][0]


@pytest.mark.asyncio
async def test_registering_disabled_mode_skips_ownership_check(session_maker, client):
    ac, state = client
    await _seed_integration_source(
        session_maker,
        state["org_id"],
        provider="github",
        external_id="acme/api",
        full_name="acme/api",
    )
    resp = await _create_source(
        ac, system="github", instance="acme/api", mode="disabled"
    )
    assert resp.status_code == 201


@pytest.mark.asyncio
async def test_instance_is_trimmed_before_persisting(client):
    ac, _ = client
    resp = await _create_source(ac, system="github", instance="  acme/api  ")
    assert resp.status_code == 201, resp.text
    assert resp.json()["instance"] == "acme/api"


@pytest.mark.asyncio
async def test_blank_instance_rejected_422(client):
    ac, _ = client
    resp = await _create_source(ac, system="github", instance="   ")
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_whitespace_variant_instance_still_hits_409(session_maker, client):
    """Regression: an un-trimmed instance must not bypass the CC5 match."""
    ac, state = client
    await _seed_integration_source(
        session_maker,
        state["org_id"],
        provider="github",
        external_id="acme/api",
        full_name="acme/api",
    )
    resp = await _create_source(ac, system="github", instance="  acme/api ")
    assert resp.status_code == 409


@pytest.mark.asyncio
async def test_inactive_integration_does_not_block_registration(session_maker, client):
    """A source row left enabled under a deactivated Integration no longer
    counts as active ownership -- registration succeeds (matched id still
    recorded for bookkeeping), it does not 409."""
    ac, state = client
    await _seed_integration_source(
        session_maker,
        state["org_id"],
        provider="github",
        external_id="acme/api",
        full_name="acme/api",
        is_enabled=True,
        integration_is_active=False,
    )
    resp = await _create_source(ac, system="github", instance="acme/api")
    assert resp.status_code == 201, resp.text
    assert resp.json()["matched_integration_source_id"] is not None


# ---------------------------------------------------------------------------
# PATCH re-check on enable
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_patch_enabling_customer_push_reruns_ownership_check(
    session_maker, client
):
    ac, state = client
    await _seed_integration_source(
        session_maker,
        state["org_id"],
        provider="github",
        external_id="acme/api",
        full_name="acme/api",
    )
    created = await _create_source(
        ac, system="github", instance="acme/api", mode="disabled"
    )
    assert created.status_code == 201
    source_id = created.json()["id"]

    resp = await ac.patch(
        f"/api/v1/admin/customer-push/sources/{source_id}",
        json={"mode": "customer_push"},
    )
    assert resp.status_code == 409


@pytest.mark.asyncio
async def test_patch_updates_fields_and_audits(session_maker, client):
    ac, state = client
    created = await _create_source(ac, system="github", instance="acme/api")
    source_id = created.json()["id"]

    resp = await ac.patch(
        f"/api/v1/admin/customer-push/sources/{source_id}",
        json={"display_name": "Acme API", "enabled": False},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["display_name"] == "Acme API"
    assert body["enabled"] is False

    actions = await _audit_actions(session_maker, state["org_id"])
    assert actions.count("ingest_source_registered") == 1
    assert actions.count("ingest_source_mode_changed") == 1


@pytest.mark.asyncio
async def test_patch_no_changes_emits_no_extra_audit_row(session_maker, client):
    ac, state = client
    created = await _create_source(ac, system="github", instance="acme/api")
    source_id = created.json()["id"]

    resp = await ac.patch(
        f"/api/v1/admin/customer-push/sources/{source_id}",
        json={"mode": "customer_push"},
    )
    assert resp.status_code == 200

    actions = await _audit_actions(session_maker, state["org_id"])
    assert actions.count("ingest_source_mode_changed") == 0


# ---------------------------------------------------------------------------
# Tokens
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_create_source_token_returns_plaintext_once(client):
    ac, state = client
    created = await _create_source(ac, system="github", instance="acme/api")
    source_id = created.json()["id"]

    resp = await ac.post(
        f"/api/v1/admin/customer-push/sources/{source_id}/tokens",
        json={"name": "ci-runner", "scopes": ["ingest:write", "ingest:status"]},
    )
    assert resp.status_code == 201, resp.text
    body = resp.json()
    assert body["token"].startswith("fcpush_")
    assert body["token_prefix"] == body["token"][:12]
    assert body["source_id"] == source_id

    list_resp = await ac.get(f"/api/v1/admin/customer-push/sources/{source_id}/tokens")
    assert list_resp.status_code == 200
    listed = list_resp.json()
    assert len(listed) == 1
    assert "token" not in listed[0]
    assert listed[0]["token_prefix"] == body["token_prefix"]

    org_list_resp = await ac.get("/api/v1/admin/customer-push/tokens")
    assert org_list_resp.status_code == 200
    assert all("token" not in t for t in org_list_resp.json())


@pytest.mark.asyncio
async def test_create_source_token_rejects_unknown_scope(client):
    ac, _ = client
    created = await _create_source(ac, system="github", instance="acme/api")
    source_id = created.json()["id"]

    resp = await ac.post(
        f"/api/v1/admin/customer-push/sources/{source_id}/tokens",
        json={"name": "bad", "scopes": ["ingest:github"]},
    )
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_create_org_token_rejects_ingest_write_400(client):
    ac, _ = client
    resp = await ac.post(
        "/api/v1/admin/customer-push/tokens",
        json={"name": "org-wide", "scopes": ["ingest:write"]},
    )
    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_create_org_token_allows_read_scopes(client):
    ac, _ = client
    resp = await ac.post(
        "/api/v1/admin/customer-push/tokens",
        json={"name": "org-wide", "scopes": ["schema:read", "ingest:status"]},
    )
    assert resp.status_code == 201, resp.text
    assert resp.json()["source_id"] is None


@pytest.mark.asyncio
async def test_rotate_token_invalidates_old_and_returns_new(session_maker, client):
    ac, state = client
    created = await _create_source(ac, system="github", instance="acme/api")
    source_id = created.json()["id"]
    token_resp = await ac.post(
        f"/api/v1/admin/customer-push/sources/{source_id}/tokens",
        json={"name": "ci-runner", "scopes": ["ingest:write"]},
    )
    old_token_id = token_resp.json()["id"]
    old_plaintext = token_resp.json()["token"]

    rotate_resp = await ac.post(
        f"/api/v1/admin/customer-push/tokens/{old_token_id}/rotate"
    )
    assert rotate_resp.status_code == 200, rotate_resp.text
    new_body = rotate_resp.json()
    assert new_body["id"] != old_token_id
    assert new_body["token"] != old_plaintext

    async with session_maker() as session:
        old_row = await session.get(IngestToken, uuid.UUID(old_token_id))
        assert old_row.revoked_at is not None
        new_row = await session.get(IngestToken, uuid.UUID(new_body["id"]))
        assert new_row.revoked_at is None
        assert new_row.source_id == old_row.source_id


@pytest.mark.asyncio
async def test_rotate_token_recomputes_expiry_from_original_ttl(session_maker, client):
    ac, state = client
    expires_at = datetime.now(timezone.utc) + timedelta(days=30)
    token_resp = await ac.post(
        "/api/v1/admin/customer-push/tokens",
        json={
            "name": "org-wide",
            "scopes": ["schema:read"],
            "expires_at": expires_at.isoformat(),
        },
    )
    assert token_resp.status_code == 201, token_resp.text
    old_token_id = token_resp.json()["id"]

    rotate_resp = await ac.post(
        f"/api/v1/admin/customer-push/tokens/{old_token_id}/rotate"
    )
    assert rotate_resp.status_code == 200, rotate_resp.text
    new_expires_at = datetime.fromisoformat(rotate_resp.json()["expires_at"])

    # Original TTL was ~30 days; the rotated token's expiry should be
    # recomputed from *now* + that TTL, not copied verbatim (Design
    # Decision 16) -- allow generous slack for test wall-clock time.
    delta_days = (new_expires_at - datetime.now(timezone.utc)).total_seconds() / 86400
    assert 29 < delta_days <= 30


@pytest.mark.asyncio
async def test_rotate_token_without_expiry_stays_unexpiring(client):
    ac, _ = client
    token_resp = await ac.post(
        "/api/v1/admin/customer-push/tokens",
        json={"name": "org-wide", "scopes": ["schema:read"]},
    )
    old_token_id = token_resp.json()["id"]

    rotate_resp = await ac.post(
        f"/api/v1/admin/customer-push/tokens/{old_token_id}/rotate"
    )
    assert rotate_resp.status_code == 200, rotate_resp.text
    assert rotate_resp.json()["expires_at"] is None


@pytest.mark.asyncio
async def test_rotate_already_revoked_token_400(client):
    ac, _ = client
    token_resp = await ac.post(
        "/api/v1/admin/customer-push/tokens",
        json={"name": "org-wide", "scopes": ["schema:read"]},
    )
    token_id = token_resp.json()["id"]
    revoke_resp = await ac.post(f"/api/v1/admin/customer-push/tokens/{token_id}/revoke")
    assert revoke_resp.status_code == 200

    rotate_resp = await ac.post(f"/api/v1/admin/customer-push/tokens/{token_id}/rotate")
    assert rotate_resp.status_code == 400


@pytest.mark.asyncio
async def test_revoke_token_is_idempotent_and_audited_once(session_maker, client):
    ac, state = client
    token_resp = await ac.post(
        "/api/v1/admin/customer-push/tokens",
        json={"name": "org-wide", "scopes": ["schema:read"]},
    )
    token_id = token_resp.json()["id"]

    first = await ac.post(f"/api/v1/admin/customer-push/tokens/{token_id}/revoke")
    assert first.status_code == 200
    assert first.json()["revoked_at"] is not None

    second = await ac.post(f"/api/v1/admin/customer-push/tokens/{token_id}/revoke")
    assert second.status_code == 200

    actions = await _audit_actions(session_maker, state["org_id"])
    assert actions.count("ingest_token_revoked") == 1


@pytest.mark.asyncio
async def test_full_lifecycle_produces_expected_audit_rows(session_maker, client):
    ac, state = client
    created = await _create_source(ac, system="github", instance="acme/api")
    source_id = created.json()["id"]
    token_resp = await ac.post(
        f"/api/v1/admin/customer-push/sources/{source_id}/tokens",
        json={"name": "ci-runner", "scopes": ["ingest:write"]},
    )
    token_id = token_resp.json()["id"]
    await ac.post(f"/api/v1/admin/customer-push/tokens/{token_id}/rotate")

    actions = await _audit_actions(session_maker, state["org_id"])
    assert actions.count("ingest_source_registered") == 1
    assert actions.count("ingest_token_created") == 1
    assert actions.count("ingest_token_rotated") == 1


@pytest.mark.asyncio
async def test_create_source_case_variant_duplicate_409(client):
    """CHAOS-2695 adversarial-review: provider instance identifiers are
    case-insensitive, so a case-variant re-registration must 409 -- otherwise
    two enabled sources own the same logical repository under split
    one-active-owner / idempotency namespaces."""
    ac, _ = client
    first = await _create_source(ac, system="github", instance="Acme/API")
    assert first.status_code == 201

    duplicate = await _create_source(ac, system="github", instance="acme/api")

    assert duplicate.status_code == 409
    assert "case-insensitive" in duplicate.json()["detail"]

    # A genuinely different instance still registers fine.
    other = await _create_source(ac, system="github", instance="acme/other")
    assert other.status_code == 201


# ---------------------------------------------------------------------------
# Validate proxy (CHAOS-2695) -- session-auth twin of the data-plane
# POST /validate for the web console's Screen 5 (master-spec CC25:
# validate-only; no console-push proxy exists).
# ---------------------------------------------------------------------------

_VALID_COMMIT_PAYLOAD = {
    "repositoryExternalId": "acme/api",
    "hash": "abc1234567",
    "authorWhen": "2026-06-25T00:00:00Z",
}


def _validate_envelope(records: list[dict]) -> dict:
    return {
        "schemaVersion": SCHEMA_VERSION,
        "idempotencyKey": "console-validate-1",
        "source": {
            "type": "customer_push",
            "system": "github",
            "instance": "acme/api",
        },
        "records": records,
    }


async def _registered_source_id(ac) -> str:
    resp = await _create_source(ac, system="github", instance="acme/api")
    assert resp.status_code == 201
    return resp.json()["id"]


@pytest.mark.asyncio
async def test_validate_proxy_valid_payload(client):
    ac, _ = client
    source_id = await _registered_source_id(ac)

    resp = await ac.post(
        f"/api/v1/admin/customer-push/sources/{source_id}/validate",
        json=_validate_envelope(
            [
                {
                    "kind": "commit.v1",
                    "externalId": "abc1234567",
                    "payload": _VALID_COMMIT_PAYLOAD,
                }
            ]
        ),
    )

    assert resp.status_code == 200
    body = resp.json()
    # snake_case admin-plane contract (web CustomerPushValidateResponse)
    assert body == {
        "valid": True,
        "items_accepted": 1,
        "items_rejected": 0,
        "errors": [],
    }


@pytest.mark.asyncio
async def test_validate_proxy_reports_per_record_errors_with_external_id(client):
    ac, _ = client
    source_id = await _registered_source_id(ac)

    bad_commit = {k: v for k, v in _VALID_COMMIT_PAYLOAD.items() if k != "hash"}
    resp = await ac.post(
        f"/api/v1/admin/customer-push/sources/{source_id}/validate",
        json=_validate_envelope(
            [
                {
                    "kind": "commit.v1",
                    "externalId": "good-1",
                    "payload": _VALID_COMMIT_PAYLOAD,
                },
                {"kind": "commit.v1", "externalId": "bad-2", "payload": bad_commit},
            ]
        ),
    )

    assert resp.status_code == 200
    body = resp.json()
    assert body["valid"] is False
    assert body["items_accepted"] == 1
    assert body["items_rejected"] == 1
    (error,) = body["errors"]
    assert error["index"] == 1
    assert error["kind"] == "commit.v1"
    # Enriched from the record wrapper (ValidationErrorItem itself has no
    # external_id) for console-table correlation.
    assert error["external_id"] == "bad-2"
    assert error["code"] == "missing_required_field"
    assert error["path"].startswith("records[1].payload")


@pytest.mark.asyncio
async def test_validate_proxy_unknown_kind_reported_per_record(client):
    ac, _ = client
    source_id = await _registered_source_id(ac)

    resp = await ac.post(
        f"/api/v1/admin/customer-push/sources/{source_id}/validate",
        json=_validate_envelope(
            [{"kind": "deployment.v1", "externalId": "d1", "payload": {}}]
        ),
    )

    assert resp.status_code == 200
    body = resp.json()
    assert body["valid"] is False
    assert body["errors"][0]["code"] == "unknown_kind"


@pytest.mark.asyncio
async def test_validate_proxy_envelope_failure_is_200_result(client):
    """Envelope-level failures render as validation RESULTS (200,
    valid:false), never 4xx -- the console panel renders these rows, and the
    web mock contract pinned this before the endpoint landed."""
    ac, _ = client
    source_id = await _registered_source_id(ac)

    resp = await ac.post(
        f"/api/v1/admin/customer-push/sources/{source_id}/validate",
        json={"schemaVersion": SCHEMA_VERSION, "records": []},
    )

    assert resp.status_code == 200
    body = resp.json()
    assert body["valid"] is False
    assert body["items_accepted"] == 0
    assert body["items_rejected"] == 0
    assert body["errors"]
    assert all(err["code"] == "invalid_envelope" for err in body["errors"])


@pytest.mark.asyncio
async def test_validate_proxy_malformed_json_is_200_result(client):
    ac, _ = client
    source_id = await _registered_source_id(ac)

    resp = await ac.post(
        f"/api/v1/admin/customer-push/sources/{source_id}/validate",
        content=b"{not json",
        headers={"content-type": "application/json"},
    )

    assert resp.status_code == 200
    body = resp.json()
    assert body["valid"] is False
    assert body["errors"][0]["code"] == "invalid_envelope"


@pytest.mark.asyncio
async def test_validate_proxy_wrong_schema_version_is_200_result(client):
    ac, _ = client
    source_id = await _registered_source_id(ac)

    envelope = _validate_envelope(
        [
            {
                "kind": "commit.v1",
                "externalId": "abc1234567",
                "payload": _VALID_COMMIT_PAYLOAD,
            }
        ]
    )
    envelope["schemaVersion"] = "external-ingest.v99"
    resp = await ac.post(
        f"/api/v1/admin/customer-push/sources/{source_id}/validate", json=envelope
    )

    assert resp.status_code == 200
    body = resp.json()
    assert body["valid"] is False
    assert body["errors"][0]["code"] == "unsupported_schema_version"
    assert body["errors"][0]["path"] == "schemaVersion"


@pytest.mark.asyncio
async def test_validate_proxy_unknown_source_404(client):
    ac, _ = client
    resp = await ac.post(
        f"/api/v1/admin/customer-push/sources/{uuid.uuid4()}/validate",
        json=_validate_envelope(
            [
                {
                    "kind": "commit.v1",
                    "externalId": "abc1234567",
                    "payload": _VALID_COMMIT_PAYLOAD,
                }
            ]
        ),
    )
    assert resp.status_code == 404
