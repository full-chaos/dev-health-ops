from __future__ import annotations

from collections.abc import AsyncIterator
from uuid import UUID, uuid4

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from dev_health_ops.credentials.types import CredentialSource, PagerDutyCredentials
from dev_health_ops.models.git import Base
from dev_health_ops.models.integrations import Integration, IntegrationSource
from dev_health_ops.models.pagerduty_webhook_binding import PagerDutyWebhookBinding
from dev_health_ops.models.settings import IntegrationCredential
from dev_health_ops.providers.pagerduty.webhook_worker import (
    _load_active_pagerduty_webhook_binding,
    _pagerduty_webhook_auth,
)
from dev_health_ops.providers.pagerduty.webhook_worker_graph import (
    load_active_pagerduty_webhook_context,
    lock_active_pagerduty_webhook_graph,
)
from dev_health_ops.providers.pagerduty.webhook_worker_shared import (
    PagerDutyWebhookAuth,
    PagerDutyWebhookWorkerContext,
    build_pagerduty_webhook_auth,
)


@pytest.fixture
async def session() -> AsyncIterator[AsyncSession]:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    async with session_factory() as database_session:
        yield database_session
    await engine.dispose()


async def _seed_active_graph(
    session: AsyncSession,
    *,
    source_enabled: bool = True,
    integration_active: bool = True,
    credential_active: bool = True,
    binding_org_id: UUID | None = None,
    binding_uses_integration_credential: bool = True,
    source_external_id: str = "account-1",
    credential_config: dict[str, str] | None = None,
) -> PagerDutyWebhookBinding:
    org_id = uuid4()
    config = credential_config or {"account_id": "account-1", "subdomain": "account-1"}
    credential = IntegrationCredential(
        provider="pagerduty",
        name="pagerduty",
        org_id=str(org_id),
        credentials_encrypted="encrypted",
        config=config,
        is_active=credential_active,
    )
    session.add(credential)
    await session.flush()
    integration = Integration(
        org_id=str(org_id),
        provider="pagerduty",
        credential_id=credential.id,
        name="PagerDuty",
        config={},
        is_active=integration_active,
    )
    session.add(integration)
    await session.flush()
    binding_credential = credential
    if not binding_uses_integration_credential:
        binding_credential = IntegrationCredential(
            provider="pagerduty",
            name="pagerduty-repointed",
            org_id=str(org_id),
            credentials_encrypted="encrypted",
            config=config,
            is_active=True,
        )
        session.add(binding_credential)
        await session.flush()
    source = IntegrationSource(
        org_id=str(org_id),
        integration_id=integration.id,
        provider="pagerduty",
        source_type="account",
        external_id=source_external_id,
        name="PagerDuty account",
        full_name="PagerDuty account",
        metadata_={},
        is_enabled=source_enabled,
    )
    session.add(source)
    await session.flush()
    binding = PagerDutyWebhookBinding(
        org_id=binding_org_id or org_id,
        integration_source_id=source.id,
        credential_id=binding_credential.id,
        provider_subscription_id="subscription-1",
        signing_secret_encrypted="encrypted",
        signing_secret_key_version="v1",
        status="active",
    )
    session.add(binding)
    await session.commit()
    return binding


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("source_enabled", "integration_active", "credential_active"),
    [(False, True, True), (True, False, True), (True, True, False)],
)
async def test_active_binding_resolution_rejects_inactive_trust_graph_member(
    session: AsyncSession,
    source_enabled: bool,
    integration_active: bool,
    credential_active: bool,
) -> None:
    binding = await _seed_active_graph(
        session,
        source_enabled=source_enabled,
        integration_active=integration_active,
        credential_active=credential_active,
    )

    resolved = await _load_active_pagerduty_webhook_binding(session, binding.id)

    assert resolved is None


@pytest.mark.anyio
async def test_active_binding_resolution_rejects_cross_organization_binding(
    session: AsyncSession,
) -> None:
    binding = await _seed_active_graph(session, binding_org_id=uuid4())

    resolved = await _load_active_pagerduty_webhook_binding(session, binding.id)

    assert resolved is None


@pytest.mark.anyio
async def test_active_binding_resolution_rejects_credential_repoint(
    session: AsyncSession,
) -> None:
    binding = await _seed_active_graph(
        session,
        binding_uses_integration_credential=False,
    )

    resolved = await _load_active_pagerduty_webhook_binding(session, binding.id)

    assert resolved is None


@pytest.mark.anyio
async def test_active_binding_resolution_rejects_source_identity_that_is_not_verified_account(
    session: AsyncSession,
) -> None:
    binding = await _seed_active_graph(session, source_external_id="source-uuid")

    resolved = await _load_active_pagerduty_webhook_binding(session, binding.id)

    assert resolved is None


@pytest.mark.anyio
async def test_active_binding_resolution_uses_verified_account_before_subdomain(
    session: AsyncSession,
) -> None:
    binding = await _seed_active_graph(
        session,
        source_external_id="account-verified",
        credential_config={"account_id": "account-verified", "subdomain": "tenant"},
    )

    context = await load_active_pagerduty_webhook_context(session, binding.id)

    assert context is not None
    assert context.provider_instance_id == "account-verified"


@pytest.mark.anyio
async def test_locked_graph_revalidates_credential_repoint_before_persistence(
    session: AsyncSession,
) -> None:
    binding = await _seed_active_graph(session)
    context = await load_active_pagerduty_webhook_context(session, binding.id)
    replacement = IntegrationCredential(
        provider="pagerduty",
        name="pagerduty-replacement",
        org_id=str(binding.org_id),
        credentials_encrypted="encrypted",
        config={"account_id": "account-1", "subdomain": "account-1"},
        is_active=True,
    )
    session.add(replacement)
    await session.flush()
    source = await session.get(IntegrationSource, binding.integration_source_id)

    assert context is not None
    assert source is not None
    integration = await session.get(Integration, source.integration_id)
    assert integration is not None
    integration.credential_id = replacement.id
    await session.flush()

    graph = await lock_active_pagerduty_webhook_graph(session, binding.id)

    assert graph is None


@pytest.mark.anyio
async def test_locked_graph_acquires_binding_before_related_trust_rows(
    session: AsyncSession,
) -> None:
    binding = await _seed_active_graph(session)

    async with session.begin():
        graph = await lock_active_pagerduty_webhook_graph(session, binding.id)

    assert graph is not None
    assert graph.context.binding_id == str(binding.id)


@pytest.mark.parametrize(
    ("credentials", "authorization"),
    [
        (
            PagerDutyCredentials(
                source=CredentialSource.DATABASE,
                auth_mode="api_token",
                api_token="api-token",
            ),
            "Token token=api-token",
        ),
        (
            PagerDutyCredentials(
                source=CredentialSource.DATABASE,
                auth_mode="oauth",
                access_token="oauth-token",
            ),
            "Bearer oauth-token",
        ),
        (
            PagerDutyCredentials(
                source=CredentialSource.DATABASE,
                auth_mode="client_credentials",
                access_token="client-token",
            ),
            "Bearer client-token",
        ),
    ],
)
def test_hydrated_pagerduty_auth_uses_the_mode_specific_strategy(
    credentials: PagerDutyCredentials,
    authorization: str,
) -> None:
    resolved = _pagerduty_webhook_auth(credentials)

    assert resolved.auth.headers()["Authorization"] == authorization


def test_webhook_worker_public_contract_uses_acyclic_shared_types() -> None:
    from dev_health_ops.providers.pagerduty.webhook_worker import (
        PagerDutyWebhookAuth as WorkerAuth,
    )
    from dev_health_ops.providers.pagerduty.webhook_worker import (
        PagerDutyWebhookWorkerContext as WorkerContext,
    )
    from dev_health_ops.providers.pagerduty.webhook_worker import (
        _pagerduty_webhook_auth as worker_build_auth,
    )

    assert WorkerAuth is PagerDutyWebhookAuth
    assert WorkerContext is PagerDutyWebhookWorkerContext
    assert worker_build_auth is build_pagerduty_webhook_auth
