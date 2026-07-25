from __future__ import annotations

from collections.abc import AsyncIterator
from uuid import UUID, uuid4

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from dev_health_ops.models import (
    PagerDutyWebhookBinding as ExportedPagerDutyWebhookBinding,
)
from dev_health_ops.models.git import Base
from dev_health_ops.models.integrations import Integration, IntegrationSource
from dev_health_ops.models.pagerduty_webhook_binding import PagerDutyWebhookBinding
from dev_health_ops.models.settings import IntegrationCredential
from dev_health_ops.providers.pagerduty.webhook_bindings import (
    CreatePagerDutyWebhookBinding,
    PagerDutyWebhookBindingInputError,
    PagerDutyWebhookBindingService,
)


@pytest.fixture(autouse=True)
def encryption_key(monkeypatch: pytest.MonkeyPatch) -> None:
    """Given a stable test key, lifecycle operations can encrypt secrets."""
    monkeypatch.setenv("SETTINGS_ENCRYPTION_KEY", "test-encryption-key")


def test_binding_model_is_registered_for_alembic_metadata() -> None:
    assert ExportedPagerDutyWebhookBinding is PagerDutyWebhookBinding


@pytest.fixture
async def session() -> AsyncIterator[AsyncSession]:
    """Given an isolated binding table, lifecycle behavior persists durably."""
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    async with session_factory() as database_session:
        yield database_session
    await engine.dispose()


async def trusted_binding_input(
    session: AsyncSession,
    *,
    org_id: UUID | None = None,
    integration_source_id: UUID | None = None,
    provider_subscription_id: str = "subscription-1",
    signing_secret: str = "initial-secret",
) -> CreatePagerDutyWebhookBinding:
    """Persist a complete active PagerDuty graph and return its binding input."""
    resolved_org_id = org_id or uuid4()
    credential = IntegrationCredential(
        provider="pagerduty",
        name=f"credential-{uuid4()}",
        org_id=str(resolved_org_id),
        credentials_encrypted="encrypted",
        config={},
    )
    integration = Integration(
        id=uuid4(),
        org_id=str(resolved_org_id),
        provider="pagerduty",
        credential_id=credential.id,
        name=f"integration-{uuid4()}",
        config={},
    )
    source = IntegrationSource(
        id=integration_source_id or uuid4(),
        org_id=str(resolved_org_id),
        integration_id=integration.id,
        provider="pagerduty",
        source_type="service",
        external_id=f"service-{uuid4()}",
        name="service",
        full_name="service",
        metadata_={},
    )
    session.add_all([credential, integration, source])
    await session.flush()
    return CreatePagerDutyWebhookBinding(
        org_id=resolved_org_id,
        integration_source_id=source.id,
        credential_id=credential.id,
        provider_subscription_id=provider_subscription_id,
        signing_secret=signing_secret,
    )


@pytest.mark.anyio
async def test_create_starts_a_candidate_and_loads_the_receivable_signing_secret(
    session: AsyncSession,
) -> None:
    """Given a new binding, when it is created, then only its resolved form exposes plaintext."""
    service = PagerDutyWebhookBindingService(session)

    binding = await service.create(await trusted_binding_input(session))
    resolved = await service.load_receivable_by_id(binding.id)

    assert binding.signing_secret_encrypted != "initial-secret"
    assert binding.signing_secret_key_version == "v1"
    assert binding.status == "candidate"
    assert resolved is not None
    assert resolved.binding.id == binding.id
    assert resolved.signing_secret == "initial-secret"


@pytest.mark.anyio
async def test_route_uuid_keeps_cross_org_duplicate_subscriptions_isolated(
    session: AsyncSession,
) -> None:
    # Given
    service = PagerDutyWebhookBindingService(session)
    first = await service.create(
        await trusted_binding_input(session, provider_subscription_id="subscription-1")
    )
    second = await service.create(
        await trusted_binding_input(session, provider_subscription_id="subscription-1")
    )

    # When
    resolved = await service.load_receivable_by_id(second.id)

    # Then
    assert first.org_id != second.org_id
    assert resolved is not None
    assert resolved.binding.id == second.id
    assert resolved.signing_secret == "initial-secret"


@pytest.mark.anyio
async def test_rotation_creates_candidate_then_revokes_old_secret_at_cutover(
    session: AsyncSession,
) -> None:
    """Given a ready candidate, when it cuts over, then the old secret is inactive."""
    service = PagerDutyWebhookBindingService(session)
    binding = await service.create(await trusted_binding_input(session))
    await service.mark_candidate_ready_from_verified_ping(binding.id)
    activated = await service.cutover_ready_candidate(binding.id)
    assert activated is not None
    initial_encrypted = binding.signing_secret_encrypted
    assert binding.credential_id is not None

    candidate_input = CreatePagerDutyWebhookBinding(
        org_id=binding.org_id,
        integration_source_id=binding.integration_source_id,
        credential_id=binding.credential_id,
        provider_subscription_id="subscription-2",
        signing_secret="rotated-secret",
    )
    candidate = await service.create_rotation_candidate(binding.id, candidate_input)
    assert candidate is not None
    ready = await service.mark_candidate_ready_from_verified_ping(candidate.id)
    cutover = await service.cutover_ready_candidate(candidate.id)
    resolved = await service.load_active_by_id(candidate.id)

    assert ready is not None
    assert cutover is not None
    assert binding.signing_secret_encrypted == initial_encrypted
    assert binding.status == "inactive"
    assert binding.rotated_at is not None
    assert resolved is not None
    assert resolved.signing_secret == "rotated-secret"


@pytest.mark.anyio
async def test_credential_removal_retains_revoked_binding_history(
    session: AsyncSession,
) -> None:
    service = PagerDutyWebhookBindingService(session)
    binding = await service.create(await trusted_binding_input(session))
    assert binding.credential_id is not None

    await service.revoke_and_detach_for_credential(binding.credential_id)

    retained = await service.load_by_id_for_org(binding.id, binding.org_id)
    assert retained is not None
    assert retained.status == "inactive"
    assert retained.revoked_at is not None
    assert retained.credential_id is None


@pytest.mark.anyio
async def test_revoke_hides_a_binding_from_active_lookups(
    session: AsyncSession,
) -> None:
    """Given an active binding, when it is revoked, then consumers cannot resolve it."""
    service = PagerDutyWebhookBindingService(session)
    binding = await service.create(await trusted_binding_input(session))

    revoked = await service.revoke(binding.id)

    assert revoked is not None
    assert revoked.status == "inactive"
    assert revoked.revoked_at is not None
    assert await service.load_active_by_id(binding.id) is None


@pytest.mark.anyio
async def test_revoke_accepts_a_candidate_and_preserves_its_original_revocation(
    session: AsyncSession,
) -> None:
    # Given
    service = PagerDutyWebhookBindingService(session)
    binding = await service.create(await trusted_binding_input(session))
    credential_id = binding.credential_id
    assert credential_id is not None

    # When
    revoked = await service.revoke(binding.id)
    await service.revoke_and_detach_for_credential(credential_id)

    # Then
    assert revoked is not None
    assert revoked.status == "inactive"
    assert revoked.revoked_at is not None
    assert binding.revoked_at == revoked.revoked_at


@pytest.mark.anyio
async def test_initial_ready_candidate_activates_without_an_existing_binding(
    session: AsyncSession,
) -> None:
    # Given
    service = PagerDutyWebhookBindingService(session)
    candidate = await service.create(await trusted_binding_input(session))
    await service.mark_candidate_ready_from_verified_ping(candidate.id)

    # When
    activated = await service.cutover_ready_candidate(candidate.id)

    # Then
    assert activated is not None
    assert activated.id == candidate.id
    assert activated.status == "active"


@pytest.mark.anyio
async def test_rotation_lifecycle_locks_source_bindings_in_uuid_order(
    session: AsyncSession,
) -> None:
    # Given
    service = PagerDutyWebhookBindingService(session)
    active = await service.create(await trusted_binding_input(session))
    await service.mark_candidate_ready_from_verified_ping(active.id)
    await service.cutover_ready_candidate(active.id)
    credential_id = active.credential_id
    assert credential_id is not None
    candidate = await service.create_rotation_candidate(
        active.id,
        CreatePagerDutyWebhookBinding(
            org_id=active.org_id,
            integration_source_id=active.integration_source_id,
            credential_id=credential_id,
            provider_subscription_id="subscription-2",
            signing_secret="rotated-secret",
        ),
    )
    assert candidate is not None

    # When
    locked = await service._repository.receivable_by_source_for_update(
        active.integration_source_id
    )

    # Then
    assert [binding.id for binding in locked] == sorted(
        binding.id for binding in locked
    )


@pytest.mark.anyio
async def test_create_rejects_guessed_cross_org_or_inactive_graph(
    session: AsyncSession,
) -> None:
    """Given an untrusted graph, when a binding is created, then no secret is stored."""
    service = PagerDutyWebhookBindingService(session)
    guessed = CreatePagerDutyWebhookBinding(
        org_id=uuid4(),
        integration_source_id=uuid4(),
        credential_id=uuid4(),
        provider_subscription_id="subscription-1",
        signing_secret="initial-secret",
    )

    with pytest.raises(PagerDutyWebhookBindingInputError):
        await service.create(guessed)
