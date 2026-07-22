from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dev_health_ops.core.encryption import decrypt_value
from dev_health_ops.models.integrations import Integration, IntegrationSource
from dev_health_ops.models.pagerduty_webhook_binding import PagerDutyWebhookBinding
from dev_health_ops.models.settings import IntegrationCredential
from dev_health_ops.providers.pagerduty.sync_auth import (
    hydrate_pagerduty_credentials_async,
)
from dev_health_ops.providers.pagerduty.webhook_worker_shared import (
    PagerDutyWebhookAuth,
    PagerDutyWebhookWorkerContext,
    build_pagerduty_webhook_auth,
)

if TYPE_CHECKING:
    from dev_health_ops.api.webhooks.pagerduty_models import PagerDutyV3Webhook
    from dev_health_ops.providers.pagerduty.webhooks import PagerDutyWebhookStore


@dataclass(frozen=True, slots=True)
class LockedPagerDutyWebhookGraph:
    binding: PagerDutyWebhookBinding
    source: IntegrationSource
    integration: Integration
    credential: IntegrationCredential
    context: PagerDutyWebhookWorkerContext


async def load_active_pagerduty_webhook_context(
    session: AsyncSession,
    binding_id: UUID,
) -> PagerDutyWebhookWorkerContext | None:
    graph = await _load_active_graph(session, binding_id)
    return graph.context if graph is not None else None


async def _load_active_pagerduty_webhook_binding(
    session: AsyncSession,
    binding_id: UUID,
) -> tuple[PagerDutyWebhookBinding, IntegrationSource] | None:
    graph = await _load_active_graph(session, binding_id)
    if graph is None:
        return None
    return graph.binding, graph.source


async def _load_active_graph(
    session: AsyncSession,
    binding_id: UUID,
) -> LockedPagerDutyWebhookGraph | None:
    result = await session.execute(
        select(
            PagerDutyWebhookBinding,
            IntegrationSource,
            Integration,
            IntegrationCredential,
        )
        .join(
            IntegrationSource,
            IntegrationSource.id == PagerDutyWebhookBinding.integration_source_id,
        )
        .join(Integration, Integration.id == IntegrationSource.integration_id)
        .join(
            IntegrationCredential,
            IntegrationCredential.id == PagerDutyWebhookBinding.credential_id,
        )
        .where(PagerDutyWebhookBinding.id == binding_id)
    )
    row = result.one_or_none()
    if row is None:
        return None
    binding, source, integration, credential = row
    return _trusted_graph(binding, source, integration, credential)


async def lock_active_pagerduty_webhook_graph(
    session: AsyncSession,
    binding_id: UUID,
) -> LockedPagerDutyWebhookGraph | None:
    binding_result = await session.execute(
        select(PagerDutyWebhookBinding)
        .where(
            PagerDutyWebhookBinding.id == binding_id,
            PagerDutyWebhookBinding.status == "active",
        )
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    binding: PagerDutyWebhookBinding | None = binding_result.scalar_one_or_none()
    if binding is None:
        return None
    source_result = await session.execute(
        select(IntegrationSource)
        .where(IntegrationSource.id == binding.integration_source_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    source: IntegrationSource | None = source_result.scalar_one_or_none()
    if source is None:
        return None
    integration_result = await session.execute(
        select(Integration)
        .where(Integration.id == source.integration_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    integration: Integration | None = integration_result.scalar_one_or_none()
    if integration is None:
        return None
    credential_result = await session.execute(
        select(IntegrationCredential)
        .where(IntegrationCredential.id == binding.credential_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    credential: IntegrationCredential | None = credential_result.scalar_one_or_none()
    if credential is None:
        return None
    return _trusted_graph(binding, source, integration, credential)


async def hydrate_locked_pagerduty_webhook_auth(
    graph: LockedPagerDutyWebhookGraph,
) -> PagerDutyWebhookAuth:
    from dev_health_ops.credentials.resolver import pagerduty_credentials_from_mapping

    encrypted_credentials = graph.credential.credentials_encrypted
    if not encrypted_credentials:
        raise RuntimeError("pagerduty webhook credential is unavailable")
    try:
        credentials = json.loads(decrypt_value(encrypted_credentials))
    except (ValueError, json.JSONDecodeError) as exc:
        raise RuntimeError("pagerduty webhook credential payload is invalid") from exc
    if not isinstance(credentials, dict):
        raise RuntimeError("pagerduty webhook credential payload is invalid")
    hydrated = await hydrate_pagerduty_credentials_async(
        credentials,
        org_id=graph.context.org_id,
    )
    resolved_credentials = pagerduty_credentials_from_mapping(hydrated)
    if resolved_credentials is None:
        raise RuntimeError("pagerduty webhook credential payload is invalid")
    return build_pagerduty_webhook_auth(resolved_credentials)


async def reconcile_pagerduty_webhook_with_locked_graph(
    *,
    binding_id: str,
    expected_context: PagerDutyWebhookWorkerContext,
    clickhouse_url: str,
    webhook: PagerDutyV3Webhook,
    received_at: datetime,
) -> object:
    from dev_health_ops.db import get_postgres_session
    from dev_health_ops.providers.pagerduty.client import PagerDutyClient
    from dev_health_ops.providers.pagerduty.webhooks import reconcile_pagerduty_webhook
    from dev_health_ops.storage import run_with_store

    async with get_postgres_session() as session:
        async with session.begin():
            graph = await lock_active_pagerduty_webhook_graph(session, UUID(binding_id))
            if graph is None or graph.context != expected_context:
                raise RuntimeError(
                    "pagerduty webhook binding changed before persistence"
                )
            pagerduty_auth = await hydrate_locked_pagerduty_webhook_auth(graph)
            client = PagerDutyClient(pagerduty_auth.auth, region=pagerduty_auth.region)
            try:

                async def persist(store: PagerDutyWebhookStore) -> object:
                    revalidated = await lock_active_pagerduty_webhook_graph(
                        session, UUID(binding_id)
                    )
                    if revalidated is None or revalidated.context != expected_context:
                        raise RuntimeError(
                            "pagerduty webhook binding changed before persistence"
                        )
                    return await reconcile_pagerduty_webhook(
                        webhook=webhook,
                        org_id=expected_context.org_id,
                        provider_instance_id=expected_context.provider_instance_id,
                        received_at=received_at,
                        store=store,
                        client=client,
                    )

                return await run_with_store(
                    clickhouse_url,
                    "clickhouse",
                    persist,
                    org_id=expected_context.org_id,
                )
            finally:
                await client.close()


def _trusted_graph(
    binding: PagerDutyWebhookBinding,
    source: IntegrationSource,
    integration: Integration,
    credential: IntegrationCredential,
) -> LockedPagerDutyWebhookGraph | None:
    if (
        binding.status != "active"
        or source.org_id != str(binding.org_id)
        or integration.org_id != source.org_id
        or credential.org_id != source.org_id
        or source.provider != "pagerduty"
        or integration.provider != "pagerduty"
        or credential.provider != "pagerduty"
        or integration.credential_id != binding.credential_id
        or not source.is_enabled
        or not integration.is_active
        or not credential.is_active
    ):
        return None
    credential_config = credential.config or {}
    account_id = credential_config.get("account_id")
    subdomain = credential_config.get("subdomain")
    provider_instance_id = account_id if isinstance(account_id, str) else subdomain
    if not isinstance(provider_instance_id, str) or not provider_instance_id.strip():
        return None
    canonical_instance_id = provider_instance_id.strip()
    if source.external_id != canonical_instance_id:
        return None
    context = PagerDutyWebhookWorkerContext(
        org_id=source.org_id,
        binding_id=str(binding.id),
        provider_instance_id=canonical_instance_id,
        credential_id=str(binding.credential_id),
    )
    return LockedPagerDutyWebhookGraph(
        binding, source, integration, credential, context
    )
