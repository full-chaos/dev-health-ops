from __future__ import annotations

from dataclasses import dataclass
from uuid import UUID

from dev_health_ops.credentials.types import PagerDutyCredentials
from dev_health_ops.db import get_postgres_session
from dev_health_ops.providers.pagerduty.auth import (
    ApiTokenAuth,
    OAuthBearerAuth,
    PagerDutyAuth,
)
from dev_health_ops.providers.pagerduty.webhook_worker_graph import (
    _load_active_pagerduty_webhook_binding,
    hydrate_locked_pagerduty_webhook_auth,
    load_active_pagerduty_webhook_context,
    lock_active_pagerduty_webhook_graph,
    reconcile_pagerduty_webhook_with_locked_graph,
)

__all__ = [
    "PagerDutyWebhookAuth",
    "PagerDutyWebhookWorkerContext",
    "_load_active_pagerduty_webhook_binding",
    "_pagerduty_webhook_auth",
    "load_pagerduty_webhook_auth",
    "reconcile_pagerduty_webhook_with_locked_graph",
    "resolve_pagerduty_webhook_binding",
]


@dataclass(frozen=True, slots=True)
class PagerDutyWebhookWorkerContext:
    org_id: str
    binding_id: str
    provider_instance_id: str
    credential_id: str


@dataclass(frozen=True, slots=True)
class PagerDutyWebhookAuth:
    auth: PagerDutyAuth
    region: str


async def resolve_pagerduty_webhook_binding(
    binding_id: str,
) -> PagerDutyWebhookWorkerContext:
    try:
        parsed_binding_id = UUID(binding_id)
    except ValueError as exc:
        raise RuntimeError("pagerduty webhook binding identity is invalid") from exc
    async with get_postgres_session() as session:
        context = await load_active_pagerduty_webhook_context(
            session, parsed_binding_id
        )
    if context is None:
        raise RuntimeError("pagerduty webhook binding is unavailable")
    return context


async def load_pagerduty_webhook_auth(
    context: PagerDutyWebhookWorkerContext,
) -> PagerDutyWebhookAuth:
    async with get_postgres_session() as session:
        async with session.begin():
            graph = await lock_active_pagerduty_webhook_graph(
                session, UUID(context.binding_id)
            )
            if graph is None or graph.context != context:
                raise RuntimeError(
                    "pagerduty webhook binding changed before persistence"
                )
            return await hydrate_locked_pagerduty_webhook_auth(graph)


def _pagerduty_webhook_auth(
    credentials: PagerDutyCredentials,
) -> PagerDutyWebhookAuth:
    match credentials.auth_mode:
        case "api_token":
            api_token = credentials.api_token
            if not api_token:
                raise RuntimeError("pagerduty webhook credential has no API token")
            auth: PagerDutyAuth = ApiTokenAuth(api_token)
        case "oauth" | "client_credentials":
            access_token = credentials.access_token
            if not access_token:
                raise RuntimeError("pagerduty webhook credential has no access token")
            auth = OAuthBearerAuth(access_token)
        case _:
            raise RuntimeError(
                "pagerduty webhook credential has an unsupported auth mode"
            )
    return PagerDutyWebhookAuth(auth=auth, region=credentials.region)
