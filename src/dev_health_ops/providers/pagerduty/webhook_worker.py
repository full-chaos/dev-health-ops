from __future__ import annotations

from uuid import UUID

from dev_health_ops.db import get_postgres_session
from dev_health_ops.providers.pagerduty.webhook_worker_graph import (
    _load_active_pagerduty_webhook_binding,
    hydrate_locked_pagerduty_webhook_auth,
    load_active_pagerduty_webhook_context,
    lock_active_pagerduty_webhook_graph,
    reconcile_pagerduty_webhook_with_locked_graph,
)
from dev_health_ops.providers.pagerduty.webhook_worker_shared import (
    PagerDutyWebhookAuth,
    PagerDutyWebhookWorkerContext,
    build_pagerduty_webhook_auth,
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


_pagerduty_webhook_auth = build_pagerduty_webhook_auth
