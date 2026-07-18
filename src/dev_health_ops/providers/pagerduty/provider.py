"""Provider registry integration for PagerDuty's operational REST boundary."""

from __future__ import annotations

from dev_health_ops.providers.base import (
    IngestionContext,
    Provider,
    ProviderBatch,
    ProviderCapabilities,
)
from dev_health_ops.providers.pagerduty.client import PagerDutyClient


class PagerDutyProvider(Provider):
    """Expose PagerDuty capabilities while dataset sync owns operational persistence."""

    name = "pagerduty"
    capabilities = ProviderCapabilities(
        work_items=False,
        status_transitions=False,
        dependencies=False,
        interactions=False,
        sprints=False,
        reopen_events=False,
        priority=False,
    )

    def __init__(self, *, client: PagerDutyClient | None = None) -> None:
        self._client = client

    def ingest(self, ctx: IngestionContext) -> ProviderBatch:
        del ctx
        observations = self._client.drain_usage_observations() if self._client else []
        return ProviderBatch(observations={"provider_usage": observations})
