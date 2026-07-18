"""Boundary-safe PagerDuty incident pagination for incremental syncs."""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import datetime
from typing import Protocol

from dev_health_ops.providers.pagerduty.client import PagerDutyClient
from dev_health_ops.providers.pagerduty.models import Incident


class IncidentCursorOptions(Protocol):
    """Pagination fields required to resume an incident collection."""

    @property
    def window_start(self) -> datetime | None:
        """Earliest incident created-at timestamp to include, or None for no lower bound."""

    @property
    def window_end(self) -> datetime | None:
        """Latest incident created-at timestamp to include, or None for no upper bound."""

    @property
    def resume_after(self) -> datetime | None:
        """Inclusive created-at watermark for resuming incident iteration, or None."""


async def iter_resumable_incidents(
    client: PagerDutyClient,
    options: IncidentCursorOptions,
) -> AsyncIterator[Incident]:
    """Yield every incident in a created-at window, inclusively from its watermark."""
    # PagerDuty filters since/until by created_at; updates to older incidents are
    # captured by Wave 3 webhooks and periodic full reconciliation.
    params: dict[str, str] = {}
    if options.window_start is not None:
        params["since"] = options.window_start.isoformat()
    if options.window_end is not None:
        params["until"] = options.window_end.isoformat()
    async for page in client.iter_incident_pages(params=params):
        for incident in page:
            source_time = incident_source_time(incident)
            if (
                options.resume_after is not None
                and source_time is not None
                and source_time < options.resume_after
            ):
                continue
            yield incident


def incident_source_time(incident: Incident) -> datetime | None:
    """Return the created-at time used for PagerDuty incident cursors."""
    return incident.created_at
