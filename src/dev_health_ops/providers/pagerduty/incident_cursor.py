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
    def window_start(self) -> datetime | None: ...

    @property
    def window_end(self) -> datetime | None: ...

    @property
    def resume_after(self) -> datetime | None: ...

    @property
    def incident_cap(self) -> int: ...


async def iter_resumable_incidents(
    client: PagerDutyClient,
    options: IncidentCursorOptions,
) -> AsyncIterator[Incident]:
    """Yield a fixed window without replaying records at its stored watermark."""
    params: dict[str, str] = {}
    if options.window_start is not None:
        params["since"] = options.window_start.isoformat()
    if options.window_end is not None:
        params["until"] = options.window_end.isoformat()
    emitted = 0
    async for page in client.iter_incident_pages(params=params):
        for incident in page:
            source_time = incident_source_time(incident)
            if (
                options.resume_after is not None
                and source_time is not None
                and source_time <= options.resume_after
            ):
                continue
            if emitted >= options.incident_cap:
                return
            yield incident
            emitted += 1


def incident_source_time(incident: Incident) -> datetime | None:
    """Return the provider event time used for PagerDuty incident cursors."""
    return incident.updated_at or incident.created_at
