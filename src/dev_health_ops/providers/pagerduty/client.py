"""Read-only PagerDuty REST V2 client composed over InstrumentedRESTCore."""

from collections.abc import AsyncIterator
from typing import TypeVar

import httpx
from pydantic import BaseModel

from dev_health_ops.providers._http import InstrumentedRESTCore
from dev_health_ops.providers.pagerduty.auth import PagerDutyAuth
from dev_health_ops.providers.pagerduty.budget import PAGERDUTY_OPERATION_RESOLVER
from dev_health_ops.providers.pagerduty.models import (
    Alert,
    BusinessService,
    EscalationPolicy,
    Incident,
    LogEntry,
    Note,
    Oncall,
    Schedule,
    Service,
    Team,
    User,
)

T = TypeVar("T", bound=BaseModel)
_ACCEPT = "application/vnd.pagerduty+json;version=2"


def pagerduty_base_url(*, region: str) -> str:
    """Return the regional PagerDuty API base URL."""
    if region == "eu":
        return "https://api.eu.pagerduty.com"
    return "https://api.pagerduty.com"


class PagerDutyClient:
    """Only exposes PagerDuty GET endpoints approved for V1."""

    def __init__(
        self,
        auth: PagerDutyAuth,
        *,
        region: str = "us",
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self._auth = auth
        self._core = InstrumentedRESTCore(
            base_url=pagerduty_base_url(region=region),
            provider="pagerduty",
            resolver=PAGERDUTY_OPERATION_RESOLVER,
            headers={"Accept": _ACCEPT},
            reset_header_name="ratelimit-reset",
            diagnostic_header_names=(
                "ratelimit-limit",
                "ratelimit-remaining",
                "ratelimit-reset",
                "retry-after",
            ),
            transport=transport,
        )

    def drain_usage_observations(self) -> list[dict[str, object]]:
        return self._core.drain_usage_observations()

    async def get_incident(self, incident_id: str) -> Incident:
        return await self._one(f"/incidents/{incident_id}", "incident", Incident)

    async def list_incidents(
        self, *, params: dict[str, str] | None = None
    ) -> list[Incident]:
        return await self._many("/incidents", "incidents", Incident, params)

    async def iter_incident_pages(
        self, *, params: dict[str, str] | None = None
    ) -> AsyncIterator[list[Incident]]:
        async for page in self._iter_many("/incidents", "incidents", Incident, params):
            yield page

    async def list_incident_alerts(self, incident_id: str) -> list[Alert]:
        return await self._many(
            f"/incidents/{incident_id}/alerts", "alerts", Alert, None
        )

    async def iter_incident_alert_pages(
        self, incident_id: str
    ) -> AsyncIterator[list[Alert]]:
        async for page in self._iter_many(
            f"/incidents/{incident_id}/alerts", "alerts", Alert, None
        ):
            yield page

    async def list_incident_log_entries(self, incident_id: str) -> list[LogEntry]:
        return await self._many(
            f"/incidents/{incident_id}/log_entries", "log_entries", LogEntry, None
        )

    async def iter_incident_log_entry_pages(
        self, incident_id: str
    ) -> AsyncIterator[list[LogEntry]]:
        async for page in self._iter_many(
            f"/incidents/{incident_id}/log_entries", "log_entries", LogEntry, None
        ):
            yield page

    async def list_incident_notes(self, incident_id: str) -> list[Note]:
        return await self._many(f"/incidents/{incident_id}/notes", "notes", Note, None)

    async def iter_incident_note_pages(
        self, incident_id: str
    ) -> AsyncIterator[list[Note]]:
        async for page in self._iter_many(
            f"/incidents/{incident_id}/notes", "notes", Note, None
        ):
            yield page

    async def list_services(self) -> list[Service]:
        return await self._many("/services", "services", Service, None)

    async def list_business_services(self) -> list[BusinessService]:
        return await self._many(
            "/business_services", "business_services", BusinessService, None
        )

    async def list_escalation_policies(self) -> list[EscalationPolicy]:
        return await self._many(
            "/escalation_policies", "escalation_policies", EscalationPolicy, None
        )

    async def list_schedules(self) -> list[Schedule]:
        return await self._many("/schedules", "schedules", Schedule, None)

    async def list_oncalls(self) -> list[Oncall]:
        return await self._many("/oncalls", "oncalls", Oncall, None)

    async def list_users(self) -> list[User]:
        return await self._many("/users", "users", User, None)

    async def list_teams(self) -> list[Team]:
        return await self._many("/teams", "teams", Team, None)

    async def _one(self, path: str, key: str, model: type[T]) -> T:
        response = await self._core.request(
            "GET",
            path,
            operation=f"pagerduty_{key}:GET {path}",
            headers=self._auth.headers(),
        )
        return model.model_validate(response.json()[key])

    async def _many(
        self, path: str, key: str, model: type[T], params: dict[str, str] | None
    ) -> list[T]:
        values: list[T] = []
        async for page in self._iter_many(path, key, model, params):
            values.extend(page)
        return values

    async def _iter_many(
        self, path: str, key: str, model: type[T], params: dict[str, str] | None
    ) -> AsyncIterator[list[T]]:
        offset = 0
        while True:
            query = {**(params or {}), "limit": "100", "offset": str(offset)}
            response = await self._core.request(
                "GET",
                path,
                operation=f"pagerduty_{key}:GET {path}",
                params=query,
                headers=self._auth.headers(),
            )
            payload = response.json()
            page = [model.model_validate(value) for value in payload.get(key, [])]
            yield page
            if not payload.get("more", False):
                return
            if not page:
                raise RuntimeError(f"PagerDuty pagination made no progress for {path}")
            offset += len(page)
