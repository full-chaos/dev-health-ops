from __future__ import annotations

import os
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import httpx

from dev_health_ops.connectors.exceptions import APIException, AuthenticationException
from dev_health_ops.metrics.testops_schemas import JobRunRow, PipelineRunExtendedRow


@dataclass(slots=True)
class PipelineSyncBatch:
    pipeline_runs: list[PipelineRunExtendedRow] = field(default_factory=list)
    job_runs: list[JobRunRow] = field(default_factory=list)
    last_synced_cursor: datetime | None = None


class BasePipelineAdapter(ABC):
    provider: str
    token_env_var: str = ""

    def __init__(
        self,
        *,
        base_url: str,
        token: str | None = None,
        per_page: int = 100,
        timeout: float = 30.0,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        resolved_token = token or self._token_from_env()
        if not resolved_token:
            raise AuthenticationException(
                f"Missing API token for {self.provider}: {self.token_env_var}"
            )

        self.base_url = base_url.rstrip("/")
        self.token = resolved_token
        self.per_page = per_page
        self.timeout = timeout
        self._transport = transport
        self._client: httpx.AsyncClient | None = None

    def _token_from_env(self) -> str | None:
        if not self.token_env_var:
            return None
        return os.getenv(self.token_env_var)

    @property
    @abstractmethod
    def default_headers(self) -> dict[str, str]:
        raise NotImplementedError

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(
                base_url=self.base_url,
                headers=self.default_headers,
                timeout=self.timeout,
                transport=self._transport,
            )
        return self._client

    async def close(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def __aenter__(self) -> BasePipelineAdapter:
        await self._get_client()
        return self

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
        await self.close()

    async def _request_json(
        self,
        method: str,
        url: str,
        *,
        params: dict[str, Any] | None = None,
    ) -> tuple[Any, httpx.Response]:
        client = await self._get_client()
        response = await client.request(method, url, params=params)
        if response.status_code == 401:
            raise AuthenticationException(
                f"{self.provider} authentication failed: {response.text}"
            )
        if response.status_code >= 400:
            raise APIException(
                f"{self.provider} API request failed: {response.status_code} {response.text}"
            )
        return response.json(), response

    async def _paginate(
        self,
        url: str,
        *,
        params: dict[str, Any] | None = None,
        data_key: str | None = None,
    ) -> list[Any]:
        aggregated: list[Any] = []
        page = 1
        current_params = dict(params or {})
        current_params.setdefault("per_page", self.per_page)

        while True:
            current_params["page"] = page
            payload, response = await self._request_json(
                "GET", url, params=current_params
            )
            items = payload.get(data_key, []) if data_key else payload
            if not isinstance(items, list):
                raise APIException(
                    f"Unexpected paginated response for {self.provider}: {type(items)!r}"
                )

            aggregated.extend(items)

            next_page = self._next_page(response, page, len(items))
            if next_page is None:
                break
            page = next_page

        return aggregated

    def _next_page(
        self, response: httpx.Response, current_page: int, item_count: int
    ) -> int | None:
        next_page_header = response.headers.get("x-next-page")
        if next_page_header:
            try:
                return int(next_page_header)
            except ValueError:
                return None
        if item_count < self.per_page:
            return None
        return current_page + 1

    @staticmethod
    def parse_datetime(value: object) -> datetime | None:
        if value is None:
            return None
        if isinstance(value, datetime):
            parsed = value
        elif isinstance(value, str):
            try:
                parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            except ValueError:
                return None
        else:
            return None

        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)

    @classmethod
    def seconds_between(
        cls, start: datetime | None, end: datetime | None
    ) -> float | None:
        if start is None or end is None:
            return None
        return max(0.0, (end - start).total_seconds())

    @staticmethod
    def coerce_trigger_source(value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.lower()
        mapping = {
            "push": "push",
            "pull_request": "pr",
            "merge_request_event": "pr",
            "merge_request": "pr",
            "schedule": "schedule",
            "workflow_dispatch": "manual",
            "web": "manual",
            "manual": "manual",
            "api": "api",
            "repository_dispatch": "api",
            "trigger": "api",
        }
        return mapping.get(normalized, normalized)

    @staticmethod
    def add_org_id(row: Any, org_id: str | None) -> Any:
        if org_id:
            row["org_id"] = org_id
        return row

    @abstractmethod
    async def fetch_pipeline_data(self, **kwargs: Any) -> PipelineSyncBatch:
        raise NotImplementedError
