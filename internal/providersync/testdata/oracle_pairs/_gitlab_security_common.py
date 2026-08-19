from __future__ import annotations

import logging
import pathlib
from contextlib import asynccontextmanager
from dataclasses import asdict, dataclass, fields
from datetime import datetime
from typing import Any
from urllib.parse import parse_qsl, urlencode

import httpx

from internal.providersync.testdata.python_oracle_loader import load_live_module

REPO_ROOT = pathlib.Path(__file__).resolve().parents[4]
PROCESSOR_SOURCE = REPO_ROOT / "src/dev_health_ops/processors/gitlab.py"
CODE_CLIENT_SOURCE = REPO_ROOT / "src/dev_health_ops/providers/gitlab/code_client.py"


def parse_time(value: str | None) -> datetime | None:
    if value is None:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def request_path(request: httpx.Request) -> str:
    pairs = sorted(parse_qsl(request.url.query.decode(), keep_blank_values=True))
    query = urlencode(pairs)
    return f"{request.url.path}?{query}" if query else request.url.path


def _endpoint_name(path: str) -> str:
    if path.endswith("/vulnerability_findings"):
        return "findings"
    if path.endswith("/dependencies"):
        return "dependencies"
    return "project"


def _headers(case: dict[str, Any], endpoint: str) -> dict[str, str]:
    configured = case.get("response_headers", {}).get(endpoint, {})
    return {str(key): str(value) for key, value in configured.items()}


def run_producer(
    case: dict[str, Any], *, apply_until: bool
) -> tuple[list[Any], list[str], int]:
    """Execute the real processor helper with deterministic HTTP responses."""

    processor = load_live_module(PROCESSOR_SOURCE)
    code_client = load_live_module(CODE_CLIENT_SOURCE)
    requests: list[str] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request_path(request))
        path = request.url.path
        endpoint = _endpoint_name(path)
        if path == "/api/v4/projects/123":
            payload: Any = case.get(
                "project", {"id": 123, "name": "api", "path_with_namespace": "acme/api"}
            )
        elif endpoint == "findings":
            payload = case.get("findings", [])
        elif endpoint == "dependencies":
            payload = case.get("dependencies", [])
        else:
            raise AssertionError(f"unexpected producer request: {request.url}")
        status = int(case.get(f"{endpoint}_status", 200))
        return httpx.Response(
            status,
            json=payload,
            headers=_headers(case, endpoint),
            request=request,
        )

    client = code_client.GitLabCodeClient(
        private_token="oracle",
        base_url="https://gitlab.test",
        max_retries=1,
        transport=httpx.MockTransport(handler),
    )

    @asynccontextmanager
    async def client_factory(_connector: object):
        async with client:
            yield client

    processor._gitlab_code_client_from_connector = client_factory
    usage: list[dict[str, Any]] = []
    # The optional GitLab endpoints deliberately exercise the producer's
    # warning-and-degrade path.  The Go oracle subprocess consumes stdout as
    # JSON, so suppress application logging for this isolated producer call;
    # the trace still records the concrete request/status effects below.
    previous_disable = logging.root.manager.disable
    logging.disable(logging.CRITICAL)
    try:
        rows = processor._fetch_gitlab_security_alerts_sync(
            object(),
            123,
            case["repo_id"],
            case["max_alerts"],
            parse_time(case.get("since")),
            usage,
        )
    finally:
        logging.disable(previous_disable)
    if apply_until and (until := parse_time(case.get("until"))) is not None:
        rows = processor._filter_after(rows, until, "created_at")
    usage_request_count = sum(int(item.get("request_count") or 0) for item in usage)
    if usage_request_count != len(requests):
        raise ValueError(
            "security producer usage observations lost physical requests: "
            f"usage={usage_request_count} requests={len(requests)}"
        )
    return rows, requests, usage_request_count


@dataclass(frozen=True)
class SecurityTraversalRow:
    alert_id: str
    source: str


@dataclass(frozen=True)
class SecurityTraversalTrace:
    producer_requests: list[str]
    usage_request_count: int
    rows: list[SecurityTraversalRow]


def traversal_fields() -> frozenset[str]:
    return frozenset(field.name for field in fields(SecurityTraversalTrace))


def build_traversal(case: dict[str, Any]) -> dict[str, Any]:
    rows, requests, usage_request_count = run_producer(case, apply_until=True)
    return asdict(
        SecurityTraversalTrace(
            producer_requests=requests,
            usage_request_count=usage_request_count,
            rows=[SecurityTraversalRow(row.alert_id, row.source) for row in rows],
        )
    )
