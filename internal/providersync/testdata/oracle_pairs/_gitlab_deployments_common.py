from __future__ import annotations

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
BASE_GIT_SOURCE = REPO_ROOT / "src/dev_health_ops/processors/base_git.py"
RELEASE_REF_SOURCE = REPO_ROOT / "src/dev_health_ops/processors/release_ref.py"


def producer_modules() -> tuple[Any, Any]:
    """Load the real deployment producer with its live code-client helpers."""

    processor = load_live_module(PROCESSOR_SOURCE)
    base_git = load_live_module(BASE_GIT_SOURCE)
    release_ref = load_live_module(RELEASE_REF_SOURCE)
    code_client = load_live_module(CODE_CLIENT_SOURCE)
    processor.build_deployment = base_git.build_deployment
    processor.get_release_ref_enrichment = release_ref.get_release_ref_enrichment
    return processor, code_client


def parse_time(value: str | None) -> datetime | None:
    if value is None:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _request_path(request: httpx.Request) -> str:
    pairs = sorted(parse_qsl(request.url.query.decode(), keep_blank_values=True))
    query = urlencode(pairs)
    return f"{request.url.path}?{query}" if query else request.url.path


def _headers(case: dict[str, Any], endpoint: str) -> dict[str, str]:
    configured = case.get("response_headers", {}).get(endpoint, {})
    headers = {str(key): str(value) for key, value in configured.items()}
    next_page_key = {
        "releases": "release_next_page",
        "deployments": "deployment_next_page",
    }.get(endpoint)
    if next_page_key and case.get(next_page_key):
        headers.setdefault("X-Next-Page", str(case[next_page_key]))
    return headers


def _status(case: dict[str, Any], endpoint: str, sha: str | None = None) -> int:
    if sha is not None:
        return int(case.get("merge_request_status_by_sha", {}).get(sha, 200))
    return int(case.get(f"{endpoint}_status", 200))


def run_producer(
    case: dict[str, Any], *, apply_until: bool
) -> tuple[list[Any], list[str], int]:
    """Execute the live producer and return rows plus its physical requests.

    The mock transport only supplies deterministic GitLab responses. The
    request order/count and usage observations are emitted by the real
    GitLabCodeClient and `_fetch_gitlab_deployments_sync` path.
    """

    processor, code_client = producer_modules()
    requests: list[str] = []
    merge_requests_by_sha = case.get("merge_requests_by_sha", {})

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(_request_path(request))
        path = request.url.path
        if path == "/api/v4/projects/123/releases":
            endpoint, payload = "release", case.get("releases", [])
            return httpx.Response(
                _status(case, endpoint),
                json=payload,
                headers=_headers(case, "releases"),
                request=request,
            )
        if path == "/api/v4/projects/123/deployments":
            endpoint, payload = "deployment", case.get("deployments", [])
            return httpx.Response(
                _status(case, endpoint),
                json=payload,
                headers=_headers(case, "deployments"),
                request=request,
            )
        if path.startswith(
            "/api/v4/projects/123/repository/commits/"
        ) and path.endswith("/merge_requests"):
            sha = path.removeprefix(
                "/api/v4/projects/123/repository/commits/"
            ).removesuffix("/merge_requests")
            return httpx.Response(
                _status(case, "merge_request", sha),
                json=merge_requests_by_sha.get(sha, []),
                headers=_headers(case, f"merge_request:{sha}"),
                request=request,
            )
        raise AssertionError(f"unexpected producer request: {request.url}")

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
    rows = processor._fetch_gitlab_deployments_sync(
        object(),
        123,
        case["repo_id"],
        case["max_deployments"],
        parse_time(case.get("since")),
        usage,
    )
    if apply_until and (until := parse_time(case.get("until"))) is not None:
        rows = processor._filter_after(rows, until, "deployed_at", "started_at")
    usage_request_count = sum(int(item.get("request_count") or 0) for item in usage)
    if usage_request_count != len(requests):
        raise ValueError(
            "deployment producer usage observations lost physical requests: "
            f"usage={usage_request_count} requests={len(requests)}"
        )
    return rows, requests, usage_request_count


@dataclass(frozen=True)
class TraversalRow:
    deployment_id: str
    release_ref: str
    release_ref_confidence: float
    pull_request_number: int | None


@dataclass(frozen=True)
class TraversalTrace:
    producer_requests: list[str]
    usage_request_count: int
    rows: list[TraversalRow]


def traversal_fields() -> frozenset[str]:
    return frozenset(field.name for field in fields(TraversalTrace))


def build_traversal(case: dict[str, Any]) -> dict[str, Any]:
    rows, requests, usage_request_count = run_producer(case, apply_until=True)
    return asdict(
        TraversalTrace(
            producer_requests=requests,
            usage_request_count=usage_request_count,
            rows=[
                TraversalRow(
                    deployment_id=row.deployment_id,
                    release_ref=row.release_ref,
                    release_ref_confidence=row.release_ref_confidence,
                    pull_request_number=row.pull_request_number,
                )
                for row in rows
            ],
        )
    )
