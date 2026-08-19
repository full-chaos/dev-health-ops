from __future__ import annotations

import asyncio
import importlib.util
import pathlib
from contextlib import asynccontextmanager
from dataclasses import asdict, dataclass, fields
from typing import Any
from urllib.parse import parse_qsl, urlencode

import httpx

from internal.providersync.testdata.python_oracle_loader import load_live_module

REPO_ROOT = pathlib.Path(__file__).resolve().parents[4]
PROCESSOR_SOURCE = REPO_ROOT / "src/dev_health_ops/processors/gitlab.py"
BASE_GIT_SOURCE = REPO_ROOT / "src/dev_health_ops/processors/base_git.py"
CODE_CLIENT_SOURCE = REPO_ROOT / "src/dev_health_ops/providers/gitlab/code_client.py"
PR_STATE_SOURCE = REPO_ROOT / "src/dev_health_ops/providers/pr_state.py"


def _request_path(request: httpx.Request) -> str:
    pairs = sorted(parse_qsl(request.url.query.decode(), keep_blank_values=True))
    query = urlencode(pairs)
    return f"{request.url.path}?{query}" if query else request.url.path


def _parse_time(value: str | None):
    if value is None:
        return None
    from datetime import datetime

    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _load_pr_state() -> Any:
    source = REPO_ROOT / "src/dev_health_ops/providers/pr_state.py"
    spec = importlib.util.spec_from_file_location(
        "gitlab_pr_state_traversal", source.resolve(strict=True)
    )
    if spec is None or spec.loader is None:
        raise RuntimeError(f"unable to load {source}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@dataclass(frozen=True)
class TraversalRow:
    number: int
    state: str
    reviews_count: int
    comments_count: int
    first_review_at: str | None


@dataclass(frozen=True)
class TraversalTrace:
    producer_requests: list[str]
    usage_request_count: int
    rows: list[TraversalRow]


def traversal_fields() -> frozenset[str]:
    return frozenset(field.name for field in fields(TraversalTrace))


class _Gate:
    def wait_sync(self) -> None:
        return None

    def reset(self) -> None:
        return None


class _BaseGit:
    @staticmethod
    def ensure_gate(gate: Any = None) -> _Gate:
        return gate or _Gate()

    @staticmethod
    def persist_batch_threadsafe(_awaitable: Any, _loop: Any) -> None:
        # The fake sink records synchronously when its insert method is called;
        # no application event loop is needed for this producer-only oracle.
        return None


class _Awaitable:
    def __await__(self):
        if False:
            yield None
        return None


class _Sink:
    def __init__(self) -> None:
        self.rows: list[Any] = []

    def insert_git_pull_requests(self, rows: list[Any]) -> _Awaitable:
        self.rows.extend(rows)
        return _Awaitable()

    def insert_git_pull_request_reviews(self, _rows: list[Any]) -> _Awaitable:
        return _Awaitable()


def build_traversal(case: dict[str, Any]) -> dict[str, Any]:
    processor = load_live_module(PROCESSOR_SOURCE)
    base_git = load_live_module(BASE_GIT_SOURCE)
    code_client = load_live_module(CODE_CLIENT_SOURCE)
    state = _load_pr_state()
    processor.BaseGitProcessor = _BaseGit
    processor.build_git_pull_request = base_git.build_git_pull_request
    processor.normalize_pr_state = state.normalize_pr_state

    requests: list[str] = []
    mr_pages = case.get("mr_pages", {})
    approvals = case.get("approvals", {})
    notes_pages = case.get("notes_pages", {})

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(_request_path(request))
        path = request.url.path
        query = dict(parse_qsl(request.url.query.decode(), keep_blank_values=True))
        if path == "/api/v4/projects/123/merge_requests":
            page = query.get("page", "1")
            payload = mr_pages.get(page, [])
            headers = {}
            if page in case.get("mr_next_pages", {}):
                headers["X-Next-Page"] = str(case["mr_next_pages"][page])
            return httpx.Response(200, json=payload, headers=headers, request=request)
        prefix = "/api/v4/projects/123/merge_requests/"
        if path.startswith(prefix) and path.endswith("/approvals"):
            iid = path[len(prefix) : -len("/approvals")].strip("/")
            return httpx.Response(200, json=approvals.get(iid, {}), request=request)
        if path.startswith(prefix) and path.endswith("/notes"):
            iid = path[len(prefix) : -len("/notes")].strip("/")
            page = query.get("page", "1")
            payload = notes_pages.get(iid, {}).get(page, [])
            headers = {}
            next_pages = case.get("notes_next_pages", {}).get(iid, {})
            if page in next_pages:
                headers["X-Next-Page"] = str(next_pages[page])
            return httpx.Response(200, json=payload, headers=headers, request=request)
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
    sink = _Sink()
    connector = type(
        "OracleConnector", (), {"per_page": int(case.get("per_page", 100))}
    )()
    processor._sync_gitlab_mrs_to_store(
        connector,
        123,
        case["repo_id"],
        sink,
        asyncio.get_event_loop_policy().new_event_loop(),
        int(case.get("per_page", 100)),
        state="all",
        since=_parse_time(case.get("since")),
        until=_parse_time(case.get("until")),
        usage_sink=usage,
    )
    usage_count = sum(int(item.get("request_count") or 0) for item in usage)
    if usage_count != len(requests):
        raise ValueError(
            f"producer usage observations lost requests: usage={usage_count} requests={len(requests)}"
        )
    return asdict(
        TraversalTrace(
            producer_requests=requests,
            usage_request_count=usage_count,
            rows=[
                TraversalRow(
                    number=int(row.number),
                    state=str(row.state),
                    reviews_count=int(getattr(row, "reviews_count", 0) or 0),
                    comments_count=int(getattr(row, "comments_count", 0) or 0),
                    first_review_at=(
                        row.first_review_at.isoformat().replace("+00:00", "Z")
                        if getattr(row, "first_review_at", None)
                        else None
                    ),
                )
                for row in sink.rows
            ],
        )
    )
