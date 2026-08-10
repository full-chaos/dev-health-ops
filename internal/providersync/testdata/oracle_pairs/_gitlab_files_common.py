from __future__ import annotations

import asyncio
import logging
import pathlib
from contextlib import asynccontextmanager
from dataclasses import asdict, dataclass
from datetime import datetime
from types import SimpleNamespace
from typing import Any
from urllib.parse import parse_qsl, unquote, urlencode

import httpx

from internal.providersync.testdata.python_oracle_loader import load_live_module

REPO_ROOT = pathlib.Path(__file__).resolve().parents[4]
PROCESSOR_SOURCE = REPO_ROOT / "src/dev_health_ops/processors/gitlab.py"
CODE_CLIENT_SOURCE = REPO_ROOT / "src/dev_health_ops/providers/gitlab/code_client.py"
BASE_GIT_SOURCE = REPO_ROOT / "src/dev_health_ops/processors/base_git.py"


def _parse_time(value: str | None) -> datetime | None:
    if value is None or value == "":
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _request_path(request: httpx.Request) -> str:
    pairs = sorted(parse_qsl(request.url.query.decode(), keep_blank_values=True))
    query = urlencode(pairs)
    path = unquote(request.url.path)
    marker = "/api/v4/projects/"
    if path.startswith(marker):
        suffix = path[len(marker) :]
        _, separator, remainder = suffix.partition("/repository")
        if separator:
            path = f"{marker}{{project}}/repository{remainder}"
    return f"{path}?{query}" if query else path


@dataclass(frozen=True)
class FileTraceRow:
    path: str
    executable: bool
    contents: str | None


@dataclass(frozen=True)
class FileTraversalTrace:
    producer_requests: list[str]
    usage_request_count: int
    tree_paths: list[str]
    rows: list[FileTraceRow]
    incomplete: list[dict[str, Any]]


def reflected_trace_fields() -> frozenset[str]:
    return frozenset(
        {"producer_requests", "usage_request_count", "tree_paths", "rows", "incomplete"}
    )


def run_python_producer(case: dict[str, Any]) -> FileTraversalTrace:
    logging.disable(logging.CRITICAL)
    processor = load_live_module(PROCESSOR_SOURCE)
    base_git = load_live_module(BASE_GIT_SOURCE)
    code_client_module = load_live_module(CODE_CLIENT_SOURCE)

    # Keep the real worker and persistence boundary in the execution path;
    # only the unrelated commit-stats/blame gates are fixed to this files unit.
    async def needs(*_args: Any, **_kwargs: Any) -> Any:
        return SimpleNamespace(files=True, blame=False, commit_stats=False)

    async def no_blame(*_args: Any, **_kwargs: Any) -> bool:
        return False

    processor.check_backfill_needs = needs
    processor.blame_backfill_needed = no_blame
    processor.backfill_file_records = base_git.backfill_file_records
    processor.historical_backfill_day = base_git.historical_backfill_day
    processor.write_historical_complexity = lambda **_kwargs: None

    requests: list[str] = []
    tree_pages = case.get("tree_pages", [[]])
    tree_next_pages = case.get("tree_next_pages", [])
    commit_rows = case.get("commit_rows", [])
    sizes = {str(path): int(value) for path, value in case.get("sizes", {}).items()}
    contents = {
        str(path): str(value) for path, value in case.get("contents", {}).items()
    }

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(_request_path(request))
        if request.url.path.endswith("/repository/commits"):
            return httpx.Response(200, json=commit_rows, request=request)
        if request.url.path.endswith("/repository/tree"):
            page = int(request.url.params.get("page", "1"))
            index = page - 1
            payload = tree_pages[index] if index < len(tree_pages) else []
            headers: dict[str, str] = {}
            if index < len(tree_next_pages) and tree_next_pages[index]:
                headers["X-Next-Page"] = str(tree_next_pages[index])
            return httpx.Response(200, json=payload, headers=headers, request=request)
        if request.url.path == "/api/graphql":
            body = json_loads(request.content)
            variables = body["variables"]
            paths = [str(path) for path in variables["paths"]]
            query = str(body["query"])
            if case.get("content_failure") and "rawTextBlob" in query:
                return httpx.Response(500, json={}, request=request)
            if "rawSize" in query:
                nodes = [
                    {"path": path, "rawSize": sizes.get(path, 100)} for path in paths
                ]
            else:
                nodes = [
                    {"path": path, "rawTextBlob": contents.get(path)} for path in paths
                ]
            return httpx.Response(
                200,
                json={"data": {"project": {"repository": {"blobs": {"nodes": nodes}}}}},
                request=request,
            )
        raise AssertionError(f"unexpected producer request: {request.url}")

    client = code_client_module.GitLabCodeClient(
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
    inserted: list[Any] = []

    class Sink:
        async def get_git_file_contents_by_path(self, _repo_id: Any) -> dict[str, str]:
            return {}

        async def insert_git_file_data(self, rows: list[Any]) -> None:
            inserted.extend(rows)

    until = _parse_time(case.get("until"))
    asyncio.run(
        processor._backfill_gitlab_missing_data(
            SimpleNamespace(),
            Sink(),
            object(),
            SimpleNamespace(id=case["repo_id"]),
            case.get("project_full_name", "Acme/API"),
            case.get("default_branch", "main"),
            None,
            include_files=True,
            include_blame=False,
            include_commit_stats=False,
            until=until,
            usage_sink=usage,
        )
    )
    usage_request_count = sum(int(item.get("request_count") or 0) for item in usage)
    if usage_request_count != len(requests):
        raise ValueError(
            "GitLab files producer usage observations lost physical requests: "
            f"usage={usage_request_count} requests={len(requests)}"
        )
    rows = sorted(
        (
            FileTraceRow(
                path=str(getattr(row, "path")),
                executable=bool(getattr(row, "executable")),
                contents=getattr(row, "contents", None),
            )
            for row in inserted
        ),
        key=lambda row: row.path,
    )
    tree_paths = [
        str(item["path"])
        for page in tree_pages
        for item in page
        if isinstance(item, dict) and item.get("type") == "blob" and item.get("path")
    ]
    scanner = processor.ComplexityScanner(
        config_path=processor.DEFAULT_COMPLEXITY_CONFIG_PATH
    )
    scannable_count = sum(1 for path in tree_paths if scanner.should_process(path))
    incomplete: list[dict[str, Any]] = []
    if scannable_count > 2_000:
        incomplete.append(
            {
                "cause": "content_cap",
                "subject": "gitlab/files",
                "limit": 2_000,
                "observed": scannable_count,
            }
        )
    if case.get("content_failure"):
        incomplete.append(
            {
                "cause": "content_fetch",
                "subject": "gitlab/files",
                "limit": 0,
                "observed": scannable_count,
            }
        )
    return FileTraversalTrace(
        producer_requests=requests,
        usage_request_count=usage_request_count,
        tree_paths=tree_paths,
        rows=rows,
        incomplete=incomplete,
    )


def json_loads(value: bytes) -> dict[str, Any]:
    import json

    decoded = json.loads(value)
    if not isinstance(decoded, dict):
        raise ValueError(f"GraphQL request was not an object: {decoded!r}")
    return decoded


def build_traversal(case: dict[str, Any]) -> dict[str, Any]:
    return asdict(run_python_producer(case))
