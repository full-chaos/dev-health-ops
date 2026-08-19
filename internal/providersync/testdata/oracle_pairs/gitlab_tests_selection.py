from __future__ import annotations

import ast
import asyncio
import io
import logging
import pathlib
import sys
import types
import uuid
import zipfile
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any

import httpx

from internal.providersync.testdata import oracle_registry
from internal.providersync.testdata.python_oracle_loader import load_live_module

REPO_ROOT = pathlib.Path(__file__).resolve().parents[4]
PROCESSOR_SOURCE = REPO_ROOT / "src/dev_health_ops/processors/gitlab.py"
CODE_CLIENT_SOURCE = REPO_ROOT / "src/dev_health_ops/providers/gitlab/code_client.py"
INGEST_SOURCE = REPO_ROOT / "src/dev_health_ops/processors/testops_ingest.py"
FETCH_UTILS_SOURCE = REPO_ROOT / "src/dev_health_ops/processors/fetch_utils.py"
PIPELINE_SOURCE = REPO_ROOT / "src/dev_health_ops/providers/gitlab/testops_pipeline.py"


def _production_constant(name: str) -> int:
    tree = ast.parse(INGEST_SOURCE.read_text())
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(
            isinstance(target, ast.Name) and target.id == name
            for target in node.targets
        ):
            value = ast.literal_eval(node.value)
            if not isinstance(value, int):
                break
            return value
    raise ValueError(f"production TestOps constant not found: {name}")


def _active_producer() -> dict[str, Any]:
    tree = ast.parse(PROCESSOR_SOURCE.read_text())
    wanted = {
        "_is_report_name",
        "_drain_gitlab_code_usage",
        "_fetch_gitlab_test_reports_sync",
    }
    functions = [
        node
        for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and node.name in wanted
    ]
    if {node.name for node in functions} != wanted:
        raise ValueError("active GitLab test-report producer functions were not found")
    fetch_tree = ast.parse(FETCH_UTILS_SOURCE.read_text())
    datetime_parser = next(
        (
            node
            for node in fetch_tree.body
            if isinstance(node, ast.FunctionDef) and node.name == "safe_parse_datetime"
        ),
        None,
    )
    if datetime_parser is None:
        raise ValueError("active safe_parse_datetime producer was not found")
    max_artifacts = _production_constant("MAX_ARTIFACTS_PER_RUN")
    namespace: dict[str, Any] = {
        "Any": Any,
        "asyncio": asyncio,
        "datetime": datetime,
        "io": io,
        "logging": logging,
        "MAX_ARTIFACTS_PER_RUN": max_artifacts,
        "timezone": timezone,
        "zipfile": zipfile,
    }
    exec(
        compile(
            ast.Module(body=[datetime_parser, *functions], type_ignores=[]),
            PROCESSOR_SOURCE,
            "exec",
        ),
        namespace,
    )
    return namespace


def _canonical_usage(observations: list[dict[str, Any]]) -> list[dict[str, Any]]:
    aggregated: dict[tuple[str, str, str], int] = {}
    for observation in observations:
        key = (
            str(observation.get("transport")),
            str(observation.get("route_family")),
            str(observation.get("dimension")),
        )
        aggregated[key] = aggregated.get(key, 0) + int(
            observation.get("request_count") or 0
        )
    return [
        {
            "transport": transport,
            "route_family": route_family,
            "dimension": dimension,
            "request_count": request_count,
        }
        for (transport, route_family, dimension), request_count in sorted(
            aggregated.items()
        )
    ]


def _build(case: dict[str, Any]) -> dict[str, Any]:
    code_client = load_live_module(CODE_CLIENT_SOURCE)
    artifact_job_ids: list[str] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path == "/api/v4/projects/123/pipelines":
            payload = case["raw_pipelines"]
        elif path.endswith("/test_report"):
            payload = {
                "test_suites": [
                    {
                        "name": "suite",
                        "test_cases": [{"name": "case", "status": "success"}],
                    }
                ]
            }
        elif path.endswith("/jobs"):
            run_id = path.split("/")[-2]
            payload = case["jobs"] if run_id == case["job_run_id"] else []
        elif path.endswith("/artifacts"):
            artifact_job_ids.append(path.split("/")[-2])
            return httpx.Response(200, content=b"zip", request=request)
        else:
            raise AssertionError(f"unexpected producer request: {request.url}")
        return httpx.Response(200, json=payload, request=request)

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

    safe_archive = types.ModuleType("dev_health_ops.connectors.utils.safe_archive")
    setattr(
        safe_archive,
        "iter_zip_members",
        lambda *_args, **_kwargs: [("coverage.info", b"TN:\n")],
    )
    sys.modules[safe_archive.__name__] = safe_archive
    producer = _active_producer()
    producer["_gitlab_code_client_from_connector"] = client_factory
    usage: list[dict[str, Any]] = []
    reports, coverage = producer["_fetch_gitlab_test_reports_sync"](
        object(),
        123,
        datetime.fromisoformat(case["since_at"].replace("Z", "+00:00")),
        case["default_branch"],
        case["max_pipelines"],
        datetime.fromisoformat(case["before_at"].replace("Z", "+00:00")),
        usage,
    )

    pipeline_module = load_live_module(PIPELINE_SOURCE)

    async def adapter_selection() -> tuple[list[str], list[dict[str, Any]]]:
        async def adapter_handler(request: httpx.Request) -> httpx.Response:
            path = request.url.path
            if path == "/projects/123":
                payload: Any = {"only_allow_merge_if_pipeline_succeeds": True}
            elif path == "/projects/123/pipelines":
                payload = case["adapter_pipelines"]
            elif path.endswith("/jobs"):
                payload = []
            else:
                raise AssertionError(f"unexpected adapter request: {request.url}")
            return httpx.Response(200, json=payload, request=request)

        adapter = pipeline_module.GitLabCIAdapter(
            base_url="https://gitlab.test",
            token="oracle",
            transport=httpx.MockTransport(adapter_handler),
        )
        async with adapter:
            batch = await adapter.fetch_pipeline_data(
                project_id=123,
                repo_id=uuid.UUID("c7198fbc-1945-3717-05d8-eb78866b4e79"),
                org_id="org-acme",
                since_date=datetime.fromisoformat(
                    case["since_at"].replace("Z", "+00:00")
                ),
                until_date=datetime.fromisoformat(
                    case["before_at"].replace("Z", "+00:00")
                ),
            )
            adapter_usage = adapter.drain_usage_observations()
        return [str(row["run_id"]) for row in batch.pipeline_runs], adapter_usage

    adapter_run_ids, adapter_usage = asyncio.run(adapter_selection())

    return {
        "report_run_ids": [row[0] for row in reports],
        "coverage_run_ids": [row[0] for row in coverage],
        "artifact_job_ids": artifact_job_ids,
        "adapter_run_ids": adapter_run_ids,
        "usage_observations": _canonical_usage([*usage, *adapter_usage]),
        "max_pipelines": _production_constant("MAX_RUNS_PER_SYNC"),
        "max_artifacts": _production_constant("MAX_ARTIFACTS_PER_RUN"),
    }


def _fields() -> frozenset[str]:
    source = PROCESSOR_SOURCE.read_text()
    anchors = {
        "report_run_ids": "test_reports.append(",
        "coverage_run_ids": "coverage_members.append(",
        "artifact_job_ids": "download_job_artifact(",
        "usage_observations": "drain_usage_observations()",
        "max_pipelines": "MAX_RUNS_PER_SYNC",
        "max_artifacts": "MAX_ARTIFACTS_PER_RUN",
    }
    missing = [field for field, anchor in anchors.items() if anchor not in source]
    if missing:
        raise ValueError(f"selection source anchors missing: {missing}")
    if "pipeline_rows.append(" not in PIPELINE_SOURCE.read_text():
        raise ValueError("active GitLab adapter selection anchor missing")
    return frozenset((*anchors, "adapter_run_ids"))


oracle_registry.register(
    oracle_registry.PairSpec(
        id="gitlab/tests/selection",
        build_row=_build,
        reflected_fields=_fields,
    )
)
