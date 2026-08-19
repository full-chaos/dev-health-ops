from __future__ import annotations

import asyncio
import contextlib
import io
import pathlib
import sys
import uuid
from datetime import datetime, timezone
from typing import Any

import httpx

from internal.providersync.testdata.field_reflection import typed_dict_field_names
from internal.providersync.testdata.python_oracle_loader import load_live_module

REPO_ROOT = pathlib.Path(__file__).resolve().parents[4]
SCHEMA_SOURCE = REPO_ROOT / "src/dev_health_ops/metrics/testops_schemas.py"
PIPELINE_SOURCE = REPO_ROOT / "src/dev_health_ops/providers/gitlab/testops_pipeline.py"
INGEST_SOURCE = REPO_ROOT / "src/dev_health_ops/processors/testops_ingest.py"


def reflected(class_name: str) -> frozenset[str]:
    return typed_dict_field_names(SCHEMA_SOURCE.read_text(), class_name)


def plain_row(row: dict[str, Any]) -> dict[str, Any]:
    result = dict(row)
    result["repo_id"] = str(result["repo_id"])
    return result


def build_pipeline_rows(case: dict[str, Any]):
    captured = io.StringIO()
    with contextlib.redirect_stdout(captured):
        module = load_live_module(PIPELINE_SOURCE)
        adapter_type = module.GitLabCIAdapter

    async def run():
        async def handler(request: httpx.Request) -> httpx.Response:
            path = request.url.path
            payload: Any
            if path == "/projects/123":
                payload = {"only_allow_merge_if_pipeline_succeeds": True}
            elif path == "/projects/123/pipelines":
                payload = [case["raw_pipeline"]]
            elif path == "/projects/123/pipelines/9001/jobs":
                payload = [case["raw_job"]]
            else:
                raise AssertionError(f"unexpected producer request: {request.url}")
            return httpx.Response(200, json=payload, request=request)

        adapter = adapter_type(
            base_url="https://gitlab.test",
            token="oracle",
            transport=httpx.MockTransport(handler),
        )
        async with adapter:
            return await adapter.fetch_pipeline_data(
                project_id=123,
                repo_id=uuid.UUID(case["repo_id"]),
                org_id=case["org_id"],
                since_date=datetime.fromisoformat(
                    case["since_at"].replace("Z", "+00:00")
                ),
                until_date=datetime.fromisoformat(
                    case["before_at"].replace("Z", "+00:00")
                ),
            )

    with contextlib.redirect_stdout(captured):
        return asyncio.run(run())


def build_native_rows(case: dict[str, Any]):
    captured = io.StringIO()
    with contextlib.redirect_stdout(captured):
        load_live_module(INGEST_SOURCE)
        module = sys.modules["dev_health_ops.processors.testops_tests"]
    started = datetime.fromisoformat(case["started_at"].replace("Z", "+00:00"))
    finished = datetime.fromisoformat(case["finished_at"].replace("Z", "+00:00"))
    with contextlib.redirect_stdout(captured):
        return asyncio.run(
            module.process_gitlab_test_report(
                repo_id=uuid.UUID(case["repo_id"]),
                run_id=case["run_id"],
                report=case["native_report"],
                org_id=case["org_id"],
                started_at=started.astimezone(timezone.utc),
                finished_at=finished.astimezone(timezone.utc),
            )
        )


def build_coverage_rows(case: dict[str, Any]):
    captured = io.StringIO()
    with contextlib.redirect_stdout(captured):
        module = load_live_module(INGEST_SOURCE)
        _, _, coverage = asyncio.run(
            module.ingest_report_members(
                [("reports/lcov.info", case["lcov"].encode())],
                repo_id=uuid.UUID(case["repo_id"]),
                run_id=case["run_id"],
                org_id=case["org_id"],
            )
        )
    return coverage
