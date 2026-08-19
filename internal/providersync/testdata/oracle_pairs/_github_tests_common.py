from __future__ import annotations

import asyncio
import contextlib
import io
import pathlib
import sys
import uuid
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

import httpx

from internal.providersync.testdata.field_reflection import typed_dict_field_names
from internal.providersync.testdata.python_oracle_loader import load_live_module

if TYPE_CHECKING:
    from dev_health_ops.metrics.testops_schemas import (
        CoverageSnapshotRow,
        TestCaseResultRow,
        TestSuiteResultRow,
    )
REPO_ROOT = pathlib.Path(__file__).resolve().parents[4]

SCHEMA_SOURCE = REPO_ROOT / "src/dev_health_ops/metrics/testops_schemas.py"
INGEST_SOURCE = REPO_ROOT / "src/dev_health_ops/processors/testops_ingest.py"
PIPELINE_SOURCE = REPO_ROOT / "src/dev_health_ops/providers/github/testops_pipeline.py"

_ingest_module = load_live_module(INGEST_SOURCE)
_schemas_module = sys.modules["dev_health_ops.metrics.testops_schemas"]
if not TYPE_CHECKING:
    CoverageSnapshotRow = _schemas_module.CoverageSnapshotRow
    TestCaseResultRow = _schemas_module.TestCaseResultRow
    TestSuiteResultRow = _schemas_module.TestSuiteResultRow
ingest_report_members = _ingest_module.ingest_report_members

JUNIT = """<testsuites><testsuite name="api" time="3.5">
<testcase name="passes" classname="tests/test_api.py::TestAPI" file="services/api/test_api.py" time="1.25"/>
<testcase name="fails" classname="tests/test_api.py::TestAPI" file="services/api/test_api.py" time="2.25"><failure message="expected 200" type="AssertionError">trace</failure><system-err>stderr</system-err></testcase>
</testsuite></testsuites>"""

LCOV = """TN:
SF:services/api/main.go
DA:1,1
DA:2,0
LF:2
LH:1
BRF:2
BRH:1
FNF:1
FNH:1
end_of_record
"""


def reflected(class_name: str) -> frozenset[str]:
    return typed_dict_field_names(SCHEMA_SOURCE.read_text(), class_name)


def build_rows(
    case: dict[str, Any],
) -> tuple[
    list[TestSuiteResultRow],
    list[TestCaseResultRow],
    list[CoverageSnapshotRow],
]:
    started = datetime.fromisoformat(case["started_at"].replace("Z", "+00:00"))
    finished = datetime.fromisoformat(case["finished_at"].replace("Z", "+00:00"))
    members = [("reports/junit.xml", JUNIT.encode())]
    if case.get("malformed_report"):
        members.append(
            (
                "reports/malformed.xml",
                b'<!DOCTYPE x [<!ENTITY x "boom">]><testsuite>&x;</testsuite>',
            )
        )
    members.append(
        (
            case.get("coverage_name", "reports/lcov.info"),
            case.get("coverage", case.get("lcov", LCOV)).encode(),
        )
    )
    # The generic oracle protocol owns stdout and stderr. The active producer
    # logs each best-effort parse failure, so suppress only that output while
    # still executing the production function and comparing its returned rows.
    captured = io.StringIO()
    logger = _ingest_module.logger
    was_disabled = logger.disabled
    logger.disabled = True
    try:
        with contextlib.redirect_stdout(captured), contextlib.redirect_stderr(captured):
            return asyncio.run(
                ingest_report_members(
                    members,
                    repo_id=uuid.UUID(case["repo_id"]),
                    run_id=case["run_id"],
                    org_id=case["org_id"],
                    started_at=started.astimezone(timezone.utc),
                    finished_at=finished.astimezone(timezone.utc),
                )
            )
    finally:
        logger.disabled = was_disabled


def build_pipeline_rows(case: dict[str, Any]):
    # Execute the active adapter through the same isolated freshness loader as
    # the report producer. Keep unrelated initialization stdout out of the
    # generic oracle's single JSON result.
    captured = io.StringIO()
    with contextlib.redirect_stdout(captured):
        pipeline_module = load_live_module(PIPELINE_SOURCE)
        github_actions_adapter = pipeline_module.GitHubActionsAdapter

    async def run():
        async def handler(request: httpx.Request) -> httpx.Response:
            path = request.url.path
            if path.endswith("/actions/runs"):
                payload = {"workflow_runs": [case["raw_run"]]}
            elif path.endswith("/jobs"):
                payload = {"jobs": [case["raw_job"]]}
            elif path.endswith("/required_status_checks"):
                payload = {
                    "contexts": case.get("required_contexts", []),
                    "checks": [],
                }
            else:
                raise AssertionError(f"unexpected producer request: {request.url}")
            return httpx.Response(200, json=payload, request=request)

        adapter = github_actions_adapter(
            base_url="https://api.github.test",
            token="oracle",
            transport=httpx.MockTransport(handler),
        )
        async with adapter:
            return await adapter.fetch_pipeline_data(
                owner="acme",
                repo="api",
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


def plain_row(row: dict[str, Any]) -> dict[str, Any]:
    result = dict(row)
    result["repo_id"] = str(result["repo_id"])
    return result
