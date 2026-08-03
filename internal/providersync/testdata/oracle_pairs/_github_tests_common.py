from __future__ import annotations

import asyncio
import pathlib
import sys
import uuid
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

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
    return asyncio.run(
        ingest_report_members(
            [
                ("reports/junit.xml", JUNIT.encode()),
                ("reports/lcov.info", case.get("lcov", LCOV).encode()),
            ],
            repo_id=uuid.UUID(case["repo_id"]),
            run_id=case["run_id"],
            org_id=case["org_id"],
            started_at=started.astimezone(timezone.utc),
            finished_at=finished.astimezone(timezone.utc),
        )
    )
