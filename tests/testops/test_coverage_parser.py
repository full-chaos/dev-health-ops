from __future__ import annotations

# pyright: reportMissingImports=false
from pathlib import Path
from uuid import uuid4

import pytest

from dev_health_ops.parsers.coverage import (
    parse_cobertura_xml,
    parse_lcov_report,
)
from dev_health_ops.processors.testops_coverage import (
    process_coverage_report,
)

FIXTURES_DIR = Path(__file__).parent / "fixtures"


def test_parse_lcov_report_extracts_totals() -> None:
    report = parse_lcov_report(FIXTURES_DIR / "sample_lcov.info")

    assert report.report_format == "lcov"
    assert report.lines_total == 9
    assert report.lines_covered == 7
    assert report.branches_total == 4
    assert report.branches_covered == 3
    assert len(report.files) == 2
    assert report.files[0].file_path == "services/api/src/app.py"


def test_parse_cobertura_xml_extracts_totals() -> None:
    report = parse_cobertura_xml(FIXTURES_DIR / "sample_cobertura.xml")

    assert report.report_format == "cobertura"
    assert report.lines_total == 8
    assert report.lines_covered == 6
    assert report.branches_total == 4
    assert report.branches_covered == 3
    assert len(report.files) == 2
    assert {file_record.file_path for file_record in report.files} == {
        "services/api/src/app.py",
        "packages/web/src/index.ts",
    }


@pytest.mark.asyncio
async def test_process_coverage_report_maps_snapshot() -> None:
    repo_id = uuid4()
    snapshot = await process_coverage_report(
        repo_id=repo_id,
        run_id="run-coverage",
        source=(FIXTURES_DIR / "sample_lcov.info").read_text(encoding="utf-8"),
        commit_hash="abc123",
        branch="feat/testops-test-ingestion",
        pr_number=77,
        org_id="chaos",
        service_path_prefixes={
            "services/api": "api-service",
            "packages/web": "web-service",
        },
    )

    assert snapshot["repo_id"] == repo_id
    assert snapshot["report_format"] == "lcov"
    assert snapshot["line_coverage_pct"] == pytest.approx(77.7777777778)
    assert snapshot["branch_coverage_pct"] == pytest.approx(75.0)
    assert snapshot["service_id"] == "api-service"
    assert snapshot["snapshot_id"]
