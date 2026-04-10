from __future__ import annotations

import hashlib
import uuid
from collections import Counter
from collections.abc import Mapping

from dev_health_ops.metrics.testops_schemas import CoverageSnapshotRow
from dev_health_ops.parsers.coverage import CoverageReport, parse_coverage_report
from dev_health_ops.processors.testops_tests import attribute_service_from_path


def _hash_identifier(*parts: str | None) -> str:
    payload = "::".join(part or "" for part in parts)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def build_snapshot_id(run_id: str, report_format: str | None) -> str:
    return _hash_identifier(run_id, report_format)


def _coverage_pct(covered: int | None, total: int | None) -> float | None:
    if covered is None or total in (None, 0):
        return None
    return (covered / total) * 100


def _service_id_for_report(
    report: CoverageReport,
    service_path_prefixes: Mapping[str, str] | None,
) -> str | None:
    matched_services = [
        attribute_service_from_path(file_record.file_path, service_path_prefixes)
        for file_record in report.files
    ]
    services = [service for service in matched_services if service]
    if not services:
        return None
    return Counter(services).most_common(1)[0][0]


async def process_coverage_report(
    *,
    repo_id: uuid.UUID,
    run_id: str,
    source: str,
    report_format: str | None = None,
    commit_hash: str | None = None,
    branch: str | None = None,
    pr_number: int | None = None,
    team_id: str | None = None,
    org_id: str = "",
    service_path_prefixes: Mapping[str, str] | None = None,
) -> CoverageSnapshotRow:
    report = parse_coverage_report(source, report_format=report_format)
    lines_total = report.lines_total
    lines_covered = report.lines_covered
    branches_total = report.branches_total
    branches_covered = report.branches_covered

    return CoverageSnapshotRow(
        repo_id=repo_id,
        run_id=run_id,
        snapshot_id=build_snapshot_id(run_id, report.report_format),
        report_format=report.report_format,
        lines_total=lines_total,
        lines_covered=lines_covered,
        line_coverage_pct=_coverage_pct(lines_covered, lines_total),
        branches_total=branches_total,
        branches_covered=branches_covered,
        branch_coverage_pct=_coverage_pct(branches_covered, branches_total),
        functions_total=report.functions_total,
        functions_covered=report.functions_covered,
        commit_hash=commit_hash,
        branch=branch,
        pr_number=pr_number,
        team_id=team_id,
        service_id=_service_id_for_report(report, service_path_prefixes),
        org_id=org_id,
    )
