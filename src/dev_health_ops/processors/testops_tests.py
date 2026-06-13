from __future__ import annotations

import hashlib
import uuid
from collections.abc import Mapping
from datetime import datetime
from typing import Any

from dev_health_ops.metrics.testops_schemas import TestCaseResultRow, TestSuiteResultRow
from dev_health_ops.parsers.junit import (
    CANONICAL_TEST_STATUSES,
    ParsedTestCase,
    ParsedTestSuite,
    parse_junit_xml,
)

# GitLab's native test_report API reports case status as one of these strings;
# map them onto our canonical statuses (notably success → passed) so the
# GitLab JSON path produces the same vocabulary as the JUnit XML path.
_GITLAB_STATUS_MAP = {
    "success": "passed",
    "failed": "failed",
    "error": "error",
    "skipped": "skipped",
}


def _hash_identifier(*parts: str | None) -> str:
    payload = "::".join(part or "" for part in parts)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def build_suite_id(run_id: str, suite_name: str, environment: str | None) -> str:
    return _hash_identifier(run_id, suite_name, environment)


def build_case_id(suite_id: str, case_name: str) -> str:
    return _hash_identifier(suite_id, case_name)


def attribute_service_from_path(
    file_path: str | None,
    service_path_prefixes: Mapping[str, str] | None = None,
) -> str | None:
    if not file_path:
        return None

    normalized_path = file_path.replace("\\", "/").lstrip("./")
    if service_path_prefixes:
        best_match: tuple[int, str] | None = None
        for prefix, service_id in service_path_prefixes.items():
            normalized_prefix = prefix.replace("\\", "/").rstrip("/")
            if normalized_path == normalized_prefix or normalized_path.startswith(
                f"{normalized_prefix}/"
            ):
                match = (len(normalized_prefix), service_id)
                if best_match is None or match[0] > best_match[0]:
                    best_match = match
        if best_match is not None:
            return best_match[1]

    parts = [part for part in normalized_path.split("/") if part]
    for marker in ("services", "apps", "packages"):
        if marker in parts:
            index = parts.index(marker)
            if index + 1 < len(parts):
                return parts[index + 1]
    return parts[0] if parts else None


def _stack_trace(case: ParsedTestCase) -> str | None:
    trace_parts = [
        part for part in [case.stack_trace, case.system_err, case.system_out] if part
    ]
    if not trace_parts:
        return None
    return "\n".join(trace_parts)[:4096]


def _suite_file_path(suite: ParsedTestSuite) -> str | None:
    if suite.file_path:
        return suite.file_path
    return next((case.file_path for case in suite.cases if case.file_path), None)


def _coerce_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _parsed_suites_from_gitlab_report(
    report: Mapping[str, Any],
) -> list[ParsedTestSuite]:
    """Map GitLab's native pipeline ``test_report`` JSON onto ParsedTestSuite.

    GitLab returns parsed JUnit results as JSON (no XML), so this normalizes the
    JSON shape into the same intermediate model the XML parser emits. Suite
    counts are derived from the mapped cases (not GitLab's declared counts) so
    the coherence invariant ``passed+failed+skipped+error == total`` always
    holds downstream. Empty suites are dropped, mirroring the XML path.
    """
    parsed: list[ParsedTestSuite] = []
    for suite in report.get("test_suites", []) or []:
        cases: list[ParsedTestCase] = []
        for case in suite.get("test_cases", []) or []:
            raw_status = str(case.get("status") or "").lower()
            status = _GITLAB_STATUS_MAP.get(raw_status, "error")
            if status not in CANONICAL_TEST_STATUSES:
                status = "error"
            stack_trace = case.get("stack_trace") or case.get("system_output")
            cases.append(
                ParsedTestCase(
                    case_name=case.get("name") or "unnamed",
                    class_name=case.get("classname"),
                    duration_seconds=_coerce_float(case.get("execution_time")),
                    status=status,
                    failure_message=None,
                    failure_type=None,
                    stack_trace=stack_trace,
                    system_out=case.get("system_output"),
                    system_err=None,
                    file_path=None,
                )
            )
        if not cases:
            continue
        counts = {status: 0 for status in CANONICAL_TEST_STATUSES}
        for case_ in cases:
            counts[case_.status] += 1
        parsed.append(
            ParsedTestSuite(
                suite_name=suite.get("name") or "unnamed",
                framework="gitlab_ci",
                duration_seconds=_coerce_float(suite.get("total_time")),
                started_at=None,
                finished_at=None,
                total_count=len(cases),
                passed_count=counts["passed"],
                failed_count=counts["failed"],
                skipped_count=counts["skipped"],
                error_count=counts["error"],
                quarantined_count=counts["quarantined"],
                cases=cases,
            )
        )
    return parsed


def _build_rows_from_parsed(
    parsed_suites: list[ParsedTestSuite],
    *,
    repo_id: uuid.UUID,
    run_id: str,
    environment: str | None = None,
    framework: str | None = None,
    team_id: str | None = None,
    org_id: str = "",
    service_path_prefixes: Mapping[str, str] | None = None,
    fallback_started_at: datetime | None = None,
    fallback_finished_at: datetime | None = None,
) -> tuple[list[TestSuiteResultRow], list[TestCaseResultRow]]:
    """Build insert-ready suite/case rows from parsed suites.

    Shared by the JUnit-XML path (``process_test_report``) and the GitLab-JSON
    path (``process_gitlab_test_report``) so both emit identical row shapes and
    coherence guarantees.

    ``fallback_started_at`` / ``fallback_finished_at`` (typically the CI run's
    timestamps) are used when a suite carries no timestamps of its own. The
    daily loader windows suites by ``coalesce(started_at, finished_at)``, so a
    suite with null timestamps (e.g. every GitLab ``test_report`` suite, or a
    JUnit file without a ``timestamp`` attribute) would otherwise be invisible
    to the rollup (CHAOS-2370).
    """
    suite_rows: list[TestSuiteResultRow] = []
    case_rows: list[TestCaseResultRow] = []

    for suite in parsed_suites:
        suite_id = build_suite_id(run_id, suite.suite_name, environment)
        service_id = attribute_service_from_path(
            _suite_file_path(suite),
            service_path_prefixes,
        )
        suite_rows.append(
            TestSuiteResultRow(
                repo_id=repo_id,
                run_id=run_id,
                suite_id=suite_id,
                suite_name=suite.suite_name,
                framework=framework or suite.framework,
                environment=environment,
                total_count=suite.total_count,
                passed_count=suite.passed_count,
                failed_count=suite.failed_count,
                skipped_count=suite.skipped_count,
                error_count=suite.error_count,
                quarantined_count=suite.quarantined_count,
                retried_count=0,
                duration_seconds=suite.duration_seconds,
                started_at=suite.started_at or fallback_started_at,
                finished_at=suite.finished_at or fallback_finished_at,
                team_id=team_id,
                service_id=service_id,
                org_id=org_id,
            )
        )

        for case in suite.cases:
            case_rows.append(
                TestCaseResultRow(
                    repo_id=repo_id,
                    run_id=run_id,
                    suite_id=suite_id,
                    case_id=build_case_id(suite_id, case.case_name),
                    case_name=case.case_name,
                    class_name=case.class_name,
                    status=case.status,
                    duration_seconds=case.duration_seconds,
                    retry_attempt=0,
                    failure_message=case.failure_message,
                    failure_type=case.failure_type,
                    stack_trace=_stack_trace(case),
                    is_quarantined=case.status == "quarantined",
                    org_id=org_id,
                )
            )

    return suite_rows, case_rows


async def process_test_report(
    *,
    repo_id: uuid.UUID,
    run_id: str,
    source: str,
    environment: str | None = None,
    framework: str | None = None,
    team_id: str | None = None,
    org_id: str = "",
    service_path_prefixes: Mapping[str, str] | None = None,
    started_at: datetime | None = None,
    finished_at: datetime | None = None,
) -> tuple[list[TestSuiteResultRow], list[TestCaseResultRow]]:
    """Parse a JUnit XML report (from a CI artifact) into insert-ready rows.

    ``started_at`` / ``finished_at`` are the CI run's timestamps, used to date
    suites that carry no ``timestamp`` of their own (see _build_rows_from_parsed).
    """
    parsed_suites = parse_junit_xml(source)
    return _build_rows_from_parsed(
        parsed_suites,
        repo_id=repo_id,
        run_id=run_id,
        environment=environment,
        framework=framework,
        team_id=team_id,
        org_id=org_id,
        service_path_prefixes=service_path_prefixes,
        fallback_started_at=started_at,
        fallback_finished_at=finished_at,
    )


async def process_gitlab_test_report(
    *,
    repo_id: uuid.UUID,
    run_id: str,
    report: Mapping[str, Any],
    environment: str | None = None,
    team_id: str | None = None,
    org_id: str = "",
    service_path_prefixes: Mapping[str, str] | None = None,
    started_at: datetime | None = None,
    finished_at: datetime | None = None,
) -> tuple[list[TestSuiteResultRow], list[TestCaseResultRow]]:
    """Map GitLab's native pipeline ``test_report`` JSON into insert-ready rows.

    Unlike ``process_test_report`` (JUnit XML), this consumes the parsed JSON
    GitLab returns from ``GET /projects/:id/pipelines/:id/test_report`` — no XML
    parsing, so no artifact download is needed for GitLab pass/fail/duration.

    GitLab's test_report carries no per-suite timestamps, so ``started_at`` /
    ``finished_at`` (the pipeline's timestamps) are REQUIRED for the suites to
    fall inside the daily rollup's window (CHAOS-2370).
    """
    parsed_suites = _parsed_suites_from_gitlab_report(report)
    return _build_rows_from_parsed(
        parsed_suites,
        repo_id=repo_id,
        run_id=run_id,
        environment=environment,
        framework="gitlab_ci",
        team_id=team_id,
        org_id=org_id,
        service_path_prefixes=service_path_prefixes,
        fallback_started_at=started_at,
        fallback_finished_at=finished_at,
    )
