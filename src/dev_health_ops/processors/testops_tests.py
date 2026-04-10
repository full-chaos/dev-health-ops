from __future__ import annotations

import hashlib
import uuid
from collections.abc import Mapping

from dev_health_ops.metrics.testops_schemas import TestCaseResultRow, TestSuiteResultRow
from dev_health_ops.parsers.junit import (
    ParsedTestCase,
    ParsedTestSuite,
    parse_junit_xml,
)


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
) -> tuple[list[TestSuiteResultRow], list[TestCaseResultRow]]:
    parsed_suites = parse_junit_xml(source)
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
                started_at=suite.started_at,
                finished_at=suite.finished_at,
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
