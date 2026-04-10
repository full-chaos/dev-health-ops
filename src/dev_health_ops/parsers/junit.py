from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from xml.etree import ElementTree as ET

CANONICAL_TEST_STATUSES = {"passed", "failed", "skipped", "error", "quarantined"}


@dataclass(frozen=True)
class ParsedTestCase:
    case_name: str
    class_name: str | None
    duration_seconds: float | None
    status: str
    failure_message: str | None = None
    failure_type: str | None = None
    stack_trace: str | None = None
    system_out: str | None = None
    system_err: str | None = None
    file_path: str | None = None


@dataclass(frozen=True)
class ParsedTestSuite:
    suite_name: str
    framework: str | None
    duration_seconds: float | None
    started_at: datetime | None
    finished_at: datetime | None
    total_count: int
    passed_count: int
    failed_count: int
    skipped_count: int
    error_count: int
    quarantined_count: int
    system_out: str | None = None
    system_err: str | None = None
    file_path: str | None = None
    cases: list[ParsedTestCase] = field(default_factory=list)


def _read_text(source: str | bytes | Path) -> str:
    if isinstance(source, Path):
        return source.read_text(encoding="utf-8")
    if isinstance(source, bytes):
        return source.decode("utf-8")
    path = Path(source)
    if path.exists():
        return path.read_text(encoding="utf-8")
    return source


def _safe_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _parse_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    normalized = value.strip().replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(normalized)
    except ValueError:
        return None


def _normalized_text(element: ET.Element | None) -> str | None:
    if element is None:
        return None
    text = "".join(element.itertext()).strip()
    return text or None


def _looks_quarantined(message: str | None, failure_type: str | None) -> bool:
    haystack = " ".join(part for part in [message, failure_type] if part).lower()
    return any(token in haystack for token in ("quarantine", "quarantined", "xfail"))


def _canonical_status(testcase: ET.Element) -> tuple[str, ET.Element | None]:
    failure = testcase.find("failure")
    error = testcase.find("error")
    skipped = testcase.find("skipped")
    if skipped is not None:
        skipped_message = skipped.get("message") or _normalized_text(skipped)
        skipped_type = skipped.get("type")
        if _looks_quarantined(skipped_message, skipped_type):
            return "quarantined", skipped
        return "skipped", skipped
    if failure is not None:
        message = failure.get("message") or _normalized_text(failure)
        failure_type = failure.get("type")
        if _looks_quarantined(message, failure_type):
            return "quarantined", failure
        return "failed", failure
    if error is not None:
        message = error.get("message") or _normalized_text(error)
        error_type = error.get("type")
        if _looks_quarantined(message, error_type):
            return "quarantined", error
        return "error", error
    return "passed", None


def _infer_framework(suite: ET.Element, cases: list[ParsedTestCase]) -> str | None:
    candidates = [suite.get("framework"), suite.get("runner"), suite.get("hostname")]
    for case in cases:
        if case.file_path and (
            case.file_path.endswith(".spec.js")
            or case.file_path.endswith(".spec.ts")
            or case.file_path.endswith(".test.js")
            or case.file_path.endswith(".test.ts")
        ):
            return "jest"
        if case.class_name and "::" in case.class_name:
            return "pytest"
        if case.file_path and case.file_path.endswith(".py"):
            return "pytest"
    for candidate in candidates:
        if not candidate:
            continue
        value = candidate.lower()
        if "jest" in value:
            return "jest"
        if "pytest" in value:
            return "pytest"
    return "junit"


def parse_junit_xml(source: str | bytes | Path) -> list[ParsedTestSuite]:
    root = ET.fromstring(_read_text(source))
    suites: list[ParsedTestSuite] = []

    candidate_suites: list[ET.Element] = []
    if root.tag == "testsuite":
        candidate_suites.append(root)
    candidate_suites.extend(root.findall(".//testsuite"))

    seen_ids: set[int] = set()
    for suite in candidate_suites:
        suite_id = id(suite)
        if suite_id in seen_ids:
            continue
        seen_ids.add(suite_id)

        testcases = [child for child in suite.findall("testcase")]
        if not testcases:
            continue

        parsed_cases: list[ParsedTestCase] = []
        for testcase in testcases:
            status, detail = _canonical_status(testcase)
            if status not in CANONICAL_TEST_STATUSES:
                status = "error"

            parsed_cases.append(
                ParsedTestCase(
                    case_name=testcase.get("name") or "unnamed",
                    class_name=testcase.get("classname"),
                    duration_seconds=_safe_float(testcase.get("time")),
                    status=status,
                    failure_message=detail.get("message")
                    if detail is not None
                    else None,
                    failure_type=detail.get("type") if detail is not None else None,
                    stack_trace=_normalized_text(detail),
                    system_out=_normalized_text(testcase.find("system-out")),
                    system_err=_normalized_text(testcase.find("system-err")),
                    file_path=testcase.get("file") or suite.get("file"),
                )
            )

        counts = {status: 0 for status in CANONICAL_TEST_STATUSES}
        for case in parsed_cases:
            counts[case.status] += 1

        started_at = _parse_timestamp(suite.get("timestamp"))
        duration_seconds = _safe_float(suite.get("time"))
        finished_at = (
            started_at + timedelta(seconds=duration_seconds)
            if started_at is not None and duration_seconds is not None
            else None
        )

        suites.append(
            ParsedTestSuite(
                suite_name=suite.get("name") or "unnamed",
                framework=_infer_framework(suite, parsed_cases),
                duration_seconds=duration_seconds,
                started_at=started_at,
                finished_at=finished_at,
                total_count=len(parsed_cases),
                passed_count=counts["passed"],
                failed_count=counts["failed"],
                skipped_count=counts["skipped"],
                error_count=counts["error"],
                quarantined_count=counts["quarantined"],
                system_out=_normalized_text(suite.find("system-out")),
                system_err=_normalized_text(suite.find("system-err")),
                file_path=suite.get("file")
                or next(
                    (case.file_path for case in parsed_cases if case.file_path), None
                ),
                cases=parsed_cases,
            )
        )

    return suites
