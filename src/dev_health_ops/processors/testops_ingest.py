"""Shared TestOps report-ingestion helpers (CHAOS-2370).

GitHub and GitLab both end up with a set of in-memory report files (JUnit XML
and coverage XML) extracted from CI artifacts. This module classifies those
files and turns them into insert-ready rows, reusing the canonical
``process_test_report`` / ``process_coverage_report`` builders so both providers
emit identical row shapes and coherence guarantees.

Volume caps live here (and in ``connectors.utils.safe_archive``) so a single
sync can't download/parse an unbounded number of artifacts.
"""

from __future__ import annotations

import logging
import uuid
from collections.abc import Iterable

from dev_health_ops.metrics.testops_schemas import (
    CoverageSnapshotRow,
    TestCaseResultRow,
    TestSuiteResultRow,
)
from dev_health_ops.processors.testops_coverage import process_coverage_report
from dev_health_ops.processors.testops_tests import process_test_report

logger = logging.getLogger(__name__)

# Per-sync volume caps (Codex review item D). Bound how much artifact work a
# single repo sync performs so we never blow CI-provider rate limits or the
# sync duration budget. Operators backfill wider windows in separate runs.
MAX_RUNS_PER_SYNC = 200  # workflow runs / pipelines scanned per repo per sync
MAX_ARTIFACTS_PER_RUN = 25  # artifact archives downloaded per run
MAX_REPORTS_PER_RUN = 200  # report files parsed per run

_JUNIT_ROOT_MARKERS = ("<testsuite", "<testsuites")
_COVERAGE_MARKERS = (
    "<coverage",
    "<cobertura",
    "clover",
    "<report",
)  # cobertura/clover/jacoco


def classify_report(filename: str, text: str) -> str | None:
    """Classify a report file as ``"junit"``, ``"coverage"``, or ``None``.

    Content sniffing (not just extension) so a misnamed file still routes
    correctly. The first KB is enough to find the root element.
    """
    head = text[:2048].lower()
    if any(marker in head for marker in _JUNIT_ROOT_MARKERS):
        return "junit"
    lowered_name = filename.lower()
    if lowered_name.endswith(".info"):
        return "coverage"  # lcov
    if any(marker in head for marker in _COVERAGE_MARKERS):
        return "coverage"
    return None


def _coverage_is_coherent(row: CoverageSnapshotRow) -> bool:
    """Reject coverage rows with impossible values (Codex review item I).

    Real reports occasionally emit malformed totals; a row where covered >
    total or a percentage outside [0, 100] would skew the rollup, so we drop it
    rather than store it. (We do NOT enforce branch<=line — that's a legitimate
    real-world shape, only a fixture convention.)
    """
    lines_total = row.get("lines_total")
    lines_covered = row.get("lines_covered")
    if (
        lines_total is not None
        and lines_covered is not None
        and lines_covered > lines_total
    ):
        return False
    for pct in (row.get("line_coverage_pct"), row.get("branch_coverage_pct")):
        if isinstance(pct, (int, float)) and not (0.0 <= pct <= 100.0):
            return False
    return True


async def ingest_report_members(
    members: Iterable[tuple[str, bytes]],
    *,
    repo_id: uuid.UUID,
    run_id: str,
    org_id: str,
    team_id: str | None = None,
) -> tuple[
    list[TestSuiteResultRow], list[TestCaseResultRow], list[CoverageSnapshotRow]
]:
    """Parse extracted artifact members into insert-ready rows.

    ``run_id`` MUST match the pipeline run's id so suite/case/coverage rows join
    to the pipeline. A single malformed report is skipped (logged) rather than
    aborting the rest.
    """
    suite_rows: list[TestSuiteResultRow] = []
    case_rows: list[TestCaseResultRow] = []
    coverage_rows: list[CoverageSnapshotRow] = []

    processed = 0
    for filename, content in members:
        if processed >= MAX_REPORTS_PER_RUN:
            logger.warning(
                "Hit per-run report cap (%d) for run %s; skipping remainder",
                MAX_REPORTS_PER_RUN,
                run_id,
            )
            break
        try:
            text = content.decode("utf-8", errors="replace")
        except Exception:  # pragma: no cover - decode is defensive
            continue
        kind = classify_report(filename, text)
        if kind is None:
            continue
        processed += 1
        try:
            if kind == "junit":
                suites, cases = await process_test_report(
                    repo_id=repo_id,
                    run_id=run_id,
                    source=text,
                    team_id=team_id,
                    org_id=org_id,
                )
                suite_rows.extend(suites)
                case_rows.extend(cases)
            else:  # coverage
                coverage = await process_coverage_report(
                    repo_id=repo_id,
                    run_id=run_id,
                    source=text,
                    team_id=team_id,
                    org_id=org_id,
                )
                if _coverage_is_coherent(coverage):
                    coverage_rows.append(coverage)
                else:
                    logger.warning(
                        "Dropping incoherent coverage row from %s (run %s)",
                        filename,
                        run_id,
                    )
        except Exception as exc:
            logger.warning(
                "Failed to parse %s report %s (run %s): %s",
                kind,
                filename,
                run_id,
                exc,
            )
            continue

    return suite_rows, case_rows, coverage_rows
