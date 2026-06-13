"""Tests for CHAOS-2370 TestOps ingestion (GitHub/GitLab → real test data).

Covers the pure-logic surface that doesn't need a live provider or ClickHouse:
- XML hardening (defusedxml: DTD/entity attacks blocked) + size cap
- safe ZIP reading (zip-slip / oversized members rejected)
- GitLab native test_report JSON → coherent rows (success→passed)
- report classification + coverage-coherence guard
- the new ``tests`` sync-target wiring (flags + admin targets)
- IngestionSink passthroughs delegate to the store
"""

from __future__ import annotations

import io
import zipfile
from uuid import uuid4

import pytest

from dev_health_ops.connectors.utils.safe_archive import iter_zip_members
from dev_health_ops.parsers import junit as junit_parser
from dev_health_ops.parsers.junit import parse_junit_xml
from dev_health_ops.processors.testops_ingest import (
    _coverage_is_coherent,
    classify_report,
    ingest_report_members,
)
from dev_health_ops.processors.testops_tests import process_gitlab_test_report


# --------------------------------------------------------------------------- #
# XML hardening                                                               #
# --------------------------------------------------------------------------- #
def test_junit_parses_plain_report() -> None:
    suites = parse_junit_xml(
        '<testsuite name="s" time="1.0">'
        '<testcase name="a" time="0.5"/>'
        '<testcase name="b" time="0.5"><failure message="x"/></testcase>'
        "</testsuite>"
    )
    assert len(suites) == 1
    assert suites[0].total_count == 2
    assert suites[0].passed_count == 1
    assert suites[0].failed_count == 1


def test_junit_rejects_dtd_and_entities() -> None:
    """An XXE/entity payload must not be parsed (defusedxml, forbid_dtd)."""
    xxe = (
        '<?xml version="1.0"?>'
        '<!DOCTYPE t [<!ENTITY x SYSTEM "file:///etc/passwd">]>'
        '<testsuite name="s"><testcase name="a">&x;</testcase></testsuite>'
    )
    with pytest.raises(Exception):
        parse_junit_xml(xxe)


def test_junit_enforces_size_cap(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(junit_parser, "_MAX_XML_BYTES", 16)
    with pytest.raises(ValueError):
        parse_junit_xml('<testsuite name="way-too-long-to-fit"/>')


# --------------------------------------------------------------------------- #
# Safe ZIP reading                                                            #
# --------------------------------------------------------------------------- #
def _zip(entries: dict[str, str]) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        for name, content in entries.items():
            archive.writestr(name, content)
    return buffer.getvalue()


def test_zip_reader_rejects_traversal_and_filters() -> None:
    data = _zip(
        {
            "results/report.xml": "<testsuite/>",
            "../escape.xml": "<evil/>",
            "/abs.xml": "<evil/>",
            "notes.txt": "ignore me",
        }
    )
    members = dict(iter_zip_members(data, name_filter=lambda n: n.endswith(".xml")))
    assert "results/report.xml" in members
    assert "../escape.xml" not in members
    assert "/abs.xml" not in members
    assert "notes.txt" not in members  # filtered by name_filter


def test_zip_reader_skips_oversized_members() -> None:
    data = _zip({"big.xml": "A" * 5000})
    members = list(
        iter_zip_members(
            data, name_filter=lambda n: n.endswith(".xml"), max_file_bytes=100
        )
    )
    assert members == []


# --------------------------------------------------------------------------- #
# GitLab native test_report JSON                                              #
# --------------------------------------------------------------------------- #
@pytest.mark.asyncio
async def test_gitlab_test_report_maps_to_coherent_rows() -> None:
    report = {
        "total_count": 3,
        "test_suites": [
            {
                "name": "rspec",
                "total_time": 1.5,
                "test_cases": [
                    {
                        "name": "p",
                        "classname": "A",
                        "execution_time": 0.5,
                        "status": "success",
                    },
                    {
                        "name": "f",
                        "classname": "A",
                        "execution_time": 0.7,
                        "status": "failed",
                        "stack_trace": "boom",
                    },
                    {
                        "name": "s",
                        "classname": "B",
                        "execution_time": 0.0,
                        "status": "skipped",
                    },
                ],
            },
            {"name": "empty", "test_cases": []},  # dropped — no cases
        ],
    }
    repo_id = uuid4()
    suites, cases = await process_gitlab_test_report(
        repo_id=repo_id, run_id="999", report=report, org_id="org-1"
    )

    assert len(suites) == 1, "empty suite is dropped like the XML path"
    suite = suites[0]
    # success → passed; counts derived from cases (coherence invariant).
    assert suite["total_count"] == 3
    assert suite["passed_count"] == 1
    assert suite["failed_count"] == 1
    assert suite["skipped_count"] == 1
    assert (
        suite["passed_count"]
        + suite["failed_count"]
        + suite["skipped_count"]
        + suite.get("error_count", 0)
        == suite["total_count"]
    )
    assert suite["framework"] == "gitlab_ci"
    assert suite["org_id"] == "org-1"
    # Linkage: every row's run_id matches the pipeline run.
    assert suite["run_id"] == "999"
    assert len(cases) == 3
    assert {c["status"] for c in cases} == {"passed", "failed", "skipped"}
    assert all(c["run_id"] == "999" for c in cases)


# --------------------------------------------------------------------------- #
# Classification + coverage coherence                                         #
# --------------------------------------------------------------------------- #
def test_classify_report() -> None:
    assert classify_report("junit.xml", "<testsuite name='x'/>") == "junit"
    assert classify_report("r.xml", "<testsuites><testsuite/></testsuites>") == "junit"
    assert classify_report("cov.xml", "<coverage line-rate='0.5'/>") == "coverage"
    assert classify_report("lcov.info", "TN:\nSF:foo.py\n") == "coverage"
    assert classify_report("readme.txt", "not a report") is None


def test_coverage_coherence_guard() -> None:
    assert _coverage_is_coherent(
        {"lines_total": 100, "lines_covered": 80, "line_coverage_pct": 80.0}  # type: ignore[typeddict-item]
    )
    # covered > total is impossible.
    assert not _coverage_is_coherent(
        {"lines_total": 50, "lines_covered": 80, "line_coverage_pct": 160.0}  # type: ignore[typeddict-item]
    )
    # percentage out of range.
    assert not _coverage_is_coherent({"line_coverage_pct": 142.0})  # type: ignore[typeddict-item]


@pytest.mark.asyncio
async def test_ingest_report_members_routes_junit_and_coverage() -> None:
    members = [
        ("junit.xml", b'<testsuite name="s"><testcase name="a"/></testsuite>'),
        ("cov.xml", b'<coverage line-rate="0.5" lines-valid="10" lines-covered="5"/>'),
        ("noise.txt", b"ignored"),
    ]
    suites, cases, coverage = await ingest_report_members(
        members, repo_id=uuid4(), run_id="42", org_id="org-1"
    )
    assert len(suites) == 1
    assert len(cases) == 1
    assert cases[0]["run_id"] == "42"
    assert len(coverage) == 1


# --------------------------------------------------------------------------- #
# `tests` sync-target wiring                                                   #
# --------------------------------------------------------------------------- #
def test_tests_target_sets_only_sync_tests_flag() -> None:
    from dev_health_ops.processors.sync import _sync_flags_for_target

    flags = _sync_flags_for_target("tests")
    assert flags["sync_tests"] is True
    assert all(not value for key, value in flags.items() if key != "sync_tests"), (
        "tests target must not enable any other sync"
    )


def test_merge_sync_flags_defaults_sync_tests_false() -> None:
    from dev_health_ops.workers.task_utils import _merge_sync_flags

    assert _merge_sync_flags(["tests"])["sync_tests"] is True
    assert _merge_sync_flags(["git"])["sync_tests"] is False
    assert _merge_sync_flags(["cicd"])["sync_tests"] is False


def test_admin_provider_targets_include_tests() -> None:
    from dev_health_ops.api.admin.routers.sync import PROVIDER_SYNC_TARGETS

    assert "tests" in PROVIDER_SYNC_TARGETS["github"]
    assert "tests" in PROVIDER_SYNC_TARGETS["gitlab"]


# --------------------------------------------------------------------------- #
# IngestionSink passthroughs                                                   #
# --------------------------------------------------------------------------- #
@pytest.mark.asyncio
async def test_ingestion_sink_passthroughs_delegate_to_store() -> None:
    from dev_health_ops.metrics.sinks.ingestion import IngestionSink

    class FakeStore:
        def __init__(self) -> None:
            self.calls: list[tuple[str, object]] = []

        async def insert_test_suite_results(self, rows: object) -> None:
            self.calls.append(("suite", rows))

        async def insert_test_case_results(self, rows: object) -> None:
            self.calls.append(("case", rows))

        async def insert_coverage_snapshots(self, rows: object) -> None:
            self.calls.append(("coverage", rows))

    store = FakeStore()
    sink = IngestionSink(store)
    await sink.insert_test_suite_results([{"a": 1}])  # type: ignore[list-item]
    await sink.insert_test_case_results([{"b": 2}])  # type: ignore[list-item]
    await sink.insert_coverage_snapshots([{"c": 3}])  # type: ignore[list-item]
    assert [name for name, _ in store.calls] == ["suite", "case", "coverage"]
