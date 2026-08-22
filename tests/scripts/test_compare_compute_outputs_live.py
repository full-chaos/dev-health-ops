"""End-to-end proof of the compute-parity comparator against live ClickHouse.

This is the acceptance evidence for CHAOS-3092 P0 that no in-memory test can
give: two isolated scratch databases, producer-derived fixtures written through
the production writers, the real Python DORA producer executed twice, and the
same three negative controls the slice is accepted on applied to real rows.

Opt-in, like every ``-m clickhouse`` test: ``ci/local_validate.sh`` does not run
the seeded ClickHouse suite and neither does CI. Run it by hand:

    CLICKHOUSE_URI=clickhouse://ch:ch@localhost:8123/<scratch> \\
      .venv/bin/python -m pytest tests/scripts/test_compare_compute_outputs_live.py -m clickhouse

The test creates and drops its OWN two databases derived from that DSN. It
never writes to the DSN's own database, and it refuses to run at all against
``default``, which holds real dev data.
"""

from __future__ import annotations

import importlib.util
import json
import math
import os
import re
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

pytestmark = pytest.mark.clickhouse

ROOT = Path(__file__).resolve().parents[2]
MANIFEST = ROOT / "contracts/compute-parity/v1/metrics.dora.json"
COMPARATOR = ROOT / "scripts/worker/compare_compute_outputs.py"
FIXTURES = ROOT / "scripts/worker/compute_parity_fixtures.py"
TABLE = "dora_metrics_daily"


def _load(name: str, path: Path) -> Any:
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


comparator = _load("compare_compute_outputs", COMPARATOR)


DATABASE_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _base_dsn() -> str:
    dsn = os.environ.get("CLICKHOUSE_URI", "").strip()
    if not dsn:
        pytest.skip("CLICKHOUSE_URI is required for the live compute-parity proof")
    database = dsn.rsplit("/", 1)[-1].split("?", 1)[0]
    if not DATABASE_NAME.match(database):
        # No usable scratch DSN is configured -- e.g. a compose-style
        # `.../${CLICKHOUSE_DB:-default}` that the shell never expanded. That is
        # "not configured", so it skips; it is NOT the `default` case below.
        pytest.skip(f"CLICKHOUSE_URI names no usable scratch database: {database!r}")
    # Not a skip: a DSN pointed at the real dev database is a caller mistake
    # that must be loud, not quietly tolerated into a green run.
    assert database != "default", (
        "refusing to run the parity proof against the shared dev database; "
        "point CLICKHOUSE_URI at a scratch db"
    )
    return dsn


def _sibling(dsn: str, suffix: str) -> str:
    head, _, database = dsn.rpartition("/")
    return f"{head}/{database.split('?', 1)[0]}_{suffix}"


def _run(argv: list[str]) -> dict[str, Any]:
    completed = subprocess.run(
        [sys.executable, *argv], capture_output=True, text=True, cwd=ROOT
    )
    assert completed.returncode == 0, completed.stderr or completed.stdout
    return json.loads(completed.stdout.strip().splitlines()[-1])


def _compare(left: str, right: str, extra: list[str]) -> tuple[int, dict[str, Any]]:
    completed = subprocess.run(
        [
            sys.executable,
            str(COMPARATOR),
            "rows",
            "--manifest",
            str(MANIFEST),
            "--left-dsn",
            left,
            "--right-dsn",
            right,
            "--left-label",
            "python",
            "--right-label",
            "python_replica",
            *extra,
        ],
        capture_output=True,
        text=True,
        cwd=ROOT,
    )
    assert completed.stdout, completed.stderr
    return completed.returncode, json.loads(completed.stdout)


def _client(dsn: str) -> Any:
    import clickhouse_connect

    return clickhouse_connect.get_client(dsn=dsn)


def _mutate(dsn: str, statement: str) -> None:
    client = _client(dsn)
    try:
        client.command(statement, settings={"mutations_sync": 2})
    finally:
        client.close()


def _reproduce(dsn: str, as_of: str) -> None:
    """Reset the right side's output table and re-run the Python producer."""
    _mutate(dsn, f"TRUNCATE TABLE {TABLE}")
    _run(
        [
            str(FIXTURES),
            "produce",
            "--kind",
            "metrics.dora",
            "--dsn",
            dsn,
            "--as-of",
            as_of,
            "--days",
            "14",
        ]
    )


@pytest.fixture(scope="module")
def destinations() -> Any:
    base = _base_dsn()
    left = _sibling(base, "pl")
    right = _sibling(base, "pr")
    _run([str(FIXTURES), "provision", "--dsn", left, "--reset"])
    _run([str(FIXTURES), "provision", "--dsn", right, "--reset"])
    seeded = _run([str(FIXTURES), "seed", "--kind", "metrics.dora", "--dsn", left])
    assert seeded["deployments"] > 0 and seeded["incidents"] > 0
    cloned = _run(
        [
            str(FIXTURES),
            "clone",
            "--kind",
            "metrics.dora",
            "--from-dsn",
            left,
            "--to-dsn",
            right,
        ]
    )
    assert cloned["tables"]["deployments"] == seeded["deployments"]
    try:
        yield left, right
    finally:
        for dsn in (left, right):
            database = dsn.rsplit("/", 1)[-1]
            head, _, _ = dsn.rpartition("/")
            client = _client(f"{head}/system")
            try:
                client.command(f"DROP DATABASE IF EXISTS {database}")
            finally:
                client.close()


def test_same_implementation_twice_reports_equal_and_the_declared_repeat_policy(
    destinations,
):
    """Self-test: Python against Python on isolated scratch databases."""
    left, right = destinations
    code, report = _compare(
        left,
        right,
        [
            "--right-exec",
            "scripts/worker/compute_parity_fixtures.py produce --kind metrics.dora --days 14",
            "--repeat",
            "2",
            "--as-of",
            "2026-08-22T00:00:00Z",
        ],
    )
    assert code == 0
    assert report["claim"] == comparator.CLAIM_ROWS
    assert report["verdict"] == comparator.VERDICT_EQUAL
    assert report["differences"] == []

    # Inputs were verified before any output was compared.
    assert report["inputs"]["verified"] is True
    assert set(report["inputs"]["tables"]) == {
        "repos",
        "deployments",
        "operational_services",
        "operational_service_repository_mappings",
        "operational_incidents",
    }

    first = report["runs"][0]["tables"][TABLE]
    second = report["runs"][1]["tables"][TABLE]
    assert first["count"]["left"] > 0
    assert first["canonical_row_digest"]["equal"]
    assert first["digest_excluded_fields"] == ["computed_at"]
    assert second["count"]["left"] == first["count"]["left"] * 2

    # dora_metrics_daily is a plain MergeTree and job_dora never deletes, so a
    # replay appends. Both sides must show it, and the manifest must say so.
    assert {entry["side"] for entry in report["repeat"]} == {"python", "python_replica"}
    assert {entry["run"] for entry in report["repeat"]} == {2}
    for entry in report["repeat"]:
        assert entry["observed"] == "append_duplicates"
        assert entry["matches_declared_policy"]
        assert entry["key_set_stable"]


def test_all_four_dora_metrics_are_actually_produced(destinations):
    """A parity run over a table that is empty of a metric proves nothing.

    time_to_restore_service only exists when the canonical incident projection
    resolves; it is the one DORA metric that depends on the seeded service
    mapping being valid, so its absence would silently narrow the comparison.
    """
    left, _ = destinations
    client = _client(left)
    try:
        result = client.query(f"SELECT DISTINCT metric_name FROM {TABLE}")
        produced = {row[0] for row in result.result_rows}
    finally:
        client.close()
    assert produced == {
        "deployment_frequency",
        "lead_time_for_changes",
        "change_failure_rate",
        "time_to_restore_service",
    }


@pytest.fixture
def perturbable(destinations) -> Any:
    """Both sides reset to exactly one clean producer run, then perturb one.

    Resetting BOTH matters: the repeat-policy test above deliberately leaves the
    destinations at two appended runs, and a control that reset only the side it
    perturbs would be comparing 84 rows against 42 and 'detecting' a difference
    it did not inject.
    """
    left, right = destinations
    _reproduce(left, "2026-08-22T00:00:00Z")
    _reproduce(right, "2026-08-22T00:00:00Z")
    code, report = _compare(left, right, ["--no-exec"])
    assert code == 0 and report["verdict"] == comparator.VERDICT_EQUAL, (
        "the control baseline must be EQUAL before a perturbation is injected, "
        "or the control proves nothing"
    )
    yield left, right


def test_negative_control_mutated_row(perturbable):
    left, right = perturbable
    _mutate(
        right,
        f"ALTER TABLE {TABLE} UPDATE value = value + 1 "
        "WHERE metric_name = 'lead_time_for_changes' AND day = "
        f"(SELECT min(day) FROM {TABLE})",
    )
    code, report = _compare(left, right, ["--no-exec"])
    assert code == 1
    assert report["verdict"] == comparator.VERDICT_DIFFERENT
    table = report["runs"][0]["tables"][TABLE]
    assert table["count"]["equal"] is True
    assert table["key_set_digest"]["equal"] is True
    assert table["canonical_row_digest"]["equal"] is False
    mutated = [d for d in report["differences"] if d["shape"] == "row_mutated"]
    assert len(mutated) == 1
    assert mutated[0]["semantic_key"]["metric_name"] == "s:lead_time_for_changes"
    assert mutated[0]["fields"][0]["column"] == "value"


def test_negative_control_dropped_row(perturbable):
    left, right = perturbable
    _mutate(
        right,
        f"ALTER TABLE {TABLE} DELETE WHERE metric_name = 'change_failure_rate' "
        f"AND day = (SELECT min(day) FROM {TABLE})",
    )
    code, report = _compare(left, right, ["--no-exec"])
    assert code == 1
    table = report["runs"][0]["tables"][TABLE]
    assert table["count"]["left"] == table["count"]["right"] + 1
    assert table["key_set_digest"]["equal"] is False
    counts = [d for d in report["differences"] if d["shape"] == "count_mismatch"]
    missing = [d for d in report["differences"] if d["shape"] == "row_missing_on_right"]
    assert len(counts) == 1
    assert len(missing) == 1
    assert missing[0]["semantic_key"]["metric_name"] == "s:change_failure_rate"


def test_negative_control_float_nudged_past_policy(perturbable):
    """One ULP, which every level except the row digest reports as identical."""
    left, right = perturbable
    client = _client(right)
    try:
        original = client.query(
            f"SELECT value FROM {TABLE} WHERE metric_name = 'time_to_restore_service' "
            "ORDER BY day LIMIT 1"
        ).result_rows[0][0]
    finally:
        client.close()
    nudged = math.nextafter(float(original), math.inf)
    assert nudged != float(original)
    _mutate(
        right,
        f"ALTER TABLE {TABLE} UPDATE value = {nudged!r} "
        f"WHERE metric_name = 'time_to_restore_service' AND value = {float(original)!r}",
    )
    code, report = _compare(left, right, ["--no-exec"])
    assert code == 1
    table = report["runs"][0]["tables"][TABLE]
    assert table["count"]["equal"] is True
    assert table["key_set_digest"]["equal"] is True
    assert table["canonical_row_digest"]["equal"] is False
    mutated = [d for d in report["differences"] if d["shape"] == "row_mutated"]
    assert mutated
    assert mutated[0]["fields"][0]["right"] == repr(nudged)


def test_two_empty_output_tables_are_indeterminate_not_equal(perturbable):
    """Absence of evidence must not read as parity.

    Two empty tables have equal counts and equal digests at every level. Before
    this was closed, a fixture that produced nothing -- or a projection that
    matched nothing on both sides -- reported EQUAL with exit code 0.
    """
    left, right = perturbable
    for dsn in (left, right):
        _mutate(dsn, f"TRUNCATE TABLE {TABLE}")
    code, report = _compare(left, right, ["--no-exec"])
    assert code == 3
    assert report["verdict"] == comparator.VERDICT_INDETERMINATE
    assert report["reason"] == f"output_empty_on_both_sides:{TABLE}"


def test_a_pinned_clock_manifest_refuses_to_run_a_producer_without_as_of(destinations):
    """metrics.dora declares a pinned run day; the producer must be handed one."""
    left, right = destinations
    completed = subprocess.run(
        [
            sys.executable,
            str(COMPARATOR),
            "rows",
            "--manifest",
            str(MANIFEST),
            "--left-dsn",
            left,
            "--right-dsn",
            right,
            "--repeat",
            "1",
        ],
        capture_output=True,
        text=True,
        cwd=ROOT,
    )
    assert completed.returncode == 2
    assert "as_of_required_for_clock_policy" in completed.stderr
