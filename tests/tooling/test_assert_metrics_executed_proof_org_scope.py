"""CHAOS-4775: `family_readback` must be org-scoped, not just repo_id-scoped.

Before this fix, `family_readback` filtered a family's table by
`repo_id IN {live repo ids for org}` and `computed_at >= run_start` only --
no `org_id` predicate. Since `repo_ids` itself already comes from
`live_repo_ids(client, org_id)`, a row that carries the TARGET org's real
repo_id but a foreign (or blank) `org_id` column satisfied the readback even
though the target org's own reader, which filters by org_id directly, would
see zero rows for that family. This is the exact "gate passes while checking
nothing" shape: a regression that drops/mislabels org_id on write would be
reported as a clean, executed proof.

The `FakeClient` below is driven by the QUERY TEXT the function under test
actually issues, not by which behavior we intend to prove: a predicate is
only applied when the SQL literally names it. That is what makes the first
test below fail against ci/assert_metrics_executed_proof.py as it stood on
origin/main (pre-CHAOS-4775) and pass against the fixed module -- captured
as an executed red/green pair in the PR body, not asserted here since the
old signature no longer exists on this branch.
"""

from __future__ import annotations

import importlib.util
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

_SCRIPT = Path(__file__).parents[2] / "ci" / "assert_metrics_executed_proof.py"

ORG_A = "org-11111111-aaaa"
ORG_B = "org-22222222-bbbb"
REPO = "11111111-1111-1111-1111-111111111111"
RUN_START = datetime(2026, 9, 1, tzinfo=timezone.utc)
LATE = RUN_START + timedelta(minutes=5)
EARLY = RUN_START - timedelta(minutes=5)


def _load_module():
    spec = importlib.util.spec_from_file_location(
        "assert_metrics_executed_proof_under_test", _SCRIPT
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class _QueryResult:
    def __init__(self, rows):
        self.result_rows = rows


class FakeClient:
    """ClickHouse-shaped fake, honest to the SQL text it is handed.

    A predicate (org_id / repo_id / computed_at) is applied only when the
    query string literally contains it -- this fake cannot be fooled into
    "simulating the fix"; it reproduces exactly what real ClickHouse would
    do for whatever WHERE clause the function under test builds.
    """

    def __init__(self, repos: dict[str, str], rows: dict[str, list[dict]]):
        self._repos = repos  # repo_id -> org_id, backs live_repo_ids/`repos`
        self._rows = rows  # table -> [{"org_id", "repo_id", "computed_at"}]

    def query(self, sql: str, parameters: dict):
        if " FROM (" in sql and "repos" in sql:
            org_id = parameters["org_id"]
            ids = sorted(rid for rid, oid in self._repos.items() if oid == org_id)
            return _QueryResult([(rid,) for rid in ids])

        table_match = re.search(r"FROM (\w+)\b", sql)
        assert table_match, f"unrecognized query, no source table: {sql}"
        table = table_match.group(1)
        rows = list(self._rows.get(table, []))

        if "org_id = {org_id" in sql:
            rows = [r for r in rows if r["org_id"] == parameters["org_id"]]
        if "repo_id IN {repo_ids" in sql:
            allowed = set(parameters["repo_ids"])
            rows = [r for r in rows if r["repo_id"] in allowed]
        if "computed_at >= {run_start" in sql:
            rows = [r for r in rows if r["computed_at"] >= parameters["run_start"]]

        if "DISTINCT repo_id" in sql:
            return _QueryResult(sorted({(r["repo_id"],) for r in rows}))

        grouped: dict[str, list[dict]] = {}
        for r in rows:
            grouped.setdefault(r["repo_id"], []).append(r)
        out = [
            (repo_id, len(items), max(i["computed_at"] for i in items))
            for repo_id, items in sorted(grouped.items())
        ]
        return _QueryResult(out)


def test_construction_cross_org_rows_do_not_satisfy_readback():
    """CONSTRUCTION (CHAOS-4775): org-A's real repo_id, but every row was
    written by org-B. org-A's readback must come back empty -- org-A's own
    reader would see zero rows for this family."""
    module = _load_module()
    client = FakeClient(
        repos={REPO: ORG_A},
        rows={
            "cicd_metrics_daily": [
                {"org_id": ORG_B, "repo_id": REPO, "computed_at": LATE}
            ]
        },
    )
    repo_ids = module.live_repo_ids(client, ORG_A)
    assert repo_ids == {REPO}

    readback = module.family_readback(
        client, "cicd_metrics_daily", ORG_A, repo_ids, RUN_START
    )

    assert readback == {}, (
        "family_readback must not surface another org's rows for a repo_id "
        f"the target org owns; got {readback}"
    )


def test_correct_org_rows_pass():
    module = _load_module()
    client = FakeClient(
        repos={REPO: ORG_A},
        rows={
            "cicd_metrics_daily": [
                {"org_id": ORG_A, "repo_id": REPO, "computed_at": LATE}
            ]
        },
    )
    repo_ids = module.live_repo_ids(client, ORG_A)

    readback = module.family_readback(
        client, "cicd_metrics_daily", ORG_A, repo_ids, RUN_START
    )

    assert readback == {REPO: {"rows": 1, "latest_computed_at": str(LATE)}}


def test_stale_rows_before_run_start_do_not_satisfy_readback():
    """Sanity companion to the org-scope fix: computed_at gating still holds
    (a row from before this run started is not evidence this run computed
    anything, org_id notwithstanding)."""
    module = _load_module()
    client = FakeClient(
        repos={REPO: ORG_A},
        rows={
            "cicd_metrics_daily": [
                {"org_id": ORG_A, "repo_id": REPO, "computed_at": EARLY}
            ]
        },
    )
    repo_ids = module.live_repo_ids(client, ORG_A)

    readback = module.family_readback(
        client, "cicd_metrics_daily", ORG_A, repo_ids, RUN_START
    )

    assert readback == {}


class _FakeSink:
    """Stands in for ClickHouseMetricsSink: same `.client` / `.close()` shape."""

    def __init__(self, client):
        self.client = client

    def close(self) -> None:
        pass


def _run_cli(
    monkeypatch: pytest.MonkeyPatch, client: FakeClient, families: list[str]
) -> int:
    module = _load_module()
    monkeypatch.setattr(module, "ClickHouseMetricsSink", lambda uri: _FakeSink(client))
    argv = [
        "assert_metrics_executed_proof.py",
        "--clickhouse-uri",
        "fake://unused",
        "--org-id",
        ORG_A,
        "--run-start",
        RUN_START.isoformat(),
        "--families",
        *families,
    ]
    monkeypatch.setattr(sys, "argv", argv)
    return module.main()


def test_cli_zero_rows_for_target_org_fails(monkeypatch, capsys):
    """Negative control: the org's repo exists, but the family never wrote
    for it. Must FAIL -- this is the CHAOS-4263 shape the oracle exists for."""
    client = FakeClient(repos={REPO: ORG_A}, rows={"cicd_metrics_daily": []})

    exit_code = _run_cli(monkeypatch, client, ["cicd"])

    assert exit_code == 1
    err = capsys.readouterr().err
    assert "zero_rows_with_source_data" in err


def test_cli_cross_org_only_rows_fail(monkeypatch, capsys):
    """Negative control (CHAOS-4775): only a foreign org wrote to the
    target org's repo_id. Must FAIL end-to-end through the CLI, not just at
    the family_readback unit level -- this is the gate's actual entry point."""
    client = FakeClient(
        repos={REPO: ORG_A},
        rows={
            "cicd_metrics_daily": [
                {"org_id": ORG_B, "repo_id": REPO, "computed_at": LATE}
            ]
        },
    )

    exit_code = _run_cli(monkeypatch, client, ["cicd"])

    assert exit_code == 1
    err = capsys.readouterr().err
    assert "zero_rows_with_source_data" in err


def test_cli_correct_org_rows_pass(monkeypatch, capsys):
    """Positive control: same shape, but the org's own repo_id, own org_id.
    Must PASS -- proves the org_id predicate does not overreach."""
    client = FakeClient(
        repos={REPO: ORG_A},
        rows={
            "cicd_metrics_daily": [
                {"org_id": ORG_A, "repo_id": REPO, "computed_at": LATE}
            ]
        },
    )

    exit_code = _run_cli(monkeypatch, client, ["cicd"])

    assert exit_code == 0
    out = capsys.readouterr().out
    assert '"org_id": "org-11111111-aaaa"' in out
