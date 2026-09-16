"""compounding_risk_team's readback: the SCOPE_ID_TEAM_FAMILIES shape.

compounding_risk_team writes compounding_risk_daily, the SAME table
compounding_risk's SCOPE_ID_REPO_FAMILIES entry reads, but at scope='team'
rather than scope='repo' -- scope_id there is a team_id (String), not a
repo_id, so it needs its own readback (scope_id_team_readback) rather than
scope_id_repo_readback's live_repo_ids cross-check, which would misread every
legitimate team_id as a dead repo id.
"""

from __future__ import annotations

import importlib.util
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

_SCRIPT = Path(__file__).parents[2] / "ci" / "assert_metrics_executed_proof.py"

ORG_A = "org-66666666-dddd"
ORG_B = "org-77777777-eeee"
TEAM = "team-alpha"
RUN_START = datetime(2026, 9, 1, tzinfo=timezone.utc)
LATE = RUN_START + timedelta(minutes=5)
EARLY = RUN_START - timedelta(minutes=5)


def _load_module():
    spec = importlib.util.spec_from_file_location(
        "assert_metrics_executed_proof_under_test_scope_team", _SCRIPT
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class _QueryResult:
    def __init__(self, rows):
        self.result_rows = rows


class _FakeClient:
    """Honest to the SQL text scope_id_team_readback issues: a predicate is
    applied only when the query string literally names it, same discipline as
    the org-scope fake this mirrors."""

    def __init__(self, rows: list[dict]):
        self._rows = rows  # {"org_id", "scope", "scope_id", "computed_at"}

    def query(self, sql: str, parameters: dict):
        assert "GROUP BY scope_id" in sql, f"unexpected query shape: {sql}"
        rows = list(self._rows)
        if "org_id = {org_id" in sql:
            rows = [r for r in rows if r["org_id"] == parameters["org_id"]]
        if "scope = 'team'" in sql:
            rows = [r for r in rows if r["scope"] == "team"]
        if "computed_at >= {run_start" in sql:
            rows = [r for r in rows if r["computed_at"] >= parameters["run_start"]]

        grouped: dict[str, list[dict]] = {}
        for r in rows:
            grouped.setdefault(r["scope_id"], []).append(r)
        out = [
            (scope_id, len(items), max(i["computed_at"] for i in items))
            for scope_id, items in sorted(grouped.items())
        ]
        return _QueryResult(out)


def test_scope_id_team_readback_excludes_repo_scope_rows():
    """CONSTRUCTION: the table holds a repo-scope row for the SAME org --
    scope_id_team_readback must count only the team-scope row, not fall back
    to counting every row in the table regardless of scope."""
    module = _load_module()
    client = _FakeClient(
        [
            {
                "org_id": ORG_A,
                "scope": "repo",
                "scope_id": "some-repo",
                "computed_at": LATE,
            },
            {"org_id": ORG_A, "scope": "team", "scope_id": TEAM, "computed_at": LATE},
        ]
    )

    readback = module.scope_id_team_readback(
        client, "compounding_risk_daily", ORG_A, RUN_START
    )

    assert readback == {TEAM: {"rows": 1, "latest_computed_at": str(LATE)}}


def test_scope_id_team_readback_is_org_scoped():
    """A foreign org's team-scope row must not satisfy this org's readback."""
    module = _load_module()
    client = _FakeClient(
        [{"org_id": ORG_B, "scope": "team", "scope_id": TEAM, "computed_at": LATE}]
    )

    readback = module.scope_id_team_readback(
        client, "compounding_risk_daily", ORG_A, RUN_START
    )

    assert readback == {}


def test_scope_id_team_readback_stale_rows_do_not_satisfy():
    """A row from before this run started is not evidence this run computed
    anything, same computed_at gating every other readback shape enforces."""
    module = _load_module()
    client = _FakeClient(
        [{"org_id": ORG_A, "scope": "team", "scope_id": TEAM, "computed_at": EARLY}]
    )

    readback = module.scope_id_team_readback(
        client, "compounding_risk_daily", ORG_A, RUN_START
    )

    assert readback == {}


def test_compounding_risk_team_is_scope_id_team_shaped_only():
    module = _load_module()
    assert "compounding_risk_team" in module.SCOPE_ID_TEAM_FAMILIES
    assert "compounding_risk_team" not in module.SCOPE_ID_REPO_FAMILIES
    assert "compounding_risk_team" not in module.TEAM_DAY_FAMILIES
    assert "compounding_risk_team" not in module.REPO_DAY_FAMILIES


def test_cli_families_choice_accepts_compounding_risk_team(monkeypatch, capsys):
    """CLI-level check: --families compounding_risk_team is a valid choice and
    routes through the SCOPE_ID_TEAM_FAMILIES dispatch branch."""
    module = _load_module()

    class _Client:
        def query(self, sql, parameters):
            if " FROM (" in sql and "repos" in sql:
                return _QueryResult([("11111111-1111-1111-1111-111111111111",)])
            assert "GROUP BY scope_id" in sql, f"unexpected query: {sql}"
            return _QueryResult([(TEAM, 1, LATE)])

    class _Sink:
        def __init__(self, uri):
            self.client = _Client()

        def close(self):
            pass

    monkeypatch.setattr(module, "ClickHouseMetricsSink", _Sink)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "assert_metrics_executed_proof.py",
            "--clickhouse-uri",
            "fake://unused",
            "--org-id",
            ORG_A,
            "--run-start",
            RUN_START.isoformat(),
            "--families",
            "compounding_risk_team",
        ],
    )

    exit_code = module.main()

    assert exit_code == 0, capsys.readouterr().err
    out = capsys.readouterr().out
    assert '"compounding_risk_team"' in out


def test_cli_compounding_risk_team_zero_rows_fails(monkeypatch, capsys):
    """Negative control: the org has a live repo (so the readiness check
    passes) but compounding_risk_team wrote nothing. Must FAIL."""
    module = _load_module()

    class _Client:
        def query(self, sql, parameters):
            if " FROM (" in sql and "repos" in sql:
                return _QueryResult([("11111111-1111-1111-1111-111111111111",)])
            assert "GROUP BY scope_id" in sql, f"unexpected query: {sql}"
            return _QueryResult([])

    class _Sink:
        def __init__(self, uri):
            self.client = _Client()

        def close(self):
            pass

    monkeypatch.setattr(module, "ClickHouseMetricsSink", _Sink)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "assert_metrics_executed_proof.py",
            "--clickhouse-uri",
            "fake://unused",
            "--org-id",
            ORG_A,
            "--run-start",
            RUN_START.isoformat(),
            "--families",
            "compounding_risk_team",
        ],
    )

    exit_code = module.main()

    assert exit_code == 1
    err = capsys.readouterr().err
    assert "zero_rows_with_source_data" in err
