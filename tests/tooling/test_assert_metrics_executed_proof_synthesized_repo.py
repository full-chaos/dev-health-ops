"""CHAOS-4290, r2 finding #4: ic_finalize's user_metrics_daily mixes two
repo_id spaces -- a git-backed identity's real repo_id, and a deterministic
per-identity SynthesizedRepoID for a work-item-only identity with no git
record at all (executor.go's writeUserMetrics). The generic REPO_DAY_FAMILIES
readback assumes every row's repo_id is a live repo; applied to this table it
misreads a synthesized row as the CHAOS-4263 dead-id shape it exists to catch,
or undercounts total_rows to zero for an org with no git-backed identity.

synthesized_repo_readback (ci/assert_metrics_executed_proof.py) fixes this by
recognising a repo_id as valid when it is EITHER a live repo OR the
synthesized id computed from that row's OWN identity column.
"""

from __future__ import annotations

import importlib.util
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

_SCRIPT = Path(__file__).parents[2] / "ci" / "assert_metrics_executed_proof.py"

ORG = "org-33333333-cccc"
RUN_START = datetime(2026, 9, 5, tzinfo=timezone.utc)
LATE = RUN_START + timedelta(minutes=5)
LIVE_REPO = "44444444-4444-4444-4444-444444444444"
DEAD_REPO = "55555555-5555-5555-5555-555555555555"


def _load_module():
    spec = importlib.util.spec_from_file_location(
        "assert_metrics_executed_proof_under_test_synth", _SCRIPT
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class _QueryResult:
    def __init__(self, rows):
        self.result_rows = rows


class _FakeClient:
    """Backs synthesized_repo_readback's single grouped query only."""

    def __init__(self, rows: list[tuple[str, str, int]]):
        self._rows = rows  # (repo_id, identity_id, count)

    def query(self, sql: str, parameters: dict):
        assert "GROUP BY repo_id" in sql, f"unexpected query shape: {sql}"
        return _QueryResult(list(self._rows))


def test_synthesized_repo_id_is_deterministic_and_org_identity_scoped():
    module = _load_module()
    first = module.synthesized_repo_id(ORG, "wi-only@example.com")
    second = module.synthesized_repo_id(ORG, "wi-only@example.com")
    assert first == second, "synthesized_repo_id must be stable across calls"
    assert first != module.synthesized_repo_id("org-other", "wi-only@example.com"), (
        "must separate orgs"
    )
    assert first != module.synthesized_repo_id(ORG, "someone-else@example.com"), (
        "must separate identities"
    )


def test_a_synthesized_row_alone_is_not_flagged_stray_and_is_counted():
    """CONSTRUCTION: an org with ONLY a work-item-only identity -- every row's
    repo_id is a SynthesizedRepoID, none is a live repo. Pre-fix this would
    read total_rows=0 (repo_ids-only filter excludes it) AND flag the
    synthesized id as a CHAOS-4263 stray -- a double false failure for a
    perfectly correct write."""
    module = _load_module()
    identity = "wi-only@example.com"
    synthesized = module.synthesized_repo_id(ORG, identity)
    client = _FakeClient([(synthesized, identity, 3)])

    total_rows, stray = module.synthesized_repo_readback(
        client, "user_metrics_daily", "author_email", ORG, {LIVE_REPO}, RUN_START
    )

    assert total_rows == 3
    assert stray == set(), f"synthesized id wrongly flagged stray: {stray}"


def test_a_git_backed_row_is_counted_via_the_live_repo_set():
    module = _load_module()
    client = _FakeClient([(LIVE_REPO, "git-user@example.com", 2)])

    total_rows, stray = module.synthesized_repo_readback(
        client, "user_metrics_daily", "author_email", ORG, {LIVE_REPO}, RUN_START
    )

    assert total_rows == 2
    assert stray == set()


def test_a_truly_dead_repo_id_is_still_flagged_stray():
    """NEGATIVE CONTROL: a repo_id that is neither a live repo nor this row's
    own synthesized id is the genuine CHAOS-4263 shape and must still fail --
    the fix must not blanket-exempt every repo_id ic_finalize ever writes."""
    module = _load_module()
    identity = "someone@example.com"
    client = _FakeClient([(DEAD_REPO, identity, 1)])

    total_rows, stray = module.synthesized_repo_readback(
        client, "user_metrics_daily", "author_email", ORG, {LIVE_REPO}, RUN_START
    )

    assert total_rows == 1
    assert stray == {DEAD_REPO}, (
        "a repo_id matching neither the live set nor this identity's own "
        f"synthesized id must still be reported stray; got {stray}"
    )


def test_ic_finalize_is_not_double_registered():
    module = _load_module()
    assert "ic_finalize" not in module.REPO_DAY_FAMILIES, (
        "ic_finalize moved to SYNTHESIZED_REPO_ID_FAMILIES; a REPO_DAY_FAMILIES "
        "entry left behind would shadow/duplicate the --families choice"
    )
    assert "ic_finalize" in module.SYNTHESIZED_REPO_ID_FAMILIES


def test_cli_families_choice_accepts_ic_finalize(monkeypatch, capsys):
    """CLI-level check: --families ic_finalize must still be a valid choice
    and must route through the new dispatch branch, not error as unknown."""
    module = _load_module()
    identity = "wi-only@example.com"
    synthesized = module.synthesized_repo_id(ORG, identity)

    class _Client:
        def query(self, sql, parameters):
            if " FROM (" in sql and "repos" in sql:
                return _QueryResult([(LIVE_REPO,)])
            assert "GROUP BY repo_id" in sql, f"unexpected query: {sql}"
            return _QueryResult([(synthesized, identity, 1)])

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
            ORG,
            "--run-start",
            RUN_START.isoformat(),
            "--families",
            "ic_finalize",
        ],
    )

    exit_code = module.main()

    assert exit_code == 0, capsys.readouterr().err
    out = capsys.readouterr().out
    assert '"ic_finalize"' in out
