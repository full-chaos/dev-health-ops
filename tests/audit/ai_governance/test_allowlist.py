"""CHAOS-2209: ai_tool_allowlist write path (admin-seeded source of truth)."""

from __future__ import annotations

import argparse
from typing import Any
from unittest.mock import MagicMock

from dev_health_ops.audit.ai_governance import cli as allowlist_cli
from dev_health_ops.audit.ai_governance.models import (
    AIToolAllowlistEntry,
    ToolAllowlistStatus,
)
from dev_health_ops.fixtures.generators.ai_governance import (
    generate_ai_tool_allowlist_entries,
)
from dev_health_ops.metrics.sinks.clickhouse.ai_governance import (
    ALLOWLIST_COLUMNS,
    _allowlist_row,
)

ORG_ID = "33333333-3333-3333-3333-333333333333"


def test_fixture_seed_matches_attribution_evidence_tooling() -> None:
    entries = generate_ai_tool_allowlist_entries(ORG_ID)

    assert entries
    assert all(entry.org_id == ORG_ID for entry in entries)
    # The PR fixture generator writes tool_name="claude-code" /
    # model_name="claude" into attribution evidence; the seed must cover that
    # pair or the governance join stays "unknown" forever.
    pairs = {(entry.tool_name, entry.model_name) for entry in entries}
    assert ("claude-code", None) in pairs
    assert ("claude-code", "claude") in pairs
    statuses = {entry.status for entry in entries}
    assert ToolAllowlistStatus.ALLOWED in statuses
    assert ToolAllowlistStatus.DISALLOWED in statuses
    assert ToolAllowlistStatus.DEPRECATED in statuses
    # Seeding must never write the sentinel "unknown" status.
    assert ToolAllowlistStatus.UNKNOWN not in statuses


def test_allowlist_row_matches_column_order() -> None:
    entry = AIToolAllowlistEntry(
        org_id=ORG_ID,
        tool_name="claude-code",
        model_name=None,
        status=ToolAllowlistStatus.ALLOWED,
        reason="policy",
    )

    row = _allowlist_row(entry)

    assert len(row) == len(ALLOWLIST_COLUMNS)
    assert row[ALLOWLIST_COLUMNS.index("org_id")] == ORG_ID
    assert row[ALLOWLIST_COLUMNS.index("tool_name")] == "claude-code"
    assert row[ALLOWLIST_COLUMNS.index("model_name")] is None
    assert row[ALLOWLIST_COLUMNS.index("status")] == "allowed"
    assert row[ALLOWLIST_COLUMNS.index("reason")] == "policy"


def _parse(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command", required=True)
    allowlist_cli.register_commands(sub)
    ns = parser.parse_args(argv)
    ns.org = ORG_ID
    ns.analytics_db = "clickhouse://test"
    return ns


def test_cli_set_writes_through_sink(monkeypatch: Any) -> None:
    sink = MagicMock()
    monkeypatch.setattr(allowlist_cli, "_sink", lambda _ns: sink)

    ns = _parse(
        [
            "ai",
            "allowlist",
            "set",
            "--tool",
            "cursor",
            "--model",
            "claude-3.5-sonnet",
            "--status",
            "deprecated",
            "--reason",
            "superseded",
        ]
    )
    exit_code = ns.func(ns)

    assert exit_code == 0
    (entries,) = sink.write_ai_tool_allowlist.call_args.args
    assert len(entries) == 1
    entry = entries[0]
    assert entry.org_id == ORG_ID
    assert entry.tool_name == "cursor"
    assert entry.model_name == "claude-3.5-sonnet"
    assert entry.status is ToolAllowlistStatus.DEPRECATED
    assert entry.reason == "superseded"


def test_cli_set_rejects_unknown_status() -> None:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command", required=True)
    allowlist_cli.register_commands(sub)

    try:
        parser.parse_args(
            ["ai", "allowlist", "set", "--tool", "x", "--status", "unknown"]
        )
    except SystemExit as exc:
        assert exc.code == 2
    else:  # pragma: no cover - argparse must reject the sentinel status
        raise AssertionError("argparse accepted status=unknown")


def test_cli_list_reads_latest_versions(monkeypatch: Any, capsys: Any) -> None:
    sink = MagicMock()
    sink.query_dicts.return_value = [
        {
            "tool_name": "claude-code",
            "model_name": None,
            "status": "allowed",
            "reason": "policy",
            "updated_at": None,
        }
    ]
    monkeypatch.setattr(allowlist_cli, "_sink", lambda _ns: sink)

    ns = _parse(["ai", "allowlist", "list"])
    exit_code = ns.func(ns)

    assert exit_code == 0
    query = sink.query_dicts.call_args.args[0]
    assert "argMax(status, computed_at)" in query
    out = capsys.readouterr().out
    assert "claude-code / *: allowed" in out
