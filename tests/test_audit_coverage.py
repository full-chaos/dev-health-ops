from pathlib import Path

import pytest

from dev_health_ops.audit.coverage import (
    AUDIT_PROVIDERS,
    _commands_check,
    compile_coverage_report,
    format_coverage_table,
    parse_provider_list,
)


def _status(value: str = "ok"):
    return {"status": value}


def test_parse_provider_list_defaults_to_all():
    assert parse_provider_list(None) == list(AUDIT_PROVIDERS)


def test_parse_provider_list_validates():
    with pytest.raises(ValueError):
        parse_provider_list("jira,unknown")


def test_compile_coverage_report_allows_unchecked_schema():
    providers = ["jira", "github"]
    schema = {"status": "unchecked", "checked": False}
    report = compile_coverage_report(
        providers=providers,
        collector={p: _status() for p in providers},
        config={p: _status() for p in providers},
        schema=schema,
        sink={p: _status() for p in providers},
        commands={p: _status() for p in providers},
    )
    assert report["overall_ok"] is True
    assert report["providers"]["jira"]["overall"] == "ok"


def test_compile_coverage_report_flags_missing_schema():
    providers = ["jira"]
    schema = {"status": "missing", "checked": True}
    report = compile_coverage_report(
        providers=providers,
        collector={"jira": _status()},
        config={"jira": _status()},
        schema=schema,
        sink={"jira": _status()},
        commands={"jira": _status()},
    )
    assert report["overall_ok"] is False
    assert report["providers"]["jira"]["overall"] == "missing"


def test_commands_check_does_not_report_ok_for_a_provider_with_no_native_route():
    """CHAOS-5351 (codex review, r1 P2): every REAL ingestion provider
    reports "ok" unconditionally on this dimension now (native Go route is
    unconditional, see the function's own comment) -- but "synthetic" has
    NO native provider-sync route at all (it's fixture/demo data via a
    different mechanism entirely, confirmed zero entries in
    internal/providersync/execution_registry.go), so it must not be
    swept into the same unconditional "ok" as github/gitlab -- that would
    be a provider-blind false-green on this specific dimension."""
    repo_root = Path(__file__).resolve().parents[1] / "src" / "dev_health_ops"
    result = _commands_check(repo_root)

    assert set(result) == set(AUDIT_PROVIDERS)
    assert result["github"]["status"] == "ok"
    assert result["gitlab"]["status"] == "ok"
    assert result["synthetic"]["status"] != "ok"


def test_format_coverage_table_renders_rows():
    providers = ["jira"]
    schema = {"status": "unchecked", "checked": False}
    report = compile_coverage_report(
        providers=providers,
        collector={"jira": _status()},
        config={"jira": _status()},
        schema=schema,
        sink={"jira": _status()},
        commands={"jira": _status()},
    )
    table = format_coverage_table(report)
    assert "provider" in table
    assert "unchecked" in table
