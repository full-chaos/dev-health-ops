"""CHAOS-6987 gap 5: check-routing-parity.py names every ledger drift and never passes a known gap silently."""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "ci" / "bigboy" / "check-routing-parity.py"
TRACKED = ROOT / "ci" / "bigboy" / "routing-ops.txt"


def _mod() -> ModuleType:
    spec = importlib.util.spec_from_file_location("check_routing_parity", SCRIPT)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod  # dataclasses resolve their module through sys.modules
    spec.loader.exec_module(mod)
    return mod


def _row(
    op: str, live: bool, mode: str = "canary", rollout: int = 100, owner: str = "go"
) -> dict[str, Any]:
    return {
        "operation": op,
        "reachable": live,
        "mode": mode if live else None,
        "rollout_percentage": rollout if live else None,
        "owner": owner if live else None,
    }


def _status(*rows: dict[str, Any], **top: Any) -> dict[str, Any]:
    doc: dict[str, Any] = {
        "catalog_loaded": True,
        "catalog_error": None,
        "registry_db_error": None,
        "classification_error": None,
        "go_plane_error": None,
        "operations": list(rows),
    }
    doc.update(top)
    return doc


OPS = "# prod list\nalpha\nbeta\ngamma KNOWN-MISSING CHAOS-6993 needs bigboy prove harness\n"


def _run(tmp_path: Path, ops: str, status: dict[str, Any], *extra: str) -> int:
    o = tmp_path / "ops.txt"
    o.write_text(ops, encoding="utf-8")
    s = tmp_path / "status.json"
    s.write_text(json.dumps(status), encoding="utf-8")
    return int(_mod().main([str(o), str(s), *extra]))


def test_parity_with_known_gap_exits_3_and_names_it(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    rc = _run(
        tmp_path,
        OPS,
        _status(_row("alpha", True), _row("beta", True), _row("gamma", False)),
    )
    out = capsys.readouterr().out
    assert rc == 3
    assert "KNOWN-MISSING: gamma (CHAOS-6993" in out
    assert "verdict=KNOWN-GAP" in out


def test_full_parity_exits_0(tmp_path: Path) -> None:
    rc = _run(
        tmp_path,
        "alpha\nbeta\n",
        _status(_row("alpha", True), _row("beta", True), _row("gamma", False)),
    )
    assert rc == 0


def test_missing_required_op_fails_named(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    rc = _run(
        tmp_path,
        OPS,
        _status(_row("alpha", True), _row("beta", False), _row("gamma", False)),
    )
    assert rc == 1
    assert (
        "operation beta is enabled on prod but NOT reachable on bigboy"
        in capsys.readouterr().err
    )


def test_extra_reachable_op_fails_named(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    rc = _run(
        tmp_path,
        OPS,
        _status(
            _row("alpha", True),
            _row("beta", True),
            _row("gamma", False),
            _row("delta", True),
        ),
    )
    assert rc == 1
    assert (
        "delta is reachable on bigboy but NOT in the prod list"
        in capsys.readouterr().err
    )


def test_known_missing_now_reachable_fails(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    rc = _run(
        tmp_path,
        OPS,
        _status(_row("alpha", True), _row("beta", True), _row("gamma", True)),
    )
    assert rc == 1
    assert "remove its marker" in capsys.readouterr().err


def test_wrong_shape_fails(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    rc = _run(
        tmp_path,
        OPS,
        _status(
            _row("alpha", True, mode="primary"),
            _row("beta", True),
            _row("gamma", False),
        ),
    )
    assert rc == 1
    assert "alpha shape" in capsys.readouterr().err


def test_listed_op_absent_from_catalog_fails(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    rc = _run(
        tmp_path,
        OPS + "zeta\n",
        _status(_row("alpha", True), _row("beta", True), _row("gamma", False)),
    )
    assert rc == 1
    assert "zeta is not in bigboy's catalog" in capsys.readouterr().err


@pytest.mark.parametrize(
    "top",
    [
        {"catalog_loaded": False, "catalog_error": "no such file"},
        {"registry_db_error": "unreachable"},
        {"go_plane_error": "down"},
    ],
)
def test_incomplete_status_is_drift_not_pass(
    tmp_path: Path, top: dict[str, Any]
) -> None:
    status = _status(
        _row("alpha", True), _row("beta", True), _row("gamma", False), **top
    )
    assert _run(tmp_path, OPS, status) == 1
    assert _run(tmp_path, OPS, status, "--to-enable") == 1


def test_empty_status_is_not_a_pass(tmp_path: Path) -> None:
    assert _run(tmp_path, OPS, _status()) == 1


def test_to_enable_lists_only_required_unreachable(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    rc = _run(
        tmp_path,
        OPS,
        _status(_row("alpha", True), _row("beta", False), _row("gamma", False)),
        "--to-enable",
    )
    assert rc == 0
    assert capsys.readouterr().out.strip() == "beta"


@pytest.mark.parametrize(
    "bad",
    [
        "",
        "# only comments\n",
        "alpha\nalpha\n",
        "gamma KNOWN-MISSING\n",
        "gamma KNOWN-MISSING no-ticket\n",
        "bad-name\n",
    ],
)
def test_malformed_list_is_input_error(tmp_path: Path, bad: str) -> None:
    assert _run(tmp_path, bad, _status(_row("alpha", True))) == 2


def test_tracked_list_parses_and_marks_testops_risk() -> None:
    ops = _mod().parse_ops(TRACKED.read_text(encoding="utf-8"))
    assert "testopsRisk" in ops.known_missing
    assert "CHAOS-6993" in ops.known_missing["testopsRisk"]
    assert "testopsRisk" not in ops.required
    # Prod has every registered operation enabled (the 2026-10-01 readback): the five
    # saved-report mutations and the three seeded reads are listed, not left out.
    assert len(ops.required) + len(ops.known_missing) == 58
    for enabled_on_prod in (
        "createSavedReport",
        "updateSavedReport",
        "deleteSavedReport",
        "cloneSavedReport",
        "triggerReport",
        "home",
        "recommendations",
        "workItemTeamAttributions",
    ):
        assert enabled_on_prod in ops.required, enabled_on_prod
