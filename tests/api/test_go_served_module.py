"""CHAOS-6847: the Go-served route refusal has ONE definition, api/go_served.py.

Every family deleted after CHAOS-6241's 32 routes imports it from there; a
second copy in a router would drift (a different detail, no log event) and the
CHAOS-6813 recogniser would still call it a stub, because it matches the call
NAME only. These tests pin the single definition, its behaviour, and that
api/main.py still uses (not redefines) it.
"""

from __future__ import annotations

import ast
import logging
from pathlib import Path

import pytest
from fastapi import HTTPException

from dev_health_ops.api import go_served, main

_SRC = Path(go_served.__file__).parent


def test_main_uses_the_single_definition():
    assert main._raise_served_by_go_api is go_served._raise_served_by_go_api


def test_no_other_module_under_api_defines_the_refusal():
    definitions = []
    for path in sorted(_SRC.rglob("*.py")):
        tree = ast.parse(path.read_text(), filename=str(path))
        for node in ast.walk(tree):
            if (
                isinstance(node, ast.ClassDef)
                and node.name == "GoServedRouteUnavailableError"
            ):
                definitions.append(f"{path.name}:{node.lineno} class")
            if (
                isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef)
                and node.name == "_raise_served_by_go_api"
            ):
                definitions.append(f"{path.name}:{node.lineno} def")
    assert sorted(d.split(":")[0] for d in definitions) == [
        "go_served.py",
        "go_served.py",
    ], definitions


def test_the_refusal_is_a_500_that_names_the_path_and_logs_the_event(
    caplog: pytest.LogCaptureFixture,
):
    with caplog.at_level(logging.ERROR, logger="dev_health_ops.api.go_served"):
        with pytest.raises(go_served.GoServedRouteUnavailableError) as raised:
            go_served._raise_served_by_go_api("/api/v1/example")
    error = raised.value
    assert isinstance(error, HTTPException)
    assert error.status_code == 500
    assert "/api/v1/example" in error.detail
    assert "query-api" in error.detail
    assert "ingress.queryApiPaths" in error.detail
    assert any(
        record.message == "rest.route_served_by_query_api"
        and getattr(record, "path", None) == "/api/v1/example"
        for record in caplog.records
    )
