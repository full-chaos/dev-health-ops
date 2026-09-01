"""Tests for the checked-in Go-API operation catalog (CHAOS-4697).

The catalog (``api/graphql/go_api_operations.json``) is a generated
artifact -- see ``scripts/go_api/generate_operation_catalog.py``'s module
docstring for why it is checked in rather than produced at runtime (the
production Python edge image has no Go toolchain). This file's drift
check keeps it honest, the same contract shape as
``api/graphql/export_schema.py``'s schema-drift detection.
"""

from __future__ import annotations

import importlib
import json
import shutil
import sys
from pathlib import Path

import pytest

from dev_health_ops.api.graphql import go_api_operation_catalog
from dev_health_ops.api.graphql.go_api_operation_catalog import (
    known_operations,
    operation_for_digest,
)

REPO_ROOT = Path(__file__).resolve().parents[3]
CATALOG_PATH = (
    REPO_ROOT / "src" / "dev_health_ops" / "api" / "graphql" / "go_api_operations.json"
)
GENERATE_SCRIPT = REPO_ROOT / "scripts" / "go_api" / "generate_operation_catalog.py"


def test_checked_in_catalog_has_not_drifted_from_registrydump():
    """Regenerates the catalog via the same producer the generate script
    uses and asserts it matches the checked-in file byte-for-byte (module
    order and formatting included, since the generator itself pins
    both). A failure here means someone changed a registered document in
    ``query_route.go`` and did not re-run
    ``scripts/go_api/generate_operation_catalog.py``.
    """
    if shutil.which("go") is None:
        pytest.skip("go toolchain not on PATH -- required to regenerate the catalog")

    sys.path.insert(0, str(GENERATE_SCRIPT.parent))
    try:
        module = importlib.import_module("generate_operation_catalog")
        importlib.reload(module)
        fresh_catalog = module.generate()
    finally:
        sys.path.remove(str(GENERATE_SCRIPT.parent))

    checked_in = json.loads(CATALOG_PATH.read_text())
    assert fresh_catalog == checked_in, (
        "go_api_operations.json has drifted from query_route.go's registered "
        "documents -- run `python3 scripts/go_api/generate_operation_catalog.py` "
        "and commit the diff"
    )


def test_known_operations_matches_checked_in_catalog():
    checked_in = json.loads(CATALOG_PATH.read_text())
    expected = {entry["operation"] for entry in checked_in}
    assert known_operations() == frozenset(expected)


def test_operation_for_digest_resolves_a_real_entry():
    checked_in = json.loads(CATALOG_PATH.read_text())
    sample = checked_in[0]
    assert operation_for_digest(sample["digest"]) == sample["operation"]


def test_operation_for_digest_returns_none_for_unknown_digest():
    assert operation_for_digest("0" * 64) is None


def test_catalog_fails_closed_on_missing_file(
    monkeypatch: pytest.MonkeyPatch, tmp_path
):
    """A missing/unreadable catalog must resolve every digest to None --
    the safe default (no operation is ever recognized as Go-eligible) --
    never raise into the request path."""
    monkeypatch.setattr(
        go_api_operation_catalog, "_CATALOG_PATH", tmp_path / "does-not-exist.json"
    )
    monkeypatch.setattr(go_api_operation_catalog, "_catalog_loaded", False)
    monkeypatch.setattr(go_api_operation_catalog, "_digest_to_operation", {})

    assert go_api_operation_catalog.operation_for_digest("anything") is None
    assert go_api_operation_catalog.known_operations() == frozenset()


def test_catalog_fails_closed_on_malformed_file(
    monkeypatch: pytest.MonkeyPatch, tmp_path
):
    bad = tmp_path / "bad.json"
    bad.write_text("not json")
    monkeypatch.setattr(go_api_operation_catalog, "_CATALOG_PATH", bad)
    monkeypatch.setattr(go_api_operation_catalog, "_catalog_loaded", False)
    monkeypatch.setattr(go_api_operation_catalog, "_digest_to_operation", {})

    assert go_api_operation_catalog.operation_for_digest("anything") is None


def test_catalog_rejects_duplicate_digests_and_fails_closed(
    monkeypatch: pytest.MonkeyPatch, tmp_path
):
    dupe = tmp_path / "dupe.json"
    dupe.write_text(
        json.dumps(
            [
                {"operation": "a", "digest": "same"},
                {"operation": "b", "digest": "same"},
            ]
        )
    )
    monkeypatch.setattr(go_api_operation_catalog, "_CATALOG_PATH", dupe)
    monkeypatch.setattr(go_api_operation_catalog, "_catalog_loaded", False)
    monkeypatch.setattr(go_api_operation_catalog, "_digest_to_operation", {})

    # Fails closed: the ambiguous entries never get loaded at all.
    assert go_api_operation_catalog.operation_for_digest("same") is None
