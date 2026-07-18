from __future__ import annotations

import importlib.util
import sys
from pathlib import Path


def _load_validator():
    script = Path(__file__).parents[2] / "scripts" / "validate_docs_ia_v2.py"
    spec = importlib.util.spec_from_file_location("validate_docs_ia_v2", script)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_committed_manifest_is_valid() -> None:
    module = _load_validator()
    root = Path(__file__).parents[2]
    nodes = module.load_nodes(root / ".github/documentation-program/ia")
    assert module.validate_nodes(nodes) == []


def test_validator_rejects_duplicate_url_and_reused_onboarding_title() -> None:
    module = _load_validator()
    nodes = [
        {
            "id": "home",
            "label": "Home",
            "url": "/",
            "parent_id": "",
            "kind": "landing",
            "nav": "true",
            "public_state": "public",
            "lifecycle": "planned",
            "provisional": "false",
        },
        *[
            {
                "id": prefix.strip("/"),
                "label": prefix,
                "url": prefix,
                "parent_id": "home",
                "kind": "landing",
                "nav": "true",
                "public_state": "public",
                "lifecycle": "planned",
                "provisional": "true" if prefix == "/get-started/" else "false",
            }
            for prefix in sorted(module.EXPECTED_TOP_LEVEL)
        ],
        {
            "id": "bad",
            "label": "First ten minutes",
            "url": "/get-started/first-ten-minutes/",
            "parent_id": "get-started",
            "kind": "tutorial",
            "nav": "true",
            "public_state": "public",
            "lifecycle": "planned",
            "provisional": "false",
        },
        {
            "id": "duplicate",
            "label": "Duplicate",
            "url": "/use/",
            "parent_id": "use",
            "kind": "task-guide",
            "nav": "true",
            "public_state": "public",
            "lifecycle": "planned",
            "provisional": "false",
        },
    ]
    errors = module.validate_nodes(nodes)
    assert any("current onboarding title" in error for error in errors)
    assert any("duplicate canonical URL" in error for error in errors)
