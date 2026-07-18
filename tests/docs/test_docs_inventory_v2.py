from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType
from typing import Any

import yaml


def _load_inventory_module() -> ModuleType:
    script = Path(__file__).parents[2] / "scripts" / "docs_inventory_v2.py"
    spec = importlib.util.spec_from_file_location("docs_inventory_v2", script)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_inventory_classifies_nav_reference_and_internal_pages(
    tmp_path: Path,
) -> None:
    module: Any = _load_inventory_module()
    docs = tmp_path / "docs"
    docs.mkdir()
    (docs / "plans").mkdir()

    (tmp_path / "mkdocs.yml").write_text(
        yaml.safe_dump(
            {
                "site_url": "https://docs.example.test",
                "nav": [{"Home": "index.md"}],
                "exclude_docs": "plans/\n",
            }
        ),
        encoding="utf-8",
    )
    (docs / "publication.yml").write_text(
        yaml.safe_dump(
            {
                "excluded_internal": ["plans/**"],
                "public_reference": ["**/*.md"],
            }
        ),
        encoding="utf-8",
    )
    (docs / "index.md").write_text(
        "---\naudience: Everyone\nowner: Docs\nlast-reviewed: 2026-07-18\n---\n"
        "# Home\n\n[Guide](guide.md)\n",
        encoding="utf-8",
    )
    (docs / "guide.md").write_text("# Guide\n", encoding="utf-8")
    (docs / "plans" / "internal.md").write_text("# Internal\n", encoding="utf-8")

    inventory = module.build_inventory(tmp_path, "example/repo")
    by_path = {row["source_path"]: row for row in inventory["rows"]}

    assert by_path["docs/index.md"]["publication_classification"] == "public-nav"
    assert by_path["docs/guide.md"]["publication_classification"] == "public-reference"
    assert (
        by_path["docs/plans/internal.md"]["publication_classification"]
        == "excluded-internal"
    )
    assert by_path["docs/index.md"]["links_out"] == ["guide.md"]
    assert by_path["docs/guide.md"]["links_in"] == ["index.md"]
    assert by_path["docs/index.md"]["current_url"] == "https://docs.example.test/"
