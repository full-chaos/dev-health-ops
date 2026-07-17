from datetime import date
from pathlib import Path

import pytest

from scripts.check_freshness_inventory import (
    FreshnessInventoryError,
    check_freshness_inventory,
    load_inventory,
)


def _write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _docs_root(tmp_path: Path) -> Path:
    docs_root = tmp_path / "docs"
    _write(docs_root / "guide.md", "# Guide\n")
    _write(docs_root / "replacement.md", "# Replacement\n")
    return docs_root


def test_load_inventory_raises_when_the_file_is_missing(tmp_path: Path) -> None:
    with pytest.raises(FreshnessInventoryError, match="missing freshness inventory"):
        load_inventory(tmp_path / "freshness-inventory.yml")


def test_check_freshness_inventory_accepts_a_fresh_refresh_entry(
    tmp_path: Path,
) -> None:
    docs_root = _docs_root(tmp_path)
    inventory_path = tmp_path / "freshness-inventory.yml"
    inventory_path.write_text(
        "- page: guide.md\n"
        "  disposition: refresh\n"
        "  owner: docs-team\n"
        "  last-reviewed: '2026-07-01'\n",
        encoding="utf-8",
    )

    entries = load_inventory(inventory_path)
    errors = check_freshness_inventory(entries, docs_root, today=date(2026, 7, 16))

    assert errors == []


def test_check_freshness_inventory_rejects_a_stale_review_date(tmp_path: Path) -> None:
    docs_root = _docs_root(tmp_path)
    inventory_path = tmp_path / "freshness-inventory.yml"
    inventory_path.write_text(
        "- page: guide.md\n"
        "  disposition: refresh\n"
        "  owner: docs-team\n"
        "  last-reviewed: '2025-01-01'\n",
        encoding="utf-8",
    )

    entries = load_inventory(inventory_path)
    errors = check_freshness_inventory(entries, docs_root, today=date(2026, 7, 16))

    assert len(errors) == 1
    assert "guide.md" in errors[0]
    assert "exceeding the 180-day review cadence" in errors[0]


def test_check_freshness_inventory_requires_a_replacement_for_retired_pages(
    tmp_path: Path,
) -> None:
    docs_root = _docs_root(tmp_path)
    inventory_path = tmp_path / "freshness-inventory.yml"
    inventory_path.write_text(
        "- page: guide.md\n"
        "  disposition: retire\n"
        "  owner: docs-team\n"
        "  last-reviewed: '2026-07-01'\n",
        encoding="utf-8",
    )

    entries = load_inventory(inventory_path)
    errors = check_freshness_inventory(entries, docs_root, today=date(2026, 7, 16))

    assert len(errors) == 1
    assert "retired pages must name a replacement page" in errors[0]


def test_check_freshness_inventory_accepts_a_retired_page_with_an_existing_replacement(
    tmp_path: Path,
) -> None:
    docs_root = _docs_root(tmp_path)
    inventory_path = tmp_path / "freshness-inventory.yml"
    inventory_path.write_text(
        "- page: guide.md\n"
        "  disposition: retire\n"
        "  owner: docs-team\n"
        "  last-reviewed: '2026-07-01'\n"
        "  replacement: replacement.md\n",
        encoding="utf-8",
    )

    entries = load_inventory(inventory_path)
    errors = check_freshness_inventory(entries, docs_root, today=date(2026, 7, 16))

    assert errors == []


def test_load_inventory_rejects_an_invalid_disposition(tmp_path: Path) -> None:
    inventory_path = tmp_path / "freshness-inventory.yml"
    inventory_path.write_text(
        "- page: guide.md\n"
        "  disposition: archive\n"
        "  owner: docs-team\n"
        "  last-reviewed: '2026-07-01'\n",
        encoding="utf-8",
    )

    with pytest.raises(FreshnessInventoryError, match="invalid disposition"):
        load_inventory(inventory_path)
