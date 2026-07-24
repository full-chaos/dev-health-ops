from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

from scripts.docs_publication import (
    PublicationClassificationError,
    classify_all,
    load_nav_paths,
)

ROOT = Path(__file__).resolve().parents[2]
DOCS_DIR = ROOT / "docs"
LEGACY_DOCS_DIR = ROOT / ".github" / "docs-legacy"
MKDOCS_PATH = ROOT / "mkdocs.yml"
MANIFEST_PATH = LEGACY_DOCS_DIR / "publication.yml"
FIXTURE_ROOT = Path(__file__).resolve().parent / "fixtures" / "unclassified_page"


def test_every_canonical_markdown_file_is_published_in_navigation() -> None:
    assert DOCS_DIR.is_dir()
    assert LEGACY_DOCS_DIR.is_dir()
    assert MANIFEST_PATH.is_file()

    classification = classify_all(DOCS_DIR, MKDOCS_PATH, MANIFEST_PATH)
    all_files = {
        path.relative_to(DOCS_DIR).as_posix() for path in DOCS_DIR.rglob("*.md")
    }

    assert set(classification) == all_files
    assert set(classification.values()) == {"public-nav"}


def test_legacy_internal_material_is_archived_outside_the_public_tree() -> None:
    for legacy_path in (
        "plans/dead-code-cleanup-plan.md",
        "plans/atlassian-client-integration.md",
        "roadmap.md",
        "project.md",
    ):
        assert (LEGACY_DOCS_DIR / legacy_path).is_file()
        assert not (DOCS_DIR / legacy_path).exists()


def test_home_page_is_public_navigation() -> None:
    classification = classify_all(DOCS_DIR, MKDOCS_PATH, MANIFEST_PATH)
    assert classification["index.md"] == "public-nav"


def test_navigation_never_points_into_the_legacy_tree() -> None:
    nav_paths = load_nav_paths(MKDOCS_PATH)
    assert nav_paths
    assert all("docs-legacy" not in path for path in nav_paths)
    assert all((DOCS_DIR / path).is_file() for path in nav_paths)


def test_rejects_unclassified_page() -> None:
    with pytest.raises(PublicationClassificationError) as excinfo:
        classify_all(
            FIXTURE_ROOT / "docs",
            FIXTURE_ROOT / "mkdocs.yml",
            FIXTURE_ROOT / "publication.yml",
        )

    assert excinfo.value.path == "mystery.md"


def test_strict_build_omits_archived_legacy_pages(tmp_path: Path) -> None:
    site_dir = tmp_path / "site"
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "mkdocs",
            "build",
            "--strict",
            "--config-file",
            "mkdocs.yml",
            "--site-dir",
            str(site_dir),
        ],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stdout + result.stderr

    assert not (site_dir / "roadmap" / "index.html").exists()
    assert not (site_dir / "project" / "index.html").exists()
    assert not (site_dir / "plans" / "dead-code-cleanup-plan" / "index.html").exists()
