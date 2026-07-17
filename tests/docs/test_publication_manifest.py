from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

from scripts.docs_publication import (
    PublicationClassificationError,
    classify_all,
    load_manifest,
    load_nav_paths,
)

ROOT = Path(__file__).resolve().parents[2]
DOCS_DIR = ROOT / "docs"
MKDOCS_PATH = ROOT / "mkdocs.yml"
MANIFEST_PATH = ROOT / "docs" / "publication.yml"

FIXTURE_ROOT = Path(__file__).resolve().parent / "fixtures" / "unclassified_page"


def test_every_docs_markdown_file_has_exactly_one_publication_classification() -> None:
    assert MANIFEST_PATH.is_file(), f"missing publication manifest: {MANIFEST_PATH}"

    classification = classify_all(DOCS_DIR, MKDOCS_PATH, MANIFEST_PATH)

    all_files = {
        path.relative_to(DOCS_DIR).as_posix() for path in DOCS_DIR.rglob("*.md")
    }
    assert set(classification) == all_files
    assert set(classification.values()) <= {
        "public-nav",
        "public-reference",
        "excluded-internal",
    }


def test_internal_planning_material_is_excluded_not_published() -> None:
    classification = classify_all(DOCS_DIR, MKDOCS_PATH, MANIFEST_PATH)

    for known_internal_path in (
        "plans/dead-code-cleanup-plan.md",
        "plans/atlassian-client-integration.md",
        "plans/github-app-marketplace.md",
        "roadmap.md",
        "project.md",
        "superpowers/specs/2026-04-14-security-alerts-ui-design.md",
    ):
        assert classification[known_internal_path] == "excluded-internal", (
            known_internal_path
        )


def test_governance_docs_added_in_todo_one_are_public_reference() -> None:
    classification = classify_all(DOCS_DIR, MKDOCS_PATH, MANIFEST_PATH)

    for known_reference_path in (
        "coverage-matrix.md",
        "decisions/unified-docs-cloudflare.md",
        "contributing/platform-contract.md",
    ):
        assert classification[known_reference_path] == "public-reference", (
            known_reference_path
        )


def test_home_page_is_public_nav() -> None:
    classification = classify_all(DOCS_DIR, MKDOCS_PATH, MANIFEST_PATH)

    assert classification["index.md"] == "public-nav"


def test_no_excluded_internal_path_is_reachable_from_nav() -> None:
    manifest = load_manifest(MANIFEST_PATH)
    nav_paths = load_nav_paths(MKDOCS_PATH)

    conflicts = [
        nav_path
        for nav_path in nav_paths
        for pattern in manifest.excluded_internal
        if _matches(nav_path, pattern)
    ]

    assert conflicts == []


def _matches(relpath: str, pattern: str) -> bool:
    import fnmatch

    return fnmatch.fnmatch(relpath, pattern)


def test_rejects_unclassified_page() -> None:
    with pytest.raises(PublicationClassificationError) as excinfo:
        classify_all(
            FIXTURE_ROOT / "docs",
            FIXTURE_ROOT / "mkdocs.yml",
            FIXTURE_ROOT / "publication.yml",
        )

    assert excinfo.value.path == "mystery.md"


def test_exclude_docs_matches_publication_manifest() -> None:
    """mkdocs.yml's `exclude_docs` (gitignore-pattern format) must stay in
    lockstep with publication.yml's `excluded_internal` (fnmatch-glob
    format), or a page could be excluded from the site while still being
    reachable in nav, or vice versa."""
    manifest = load_manifest(MANIFEST_PATH)

    mkdocs_raw = MKDOCS_PATH.read_text(encoding="utf-8")
    exclude_docs_block = yaml.safe_load(
        "exclude_docs: |\n"
        + "\n".join(
            f"  {line}"
            for line in mkdocs_raw.split("exclude_docs: |\n", 1)[1]
            .split("\n\nplugins:", 1)[0]
            .splitlines()
        )
    )["exclude_docs"]
    gitignore_patterns = tuple(line for line in exclude_docs_block.splitlines() if line)

    translated = tuple(
        pattern + "**" if pattern.endswith("/") else pattern
        for pattern in gitignore_patterns
    )

    assert set(translated) == set(manifest.excluded_internal)


def test_strict_build_excludes_internal_docs_from_search_index(
    tmp_path: Path,
) -> None:
    site_dir = tmp_path / "site"
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "mkdocs",
            "build",
            "--strict",
            "--site-dir",
            str(site_dir),
        ],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stdout + result.stderr

    search_index_path = site_dir / "search" / "search_index.json"
    assert search_index_path.is_file()
    search_index = json.loads(search_index_path.read_text(encoding="utf-8"))
    indexed_locations = tuple(entry["location"] for entry in search_index["docs"])

    classification = classify_all(DOCS_DIR, MKDOCS_PATH, MANIFEST_PATH)
    excluded_docs = [
        relpath
        for relpath, bucket in classification.items()
        if bucket == "excluded-internal"
    ]
    assert excluded_docs, "expected at least one excluded-internal doc"
    for relpath in excluded_docs:
        slug = relpath.removesuffix(".md") + "/"
        leaked = [
            location
            for location in indexed_locations
            if location == slug or location.startswith(f"{slug}#")
        ]
        assert leaked == [], (
            f"excluded-internal doc leaked into search index: {relpath} -> {leaked}"
        )


def test_rejects_a_publication_manifest_where_a_bucket_is_not_a_list(
    tmp_path: Path,
) -> None:
    """A string value for excluded_internal must not silently degrade into
    per-character glob patterns via tuple("not-a-list") -> ('n','o',...)."""
    (tmp_path / "docs").mkdir()
    (tmp_path / "docs" / "index.md").write_text("# Home\n", encoding="utf-8")
    (tmp_path / "mkdocs.yml").write_text("nav:\n  - Home: index.md\n", encoding="utf-8")
    manifest_path = tmp_path / "publication.yml"
    manifest_path.write_text(
        "excluded_internal: not-a-list\npublic_reference: []\n", encoding="utf-8"
    )

    with pytest.raises(PublicationClassificationError) as excinfo:
        classify_all(tmp_path / "docs", tmp_path / "mkdocs.yml", manifest_path)

    assert "excluded_internal" in str(excinfo.value)
    assert "must be a YAML list" in str(excinfo.value)
