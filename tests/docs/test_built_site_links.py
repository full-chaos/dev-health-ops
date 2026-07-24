import json
import subprocess
import sys
from pathlib import Path

from scripts.check_built_site_links import check_built_site

ROOT = Path(__file__).resolve().parents[2]
DOCS_DIR = ROOT / "docs"
MKDOCS_PATH = ROOT / "mkdocs.yml"
PUBLICATION_VALIDATOR = ROOT / "scripts" / "validate_docs_v2_publication.py"
DOC_PROGRAM = ROOT / ".github" / "documentation-program"
IA_DIR = DOC_PROGRAM / "ia"
PHASE9_DIR = DOC_PROGRAM / "phase-9"
MIGRATED_SOURCE_MAP = DOC_PROGRAM / "content" / "migrated-source-pages.json"

# Navigated canonical pages that must publish to public URLs. The final
# entry is a directly migrated source page, so this also proves the
# migrated-source contract renders into the public tree.
CANONICAL_BUILT_PAGES = (
    ("use/index.md", ("use", "index.html")),
    ("reference/index.md", ("reference", "index.html")),
    (
        "use/investment/investigate-effort.md",
        ("use", "investment", "investigate-effort", "index.html"),
    ),
    (
        "reference/metrics/weighting-and-aggregation.md",
        ("reference", "metrics", "weighting-and-aggregation", "index.html"),
    ),
    (
        "reference/taxonomies/investment.md",
        ("reference", "taxonomies", "investment", "index.html"),
    ),
    ("reference/cli/index.md", ("reference", "cli", "index.html")),
)


def _write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _built_html_for(site_dir: Path, source_rel: str) -> Path:
    """Map a ``docs/``-relative Markdown path to its built HTML file."""
    parts = source_rel.split("/")
    name = parts[-1]
    if name == "index.md":
        return site_dir.joinpath(*parts[:-1], "index.html")
    return site_dir.joinpath(*parts[:-1], name[: -len(".md")], "index.html")


def test_check_built_site_accepts_a_clean_two_page_site(tmp_path: Path) -> None:
    site_dir = tmp_path / "site"
    _write(
        site_dir / "index.html",
        '<html><body><a href="other/index.html">other</a>'
        '<a href="index.html#section-one">jump</a>'
        '<h2 id="section-one">Section One</h2></body></html>',
    )
    _write(site_dir / "other" / "index.html", "<html><body>Other page</body></html>")

    assert check_built_site(site_dir) == []


def test_check_built_site_rejects_a_broken_built_asset(tmp_path: Path) -> None:
    site_dir = tmp_path / "site"
    _write(
        site_dir / "index.html",
        '<html><head><link rel="stylesheet" href="assets/theme.css"></head>'
        "<body>Home</body></html>",
    )

    errors = check_built_site(site_dir)

    assert len(errors) == 1
    assert "assets/theme.css" in errors[0]
    assert "missing built asset" in errors[0]


def test_check_built_site_rejects_a_missing_anchor(tmp_path: Path) -> None:
    site_dir = tmp_path / "site"
    _write(
        site_dir / "index.html",
        '<html><body><a href="index.html#does-not-exist">jump</a>'
        '<h2 id="section-one">Section One</h2></body></html>',
    )

    errors = check_built_site(site_dir)

    assert len(errors) == 1
    assert "missing anchor 'does-not-exist'" in errors[0]


def test_check_built_site_ignores_external_and_mailto_links(tmp_path: Path) -> None:
    site_dir = tmp_path / "site"
    _write(
        site_dir / "index.html",
        '<html><body><a href="https://example.com/">external</a>'
        '<a href="mailto:team@example.com">email</a></body></html>',
    )

    assert check_built_site(site_dir) == []


def test_check_built_site_ignores_unrendered_jinja_but_rejects_real_assets(
    tmp_path: Path,
) -> None:
    site_dir = tmp_path / "site"
    _write(
        site_dir / "overrides" / "main.html",
        "<a href=\"{{ metadata.get('next').get('url') | url }}\">next</a>"
        '<link rel="stylesheet" href="assets/missing.css">',
    )

    errors = check_built_site(site_dir)

    assert len(errors) == 1
    assert "assets/missing.css" in errors[0]


def test_canonical_publication_gate_is_not_driven_by_docs_publication_yml() -> None:
    """The canonical gate is driven by mkdocs.yml + the documentation-program
    IA, phase-9 redirects, and the migrated-source map -- not a
    ``docs/publication.yml`` manifest."""
    assert not (DOCS_DIR / "publication.yml").exists()

    assert MKDOCS_PATH.is_file()
    assert PUBLICATION_VALIDATOR.is_file()
    assert MIGRATED_SOURCE_MAP.is_file()
    assert IA_DIR.is_dir() and any(IA_DIR.glob("*.tsv"))
    assert (PHASE9_DIR / "redirects.tsv").is_file()

    validation = subprocess.run(
        [sys.executable, str(PUBLICATION_VALIDATOR)],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    assert validation.returncode == 0, validation.stdout + validation.stderr


def test_canonical_pages_build_to_public_urls_without_broken_links(
    tmp_path: Path,
) -> None:
    """The strict canonical build publishes navigated pages -- including every
    directly migrated source -- and the rendered site has no broken internal
    links, anchors, or assets."""
    site_dir = tmp_path / "site"
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "mkdocs",
            "build",
            "--strict",
            "--config-file",
            str(MKDOCS_PATH),
            "--site-dir",
            str(site_dir),
        ],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stdout + result.stderr

    for source_rel, built_parts in CANONICAL_BUILT_PAGES:
        assert (DOCS_DIR / source_rel).is_file(), source_rel
        assert site_dir.joinpath(*built_parts).is_file(), built_parts

    migrated = json.loads(MIGRATED_SOURCE_MAP.read_text(encoding="utf-8"))
    assert migrated, "the migrated-source contract must not be empty"
    for target_rel in migrated:
        assert (DOCS_DIR / target_rel).is_file(), target_rel
        assert _built_html_for(site_dir, target_rel).is_file(), target_rel

    assert check_built_site(site_dir) == []
