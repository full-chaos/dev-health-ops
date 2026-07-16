import subprocess
from datetime import date
from pathlib import Path

from scripts.check_built_site_links import check_built_site
from scripts.check_code_prerequisites import pages_missing_prerequisite_link
from scripts.check_docs_links import check_docs
from scripts.check_external_links import FetchResult, check_external_links
from scripts.check_freshness_inventory import check_freshness_inventory, load_inventory


def _write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def test_broken_source_link_fails() -> None:
    """Given a markdown page linking to a file that does not exist, when the
    source-link guard runs, then it reports exactly that missing target."""
    root = Path(__file__).resolve().parents[2]
    fixture_root = root / "tests" / "docs" / "fixtures" / "broken_source_link"

    errors = check_docs(fixture_root / "docs", fixture_root)

    assert len(errors) == 1
    assert "does-not-exist.md" in errors[0]
    assert "missing file" in errors[0]


def test_broken_built_asset_fails(tmp_path: Path) -> None:
    """Given built HTML referencing a stylesheet that was never emitted, when
    the built-site guard runs, then it reports the missing built asset."""
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


def test_expired_allowlist_fails(tmp_path: Path) -> None:
    """Given an allowlist entry whose expiry date has passed, when the
    external-link guard runs, then it fails even though the URL is
    reachable, naming the URL and its expiry date."""
    site_dir = tmp_path / "site"
    _write(
        site_dir / "index.html", '<a href="https://expired.example.com/">expired</a>'
    )
    allowlist_path = tmp_path / "allowlist.yml"
    allowlist_path.write_text(
        "- url: https://expired.example.com/\n"
        "  owner: docs-content\n"
        "  reason: temporary vendor outage\n"
        "  expires: '2026-01-01'\n",
        encoding="utf-8",
    )

    def always_ok(_url: str) -> FetchResult:
        return True, "status 200"

    errors = check_external_links(
        site_dir, allowlist_path, always_ok, date(2026, 7, 16)
    )

    assert len(errors) == 1
    assert "https://expired.example.com/" in errors[0]
    assert "expired on 2026-01-01" in errors[0]


def test_stale_review_date_fails(tmp_path: Path) -> None:
    """Given a freshness-inventory entry last reviewed more than 180 days
    ago, when the freshness guard runs, then it names the page and its
    review age."""
    docs_root = tmp_path / "docs"
    _write(docs_root / "guide.md", "# Guide\n")
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


def test_missing_code_prerequisite_fails(tmp_path: Path) -> None:
    """Given an in-scope page with a credentialed code sample but no link to
    getting-started.md, when the prerequisite guard runs, then it names the
    page."""
    docs_root = tmp_path / "docs"
    _write(
        docs_root / "guide.md",
        "```bash\nCLICKHOUSE_URI=clickhouse://ch:ch@localhost:8123/default dev-hops metrics daily\n```\n",
    )

    missing = pages_missing_prerequisite_link(docs_root, frozenset({"guide.md"}))

    assert missing == ["guide.md"]


def test_skipped_deploy_no_op_condition_fails() -> None:
    """Given the docs-guards aggregate job's real bash script, when the
    upstream changes job fails and docs-guards-job is consequently skipped,
    then the aggregate reports failure rather than a masked pass."""
    root = Path(__file__).resolve().parents[2]
    workflow_path = root / ".github" / "workflows" / "docs-guards.yml"

    import yaml

    workflow = yaml.safe_load(workflow_path.read_text(encoding="utf-8"))
    script = workflow["jobs"]["docs-guards"]["steps"][0]["run"]

    result = subprocess.run(
        ["bash", "-c", script],
        check=False,
        env={
            "PATH": "/usr/bin:/bin",
            "CHANGES_RESULT": "failure",
            "DOCS_GUARDS_RESULT": "skipped",
        },
        capture_output=True,
        text=True,
    )

    assert result.returncode == 1, result.stdout + result.stderr
