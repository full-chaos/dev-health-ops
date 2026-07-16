from pathlib import Path

from scripts.check_built_site_links import check_built_site


def _write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


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
