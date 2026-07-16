from datetime import date
from pathlib import Path

from scripts.check_external_links import FetchResult, check_external_links, main


def _write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _always_ok(_url: str) -> FetchResult:
    return True, "status 200"


def _always_broken(_url: str) -> FetchResult:
    return False, "status 404"


def test_check_external_links_accepts_a_reachable_link(tmp_path: Path) -> None:
    site_dir = tmp_path / "site"
    _write(site_dir / "index.html", '<a href="https://example.com/">example</a>')
    allowlist_path = tmp_path / "allowlist.yml"

    errors = check_external_links(
        site_dir, allowlist_path, _always_ok, date(2026, 7, 16)
    )

    assert errors == []


def test_check_external_links_rejects_a_broken_unlisted_link(tmp_path: Path) -> None:
    site_dir = tmp_path / "site"
    _write(site_dir / "index.html", '<a href="https://broken.example.com/">broken</a>')
    allowlist_path = tmp_path / "allowlist.yml"

    errors = check_external_links(
        site_dir, allowlist_path, _always_broken, date(2026, 7, 16)
    )

    assert len(errors) == 1
    assert "broken external link https://broken.example.com/" in errors[0]


def test_check_external_links_rejects_an_expired_allowlist_entry_even_when_reachable(
    tmp_path: Path,
) -> None:
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

    errors = check_external_links(
        site_dir, allowlist_path, _always_ok, date(2026, 7, 16)
    )

    assert len(errors) == 1
    assert "allowlist entry for https://expired.example.com/ expired" in errors[0]


def test_check_external_links_skips_a_link_covered_by_an_unexpired_allowlist_entry(
    tmp_path: Path,
) -> None:
    site_dir = tmp_path / "site"
    _write(site_dir / "index.html", '<a href="https://flaky.example.com/">flaky</a>')
    allowlist_path = tmp_path / "allowlist.yml"
    allowlist_path.write_text(
        "- url: https://flaky.example.com/\n"
        "  owner: docs-content\n"
        "  reason: known flaky vendor endpoint\n"
        "  expires: '2027-01-01'\n",
        encoding="utf-8",
    )

    errors = check_external_links(
        site_dir, allowlist_path, _always_broken, date(2026, 7, 16)
    )

    assert errors == []


def test_check_external_links_retries_before_failing(tmp_path: Path) -> None:
    site_dir = tmp_path / "site"
    _write(site_dir / "index.html", '<a href="https://flaky.example.com/">flaky</a>')
    allowlist_path = tmp_path / "allowlist.yml"
    calls: list[str] = []

    def fetcher(url: str) -> FetchResult:
        calls.append(url)
        return (True, "status 200") if len(calls) >= 2 else (False, "status 503")

    errors = check_external_links(
        site_dir, allowlist_path, fetcher, date(2026, 7, 16), max_retries=3
    )

    assert errors == []
    assert len(calls) == 2


def test_check_external_links_deduplicates_repeated_urls(tmp_path: Path) -> None:
    site_dir = tmp_path / "site"
    _write(
        site_dir / "index.html",
        '<a href="https://example.com/">one</a><a href="https://example.com/">two</a>',
    )
    _write(site_dir / "other.html", '<a href="https://example.com/">three</a>')
    allowlist_path = tmp_path / "allowlist.yml"
    calls: list[str] = []

    def fetcher(url: str) -> FetchResult:
        calls.append(url)
        return True, "status 200"

    check_external_links(site_dir, allowlist_path, fetcher, date(2026, 7, 16))

    assert calls == ["https://example.com/"]


def test_check_external_links_resolves_same_site_canonical_anchor_locally(
    tmp_path: Path,
) -> None:
    site_dir = tmp_path / "site"
    _write(
        site_dir / "index.html",
        '<a href="https://docs.fullchaos.dev/guide/#details">guide</a>',
    )
    _write(site_dir / "guide" / "index.html", '<section id="details"></section>')
    allowlist_path = tmp_path / "allowlist.yml"
    calls: list[str] = []

    def unavailable_fetcher(url: str) -> FetchResult:
        calls.append(url)
        return False, "DNS unavailable"

    errors = check_external_links(
        site_dir,
        allowlist_path,
        unavailable_fetcher,
        date(2026, 7, 16),
        site_url="https://docs.fullchaos.dev",
    )

    assert errors == []
    assert calls == []


def test_check_external_links_rejects_missing_same_site_canonical_path(
    tmp_path: Path,
) -> None:
    site_dir = tmp_path / "site"
    _write(
        site_dir / "index.html",
        '<a href="https://docs.fullchaos.dev/missing/">missing</a>',
    )
    allowlist_path = tmp_path / "allowlist.yml"

    errors = check_external_links(
        site_dir,
        allowlist_path,
        _always_broken,
        date(2026, 7, 16),
        site_url="https://docs.fullchaos.dev",
    )

    assert errors == [
        "index.html: missing built page for https://docs.fullchaos.dev/missing/"
    ]


def test_check_external_links_rejects_missing_same_site_canonical_anchor(
    tmp_path: Path,
) -> None:
    site_dir = tmp_path / "site"
    _write(
        site_dir / "index.html",
        '<a href="https://docs.fullchaos.dev/guide/#missing">guide</a>',
    )
    _write(site_dir / "guide" / "index.html", '<section id="details"></section>')
    allowlist_path = tmp_path / "allowlist.yml"

    errors = check_external_links(
        site_dir,
        allowlist_path,
        _always_broken,
        date(2026, 7, 16),
        site_url="https://docs.fullchaos.dev",
    )

    assert errors == [
        "index.html: missing anchor 'missing' for https://docs.fullchaos.dev/guide/#missing"
    ]


def test_check_external_links_matches_equivalent_same_site_origins(
    tmp_path: Path,
) -> None:
    site_dir = tmp_path / "site"
    _write(
        site_dir / "index.html",
        '<a href="https://DOCS.FULLCHAOS.DEV:443/guide/#details">guide</a>',
    )
    _write(site_dir / "guide" / "index.html", '<section id="details"></section>')
    allowlist_path = tmp_path / "allowlist.yml"

    errors = check_external_links(
        site_dir,
        allowlist_path,
        _always_broken,
        date(2026, 7, 16),
        site_url="https://docs.fullchaos.dev",
    )

    assert errors == []


def test_main_accepts_legacy_external_link_checker_arguments(tmp_path: Path) -> None:
    site_dir = tmp_path / "site"
    _write(site_dir / "index.html", "")

    exit_code = main(["--built-site", str(site_dir)])

    assert exit_code == 0


def test_check_external_links_rejects_expired_same_site_allowlist_entry(
    tmp_path: Path,
) -> None:
    site_dir = tmp_path / "site"
    _write(
        site_dir / "index.html",
        '<a href="https://docs.fullchaos.dev/guide/">guide</a>',
    )
    _write(site_dir / "guide" / "index.html", "")
    allowlist_path = tmp_path / "allowlist.yml"
    allowlist_path.write_text(
        "- url: https://docs.fullchaos.dev/guide/\n"
        "  owner: docs-content\n"
        "  reason: temporary canonical migration\n"
        "  expires: '2026-01-01'\n",
        encoding="utf-8",
    )

    errors = check_external_links(
        site_dir,
        allowlist_path,
        _always_broken,
        date(2026, 7, 16),
        site_url="https://docs.fullchaos.dev",
    )

    assert errors == [
        "index.html: allowlist entry for https://docs.fullchaos.dev/guide/ expired on "
        "2026-01-01 (owner: docs-content, reason: temporary canonical migration)"
    ]
