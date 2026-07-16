#!/usr/bin/env python3
"""Validate external links referenced by a built docs site's HTML output.

Crawls a built MkDocs site directory for absolute ``http(s)://`` hrefs,
verifies each is reachable with bounded retries and a timeout, and treats a
``docs/external-link-allowlist.yml`` entry as an override only while its
``expires`` date has not passed. An expired allowlist entry fails the guard
even if the underlying URL happens to be reachable, so stale exceptions are
forced back into review rather than aging out silently.
"""

from __future__ import annotations

import argparse
import re
import sys
import urllib.error
import urllib.request
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_ALLOWLIST = ROOT / "docs" / "external-link-allowlist.yml"

HREF_RE = re.compile(r'<a\b[^>]*\bhref="(https?://[^"]+)"')
USER_AGENT = (
    "Mozilla/5.0 (compatible; dev-health-docs-link-check/1.0; "
    "+https://github.com/full-chaos/dev-health-ops)"
)

FetchResult = tuple[bool, str]
Fetcher = Callable[[str], FetchResult]


@dataclass(frozen=True, slots=True)
class AllowlistEntry:
    url: str
    owner: str
    reason: str
    expires: date


def load_allowlist(path: Path) -> dict[str, AllowlistEntry]:
    if not path.is_file():
        return {}
    raw = yaml.safe_load(path.read_text(encoding="utf-8")) or []
    if not isinstance(raw, list):
        raise ValueError(f"{path} must be a YAML list of entries")
    entries: dict[str, AllowlistEntry] = {}
    for item in raw:
        if not isinstance(item, dict):
            raise ValueError(f"{path} entries must be mappings, got {item!r}")
        url = item.get("url")
        owner = item.get("owner")
        reason = item.get("reason")
        expires_raw = item.get("expires")
        if (
            not isinstance(url, str)
            or not isinstance(owner, str)
            or not isinstance(reason, str)
            or not isinstance(expires_raw, str)
        ):
            raise ValueError(f"{path} entry missing required string fields: {item!r}")
        entries[url] = AllowlistEntry(
            url=url, owner=owner, reason=reason, expires=date.fromisoformat(expires_raw)
        )
    return entries


def iter_external_links(site_dir: Path) -> Iterable[tuple[Path, str]]:
    for html_path in sorted(site_dir.rglob("*.html")):
        text = html_path.read_text(encoding="utf-8")
        for match in HREF_RE.finditer(text):
            yield html_path, match.group(1)


def _fetch_with_retries(url: str, fetcher: Fetcher, max_retries: int) -> FetchResult:
    detail = "no attempts made"
    for _ in range(max_retries):
        ok, detail = fetcher(url)
        if ok:
            return True, detail
    return False, detail


def check_external_links(
    site_dir: Path,
    allowlist_path: Path,
    fetcher: Fetcher,
    today: date,
    max_retries: int = 3,
) -> list[str]:
    allowlist = load_allowlist(allowlist_path)
    errors: list[str] = []
    seen: set[str] = set()
    for source, url in iter_external_links(site_dir):
        if url in seen:
            continue
        seen.add(url)
        entry = allowlist.get(url)
        if entry is not None and entry.expires < today:
            errors.append(
                f"{source.relative_to(site_dir)}: allowlist entry for {url} expired on "
                f"{entry.expires.isoformat()} (owner: {entry.owner}, reason: {entry.reason})"
            )
            continue
        if entry is not None:
            continue
        ok, detail = _fetch_with_retries(url, fetcher, max_retries)
        if not ok:
            errors.append(
                f"{source.relative_to(site_dir)}: broken external link {url}: {detail}"
            )
    return errors


def default_fetcher(url: str, timeout_seconds: float = 10.0) -> FetchResult:
    for method in ("HEAD", "GET"):
        request = urllib.request.Request(
            url, method=method, headers={"User-Agent": USER_AGENT}
        )
        try:
            with urllib.request.urlopen(request, timeout=timeout_seconds) as response:  # noqa: S310 - link-check boundary
                if response.status < 400:
                    return True, f"status {response.status} ({method})"
                detail = f"status {response.status} ({method})"
        except urllib.error.HTTPError as error:
            if error.code < 400:
                return True, f"status {error.code} ({method})"
            detail = f"status {error.code} ({method})"
        except (urllib.error.URLError, TimeoutError, OSError) as error:
            detail = f"{error} ({method})"
    return False, detail


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(
        description="Check external links in a built docs site."
    )
    parser.add_argument("--built-site", type=Path, required=True)
    parser.add_argument("--allowlist", type=Path, default=DEFAULT_ALLOWLIST)
    args = parser.parse_args(argv)

    if not args.built_site.is_dir():
        print(
            f"ERROR: built site directory not found: {args.built_site}", file=sys.stderr
        )
        return 1

    errors = check_external_links(
        args.built_site, args.allowlist, default_fetcher, date.today()
    )
    if errors:
        for error in errors:
            print(f"ERROR: {error}")
        return 1
    print("External link check passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
