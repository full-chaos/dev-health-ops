#!/usr/bin/env python3
"""Crawl a built MkDocs site's HTML for broken internal links, anchors, and assets.

This complements ``check_docs_links.py`` (which checks relative Markdown
links in the source tree) by validating the actual rendered HTML output:
stylesheet/script/image ``src`` references, cross-page ``href`` targets, and
in-page anchor fragments must all resolve to files that exist inside the
built site.
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path
from urllib.parse import unquote, urlsplit

ID_RE = re.compile(r'\bid="([^"]+)"')
INTERNAL_REF_RE = re.compile(r'\b(?:href|src)="([^"]+)"')


def anchors_for(html_path: Path) -> set[str]:
    return set(ID_RE.findall(html_path.read_text(encoding="utf-8")))


def should_skip(target: str) -> bool:
    if not target or target.startswith(
        ("http://", "https://", "mailto:", "tel:", "javascript:")
    ):
        return True
    parsed = urlsplit(target)
    return bool(parsed.scheme or parsed.netloc)


def resolve_destination(
    html_path: Path, site_dir: Path, target_path_part: str
) -> Path | None:
    destination = (
        (html_path.parent / target_path_part).resolve()
        if target_path_part
        else html_path
    )
    try:
        destination.relative_to(site_dir.resolve())
    except ValueError:
        return None
    if destination.is_dir():
        destination = destination / "index.html"
    return destination


def check_built_site(site_dir: Path) -> list[str]:
    errors: list[str] = []
    anchor_cache: dict[Path, set[str]] = {}
    for html_path in sorted(site_dir.rglob("*.html")):
        text = html_path.read_text(encoding="utf-8")
        for raw_target in INTERNAL_REF_RE.findall(text):
            if should_skip(raw_target):
                continue
            parsed = urlsplit(raw_target)
            target_path_part = unquote(parsed.path)
            anchor = unquote(parsed.fragment)
            if not target_path_part and not anchor:
                continue
            destination = resolve_destination(html_path, site_dir, target_path_part)
            if destination is None:
                continue
            if not destination.exists():
                errors.append(
                    f"{html_path.relative_to(site_dir)} -> {raw_target}: missing built asset"
                )
                continue
            if anchor and destination.suffix == ".html":
                anchors = anchor_cache.setdefault(destination, anchors_for(destination))
                if anchor not in anchors:
                    errors.append(
                        f"{html_path.relative_to(site_dir)} -> {raw_target}: missing anchor '{anchor}'"
                    )
    return errors


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(
        description="Crawl a built MkDocs site for broken internal links, anchors, and assets."
    )
    parser.add_argument("--site-dir", type=Path, required=True)
    args = parser.parse_args(argv)

    if not args.site_dir.is_dir():
        print(
            f"ERROR: built site directory not found: {args.site_dir}", file=sys.stderr
        )
        return 1

    errors = check_built_site(args.site_dir)
    if errors:
        for error in errors:
            print(f"ERROR: {error}")
        return 1
    print("Built-site link check passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
