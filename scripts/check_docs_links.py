#!/usr/bin/env python3
"""Check relative markdown links and anchors under docs/."""

from __future__ import annotations

import re
from pathlib import Path
from urllib.parse import unquote, urlsplit

ROOT = Path(__file__).resolve().parents[1]
DOCS_ROOT = ROOT / "docs"

INLINE_LINK_RE = re.compile(r"(?<!!)\[[^\]\n]+\]\(([^)\s]+)(?:\s+\"[^\"]*\")?\)")
REFERENCE_DEF_RE = re.compile(r"^\[[^\]]+\]:\s+(\S+)", re.MULTILINE)
HTML_ID_RE = re.compile(r"\bid=[\"']([^\"']+)[\"']")
HEADING_RE = re.compile(r"^(#{1,6})\s+(.+?)\s*#*\s*$")


def slugify(heading: str) -> str:
    heading = re.sub(r"<[^>]+>", "", heading)
    heading = re.sub(r"`([^`]*)`", r"\1", heading)
    heading = heading.strip().lower()
    heading = re.sub(r"[^\w\s-]", "", heading)
    heading = re.sub(r"[\s_-]+", "-", heading).strip("-")
    return heading


def anchors_for(path: Path) -> set[str]:
    anchors = {""}
    text = path.read_text(encoding="utf-8")
    counts: dict[str, int] = {}
    for line in text.splitlines():
        match = HEADING_RE.match(line)
        if not match:
            continue
        base = slugify(match.group(2))
        if not base:
            continue
        count = counts.get(base, 0)
        counts[base] = count + 1
        anchors.add(base if count == 0 else f"{base}-{count}")
    anchors.update(HTML_ID_RE.findall(text))
    return anchors


def iter_links(path: Path) -> list[str]:
    text = path.read_text(encoding="utf-8")
    return [*INLINE_LINK_RE.findall(text), *REFERENCE_DEF_RE.findall(text)]


def should_skip(target: str) -> bool:
    if not target or target.startswith(("http://", "https://", "mailto:", "tel:")):
        return True
    if target.startswith("#"):
        return False
    parsed = urlsplit(target)
    return bool(parsed.scheme or parsed.netloc) or target.startswith("/")


def check_link(
    source: Path, raw_target: str, anchor_cache: dict[Path, set[str]]
) -> str | None:
    if should_skip(raw_target):
        return None
    parsed = urlsplit(raw_target)
    target_path = unquote(parsed.path)
    anchor = unquote(parsed.fragment)

    if target_path and not target_path.endswith(".md"):
        return None

    destination = source if not target_path else (source.parent / target_path).resolve()
    try:
        destination.relative_to(DOCS_ROOT)
    except ValueError:
        return None

    if not destination.exists():
        return f"{source.relative_to(ROOT)} -> {raw_target}: missing file"
    if destination.suffix != ".md":
        return None

    if anchor:
        anchors = anchor_cache.setdefault(destination, anchors_for(destination))
        if anchor not in anchors:
            return f"{source.relative_to(ROOT)} -> {raw_target}: missing anchor"
    return None


def main() -> int:
    errors: list[str] = []
    anchor_cache: dict[Path, set[str]] = {}
    for path in sorted(DOCS_ROOT.rglob("*.md")):
        for target in iter_links(path):
            error = check_link(path, target, anchor_cache)
            if error:
                errors.append(error)
    if errors:
        for error in errors:
            print(f"ERROR: {error}")
        return 1
    print("Docs link check passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
