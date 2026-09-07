#!/usr/bin/env python3
"""Check relative Markdown links and anchors under the canonical docs tree."""

from __future__ import annotations

import re
import unicodedata
from pathlib import Path
from urllib.parse import unquote, urlsplit

ROOT = Path(__file__).resolve().parents[1]
DOCS_ROOT = ROOT / "docs"

INLINE_LINK_RE = re.compile(r"(?<!!)\[[^\]\n]+\]\(([^)\s]+)(?:\s+\"[^\"]*\")?\)")
REFERENCE_DEF_RE = re.compile(r"^\[[^\]]+\]:\s+(\S+)", re.MULTILINE)
HTML_ID_RE = re.compile(r"\bid=[\"']([^\"']+)[\"']")
HEADING_RE = re.compile(r"^(#{1,6})\s+(.+?)\s*#*\s*$")
FENCE_RE = re.compile(r"^\s*(```|~~~)")


def slugify(heading: str) -> str:
    """Mirror mkdocs' real anchor algorithm (python-markdown's `toc`
    extension, `markdown.extensions.toc.slugify`, `unicode=False` -- this
    repo's mkdocs.yml does not opt into unicode slugs), so an anchor that
    passes this checker matches what `mkdocs build` actually renders and
    what `tests/docs/test_built_site_links.py` checks against real built
    HTML (CHAOS-5417/CHAOS-5439: the old regex here additionally folded
    `_` into `-`, which markdown's own slugify never does -- an anchor
    that satisfied only THIS checker then 404'd on the real built site).

    HTML-tag and backtick stripping happens here because this function
    works from the raw markdown heading text, not the HTML this extension
    receives internally after markdown has already rendered inline code
    spans -- that pre-step has no equivalent in the upstream function to
    mirror.
    """
    heading = re.sub(r"<[^>]+>", "", heading)
    heading = re.sub(r"`([^`]*)`", r"\1", heading)
    # unicode=False path: fold to the closest ASCII form first (accents
    # stripped, e.g. "café" -> "cafe"), exactly as upstream does before
    # its own punctuation strip.
    heading = unicodedata.normalize("NFKD", heading)
    heading = heading.encode("ascii", "ignore").decode("ascii")
    heading = re.sub(r"[^\w\s-]", "", heading).strip().lower()
    # Only whitespace/hyphen RUNS collapse to a single hyphen -- `_` is
    # `\w` and passes through untouched, matching upstream exactly
    # (upstream does not strip a leading/trailing hyphen either; neither
    # does this).
    return re.sub(r"[-\s]+", "-", heading)


def anchors_for(path: Path) -> set[str]:
    anchors = {""}
    text = path.read_text(encoding="utf-8")
    counts: dict[str, int] = {}
    fence: str | None = None
    for line in text.splitlines():
        fence_match = FENCE_RE.match(line)
        if fence_match:
            marker = fence_match.group(1)
            fence = None if fence == marker else (fence or marker)
            continue
        if fence is not None:
            # `#` inside a fenced block is shell syntax, not a heading.
            continue
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
    source: Path,
    raw_target: str,
    anchor_cache: dict[Path, set[str]],
    docs_root: Path,
    root: Path,
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
        destination.relative_to(docs_root)
    except ValueError:
        return None

    if not destination.exists():
        return f"{source.relative_to(root)} -> {raw_target}: missing file"
    if destination.suffix != ".md":
        return None

    if anchor:
        anchors = anchor_cache.setdefault(destination, anchors_for(destination))
        if anchor not in anchors:
            return f"{source.relative_to(root)} -> {raw_target}: missing anchor"
    return None


def check_docs(docs_root: Path, root: Path) -> list[str]:
    errors: list[str] = []
    anchor_cache: dict[Path, set[str]] = {}
    for path in sorted(docs_root.rglob("*.md")):
        for target in iter_links(path):
            error = check_link(path, target, anchor_cache, docs_root, root)
            if error:
                errors.append(error)
    return errors


def main() -> int:
    errors = check_docs(DOCS_ROOT, ROOT)
    if errors:
        for error in errors:
            print(f"ERROR: {error}")
        return 1
    print("Docs link check passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
