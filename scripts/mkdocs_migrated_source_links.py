"""MkDocs hook for pages copied directly from the legacy documentation tree.

The migrated page body remains byte-for-byte equivalent to its source document.
Relative links are rewritten to the corresponding repository source URL at render
time so the public candidate does not publish broken paths while the remaining
legacy pages are migrated into the canonical information architecture.
"""

from __future__ import annotations

import json
import posixpath
import re
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit, urlunsplit

ROOT = Path(__file__).resolve().parents[1]
MAPPING_PATH = (
    ROOT
    / ".github"
    / "documentation-program"
    / "content"
    / "migrated-source-pages.json"
)
REPOSITORY_BLOB_ORIGIN = "https://github.com/full-chaos/dev-health-ops/blob/main"
REPOSITORY_RAW_ORIGIN = (
    "https://raw.githubusercontent.com/full-chaos/dev-health-ops/main"
)
INLINE_LINK_RE = re.compile(
    r"(?P<prefix>!?\[[^\]\n]*\]\()(?P<destination>[^)\n]+)(?P<suffix>\))"
)
REFERENCE_LINK_RE = re.compile(
    r"^(?P<prefix>\s*\[[^\]]+\]:\s*)(?P<destination>\S+)(?P<suffix>.*)$"
)
EMPTY_ANCHOR_RE = re.compile(
    r'<a\s+id=(?P<quote>["\'])(?P<id>[^"\']+)(?P=quote)\s*></a>',
    re.IGNORECASE,
)


def _load_mapping() -> dict[str, str]:
    loaded = json.loads(MAPPING_PATH.read_text(encoding="utf-8"))
    if not isinstance(loaded, dict):
        raise ValueError(f"Expected an object in {MAPPING_PATH}")
    return {str(key): str(value) for key, value in loaded.items()}


MIGRATED_SOURCE_PAGES = _load_mapping()


def _split_destination(value: str) -> tuple[str, str]:
    stripped = value.strip()
    if stripped.startswith("<") and ">" in stripped:
        end = stripped.index(">") + 1
        return stripped[:end], stripped[end:]
    parts = stripped.split(maxsplit=1)
    return parts[0], f" {parts[1]}" if len(parts) == 2 else ""


def _rewrite_url(url: str, source_path: str, *, image: bool) -> str:
    wrapped = url.startswith("<") and url.endswith(">")
    bare = url[1:-1] if wrapped else url
    parsed = urlsplit(bare)
    if (
        parsed.scheme
        or parsed.netloc
        or bare.startswith(("#", "/", "mailto:", "tel:"))
        or not parsed.path
    ):
        return url

    resolved = posixpath.normpath(
        posixpath.join(posixpath.dirname(source_path), parsed.path)
    )
    if resolved.startswith("../"):
        return url

    origin = REPOSITORY_RAW_ORIGIN if image else REPOSITORY_BLOB_ORIGIN
    origin_parts = urlsplit(origin)
    rewritten = urlunsplit(
        (
            origin_parts.scheme,
            origin_parts.netloc,
            f"{origin_parts.path}/{resolved}",
            parsed.query,
            parsed.fragment,
        )
    )
    return f"<{rewritten}>" if wrapped else rewritten


def _rewrite_markdown(markdown: str, source_path: str) -> str:
    output: list[str] = []
    fence: str | None = None

    for line in markdown.splitlines(keepends=True):
        stripped = line.lstrip()
        if stripped.startswith(("```", "~~~")):
            marker = stripped[:3]
            fence = None if fence == marker else marker if fence is None else fence
            output.append(line)
            continue
        if fence is not None:
            output.append(line)
            continue

        line = EMPTY_ANCHOR_RE.sub(
            lambda match: f'<span id="{match.group("id")}"></span>',
            line,
        )

        reference = REFERENCE_LINK_RE.match(line)
        if reference:
            destination, title = _split_destination(reference.group("destination"))
            rewritten = _rewrite_url(destination, source_path, image=False)
            output.append(
                f"{reference.group('prefix')}{rewritten}{title}"
                f"{reference.group('suffix')}"
            )
            continue

        def replace_inline(match: re.Match[str]) -> str:
            destination, title = _split_destination(match.group("destination"))
            rewritten = _rewrite_url(
                destination,
                source_path,
                image=match.group("prefix").startswith("!["),
            )
            return (
                f"{match.group('prefix')}{rewritten}{title}{match.group('suffix')}"
            )

        output.append(INLINE_LINK_RE.sub(replace_inline, line))

    return "".join(output)


def on_page_markdown(
    markdown: str,
    page: Any,
    config: Any,
    files: Any,
) -> str:
    """Rewrite source-relative links for explicitly migrated pages."""

    source_path = MIGRATED_SOURCE_PAGES.get(page.file.src_path)
    if not source_path:
        return markdown
    return _rewrite_markdown(markdown, source_path)
