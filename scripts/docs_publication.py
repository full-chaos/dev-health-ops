#!/usr/bin/env python3
"""Classify canonical Markdown pages for publication.

The canonical public tree lives under ``docs/`` and is navigated by ``mkdocs.yml``.
The former mixed public/internal corpus and its publication patterns are preserved
under ``.github/docs-legacy/`` for compatibility checks and historical evidence.

Classification algorithm (order matters):

1. A path reachable from ``mkdocs.yml``'s ``nav:`` tree is ``public-nav``.
   It is an error for a nav-reachable path to also match an
   ``excluded_internal`` pattern.
2. A path matching an ``excluded_internal`` glob is ``excluded-internal``.
3. A path matching a ``public_reference`` glob is ``public-reference``.
4. Anything else is unclassified and fails closed.
"""

from __future__ import annotations

import fnmatch
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import yaml

Classification = Literal["public-nav", "public-reference", "excluded-internal"]


class PublicationClassificationError(Exception):
    """Raised when a page has no publication disposition or a contradiction."""

    def __init__(self, path: str, reason: str) -> None:
        super().__init__(f"{path}: {reason}")
        self.path = path
        self.reason = reason


@dataclass(frozen=True, slots=True)
class PublicationManifest:
    excluded_internal: tuple[str, ...]
    public_reference: tuple[str, ...]


def load_manifest(manifest_path: Path) -> PublicationManifest:
    raw = yaml.safe_load(manifest_path.read_text(encoding="utf-8")) or {}
    if not isinstance(raw, dict):
        raise PublicationClassificationError(
            str(manifest_path), "publication.yml must parse to a mapping"
        )
    return PublicationManifest(
        excluded_internal=_string_list(raw, "excluded_internal", manifest_path),
        public_reference=_string_list(raw, "public_reference", manifest_path),
    )


def _string_list(
    raw: dict[str, object], key: str, manifest_path: Path
) -> tuple[str, ...]:
    value = raw.get(key)
    if value is None:
        return ()
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise PublicationClassificationError(
            str(manifest_path),
            f"publication.yml key '{key}' must be a YAML list of glob strings, "
            f"got {value!r}",
        )
    return tuple(value)


class _NavOnlyLoader(yaml.SafeLoader):
    """SafeLoader that tolerates inert MkDocs plugin-specific Python tags."""


def _ignore_unknown_tag(
    loader: yaml.SafeLoader, tag_suffix: str, node: yaml.Node
) -> None:
    del loader, tag_suffix, node
    return None


_NavOnlyLoader.add_multi_constructor(
    "tag:yaml.org,2002:python/name:", _ignore_unknown_tag
)


def load_nav_paths(mkdocs_yml_path: Path) -> frozenset[str]:
    raw = (
        yaml.load(  # noqa: S506 - SafeLoader plus one inert tag constructor
            mkdocs_yml_path.read_text(encoding="utf-8"), Loader=_NavOnlyLoader
        )
        or {}
    )
    return frozenset(_walk_nav(raw.get("nav") or []))


def _walk_nav(node: object) -> list[str]:
    paths: list[str] = []
    if isinstance(node, str):
        paths.append(node)
    elif isinstance(node, list):
        for item in node:
            paths.extend(_walk_nav(item))
    elif isinstance(node, dict):
        for value in node.values():
            paths.extend(_walk_nav(value))
    return paths


def _matches_any(relpath: str, patterns: tuple[str, ...]) -> bool:
    return any(fnmatch.fnmatch(relpath, pattern) for pattern in patterns)


def classify_doc(
    relpath: str, nav_paths: frozenset[str], manifest: PublicationManifest
) -> Classification:
    in_nav = relpath in nav_paths
    excluded = _matches_any(relpath, manifest.excluded_internal)

    if in_nav and excluded:
        raise PublicationClassificationError(
            relpath,
            "listed in mkdocs.yml nav but also matches an excluded_internal "
            "pattern in publication.yml",
        )
    if in_nav:
        return "public-nav"
    if excluded:
        return "excluded-internal"
    if _matches_any(relpath, manifest.public_reference):
        return "public-reference"
    raise PublicationClassificationError(
        relpath,
        "matches no publication.yml rule (excluded_internal, "
        "public_reference) and is not present in mkdocs.yml nav",
    )


def classify_all(
    docs_dir: Path, mkdocs_yml_path: Path, manifest_path: Path
) -> dict[str, Classification]:
    manifest = load_manifest(manifest_path)
    nav_paths = load_nav_paths(mkdocs_yml_path)
    result: dict[str, Classification] = {}
    for md_file in sorted(docs_dir.rglob("*.md")):
        relpath = md_file.relative_to(docs_dir).as_posix()
        result[relpath] = classify_doc(relpath, nav_paths, manifest)
    return result


def main(argv: list[str]) -> int:
    del argv
    root = Path(__file__).resolve().parents[1]
    try:
        classification = classify_all(
            root / "docs",
            root / "mkdocs.yml",
            root / ".github" / "docs-legacy" / "publication.yml",
        )
    except PublicationClassificationError as error:
        print(f"publication classification error: {error}", file=sys.stderr)
        return 1

    counts: dict[str, int] = {}
    for bucket in classification.values():
        counts[bucket] = counts.get(bucket, 0) + 1
    for bucket_name, count in sorted(counts.items()):
        print(f"{bucket_name}: {count}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
