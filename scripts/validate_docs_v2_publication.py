#!/usr/bin/env python3
"""Validate the v2 documentation candidate and emit the Phase 9 publication inventory."""

from __future__ import annotations

import csv
import json
import posixpath
import re
import sys
from collections import Counter
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import yaml

ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = ROOT / "mkdocs.prototype.yml"
PHASE9 = ROOT / ".github" / "documentation-program" / "phase-9"
IA_DIR = ROOT / ".github" / "documentation-program" / "ia"
MIGRATED_SOURCE_MAP_PATH = (
    ROOT / ".github" / "documentation-program" / "content" / "migrated-source-pages.json"
)
BUILD = ROOT / ".build"
LINK_RE = re.compile(r"!?\[[^\]]*\]\(([^)]+)\)")
FRONT_MATTER_RE = re.compile(r"\A---\s*\n(.*?)\n---\s*\n", re.DOTALL)


def _load_yaml(path: Path) -> dict[str, Any]:
    loaded = yaml.safe_load(path.read_text(encoding="utf-8"))
    return loaded if isinstance(loaded, dict) else {}


def _load_json_map(path: Path) -> dict[str, str]:
    loaded = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(loaded, dict):
        raise ValueError(f"Expected an object in {path}")
    return {str(key): str(value) for key, value in loaded.items()}


def _front_matter(path: Path) -> dict[str, Any]:
    match = FRONT_MATTER_RE.match(path.read_text(encoding="utf-8"))
    if not match:
        return {}
    loaded = yaml.safe_load(match.group(1))
    return loaded if isinstance(loaded, dict) else {}


def _flatten_nav(node: Any, trail: tuple[str, ...] = ()) -> list[dict[str, str]]:
    records: list[dict[str, str]] = []
    if isinstance(node, str):
        label = trail[-1] if trail else Path(node).stem.replace("-", " ").title()
        nav_path = " > ".join(trail) if trail else label
        return [
            {
                "source_path": node.lstrip("/"),
                "label": label,
                "nav_path": nav_path,
            }
        ]
    if isinstance(node, list):
        for child in node:
            records.extend(_flatten_nav(child, trail))
        return records
    if not isinstance(node, dict):
        return records
    for label, child in node.items():
        next_trail = (*trail, str(label))
        if isinstance(child, str):
            records.append(
                {
                    "source_path": child.lstrip("/"),
                    "label": str(label),
                    "nav_path": " > ".join(next_trail),
                }
            )
        else:
            records.extend(_flatten_nav(child, next_trail))
    return records


def _canonical_url(source_path: str) -> str:
    value = source_path.strip().lstrip("/")
    if value == "index.md":
        return "/"
    if value.endswith("/index.md"):
        return f"/{value[: -len('index.md')]}"
    if value.endswith(".md"):
        value = value[:-3]
    return f"/{value.strip('/')}/"


def _load_ia() -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for path in sorted(IA_DIR.glob("*.tsv")):
        with path.open(encoding="utf-8", newline="") as handle:
            rows.extend(csv.DictReader(handle, delimiter="\t"))
    return rows


def _expanded_candidates(targets: list[Path]) -> list[Path]:
    expanded: list[Path] = []
    for target in targets:
        expanded.append(target)
        if target.suffix == "":
            expanded.append(target.with_suffix(".md"))
            expanded.append(target / "index.md")
        elif target.suffix.lower() in {".html", ".htm"}:
            expanded.append(target.with_suffix(".md"))
    return expanded


def _resolve_source_relative(source_path: str, href: str) -> bool:
    parsed = urlparse(href)
    if parsed.scheme or parsed.netloc or href.startswith(("#", "mailto:", "tel:")):
        return True
    if not parsed.path:
        return True
    if parsed.path.startswith("/"):
        return False

    resolved = posixpath.normpath(
        posixpath.join(posixpath.dirname(source_path), parsed.path)
    )
    if resolved.startswith("../"):
        return False
    target = (ROOT / resolved).resolve()
    try:
        target.relative_to(ROOT.resolve())
    except ValueError:
        return False
    return any(candidate.exists() for candidate in _expanded_candidates([target]))


def _resolve_relative(
    page: Path,
    source_path: str,
    href: str,
    docs_dir: Path,
    migrated_sources: dict[str, str],
) -> bool:
    parsed = urlparse(href)
    if parsed.scheme or parsed.netloc or href.startswith(("#", "mailto:", "tel:")):
        return True
    path_part = parsed.path
    if not path_part:
        return True
    if path_part.startswith("/"):
        targets = [docs_dir / path_part.strip("/")]
    else:
        targets = [(page.parent / path_part).resolve()]
    if any(candidate.exists() for candidate in _expanded_candidates(targets)):
        return True

    migrated_source = migrated_sources.get(source_path)
    return bool(migrated_source and _resolve_source_relative(migrated_source, href))


def main() -> int:
    errors: list[str] = []
    config = _load_yaml(CONFIG_PATH)
    docs_dir = (ROOT / str(config.get("docs_dir") or "docs-prototype")).resolve()
    nav_records = _flatten_nav(config.get("nav", []))
    nav_paths = [record["source_path"] for record in nav_records]
    migrated_sources = _load_json_map(MIGRATED_SOURCE_MAP_PATH)
    migrated_urls = {_canonical_url(path) for path in migrated_sources}

    duplicate_paths = [path for path, count in Counter(nav_paths).items() if count > 1]
    if duplicate_paths:
        errors.append(f"duplicate navigation paths: {duplicate_paths}")

    missing = [path for path in nav_paths if not (docs_dir / path).is_file()]
    if missing:
        errors.append(f"navigation targets do not exist: {missing[:30]}")

    actual_markdown = {
        path.relative_to(docs_dir).as_posix() for path in docs_dir.rglob("*.md")
    }
    off_nav = sorted(actual_markdown - set(nav_paths))
    if off_nav:
        errors.append(f"unclassified Markdown outside navigation: {off_nav[:30]}")

    for target_path, source_path in sorted(migrated_sources.items()):
        if target_path not in actual_markdown:
            errors.append(f"migrated-source target does not exist: {target_path}")
        if target_path not in nav_paths:
            errors.append(f"migrated-source target is not navigated: {target_path}")
        if not (ROOT / source_path).is_file():
            errors.append(f"migrated-source input does not exist: {source_path}")

    ia_rows = _load_ia()
    ia_by_url = {row["url"]: row for row in ia_rows if row.get("url")}
    nav_urls = {_canonical_url(path) for path in nav_paths}
    non_ia = sorted(nav_urls - set(ia_by_url) - migrated_urls)
    if non_ia:
        errors.append(f"candidate URLs outside the approved IA: {non_ia[:30]}")

    page_ids: dict[str, str] = {}
    publication_rows: list[dict[str, str]] = []
    for record in nav_records:
        source_path = record["source_path"]
        page = docs_dir / source_path
        metadata = _front_matter(page) if page.is_file() else {}
        url = _canonical_url(source_path)
        ia = ia_by_url.get(url, {})
        migrated = source_path in migrated_sources
        page_id = str(
            metadata.get("page_id")
            or ia.get("id")
            or (f"migrated-{Path(source_path).stem}" if migrated else "")
        )
        if page_id:
            if page_id in page_ids:
                errors.append(
                    f"duplicate page_id {page_id}: {page_ids[page_id]} and {source_path}"
                )
            page_ids[page_id] = source_path
        publication_rows.append(
            {
                "page_id": page_id,
                "canonical_url": url,
                "source_path": source_path,
                "nav_path": record["nav_path"],
                "label": record["label"],
                "content_type": str(
                    metadata.get("content_type")
                    or ia.get("kind")
                    or ("migrated-source" if migrated else "")
                ),
                "owner": str(metadata.get("owner") or "documentation"),
                "lifecycle": str(metadata.get("lifecycle") or "active"),
                "publication_state": (
                    "public-migrated-source" if migrated else "public-candidate"
                ),
            }
        )

    for source_path in sorted(actual_markdown):
        page = docs_dir / source_path
        text = page.read_text(encoding="utf-8")
        for raw in LINK_RE.findall(text):
            href = raw.strip().split()[0].strip("<>")
            if not _resolve_relative(
                page,
                source_path,
                href,
                docs_dir,
                migrated_sources,
            ):
                errors.append(f"broken local link: {source_path} -> {raw}")

    redirects_path = PHASE9 / "redirects.tsv"
    redirects: list[dict[str, str]] = []
    if redirects_path.is_file():
        with redirects_path.open(encoding="utf-8", newline="") as handle:
            redirects = list(csv.DictReader(handle, delimiter="\t"))
        seen_sources: dict[str, str] = {}
        for row in redirects:
            source = row.get("source_path", "").strip()
            target = row.get("target_path", "").strip()
            if source in seen_sources and seen_sources[source] != target:
                errors.append(
                    f"redirect conflict for {source}: {seen_sources[source]} vs {target}"
                )
            seen_sources[source] = target
            if target not in ia_by_url and target not in migrated_urls:
                errors.append(
                    f"redirect target outside approved IA: {source} -> {target}"
                )

    BUILD.mkdir(parents=True, exist_ok=True)
    inventory_path = BUILD / "docs-v2-publication-inventory.tsv"
    inventory_fields = [
        "page_id",
        "canonical_url",
        "source_path",
        "nav_path",
        "label",
        "content_type",
        "owner",
        "lifecycle",
        "publication_state",
    ]
    with inventory_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle, fieldnames=inventory_fields, delimiter="\t", lineterminator="\n"
        )
        writer.writeheader()
        writer.writerows(sorted(publication_rows, key=lambda row: row["canonical_url"]))

    coverage_path = BUILD / "docs-v2-ia-coverage.tsv"
    with coverage_path.open("w", encoding="utf-8", newline="") as handle:
        fields = ["page_id", "canonical_url", "label", "kind", "state"]
        writer = csv.DictWriter(
            handle, fieldnames=fields, delimiter="\t", lineterminator="\n"
        )
        writer.writeheader()
        for row in sorted(ia_rows, key=lambda item: item.get("url", "")):
            url = row.get("url", "")
            writer.writerow(
                {
                    "page_id": row.get("id", ""),
                    "canonical_url": url,
                    "label": row.get("label", ""),
                    "kind": row.get("kind", ""),
                    "state": "implemented" if url in nav_urls else "withheld",
                }
            )

    summary = [
        "# Documentation v2 publication summary",
        "",
        f"- Public candidate pages: **{len(publication_rows)}**",
        f"- Frozen IA nodes: **{len(ia_rows)}**",
        f"- Direct source migrations: **{len(migrated_sources)}**",
        f"- Published candidate URLs: **{len(nav_urls)}**",
        f"- Withheld IA nodes: **{len(set(ia_by_url) - nav_urls)}**",
        f"- Legacy redirect sources: **{len(redirects)}**",
        f"- Unclassified Markdown pages: **{len(off_nav)}**",
        f"- Validation errors: **{len(errors)}**",
        "",
        "The current production documentation remains the WIP baseline. This inventory is the Phase 9 publication candidate for later quality and cutover gates.",
        "",
    ]
    (BUILD / "docs-v2-publication-summary.md").write_text(
        "\n".join(summary), encoding="utf-8"
    )

    if errors:
        print("\n".join(errors[:100]), file=sys.stderr)
        return 1
    print(
        f"Validated {len(publication_rows)} candidate pages, {len(redirects)} redirects, "
        f"{len(ia_rows)} frozen IA nodes, and {len(migrated_sources)} direct migrations."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
