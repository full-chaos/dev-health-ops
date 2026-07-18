#!/usr/bin/env python3
"""Build a deterministic inventory of the Dev Health documentation system."""

from __future__ import annotations

import argparse
import fnmatch
import json
import re
from collections import defaultdict
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import cast
from urllib.parse import urlparse

import yaml

MARKDOWN_LINK_RE = re.compile(r"!?\[[^\]]*\]\(([^)]+)\)")
FRONT_MATTER_RE = re.compile(r"\A---\s*\n(.*?)\n---\s*\n", re.DOTALL)
DOC_TOOL_HINTS = (
    "docs",
    "documentation",
    "link",
    "freshness",
    "publication",
    "taxonomy",
    "evidence",
    "mkdocs",
)
ASSET_SUFFIXES = {".png", ".jpg", ".jpeg", ".webp", ".svg", ".gif"}
OVERRIDE_SUFFIXES = {".html", ".css", ".js", ".svg"}
CONFIG_PATHS = {
    "mkdocs.yml",
    "requirements-docs.txt",
    "docs/publication.yml",
    "docs/freshness-inventory.yml",
    "docs/search-acceptance.json",
    "Makefile",
}


@dataclass(slots=True)
class InventoryRow:
    source_repo: str
    source_path: str
    artifact_type: str
    current_url: str | None
    current_nav_location: list[str]
    content_type: str | None
    primary_audience: str | None
    secondary_audiences: list[str]
    product_area: str | None
    owner: str | None
    last_meaningful_review: str | None
    generated: bool
    public_today: bool
    publication_classification: str
    duplicate_group: str | None
    known_accuracy_risk: str | None
    known_usability_risk: str | None
    build_dependencies: list[str]
    links_in: list[str]
    links_out: list[str]
    notes: str | None


def _load_yaml(path: Path) -> dict[str, object]:
    if not path.exists():
        return {}
    loaded = yaml.safe_load(path.read_text(encoding="utf-8"))
    return cast(dict[str, object], loaded) if isinstance(loaded, dict) else {}


def _normalise_doc_path(path: str) -> str:
    value = path.strip().lstrip("/")
    return f"{value}index.md" if value.endswith("/") else value


def _flatten_nav(node: object, trail: tuple[str, ...] = ()) -> dict[str, list[str]]:
    result: dict[str, list[str]] = {}
    if isinstance(node, list):
        for child in node:
            result.update(_flatten_nav(child, trail))
        return result
    if not isinstance(node, dict):
        return result
    for label, child in node.items():
        next_trail = (*trail, str(label))
        if isinstance(child, str):
            result[_normalise_doc_path(child)] = list(next_trail)
        else:
            result.update(_flatten_nav(child, next_trail))
    return result


def _extract_front_matter(text: str) -> dict[str, object]:
    match = FRONT_MATTER_RE.match(text)
    if not match:
        return {}
    parsed = yaml.safe_load(match.group(1))
    return cast(dict[str, object], parsed) if isinstance(parsed, dict) else {}


def _pattern_lines(value: object) -> list[str]:
    if isinstance(value, str):
        return [
            line.strip()
            for line in value.splitlines()
            if line.strip() and not line.lstrip().startswith("#")
        ]
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    return []


def _matches_any(path: str, patterns: list[str]) -> bool:
    for pattern in patterns:
        if fnmatch.fnmatch(path, pattern):
            return True
        if pattern.startswith("**/") and fnmatch.fnmatch(path, pattern[3:]):
            return True
        if pattern.endswith("/") and path.startswith(pattern):
            return True
    return False


def _publication_classification(
    rel_doc_path: str,
    nav_paths: set[str],
    excluded_patterns: list[str],
    manifest: dict[str, object],
) -> str:
    if rel_doc_path in nav_paths:
        return "public-nav"
    if _matches_any(rel_doc_path, excluded_patterns):
        return "excluded-internal"
    if _matches_any(rel_doc_path, _pattern_lines(manifest.get("excluded_internal"))):
        return "excluded-internal"
    if _matches_any(rel_doc_path, _pattern_lines(manifest.get("public_reference"))):
        return "public-reference"
    return "unclassified"


def _infer_content_type(path: str, front_matter: dict[str, object]) -> str:
    explicit = front_matter.get("content_type") or front_matter.get("template")
    if explicit:
        return str(explicit).removesuffix(".html")

    lowered = path.lower()
    name = Path(lowered).name
    if "troubleshoot" in lowered or "runbook" in lowered:
        return "troubleshooting"
    if "/api/" in f"/{lowered}" or name.startswith("api-"):
        return "api-reference"
    if "cli" in lowered or "command" in name:
        return "cli-reference"
    if "configuration" in lowered or "config" in name:
        return "configuration-reference"
    if "architecture" in lowered or "/adr/" in f"/{lowered}":
        return "architecture"
    if "getting-started" in lowered or "quickstart" in lowered or "first-" in name:
        return "tutorial"
    if "glossary" in lowered or "taxonomy" in lowered or "metrics" in lowered:
        return "reference"
    if "user-guide" in lowered or "journey" in lowered:
        return "task-guide"
    return "landing" if name == "index.md" else "explanation"


def _infer_product_area(path: str) -> str | None:
    parts = Path(path).parts
    if not parts:
        return None
    if parts[0] == "customer-push-ingestion":
        return "integrations"
    return parts[0]


def _resolve_local_link(source: str, href: str) -> str | None:
    parsed = urlparse(href.strip())
    if parsed.scheme or parsed.netloc or href.startswith(("#", "mailto:")):
        return None
    if not parsed.path:
        return None

    source_dir = Path(source).parent
    target = (
        Path(parsed.path.lstrip("/"))
        if parsed.path.startswith("/")
        else source_dir / parsed.path
    )
    collapsed: list[str] = []
    for part in target.parts:
        if part == ".":
            continue
        if part == "..":
            if collapsed:
                collapsed.pop()
            continue
        collapsed.append(part)
    target = Path(*collapsed)

    if target.suffix == "":
        target /= "index.md"
    elif target.suffix.lower() in {".html", ".htm"}:
        target = target.with_suffix(".md")
    return target.as_posix()


def _extract_links(source: str, text: str) -> list[str]:
    links = {
        target
        for href in MARKDOWN_LINK_RE.findall(text)
        if (target := _resolve_local_link(source, href)) is not None
    }
    return sorted(links)


def _doc_url(site_url: str | None, rel_doc_path: str) -> str | None:
    if not site_url:
        return None
    if rel_doc_path == "index.md":
        suffix = ""
    elif rel_doc_path.endswith("/index.md"):
        suffix = rel_doc_path[: -len("index.md")]
    else:
        suffix = f"{rel_doc_path.removesuffix('.md')}/"
    return f"{site_url.rstrip('/')}/{suffix}"


def _is_generated(path: str, text: str) -> bool:
    markers = (
        "generated file",
        "do not edit",
        "generated from",
        "auto-generated",
        "autogenerated",
    )
    lowered = path.lower()
    return any(marker in text[:2000].lower() for marker in markers) or any(
        token in lowered for token in ("generated", ".golden.", "fixture")
    )


def _docs_artifacts(repo_root: Path) -> list[Path]:
    candidates = {
        repo_root / path for path in CONFIG_PATHS if (repo_root / path).is_file()
    }

    overrides = repo_root / "docs" / "overrides"
    if overrides.exists():
        candidates.update(
            path
            for path in overrides.rglob("*")
            if path.is_file() and path.suffix.lower() in OVERRIDE_SUFFIXES
        )

    docs_qa = repo_root / "docs-qa"
    if docs_qa.exists():
        candidates.update(
            path
            for path in docs_qa.rglob("*")
            if path.is_file() and "node_modules" not in path.parts
        )

    scripts = repo_root / "scripts"
    if scripts.exists():
        candidates.update(
            path
            for path in scripts.iterdir()
            if path.is_file()
            and any(hint in path.name.lower() for hint in DOC_TOOL_HINTS)
        )

    workflows = repo_root / ".github" / "workflows"
    if workflows.exists():
        candidates.update(
            path
            for path in workflows.iterdir()
            if path.is_file() and "docs" in path.name.lower()
        )
    return sorted(candidates)


def build_inventory(repo_root: Path, repository_name: str) -> dict[str, object]:
    repo_root = repo_root.resolve()
    mkdocs = _load_yaml(repo_root / "mkdocs.yml")
    manifest = _load_yaml(repo_root / "docs" / "publication.yml")
    nav = _flatten_nav(mkdocs.get("nav", []))
    site_url_value = str(mkdocs.get("site_url") or "").strip()
    site_url = site_url_value or None
    excluded_patterns = _pattern_lines(mkdocs.get("exclude_docs"))

    docs_root = repo_root / "docs"
    markdown_paths = sorted(docs_root.rglob("*.md")) if docs_root.exists() else []
    page_text = {
        path.relative_to(docs_root).as_posix(): path.read_text(encoding="utf-8")
        for path in markdown_paths
    }
    page_links = {path: _extract_links(path, text) for path, text in page_text.items()}
    inbound: defaultdict[str, set[str]] = defaultdict(set)
    for source, targets in page_links.items():
        for target in targets:
            inbound[target].add(source)

    rows: list[InventoryRow] = []
    for path in markdown_paths:
        rel = path.relative_to(docs_root).as_posix()
        text = page_text[rel]
        front_matter = _extract_front_matter(text)
        classification = _publication_classification(
            rel,
            set(nav),
            excluded_patterns,
            manifest,
        )
        secondary = front_matter.get("secondary_audiences", [])
        if isinstance(secondary, str):
            secondary_audiences = [secondary]
        elif isinstance(secondary, list):
            secondary_audiences = [str(value) for value in secondary]
        else:
            secondary_audiences = []

        audience = front_matter.get("audience")
        owner = front_matter.get("owner")
        last_reviewed = front_matter.get("last-reviewed")
        rows.append(
            InventoryRow(
                source_repo=repository_name,
                source_path=f"docs/{rel}",
                artifact_type="markdown-page",
                current_url=_doc_url(site_url, rel),
                current_nav_location=nav.get(rel, []),
                content_type=_infer_content_type(rel, front_matter),
                primary_audience=str(audience) if audience else None,
                secondary_audiences=secondary_audiences,
                product_area=_infer_product_area(rel),
                owner=str(owner) if owner else None,
                last_meaningful_review=str(last_reviewed) if last_reviewed else None,
                generated=_is_generated(rel, text),
                public_today=classification in {"public-nav", "public-reference"},
                publication_classification=classification,
                duplicate_group=None,
                known_accuracy_risk=None,
                known_usability_risk=None,
                build_dependencies=[],
                links_in=sorted(inbound.get(rel, set())),
                links_out=page_links.get(rel, []),
                notes=None,
            )
        )

    for path in _docs_artifacts(repo_root):
        rel = path.relative_to(repo_root).as_posix()
        if rel.startswith("docs/") and rel.endswith(".md"):
            continue
        suffix = path.suffix.lower()
        if suffix in ASSET_SUFFIXES:
            artifact_type = "visual-asset"
        elif rel.startswith("docs/overrides/"):
            artifact_type = "theme-override"
        elif rel.startswith("docs-qa/"):
            artifact_type = "browser-qa"
        elif rel.startswith(".github/workflows/"):
            artifact_type = "workflow"
        elif rel in CONFIG_PATHS:
            artifact_type = "configuration"
        else:
            artifact_type = "tooling"
        rows.append(
            InventoryRow(
                source_repo=repository_name,
                source_path=rel,
                artifact_type=artifact_type,
                current_url=None,
                current_nav_location=[],
                content_type=None,
                primary_audience=None,
                secondary_audiences=[],
                product_area="documentation-system",
                owner=None,
                last_meaningful_review=None,
                generated=False,
                public_today=False,
                publication_classification="supporting-artifact",
                duplicate_group=None,
                known_accuracy_risk=None,
                known_usability_risk=None,
                build_dependencies=[],
                links_in=[],
                links_out=[],
                notes=None,
            )
        )

    serialised = [
        asdict(row) for row in sorted(rows, key=lambda item: item.source_path)
    ]
    counts: defaultdict[str, int] = defaultdict(int)
    for row in serialised:
        classification = cast(str, row["publication_classification"])
        artifact_type = cast(str, row["artifact_type"])
        counts[classification] += 1
        counts[f"artifact:{artifact_type}"] += 1

    return {
        "schema_version": 1,
        "repository": repository_name,
        "generated_from": ".",
        "row_count": len(serialised),
        "counts": dict(sorted(counts.items())),
        "rows": serialised,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=Path("."))
    parser.add_argument("--repository", default="full-chaos/dev-health-ops")
    parser.add_argument(
        "--output",
        type=Path,
        default=Path(".build/documentation-inventory.json"),
    )
    args = parser.parse_args()

    inventory = build_inventory(args.root, args.repository)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(inventory, indent=2) + "\n", encoding="utf-8")
    print(f"wrote {inventory['row_count']} rows to {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
