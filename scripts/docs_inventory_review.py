#!/usr/bin/env python3
"""Extend the documentation inventory to every review-relevant source and gap.

The existing ``docs_inventory_v2`` module remains the factual scanner for the
canonical MkDocs site. This wrapper adds sources intentionally outside
``docs/``—the archived legacy tree, internal program evidence, repository entry
points, visual assets, and the known runtime publication gap—without turning
the inventory into a publishing framework.

Committed inventory output is excluded from subsequent scans so the inventory
cannot recursively inventory itself. The reviewed ``dev-health-web`` snapshot
is committed separately in the inventory directory and therefore no longer
appears as an unresolved external gap.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import sys
from collections import Counter
from pathlib import Path
from types import ModuleType
from typing import Any

import yaml

PYTHON_NAME_TAG = "tag:yaml.org,2002:python/name:"
IMAGE_SUFFIXES = {".png", ".jpg", ".jpeg", ".webp", ".svg", ".gif"}
LEGACY_ASSET_SUFFIXES = IMAGE_SUFFIXES | {".css", ".js"}
ENTRY_POINT_NAMES = {
    "README.md",
    "CONTRIBUTING.md",
    "AGENTS.md",
    "SECURITY.md",
    "CODE_OF_CONDUCT.md",
}


def _preserve_python_name(
    loader: yaml.SafeLoader,
    suffix: str,
    node: yaml.Node,
) -> str:
    """Represent MkDocs callable tags as inert dotted names."""

    del loader, node
    return suffix


def _install_tolerant_yaml_loader() -> None:
    """Load MkDocs tags safely and ignore horizontal-rule false positives."""

    yaml.SafeLoader.add_multi_constructor(PYTHON_NAME_TAG, _preserve_python_name)
    original_safe_load = yaml.safe_load

    def tolerant_safe_load(stream: Any) -> Any:
        try:
            return original_safe_load(stream)
        except yaml.YAMLError:
            return {}

    yaml.safe_load = tolerant_safe_load


def _load_base_module(repo_root: Path) -> ModuleType:
    _install_tolerant_yaml_loader()
    script = repo_root / "scripts" / "docs_inventory_v2.py"
    spec = importlib.util.spec_from_file_location("docs_inventory_v2", script)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to import {script}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _blank_row(
    repository: str,
    source_path: str,
    artifact_type: str,
    publication_classification: str,
    *,
    content_type: str | None = None,
    product_area: str | None = None,
    generated: bool = False,
    public_today: bool = False,
    notes: str | None = None,
) -> dict[str, Any]:
    return {
        "source_repo": repository,
        "source_path": source_path,
        "artifact_type": artifact_type,
        "current_url": None,
        "current_nav_location": [],
        "content_type": content_type,
        "primary_audience": None,
        "secondary_audiences": [],
        "product_area": product_area,
        "owner": None,
        "last_meaningful_review": None,
        "generated": generated,
        "public_today": public_today,
        "publication_classification": publication_classification,
        "duplicate_group": None,
        "known_accuracy_risk": None,
        "known_usability_risk": None,
        "build_dependencies": [],
        "links_in": [],
        "links_out": [],
        "notes": notes,
    }


def _add_file(
    rows_by_path: dict[str, dict[str, Any]],
    repository: str,
    repo_root: Path,
    path: Path,
    artifact_type: str,
    publication_classification: str,
    *,
    product_area: str,
    content_type: str | None = None,
    notes: str | None = None,
) -> None:
    source_path = path.relative_to(repo_root).as_posix()
    if source_path in rows_by_path:
        return
    rows_by_path[source_path] = _blank_row(
        repository,
        source_path,
        artifact_type,
        publication_classification,
        content_type=content_type,
        product_area=product_area,
        generated="generated" in source_path.lower(),
        notes=notes,
    )


def build_review_inventory(repo_root: Path, repository: str) -> dict[str, Any]:
    repo_root = repo_root.resolve()
    base = _load_base_module(repo_root)
    inventory = base.build_inventory(repo_root, repository)
    rows_by_path = {row["source_path"]: row for row in inventory["rows"]}

    legacy_root = repo_root / ".github" / "docs-legacy"
    if legacy_root.exists():
        for path in sorted(legacy_root.rglob("*")):
            if not path.is_file():
                continue
            suffix = path.suffix.lower()
            if suffix == ".md":
                artifact_type = "legacy-page"
                content_type = base._infer_content_type(
                    path.relative_to(legacy_root).as_posix(),
                    {},
                )
            elif suffix in LEGACY_ASSET_SUFFIXES:
                artifact_type = "legacy-asset"
                content_type = None
            else:
                artifact_type = "legacy-support"
                content_type = None
            _add_file(
                rows_by_path,
                repository,
                repo_root,
                path,
                artifact_type,
                "archived-source",
                product_area="documentation-legacy",
                content_type=content_type,
                notes="Preserved pre-cutover documentation source; not in the public build.",
            )

    program_root = repo_root / ".github" / "documentation-program"
    inventory_output_root = program_root / "inventory"
    if program_root.exists():
        for path in sorted(program_root.rglob("*")):
            if not path.is_file() or path.is_relative_to(inventory_output_root):
                continue
            _add_file(
                rows_by_path,
                repository,
                repo_root,
                path,
                "internal-program",
                "excluded-internal",
                product_area="documentation-program",
                notes="Internal remediation evidence; must never enter the public build.",
            )

    docs_root = repo_root / "docs"
    if docs_root.exists():
        for path in sorted(docs_root.rglob("*")):
            if path.is_file() and path.suffix.lower() in IMAGE_SUFFIXES:
                _add_file(
                    rows_by_path,
                    repository,
                    repo_root,
                    path,
                    "visual-asset",
                    "supporting-artifact",
                    product_area="documentation-assets",
                    notes=(
                        "Published documentation visual; disposition follows its owning page."
                    ),
                )

    for path in sorted(repo_root.iterdir()):
        if path.is_file() and path.name in ENTRY_POINT_NAMES:
            _add_file(
                rows_by_path,
                repository,
                repo_root,
                path,
                "repository-entry-point",
                "external-entry-point",
                product_area="repository-guidance",
                notes=(
                    "Public repository entry point; links into documentation "
                    "must be inventoried and updated at cutover."
                ),
            )

    for pattern in ("**/AGENTS*.md", "**/CONTRIBUTING*.md"):
        for path in sorted(repo_root.glob(pattern)):
            if path.is_file() and not path.is_relative_to(docs_root):
                _add_file(
                    rows_by_path,
                    repository,
                    repo_root,
                    path,
                    "repository-guidance",
                    "excluded-internal",
                    product_area="contributor-guidance",
                    notes=(
                        "Repository or agent guidance; evaluate for one "
                        "canonical contributor source."
                    ),
                )

    legacy_config = legacy_root / "mkdocs.yml"
    if legacy_config.is_file():
        _add_file(
            rows_by_path,
            repository,
            repo_root,
            legacy_config,
            "configuration",
            "archived-source",
            product_area="documentation-legacy",
            notes="Archived pre-cutover build configuration.",
        )

    for pattern in ("wrangler*.toml", "wrangler*.jsonc", "**/*cloudflare*.yml", "**/*cloudflare*.yaml"):
        for path in sorted(repo_root.glob(pattern)):
            if path.is_file():
                _add_file(
                    rows_by_path,
                    repository,
                    repo_root,
                    path,
                    "publication-configuration",
                    "supporting-artifact",
                    product_area="documentation-publication",
                    notes=(
                        "Cloudflare/publication configuration requiring an "
                        "explicit delivery disposition."
                    ),
                )

    live_preview_path = "external://dev-health-docs.fullchaos.workers.dev"
    rows_by_path.setdefault(
        live_preview_path,
        _blank_row(
            repository,
            live_preview_path,
            "external-gap",
            "gap-unverified",
            product_area="cross-repository-or-runtime",
            notes=(
                "The live Workers preview requires a URL, header, redirect, "
                "search, and publication-state crawl outside the repository inventory."
            ),
        ),
    )

    rows = [rows_by_path[path] for path in sorted(rows_by_path)]
    counts: Counter[str] = Counter()
    for row in rows:
        counts[row["publication_classification"]] += 1
        counts[f"artifact:{row['artifact_type']}"] += 1

    return {
        "schema_version": 2,
        "repository": repository,
        "generated_from": ".",
        "row_count": len(rows),
        "counts": dict(sorted(counts.items())),
        "rows": rows,
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

    inventory = build_review_inventory(args.root, args.repository)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(inventory, indent=2) + "\n", encoding="utf-8")
    print(f"wrote {inventory['row_count']} rows to {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
