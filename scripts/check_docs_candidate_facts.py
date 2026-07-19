#!/usr/bin/env python3
"""Protect objective Investment taxonomy facts used by the docs candidate."""

from __future__ import annotations

import ast
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
REGISTRY = ROOT / "src" / "dev_health_ops" / "investment_taxonomy.py"
DOCUMENT = ROOT / "docs-prototype" / "reference" / "taxonomies" / "investment.md"
KEY_RE = re.compile(r"`([a-z][a-z_]*(?:\.[a-z][a-z_]*)?)`")


def _literal_set(path: Path, name: str) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    for node in tree.body:
        value: ast.expr | None = None
        target_matches = False
        if isinstance(node, ast.Assign):
            target_matches = any(
                isinstance(target, ast.Name) and target.id == name
                for target in node.targets
            )
            value = node.value
        elif isinstance(node, ast.AnnAssign):
            target_matches = isinstance(node.target, ast.Name) and node.target.id == name
            value = node.value
        if not target_matches or value is None:
            continue
        if not isinstance(value, ast.Set):
            raise ValueError(f"{name} is not a set literal in {path}")
        result: set[str] = set()
        for element in value.elts:
            if not isinstance(element, ast.Constant) or not isinstance(
                element.value, str
            ):
                raise ValueError(f"{name} contains a non-string literal")
            result.add(element.value)
        return result
    raise ValueError(f"{name} not found in {path}")


def main() -> int:
    if not REGISTRY.is_file() or not DOCUMENT.is_file():
        print("ERROR: candidate taxonomy source or page is missing")
        return 1

    try:
        themes = _literal_set(REGISTRY, "THEMES")
        subcategories = _literal_set(REGISTRY, "SUBCATEGORIES")
    except (OSError, SyntaxError, ValueError) as exc:
        print(f"ERROR: {exc}")
        return 1

    documented = set(KEY_RE.findall(DOCUMENT.read_text(encoding="utf-8")))
    expected = themes | subcategories
    missing = expected - documented
    unknown = {
        key
        for key in documented - expected
        if "." in key or key in themes
    }

    errors: list[str] = []
    if missing:
        errors.append(f"missing canonical taxonomy keys: {sorted(missing)}")
    if unknown:
        errors.append(f"unknown taxonomy keys: {sorted(unknown)}")

    if errors:
        for error in errors:
            print(f"ERROR: {error}")
        return 1

    print(
        "Candidate fact drift check passed for "
        f"{len(themes)} themes and {len(subcategories)} subcategories"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
