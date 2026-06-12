#!/usr/bin/env python3
"""Render the generated taxonomy key block in investment docs."""

from __future__ import annotations

import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
TAXONOMY_PATH = ROOT / "src" / "dev_health_ops" / "investment_taxonomy.py"
DOC_PATH = ROOT / "docs" / "product" / "investment-taxonomy.md"

BEGIN = "<!-- BEGIN GENERATED TAXONOMY -->"
END = "<!-- END GENERATED TAXONOMY -->"


def _literal_set_order(source: str, name: str) -> list[str]:
    tree = ast.parse(source)
    for node in tree.body:
        if isinstance(node, ast.Assign):
            target_matches = any(
                isinstance(target, ast.Name) and target.id == name
                for target in node.targets
            )
            value = node.value
        elif isinstance(node, ast.AnnAssign) and node.value is not None:
            target_matches = (
                isinstance(node.target, ast.Name) and node.target.id == name
            )
            value = node.value
        else:
            continue
        if not target_matches:
            continue
        if not isinstance(value, ast.Set):
            raise ValueError(f"{name} is not a set literal in {TAXONOMY_PATH}")
        values: list[str] = []
        for element in value.elts:
            if not isinstance(element, ast.Constant) or not isinstance(
                element.value, str
            ):
                raise ValueError(f"{name} contains a non-string literal")
            values.append(element.value)
        return values
    raise ValueError(f"{name} not found in {TAXONOMY_PATH}")


def load_taxonomy() -> tuple[list[str], list[str], dict[str, str]]:
    source = TAXONOMY_PATH.read_text(encoding="utf-8")
    themes = _literal_set_order(source, "THEMES")
    subcategories = _literal_set_order(source, "SUBCATEGORIES")
    theme_set = set(themes)

    mapping: dict[str, str] = {}
    for subcategory in subcategories:
        theme, separator, _ = subcategory.partition(".")
        if not separator:
            raise ValueError(f"subcategory key lacks theme prefix: {subcategory}")
        if theme not in theme_set:
            raise ValueError(
                f"subcategory {subcategory} maps to unknown theme prefix {theme}"
            )
        mapping[subcategory] = theme
    return themes, subcategories, mapping


def render_block(
    themes: list[str], subcategories: list[str], mapping: dict[str, str]
) -> str:
    lines = [
        BEGIN,
        "```text",
        "# THEMES",
        *themes,
        "",
        "# SUBCATEGORIES (theme.subcategory)",
        *subcategories,
        "",
        "# SUBCATEGORY_TO_THEME",
    ]
    lines.extend(
        f"{subcategory} -> {mapping[subcategory]}" for subcategory in subcategories
    )
    lines.extend(["```", END])
    return "\n".join(lines)


def update_doc() -> None:
    themes, subcategories, mapping = load_taxonomy()
    rendered = render_block(themes, subcategories, mapping)
    doc = DOC_PATH.read_text(encoding="utf-8")
    start = doc.find(BEGIN)
    stop = doc.find(END)
    if start == -1 or stop == -1 or stop < start:
        raise SystemExit(f"Generated taxonomy markers not found in {DOC_PATH}")
    stop += len(END)
    updated = f"{doc[:start]}{rendered}{doc[stop:]}"
    DOC_PATH.write_text(updated, encoding="utf-8")


if __name__ == "__main__":
    update_doc()
