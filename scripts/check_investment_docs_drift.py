#!/usr/bin/env python3
"""Fail on factual drift between investment docs and canonical Python registries."""

from __future__ import annotations

import ast
import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DOCS = ROOT / "docs"
TAXONOMY_PATH = ROOT / "src" / "dev_health_ops" / "investment_taxonomy.py"
TAXONOMY_DOC = DOCS / "reference" / "taxonomies" / "investment.md"

BEGIN = "<!-- BEGIN GENERATED TAXONOMY -->"
END = "<!-- END GENERATED TAXONOMY -->"
KEY_RE = re.compile(r"`([a-z][a-z_]*(?:\.[a-z][a-z_]*)?)`")
JSON_BLOCK_RE = re.compile(r"```json\s*(.*?)\s*```", re.DOTALL)
DOC_KEY_PLACEHOLDERS = {"theme.subcategory"}


def _literal_set(path: Path, name: str) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
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
            raise ValueError(f"{name} is not a set literal in {path}")
        values: set[str] = set()
        for element in value.elts:
            if not isinstance(element, ast.Constant) or not isinstance(
                element.value, str
            ):
                raise ValueError(f"{name} contains a non-string literal")
            values.add(element.value)
        return values
    raise ValueError(f"{name} not found in {path}")


def _generated_block(doc: str) -> str:
    start = doc.find(BEGIN)
    stop = doc.find(END)
    if start == -1 or stop == -1 or stop < start:
        raise ValueError(f"generated taxonomy markers missing in {TAXONOMY_DOC}")
    return doc[start + len(BEGIN) : stop]


def _keys_from_generated_block(block: str) -> tuple[set[str], set[str], dict[str, str]]:
    themes: set[str] = set()
    subcategories: set[str] = set()
    mapping: dict[str, str] = {}
    section = ""
    for raw_line in block.splitlines():
        line = raw_line.strip()
        if not line or line == "```text" or line == "```":
            continue
        if line.startswith("# "):
            section = line.removeprefix("# ")
            continue
        if section == "THEMES":
            themes.add(line)
        elif section == "SUBCATEGORIES (theme.subcategory)":
            subcategories.add(line)
        elif section == "SUBCATEGORY_TO_THEME":
            subcategory, separator, theme = line.partition(" -> ")
            if not separator:
                raise ValueError(f"bad mapping line in generated block: {line}")
            mapping[subcategory] = theme
    return themes, subcategories, mapping


def check_taxonomy_doc() -> list[str]:
    errors: list[str] = []
    canonical_themes = _literal_set(TAXONOMY_PATH, "THEMES")
    canonical_subcategories = _literal_set(TAXONOMY_PATH, "SUBCATEGORIES")
    canonical_mapping = {
        subcategory: subcategory.split(".", 1)[0]
        for subcategory in canonical_subcategories
    }

    doc = TAXONOMY_DOC.read_text(encoding="utf-8")
    block_themes, block_subcategories, block_mapping = _keys_from_generated_block(
        _generated_block(doc)
    )
    if block_themes != canonical_themes:
        errors.append(
            f"generated theme keys drift: expected {sorted(canonical_themes)}, "
            f"found {sorted(block_themes)}"
        )
    if block_subcategories != canonical_subcategories:
        errors.append(
            "generated subcategory keys drift: "
            f"expected {sorted(canonical_subcategories)}, found {sorted(block_subcategories)}"
        )
    if block_mapping != canonical_mapping:
        errors.append(
            f"generated subcategory mapping drift: expected {canonical_mapping}, "
            f"found {block_mapping}"
        )

    documented = set(KEY_RE.findall(doc))
    documented_themes = {
        key for key in documented if "." not in key and key in canonical_themes
    }
    documented_subcategories = {
        key
        for key in documented
        if "." in key and not key.endswith(".py") and key not in DOC_KEY_PLACEHOLDERS
    }
    unknown_subcategories = documented_subcategories - canonical_subcategories
    if unknown_subcategories:
        errors.append(
            f"documented unknown subcategory keys: {sorted(unknown_subcategories)}"
        )
    if documented_themes != canonical_themes:
        errors.append(
            f"documented theme keys drift: expected {sorted(canonical_themes)}, "
            f"found {sorted(documented_themes)}"
        )
    if not canonical_subcategories.issubset(documented_subcategories):
        errors.append(
            "documented subcategory keys missing: "
            f"{sorted(canonical_subcategories - documented_subcategories)}"
        )
    return errors


def _unknown_taxonomy_example_keys(document: str, source: str) -> list[str]:
    errors: list[str] = []
    canonical_themes = _literal_set(TAXONOMY_PATH, "THEMES")
    canonical_subcategories = _literal_set(TAXONOMY_PATH, "SUBCATEGORIES")
    for index, raw_json in enumerate(JSON_BLOCK_RE.findall(document), start=1):
        try:
            payload = json.loads(raw_json)
        except json.JSONDecodeError:
            continue
        if not isinstance(payload, dict):
            continue
        taxonomies = [payload]
        investment = payload.get("investment")
        if isinstance(investment, dict):
            taxonomies.append(investment)
        for taxonomy in taxonomies:
            for field, canonical in (
                ("themes", canonical_themes),
                ("subcategories", canonical_subcategories),
            ):
                values = taxonomy.get(field)
                if not isinstance(values, dict):
                    continue
                unknown_keys = set(values) - canonical
                if unknown_keys:
                    errors.append(
                        f"{source} JSON example #{index} unknown {field} keys: "
                        f"{sorted(unknown_keys)}"
                    )
    return errors


def check_published_investment_examples() -> list[str]:
    errors: list[str] = []
    for path in sorted(DOCS.rglob("*.md")):
        relpath = path.relative_to(DOCS).as_posix()
        errors.extend(
            _unknown_taxonomy_example_keys(path.read_text(encoding="utf-8"), relpath)
        )
    return errors


def main() -> int:
    errors = [
        *check_taxonomy_doc(),
        *check_published_investment_examples(),
    ]
    if errors:
        for error in errors:
            print(f"ERROR: {error}")
        return 1
    print("Investment docs drift check passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
