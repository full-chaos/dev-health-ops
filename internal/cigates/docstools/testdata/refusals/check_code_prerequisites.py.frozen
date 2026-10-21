#!/usr/bin/env python3
"""Guard that credentialed CLI code samples link to a setup prerequisite.

Any docs page containing a fenced code sample that sets ``CLICKHOUSE_URI`` or
``POSTGRES_URI`` must also link to ``getting-started.md``, so a reader lands
on the credential-setup prerequisite before running the sample. This guard is
scoped to an explicit participant list (``docs/code-prerequisite-scope.yml``)
rather than the whole corpus, so it can be adopted page-by-page as existing
content is rewritten instead of failing on unrelated legacy pages in one
shot.
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DOCS_ROOT = ROOT / "docs"
DEFAULT_SCOPE = ROOT / "docs" / "code-prerequisite-scope.yml"

CODE_FENCE_RE = re.compile(r"```[a-zA-Z]*\n(.*?)```", re.DOTALL)
CREDENTIAL_ENV_RE = re.compile(r"\b(?:CLICKHOUSE_URI|POSTGRES_URI)=")
PREREQUISITE_LINK_RE = re.compile(r"\]\([^)]*getting-started\.md[^)]*\)")


def load_scope(path: Path) -> frozenset[str]:
    if not path.is_file():
        return frozenset()
    raw = yaml.safe_load(path.read_text(encoding="utf-8")) or []
    if not isinstance(raw, list) or not all(isinstance(item, str) for item in raw):
        raise ValueError(f"{path} must be a YAML list of relative doc paths")
    return frozenset(raw)


def has_credentialed_sample(text: str) -> bool:
    return any(CREDENTIAL_ENV_RE.search(block) for block in CODE_FENCE_RE.findall(text))


def pages_missing_prerequisite_link(
    docs_root: Path, scope: frozenset[str]
) -> list[str]:
    missing: list[str] = []
    for relpath in sorted(scope):
        doc_path = docs_root / relpath
        if not doc_path.is_file():
            missing.append(f"{relpath}: listed in scope but the page does not exist")
            continue
        text = doc_path.read_text(encoding="utf-8")
        if has_credentialed_sample(text) and not PREREQUISITE_LINK_RE.search(text):
            missing.append(relpath)
    return missing


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(
        description="Check that in-scope credentialed code samples link to getting-started.md."
    )
    parser.add_argument("--docs-root", type=Path, default=DEFAULT_DOCS_ROOT)
    parser.add_argument("--scope", type=Path, default=DEFAULT_SCOPE)
    args = parser.parse_args(argv)

    scope = load_scope(args.scope)
    missing = pages_missing_prerequisite_link(args.docs_root, scope)
    if missing:
        for entry in missing:
            print(f"ERROR: {entry}")
        return 1
    print("Code prerequisite check passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
