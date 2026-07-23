#!/usr/bin/env python3
"""Execute the live Python work-item argument contract with minimal stubs."""

from __future__ import annotations

import ast
import json
import pathlib
import sys
from types import SimpleNamespace
from typing import Any

WORK_ITEM_DATASETS = (
    "work-items",
    "work-item-labels",
    "work-item-projects",
    "work-item-history",
    "work-item-comments",
)


def selected_module(source: pathlib.Path) -> dict[str, Any]:
    tree = ast.parse(source.read_text(encoding="utf-8"), filename=str(source))
    selected: list[ast.stmt] = []
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name in {
            "_explicit_flags",
            "_work_item_kwargs",
        }:
            selected.append(node)
        elif isinstance(node, ast.Assign) and any(
            isinstance(target, ast.Name) and target.id == "_PROCESSOR_FLAG_NAMES"
            for target in node.targets
        ):
            selected.append(node)
    namespace: dict[str, Any] = {
        "Any": Any,
        "SyncTaskContext": Any,
        "_WORK_ITEM_DATASETS": set(WORK_ITEM_DATASETS),
        "_window_day": lambda _context: "day",
        "_window_backfill_days": lambda _context: 1,
        "_credentials_mapping": lambda _context: {},
        "_gitlab_credentials": lambda _context: ("token", "https://gitlab.example"),
    }
    exec(
        compile(ast.Module(body=selected, type_ignores=[]), str(source), "exec"),
        namespace,
    )
    return namespace


def main() -> int:
    if len(sys.argv) != 2:
        return 2
    namespace = selected_module(pathlib.Path(sys.argv[1]).resolve())
    work_item_kwargs = namespace["_work_item_kwargs"]
    result: dict[str, dict[str, dict[str, object]]] = {}
    for provider in ("github", "gitlab"):
        result[provider] = {}
        for dataset in WORK_ITEM_DATASETS:
            variants: list[dict[str, object]] = []
            for sync_prs in (False, True):
                context = SimpleNamespace(
                    db_url="clickhouse://fixture",
                    provider=provider,
                    org_id="org",
                    source_external_id="source",
                    source_is_org_wide_placeholder=False,
                    dataset_key=dataset,
                    processor_flags={"sync_prs": sync_prs},
                )
                kwargs = work_item_kwargs(context)
                variants.append(
                    {
                        "sync_prs": sync_prs,
                        "include_issues": kwargs.get("include_issues"),
                        "include_pull_requests": kwargs.get("include_pull_requests"),
                        "has_fetch_comments": "fetch_comments" in kwargs,
                        "has_fetch_milestones": "fetch_milestones" in kwargs,
                    }
                )
            result[provider][dataset] = {"variants": variants}
    json.dump(result, sys.stdout, sort_keys=True, separators=(",", ":"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
