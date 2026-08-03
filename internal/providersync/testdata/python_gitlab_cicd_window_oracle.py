#!/usr/bin/env python3
from __future__ import annotations

import ast
import json
import pathlib
import sys


def _pipeline_window_name(processor_source: str) -> str:
    tree = ast.parse(processor_source)
    functions = [
        node
        for node in tree.body
        if isinstance(node, ast.AsyncFunctionDef)
        and node.name == "process_gitlab_project"
    ]
    if len(functions) != 1:
        raise ValueError("active process_gitlab_project function is not unique")
    calls: list[ast.Call] = []
    for node in ast.walk(functions[0]):
        if not isinstance(node, ast.Call) or len(node.args) < 6:
            continue
        function = node.func
        if not (
            isinstance(function, ast.Attribute)
            and function.attr == "run_in_executor"
            and isinstance(node.args[1], ast.Name)
            and node.args[1].id == "_fetch_gitlab_pipelines_sync"
        ):
            continue
        calls.append(node)
    if len(calls) != 1:
        raise ValueError(
            "active process_gitlab_project pipeline call must be uniquely discoverable"
        )
    max_argument = calls[0].args[5]
    if not isinstance(max_argument, ast.Name):
        raise ValueError(
            "pipeline max argument is no longer a named production constant"
        )
    return max_argument.id


def _imported_from_utils(processor_source: str, name: str) -> bool:
    tree = ast.parse(processor_source)
    return any(
        isinstance(node, ast.ImportFrom)
        and node.module == "dev_health_ops.utils"
        and any(alias.name == name and alias.asname is None for alias in node.names)
        for node in tree.body
    )


def _integer_assignment(source: str, name: str) -> int:
    tree = ast.parse(source)
    matches = [
        node.value.value
        for node in tree.body
        if isinstance(node, ast.Assign)
        and len(node.targets) == 1
        and isinstance(node.targets[0], ast.Name)
        and node.targets[0].id == name
        and isinstance(node.value, ast.Constant)
        and isinstance(node.value.value, int)
    ]
    if len(matches) != 1:
        raise ValueError(f"{name} must have one literal integer assignment")
    return matches[0]


def main() -> int:
    if len(sys.argv) != 3:
        return 2
    processor = pathlib.Path(sys.argv[1]).read_text()
    utils = pathlib.Path(sys.argv[2]).read_text()
    name = _pipeline_window_name(processor)
    if not _imported_from_utils(processor, name):
        raise ValueError(
            f"pipeline max {name} is not imported from dev_health_ops.utils"
        )
    json.dump({"max_pipelines": _integer_assignment(utils, name)}, sys.stdout)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
