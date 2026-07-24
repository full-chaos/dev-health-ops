from __future__ import annotations

import ast
import json
import sys
from pathlib import Path


def _dataset_values(path: Path) -> dict[str, str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    for node in tree.body:
        if isinstance(node, ast.ClassDef) and node.name == "DatasetKey":
            return {
                statement.targets[0].id: statement.value.value
                for statement in node.body
                if isinstance(statement, ast.Assign)
                and len(statement.targets) == 1
                and isinstance(statement.targets[0], ast.Name)
                and isinstance(statement.value, ast.Constant)
                and isinstance(statement.value.value, str)
            }
    raise RuntimeError("DatasetKey enum not found")


def _frozenset(
    tree: ast.Module,
    name: str,
    dataset_values: dict[str, str],
) -> list[str]:
    for node in tree.body:
        if (
            isinstance(node, ast.Assign)
            and any(
                isinstance(target, ast.Name) and target.id == name
                for target in node.targets
            )
            and isinstance(node.value, ast.Call)
            and isinstance(node.value.func, ast.Name)
            and node.value.func.id == "frozenset"
            and len(node.value.args) == 1
            and isinstance(node.value.args[0], ast.Set)
        ):
            values: list[str] = []
            for element in node.value.args[0].elts:
                if isinstance(element, ast.Constant) and isinstance(element.value, str):
                    values.append(element.value)
                    continue
                if (
                    isinstance(element, ast.Attribute)
                    and element.attr == "value"
                    and isinstance(element.value, ast.Attribute)
                    and isinstance(element.value.value, ast.Name)
                    and element.value.value.id == "DatasetKey"
                ):
                    values.append(dataset_values[element.value.attr])
                    continue
                raise RuntimeError(f"unsupported {name} entry: {ast.dump(element)}")
            return sorted(values)
    raise RuntimeError(f"{name} not found")


def main() -> int:
    if len(sys.argv) != 3:
        raise SystemExit("usage: python_linear_recovery_oracle.py SYNC_UNITS DATASETS")
    sync_units = Path(sys.argv[1])
    dataset_values = _dataset_values(Path(sys.argv[2]))
    tree = ast.parse(sync_units.read_text(encoding="utf-8"), filename=str(sync_units))
    print(
        json.dumps(
            {
                "datasets": _frozenset(
                    tree,
                    "_LINEAR_BACKFILL_WORK_ITEM_DATASETS",
                    dataset_values,
                ),
                "retry_surfaces": _frozenset(
                    tree,
                    "_LINEAR_BACKFILL_WORK_ITEM_IN_BAND_WRITE_SURFACES",
                    dataset_values,
                ),
                "proven_safe_surfaces": _frozenset(
                    tree,
                    "_CLICKHOUSE_RETRY_PROVEN_SAFE_SURFACES",
                    dataset_values,
                ),
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
