from __future__ import annotations

import asyncio
import pathlib
from typing import Any

from internal.providersync.testdata import oracle_registry
from internal.providersync.testdata.field_reflection import class_annotated_field_names
from internal.providersync.testdata.python_oracle_loader import load_live_module

REPO_ROOT = pathlib.Path(__file__).resolve().parents[4]
_BASE_GIT_SOURCE = REPO_ROOT / "src/dev_health_ops/processors/base_git.py"
_MODEL_SOURCE = REPO_ROOT / "src/dev_health_ops/models/git.py"


class _Sink:
    def __init__(self) -> None:
        self.rows: list[Any] = []

    async def insert_git_file_data(self, rows: list[Any]) -> None:
        self.rows.extend(rows)


def _reflected_fields() -> frozenset[str]:
    return class_annotated_field_names(_MODEL_SOURCE.read_text(), "GitFile")


async def _build_row_async(case: dict[str, Any]) -> dict[str, Any]:
    base_git = load_live_module(_BASE_GIT_SOURCE)
    sink = _Sink()
    await base_git.backfill_file_records(
        sink,
        case["repo_id"],
        [case["path"]],
        "acme/api",
        contents_by_path=case.get("contents_by_path"),
    )
    return dict(vars(sink.rows[0]))


def _build_row(case: dict[str, Any]) -> dict[str, Any]:
    return asyncio.run(_build_row_async(case))


oracle_registry.register(
    oracle_registry.PairSpec(
        id="github/files/row",
        build_row=_build_row,
        reflected_fields=_reflected_fields,
        excluded_fields={
            "last_synced": "stamped by ClickHouseStore.insert_git_file_data, not backfill_file_records",
        },
    )
)
