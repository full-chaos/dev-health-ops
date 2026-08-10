"""GitLab files row oracle over the live worker persistence boundary.

The pair executes ``backfill_file_records`` from the real Python worker
module, not a hand-written GitFile constructor. The fake sink only records the
objects handed to the persistence boundary; it does not normalize or select
fields on the producer's behalf.
"""

from __future__ import annotations

import asyncio
import pathlib
from typing import Any

from internal.providersync.testdata import oracle_registry
from internal.providersync.testdata.field_reflection import class_annotated_field_names
from internal.providersync.testdata.python_oracle_loader import load_live_module

REPO_ROOT = pathlib.Path(__file__).resolve().parents[4]
BASE_GIT_SOURCE = REPO_ROOT / "src/dev_health_ops/processors/base_git.py"
MODEL_SOURCE = REPO_ROOT / "src/dev_health_ops/models/git.py"


def _reflected_fields() -> frozenset[str]:
    return class_annotated_field_names(MODEL_SOURCE.read_text(), "GitFile")


def _build_row(case: dict[str, Any]) -> dict[str, Any]:
    base_git = load_live_module(BASE_GIT_SOURCE)
    inserted: list[Any] = []

    class Sink:
        async def get_git_file_contents_by_path(self, _repo_id: Any) -> dict[str, str]:
            return {
                str(path): str(value)
                for path, value in case.get("existing_contents", {}).items()
            }

        async def insert_git_file_data(self, rows: list[Any]) -> None:
            inserted.extend(rows)

    async def run() -> None:
        await base_git.backfill_file_records(
            Sink(),
            case["repo_id"],
            [case["path"]],
            case.get("project_full_name", "Acme/API"),
            contents_by_path={
                str(path): str(value)
                for path, value in case.get("contents_by_path", {}).items()
            },
        )

    asyncio.run(run())
    if len(inserted) != 1:
        raise ValueError(f"GitLab files worker produced {len(inserted)} rows")
    return vars(inserted[0])


oracle_registry.register(
    oracle_registry.PairSpec(
        id="gitlab/files/row",
        build_row=_build_row,
        reflected_fields=_reflected_fields,
        excluded_fields={
            "last_synced": "Python GitFile defaults this persistence timestamp after backfill_file_records; Go carries the normalized collection instant",
        },
    )
)
