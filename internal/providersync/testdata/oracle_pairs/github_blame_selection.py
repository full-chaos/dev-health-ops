"""Live select_unblamed_paths oracle for CHAOS-3343.

The oracle executes processors/base_git.py::select_unblamed_paths itself. Its
single ``paths`` wrapper key carries the function's complete list result; no
production result field is selected or omitted by the wrapper.
"""

from __future__ import annotations

import asyncio
import pathlib
from typing import Any

from internal.providersync.testdata import oracle_registry
from internal.providersync.testdata.python_oracle_loader import load_live_module

REPO_ROOT = pathlib.Path(__file__).resolve().parents[4]
_BASE_GIT_SOURCE = REPO_ROOT / "src/dev_health_ops/processors/base_git.py"


class _Store:
    def __init__(self, paths: list[str]) -> None:
        self.paths = paths

    async def get_blamed_paths(self, _repo_id: str) -> set[str]:
        return set(self.paths)


def _build_selection(case: dict[str, Any]) -> dict[str, Any]:
    module = load_live_module(_BASE_GIT_SOURCE)
    selected = asyncio.run(
        module.select_unblamed_paths(
            _Store(list(case["blamed_paths"])),
            "repo-id",
            list(case["file_paths"]),
            int(case["max_files"]),
        )
    )
    return {"paths": selected}


oracle_registry.register(
    oracle_registry.PairSpec(
        id="github/blame/selection",
        build_row=_build_selection,
        # The production boundary returns one list rather than a record. The
        # wrapper exposes that entire value under one protocol key, so there
        # is no narrower field set that could hide a production result.
        reflected_fields=lambda: frozenset({"paths"}),
    )
)
